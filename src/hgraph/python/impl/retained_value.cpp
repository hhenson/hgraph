#include <hgraph/python/retained_value.h>

#include <hgraph/python/object_semantics.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/utils/intern_table.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_ops.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

// The Python-retained value entry (moved here from
// types/metadata/value_plan_factory.cpp: the remaining layering step named in
// python_bridge.rst). The plan factory calls into this unit only through the
// PythonStorageProvider table registered below.
namespace hgraph::python_bridge
{
    namespace
    {
        [[nodiscard]] const PyBundleClassInfo *retained_bundle_info(const ValueTypeMetaData *schema)
        {
            const auto &registry = bundle_class_info_registry();
            const auto  found    = registry.find(schema);
            return found != registry.end() ? &found->second : nullptr;
        }

        [[nodiscard]] bool
        retained_python_supported(const ValueTypeMetaData *schema) noexcept {
          if (schema == nullptr) {
            return false;
          }
          if (python_bridge::is_python_bundle_schema(schema)) {
            return true;
          }
          switch (schema->value_kind()) {
          case ValueTypeKind::Atomic:
            return schema == scalar_descriptor<Bool>::value_meta() ||
                   schema == scalar_descriptor<Int>::value_meta() ||
                   schema == scalar_descriptor<Float>::value_meta() ||
                   schema == scalar_descriptor<Str>::value_meta() ||
                   schema == scalar_descriptor<Bytes>::value_meta();
          case ValueTypeKind::Tuple:
            for (std::size_t index = 0; index < schema->field_count; ++index) {
              if (!retained_python_supported(schema->fields[index].type)) {
                return false;
              }
            }
            return true;
          case ValueTypeKind::List:
            return schema->fixed_size == 0 &&
                   !schema->has(ValueTypeFlags::ShapedArray) &&
                   retained_python_supported(schema->element_type);
          case ValueTypeKind::Bundle:
            return retained_bundle_info(schema) != nullptr;
          case ValueTypeKind::Set:
          case ValueTypeKind::Map:
          case ValueTypeKind::CyclicBuffer:
          case ValueTypeKind::Queue:
          case ValueTypeKind::Any:
            return false;
          }
          return false;
        }

        [[nodiscard]] bool
        python_cache_beneficial(ValueTypeRef native_binding) noexcept {
          const auto *schema = native_binding.schema();
          return retained_python_supported(schema) &&
                 !python_bridge::is_python_bundle_binding(native_binding) &&
                 schema != scalar_descriptor<Bool>::value_meta() &&
                 schema != scalar_descriptor<Int>::value_meta() &&
                 schema != scalar_descriptor<Float>::value_meta();
        }

        [[nodiscard]] ValueTypeRef
        python_bundle_storage_binding(ValueTypeRef source_binding) {
          if (python_bridge::is_python_bundle_binding(source_binding)) {
            return source_binding;
          }
          const auto *schema = source_binding.schema();
          if (!python_bridge::is_python_bundle_schema(schema)) {
            return {};
          }
          const auto *ops = try_value_ops<IndexedValueOps>(source_binding);
          if (ops == nullptr || ops->element_binding == nullptr) {
            return {};
          }
          std::vector<ValueTypeRef> fields;
          fields.reserve(schema->field_count);
          for (std::size_t index = 0; index < schema->field_count; ++index) {
            const auto field = ops->element_binding(ops->context, nullptr, index);
            if (!field) {
              return {};
            }
            fields.push_back(field);
          }
          return python_bridge::python_bundle_binding_for(schema, fields);
        }

        [[noreturn]] void invalid_retained_python_value(const ValueTypeMetaData &schema,
                                                        std::string_view reason) {
          throw nb::type_error(("Python value for '" + std::string{schema.name()} +
                                "' " + std::string{reason})
                                   .c_str());
        }

        [[nodiscard]] nb::object
        prepare_retained_python_value(const ValueTypeMetaData *schema,
                                      nb::handle source) {
          if (schema == nullptr || !source.is_valid() || source.is_none()) {
            throw nb::type_error(
                "retained Python output storage requires a typed non-None value");
          }
          if (schema->value_kind() == ValueTypeKind::Bundle) {
            const auto *info = retained_bundle_info(schema);
            if (info == nullptr || !info->type.is_valid()) {
              invalid_retained_python_value(*schema, "has no registered Python class");
            }
            if (nb::isinstance(source, info->type)) {
              return nb::borrow<nb::object>(source);
            }
            // Dict and other accepted bridge inputs must retain the declared Python
            // read shape, not the raw source object.
            const auto binding = ValuePlanFactory::instance().type_for(schema);
            Value normalized{binding};
            binding.ops_ref().from_python(
                binding, const_cast<void *>(normalized.view().data()), source);
            return binding.ops_ref().to_python(normalized.view().data());
          }

          if (schema->value_kind() == ValueTypeKind::Atomic) {
            if (schema == scalar_descriptor<Bool>::value_meta()) {
              if (!PyBool_Check(source.ptr())) {
                invalid_retained_python_value(*schema, "requires bool");
              }
              return nb::borrow<nb::object>(source);
            }
            if (schema == scalar_descriptor<Int>::value_meta()) {
              if (PyLong_CheckExact(source.ptr())) {
                static_cast<void>(PyLong_AsLongLong(source.ptr()));
                if (PyErr_Occurred() != nullptr) {
                  throw nb::python_error();
                }
                return nb::borrow<nb::object>(source);
              }
              if (PyUnicode_Check(source.ptr()) || PyBytes_Check(source.ptr())) {
                invalid_retained_python_value(*schema, "requires an integer");
              }
              nb::object normalized = nb::steal(PyNumber_Long(source.ptr()));
              if (!normalized.is_valid()) {
                throw nb::python_error();
              }
              static_cast<void>(PyLong_AsLongLong(normalized.ptr()));
              if (PyErr_Occurred() != nullptr) {
                throw nb::python_error();
              }
              return normalized;
            }
            if (schema == scalar_descriptor<Float>::value_meta()) {
              if (PyFloat_CheckExact(source.ptr())) {
                return nb::borrow<nb::object>(source);
              }
              if (PyUnicode_Check(source.ptr()) || PyBytes_Check(source.ptr())) {
                invalid_retained_python_value(*schema, "requires a number");
              }
              nb::object normalized = nb::steal(PyNumber_Float(source.ptr()));
              if (!normalized.is_valid()) {
                throw nb::python_error();
              }
              return normalized;
            }
            if (schema == scalar_descriptor<Str>::value_meta()) {
              if (!PyUnicode_Check(source.ptr())) {
                invalid_retained_python_value(*schema, "requires str");
              }
              return PyUnicode_CheckExact(source.ptr())
                         ? nb::borrow<nb::object>(source)
                         : nb::steal(PyUnicode_FromObject(source.ptr()));
            }
            if (schema == scalar_descriptor<Bytes>::value_meta()) {
              if (!PyBytes_Check(source.ptr())) {
                invalid_retained_python_value(*schema, "requires bytes");
              }
              if (PyBytes_CheckExact(source.ptr())) {
                return nb::borrow<nb::object>(source);
              }
              char *buffer = nullptr;
              Py_ssize_t length = 0;
              if (PyBytes_AsStringAndSize(source.ptr(), &buffer, &length) != 0) {
                throw nb::python_error();
              }
              return nb::steal(PyBytes_FromStringAndSize(buffer, length));
            }
            invalid_retained_python_value(*schema, "has no Python-only storage policy");
          }

          if (schema->value_kind() == ValueTypeKind::Tuple) {
            if (PySequence_Check(source.ptr()) == 0) {
              invalid_retained_python_value(*schema, "expects a Python list or tuple");
            }
            const Py_ssize_t count = PySequence_Size(source.ptr());
            if (count < 0) {
              throw nb::python_error();
            }
            const auto field_count = static_cast<Py_ssize_t>(schema->field_count);
            if (count < field_count) {
              invalid_retained_python_value(
                  *schema, "does not provide every declared tuple field");
            }
            bool exact = PyTuple_CheckExact(source.ptr()) && count == field_count;
            nb::tuple normalized = nb::steal<nb::tuple>(PyTuple_New(field_count));
            if (!normalized.is_valid()) {
              throw nb::python_error();
            }
            for (Py_ssize_t index = 0; index < field_count; ++index) {
              nb::object item = nb::steal(PySequence_GetItem(source.ptr(), index));
              if (!item.is_valid()) {
                throw nb::python_error();
              }
              nb::object prepared =
                  item.is_none()
                      ? nb::none()
                      : prepare_retained_python_value(
                            schema->fields[static_cast<std::size_t>(index)].type, item);
              exact = exact && prepared.is(item);
              if (PyTuple_SetItem(normalized.ptr(), index, prepared.release().ptr()) !=
                  0) {
                throw nb::python_error();
              }
            }
            return exact ? nb::borrow<nb::object>(source)
                         : nb::object{std::move(normalized)};
          }

          if (schema->value_kind() == ValueTypeKind::List && schema->fixed_size == 0) {
            const bool sequence = PyList_Check(source.ptr()) ||
                                  PyTuple_Check(source.ptr()) ||
                                  nb::hasattr(source, "__array_interface__");
            if (!sequence) {
              invalid_retained_python_value(*schema, "expects a Python list or tuple");
            }
            nb::object source_object = nb::borrow<nb::object>(source);
            nb::list prepared_items;
            bool unchanged = schema->has(ValueTypeFlags::VariadicTuple)
                                 ? PyTuple_CheckExact(source.ptr())
                                 : PyList_CheckExact(source.ptr());
            for (nb::handle item : nb::iter(source_object)) {
              if (item.is_none()) {
                invalid_retained_python_value(*schema, "does not allow None elements");
              }
              nb::object prepared =
                  prepare_retained_python_value(schema->element_type, item);
              unchanged = unchanged && prepared.is(item);
              prepared_items.append(std::move(prepared));
            }
            if (unchanged) {
              return source_object;
            }
            return schema->has(ValueTypeFlags::VariadicTuple)
                       ? nb::object{nb::tuple(prepared_items)}
                       : nb::object{std::move(prepared_items)};
          }

          invalid_retained_python_value(*schema, "has no Python-only storage policy");
        }

        struct PythonRetainedBindingEntry {
          const ValueTypeMetaData *schema{nullptr};
          ValueOps ops{};
          ValueTypeRef binding{};

          explicit PythonRetainedBindingEntry(const ValueTypeMetaData *value_schema)
              : schema(value_schema) {
            if (!retained_python_supported(schema) ||
                python_bridge::is_python_bundle_schema(schema)) {
              throw std::invalid_argument(
                  "Python-retained binding requires a supported non-native schema");
            }
            ops.kind = ValueOpsKind::Base;
            ops.context = this;
            ops.allows_mutation = false;
            ops.hash_impl = schema->is_hashable() ? &hash : nullptr;
            ops.equals_impl = schema->is_equatable() ? &equals : nullptr;
            ops.compare_impl = schema->is_comparable() ? &compare : nullptr;
            ops.to_string_impl = &to_string;
            ops.to_python_impl = &to_python;
            ops.from_python_impl = &from_python;
            ops.accepts_source_impl = &accepts_source;
            ops.copy_assign_from_impl = &copy_assign_from;
            ops.move_assign_from_impl = &move_assign_from;
            ops.format_string_impl = &to_string;
            binding = intern_value_type(
                *schema, MemoryUtils::plan_for<python_bridge::PythonValueHolder>(),
                ops);
          }

          [[nodiscard]] static const PythonRetainedBindingEntry &
          entry(const void *context) noexcept {
            return *static_cast<const PythonRetainedBindingEntry *>(context);
          }

          [[nodiscard]] static python_bridge::PythonValueHolder &value(void *memory) {
            if (memory == nullptr) {
              throw std::logic_error(
                  "Python-retained value operation requires live memory");
            }
            return *static_cast<python_bridge::PythonValueHolder *>(memory);
          }

          [[nodiscard]] static const python_bridge::PythonValueHolder &
          value(const void *memory) {
            if (memory == nullptr) {
              throw std::logic_error(
                  "Python-retained value operation requires live memory");
            }
            return *static_cast<const python_bridge::PythonValueHolder *>(memory);
          }

          // Value semantics come from the bridge's one set of Python-object
          // primitives (python_bridge::object_*); this entry owns only the holder.
          static std::size_t hash(const void *, const void *memory) {
            const auto &stored = value(memory);
            if (stored.object == nullptr) {
              throw std::logic_error("cannot hash an empty Python-retained value");
            }
            return python_bridge::object_hash(stored.object);
          }

          static bool equals(const void *, const void *lhs, const void *rhs) {
            return python_bridge::object_equals(value(lhs).object, value(rhs).object);
          }

          static std::partial_ordering compare(const void *, const void *lhs,
                                               const void *rhs) noexcept {
            return python_bridge::object_compare(value(lhs).object, value(rhs).object);
          }

          static std::string to_string(const void *, const void *memory) {
            const auto &stored = value(memory);
            if (stored.object == nullptr) {
              return "<empty Python-retained value>";
            }
            return python_bridge::object_str(stored.object);
          }

          static nb::object to_python(const void *, const void *memory) {
            return value(memory).get();
          }

          static void from_python(const void *context, const ValueTypeRef &,
                                  void *memory, nb::handle source) {
            const auto &self = entry(context);
            value(memory).set(prepare_retained_python_value(self.schema, source));
          }

          static bool accepts_source(const void *context, ValueTypeRef binding,
                                     ValueTypeRef source) noexcept {
            const auto &self = entry(context);
            return binding == self.binding && source && source.schema() == self.schema;
          }

          static void copy_assign_from(const void *context, ValueTypeRef, void *dst,
                                       ValueTypeRef source, const void *src) {
            const auto &self = entry(context);
            if (source == self.binding) {
              value(dst) = value(src);
              return;
            }
            nb::gil_scoped_acquire gil;
            value(dst).set(prepare_retained_python_value(
                self.schema, source.ops_ref().to_python(src)));
          }

          static void move_assign_from(const void *context, ValueTypeRef binding,
                                       void *dst, ValueTypeRef source, void *src) {
            const auto &self = entry(context);
            if (source == self.binding) {
              value(dst) = std::move(value(src));
              return;
            }
            copy_assign_from(context, binding, dst, source, src);
          }
        };

        // Entries are address-published AND immortal: python Values can outlive a
        // registry reset, so a reset drops only the key index (clear_index) while
        // every entry stays alive. The table itself is leaked for the same reason.
        [[nodiscard]] InternTable<const ValueTypeMetaData *,
                                  std::unique_ptr<PythonRetainedBindingEntry>> &
        python_retained_bindings() noexcept {
          static auto *bindings =
              new InternTable<const ValueTypeMetaData *,
                              std::unique_ptr<PythonRetainedBindingEntry>>{};
          return *bindings;
        }

        [[nodiscard]] ValueTypeRef
        python_retained_binding_for(const ValueTypeMetaData *schema) {
          // intern_serialized: the entry ctor interns a TypeRecord pointing at
          // itself, so a racing loser must never be constructed and destroyed.
          return python_retained_bindings()
              .intern_serialized(
                  schema,
                  [&] { return std::make_unique<PythonRetainedBindingEntry>(schema); })
              ->binding;
        }

        void clear_python_retained_bindings() noexcept {
          python_retained_bindings().clear_index();
        }

        const ValuePlanFactory::PythonStorageProvider &provider_table()
        {
            static const ValuePlanFactory::PythonStorageProvider table{
                .retained_supported     = &retained_python_supported,
                .cache_beneficial       = &python_cache_beneficial,
                .bundle_storage_binding = &python_bundle_storage_binding,
                .python_bundle_schema   = &is_python_bundle_schema,
                .retained_binding_for   = &python_retained_binding_for,
                .python_holder_plan     = [] { return &MemoryUtils::plan_for<PythonValueHolder>(); },
                .clear_retained_bindings = &clear_python_retained_bindings,
            };
            return table;
        }
    }  // namespace

    void register_python_storage_provider() noexcept
    {
        ValuePlanFactory::set_python_storage_provider(&provider_table());
    }

    nb::object prepare_python_storage_value(const ValueTypeMetaData *schema, nb::handle source)
    {
        if (!retained_python_supported(schema))
        {
            throw std::logic_error("ValuePlanFactory has no retained-Python policy for this schema");
        }
        return prepare_retained_python_value(schema, source);
    }
}  // namespace hgraph::python_bridge
