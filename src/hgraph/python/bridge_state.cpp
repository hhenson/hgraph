#include <hgraph/python/bridge_state.h>
#include <hgraph/python/native_scalar_registration.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES

#include <hgraph/types/frame.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/series.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value_callable.h>
#include <hgraph/types/wired_fn.h>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph::python_bridge {
PythonValueHolder::PythonValueHolder(const PythonValueHolder &other)
    : object(other.object) {
  if (object != nullptr) {
    nb::gil_scoped_acquire gil;
    Py_INCREF(object);
  }
}

PythonValueHolder::PythonValueHolder(PythonValueHolder &&other) noexcept
    : object(std::exchange(other.object, nullptr)) {}

PythonValueHolder &
PythonValueHolder::operator=(const PythonValueHolder &other) {
  if (this != &other) {
    PythonValueHolder replacement{other};
    swap(replacement);
  }
  return *this;
}

PythonValueHolder &
PythonValueHolder::operator=(PythonValueHolder &&other) noexcept {
  if (this != &other) {
    PythonValueHolder replacement{std::move(other)};
    swap(replacement);
  }
  return *this;
}

PythonValueHolder::~PythonValueHolder() { clear(); }

void PythonValueHolder::clear() noexcept {
  if (object == nullptr) {
    return;
  }
  nb::gil_scoped_acquire gil;
  PyObject *previous = std::exchange(object, nullptr);
  Py_DECREF(previous);
}

void PythonValueHolder::set(nb::handle source) {
  if (!source.is_valid() || source.is_none()) {
    throw nb::type_error(
        "retained Python output value requires a non-None object");
  }
  PyObject *replacement = source.ptr();
  Py_INCREF(replacement);
  PyObject *previous = std::exchange(object, replacement);
  Py_XDECREF(previous);
}

nb::object PythonValueHolder::get() const {
  return object != nullptr ? nb::borrow<nb::object>(object) : nb::none();
}

namespace {
struct NativeScalarRegistrations {
  std::unordered_map<PyObject *,
                     std::pair<nb::object, const ValueTypeMetaData *>>
      by_python_type;
  std::unordered_map<const ValueTypeMetaData *, nb::object> by_native_type;
};

NativeScalarRegistrations &native_scalar_registrations() {
  static auto *registrations = new NativeScalarRegistrations{};
  return *registrations;
}

struct PythonBundleValue {
  PyObject *object{nullptr};
  const ValueTypeMetaData *active_schema{nullptr};
  ValueTypeRef active_binding{};
  mutable std::vector<Value> projected_fields{};

  PythonBundleValue() noexcept = default;

  PythonBundleValue(const PythonBundleValue &other)
      : object(other.object), active_schema(other.active_schema),
        active_binding(other.active_binding) {
    if (object != nullptr) {
      nb::gil_scoped_acquire gil;
      Py_INCREF(object);
    }
  }

  PythonBundleValue(PythonBundleValue &&other) noexcept
      : object(std::exchange(other.object, nullptr)),
        active_schema(std::exchange(other.active_schema, nullptr)),
        active_binding(std::move(other.active_binding)),
        projected_fields(std::move(other.projected_fields)) {}

  PythonBundleValue &operator=(const PythonBundleValue &other) {
    if (this == &other) {
      return *this;
    }
    PythonBundleValue replacement{other};
    swap(replacement);
    return *this;
  }

  PythonBundleValue &operator=(PythonBundleValue &&other) noexcept {
    if (this == &other) {
      return *this;
    }
    PythonBundleValue replacement{std::move(other)};
    swap(replacement);
    return *this;
  }

  ~PythonBundleValue() {
    projected_fields.clear();
    if (object != nullptr) {
      nb::gil_scoped_acquire gil;
      Py_DECREF(object);
    }
  }

  void swap(PythonBundleValue &other) noexcept {
    std::swap(object, other.object);
    std::swap(active_schema, other.active_schema);
    std::swap(active_binding, other.active_binding);
    projected_fields.swap(other.projected_fields);
  }

  void set(nb::handle source, const ValueTypeMetaData *schema,
           ValueTypeRef binding) {
    if (!source.is_valid() || source.is_none()) {
      throw nb::type_error("Python-backed Bundle requires a non-None object");
    }
    PyObject *replacement = source.ptr();
    Py_INCREF(replacement);
    PyObject *previous = object;
    object = replacement;
    active_schema = schema;
    active_binding = binding;
    projected_fields.clear();
    Py_XDECREF(previous);
  }
};

struct PythonBundleBindingEntry {
  const ValueTypeMetaData *schema{nullptr};
  std::vector<ValueTypeRef> field_bindings{};
  IndexedValueOps ops{};
  ValueTypeRef binding{};

  PythonBundleBindingEntry(const ValueTypeMetaData *value_schema,
                           std::vector<ValueTypeRef> realized_fields)
      : schema(value_schema), field_bindings(std::move(realized_fields)) {
    if (schema == nullptr || !schema->is_named_bundle()) {
      throw std::invalid_argument(
          "Python-owned value binding requires a named Bundle schema");
    }
    if (field_bindings.size() != schema->field_count) {
      throw std::invalid_argument(
          "Python-owned Bundle field binding count does not match its schema");
    }

    ops.kind = ValueOpsKind::Indexed;
    ops.context = this;
    ops.allows_mutation = false;
    ops.hash_impl = schema->is_hashable() ? &hash : nullptr;
    ops.equals_impl = &equals;
    // Python objects do not gain ordering merely because their declared
    // fields are ordered.
    ops.compare_impl = nullptr;
    ops.to_string_impl = &to_string;
    ops.to_python_impl = &to_python;
    ops.from_python_impl = &from_python;
    ops.accepts_source_impl = &accepts_source;
    ops.copy_assign_from_impl = &copy_assign_from;
    ops.move_assign_from_impl = &move_assign_from;
    ops.concrete_type_impl = &concrete_type;
    ops.concrete_memory_impl = &concrete_memory;
    ops.can_materialize_source_impl = &can_materialize_source;
    ops.size = &size;
    ops.element_at = &element_at;
    ops.element_binding = &element_binding;
    ops.make_range = &make_range;

    binding = intern_value_type(
        *schema, MemoryUtils::plan_for<PythonBundleValue>(), ops);
  }

  [[nodiscard]] static const PythonBundleBindingEntry &
  entry(const void *context) noexcept {
    return *static_cast<const PythonBundleBindingEntry *>(context);
  }

  [[nodiscard]] static PythonBundleValue &value(void *memory) {
    if (memory == nullptr) {
      throw std::logic_error(
          "Python-owned Bundle operation requires live value memory");
    }
    return *static_cast<PythonBundleValue *>(memory);
  }

  [[nodiscard]] static const PythonBundleValue &value(const void *memory) {
    if (memory == nullptr) {
      throw std::logic_error(
          "Python-owned Bundle operation requires live value memory");
    }
    return *static_cast<const PythonBundleValue *>(memory);
  }

  [[nodiscard]] const PyBundleClassInfo &class_info() const {
    const auto found = bundle_class_info_registry().find(schema);
    if (found == bundle_class_info_registry().end() ||
        !found->second.type.is_valid()) {
      throw std::logic_error(
          "Python-owned Bundle class registration is incomplete");
    }
    return found->second;
  }

  [[nodiscard]] static bool
  structurally_compatible(const ValueTypeMetaData *target,
                          const ValueTypeMetaData *source) noexcept {
    if (target == nullptr || source == nullptr ||
        source->try_value_kind() != ValueTypeKind::Bundle) {
      return false;
    }
    if (target == source || source == target->wrapped_un_named) {
      return true;
    }
    if (target->is_named_bundle() && source->is_named_bundle() &&
        TypeRegistry::instance().bundle_is_a(source, target)) {
      return true;
    }
    if (target->field_count != source->field_count) {
      return false;
    }
    for (std::size_t index = 0; index < target->field_count; ++index) {
      const auto &lhs = target->fields[index];
      const auto &rhs = source->fields[index];
      if (lhs.type != rhs.type) {
        return false;
      }
      const std::string_view lhs_name =
          lhs.name != nullptr ? std::string_view{lhs.name} : std::string_view{};
      const std::string_view rhs_name =
          rhs.name != nullptr ? std::string_view{rhs.name} : std::string_view{};
      if (lhs_name != rhs_name) {
        return false;
      }
    }
    return true;
  }

  static std::size_t hash(const void *, const void *memory) {
    const auto &stored = value(memory);
    if (stored.object == nullptr) {
      throw std::logic_error("cannot hash an empty Python-owned Bundle");
    }
    nb::gil_scoped_acquire gil;
    const Py_hash_t result = PyObject_Hash(stored.object);
    if (result == -1) {
      throw nb::python_error();
    }
    return static_cast<std::size_t>(result);
  }

  static bool equals(const void *, const void *lhs, const void *rhs) {
    const auto &left = *static_cast<const PythonBundleValue *>(lhs);
    const auto &right = *static_cast<const PythonBundleValue *>(rhs);
    if (left.object == right.object) {
      return true;
    }
    if (left.object == nullptr || right.object == nullptr) {
      return false;
    }
    nb::gil_scoped_acquire gil;
    const int result =
        PyObject_RichCompareBool(left.object, right.object, Py_EQ);
    if (result < 0) {
      throw nb::python_error();
    }
    return result == 1;
  }

  static std::string to_string(const void *, const void *memory) {
    const auto &stored = value(memory);
    if (stored.object == nullptr) {
      return "<empty Python-owned Bundle>";
    }
    nb::gil_scoped_acquire gil;
    nb::object text = nb::steal(PyObject_Str(stored.object));
    if (!text.is_valid()) {
      throw nb::python_error();
    }
    return nb::cast<std::string>(text);
  }

  static nb::object to_python(const void *, const void *memory) {
    const auto &stored = value(memory);
    return stored.object != nullptr ? nb::borrow<nb::object>(stored.object)
                                    : nb::none();
  }

  static void from_python(const void *context, const ValueTypeRef &binding,
                          void *memory, nb::handle source) {
    const auto &self = entry(context);
    const auto &info = self.class_info();
    if (!source.is_valid() || source.is_none() ||
        !nb::isinstance(source, info.type)) {
      throw nb::type_error(
          ("value is not an instance of Python-backed Bundle '" +
           std::string{self.schema->name()} + "'")
              .c_str());
    }
    value(memory).set(source, self.schema, binding);
  }

  static bool accepts_source(const void *context, ValueTypeRef binding,
                             ValueTypeRef source) noexcept {
    const auto &self = entry(context);
    return binding == self.binding && source &&
           structurally_compatible(self.schema, source.schema());
  }

  [[nodiscard]] static nb::object
  construct_from_indexed(const PythonBundleBindingEntry &self,
                         ValueTypeRef source, const void *source_memory) {
    const auto concrete = source.ops_ref().concrete_type(source, source_memory);
    const auto *concrete_memory =
        source.ops_ref().concrete_memory(source_memory);
    if (!concrete || concrete_memory == nullptr) {
      throw std::invalid_argument(
          "Python-owned Bundle source has no concrete value");
    }

    if (is_python_bundle_schema(concrete.schema()) &&
        concrete.plan() == &MemoryUtils::plan_for<PythonBundleValue>()) {
      nb::object object = concrete.ops_ref().to_python(concrete_memory);
      if (nb::isinstance(object, self.class_info().type)) {
        return object;
      }
    }

    const auto *indexed = checked_value_ops<IndexedValueOps>(
        concrete, "Python-owned Bundle construction source");
    if (indexed->size == nullptr || indexed->element_at == nullptr ||
        indexed->element_binding == nullptr ||
        indexed->size(indexed->context, concrete_memory) !=
            self.schema->field_count) {
      throw std::invalid_argument(
          "Python-owned Bundle source has an incompatible field shape");
    }

    const auto &info = self.class_info();
    nb::dict arguments;
    for (std::size_t index = 0; index < self.schema->field_count; ++index) {
      if (index >= info.constructor_fields.size() ||
          !info.constructor_fields[index]) {
        continue;
      }
      const void *field_memory =
          indexed->element_at(indexed->context, concrete_memory, index);
      if (field_memory == nullptr &&
          index < info.defaulted_constructor_fields.size() &&
          info.defaulted_constructor_fields[index]) {
        continue;
      }
      if (field_memory == nullptr) {
        throw std::invalid_argument(
            "Python-backed Bundle '" + std::string{self.schema->name()} +
            "' cannot be constructed because required field '" +
            std::string{self.schema->fields[index].name} + "' is unset");
      }
      nb::object field_value =
          indexed->element_binding(indexed->context, concrete_memory, index)
              .ops_ref()
              .to_python(field_memory);
      arguments[info.field_names[index]] = std::move(field_value);
    }

    nb::tuple positional = nb::steal<nb::tuple>(PyTuple_New(0));
    nb::object result = nb::steal(
        PyObject_Call(info.type.ptr(), positional.ptr(), arguments.ptr()));
    if (!result.is_valid()) {
      throw nb::python_error();
    }
    return result;
  }

  static void copy_assign_from(const void *context, ValueTypeRef, void *dst,
                               ValueTypeRef source, const void *src) {
    const auto &self = entry(context);
    nb::gil_scoped_acquire gil;
    const auto concrete = source.ops_ref().concrete_type(source, src);
    const auto *concrete_memory = source.ops_ref().concrete_memory(src);
    if (concrete && concrete_memory != nullptr &&
        is_python_bundle_schema(concrete.schema()) &&
        concrete.plan() == &MemoryUtils::plan_for<PythonBundleValue>()) {
      const auto &stored =
          *static_cast<const PythonBundleValue *>(concrete_memory);
      if (stored.object != nullptr &&
          nb::isinstance(nb::handle{stored.object}, self.class_info().type)) {
        value(dst) = stored;
        return;
      }
    }
    nb::object object = construct_from_indexed(self, source, src);
    value(dst).set(object, self.schema, self.binding);
  }

  static void move_assign_from(const void *context, ValueTypeRef binding,
                               void *dst, ValueTypeRef source, void *src) {
    copy_assign_from(context, binding, dst, source, src);
  }

  static ValueTypeRef concrete_type(const void *context, ValueTypeRef,
                                    const void *memory) noexcept {
    if (memory != nullptr) {
      const auto &stored = *static_cast<const PythonBundleValue *>(memory);
      if (stored.active_binding) {
        return stored.active_binding;
      }
    }
    return entry(context).binding;
  }

  static bool can_materialize_source(const void *context, ValueTypeRef source,
                                     const void *source_memory) {
    const auto &self = entry(context);
    const auto concrete = source.ops_ref().concrete_type(source, source_memory);
    const auto *concrete_memory =
        source.ops_ref().concrete_memory(source_memory);
    if (!concrete || concrete_memory == nullptr) {
      return false;
    }
    const auto *indexed = checked_value_ops<IndexedValueOps>(
        concrete, "Python-owned Bundle materialisation source");
    if (indexed->size(indexed->context, concrete_memory) !=
        self.schema->field_count) {
      return false;
    }

    const auto &info = self.class_info();
    for (std::size_t index = 0; index < self.schema->field_count; ++index) {
      if (index >= info.constructor_fields.size() ||
          !info.constructor_fields[index]) {
        continue;
      }
      const bool has_default =
          index < info.defaulted_constructor_fields.size() &&
          info.defaulted_constructor_fields[index];
      if (!has_default && indexed->element_at(indexed->context, concrete_memory,
                                              index) == nullptr) {
        return false;
      }
    }
    return true;
  }

  static const void *concrete_memory(const void *,
                                     const void *memory) noexcept {
    return memory;
  }

  static std::size_t size(const void *context, const void *) noexcept {
    return entry(context).field_bindings.size();
  }

  static ValueTypeRef element_binding(const void *context, const void *,
                                      std::size_t index) noexcept {
    const auto &self = entry(context);
    return index < self.field_bindings.size() ? self.field_bindings[index]
                                              : ValueTypeRef{};
  }

  static const void *element_at(const void *context, const void *memory,
                                std::size_t index) {
    const auto &self = entry(context);
    const auto &stored = value(memory);
    if (index >= self.field_bindings.size()) {
      throw std::out_of_range(
          "Python-owned Bundle field index is out of range");
    }
    if (stored.object == nullptr) {
      return nullptr;
    }

    nb::gil_scoped_acquire gil;
    const auto &info = self.class_info();
    PyObject *raw =
        PyObject_GetAttr(stored.object, info.field_names[index].ptr());
    if (raw == nullptr) {
      if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
        PyErr_Clear();
        return nullptr;
      }
      try {
        throw nb::python_error();
      } catch (...) {
        const char *field_name = self.schema->fields[index].name;
        std::throw_with_nested(std::runtime_error(
            "Python-backed Bundle '" + std::string{self.schema->name()} +
            "' failed to read field '" +
            std::string{field_name != nullptr ? field_name : "<unnamed>"} +
            "'"));
      }
    }
    nb::object attribute = nb::steal(raw);
    if (attribute.is_none()) {
      return nullptr;
    }

    auto &fields = stored.projected_fields;
    if (fields.empty()) {
      fields.resize(self.field_bindings.size());
    }
    if (!fields[index].binding()) {
      fields[index] = Value{self.field_bindings[index]};
    }
    try {
      fields[index].view().assign_from_python(attribute);
    } catch (...) {
      const auto *declared = self.schema->fields[index].type;
      nb::object actual_type = nb::steal(PyObject_Type(attribute.ptr()));
      std::string actual =
          actual_type.is_valid()
              ? nb::cast<std::string>(
                    nb::getattr(actual_type, "__name__", nb::str{"<unknown>"}))
              : std::string{"<unknown>"};
      const char *field_name = self.schema->fields[index].name;
      std::throw_with_nested(std::invalid_argument(
          "Python-backed Bundle '" + std::string{self.schema->name()} +
          "' field '" +
          std::string{field_name != nullptr ? field_name : "<unnamed>"} +
          "' declared as '" +
          std::string{declared != nullptr ? declared->name() : "<unknown>"} +
          "' cannot convert value of type '" + actual + "'"));
    }
    return fields[index].view().data();
  }

  static ValueView range_project(const void *context, const void *memory,
                                 std::size_t index) {
    return ValueView{element_binding(context, memory, index),
                     element_at(context, memory, index)};
  }

  static Range<ValueView> make_range(const void *context, const void *memory) {
    return Range<ValueView>{
        .context = context,
        .memory = memory,
        .limit = size(context, memory),
        .predicate = nullptr,
        .projector = &range_project,
    };
  }
};

struct PythonBundleBindings {
  std::recursive_mutex mutex{};
  std::unordered_map<const ValueTypeMetaData *,
                     std::vector<PythonBundleBindingEntry *>>
      current{};
  std::vector<std::unique_ptr<PythonBundleBindingEntry>> immortal{};
};

PythonBundleBindings &python_bundle_bindings() {
  static auto *bindings = new PythonBundleBindings{};
  return *bindings;
}

[[nodiscard]] bool
same_field_bindings(const PythonBundleBindingEntry &entry,
                    std::span<const ValueTypeRef> requested) noexcept {
  return entry.field_bindings.size() == requested.size() &&
         std::ranges::equal(entry.field_bindings, requested);
}
} // namespace

nb::object &cmp_result_enum_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &divide_by_zero_enum_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &record_as_of_enum_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &record_removes_enum_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &to_table_mode_enum_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &removed_sentinel_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &remove_if_exists_sentinel_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &removed_class_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &set_delta_class_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &delta_shaper_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

PyInferValueFn &py_infer_value_slot() {
  static PyInferValueFn slot = nullptr;
  return slot;
}

nb::dict &bundle_class_registry() {
  static auto *registry = new nb::dict{};
  return *registry;
}

std::unordered_map<const void *, PyBundleClassInfo> &
bundle_class_info_registry() {
  static auto *registry =
      new std::unordered_map<const void *, PyBundleClassInfo>{};
  return *registry;
}

std::unordered_map<const void *, const void *> &tsb_compound_value_registry() {
  static auto *registry = new std::unordered_map<const void *, const void *>{};
  return *registry;
}

void register_python_bundle_binding(const ValueTypeMetaData *schema) {
  if (schema == nullptr || !schema->is_named_bundle()) {
    throw std::invalid_argument(
        "register_python_bundle_binding requires a named Bundle schema");
  }
  const auto info = bundle_class_info_registry().find(schema);
  if (info == bundle_class_info_registry().end() ||
      !info->second.type.is_valid()) {
    throw std::invalid_argument("register_python_bundle_binding requires "
                                "registered Python class metadata");
  }

  PyObject *hash_method =
      PyObject_GetAttrString(info->second.type.ptr(), "__hash__");
  if (hash_method == nullptr) {
    nb::raise_python_error();
  }
  const bool hashable = hash_method != Py_None;
  Py_DECREF(hash_method);
  auto *mutable_schema = const_cast<ValueTypeMetaData *>(schema);
  mutable_schema->flags =
      (mutable_schema->flags | ValueTypeFlags::Equatable) &
      ~(ValueTypeFlags::TriviallyConstructible |
        ValueTypeFlags::TriviallyDestructible |
        ValueTypeFlags::TriviallyCopyable | ValueTypeFlags::BufferCompatible |
        ValueTypeFlags::Comparable | ValueTypeFlags::Hashable);
  if (hashable) {
    mutable_schema->flags |= ValueTypeFlags::Hashable;
  }

  auto &bindings = python_bundle_bindings();
  {
    std::lock_guard lock(bindings.mutex);
    bindings.current.try_emplace(schema);
  }
  std::vector<ValueTypeRef> fields;
  fields.reserve(schema->field_count);
  for (std::size_t index = 0; index < schema->field_count; ++index) {
    fields.push_back(
        ValuePlanFactory::instance().type_for(schema->fields[index].type));
  }
  const auto binding = python_bundle_binding_for(schema, fields);
  if (!binding) {
    throw std::logic_error("failed to create Python-owned Bundle binding");
  }
  ValuePlanFactory::instance().register_type(binding);
}

bool is_python_bundle_schema(const ValueTypeMetaData *schema) noexcept {
  if (schema == nullptr) {
    return false;
  }
  auto &bindings = python_bundle_bindings();
  std::lock_guard lock(bindings.mutex);
  return bindings.current.contains(schema);
}

bool is_python_bundle_binding(ValueTypeRef binding) noexcept {
  if (!binding) {
    return false;
  }
  auto &bindings = python_bundle_bindings();
  std::lock_guard lock(bindings.mutex);
  const auto current = bindings.current.find(binding.schema());
  if (current == bindings.current.end()) {
    return false;
  }
  return std::ranges::any_of(
      current->second,
      [binding](const PythonBundleBindingEntry *entry) {
        return entry != nullptr && entry->binding == binding;
      });
}

ValueTypeRef
python_bundle_binding_for(const ValueTypeMetaData *schema,
                          std::span<const ValueTypeRef> field_bindings) {
  if (schema == nullptr) {
    return {};
  }
  auto &bindings = python_bundle_bindings();
  std::lock_guard lock(bindings.mutex);
  const auto current = bindings.current.find(schema);
  if (current == bindings.current.end()) {
    // Only schemas explicitly installed through
    // register_python_bundle_binding own Python objects.
    return {};
  }
  for (const auto *entry : current->second) {
    if (same_field_bindings(*entry, field_bindings)) {
      return entry->binding;
    }
  }
  auto created = std::make_unique<PythonBundleBindingEntry>(
      schema,
      std::vector<ValueTypeRef>{field_bindings.begin(), field_bindings.end()});
  const auto result = created->binding;
  current->second.push_back(created.get());
  bindings.immortal.push_back(std::move(created));
  return result;
}

void clear_python_bundle_bindings() noexcept {
  auto &bindings = python_bundle_bindings();
  std::lock_guard lock(bindings.mutex);
  bindings.current.clear();
}

void register_native_scalar_type(nb::handle python_type,
                                 const ValueTypeMetaData *native_value_type) {
  if (PyType_Check(python_type.ptr()) == 0) {
    throw nb::type_error("python_type must be a Python class");
  }
  if (native_value_type == nullptr ||
      native_value_type->value_kind() != ValueTypeKind::Atomic) {
    throw nb::type_error("native_value_type must be an atomic scalar schema");
  }

  auto &registrations = native_scalar_registrations();
  const auto python_entry =
      registrations.by_python_type.find(python_type.ptr());
  if (python_entry != registrations.by_python_type.end() &&
      python_entry->second.second != native_value_type) {
    throw std::invalid_argument("Python class is already registered to a "
                                "different native scalar schema");
  }
  const auto native_entry =
      registrations.by_native_type.find(native_value_type);
  if (native_entry != registrations.by_native_type.end() &&
      !native_entry->second.is(python_type)) {
    throw std::invalid_argument("native scalar schema is already registered to "
                                "a different Python class");
  }
  if (python_entry != registrations.by_python_type.end()) {
    return;
  }

  nb::object retained = nb::borrow<nb::object>(python_type);
  registrations.by_python_type.emplace(python_type.ptr(),
                                       std::pair{retained, native_value_type});
  registrations.by_native_type.emplace(native_value_type, std::move(retained));
}

const ValueTypeMetaData *native_scalar_type_for_python(nb::handle python_type) {
  const auto &registrations = native_scalar_registrations();
  const auto found = registrations.by_python_type.find(python_type.ptr());
  return found == registrations.by_python_type.end() ? nullptr
                                                     : found->second.second;
}

nb::object
python_type_for_native_scalar(const ValueTypeMetaData *native_value_type) {
  const auto &registrations = native_scalar_registrations();
  const auto found = registrations.by_native_type.find(native_value_type);
  return found == registrations.by_native_type.end()
             ? nb::none()
             : nb::borrow<nb::object>(found->second);
}

const ValueTypeMetaData *native_scalar_type_for_value(nb::handle value) {
  if (!value.is_valid()) {
    return nullptr;
  }
  if (const auto *exact =
          native_scalar_type_for_python(nb::handle(Py_TYPE(value.ptr())))) {
    return exact;
  }
  const auto &registrations = native_scalar_registrations();
  for (const auto &[python_type, entry] : registrations.by_python_type) {
    static_cast<void>(python_type);
    if (nb::isinstance(value, entry.first)) {
      return entry.second;
    }
  }
  return nullptr;
}

void clear_native_scalar_types() noexcept {
  auto &registrations = native_scalar_registrations();
  registrations.by_native_type.clear();
  registrations.by_python_type.clear();
}
} // namespace hgraph::python_bridge

namespace hgraph {
#define HGRAPH_DEFINE_PYTHON_CONVERSION_TRAIT(Type, Label)                     \
  python_conversion_traits<Type>::ToPythonHook &                               \
  python_conversion_traits<Type>::to_python_hook() noexcept {                  \
    static ToPythonHook hook = nullptr;                                        \
    return hook;                                                               \
  }                                                                            \
  python_conversion_traits<Type>::FromPythonHook &                             \
  python_conversion_traits<Type>::from_python_hook() noexcept {                \
    static FromPythonHook hook = nullptr;                                      \
    return hook;                                                               \
  }                                                                            \
  nanobind::object python_conversion_traits<Type>::to_python(                  \
      const Type &value) {                                                     \
    const auto hook = to_python_hook();                                        \
    if (hook == nullptr) {                                                     \
      throw std::logic_error(                                                  \
          Label " python conversion hook not installed (import the module)");  \
    }                                                                          \
    return hook(value);                                                        \
  }                                                                            \
  Type python_conversion_traits<Type>::from_python(nanobind::handle source) {  \
    const auto hook = from_python_hook();                                      \
    if (hook == nullptr) {                                                     \
      throw std::logic_error(                                                  \
          Label " python conversion hook not installed (import the module)");  \
    }                                                                          \
    return hook(source);                                                       \
  }

HGRAPH_DEFINE_PYTHON_CONVERSION_TRAIT(Frame, "Frame")
HGRAPH_DEFINE_PYTHON_CONVERSION_TRAIT(Series, "Series")
HGRAPH_DEFINE_PYTHON_CONVERSION_TRAIT(TimeSeriesReference,
                                      "TimeSeriesReference")
HGRAPH_DEFINE_PYTHON_CONVERSION_TRAIT(ValueCallable, "ValueCallable")
HGRAPH_DEFINE_PYTHON_CONVERSION_TRAIT(WiredFn, "WiredFn")

#undef HGRAPH_DEFINE_PYTHON_CONVERSION_TRAIT
} // namespace hgraph

#endif
