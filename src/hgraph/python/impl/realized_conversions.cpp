#include "python_ops_families.h"

#include "../../types/metadata/detail/realized_value_seams.h"

#include <hgraph/python/bridge_state.h>
#include <hgraph/python/conversion.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/value/value.h>

#include <fmt/format.h>
#include <nanobind/nanobind.h>

#include <algorithm>
#include <cstddef>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

/**
 * Value <-> Python for the realized structural values (RFC 0035, PR 3):
 * composite Tuple / Bundle, fixed and bounded arrays, owned and shared
 * entries, the closed Bundle and its pooled form. The bodies read the
 * families' private contexts through ``realized_value_seams.h``; allocation
 * replacement and active-type switching stay in the type layer behind the
 * ``*_assign`` seams, which hand this unit the payload to convert into.
 */
namespace hgraph::python_bridge
{
    namespace
    {
        using realized_detail::ArrayIndexedContext;
        using realized_detail::CompositeIndexedContext;
        using realized_detail::PolymorphicAlternatives;

        // -- shared helpers ------------------------------------------------------
        void require_python_source(nb::handle source, const char *what)
        {
            if (source.is_none()) { throw std::invalid_argument(std::string{what} + " requires a non-None value"); }
        }

        [[nodiscard]] bool is_python_sequence(nb::handle source) { return PySequence_Check(source.ptr()) != 0; }

        void assign_child_from_python(const ValueTypeRef &binding, void *memory, nb::handle source, const char *what)
        {
            if (memory == nullptr) { throw std::runtime_error(std::string{what} + " child memory is not live"); }
            if (source.is_none()) { throw std::invalid_argument(std::string{what} + " does not allow None elements"); }
            from_python(binding.ops_ref(), binding, memory, source);
        }

        [[nodiscard]] const PyBundleClassInfo *python_bundle_info(const ValueTypeMetaData *schema)
        {
            const auto &registry = bundle_class_info_registry();
            const auto  found    = registry.find(schema);
            return found != registry.end() ? &found->second : nullptr;
        }

        /** The fill callback of the ``*_assign`` seams: convert the handle into the payload. */
        void fill_payload_from_handle(void *fill_context, ValueTypeRef target, void *payload)
        {
            const nb::handle source = *static_cast<const nb::handle *>(fill_context);
            from_python(target.ops_ref(), target, payload, source);
        }

        // -- composite (Tuple / Bundle) -------------------------------------------
        [[nodiscard]] nb::object composite_value_to_python(const void *context, const void *memory)
        {
            if (memory == nullptr) { throw std::runtime_error("composite to_python requires live value memory"); }
            const auto *state  = static_cast<const CompositeIndexedContext *>(context);
            const bool  bundle = state->schema != nullptr && state->schema->value_kind() == ValueTypeKind::Bundle;

            if (bundle)
            {
                // A NAMED bundle with a registered python class rebuilds the
                // class (CompoundScalar read-back; UNSET fields -> None).
                const auto *bundle_info = !state->schema->name().empty() ? python_bundle_info(state->schema) : nullptr;
                if (bundle_info == nullptr)
                {
                    nb::dict result;
                    for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
                    {
                        const char *name = state->schema->fields[index].name;
                        if (name == nullptr || *name == '\0') { continue; }
                        if (!realized_detail::composite_field_is_set(state, memory, index)) { continue; }
                        const auto &ops   = state->child_bindings[index].ops_ref();
                        const auto *child = static_cast<const std::byte *>(memory) + state->offsets[index];
                        result[nb::str{name}] = to_python(ops, child);
                    }
                    return result;
                }

                nb::dict   constructor_arguments;
                nb::object result;
                if (!bundle_info->requires_constructor)
                {
                    // Ordinary dataclasses can be rebuilt without re-running
                    // __init__/__post_init__ on every TS.value read.
                    result = nb::steal(
                        bundle_info->allocator(reinterpret_cast<PyTypeObject *>(bundle_info->type.ptr()), 0));
                    if (!result.is_valid()) { nb::raise_python_error(); }
                }
                for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
                {
                    const char *name = state->schema->fields[index].name;
                    if (name == nullptr || *name == '\0') { continue; }
                    nb::handle key   = bundle_info->field_names[index];
                    const bool set   = realized_detail::composite_field_is_set(state, memory, index);
                    nb::object value = set ? to_python(state->child_bindings[index],
                                                       static_cast<const std::byte *>(memory) + state->offsets[index])
                                           : nb::none();
                    if (bundle_info->requires_constructor)
                    {
                        if (bundle_info->constructor_fields[index]) { constructor_arguments[key] = value; }
                    }
                    else if (bundle_info->field_overrides[index].is_valid())
                    {
                        bundle_info->field_overrides[index](result, value);
                    }
                    else if (PyObject_GenericSetAttr(result.ptr(), key.ptr(), value.ptr()) != 0)
                    {
                        nb::raise_python_error();
                    }
                }
                if (bundle_info->requires_constructor)
                {
                    nb::tuple positional = nb::steal<nb::tuple>(PyTuple_New(0));
                    result = nb::steal(PyObject_Call(bundle_info->type.ptr(), positional.ptr(), constructor_arguments.ptr()));
                    if (!result.is_valid()) { nb::raise_python_error(); }
                }
                return result;
            }

            nb::list result;
            for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
            {
                // UNSET tuple fields read back as None (field validity - the
                // relaxed combine/partial convert convention).
                if (!realized_detail::composite_field_is_set(state, memory, index))
                {
                    result.append(nb::none());
                    continue;
                }
                const auto &ops   = state->child_bindings[index].ops_ref();
                const auto *child = static_cast<const std::byte *>(memory) + state->offsets[index];
                result.append(to_python(ops, child));
            }
            return nb::tuple(result);
        }

        void fill_composite_from_sequence(const CompositeIndexedContext *state, void *memory, nb::handle source,
                                          const char *what)
        {
            if (!is_python_sequence(source))
            {
                throw std::invalid_argument(std::string{what} + " expects a Python list or tuple");
            }

            nb::object   object   = nb::borrow<nb::object>(source);
            nb::sequence sequence = nb::cast<nb::sequence>(object);
            const auto   count    = static_cast<std::size_t>(nb::len(sequence));
            // hgraph parity: a LONGER python sequence fills a fixed tuple's
            // leading fields (python tuples don't length-validate upstream).
            if (count > state->child_bindings.size()) { /* truncate below */ }
            else if (count != state->child_bindings.size())
            {
                throw std::invalid_argument(
                    fmt::format("{} expects {} elements, got {}", what, state->child_bindings.size(), count));
            }

            realized_detail::composite_set_all_validity(state, memory, true);  // sequences supply every field
            const std::size_t fill_count = std::min(count, state->child_bindings.size());
            for (std::size_t index = 0; index < fill_count; ++index)
            {
                nb::object element = sequence[index];
                // None = UNSET (field validity) - the TABLE row convention:
                // to_python reads holes back as None, so None round-trips.
                if (element.is_none())
                {
                    realized_detail::composite_set_field_validity(state, memory, index, false);
                    continue;
                }
                auto *child = static_cast<std::byte *>(memory) + state->offsets[index];
                assign_child_from_python(state->child_bindings[index], child, element, what);
            }
        }

        void composite_value_from_python(const void *context, const ValueTypeRef &, void *memory, nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("composite from_python requires live value memory"); }
            require_python_source(source, "Composite value");

            const auto *state  = static_cast<const CompositeIndexedContext *>(context);
            const bool  bundle = state->schema != nullptr && state->schema->value_kind() == ValueTypeKind::Bundle;
            if (!bundle)
            {
                fill_composite_from_sequence(state, memory, source, "Tuple value");
                return;
            }

            nb::object  object      = nb::borrow<nb::object>(source);
            const auto *bundle_info = !state->schema->name().empty() ? python_bundle_info(state->schema) : nullptr;
            const bool  exact_bundle_class =
                bundle_info != nullptr &&
                Py_TYPE(object.ptr()) == reinterpret_cast<PyTypeObject *>(bundle_info->type.ptr());
            if (!exact_bundle_class && nb::isinstance<nb::dict>(object))
            {
                nb::dict map = nb::cast<nb::dict>(object);
                realized_detail::composite_set_all_validity(state, memory, false);
                for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
                {
                    const char *name = state->schema->fields[index].name;
                    if (name == nullptr || *name == '\0')
                    {
                        throw std::invalid_argument("Bundle value has an unnamed field and cannot be loaded from dict");
                    }
                    nb::object fallback_key;
                    nb::handle key;
                    if (bundle_info != nullptr) { key = bundle_info->field_names[index]; }
                    else
                    {
                        fallback_key = nb::str{name};
                        key          = fallback_key;
                    }
                    // PARTIAL dicts mark exactly the provided keys (field
                    // validity, core_concepts.rst) - absent = UNSET.
                    if (!map.contains(key)) { continue; }
                    nb::object value = map[key];
                    // None = UNSET (field validity - the same convention as
                    // the attribute form; eval_node bundles tick partially).
                    if (value.is_none()) { continue; }
                    auto *child = static_cast<std::byte *>(memory) + state->offsets[index];
                    assign_child_from_python(state->child_bindings[index], child, value, "Bundle value");
                    realized_detail::composite_set_field_validity(state, memory, index, true);
                }
                return;
            }

            if (!exact_bundle_class && is_python_sequence(source))
            {
                fill_composite_from_sequence(state, memory, source, "Bundle value");
                return;
            }

            // ATTRIBUTE form (dataclass / CompoundScalar instances): None
            // fields are UNSET (field validity - the CS convention).
            realized_detail::composite_set_all_validity(state, memory, false);
            for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
            {
                const char *name = state->schema->fields[index].name;
                if (name == nullptr || *name == '\0')
                {
                    throw std::invalid_argument("Bundle value has an unnamed field and cannot be loaded from attributes");
                }
                nb::object fallback_key;
                nb::handle key;
                if (bundle_info != nullptr) { key = bundle_info->field_names[index]; }
                else
                {
                    fallback_key = nb::str{name};
                    key          = fallback_key;
                }
                // Exact registered data objects do not need a user-defined
                // __getattribute__ dispatch for their declared fields. Keep
                // the general protocol for accepted proxy/attribute objects.
                PyObject *raw_value = exact_bundle_class ? PyObject_GenericGetAttr(object.ptr(), key.ptr())
                                                         : PyObject_GetAttr(object.ptr(), key.ptr());
                if (raw_value == nullptr)
                {
                    if (PyErr_ExceptionMatches(PyExc_AttributeError))
                    {
                        PyErr_Clear();
                        continue;
                    }
                    nb::raise_python_error();
                }
                nb::object value = nb::steal(raw_value);
                if (value.is_none()) { continue; }
                auto *child = static_cast<std::byte *>(memory) + state->offsets[index];
                assign_child_from_python(state->child_bindings[index], child, value, "Bundle value");
                realized_detail::composite_set_field_validity(state, memory, index, true);
            }
        }

        // -- fixed / bounded arrays -------------------------------------------------
        [[nodiscard]] nb::object array_value_to_python(const void *context, const void *memory)
        {
            if (memory == nullptr) { throw std::runtime_error("array to_python requires live value memory"); }
            const auto *state = static_cast<const ArrayIndexedContext *>(context);
            const auto &ops   = state->element_binding.ops_ref();
            const auto  size  = realized_detail::array_size(context, memory);
            if (can_to_python_buffer(ops, state->element_binding))
            {
                struct ArrayBufferOwner
                {
                    const void                *memory{nullptr};
                    const ArrayIndexedContext *state{nullptr};
                };
                const ArrayBufferOwner owner{memory, state};
                const auto             element_at = [](const void *owner_memory, std::size_t index) -> const void * {
                    const auto *owner_state = static_cast<const ArrayBufferOwner *>(owner_memory);
                    return static_cast<const std::byte *>(owner_state->memory) + owner_state->state->data_offset +
                           index * owner_state->state->stride;
                };
                return to_python_buffer(ops, state->element_binding,
                                        ValueArraySource{
                                            .owner      = &owner,
                                            .size       = size,
                                            .element_at = element_at,
                                            .first =
                                                ValueArraySpan{
                                                    .data   = static_cast<const std::byte *>(memory) + state->data_offset,
                                                    .size   = size,
                                                    .stride = state->stride,
                                                },
                                        });
            }

            nb::list result;
            for (std::size_t index = 0; index < size; ++index)
            {
                const auto *child = static_cast<const std::byte *>(memory) + state->data_offset + index * state->stride;
                result.append(to_python(ops, child));
            }
            return result;
        }

        [[nodiscard]] nb::object array_value_to_numpy(const void *context, const void *memory)
        {
            nb::object value = array_value_to_python(context, memory);
            return nb::module_::import_("numpy").attr("asarray")(std::move(value));
        }

        void array_value_from_python(const void *context, const ValueTypeRef &, void *memory, nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("array from_python requires live value memory"); }
            require_python_source(source, "Fixed List value");
            if (!is_python_sequence(source)) { throw std::invalid_argument("Fixed List value expects a Python list or tuple"); }

            const auto  *state    = static_cast<const ArrayIndexedContext *>(context);
            nb::object   object   = nb::borrow<nb::object>(source);
            nb::sequence sequence = nb::cast<nb::sequence>(object);
            const auto   count    = static_cast<std::size_t>(nb::len(sequence));
            if ((!state->bounded && count != state->capacity) || (state->bounded && count > state->capacity))
            {
                if (state->bounded)
                {
                    throw std::invalid_argument(
                        fmt::format("Array value accepts at most {} elements, got {}", state->capacity, count));
                }
                throw std::invalid_argument(
                    fmt::format("Fixed List value expects {} elements, got {}", state->capacity, count));
            }
            if (state->bounded) { realized_detail::array_resize(context, memory, count); }

            for (std::size_t index = 0; index < count; ++index)
            {
                nb::object element = sequence[index];
                auto      *child   = static_cast<std::byte *>(memory) + state->data_offset + index * state->stride;
                assign_child_from_python(state->element_binding, child, element, "Fixed List value");
            }
        }

        // -- owned and shared entries ---------------------------------------------
        [[nodiscard]] nb::object owned_entry_to_python(const void *, const void *memory)
        {
            const auto type = realized_detail::owned_entry_active_type(memory);
            if (!type) { return nb::none(); }
            return to_python(type, realized_detail::owned_entry_payload(memory));
        }

        void owned_entry_from_python(const void *context, const ValueTypeRef &, void *memory, nb::handle source)
        {
            if (source.is_none())
            {
                realized_detail::owned_entry_reset(memory);
                return;
            }
            realized_detail::owned_entry_assign(context, memory, &fill_payload_from_handle, &source);
        }

        [[nodiscard]] nb::object shared_entry_to_python(const void *, const void *memory)
        {
            const auto type = realized_detail::shared_entry_active_type(memory);
            if (!type) { return nb::none(); }
            return to_python(type, realized_detail::shared_entry_payload(memory));
        }

        void shared_entry_from_python(const void *context, const ValueTypeRef &, void *memory, nb::handle source)
        {
            if (source.is_none())
            {
                realized_detail::shared_entry_reset(memory);
                return;
            }
            realized_detail::shared_entry_assign(context, memory, &fill_payload_from_handle, &source);
        }

        // -- closed Bundle: which alternative a Python source is -------------------
        [[nodiscard]] ValueTypeRef python_source_type(const PolymorphicAlternatives &view, nb::handle source)
        {
            nb::object                object       = nb::borrow<nb::object>(source);
            const nb::object          source_class = nb::getattr(object, "__class__");
            std::vector<ValueTypeRef> class_matches;
            std::size_t               best_distance = std::numeric_limits<std::size_t>::max();
            auto                     &registry      = bundle_class_info_registry();
            for (const auto alternative : view.alternatives)
            {
                const auto found = registry.find(alternative.schema());
                if (found == registry.end() || !found->second.type.is_valid() ||
                    !nb::isinstance(object, found->second.type))
                {
                    continue;
                }
                const auto  mro      = nb::cast<nb::tuple>(nb::getattr(source_class, "__mro__"));
                std::size_t distance = std::numeric_limits<std::size_t>::max();
                for (std::size_t index = 0; index < mro.size(); ++index)
                {
                    if (mro[index].is(found->second.type))
                    {
                        distance = index;
                        break;
                    }
                }
                if (distance < best_distance)
                {
                    best_distance = distance;
                    class_matches.clear();
                }
                if (distance == best_distance) { class_matches.push_back(alternative); }
            }
            if (class_matches.size() == 1) { return class_matches.front(); }
            if (!class_matches.empty() && nb::hasattr(object, "__orig_class__"))
            {
                const auto   alias = nb::getattr(object, "__orig_class__");
                ValueTypeRef matched{};
                for (const auto candidate : class_matches)
                {
                    const auto &info = registry.at(candidate.schema());
                    if (!info.specialization.is_valid()) { continue; }
                    const int equal = PyObject_RichCompareBool(alias.ptr(), info.specialization.ptr(), Py_EQ);
                    if (equal < 0) { nb::raise_python_error(); }
                    if (equal == 0) { continue; }
                    if (matched) { throw std::invalid_argument("Python structured scalar specialization is ambiguous"); }
                    matched = candidate;
                }
                if (matched) { return matched; }
            }
            if (class_matches.size() > 1)
            {
                using InferValueFn = Value (*)(nb::handle);
                const auto infer   = reinterpret_cast<InferValueFn>(py_infer_value_slot());
                if (infer == nullptr) { throw std::logic_error("Python Bundle inference hook is not installed"); }
                ValueTypeRef best{};
                std::size_t  best_score = 0;
                bool         ambiguous  = false;
                for (const auto candidate : class_matches)
                {
                    std::size_t score = 0;
                    for (std::size_t index = 0; index < candidate.schema()->field_count; ++index)
                    {
                        const auto &field = candidate.schema()->fields[index];
                        if (field.name == nullptr || !nb::hasattr(object, field.name)) { continue; }
                        const auto field_value = nb::getattr(object, field.name);
                        if (field_value.is_none() || field_value.ptr() == object.ptr()) { continue; }
                        const Value inferred = infer(field_value);
                        if (inferred.schema() == field.type) { score += 2; }
                        else if (inferred.schema() != nullptr && field.type != nullptr &&
                                 inferred.schema()->value_kind() == ValueTypeKind::Bundle &&
                                 field.type->value_kind() == ValueTypeKind::Bundle &&
                                 TypeRegistry::instance().value_is_a(inferred.schema(), field.type))
                        {
                            ++score;
                        }
                    }
                    if (!best || score > best_score)
                    {
                        best       = candidate;
                        best_score = score;
                        ambiguous  = false;
                    }
                    else if (score == best_score) { ambiguous = true; }
                }
                if (!ambiguous) { return best; }
                throw std::invalid_argument("Python structured scalar schema inference is ambiguous");
            }

            if (nb::isinstance<nb::dict>(object))
            {
                const auto discriminator = view.declared->bundle_discriminator();
                nb::dict   map           = nb::cast<nb::dict>(object);
                nb::str    key{std::string{discriminator}.c_str()};
                if (!map.contains(key))
                {
                    throw std::invalid_argument("polymorphic Bundle dictionaries require the configured type discriminator");
                }
                const auto   requested = nb::cast<std::string>(map[key]);
                ValueTypeRef match{};
                for (const auto alternative : view.alternatives)
                {
                    if (alternative.schema()->matches_bundle_discriminator(requested))
                    {
                        if (match) { throw std::invalid_argument("polymorphic Bundle discriminator is ambiguous"); }
                        match = alternative;
                    }
                }
                if (match) { return match; }
                throw std::invalid_argument("polymorphic Bundle discriminator names no valid alternative");
            }
            const nb::object  source_module   = nb::getattr(source_class, "__module__");
            const nb::object  source_qualname = nb::getattr(source_class, "__qualname__");
            const std::string source_name =
                nb::cast<std::string>(source_module) + "." + nb::cast<std::string>(source_qualname);
            throw std::invalid_argument("value of Python type '" + source_name + "' is not an instance of closed Bundle '" +
                                        std::string{view.declared->name()} + "'");
        }

        ValueTypeRef polymorphic_source_type(const void *context, PyRef source)
        {
            return python_source_type(*static_cast<const PolymorphicAlternatives *>(context), handle_of(source));
        }

        [[nodiscard]] nb::object closed_bundle_to_python(const void *context, const void *memory)
        {
            const auto active = realized_detail::union_entry_active_type(context, memory);
            if (!active) { throw std::logic_error("closed Bundle has an invalid active type"); }
            return to_python(active, realized_detail::union_entry_payload(context, memory));
        }

        void closed_bundle_from_python(const void *context, const ValueTypeRef &, void *memory, nb::handle source)
        {
            const auto requested = python_source_type(realized_detail::union_entry_alternatives(context), source);
            realized_detail::union_entry_assign(context, memory, requested, &fill_payload_from_handle, &source);
        }

        [[nodiscard]] nb::object pooled_to_python(const void *context, const void *memory)
        {
            const auto active = realized_detail::pooled_entry_active_type(context, memory);
            if (!active) { throw std::logic_error("pooled closed Bundle has an invalid active type"); }
            return to_python(active, realized_detail::pooled_entry_payload(context, memory));
        }

        void pooled_from_python(const void *context, const ValueTypeRef &, void *memory, nb::handle source)
        {
            const auto external_type = realized_detail::pooled_entry_resolve_source(context, borrow(source));
            realized_detail::pooled_entry_assign(context, memory, external_type, &fill_payload_from_handle, &source);
        }
    }  // namespace

    void fill_realized_conversions(PythonOps::Realized &section) noexcept
    {
        section.composite_to_python       = &to_python_slot<&composite_value_to_python>;
        section.composite_from_python     = &from_python_slot<&composite_value_from_python>;
        section.array_to_python           = &to_python_slot<&array_value_to_python>;
        section.array_to_numpy            = &to_python_slot<&array_value_to_numpy>;
        section.array_from_python         = &from_python_slot<&array_value_from_python>;
        section.owned_to_python           = &to_python_slot<&owned_entry_to_python>;
        section.owned_from_python         = &from_python_slot<&owned_entry_from_python>;
        section.shared_to_python          = &to_python_slot<&shared_entry_to_python>;
        section.shared_from_python        = &from_python_slot<&shared_entry_from_python>;
        section.closed_bundle_to_python   = &to_python_slot<&closed_bundle_to_python>;
        section.closed_bundle_from_python = &from_python_slot<&closed_bundle_from_python>;
        section.pooled_to_python          = &to_python_slot<&pooled_to_python>;
        section.pooled_from_python        = &from_python_slot<&pooled_from_python>;
        section.polymorphic_source_type   = &polymorphic_source_type;
    }
}  // namespace hgraph::python_bridge
