#include "python_ops_families.h"

#include <hgraph/python/conversion.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/mutable_container_ops.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#include <nanobind/nanobind.h>

#include <stdexcept>
#include <string>

/**
 * Value <-> Python for the compact and mutable containers (RFC 0035, PR 2).
 * These bodies read the containers' PUBLIC storage API (``compact_storage.h``,
 * ``mutable_container_ops.h``) and rebuild through the value builders; the
 * ops tables in the type layer hold forwarders into the ``Compact`` and
 * ``Mutable`` sections this unit fills.
 */
namespace hgraph::python_bridge
{
    namespace
    {
        // -- compact sequences: buffer fast path -------------------------------
        const void *list_value_array_element_at(const void *owner, std::size_t index)
        {
            return static_cast<const ListStorage *>(owner)->element_at(index);
        }

        const void *cyclic_buffer_value_array_element_at(const void *owner, std::size_t index)
        {
            return static_cast<const CyclicBufferStorage *>(owner)->element_at(index);
        }

        const void *queue_value_array_element_at(const void *owner, std::size_t index)
        {
            return static_cast<const QueueStorage *>(owner)->element_at(index);
        }

        nb::object sequence_to_python_buffer(const ValueTypeRef &element_binding, ValueArraySource source)
        {
            const auto &ops = element_binding.ops_ref();
            return can_to_python_buffer(ops, element_binding) ? to_python_buffer(ops, element_binding, source)
                                                              : nb::object{};
        }

        [[nodiscard]] ValueArraySpan compact_sequence_span(const ValueTypeRef &element_binding, const void *data,
                                                           std::size_t size)
        {
            return ValueArraySpan{
                .data   = size == 0 ? nullptr : data,
                .size   = size,
                .stride = element_binding.checked_plan().layout.size,
            };
        }

        // -- compact to_python ---------------------------------------------------
        nb::object list_to_python(const void *, const void *memory)
        {
            const auto *storage = static_cast<const ListStorage *>(memory);
            if (storage == nullptr || storage->element_binding() == nullptr)
            {
                throw std::runtime_error("List to_python requires live storage with an element binding");
            }
            // Any hole disables the trivially-copyable buffer fast-path.
            bool dense = true;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                if (!storage->element_set(i))
                {
                    dense = false;
                    break;
                }
            }
            if (dense)
            {
                if (nb::object buffer = sequence_to_python_buffer(
                        storage->element_binding(),
                        ValueArraySource{
                            .owner      = storage,
                            .size       = storage->size(),
                            .element_at = &list_value_array_element_at,
                            .first      = compact_sequence_span(storage->element_binding(),
                                                                storage->size() == 0 ? nullptr : storage->element_at(0),
                                                                storage->size()),
                        });
                    buffer.is_valid())
                {
                    return buffer;
                }
            }

            const auto  element_binding = storage->element_binding();
            const auto &ops             = element_binding.ops_ref();
            nb::list    result;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                // UNSET holes read back as None.
                result.append(storage->element_set(i) ? to_python(ops, storage->element_at(i)) : nb::none());
            }
            return result;
        }

        /** The VARIADIC-TUPLE variant: same storage, python reads back a
            TUPLE (ops-variant selection at binding time - the type-erasure
            rule: no runtime flag checks). */
        nb::object list_to_python_tuple(const void *context, const void *memory)
        {
            return nb::tuple(list_to_python(context, memory));
        }

        /** Shaped-array variant selected when the binding is interned. */
        nb::object list_to_python_array(const void *context, const void *memory)
        {
            nb::object value = list_to_python(context, memory);
            return nb::module_::import_("numpy").attr("asarray")(std::move(value));
        }

        nb::object cyclic_buffer_to_python(const void *, const void *memory)
        {
            const auto *storage = static_cast<const CyclicBufferStorage *>(memory);
            if (storage == nullptr || storage->element_binding() == nullptr)
            {
                throw std::runtime_error("CyclicBuffer to_python requires live storage with an element binding");
            }
            if (nb::object buffer = sequence_to_python_buffer(storage->element_binding(), [&]() {
                    const auto size       = storage->size();
                    const auto head       = storage->head();
                    const auto first_size = size == 0 ? 0 : size - head;
                    return ValueArraySource{
                        .owner      = storage,
                        .size       = size,
                        .element_at = &cyclic_buffer_value_array_element_at,
                        .first      = compact_sequence_span(storage->element_binding(),
                                                            first_size == 0 ? nullptr : storage->element_at(0), first_size),
                        .second     = compact_sequence_span(storage->element_binding(),
                                                            first_size == size ? nullptr : storage->element_at(first_size),
                                                            size - first_size),
                    };
                }());
                buffer.is_valid())
            {
                return buffer;
            }

            const auto  element_binding = storage->element_binding();
            const auto &ops             = element_binding.ops_ref();
            nb::list    result;
            for (std::size_t i = 0; i < storage->size(); ++i) { result.append(to_python(ops, storage->element_at(i))); }
            return result;
        }

        nb::object queue_to_python(const void *, const void *memory)
        {
            const auto *storage = static_cast<const QueueStorage *>(memory);
            if (storage == nullptr || storage->element_binding() == nullptr)
            {
                throw std::runtime_error("Queue to_python requires live storage with an element binding");
            }
            if (nb::object buffer = sequence_to_python_buffer(
                    storage->element_binding(),
                    ValueArraySource{
                        .owner      = storage,
                        .size       = storage->size(),
                        .element_at = &queue_value_array_element_at,
                        .first      = compact_sequence_span(storage->element_binding(),
                                                            storage->size() == 0 ? nullptr : storage->element_at(0),
                                                            storage->size()),
                    });
                buffer.is_valid())
            {
                return buffer;
            }

            const auto  element_binding = storage->element_binding();
            const auto &ops             = element_binding.ops_ref();
            nb::list    result;
            for (std::size_t i = 0; i < storage->size(); ++i) { result.append(to_python(ops, storage->element_at(i))); }
            return result;
        }

        nb::object set_to_python(const void *, const void *memory)
        {
            const auto *storage = static_cast<const SetStorage *>(memory);
            if (storage == nullptr || storage->element_binding() == nullptr)
            {
                throw std::runtime_error("Set to_python requires live storage with an element binding");
            }
            const auto  element_binding = storage->element_binding();
            const auto &ops             = element_binding.ops_ref();
            nb::list    items;
            for (std::size_t i = 0; i < storage->size(); ++i) { items.append(to_python(ops, storage->element_at(i))); }
            // A compact Set is the value-layer realization of Python's
            // immutable frozenset scalar. This concrete ValueOps strategy must
            // establish that public representation itself; TSData strategies
            // and Python facades consume the erased result unchanged. Slot-
            // backed TSS collections install different ValueOps and retain
            // their collection-specific mutable set surface.
            return nb::steal(PyFrozenSet_New(items.ptr()));
        }

        nb::object map_to_python(const void *, const void *memory)
        {
            const auto *storage = static_cast<const MapStorage *>(memory);
            if (storage == nullptr || storage->key_binding() == nullptr || storage->value_binding() == nullptr)
            {
                throw std::runtime_error("Map to_python requires live storage with key/value bindings");
            }
            const auto  key_binding   = storage->key_binding();
            const auto  value_binding = storage->value_binding();
            const auto &key_ops       = key_binding.ops_ref();
            const auto &value_ops     = value_binding.ops_ref();
            nb::dict    result;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                // UNSET values (None-valued entries) read back as None.
                result[to_python(key_ops, storage->key_at(i))] =
                    storage->value_set(i) ? to_python(value_ops, storage->value_at_index(i)) : nb::none();
            }
            return result;
        }

        nb::object map_key_adapter_to_python(const void *, const void *memory)
        {
            const auto *storage = static_cast<const MapStorage *>(memory);
            if (storage == nullptr || storage->key_binding() == nullptr)
            {
                throw std::runtime_error("Map key-set to_python requires live storage with a key binding");
            }
            const auto  key_binding = storage->key_binding();
            const auto &ops         = key_binding.ops_ref();
            nb::set     result;
            for (std::size_t i = 0; i < storage->size(); ++i) { result.add(to_python(ops, storage->key_at(i))); }
            return result;
        }

        // -- compact from_python -------------------------------------------------
        [[nodiscard]] const ValueTypeMetaData &checked_schema(const ValueTypeRef &binding, ValueTypeKind kind,
                                                              const char *what)
        {
            if (!binding.valid()) { throw std::logic_error(std::string{what} + " requires a valid binding"); }
            if (binding.schema()->value_kind() != kind)
            {
                throw std::logic_error(std::string{what} + " received the wrong value kind");
            }
            return *binding.schema();
        }

        template <typename State>
        [[nodiscard]] const State &checked_container_state(const ValueTypeRef &binding, const char *what)
        {
            const auto *state = static_cast<const State *>(binding.checked_plan().lifecycle_context);
            if (state == nullptr)
            {
                throw std::logic_error(std::string{what} + ": binding has no container lifecycle state");
            }
            return *state;
        }

        void require_non_none(nb::handle source, const char *what)
        {
            if (source.is_none()) { throw std::invalid_argument(std::string{what} + " requires a non-None value"); }
        }

        [[nodiscard]] bool is_sequence(nb::handle source)
        {
            nb::object object = nb::borrow<nb::object>(source);
            if (nb::isinstance<nb::list>(object) || nb::isinstance<nb::tuple>(object)) { return true; }
            // numpy arrays round-trip TSW values (a window's .value is an
            // ndarray; array scalars are tuple values in this runtime).
            return nb::hasattr(object, "__array_interface__");
        }

        template <typename Append>
        void for_each_sequence_item(nb::handle source, const char *what, Append append)
        {
            if (!is_sequence(source))
            {
                throw std::invalid_argument(std::string{what} + " expects a Python list or tuple");
            }

            nb::object   object = nb::borrow<nb::object>(source);
            nb::iterator it     = nb::iter(object);
            while (it != nb::iterator::sentinel())
            {
                nb::handle item = *it;
                if (item.is_none()) { throw std::invalid_argument(std::string{what} + " does not allow None elements"); }
                append(item);
                ++it;
            }
        }

        [[nodiscard]] Value value_from_python(const ValueTypeRef &binding, nb::handle source)
        {
            require_non_none(source, "from_python");
            Value out{binding};
            from_python(binding.ops_ref(), binding, const_cast<void *>(out.view().data()), source);
            return out;
        }

        void list_from_python(const void *, const ValueTypeRef &binding, void *memory, nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("List from_python requires live storage"); }
            const auto &schema = checked_schema(binding, ValueTypeKind::List, "List from_python");
            if (schema.is_fixed_size())
            {
                throw std::logic_error("List from_python compact storage is only valid for dynamic lists");
            }

            const auto element_binding = checked_container_state<ListState>(binding, "List from_python").element_binding;
            ListBuilder builder{element_binding};
            for_each_sequence_item(source, "List value", [&](nb::handle item) {
                Value element = value_from_python(element_binding, item);
                builder.push_back_copy(element.view().data());
            });

            *static_cast<ListStorage *>(memory) = builder.build_storage();
        }

        void cyclic_buffer_from_python(const void *, const ValueTypeRef &binding, void *memory, nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("CyclicBuffer from_python requires live storage"); }
            const auto &schema = checked_schema(binding, ValueTypeKind::CyclicBuffer, "CyclicBuffer from_python");
            if (schema.fixed_size == 0) { throw std::invalid_argument("CyclicBuffer value requires a non-zero capacity"); }

            const auto element_binding =
                checked_container_state<CyclicBufferState>(binding, "CyclicBuffer from_python").element_binding;
            CyclicBufferBuilder builder{element_binding, schema.fixed_size};
            for_each_sequence_item(source, "CyclicBuffer value", [&](nb::handle item) {
                Value element = value_from_python(element_binding, item);
                builder.push_back_copy(element.view().data());
            });

            *static_cast<CyclicBufferStorage *>(memory) = builder.build_storage();
        }

        void queue_from_python(const void *, const ValueTypeRef &binding, void *memory, nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("Queue from_python requires live storage"); }
            const auto &schema = checked_schema(binding, ValueTypeKind::Queue, "Queue from_python");

            const auto element_binding = checked_container_state<QueueState>(binding, "Queue from_python").element_binding;
            QueueBuilder builder{element_binding, schema.fixed_size};
            for_each_sequence_item(source, "Queue value", [&](nb::handle item) {
                Value element = value_from_python(element_binding, item);
                builder.push_copy(element.view().data());
            });

            *static_cast<QueueStorage *>(memory) = builder.build_storage();
        }

        void set_from_python(const void *, const ValueTypeRef &binding, void *memory, nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("Set from_python requires live storage"); }
            (void)checked_schema(binding, ValueTypeKind::Set, "Set from_python");

            nb::object object = nb::borrow<nb::object>(source);
            if (!nb::isinstance<nb::set>(object) && !nb::isinstance<nb::frozenset>(object) &&
                !nb::isinstance<nb::list>(object) && !nb::isinstance<nb::tuple>(object))
            {
                throw std::invalid_argument("Set value expects a Python set, frozenset, list, or tuple");
            }

            const auto element_binding = checked_container_state<SetState>(binding, "Set from_python").element_binding;
            SetBuilder   builder{element_binding};
            nb::iterator it = nb::iter(object);
            while (it != nb::iterator::sentinel())
            {
                nb::handle item = *it;
                if (item.is_none()) { throw std::invalid_argument("Set value does not allow None elements"); }
                Value element = value_from_python(element_binding, item);
                builder.insert_copy(element.view().data());
                ++it;
            }

            *static_cast<SetStorage *>(memory) = builder.build_storage();
        }

        void map_from_python(const void *, const ValueTypeRef &binding, void *memory, nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("Map from_python requires live storage"); }
            (void)checked_schema(binding, ValueTypeKind::Map, "Map from_python");

            nb::object object = nb::borrow<nb::object>(source);
            if (!nb::isinstance<nb::dict>(object) && !nb::hasattr(object, "items"))
            {
                throw std::invalid_argument("Map value expects a Python dict or dict-like object");
            }

            const auto &state         = checked_container_state<MapState>(binding, "Map from_python");
            const auto  key_binding   = state.key_binding;
            const auto  value_binding = state.value_binding;
            MapBuilder  builder{key_binding, value_binding};

            nb::object   items = nb::hasattr(object, "items") ? object.attr("items")() : object;
            nb::iterator it    = nb::iter(items);
            while (it != nb::iterator::sentinel())
            {
                nb::tuple pair = nb::cast<nb::tuple>(*it);
                if (pair.size() != 2) { throw std::invalid_argument("Map items() must yield key/value pairs"); }
                if (pair[0].is_none()) { throw std::invalid_argument("Map value does not allow None keys"); }

                Value key = value_from_python(key_binding, nb::borrow<nb::object>(pair[0]));
                if (pair[1].is_none())
                {
                    // A None VALUE is an unset entry (value holes; element
                    // validity) - it reads back as None.
                    builder.set_item_unset(key.view().data());
                }
                else
                {
                    Value value = value_from_python(value_binding, nb::borrow<nb::object>(pair[1]));
                    builder.set_item_copy(key.view().data(), value.view().data());
                }
                ++it;
            }

            *static_cast<MapStorage *>(memory) = builder.build_storage();
        }

        // -- mutable containers (read-back only) ----------------------------------
        nb::object mutable_list_to_python(const void *, const void *memory)
        {
            const auto *storage = static_cast<const MutableListStorage *>(memory);
            if (storage->element_binding() == nullptr) { return nb::list(); }
            const auto  element_binding = storage->element_binding();
            const auto &ops             = element_binding.ops_ref();
            nb::list    result;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                // UNSET elements (holes) read back as None.
                result.append(storage->element_set(i) ? to_python(ops, storage->element_at(i)) : nb::none());
            }
            return result;
        }

        nb::object mutable_map_to_python(const void *, const void *m)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            nb::dict    result;
            const auto  key_binding   = s->key_binding();
            const auto  value_binding = s->value_binding();
            if (key_binding == nullptr) { return result; }
            const auto &kops = key_binding.ops_ref();
            const auto &vops = value_binding.ops_ref();
            for (std::size_t slot = 0; slot < s->slot_capacity(); ++slot)
            {
                if (!s->slot_live(slot)) { continue; }
                result[to_python(kops, s->key_at(slot))] = to_python(vops, s->value_at_slot(slot));
            }
            return result;
        }

        nb::object mutable_set_to_python(const void *, const void *m)
        {
            const auto *s = static_cast<const MutableSetStorage *>(m);
            nb::list    items;
            if (s->element_binding() != nullptr)
            {
                const auto  element_binding = s->element_binding();
                const auto &eops            = element_binding.ops_ref();
                for (std::size_t slot = 0; slot < s->slot_capacity(); ++slot)
                {
                    if (!s->slot_live(slot)) { continue; }
                    items.append(to_python(eops, s->key_at(slot)));
                }
            }
            return nb::steal(PyFrozenSet_New(items.ptr()));
        }
    }  // namespace

    void fill_compact_container_conversions(PythonOps::Compact &section) noexcept
    {
        section.list_to_python            = &to_python_slot<&list_to_python>;
        section.list_to_python_tuple      = &to_python_slot<&list_to_python_tuple>;
        section.list_to_python_array      = &to_python_slot<&list_to_python_array>;
        section.list_from_python          = &from_python_slot<&list_from_python>;
        section.cyclic_buffer_to_python   = &to_python_slot<&cyclic_buffer_to_python>;
        section.cyclic_buffer_from_python = &from_python_slot<&cyclic_buffer_from_python>;
        section.queue_to_python           = &to_python_slot<&queue_to_python>;
        section.queue_from_python         = &from_python_slot<&queue_from_python>;
        section.set_to_python             = &to_python_slot<&set_to_python>;
        section.set_from_python           = &from_python_slot<&set_from_python>;
        section.map_to_python             = &to_python_slot<&map_to_python>;
        section.map_from_python           = &from_python_slot<&map_from_python>;
        section.map_key_adapter_to_python = &to_python_slot<&map_key_adapter_to_python>;
    }

    void fill_mutable_container_conversions(PythonOps::Mutable &section) noexcept
    {
        section.list_to_python = &to_python_slot<&mutable_list_to_python>;
        section.map_to_python  = &to_python_slot<&mutable_map_to_python>;
        section.set_to_python  = &to_python_slot<&mutable_set_to_python>;
    }
}  // namespace hgraph::python_bridge
