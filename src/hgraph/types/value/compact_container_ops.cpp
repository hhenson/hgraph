#include <hgraph/types/value/mutable_container_ops.h>
#include <hgraph/types/value/value_builder.h>

#include <stdexcept>
#include <string>

namespace hgraph
{
    namespace container_ops_detail
    {
        namespace
        {
            template <typename State>
            [[nodiscard]] const State &compact_target_state(ValueTypeRef binding, const char *what)
            {
                const auto *state = static_cast<const State *>(binding.lifecycle_context());
                if (state == nullptr)
                {
                    throw std::logic_error(std::string{what} + " target has no lifecycle state");
                }
                return *state;
            }
        } // namespace

        bool compact_accepts_source(const void *, ValueTypeRef binding, ValueTypeRef source) noexcept
        {
            return binding && source && (binding.schema() == source.schema() || binding.plan() == source.plan());
        }

        void compact_list_copy_assign_from(const void *, ValueTypeRef binding, void *dst, ValueTypeRef source,
                                           const void *src)
        {
            if (binding == source)
            {
                binding.copy_assign_at(dst, src);
                return;
            }
            const auto &state = compact_target_state<ListState>(binding, "List");
            ListBuilder builder{state.element_binding};
            const ValueView source_view{source, src};
            const auto values = source_view.as_list();
            for (std::size_t index = 0; index < values.size(); ++index)
            {
                if (!values.element_valid(index))
                {
                    builder.push_back_unset();
                    continue;
                }
                builder.push_back(values.at(index));
            }
            *static_cast<ListStorage *>(dst) = builder.build_storage();
        }

        void compact_list_move_assign_from(const void *context, ValueTypeRef binding, void *dst, ValueTypeRef source,
                                           void *src)
        {
            if (binding == source)
            {
                binding.move_assign_at(dst, src);
                return;
            }
            compact_list_copy_assign_from(context, binding, dst, source, src);
        }

        void compact_set_copy_assign_from(const void *, ValueTypeRef binding, void *dst, ValueTypeRef source,
                                          const void *src)
        {
            if (binding == source)
            {
                binding.copy_assign_at(dst, src);
                return;
            }
            const auto &state = compact_target_state<SetState>(binding, "Set");
            SetBuilder builder{state.element_binding};
            const ValueView source_view{source, src};
            for (const auto child : source_view.as_set())
            {
                static_cast<void>(builder.insert(child));
            }
            *static_cast<SetStorage *>(dst) = builder.build_storage();
        }

        void compact_set_move_assign_from(const void *context, ValueTypeRef binding, void *dst, ValueTypeRef source,
                                          void *src)
        {
            if (binding == source)
            {
                binding.move_assign_at(dst, src);
                return;
            }
            compact_set_copy_assign_from(context, binding, dst, source, src);
        }

        void compact_map_copy_assign_from(const void *, ValueTypeRef binding, void *dst, ValueTypeRef source,
                                          const void *src)
        {
            if (binding == source)
            {
                binding.copy_assign_at(dst, src);
                return;
            }
            const auto &state = compact_target_state<MapState>(binding, "Map");
            MapBuilder builder{state.key_binding, state.value_binding};
            const ValueView source_view{source, src};
            for (const auto entry : source_view.as_map())
            {
                if (!entry.second.has_value())
                {
                    builder.set_item_unset(entry.first);
                    continue;
                }
                builder.set_item(entry.first, entry.second);
            }
            *static_cast<MapStorage *>(dst) = builder.build_storage();
        }

        void compact_map_move_assign_from(const void *context, ValueTypeRef binding, void *dst, ValueTypeRef source,
                                          void *src)
        {
            if (binding == source)
            {
                binding.move_assign_at(dst, src);
                return;
            }
            compact_map_copy_assign_from(context, binding, dst, source, src);
        }

        void compact_cyclic_buffer_copy_assign_from(const void *, ValueTypeRef binding, void *dst, ValueTypeRef source,
                                                    const void *src)
        {
            if (binding == source)
            {
                binding.copy_assign_at(dst, src);
                return;
            }
            const auto &state = compact_target_state<CyclicBufferState>(binding, "CyclicBuffer");
            CyclicBufferBuilder builder{state.element_binding, state.capacity};
            const ValueView source_view{source, src};
            const auto values = source_view.as_cyclic_buffer();
            for (std::size_t index = 0; index < values.size(); ++index)
            {
                builder.push_back(values.at(index));
            }
            *static_cast<CyclicBufferStorage *>(dst) = builder.build_storage();
        }

        void compact_cyclic_buffer_move_assign_from(const void *context, ValueTypeRef binding, void *dst,
                                                    ValueTypeRef source, void *src)
        {
            if (binding == source)
            {
                binding.move_assign_at(dst, src);
                return;
            }
            compact_cyclic_buffer_copy_assign_from(context, binding, dst, source, src);
        }

        void compact_queue_copy_assign_from(const void *, ValueTypeRef binding, void *dst, ValueTypeRef source,
                                            const void *src)
        {
            if (binding == source)
            {
                binding.copy_assign_at(dst, src);
                return;
            }
            const auto &state = compact_target_state<QueueState>(binding, "Queue");
            QueueBuilder builder{state.element_binding, state.max_capacity};
            const ValueView source_view{source, src};
            const auto values = source_view.as_queue();
            for (std::size_t index = 0; index < values.size(); ++index)
            {
                builder.push(values.at(index));
            }
            *static_cast<QueueStorage *>(dst) = builder.build_storage();
        }

        void compact_queue_move_assign_from(const void *context, ValueTypeRef binding, void *dst, ValueTypeRef source,
                                            void *src)
        {
            if (binding == source)
            {
                binding.move_assign_at(dst, src);
                return;
            }
            compact_queue_copy_assign_from(context, binding, dst, source, src);
        }
    } // namespace container_ops_detail

    ValueTypeRef compact_list_type(const ValueTypeRef &element_binding)
    {
        const auto *meta = TypeRegistry::instance().list(element_binding.schema(), /*fixed_size=*/0);
        return compact_list_type(element_binding, *meta);
    }

    ValueTypeRef compact_list_type(const ValueTypeRef &element_binding, const ValueTypeMetaData &meta)
    {
        // Select the Python read-back shape when the canonical binding is
        // created. Keeping this function in hgraph_runtime also gives every
        // extension DSO the same plan, ops table, and interned TypeRecord.
        const auto &ops = meta.has(ValueTypeFlags::VariadicTuple)
                              ? container_ops_detail::compact_list_ops_impl<true>()
                              : meta.has(ValueTypeFlags::ShapedArray)
                                    ? container_ops_detail::compact_list_ops_impl<false, true>()
                                    : compact_list_ops();
        const auto &plan = compact_list_plan(element_binding);
        if (meta.is_nullable()) { return intern_value_type(meta, plan, ops); }
        const auto &debug = intern_dynamic_debug_descriptor(
            meta.header, plan, DebugLayoutKind::Sequence, nullptr, element_binding.record(),
            DebugDynamicLayout{
                .magic = DEBUG_DYNAMIC_LAYOUT_MAGIC,
                .abi_version = DEBUG_DYNAMIC_LAYOUT_ABI_VERSION,
                .kind = DebugDynamicKind::Contiguous,
                .flags = DebugDynamicFlags::DataIsIndirect,
                .size_offset = ListStorage::debug_size_offset(),
                .data_offset = ListStorage::debug_data_offset(),
                .stride = element_binding.checked_plan().layout.size,
            });
        return intern_value_type(meta, plan, ops, &debug);
    }

    ValueTypeRef compact_set_type(const ValueTypeRef &element_binding)
    {
        const auto *meta = TypeRegistry::instance().set(element_binding.schema());
        const auto &plan = compact_set_plan(element_binding);
        const SetStorage exemplar;
        const auto &debug = intern_dynamic_debug_descriptor(
            meta->header, plan, DebugLayoutKind::Sequence, nullptr, element_binding.record(),
            exemplar.debug_layout(element_binding.checked_plan().layout.size));
        return intern_value_type(*meta, plan, compact_set_ops(), &debug);
    }

    ValueTypeRef compact_map_type(const ValueTypeRef &key_binding, const ValueTypeRef &value_binding)
    {
        const auto *meta = TypeRegistry::instance().map(key_binding.schema(), value_binding.schema());
        return intern_value_type(*meta, compact_map_plan(key_binding, value_binding), compact_map_ops());
    }

    ValueTypeRef compact_cyclic_buffer_type(const ValueTypeRef &element_binding, std::size_t capacity)
    {
        const auto *meta = TypeRegistry::instance().cyclic_buffer(element_binding.schema(), capacity);
        const auto &plan = compact_cyclic_buffer_plan(element_binding, capacity);
        const auto &debug = intern_dynamic_debug_descriptor(
            meta->header, plan, DebugLayoutKind::Sequence, nullptr, element_binding.record(),
            DebugDynamicLayout{
                .magic = DEBUG_DYNAMIC_LAYOUT_MAGIC,
                .abi_version = DEBUG_DYNAMIC_LAYOUT_ABI_VERSION,
                .kind = DebugDynamicKind::Contiguous,
                .flags = DebugDynamicFlags::DataIsIndirect | DebugDynamicFlags::HasHead,
                .size_offset = CyclicBufferStorage::debug_size_offset(),
                .data_offset = CyclicBufferStorage::debug_data_offset(),
                .stride = element_binding.checked_plan().layout.size,
                .auxiliary_offset = CyclicBufferStorage::debug_head_offset(),
            });
        return intern_value_type(*meta, plan, compact_cyclic_buffer_ops(), &debug);
    }

    ValueTypeRef compact_queue_type(const ValueTypeRef &element_binding, std::size_t max_capacity)
    {
        const auto *meta = TypeRegistry::instance().queue(element_binding.schema(), max_capacity);
        const auto &plan = compact_queue_plan(element_binding, max_capacity);
        const auto &debug = intern_dynamic_debug_descriptor(
            meta->header, plan, DebugLayoutKind::Sequence, nullptr, element_binding.record(),
            DebugDynamicLayout{
                .magic = DEBUG_DYNAMIC_LAYOUT_MAGIC,
                .abi_version = DEBUG_DYNAMIC_LAYOUT_ABI_VERSION,
                .kind = DebugDynamicKind::Contiguous,
                .flags = DebugDynamicFlags::DataIsIndirect,
                .size_offset = QueueStorage::debug_size_offset(),
                .data_offset = QueueStorage::debug_data_offset(),
                .stride = element_binding.checked_plan().layout.size,
            });
        return intern_value_type(*meta, plan, compact_queue_ops(), &debug);
    }

    ValueTypeRef compact_map_key_set_type(const ValueTypeRef &key_binding,
                                          const ValueTypeRef &value_binding)
    {
        const auto *set_meta = TypeRegistry::instance().set(key_binding.schema());
        return intern_value_type(
            *set_meta, compact_map_plan(key_binding, value_binding), compact_map_key_set_ops());
    }

    void clear_compact_container_plans() noexcept
    {
        compact_detail::list_registry().clear();
        compact_detail::set_registry().clear();
        compact_detail::map_registry().clear();
        compact_detail::cyclic_buffer_registry().clear();
        compact_detail::queue_registry().clear();
    }

    ValueTypeRef mutable_list_type(const ValueTypeRef &element_binding)
    {
        const auto *meta = TypeRegistry::instance().mutable_list(element_binding.schema());
        const auto &plan = mutable_list_plan(element_binding);
        if (meta->is_nullable()) { return intern_value_type(*meta, plan, mutable_list_ops()); }
        const MutableListStorage exemplar{element_binding};
        const auto &debug = intern_dynamic_debug_descriptor(
            meta->header, plan, DebugLayoutKind::Sequence, nullptr, element_binding.record(), exemplar.debug_layout());
        return intern_value_type(*meta, plan, mutable_list_ops(), &debug);
    }

    ValueTypeRef mutable_map_type(const ValueTypeRef &key_binding, const ValueTypeRef &value_binding)
    {
        const auto *meta = TypeRegistry::instance().mutable_map(key_binding.schema(), value_binding.schema());
        const auto &plan = mutable_map_plan(key_binding, value_binding);
        const MutableMapStorage exemplar{key_binding, value_binding};
        const auto &debug = intern_dynamic_debug_descriptor(
            meta->header, plan, DebugLayoutKind::KeyedSlots, key_binding.record(), value_binding.record(),
            exemplar.debug_layout());
        return intern_value_type(*meta, plan, mutable_map_ops(), &debug);
    }

    ValueTypeRef mutable_set_type(const ValueTypeRef &element_binding)
    {
        const auto *meta = TypeRegistry::instance().mutable_set(element_binding.schema());
        const auto &plan = mutable_set_plan(element_binding);
        const MutableSetStorage exemplar{element_binding};
        const auto &debug = intern_dynamic_debug_descriptor(
            meta->header, plan, DebugLayoutKind::Sequence, nullptr, element_binding.record(), exemplar.debug_layout());
        return intern_value_type(*meta, plan, mutable_set_ops(), &debug);
    }

    void clear_mutable_container_plans() noexcept
    {
        mutable_container_detail::list_registry().clear();
        mutable_container_detail::map_registry().clear();
        mutable_container_detail::set_registry().clear();
    }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
    namespace container_ops_detail
    {
        namespace
        {
            [[nodiscard]] const ValueTypeMetaData &checked_schema(const ValueTypeRef &binding,
                                                                  ValueTypeKind           kind,
                                                                  const char             *what)
            {
                if (!binding.valid()) { throw std::logic_error(std::string{what} + " requires a valid binding"); }
                if (binding.schema()->value_kind() != kind)
                {
                    throw std::logic_error(std::string{what} + " received the wrong value kind");
                }
                return *binding.schema();
            }

            template <typename State>
            [[nodiscard]] const State &checked_container_state(const ValueTypeRef &binding,
                                                               const char *what)
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
                binding.ops_ref().from_python(binding, const_cast<void *>(out.view().data()), source);
                return out;
            }
        }  // namespace

        void list_from_python(const void *, const ValueTypeRef &binding, void *memory, nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("List from_python requires live storage"); }
            const auto &schema = checked_schema(binding, ValueTypeKind::List, "List from_python");
            if (schema.fixed_size != 0)
            {
                throw std::logic_error("List from_python compact storage is only valid for dynamic lists");
            }

            const auto element_binding =
                checked_container_state<ListState>(binding, "List from_python").element_binding;
            ListBuilder builder{element_binding};
            for_each_sequence_item(source, "List value", [&](nb::handle item) {
                Value element = value_from_python(element_binding, item);
                builder.push_back_copy(element.view().data());
            });

            *static_cast<ListStorage *>(memory) = builder.build_storage();
        }

        void cyclic_buffer_from_python(const void *, const ValueTypeRef &binding, void *memory,
                                       nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("CyclicBuffer from_python requires live storage"); }
            const auto &schema = checked_schema(binding, ValueTypeKind::CyclicBuffer, "CyclicBuffer from_python");
            if (schema.fixed_size == 0)
            {
                throw std::invalid_argument("CyclicBuffer value requires a non-zero capacity");
            }

            const auto element_binding = checked_container_state<CyclicBufferState>(
                binding, "CyclicBuffer from_python").element_binding;
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

            const auto element_binding = checked_container_state<QueueState>(
                binding, "Queue from_python").element_binding;
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

            const auto element_binding =
                checked_container_state<SetState>(binding, "Set from_python").element_binding;
            SetBuilder builder{element_binding};
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

            const auto &state = checked_container_state<MapState>(binding, "Map from_python");
            const auto key_binding   = state.key_binding;
            const auto value_binding = state.value_binding;
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

        void map_key_adapter_from_python(const void *, const ValueTypeRef &, void *, nb::handle)
        {
            throw std::logic_error("Map key-set adapter is read-only and cannot load from Python");
        }
    }  // namespace container_ops_detail
#endif
}  // namespace hgraph
