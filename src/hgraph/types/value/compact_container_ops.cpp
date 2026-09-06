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
            if (!binding || !source) { return false; }
            if (binding.schema() == source.schema() || binding.plan() == source.plan()) { return true; }
            return TypeRegistry::instance().value_is_a(source.schema(), binding.schema());
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
    } // namespace container_ops_detail

    ValueTypeRef compact_element_binding(const ValueTypeRef &container_binding)
    {
        using namespace container_ops_detail;
        const auto *ops = container_binding ? container_binding.ops() : nullptr;
        if (ops == &compact_list_ops() || ops == &compact_list_ops_impl<true>())
        {
            return compact_target_state<ListState>(container_binding, "List").element_binding;
        }
        if (ops == &compact_set_ops()) { return compact_target_state<SetState>(container_binding, "Set").element_binding; }
        if (ops == &compact_cyclic_buffer_ops())
        {
            return compact_target_state<CyclicBufferState>(container_binding, "CyclicBuffer").element_binding;
        }
        if (ops == &compact_queue_ops()) { return compact_target_state<QueueState>(container_binding, "Queue").element_binding; }
        throw std::logic_error("compact_element_binding: binding is not a compact list, set, cyclic buffer or queue");
    }

    std::pair<ValueTypeRef, ValueTypeRef> compact_map_bindings(const ValueTypeRef &map_binding)
    {
        using namespace container_ops_detail;
        const auto *ops = map_binding ? map_binding.ops() : nullptr;
        if (ops != &compact_map_ops() && ops != &compact_map_key_set_ops())
        {
            throw std::logic_error("compact_map_bindings: binding is not a compact map");
        }
        const auto &state = compact_target_state<MapState>(map_binding, "Map");
        return {state.key_binding, state.value_binding};
    }

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

}  // namespace hgraph
