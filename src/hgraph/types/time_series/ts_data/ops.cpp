#include <hgraph/types/time_series/ts_data.h>

#include <hgraph/types/value/value.h>

#include <stdexcept>
#include <string>

namespace hgraph::ts_data_detail
{
    namespace
    {
        std::size_t empty_inspection_field_count(const void *) noexcept { return 0; }

        TSDataInspectionField empty_inspection_field_at(const void *, std::size_t)
        {
            throw std::out_of_range("TSData inspection field index is out of range");
        }
    }

    const TSDataInspectionOps &empty_inspection_ops() noexcept
    {
        static const TSDataInspectionOps ops{
            .field_count_impl = &empty_inspection_field_count,
            .field_at_impl = &empty_inspection_field_at,
        };
        return ops;
    }

    [[noreturn]] void missing_ts_data_op(const char *name)
    {
        throw std::logic_error(std::string{"TSDataOps is missing "} + name + " implementation");
    }

    const TSDataLayout *missing_layout(const void *)
    {
        missing_ts_data_op("layout");
    }

    const TSDataTracking *missing_tracking(const void *, const void *)
    {
        missing_ts_data_op("tracking");
    }

    TSDataTracking *missing_mutable_tracking(const void *, void *)
    {
        missing_ts_data_op("mutable tracking");
    }

    bool missing_has_current_value(const void *, const void *)
    {
        missing_ts_data_op("validity");
    }

    bool missing_all_valid(const void *, const void *)
    {
        missing_ts_data_op("recursive validity");
    }

    const void *missing_value_memory(const void *, const void *)
    {
        missing_ts_data_op("value memory");
    }

    void *missing_mutable_value_memory(const void *, void *)
    {
        missing_ts_data_op("mutable value memory");
    }

    const void *missing_delta_memory(const void *, const void *)
    {
        missing_ts_data_op("delta memory");
    }

    void *missing_mutable_delta_memory(const void *, void *)
    {
        missing_ts_data_op("mutable delta memory");
    }


    void noop_record_child_modified(const void *, void *, std::size_t, DateTime) {}

    bool no_structural_delta(const void *, const void *, DateTime) noexcept
    {
        return false;
    }

    bool missing_copy_value_from(const void *, void *, const ValueView &, DateTime)
    {
        missing_ts_data_op("copy value");
    }

    bool missing_move_value_from(const void *, void *, ValueView, DateTime)
    {
        missing_ts_data_op("move value");
    }

    Value missing_empty_delta(const TSRoleTypeRef &)
    {
        missing_ts_data_op("empty delta");
    }

    const TSDataLayout *default_layout(const void *)
    {
        static const TSDataLayout layout{};
        return &layout;
    }

    const TSDataTracking *default_tracking(const void *, const void *)
    {
        static const TSDataTracking tracking{};
        return &tracking;
    }

    bool default_has_current_value(const void *, const void *)
    {
        return false;
    }

    bool default_all_valid(const void *, const void *)
    {
        return false;
    }

    const void *default_value_memory(const void *, const void *)
    {
        return nullptr;
    }

    const void *default_delta_memory(const void *, const void *)
    {
        return nullptr;
    }

    const TSDataOps &default_ts_data_ops() noexcept
    {
        static const TSDataOps table{
            .context = nullptr,
            .kind = TSTypeKind::SIGNAL,
            .allows_mutation = false,
            .layout_impl = &default_layout,
            .tracking_impl = &default_tracking,
            .mutable_tracking_impl = &missing_mutable_tracking,
            .has_current_value_impl = &default_has_current_value,
            .all_valid_impl = &default_all_valid,
            .value_memory_impl = &default_value_memory,
            .mutable_value_memory_impl = &missing_mutable_value_memory,
            .delta_memory_impl = &default_delta_memory,
            .mutable_delta_memory_impl = &missing_mutable_delta_memory,
            .indexed_child_count_impl = &default_indexed_child_count,
            .indexed_child_binding_impl = &default_indexed_child_binding,
            .indexed_child_memory_impl = &default_indexed_child_memory,
        };
        return table;
    }

    namespace
    {
        // No-value answers shared by the derived sentinel tables. Every thunk
        // ignores its memory pointer: unbound views carry null data.
        std::size_t sentinel_zero(const void *, const void *) noexcept { return 0; }
        bool        sentinel_false_slot(const void *, const void *, std::size_t) noexcept { return false; }
        ValueView sentinel_empty_key(const void *, const void *, std::size_t) noexcept { return {}; }
        const void *sentinel_null_slot(const void *, const void *, std::size_t) noexcept { return nullptr; }
        bool        sentinel_contains(const void *, const void *, const ValueView &) noexcept { return false; }
        std::size_t sentinel_find(const void *, const void *, const ValueView &) noexcept
        {
            return TS_DATA_NO_CHILD_ID;
        }
        std::size_t sentinel_next(const void *, const void *, std::size_t) noexcept
        {
            return TS_DATA_NO_CHILD_ID;
        }
        Range<ValueView> sentinel_value_range(const void *, const void *) noexcept { return {}; }
        Range<TSDataView> sentinel_ts_range(const void *, const void *) noexcept { return {}; }
        KeyValueRange<ValueView, TSDataView> sentinel_kv_range(const void *, const void *) noexcept
        {
            return {};
        }
        TSRoleTypeRef sentinel_child_binding_at_slot(const void *, const void *, std::size_t) noexcept
        {
            return {};
        }
        const void *sentinel_window_element(const void *, const void *, std::size_t) noexcept
        {
            return nullptr;
        }
        DateTime sentinel_window_time(const void *, const void *, std::size_t) noexcept { return MIN_DT; }
        bool     sentinel_window_full(const void *, const void *) noexcept { return false; }

        void install_sentinel_slot_surface(TSSDataOps &table) noexcept
        {
            table.size_impl                       = &sentinel_zero;
            table.slot_capacity_impl              = &sentinel_zero;
            table.slot_occupied_impl              = &sentinel_false_slot;
            table.slot_live_impl                  = &sentinel_false_slot;
            table.slot_added_impl                 = &sentinel_false_slot;
            table.slot_removed_impl               = &sentinel_false_slot;
            table.next_added_slot_impl            = &sentinel_next;
            table.next_removed_slot_impl          = &sentinel_next;
            table.key_at_slot_impl                = &sentinel_empty_key;
            table.contains_impl                   = &sentinel_contains;
            table.find_slot_impl                  = &sentinel_find;
            table.make_values_range_impl          = &sentinel_value_range;
            table.make_added_values_range_impl    = &sentinel_value_range;
            table.make_removed_values_range_impl  = &sentinel_value_range;
            // Subscription is a side effect, not an empty-value read: silently
            // discarding it on an unbound view would lose notifications, so
            // the throwing defaults stay installed (review finding on #492).
        }
    }  // namespace

    std::size_t default_indexed_child_count(const void *, const void *) { return 0; }
    TSRoleTypeRef default_indexed_child_binding(const void *, const void *, std::size_t) { return {}; }
    const void *default_indexed_child_memory(const void *, const void *, std::size_t) { return nullptr; }

    const TSSDataOps &default_tss_data_ops() noexcept
    {
        static const TSSDataOps table = [] {
            TSSDataOps t{};
            static_cast<TSDataOps &>(t) = default_ts_data_ops();
            t.kind                      = TSTypeKind::TSS;
            install_sentinel_slot_surface(t);
            return t;
        }();
        return table;
    }

    const TSDDataOps &default_tsd_data_ops() noexcept
    {
        static const TSDDataOps table = [] {
            TSDDataOps t{};
            static_cast<TSDataOps &>(t) = default_ts_data_ops();
            t.kind                      = TSTypeKind::TSD;
            install_sentinel_slot_surface(t);
            t.structural_delta_current_impl      = &no_structural_delta;
            t.child_binding_at_slot_impl         = &sentinel_child_binding_at_slot;
            t.child_at_slot_impl                 = &sentinel_null_slot;
            t.slot_modified_impl                 = &sentinel_false_slot;
            t.next_modified_slot_impl            = &sentinel_next;
            t.make_ts_values_range_impl          = &sentinel_ts_range;
            t.make_valid_keys_range_impl         = &sentinel_value_range;
            t.make_valid_ts_values_range_impl    = &sentinel_ts_range;
            t.make_modified_keys_range_impl      = &sentinel_value_range;
            t.make_modified_ts_values_range_impl = &sentinel_ts_range;
            t.make_added_ts_values_range_impl    = &sentinel_ts_range;
            t.make_removed_ts_values_range_impl  = &sentinel_ts_range;
            t.make_ts_kv_range_impl              = &sentinel_kv_range;
            t.make_valid_ts_kv_range_impl        = &sentinel_kv_range;
            t.make_modified_ts_kv_range_impl     = &sentinel_kv_range;
            t.make_added_ts_kv_range_impl        = &sentinel_kv_range;
            t.make_removed_ts_kv_range_impl      = &sentinel_kv_range;
            return t;
        }();
        return table;
    }

    const IndexedTSDataOps &default_indexed_ts_data_ops() noexcept
    {
        static const IndexedTSDataOps table = [] {
            IndexedTSDataOps t{};
            static_cast<TSDataOps &>(t) = default_ts_data_ops();
            t.kind                      = TSTypeKind::TSL;
            t.size_impl                 = &sentinel_zero;
            t.element_binding_impl      = &default_indexed_child_binding;
            t.element_memory_impl       = &default_indexed_child_memory;
            return t;
        }();
        return table;
    }

    const TSWDataOps &default_tsw_data_ops() noexcept
    {
        static const TSWDataOps table = [] {
            TSWDataOps t{};
            static_cast<TSDataOps &>(t) = default_ts_data_ops();
            t.kind                      = TSTypeKind::TSW;
            t.size_impl                 = &sentinel_zero;
            t.element_at_impl           = &sentinel_window_element;
            t.time_at_impl              = &sentinel_window_time;
            t.time_element_at_impl      = &sentinel_window_element;
            t.capacity_impl             = &sentinel_zero;
            t.full_impl                 = &sentinel_window_full;
            return t;
        }();
        return table;
    }

    Value missing_capture_delta(const TSInputView &)
    {
        missing_ts_data_op("capture delta");
    }

    bool missing_delta_has_effect(const TSOutputView &, const ValueView &)
    {
        missing_ts_data_op("delta effect test");
    }

    void missing_apply_delta(const TSOutputView &, const ValueView &)
    {
        missing_ts_data_op("apply delta");
    }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
    bool missing_from_python(const void *, void *, nb::handle, DateTime)
    {
        missing_ts_data_op("from Python");
    }

    nb::object missing_to_python(const void *, const void *)
    {
        missing_ts_data_op("to Python");
    }

    nb::object missing_delta_to_python(const void *, const void *, DateTime)
    {
        missing_ts_data_op("delta to Python");
    }
#endif

    std::size_t missing_indexed_size(const void *, const void *)
    {
        missing_ts_data_op("indexed size");
    }

    TSRoleTypeRef missing_indexed_element_binding(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("indexed element binding");
    }

    const void *missing_indexed_element_memory(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("indexed element memory");
    }

    void *missing_mutable_indexed_element_memory(const void *, void *, std::size_t)
    {
        missing_ts_data_op("mutable indexed element memory");
    }

    bool noop_clear_collection(const TSDataView &, DateTime) noexcept
    {
        return false;
    }

    bool clear_tss_collection(const TSDataView &view, DateTime modified_time)
    {
        if (!view.valid()) { return false; }
        auto set      = view.as_set();
        auto mutation = set.begin_mutation(modified_time);
        mutation.clear();
        return true;
    }

    bool clear_tsd_collection(const TSDataView &view, DateTime modified_time)
    {
        if (!view.valid()) { return false; }
        auto dict     = view.as_dict();
        auto mutation = dict.begin_mutation(modified_time);
        mutation.clear();
        return true;
    }

    std::size_t missing_slot_size(const void *, const void *)
    {
        missing_ts_data_op("slot collection size");
    }

    std::size_t missing_slot_capacity(const void *, const void *)
    {
        missing_ts_data_op("slot collection capacity");
    }

    bool missing_slot_predicate(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("slot predicate");
    }

    ValueView missing_key_at_slot(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("key at slot");
    }

    bool missing_contains_key(const void *, const void *, const ValueView &)
    {
        missing_ts_data_op("key containment");
    }

    std::size_t missing_find_key_slot(const void *, const void *, const ValueView &)
    {
        missing_ts_data_op("key slot lookup");
    }

    std::size_t missing_next_modified_slot(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("next modified slot");
    }

    std::size_t missing_next_delta_slot(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("next structural delta slot");
    }

    Range<ValueView> missing_value_range(const void *, const void *)
    {
        missing_ts_data_op("value range");
    }

    KeyValueRange<ValueView, TSDataView> missing_ts_data_kv_range(const void *, const void *)
    {
        missing_ts_data_op("TSData key/value range");
    }

    Range<TSDataView> missing_ts_data_range(const void *, const void *)
    {
        missing_ts_data_op("TSData value range");
    }

    SlotTSDataMutationResult missing_insert_key(const void *, void *, const ValueView &, DateTime)
    {
        missing_ts_data_op("key insertion");
    }

    SlotTSDataMutationResult missing_insert_key_move(const void *, void *, const ValueView &, DateTime)
    {
        missing_ts_data_op("key move insertion");
    }

    SlotTSDataMutationResult missing_remove_key(const void *, void *, const ValueView &, DateTime)
    {
        missing_ts_data_op("key removal");
    }

    SlotTSDataMutationResult missing_remove_slot(const void *, void *, std::size_t, DateTime)
    {
        missing_ts_data_op("slot removal");
    }

    bool missing_touch_slots(const void *, void *, DateTime)
    {
        missing_ts_data_op("slot collection touch");
    }

    void missing_reserve_slots(const void *, void *, std::size_t)
    {
        missing_ts_data_op("slot reservation");
    }

    void missing_subscribe_slot_observer(const void *, void *, SlotObserver *)
    {
        missing_ts_data_op("slot observer subscription");
    }

    void missing_unsubscribe_slot_observer(const void *, void *, SlotObserver *)
    {
        missing_ts_data_op("slot observer unsubscription");
    }

    const void *missing_child_at_slot(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("child at slot");
    }

    std::size_t missing_window_size(const void *, const void *) { missing_ts_data_op("window size"); }

    const void *missing_window_element(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("window element");
    }

    DateTime missing_window_time(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("window element time");
    }

    const void *missing_window_time_element(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("window time element");
    }

    std::size_t missing_window_capacity(const void *, const void *) { missing_ts_data_op("window capacity"); }

    bool missing_window_full(const void *, const void *) { missing_ts_data_op("window full"); }

    void missing_window_push(const void *, void *, const ValueView &, DateTime)
    {
        missing_ts_data_op("window push");
    }

    void missing_window_clear(const void *, void *, DateTime) { missing_ts_data_op("window clear"); }
}  // namespace hgraph::ts_data_detail

namespace hgraph
{
    void strip_to_read_only(TSSDataOps &ops) noexcept
    {
        const TSDataOps  base_defaults{};
        const TSSDataOps set_defaults{};
        ops.allows_mutation                   = false;
        ops.direct_native_value               = false;
        ops.copy_value_from_impl              = base_defaults.copy_value_from_impl;
        ops.move_value_from_impl              = base_defaults.move_value_from_impl;
        ops.apply_delta_impl                  = base_defaults.apply_delta_impl;
        ops.clear_collection_impl             = base_defaults.clear_collection_impl;
        ops.mutable_value_memory_impl         = base_defaults.mutable_value_memory_impl;
        ops.mutable_delta_memory_impl         = base_defaults.mutable_delta_memory_impl;
        ops.mutable_indexed_child_memory_impl = base_defaults.mutable_indexed_child_memory_impl;
#if HGRAPH_ENABLE_PYTHON_USER_NODES
        ops.from_python_impl                  = base_defaults.from_python_impl;
#endif
        ops.insert_key_impl      = set_defaults.insert_key_impl;
        ops.insert_key_move_impl = set_defaults.insert_key_move_impl;
        ops.remove_key_impl      = set_defaults.remove_key_impl;
        ops.remove_slot_impl     = set_defaults.remove_slot_impl;
        ops.touch_impl           = set_defaults.touch_impl;
        ops.reserve_impl         = set_defaults.reserve_impl;
    }
}  // namespace hgraph
