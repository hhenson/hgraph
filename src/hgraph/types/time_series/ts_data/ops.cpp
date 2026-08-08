#include <hgraph/types/time_series/ts_data.h>

#include <hgraph/types/value/value.h>

#include <stdexcept>
#include <string>

namespace hgraph::ts_data_detail
{
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

    void noop_reset_delta(const void *, void *) {}

    void noop_record_child_modified(const void *, void *, std::size_t, DateTime) {}

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
        };
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

    const void *missing_key_at_slot(const void *, const void *, std::size_t)
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
}  // namespace hgraph::ts_data_detail
