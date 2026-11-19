#include "record_replay_frame_impl.h"

namespace hgraph
{
    // The recording option scalars are now extension-owned (RFC 0025
    // checkpoint 5); the extension is their only consumer, so one canonical
    // plan/ops address lives here rather than in hgraph_stdlib.
    template HGRAPH_PERSISTENCE_EXPORT const MemoryUtils::StoragePlan &
    MemoryUtils::plan_for<persistence::RecordAsOf>() noexcept;
    template HGRAPH_PERSISTENCE_EXPORT const ValueOps &
    ops_for<persistence::RecordAsOf>() noexcept;
    template HGRAPH_PERSISTENCE_EXPORT const MemoryUtils::StoragePlan &
    MemoryUtils::plan_for<persistence::RecordRemoves>() noexcept;
    template HGRAPH_PERSISTENCE_EXPORT const ValueOps &
    ops_for<persistence::RecordRemoves>() noexcept;
}  // namespace hgraph

namespace hgraph::persistence
{
    Value frame_seed_resolver(GlobalStateView state, std::string_view fq_key,
                              const TSValueTypeMetaData *schema, DateTime start_time)
    {
        if (schema == nullptr)
        {
            return {};
        }
        if (schema->contains_window())
        {
            const auto history = record_replay_frame_detail::recorded_window_state(state, fq_key, schema, start_time);
            return history ? Value{history->view().value()} : Value{};
        }
        return replay_const_value(state, fq_key, schema->value_schema, start_time,
                                  table::config(state).as_of.value_or(MAX_DT));
    }

    void register_record_replay_frame_operators()
    {
        register_overload<stdlib::record, record_frame_impl>();
        register_overload<stdlib::replay, replay_frame_impl>();
        register_overload<stdlib::compare, compare_impl>();
        register_overload<stdlib::replay_const, replay_const_impl>();
    }
}  // namespace hgraph::persistence
