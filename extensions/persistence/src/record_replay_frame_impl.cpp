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
    void register_record_replay_frame_operators()
    {
        register_overload<stdlib::record, record_frame_impl>();
        register_overload<stdlib::replay, replay_frame_impl>();
        register_overload<stdlib::compare, compare_impl>();
        register_overload<stdlib::replay_const, replay_const_impl>();
    }
}  // namespace hgraph::persistence
