#ifndef HGRAPH_CPP_TS_INPUT_TARGET_LINK_OPS_H
#define HGRAPH_CPP_TS_INPUT_TARGET_LINK_OPS_H

#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/time_series/ts_input/target_link.h>
#include <hgraph/types/utils/memory_utils.h>

#include <cstddef>
#include <utility>

namespace hgraph::detail
{
    struct TSInputTargetLinkIndexedAccess;
    struct TSInputTargetLinkSlotAccess;

    struct TSInputTargetLinkContext
    {
        const TSValueTypeMetaData *schema{nullptr};
        std::size_t                storage_offset{0};
        const TSInputTargetLinkStorageAccessOps *storage_access{
            &target_link_storage_access_for(TSTypeKind::TS)};
        const TSDataLayout        *active_layout{nullptr};
        const TSDataOps           *active_ops{nullptr};
        const TSInputTargetLinkSlotAccess *slot_access{nullptr};
        const TSInputTargetLinkIndexedAccess *indexed_access{nullptr};
    };

    using TSInputTargetLinkContextStorage =
        MemoryUtils::ErasedOwner<MemoryUtils::HeapOnlyStoragePolicy>;

    /**
     * Exact erased ownership plus the stable results published by a target-
     * link context. The concrete storage plan owns destruction; consumers do
     * not recover results by downcasting the erased payload.
     */
    struct TSInputTargetLinkContextOwner
    {
        TSInputTargetLinkContextStorage storage;
        const TSInputTargetLinkContext *context_result;
        const TSDataOps                 *ops_result;

        TSInputTargetLinkContextOwner(TSInputTargetLinkContextStorage storage_,
                                      const TSInputTargetLinkContext &context_,
                                      const TSDataOps &ops_)
            : storage(std::move(storage_)), context_result(&context_), ops_result(&ops_)
        {}

        TSInputTargetLinkContextOwner(const TSInputTargetLinkContextOwner &) = delete;
        TSInputTargetLinkContextOwner &operator=(const TSInputTargetLinkContextOwner &) = delete;
        TSInputTargetLinkContextOwner(TSInputTargetLinkContextOwner &&) noexcept = default;
        TSInputTargetLinkContextOwner &operator=(TSInputTargetLinkContextOwner &&) noexcept = default;

        [[nodiscard]] const TSInputTargetLinkContext &context() const noexcept { return *context_result; }
        [[nodiscard]] const TSDataOps &ops() const noexcept { return *ops_result; }
    };

    using TSInputTargetLinkContextBuilder = TSInputTargetLinkContextOwner (*)(
        const TSValueTypeMetaData &schema,
        const MemoryUtils::StoragePlan &root_plan,
        std::size_t storage_offset,
        const TSDataLayout &regular_layout);

    [[nodiscard]] const TSInputTargetLinkStorage *target_link_storage_at(
        const TSInputTargetLinkContext &context,
        const void *memory) noexcept;
    [[nodiscard]] TSInputTargetLinkStorage *target_link_storage_at(
        const TSInputTargetLinkContext &context,
        void *memory) noexcept;

    [[nodiscard]] const TSInputTargetLinkContext *target_link_context_for_ops(const TSDataOps *ops) noexcept;
    [[nodiscard]] const TSInputTargetLinkContextBuilder &target_link_context_builder_for(TSTypeKind kind);
}  // namespace hgraph::detail

#endif  // HGRAPH_CPP_TS_INPUT_TARGET_LINK_OPS_H
