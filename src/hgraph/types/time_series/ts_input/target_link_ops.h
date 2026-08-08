#ifndef HGRAPH_CPP_TS_INPUT_TARGET_LINK_OPS_H
#define HGRAPH_CPP_TS_INPUT_TARGET_LINK_OPS_H

#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/time_series/ts_input/target_link.h>
#include <hgraph/types/utils/memory_utils.h>

#include <cstddef>
#include <memory>

namespace hgraph::detail
{
    struct TSInputTargetLinkIndexedAccess;
    struct TSInputTargetLinkSlotAccess;

    struct TSInputTargetLinkContext
    {
        virtual ~TSInputTargetLinkContext() = default;

        const TSValueTypeMetaData *schema{nullptr};
        std::size_t                storage_offset{0};
        const TSInputTargetLinkStorageAccessOps *storage_access{
            &target_link_storage_access_for(TSTypeKind::TS)};
        const TSDataLayout        *active_layout{nullptr};
        const TSDataOps           *active_ops{nullptr};
        const TSInputTargetLinkSlotAccess *slot_access{nullptr};
        const TSInputTargetLinkIndexedAccess *indexed_access{nullptr};
    };

    using TSInputTargetLinkContextBuilder = std::unique_ptr<TSInputTargetLinkContext> (*)(
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
