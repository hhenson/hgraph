#ifndef HGRAPH_CPP_TS_INPUT_TARGET_LINK_STRUCTURAL_STATE_H
#define HGRAPH_CPP_TS_INPUT_TARGET_LINK_STRUCTURAL_STATE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/storage_metrics.h>
#include <hgraph/types/temporal.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/utils/slot_observer.h>

namespace hgraph::detail
{
    struct TSInputTargetLinkStorage;

    /**
     * Passive type-erased contract for target-link structural state.
     *
     * The selected ops table is immutable for a planned storage object and is
     * always non-null. Non-structural links bind the canonical no-op table;
     * TSS/TSD links bind the concrete structural implementation without
     * exposing that representation to the semantic target-link owner.
     */
    struct HGRAPH_EXPORT TSInputTargetLinkStructuralOps
    {
        bool supports_structural{false};

        void (*clear_transition)(TSInputTargetLinkStorage &owner) noexcept;
        void (*subscribe_key_set_tracking)(TSInputTargetLinkStorage &owner);
        void (*subscribe_slot_observers)(TSInputTargetLinkStorage &owner);
        void (*publish_sampled_transition)(TSInputTargetLinkStorage &owner,
                                           DateTime modified_time);
        void (*detach_target)(TSInputTargetLinkStorage &owner,
                              bool retain_structural_target,
                              DateTime modified_time);
        void (*unbind_noexcept)(TSInputTargetLinkStorage &owner) noexcept;
        void (*source_invalidated)(TSInputTargetLinkStorage &owner) noexcept;

        void (*add_slot_observer)(TSInputTargetLinkStorage &owner,
                                  SlotObserver *observer);
        void (*remove_slot_observer)(TSInputTargetLinkStorage &owner,
                                     SlotObserver *observer);

        const TSDataTracking &(*key_set_tracking)(TSInputTargetLinkStorage &owner);
        TSDataTracking &(*mutable_key_set_tracking)(TSInputTargetLinkStorage &owner);
        void (*record_key_set_modified)(TSInputTargetLinkStorage &owner,
                                        DateTime modified_time);
        void (*key_set_source_invalidated)(TSInputTargetLinkStorage &owner,
                                           const TSDataTracking *source) noexcept;

        TSDataView (*previous_target_view)(const TSInputTargetLinkStorage &owner) noexcept;
        bool (*transition_active)(const TSInputTargetLinkStorage &owner) noexcept;
        bool (*sampled_transition)(const TSInputTargetLinkStorage &owner) noexcept;
        DateTime (*transition_time)(const TSInputTargetLinkStorage &owner) noexcept;
        DynamicStorageMetrics (*dynamic_storage_metrics)(
            const TSInputTargetLinkStorage &owner) noexcept;

        void (*before_move_assignment_source)(TSInputTargetLinkStorage &owner) noexcept;
        void (*resubscribe_after_move_assignment)(TSInputTargetLinkStorage &owner) noexcept;
    };

    /** Raw-storage access selected alongside the storage plan. */
    struct HGRAPH_EXPORT TSInputTargetLinkStorageAccessOps
    {
        const TSInputTargetLinkStorage *(*get_const)(const void *memory) noexcept;
        TSInputTargetLinkStorage *(*get_mutable)(void *memory) noexcept;
    };

    /** Canonical no-op structural table used by default and moved-from owners. */
    [[nodiscard]] HGRAPH_EXPORT const TSInputTargetLinkStructuralOps &
    target_link_no_structural_ops() noexcept;

    /** Wiring-time storage plan selected from immutable schema metadata. */
    [[nodiscard]] HGRAPH_EXPORT const MemoryUtils::StoragePlan &
    target_link_storage_plan_for(TSTypeKind kind) noexcept;

    /** Raw-storage caster paired with ``target_link_storage_plan_for``. */
    [[nodiscard]] HGRAPH_EXPORT const TSInputTargetLinkStorageAccessOps &
    target_link_storage_access_for(TSTypeKind kind) noexcept;
}  // namespace hgraph::detail

#endif  // HGRAPH_CPP_TS_INPUT_TARGET_LINK_STRUCTURAL_STATE_H
