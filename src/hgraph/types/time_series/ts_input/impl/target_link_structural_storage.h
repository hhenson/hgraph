#ifndef HGRAPH_CPP_TS_INPUT_IMPL_TARGET_LINK_STRUCTURAL_STORAGE_H
#define HGRAPH_CPP_TS_INPUT_IMPL_TARGET_LINK_STRUCTURAL_STORAGE_H

#include <hgraph/types/time_series/ts_input/target_link.h>

#include <memory>

namespace hgraph::detail
{
    /** Concrete TSS/TSD representation; selected only by the structural plan. */
    struct TSInputTargetLinkStructuralStorage final : TSInputTargetLinkStorage
    {
        struct StructuralTransition;

        TSInputTargetLinkStructuralStorage() noexcept;
        TSInputTargetLinkStructuralStorage(const TSInputTargetLinkStructuralStorage &other);
        TSInputTargetLinkStructuralStorage &operator=(const TSInputTargetLinkStructuralStorage &other);
        TSInputTargetLinkStructuralStorage(TSInputTargetLinkStructuralStorage &&other) noexcept;
        TSInputTargetLinkStructuralStorage &operator=(TSInputTargetLinkStructuralStorage &&other) noexcept;
        ~TSInputTargetLinkStructuralStorage() noexcept;

        SlotObserverList slot_observers{};
        bool slot_observers_subscribed{false};
        mutable std::unique_ptr<StructuralTransition> structural_transition{};
    };
}  // namespace hgraph::detail

#endif  // HGRAPH_CPP_TS_INPUT_IMPL_TARGET_LINK_STRUCTURAL_STORAGE_H
