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
        struct ProducerMove;

        TSInputTargetLinkStructuralStorage() noexcept;
        TSInputTargetLinkStructuralStorage(const TSInputTargetLinkStructuralStorage &other);
        TSInputTargetLinkStructuralStorage &operator=(const TSInputTargetLinkStructuralStorage &other);
        TSInputTargetLinkStructuralStorage(TSInputTargetLinkStructuralStorage &&other) noexcept;
        TSInputTargetLinkStructuralStorage &operator=(TSInputTargetLinkStructuralStorage &&other) noexcept;
        ~TSInputTargetLinkStructuralStorage() noexcept;

        void follow_endpoint();
        void unfollow_endpoint() noexcept;
        void producer_changed(DateTime time);
        void producer_changed(ProducerMove &move);
        void adopt_producer(ProducerMove &move);
        void expire_followers() noexcept;

        // SlotObserverList supplies re-entrant traversal and the indexed spill.
        struct Follower final : SlotObserver
        {
            explicit Follower(TSInputTargetLinkStructuralStorage &link) noexcept : owner(link) {}
            void on_capacity(std::size_t, std::size_t) override {}
            void on_insert(std::size_t) override {}
            void on_remove(std::size_t) override {}
            void on_erase(std::size_t) override {}
            void on_clear() override {}

            TSInputTargetLinkStructuralStorage &owner;
        };

        TSOutputHandle producer{};
        TSInputTargetLinkStructuralStorage *followed{nullptr};
        SlotObserverList followers{};
        Follower follower{*this};
        SlotObserverList slot_observers{};
        bool slot_observers_subscribed{false};
        mutable std::unique_ptr<StructuralTransition> structural_transition{};
    };
}  // namespace hgraph::detail

#endif  // HGRAPH_CPP_TS_INPUT_IMPL_TARGET_LINK_STRUCTURAL_STORAGE_H
