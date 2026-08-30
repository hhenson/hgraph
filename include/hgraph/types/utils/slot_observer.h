#ifndef HGRAPH_CPP_ROOT_V2_SLOT_OBSERVER_H
#define HGRAPH_CPP_ROOT_V2_SLOT_OBSERVER_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/storage_metrics.h>
#include <hgraph/util/tagged_ptr.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

namespace hgraph
{
    /**
     * Observer for stable-slot structural lifecycle events.
     *
     * Set and map storage can keep parallel state over the same slot ids.
     * This protocol provides the structural hooks needed to keep that
     * parallel state synchronised with slot-capacity growth, logical
     * removal, physical erase, and clear.
     *
     * Implementations subclass ``SlotObserver`` and register themselves with
     * a slot store via ``add_slot_observer``; the store fires the matching
     * hook whenever the corresponding event happens.
     */
    struct HGRAPH_CLASS_EXPORT SlotObserver
    {
        virtual ~SlotObserver() = default;

        /** Capacity grew from ``old_capacity`` to ``new_capacity`` slots. */
        virtual void on_capacity(size_t old_capacity, size_t new_capacity) = 0;
        /** A new payload was constructed at ``slot``. */
        virtual void on_insert(size_t slot) = 0;
        /** ``slot`` was logically removed but is still constructed (pending erase). */
        virtual void on_remove(size_t slot) = 0;
        /** ``slot``'s payload has been physically destroyed and the slot id freed. */
        virtual void on_erase(size_t slot) = 0;
        /** All payloads have been cleared and capacity reset. */
        virtual void on_clear() = 0;
    };

    /**
     * Compact observer list with de-duplicated registration and explicit
     * structural notifications.
     *
     * Empty and single-observer states occupy one tagged pointer. A spill
     * allocation is introduced only for two or more observers. Traversal is
     * representation-neutral: callbacks see the non-null observers present
     * when traversal starts, removals before an observer's turn suppress that
     * callback, and additions are deferred until the next traversal.
     */
    class HGRAPH_CLASS_EXPORT SlotObserverList
    {
      public:
        SlotObserverList() noexcept = default;
        SlotObserverList(const SlotObserverList &other);
        SlotObserverList &operator=(const SlotObserverList &other);
        SlotObserverList(SlotObserverList &&other) noexcept;
        SlotObserverList &operator=(SlotObserverList &&other) noexcept;
        ~SlotObserverList() noexcept;

        /** Register an observer; ignored if null and asserted against when duplicated. */
        void add(SlotObserver *observer);
        /** Unregister an observer; ignored if null and asserted when not registered. */
        void remove(SlotObserver *observer);

        /** True when no observers are registered. */
        [[nodiscard]] bool empty() const noexcept;
        /** Number of currently registered, non-null observers. */
        [[nodiscard]] std::size_t size() const noexcept;
        /** True when ``observer`` is currently registered. */
        [[nodiscard]] bool contains(const SlotObserver *observer) const noexcept;

        /**
         * Visit each observer without exposing the concrete storage strategy.
         * The visitor may re-enter this list and may add, remove, or clear
         * observers. Cleanup remains exception-safe.
         */
        template <typename Visitor>
        void for_each(Visitor &&visitor) const
        {
            using VisitorType = std::remove_reference_t<Visitor>;
            auto *context = const_cast<void *>(
                static_cast<const void *>(std::addressof(visitor)));
            for_each_erased(context, [](void *erased, SlotObserver *observer) {
                std::invoke(*static_cast<VisitorType *>(erased), observer);
            });
        }

        /** Exact occupied/retained heap bytes for multi-observer spill storage. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept;

        /** Drop every registered observer without notifying. */
        void clear() noexcept;

        /** Invoke ``on_capacity`` on every registered observer. */
        void notify_capacity(std::size_t old_capacity, std::size_t new_capacity) const;
        /** Invoke ``on_insert`` on every registered observer. */
        void notify_insert(std::size_t slot) const;
        /** Invoke ``on_remove`` on every registered observer. */
        void notify_remove(std::size_t slot) const;
        /** Invoke ``on_erase`` on every registered observer. */
        void notify_erase(std::size_t slot) const;
        /** Invoke ``on_clear`` on every registered observer. */
        void notify_clear() const;

      private:
        struct ObserverList;

        enum class Representation : std::uintptr_t
        {
            single = 0,
            many = 1,
        };

        using ObserverStorage = tagged_void_ptr<1, Representation>;
        using ErasedVisitor = void (*)(void *context, SlotObserver *observer);

        [[nodiscard]] SlotObserver *single() const noexcept;
        [[nodiscard]] ObserverList *many() const noexcept;
        void set_single(SlotObserver *observer) noexcept;
        void set_many(ObserverList *observers) noexcept;
        void compact_many(ObserverList &observers) noexcept;
        void for_each_erased(void *context, ErasedVisitor visitor) const;

        ObserverStorage observers_{};
    };

    static_assert(sizeof(SlotObserverList) <= sizeof(void *),
                  "SlotObserverList should remain a one-word tagged observer handle");
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_V2_SLOT_OBSERVER_H
