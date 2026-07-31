#ifndef HGRAPH_CPP_ROOT_V2_SLOT_OBSERVER_H
#define HGRAPH_CPP_ROOT_V2_SLOT_OBSERVER_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/storage_metrics.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <vector>

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
    struct HGRAPH_EXPORT SlotObserver
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
     * Small observer list with de-duplicated registration and explicit
     * structural notifications.
     *
     * Owned by each slot store; observers register and unregister themselves
     * during their own construction and destruction. Removal uses a swap-
     * with-back-and-pop so the list stays compact, and re-registration of
     * the same pointer is asserted against in debug builds.
     */
    class SlotObserverList
    {
      public:
        /** Register an observer; ignored if ``observer`` is null. Asserts on duplicate registration. */
        void add(SlotObserver *observer)
        {
            if (observer == nullptr) {
                return;
            }

            const auto it = std::find(m_observers.begin(), m_observers.end(), observer);
            assert(it == m_observers.end() && "slot observer registered twice");
            if (it == m_observers.end()) {
                m_observers.push_back(observer);
            }
        }

        /** Unregister an observer; ignored if ``observer`` is null. Asserts when not registered. */
        void remove(SlotObserver *observer)
        {
            if (observer == nullptr) {
                return;
            }

            const auto it = std::find(m_observers.begin(), m_observers.end(), observer);
            assert(it != m_observers.end() && "removing unregistered slot observer");
            if (it == m_observers.end()) {
                return;
            }

            if (m_notify_depth != 0) {
                *it = nullptr;
                m_compact_pending = true;
                return;
            }

            if (it != m_observers.end() - 1) {
                *it = m_observers.back();
            }
            m_observers.pop_back();
        }

        /** True when no observers are registered. */
        [[nodiscard]] bool empty() const noexcept { return m_observers.empty(); }
        /** Read-only access to the registered observer pointers. */
        [[nodiscard]] const std::vector<SlotObserver *> &entries() const noexcept { return m_observers; }

        /** Exact occupied/retained heap bytes for observer pointer storage. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            return {
                .live_bytes = m_observers.size() * sizeof(SlotObserver *),
                .reserved_bytes = m_observers.capacity() * sizeof(SlotObserver *),
            };
        }

        /** Drop every registered observer without notifying. */
        void clear() noexcept
        {
            if (m_notify_depth != 0) {
                std::fill(m_observers.begin(), m_observers.end(), nullptr);
                m_compact_pending = true;
                return;
            }
            m_observers.clear();
        }

        /** Invoke ``on_capacity`` on every registered observer. */
        void notify_capacity(size_t old_capacity, size_t new_capacity) const
        {
            NotifyGuard guard{*this};
            const auto limit = m_observers.size();
            for (size_t index = 0; index < limit; ++index) {
                auto *observer = m_observers[index];
                if (observer != nullptr) {
                    observer->on_capacity(old_capacity, new_capacity);
                }
            }
        }

        /** Invoke ``on_insert`` on every registered observer. */
        void notify_insert(size_t slot) const
        {
            NotifyGuard guard{*this};
            const auto limit = m_observers.size();
            for (size_t index = 0; index < limit; ++index) {
                auto *observer = m_observers[index];
                if (observer != nullptr) {
                    observer->on_insert(slot);
                }
            }
        }

        /** Invoke ``on_remove`` on every registered observer. */
        void notify_remove(size_t slot) const
        {
            NotifyGuard guard{*this};
            const auto limit = m_observers.size();
            for (size_t index = 0; index < limit; ++index) {
                auto *observer = m_observers[index];
                if (observer != nullptr) {
                    observer->on_remove(slot);
                }
            }
        }

        /** Invoke ``on_erase`` on every registered observer. */
        void notify_erase(size_t slot) const
        {
            NotifyGuard guard{*this};
            const auto limit = m_observers.size();
            for (size_t index = 0; index < limit; ++index) {
                auto *observer = m_observers[index];
                if (observer != nullptr) {
                    observer->on_erase(slot);
                }
            }
        }

        /** Invoke ``on_clear`` on every registered observer. */
        void notify_clear() const
        {
            NotifyGuard guard{*this};
            const auto limit = m_observers.size();
            for (size_t index = 0; index < limit; ++index) {
                auto *observer = m_observers[index];
                if (observer != nullptr) {
                    observer->on_clear();
                }
            }
        }

      private:
        struct NotifyGuard
        {
            explicit NotifyGuard(const SlotObserverList &owner) noexcept
                : owner(owner)
            {
                ++owner.m_notify_depth;
            }

            NotifyGuard(const NotifyGuard &) = delete;
            NotifyGuard &operator=(const NotifyGuard &) = delete;

            ~NotifyGuard() noexcept
            {
                --owner.m_notify_depth;
                if (owner.m_notify_depth == 0 && owner.m_compact_pending) {
                    owner.compact();
                }
            }

            const SlotObserverList &owner;
        };

        void compact() const noexcept
        {
            std::erase(m_observers, nullptr);
            m_compact_pending = false;
        }

        mutable std::vector<SlotObserver *> m_observers{};
        mutable size_t                      m_notify_depth{0};
        mutable bool                        m_compact_pending{false};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_V2_SLOT_OBSERVER_H
