#ifndef HGRAPH_CPP_ROOT_V2_SLOT_OBSERVER_H
#define HGRAPH_CPP_ROOT_V2_SLOT_OBSERVER_H

#include <hgraph/hgraph_export.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <vector>

namespace hgraph::v2
{
    /**
     * Observer for stable-slot structural lifecycle events.
     *
     * Set and map storage can keep parallel state over the same slot ids. This
     * protocol provides the structural hooks needed to keep that parallel state
     * synchronized with slot-capacity growth, logical removal, physical erase,
     * and clear.
     */
    struct HGRAPH_EXPORT SlotObserver
    {
        virtual ~SlotObserver() = default;

        virtual void on_capacity(size_t old_capacity, size_t new_capacity) = 0;
        virtual void on_insert(size_t slot) = 0;
        virtual void on_remove(size_t slot) = 0;
        virtual void on_erase(size_t slot) = 0;
        virtual void on_clear() = 0;
    };

    /**
     * Small observer list with de-duplicated registration and explicit
     * structural notifications.
     */
    class SlotObserverList
    {
      public:
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

            if (it != m_observers.end() - 1) {
                *it = m_observers.back();
            }
            m_observers.pop_back();
        }

        [[nodiscard]] bool empty() const noexcept { return m_observers.empty(); }
        [[nodiscard]] const std::vector<SlotObserver *> &entries() const noexcept { return m_observers; }

        void clear() noexcept
        {
            m_observers.clear();
        }

        void notify_capacity(size_t old_capacity, size_t new_capacity) const
        {
            for (auto *observer : m_observers) {
                if (observer != nullptr) {
                    observer->on_capacity(old_capacity, new_capacity);
                }
            }
        }

        void notify_insert(size_t slot) const
        {
            for (auto *observer : m_observers) {
                if (observer != nullptr) {
                    observer->on_insert(slot);
                }
            }
        }

        void notify_remove(size_t slot) const
        {
            for (auto *observer : m_observers) {
                if (observer != nullptr) {
                    observer->on_remove(slot);
                }
            }
        }

        void notify_erase(size_t slot) const
        {
            for (auto *observer : m_observers) {
                if (observer != nullptr) {
                    observer->on_erase(slot);
                }
            }
        }

        void notify_clear() const
        {
            for (auto *observer : m_observers) {
                if (observer != nullptr) {
                    observer->on_clear();
                }
            }
        }

      private:
        std::vector<SlotObserver *> m_observers{};
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_V2_SLOT_OBSERVER_H
