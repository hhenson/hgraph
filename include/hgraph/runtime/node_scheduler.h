#ifndef HGRAPH_RUNTIME_NODE_SCHEDULER_H
#define HGRAPH_RUNTIME_NODE_SCHEDULER_H

#include <hgraph/runtime/node_scheduler_view.h>

#include <map>

namespace hgraph
{
    /**
     * Persistent per-node scheduler **state** — the small footprint stored on a
     * node that declares a ``NodeScheduler``. It holds the sorted vector of pending
     * ``(time, tag)`` events (ordered by time then tag, unique) and the
     * ``tag -> time`` index used to replace/cancel tagged schedules. A node that never schedules
     * stores nothing (the slot exists only when ``uses_scheduler`` is set).
     *
     * Behaviour lives on the :cpp:class:`NodeScheduler` view, constructed on
     * demand when the scheduler is injected — the value/view split keeps the node
     * memory minimal and the graph/node-index/now context out of storage.
     */
    struct HGRAPH_CLASS_EXPORT NodeSchedulerState
    {
        using Event = std::pair<DateTime, std::string>;

        /**
         * Pending events, kept sorted by ``(time, tag)`` and unique, in a vector
         * rather than a ``std::set``: a self-rescheduling source (every
         * ``@generator``) inserts and pops one event per tick, and the set paid
         * a node allocation and release for each. The vector keeps its capacity,
         * so steady-state scheduling allocates nothing. Consumed events form a
         * prefix, removed in batches once it is at least as large as the live
         * suffix: draining N events takes O(N) moves, not O(N squared).
         * Inspect pending_events(), not the backing vector's consumed prefix.
         */
        std::vector<Event>              events{};
        std::map<std::string, DateTime> tags{};
        std::size_t                    events_head{0};

        [[nodiscard]] std::span<const Event> pending_events() const noexcept
        {
            return std::span<const Event>{events}.subspan(events_head);
        }

        void clear_events() noexcept
        {
            events.clear();
            events_head = 0;
        }

        /** Consume the earliest event without shifting the live suffix per tick. */
        void pop_event()
        {
            if (pending_events().empty()) { return; }
            events[events_head].second.clear();
            ++events_head;
            compact_events();
        }

        /** Insert keeping order and uniqueness (a duplicate is a no-op). */
        void insert_event(Event event)
        {
            if (pending_events().empty() || events.back() < event)
            {
                events.push_back(std::move(event));
                return;
            }
            const auto first = events.begin() + static_cast<std::ptrdiff_t>(events_head);
            const auto position = std::lower_bound(first, events.end(), event);
            if (position != events.end() && *position == event) { return; }
            events.insert(position, std::move(event));
        }

        /** Erase one exact event if present. */
        void erase_event(const Event &event)
        {
            const auto first = events.begin() + static_cast<std::ptrdiff_t>(events_head);
            const auto position = std::lower_bound(first, events.end(), event);
            if (position == events.end() || *position != event) { return; }
            if (position == first) { pop_event(); }
            else
            {
                events.erase(position);
                compact_events();
            }
        }

      private:
        void compact_events()
        {
            if (events_head == events.size()) { clear_events(); }
            else if (events_head >= events.size() - events_head)
            {
                events.erase(events.begin(), events.begin() + static_cast<std::ptrdiff_t>(events_head));
                events_head = 0;
            }
        }
    };

}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_NODE_SCHEDULER_H
