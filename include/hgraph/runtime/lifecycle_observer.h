// runtime/lifecycle_observer.h — the open, runtime-populated set of listeners for
// graph/node start, stop, and evaluation events (design record:
// docs/source/developer_guide/architecture.rst, "Lifecycle Observers"). Mirrors
// types/utils/slot_observer.h's shape (raw non-owning pointers, self-registering,
// reentrancy-safe notify), not a shared generic template with it — two small
// concrete observer lists for two distinct event sets.
#ifndef HGRAPH_RUNTIME_LIFECYCLE_OBSERVER_H
#define HGRAPH_RUNTIME_LIFECYCLE_OBSERVER_H

#include <hgraph/hgraph_export.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <vector>

namespace hgraph
{
    class GraphView;
    class NodeView;

    /**
     * Observer for graph/node lifecycle events.
     *
     * Registered on a ``GraphExecutorBuilder`` (build time) or directly on a
     * running executor/graph's ``LifecycleObserverList`` (runtime). One flat
     * list per executor instance: nested graphs share the root's list (see
     * ``GraphView::lifecycle_observers`` / ``GraphExecutorView::lifecycle_observers``),
     * so a single registration observes the root graph and every nested graph
     * and node beneath it. All hooks default to no-ops; implementations
     * override only what they need.
     */
    struct HGRAPH_EXPORT LifecycleObserver
    {
        virtual ~LifecycleObserver() = default;

        virtual void on_before_start_graph(const GraphView &) {}
        virtual void on_after_start_graph(const GraphView &) {}
        virtual void on_start_graph_failed(const GraphView &) {}
        virtual void on_before_start_node(const NodeView &) {}
        virtual void on_after_start_node(const NodeView &) {}
        virtual void on_start_node_failed(const NodeView &) {}

        virtual void on_before_graph_evaluation(const GraphView &) {}
        virtual void on_after_graph_evaluation(const GraphView &) {}
        virtual void on_before_node_evaluation(const NodeView &) {}
        virtual void on_after_node_evaluation(const NodeView &) {}
        virtual void on_after_graph_push_nodes_evaluation(const GraphView &) {}

        virtual void on_before_stop_node(const NodeView &) {}
        virtual void on_after_stop_node(const NodeView &) {}
        virtual void on_stop_node_failed(const NodeView &) {}
        virtual void on_before_stop_graph(const GraphView &) {}
        virtual void on_after_stop_graph(const GraphView &) {}
        virtual void on_stop_graph_failed(const GraphView &) {}
    };

    /**
     * Small observer list with de-duplicated registration and lifecycle
     * notifications, walked in registration order (architecture.rst,
     * "Observer And Callback Draining").
     *
     * Owned by the executor's runtime storage; observers are registered and
     * unregistered by the caller (build time via
     * ``GraphExecutorBuilder::add_lifecycle_observer``, or at any point at
     * runtime via the accessor on ``GraphExecutorView``/``GraphView``).
     * Removal uses swap-with-back-and-pop so the list stays compact, and
     * re-registration of the same pointer is asserted against in debug
     * builds. Notification is reentrancy-safe: an observer may add or remove
     * observers (including itself) from within a callback.
     */
    class LifecycleObserverList
    {
      public:
        /** Register an observer; ignored if ``observer`` is null. Asserts on duplicate registration. */
        void add(LifecycleObserver *observer)
        {
            if (observer == nullptr) { return; }

            const auto it = std::find(m_observers.begin(), m_observers.end(), observer);
            assert(it == m_observers.end() && "lifecycle observer registered twice");
            if (it == m_observers.end()) { m_observers.push_back(observer); }
        }

        /** Unregister an observer; ignored if ``observer`` is null. Asserts when not registered. */
        void remove(LifecycleObserver *observer)
        {
            if (observer == nullptr) { return; }

            const auto it = std::find(m_observers.begin(), m_observers.end(), observer);
            assert(it != m_observers.end() && "removing unregistered lifecycle observer");
            if (it == m_observers.end()) { return; }

            if (m_notify_depth != 0)
            {
                *it = nullptr;
                m_compact_pending = true;
                return;
            }

            if (it != m_observers.end() - 1) { *it = m_observers.back(); }
            m_observers.pop_back();
        }

        /** True when no observers are registered. */
        [[nodiscard]] bool empty() const noexcept { return m_observers.empty(); }

        void notify_before_start_graph(const GraphView &graph) const
        {
            NotifyGuard guard{*this};
            const auto  limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr) { observer->on_before_start_graph(graph); }
            }
        }

        void notify_after_start_graph(const GraphView &graph) const
        {
            NotifyGuard guard{*this};
            const auto  limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr) { observer->on_after_start_graph(graph); }
            }
        }

        void notify_start_graph_failed(const GraphView &graph) const
        {
            NotifyGuard guard{*this};
            const auto limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr)
                {
                    observer->on_start_graph_failed(graph);
                }
            }
        }

        void notify_before_start_node(const NodeView &node) const
        {
            NotifyGuard guard{*this};
            const auto  limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr) { observer->on_before_start_node(node); }
            }
        }

        void notify_after_start_node(const NodeView &node) const
        {
            NotifyGuard guard{*this};
            const auto  limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr) { observer->on_after_start_node(node); }
            }
        }

        void notify_start_node_failed(const NodeView &node) const
        {
            NotifyGuard guard{*this};
            const auto limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr)
                {
                    observer->on_start_node_failed(node);
                }
            }
        }

        void notify_before_graph_evaluation(const GraphView &graph) const
        {
            NotifyGuard guard{*this};
            const auto  limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr) { observer->on_before_graph_evaluation(graph); }
            }
        }

        void notify_after_graph_evaluation(const GraphView &graph) const
        {
            NotifyGuard guard{*this};
            const auto  limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr) { observer->on_after_graph_evaluation(graph); }
            }
        }

        void notify_before_node_evaluation(const NodeView &node) const
        {
            NotifyGuard guard{*this};
            const auto  limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr) { observer->on_before_node_evaluation(node); }
            }
        }

        void notify_after_node_evaluation(const NodeView &node) const
        {
            NotifyGuard guard{*this};
            const auto  limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr) { observer->on_after_node_evaluation(node); }
            }
        }

        void notify_after_graph_push_nodes_evaluation(const GraphView &graph) const
        {
            NotifyGuard guard{*this};
            const auto limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr)
                {
                    observer->on_after_graph_push_nodes_evaluation(graph);
                }
            }
        }

        void notify_before_stop_node(const NodeView &node) const
        {
            NotifyGuard guard{*this};
            const auto  limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr) { observer->on_before_stop_node(node); }
            }
        }

        void notify_after_stop_node(const NodeView &node) const
        {
            NotifyGuard guard{*this};
            const auto  limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr) { observer->on_after_stop_node(node); }
            }
        }

        void notify_stop_node_failed(const NodeView &node) const
        {
            NotifyGuard guard{*this};
            const auto limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr)
                {
                    observer->on_stop_node_failed(node);
                }
            }
        }

        void notify_before_stop_graph(const GraphView &graph) const
        {
            NotifyGuard guard{*this};
            const auto  limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr) { observer->on_before_stop_graph(graph); }
            }
        }

        void notify_after_stop_graph(const GraphView &graph) const
        {
            NotifyGuard guard{*this};
            const auto  limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr) { observer->on_after_stop_graph(graph); }
            }
        }

        void notify_stop_graph_failed(const GraphView &graph) const
        {
            NotifyGuard guard{*this};
            const auto limit = m_observers.size();
            for (std::size_t index = 0; index < limit; ++index)
            {
                if (auto *observer = m_observers[index]; observer != nullptr)
                {
                    observer->on_stop_graph_failed(graph);
                }
            }
        }

      private:
        struct NotifyGuard
        {
            explicit NotifyGuard(const LifecycleObserverList &owner) noexcept : owner(owner) { ++owner.m_notify_depth; }

            NotifyGuard(const NotifyGuard &) = delete;
            NotifyGuard &operator=(const NotifyGuard &) = delete;

            ~NotifyGuard() noexcept
            {
                --owner.m_notify_depth;
                if (owner.m_notify_depth == 0 && owner.m_compact_pending) { owner.compact(); }
            }

            const LifecycleObserverList &owner;
        };

        void compact() const noexcept
        {
            std::erase(m_observers, nullptr);
            m_compact_pending = false;
        }

        mutable std::vector<LifecycleObserver *> m_observers{};
        mutable std::size_t                      m_notify_depth{0};
        mutable bool                              m_compact_pending{false};
    };
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_LIFECYCLE_OBSERVER_H
