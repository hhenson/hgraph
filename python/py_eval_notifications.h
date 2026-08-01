/**
 * One-shot evaluation-cycle notifications (theme-C ruling 2026-08-01;
 * upstream ``EvaluationEngineApi.add_before/after_evaluation_notification``
 * parity). Python callables queue on the run and fire exactly once at the
 * next root-cycle boundary: the before-queue drains at the root graph's
 * before-evaluation notification, the after-queue at its after-evaluation
 * notification. Only these two hooks are exposed to python — lifecycle
 * observers stay C++-only per the recorded native-observer rule.
 *
 * Same thread-scoped arming pattern (and rationale) as the cycle-GIL
 * holder and the value mirror; the observer registers lazily on first use
 * so runs that never queue a notification pay nothing. Draining swaps the
 * queue first (a callback re-queuing itself lands in the NEXT cycle,
 * upstream semantics) and self-acquires the GIL — depending on lazy
 * registration order these hooks can run after the cycle-GIL holder
 * releases.
 */
#ifndef HGRAPH_PYTHON_PY_EVAL_NOTIFICATIONS_H
#define HGRAPH_PYTHON_PY_EVAL_NOTIFICATIONS_H

#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/lifecycle_observer.h>

#include <nanobind/nanobind.h>

#include <Python.h>

#include <thread>
#include <utility>
#include <vector>

namespace hgraph::python_bridge
{
    class PyEvalNotifications final : public LifecycleObserver
    {
      public:
        void bind_root(const void *root_graph_data, LifecycleObserverList *observers) noexcept
        {
            root_      = root_graph_data;
            observers_ = observers;
            owner_     = std::this_thread::get_id();
        }

        /** Queue ``fn`` (GIL held — called from node trampolines). */
        void add_before(nanobind::object fn)
        {
            ensure_registered();
            before_.push_back(std::move(fn));
        }

        void add_after(nanobind::object fn)
        {
            ensure_registered();
            after_.push_back(std::move(fn));
        }

        void on_before_graph_evaluation(const GraphView &graph) override
        {
            if (graph.data() != root_) { return; }
            drain(before_);
        }

        void on_after_graph_evaluation(const GraphView &graph) override
        {
            if (graph.data() != root_) { return; }
            drain(after_);
        }

        /** Run teardown (GIL held). */
        void clear()
        {
            before_.clear();
            after_.clear();
        }

      private:
        void ensure_registered()
        {
            if (registered_ || observers_ == nullptr) { return; }
            if (owner_ != std::this_thread::get_id()) { return; }
            observers_->add(this);
            registered_ = true;
        }

        void drain(std::vector<nanobind::object> &queue)
        {
            if (queue.empty()) { return; }
            const PyGILState_STATE gil = PyGILState_Ensure();
            // One-shot semantics: swap first so a callback that re-queues
            // itself fires at the NEXT boundary, not in this drain.
            std::vector<nanobind::object> pending;
            pending.swap(queue);
            for (nanobind::object &fn : pending)
            {
                try
                {
                    fn();
                }
                catch (...)
                {
                    // A failing notification must not mask evaluation state
                    // (same containment rule as the observer machinery).
                    PyErr_Clear();
                }
            }
            pending.clear();
            PyGILState_Release(gil);
        }

        const void            *root_{nullptr};
        LifecycleObserverList *observers_{nullptr};
        std::thread::id        owner_{};
        std::vector<nanobind::object> before_{};
        std::vector<nanobind::object> after_{};
        bool                   registered_{false};
    };

    /** The run currently evaluating on THIS THREAD (same pattern/rationale
        as ``py_active_cycle_gil``). */
    inline thread_local PyEvalNotifications *py_active_eval_notifications{nullptr};
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_PYTHON_PY_EVAL_NOTIFICATIONS_H
