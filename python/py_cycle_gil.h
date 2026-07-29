/**
 * Cycle-scoped GIL holding for python user nodes (design record:
 * docs/source/developer_guide/python_bridge.rst, "GIL boundaries").
 *
 * The run loop still releases the GIL the instant it is entered; the
 * refinement here is that the FIRST python re-entry of an evaluation
 * cycle takes the GIL and keeps it for the remainder of that cycle
 * instead of releasing it per call. ``PyCycleGilObserver`` (registered
 * LAST on the executor's lifecycle-observer list so every other
 * observer's after-hook still sees the GIL held) releases it when the
 * ROOT graph's after-evaluation notification fires — which happens on
 * normal completion and on an escaping exception alike, and is exactly
 * the point after which a real-time executor may block waiting for push
 * sources. Python sender threads therefore keep the same liveness
 * guarantee as before: the GIL is always free while the loop waits
 * between cycles.
 *
 * Nested graph evaluations share the root's observer list; only the
 * root graph's boundaries arm/release the hold (a mesh child pause
 * suppresses ITS after-notification, so depth counting would leak — the
 * root's after always fires, executor.cpp treats a root pause as a
 * logic error).
 */
#ifndef HGRAPH_PYTHON_PY_CYCLE_GIL_H
#define HGRAPH_PYTHON_PY_CYCLE_GIL_H

#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/lifecycle_observer.h>

#include <Python.h>

#include <thread>

namespace hgraph::python_bridge
{
    class PyCycleGilObserver final : public LifecycleObserver
    {
      public:
        /** Root identity is bound after ``make_executor`` (the storage
            pointer is stable for the run's lifetime). */
        void bind_root(const void *root_graph_data) noexcept { root_ = root_graph_data; }

        void on_before_graph_evaluation(const GraphView &graph) override
        {
            if (graph.data() != root_) { return; }
            owner_    = std::this_thread::get_id();
            in_cycle_ = true;
        }

        void on_after_graph_evaluation(const GraphView &graph) override
        {
            if (graph.data() != root_) { return; }
            in_cycle_ = false;
            if (held_)
            {
                held_ = false;
                PyGILState_Release(state_);
            }
        }

        /** Take (or join) the cycle hold. Returns false outside a root
            cycle or off the evaluation thread — the caller then manages
            the GIL locally, exactly as before. */
        [[nodiscard]] bool try_hold() noexcept
        {
            if (!in_cycle_ || owner_ != std::this_thread::get_id()) { return false; }
            if (!held_)
            {
                state_ = PyGILState_Ensure();
                held_  = true;
            }
            return true;
        }

      private:
        const void      *root_{nullptr};
        std::thread::id  owner_{};
        PyGILState_STATE state_{};
        bool             in_cycle_{false};
        bool             held_{false};
    };

    /** The run currently evaluating on this process (the bridge rejects a
        second concurrent run before entering the loop, so a plain global
        suffices; ``try_hold`` still thread-checks for the lowered path and
        sender threads). Set/cleared around ``run()`` in py_wiring. */
    inline PyCycleGilObserver *py_active_cycle_gil{nullptr};

    /** Drop-in replacement for ``nb::gil_scoped_acquire`` on the per-tick
        node trampolines: joins the cycle hold when one is active (no
        release on destruction — the observer releases at cycle end),
        otherwise falls back to a plain local acquire/release pair. */
    class PyCycleGil
    {
      public:
        PyCycleGil()
        {
            if (auto *holder = py_active_cycle_gil; holder != nullptr && holder->try_hold()) { return; }
            state_ = PyGILState_Ensure();
            local_ = true;
        }

        PyCycleGil(const PyCycleGil &) = delete;
        PyCycleGil &operator=(const PyCycleGil &) = delete;

        ~PyCycleGil()
        {
            if (local_) { PyGILState_Release(state_); }
        }

      private:
        PyGILState_STATE state_{};
        bool             local_{false};
    };
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_PYTHON_PY_CYCLE_GIL_H
