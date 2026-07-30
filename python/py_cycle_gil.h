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
        /** Root identity and the executor's observer list are bound after
            ``make_executor`` (both stable for the run's lifetime; ``bind_root``
            runs on the evaluation thread, before the run loop starts). The
            observer stays OFF the list until the first python re-entry
            registers it lazily, so pure-native runs never pay the per-cycle
            hook dispatch (measured ~2% on the native tick benchmark). */
        void bind_root(const void *root_graph_data, LifecycleObserverList *observers) noexcept
        {
            root_      = root_graph_data;
            observers_ = observers;
            owner_     = std::this_thread::get_id();
        }

        void on_before_graph_evaluation(const GraphView &graph) override
        {
            if (graph.data() != root_) { return; }
            // Self-heal: if a PRECEDING observer threw out of the previous
            // cycle's after-notification before this (last-registered)
            // observer ran, the hold leaked past the cycle boundary. Release
            // it here — the next cycle is only now beginning, so the GIL was
            // wrongly held across exactly the window that already misbehaved,
            // and the imbalance stops at one cycle.
            release_if_held();
            in_cycle_ = true;
        }

        void on_after_graph_evaluation(const GraphView &graph) override
        {
            if (graph.data() != root_) { return; }
            in_cycle_ = false;
            release_if_held();
        }

        /** Balance the hold if armed; safe on the owning thread only (the
            run wrapper's cleanup and the observer hooks). */
        void release_if_held() noexcept
        {
            if (held_)
            {
                held_ = false;
                PyGILState_Release(state_);
            }
        }

        /** Take (or join) the cycle hold. Returns false off the evaluation
            thread — the caller then manages the GIL locally, exactly as
            before. */
        [[nodiscard]] bool try_hold() noexcept
        {
            if (observers_ == nullptr || owner_ != std::this_thread::get_id()) { return false; }
            if (!registered_)
            {
                // First python re-entry of the run: join the observer list
                // (appended last, so the release still runs after every other
                // observer's after-hook; runtime add is a sanctioned use of
                // the list). The current cycle is already past its
                // before-notification, so arm the cycle flag here.
                observers_->add(this);
                registered_ = true;
                in_cycle_   = true;
            }
            if (!in_cycle_) { return false; }
            if (!held_)
            {
                state_ = PyGILState_Ensure();
                held_  = true;
            }
            return true;
        }

      private:
        const void            *root_{nullptr};
        LifecycleObserverList *observers_{nullptr};
        std::thread::id        owner_{};
        PyGILState_STATE       state_{};
        bool                   in_cycle_{false};
        bool                   held_{false};
        bool                   registered_{false};
    };

    /** The run currently evaluating on THIS THREAD. Runs are per-thread (the
        active-runtime guard in py_runtime.h is thread_local, and the
        run_graph_on_thread adaptor runs graphs on background python threads
        concurrently), so the holder must be too: a process-global pointer
        could be read by one run's trampoline while another thread's run tears
        its observer down. A trampoline only ever sees its own thread's
        holder; ``try_hold``'s owner check is then a belt-and-braces
        invariant, not the safety mechanism. Set/cleared around ``run()`` in
        py_wiring. (Deliberate exception to the no-thread_local rule: this is
        bridge-boundary state keyed by python thread, like the GIL itself.) */
    inline thread_local PyCycleGilObserver *py_active_cycle_gil{nullptr};

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
