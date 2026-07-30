/**
 * Cycle-scoped GIL holding for python user nodes (design record:
 * docs/source/developer_guide/python_bridge.rst, "GIL boundaries").
 *
 * The run loop still releases the GIL the instant it is entered; the
 * refinement is COARSE-GRAINED (Howard's ruling, 2026-07-30): once a run
 * is known to contain python nodes, the GIL is taken at the start of
 * every evaluation cycle and released at its end. Node trampolines keep
 * their ordinary ``nb::gil_scoped_acquire``, which degenerates to a cheap
 * recursive ensure while the cycle holds the lock — there is no hold
 * hand-off between trampolines and observer to get wrong.
 *
 * "Contains python nodes" is detected at runtime rather than threaded
 * through core ``Wiring``: the first python trampoline call of the run
 * registers the observer (``py_cycle_gil_note_python_call``), so
 * pure-native runs never pay the per-cycle hook dispatch (measured ~2% on
 * the native tick benchmark) or any GIL traffic. The activating cycle
 * finishes on the trampolines' own local pairs; the coarse hold begins at
 * the next root before-notification.
 *
 * The observer is registered LAST on the executor's lifecycle-observer
 * list so every other observer's after-hook still sees the GIL held, and
 * it releases when the ROOT graph's after-evaluation notification fires —
 * which happens on normal completion and escaping exceptions alike, and
 * precedes any executor wait, so python sender threads keep the same
 * liveness guarantee as before (nested graph evaluations share the root's
 * list and are ignored; a mesh child pause suppresses ITS after-
 * notification, the root's always fires). Two nets bound a buggy observer
 * that throws out of the after loop before the release runs: the next
 * root before-notification finds the hold already armed and simply keeps
 * it (no double-ensure), and the run wrapper releases on exit — the
 * ``PyGILState`` pairing always closes.
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
            ``make_executor`` (both stable for the run's lifetime;
            ``bind_root`` runs on the evaluation thread, before the run loop
            starts). */
        void bind_root(const void *root_graph_data, LifecycleObserverList *observers) noexcept
        {
            root_      = root_graph_data;
            observers_ = observers;
            owner_     = std::this_thread::get_id();
        }

        /** First python re-entry of the run: join the observer list. */
        void activate() noexcept
        {
            if (registered_ || observers_ == nullptr) { return; }
            if (owner_ != std::this_thread::get_id()) { return; }
            observers_->add(this);
            registered_ = true;
        }

        void on_before_graph_evaluation(const GraphView &graph) override
        {
            if (graph.data() != root_) { return; }
            if (!held_)
            {
                state_ = PyGILState_Ensure();
                held_  = true;
            }
        }

        void on_after_graph_evaluation(const GraphView &graph) override
        {
            if (graph.data() != root_) { return; }
            release_if_held();
        }

        /** Balance the hold if armed; owning thread only (the observer
            hooks and the run wrapper's cleanup). */
        void release_if_held() noexcept
        {
            if (held_)
            {
                held_ = false;
                PyGILState_Release(state_);
            }
        }

      private:
        const void            *root_{nullptr};
        LifecycleObserverList *observers_{nullptr};
        std::thread::id        owner_{};
        PyGILState_STATE       state_{};
        bool                   held_{false};
        bool                   registered_{false};
    };

    /** The run currently evaluating on THIS THREAD. Runs are per-thread (the
        active-runtime guard in py_runtime.h is thread_local, and the
        run_graph_on_thread adaptor runs graphs on background python threads
        concurrently), so the holder must be too: a process-global pointer
        could be read by one run's trampoline while another thread's run
        tears its observer down. Set/cleared around ``run()`` in py_wiring.
        (Deliberate exception to the no-thread_local rule: bridge-boundary
        state keyed by python thread, like the GIL itself.) */
    inline thread_local PyCycleGilObserver *py_active_cycle_gil{nullptr};

    /** Called by the per-tick python-node trampolines just before their
        ordinary GIL acquire: proves the run contains python nodes and arms
        the coarse per-cycle hold from the next cycle on. */
    inline void py_cycle_gil_note_python_call() noexcept
    {
        if (auto *holder = py_active_cycle_gil; holder != nullptr) { holder->activate(); }
    }
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_PYTHON_PY_CYCLE_GIL_H
