/**
 * Python-value mirror on outputs (issue #204; design record:
 * docs/source/developer_guide/python_bridge.rst, "Python-value mirror").
 *
 * When a python node's result is applied to its C++ output, the ORIGINAL
 * PyObject is retained keyed by {output storage pointer, last-modified
 * time}; a python consumer whose read resolves to that output at the same
 * lmt receives a new reference to the retained object instead of
 * converting — the C++ storage stays canonical for native readers,
 * record/replay, and serialization. Sound because each output has exactly
 * one writer: an lmt match proves the mirrored python write was the last
 * write. Handing out the same object is within contract — output values
 * are immutable by graph semantics (mutating one from python is UB,
 * Howard's ruling 2026-07-30) — and matches upstream hgraph's
 * reference-passing behaviour.
 *
 * Memory discipline: entries exist only for outputs a python node has
 * actually tried to READ (a read miss inserts an empty "wanted" slot;
 * writes fill only wanted slots), each holds at most the one current
 * object, the producing node's stop trampoline erases its entry (which
 * also closes the nested-slot-reuse staleness hole: children stop before
 * their storage is reused, and single-writer covers everything else), and
 * the whole map dies with the run. All access happens under the GIL
 * (python trampolines and run setup/teardown only; native code never
 * touches it).
 */
#ifndef HGRAPH_PYTHON_PY_VALUE_MIRROR_H
#define HGRAPH_PYTHON_PY_VALUE_MIRROR_H

#include <hgraph/types/static_schema.h>
#include <hgraph/util/date_time.h>

#include <Python.h>

#include <ankerl/unordered_dense.h>

#include <cstddef>

namespace hgraph::python_bridge
{
    class PyValueMirror
    {
      public:
        PyValueMirror() = default;
        PyValueMirror(const PyValueMirror &) = delete;
        PyValueMirror &operator=(const PyValueMirror &) = delete;

        ~PyValueMirror()
        {
            // Defensive: the run wrapper clears explicitly under the GIL;
            // this covers retained-run teardown from arbitrary points.
            if (!entries_.empty())
            {
                const PyGILState_STATE gil = PyGILState_Ensure();
                clear();
                PyGILState_Release(gil);
            }
        }

        /** Read-side probe (GIL held). A hit returns a NEW reference to the
            mirrored object; a miss marks the output as wanted so future
            python writes mirror it, and returns null (caller converts). */
        [[nodiscard]] PyObject *probe(const void *output_data, DateTime last_modified)
        {
            auto [it, inserted] = entries_.try_emplace(output_data);
            if (inserted) { return nullptr; }
            Entry &entry = it->second;
            if (entry.object == nullptr || entry.last_modified != last_modified) { return nullptr; }
            Py_INCREF(entry.object);
            return entry.object;
        }

        /** Write-side store (GIL held): fills only WANTED slots — an output
            no python node reads never retains anything. Replaces any prior
            object (one object per output, no history). */
        void store(const void *output_data, DateTime last_modified, PyObject *object)
        {
            const auto it = entries_.find(output_data);
            if (it == entries_.end()) { return; }
            Entry &entry = it->second;
            PyObject *previous = entry.object;
            Py_INCREF(object);
            entry.object        = object;
            entry.last_modified = last_modified;
            Py_XDECREF(previous);
        }

        /** Producing-node stop (GIL held): drop the entry so a reused slot
            can never serve a stale object. */
        void erase(const void *output_data)
        {
            const auto it = entries_.find(output_data);
            if (it == entries_.end()) { return; }
            Py_XDECREF(it->second.object);
            entries_.erase(it);
        }

        /** Run teardown (GIL held). */
        void clear()
        {
            for (auto &[key, entry] : entries_)
            {
                static_cast<void>(key);
                Py_XDECREF(entry.object);
            }
            entries_.clear();
        }

        [[nodiscard]] std::size_t size() const noexcept { return entries_.size(); }

      private:
        struct Entry
        {
            DateTime  last_modified{MIN_DT};
            PyObject *object{nullptr};
        };

        ankerl::unordered_dense::map<const void *, Entry> entries_{};
    };

    /** Mirror only where conversion is EXPENSIVE (issue #204 measurement:
        int-typed chains regressed 3.7% — a PyLong construction is cheaper
        than the map probe+store the mirror adds). Cheap scalar schemas are
        excluded at BOTH probe and store, so their outputs are never marked
        wanted and their reads never probe. Pointer compares only. */
    [[nodiscard]] inline bool py_mirror_eligible(const ValueTypeMetaData *value_schema) noexcept
    {
        if (value_schema == nullptr) { return false; }
        static const ValueTypeMetaData *const cheap[] = {
            scalar_descriptor<Int>::value_meta(),
            scalar_descriptor<Float>::value_meta(),
            scalar_descriptor<Bool>::value_meta(),
        };
        for (const auto *meta : cheap)
        {
            if (value_schema == meta) { return false; }
        }
        return true;
    }

    /** The run currently evaluating on THIS THREAD (same per-thread pattern
        and rationale as ``py_active_cycle_gil``). Null outside a run — every
        probe/store site checks. */
    inline thread_local PyValueMirror *py_active_value_mirror{nullptr};
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_PYTHON_PY_VALUE_MIRROR_H
