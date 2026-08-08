#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_MEMORY_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_MEMORY_IMPL_H

#include <hgraph/lib/std/operators/io.h>              // record / replay / compare markers
#include <hgraph/lib/testing/record_replay_buffer.h>  // cycle-aligned buffer-format helpers
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

namespace hgraph::stdlib
{
    /**
     * The **in-memory** ``record`` / ``replay`` / ``compare`` backends — the one
     * place every in-memory record/replay operator lives (design record:
     * ``record_replay_table.rst`` — *In-memory record/replay — sparse vs dense*).
     * ``record`` carries two storage shapes, selected purely by the
     * record/replay *model* (``record_replay::Config::model``); ``replay`` is a
     * SINGLE operator serving both.
     *
     * - ``dense_record_impl`` — the DENSE cycle-aligned harness recorder,
     *   selected under ``IN_MEMORY_DENSE``: a plain-key ``List`` indexed by
     *   evaluation cycle (``MIN_ST + i*MIN_TD``; a hole = no tick), read back with
     *   ``get_recorded_values`` / ``Run.recorded``. It is the graph testing
     *   harness recorder.
     * - ``sparse_record_impl`` — the SPARSE absolute-time recorder, selected
     *   under the default ``IN_MEMORY``: a ``List`` of ``(evaluation_time, delta)``
     *   tuples under ``:memory:<fq_recordable_id>.<key>``, appended across runs
     *   and tolerant of arbitrary cross-cycle gaps (real-time alarms,
     *   ``@component`` persistence, RECOVER). Upstream ``_record_replay_in_memory``.
     * - ``replay_impl`` — the ONE in-memory replay.
     *   Replay is not split by the record model: with no ``recordable_id`` it reads
     *   the seeded/recorded plain-key buffer (dense cycle-aligned); with an
     *   explicit ``recordable_id`` it reads the absolute-time
     *   ``:memory:<fq_recordable_id>.<key>`` recording (component recover /
     *   cross-run reads). Seeds (``set_replay_values`` / ``Run.set_replay``) are
     *   always the plain-key layout, so a bare ``replay(key)`` just replays them.
     *
     * The cycle-aligned buffer helpers and the seed/read API stay in
     * ``lib/testing/record_replay.h`` (the harness data layer these backends and
     * the tests share). The frame (``DATA_FRAME``) backend is the sibling
     * ``record_replay_frame_impl.h``.
     */

    namespace record_replay_memory_detail
    {
        /** The absolute-time recording key: ``:memory:<fq_recordable_id>.<key>``. */
        [[nodiscard]] inline std::string memory_recording_key(
            TraitsView traits, std::string_view recordable_id, std::string_view key)
        {
            return ":memory:" + record_replay::fq_recordable_id(traits, recordable_id) +
                   "." + std::string{key};
        }
    }  // namespace record_replay_memory_detail

    /**
     * DENSE cycle-aligned record (the testing harness recorder). A single erased
     * sink over a deferred time-series type (``TsVar``), resolved from its
     * connected input port at wiring; behaviour is schema-as-data via the runtime
     * ``capture_delta``. Buffer is a plain-key ``List`` indexed by evaluation
     * cycle (a hole = no tick), read back with ``get_recorded_values``.
     */
    struct dense_record_impl
    {
        static constexpr auto name = "dense_record";

        /** Selected under the ``IN_MEMORY_DENSE`` model. */
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::model_is(context.global_state, record_replay::IN_MEMORY_DENSE);
        }

        // hgraph parity: record(ts) defaults key to "out".
        static auto defaults() { return std::tuple{arg<"key">(Str{"out"}), arg<"sparse">(Bool{false})}; }

        static void start(Scalar<"key", std::string> key, Scalar<"sparse", Bool>, GlobalStateView gs)
        {
            // Both buffer layouts are TYPED by the recorded delta schema,
            // which only eval sees - creation is lazy on the first tick (a
            // never-ticking recording reads back empty either way). A seeded
            // state may contain the prior run's result under this key.
            gs.erase(key.value());
        }

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, Scalar<"key", std::string> key,
                         Scalar<"sparse", Bool> sparse, GlobalStateView gs, DateTime now)
        {
            // Record every TICK, plus TICK-window TSW pre-validity ticks: a
            // window below its min period is invalid yet its delta stream
            // already flows (hgraph records those). Invalid structural
            // collection unbinds are also recorded when they carry a real
            // removal delta over a previously published key set.
            if (!ts.modified()) { return; }
            if (!ts.valid())
            {
                const auto *schema = ts.base().schema();
                const bool tick_window = schema->kind == TSTypeKind::TSW && !schema->data.tsw.is_duration_based;
                bool structural_removal = false;
                if (schema->kind == TSTypeKind::TSS)
                {
                    const auto input = ts.base().as_set();
                    const auto removed = input.removed();
                    structural_removal = removed.begin() != removed.end();
                }
                else if (schema->kind == TSTypeKind::TSD)
                {
                    const auto input = ts.base().as_dict();
                    const auto removed = input.removed_keys();
                    structural_removal = removed.begin() != removed.end();
                }
                if (!structural_removal && !tick_window) { return; }
            }
            // The canonical per-tick delta, rebuilt as an owned value-layer Value (the
            // runtime's transient delta storage omits copy hooks).
            Value delta = capture_delta(ts.base());
            const auto *schema = ts.base().schema();
            if (schema->kind == TSTypeKind::TSL && schema->fixed_size() != 0 && delta.view().as_map().empty())
            {
                // A fixed structural list can remain historically valid after every
                // routed REF leaf silently unbinds. Its parent may be scheduled, but
                // the empty map is not a tick and must remain a dense-buffer hole.
                return;
            }
            if (sparse.value())
            {
                // SPARSE (the harness's __elide__): TYPED (time, delta)
                // tuple entries in evaluation order - one entry per tick
                // regardless of the gap, no Any boxing.
                const auto delta_binding = testing::recording_binding_for(schema->delta_value_schema);
                ValueView buffer = gs.get(key.value());
                if (!buffer.valid())
                {
                    gs.set(key.value(), testing::make_sparse_buffer(delta_binding));
                    buffer = gs.get(key.value());
                }
                auto mutation = buffer.as_list().begin_mutation();
                mutation.push_back(testing::make_sparse_entry(delta_binding, now, std::move(delta)).view());
                return;
            }
            // DENSE: a TYPED List<delta_schema>; skipped cycles are UNSET
            // elements (element validity) - one default-constructed slot per
            // hole instead of a boxed Any.
            const auto delta_binding = testing::recording_binding_for(schema->delta_value_schema);
            ValueView buffer = gs.get(key.value());
            if (!buffer.valid())
            {
                gs.set(key.value(), testing::make_dense_buffer(delta_binding));
                buffer = gs.get(key.value());
            }
            const std::size_t offset   = testing::cycle_offset(now);
            auto              list     = buffer.as_list();
            auto              mutation = list.begin_mutation();
            std::size_t       size     = list.size();
            if (offset - size > testing::max_dense_cycles)
            {
                throw std::logic_error(
                    "record: the tick gap spans too many cycles to record densely - "
                    "record sparse (python: eval_node __elide__=True)");
            }
            while (size < offset)  // pad skipped cycles so the buffer index matches the evaluation cycle
            {
                mutation.push_back_unset();
                ++size;
            }
            mutation.push_back(delta.view());
        }
    };

    /** Persistent SPARSE in-GlobalState record backend (absolute-time). Unlike
        the cycle-aligned harness, this preserves absolute evaluation times and
        appends across runs so Recover|Record can continue a recording. */
    struct sparse_record_impl
    {
        static constexpr auto name = "sparse_record";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::model_is(context.global_state, record_replay::IN_MEMORY);
        }

        // hgraph parity: bare ``record(ts)`` records under the default
        // recordable id ``nodes.record`` (matching ``get_recorded_value``'s
        // default read of ``:memory:nodes.record.<key>``); a component scope
        // or explicit ``recordable_id=`` overrides.
        static auto defaults()
        {
            return std::tuple{arg<"key">(Str{"out"}), arg<"recordable_id">(Str{"nodes.record"})};
        }

        static void eval(
            In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
            Scalar<"key", Str> key,
            Scalar<"recordable_id", Str> recordable_id,
            TraitsView traits, GlobalStateView gs, DateTime now)
        {
            if (!ts.modified()) { return; }
            const std::string fq_key = record_replay_memory_detail::memory_recording_key(
                traits, recordable_id.value(), key.value());
            Value delta = capture_delta(ts.base());
            ValueView buffer = gs.get(fq_key);
            if (!buffer.valid())
            {
                gs.set(fq_key, testing::make_sparse_buffer(delta.binding()));
                buffer = gs.get(fq_key);
            }
            auto mutation = buffer.as_list().begin_mutation();
            const auto delta_binding = delta.binding();
            Value entry = testing::make_sparse_entry(
                delta_binding, now, std::move(delta));
            mutation.push_back(entry.view());
        }
    };

    /**
     * The single in-memory replay. Replay is not split by the record model: it
     * reads whatever was seeded/recorded, keyed on the presence of a
     * ``recordable_id``:
     *
     * - no ``recordable_id`` (a bare ``replay(key)``, the harness) → the DENSE
     *   cycle-aligned plain-key buffer (``set_replay_values`` seeds, or a dense
     *   recording), emitting each cycle's delta;
     * - explicit ``recordable_id`` (component ReplayOutput / Replay / Compare) →
     *   the SPARSE absolute-time ``:memory:<fq_recordable_id>.<key>`` recording,
     *   emitting each recorded delta at its recorded time (gaps included).
     */
    struct replay_impl
    {
        static constexpr auto name = "replay_in_memory";
        static constexpr bool schedule_on_start = true;

        /** Selected under BOTH in-memory models (replay is model-agnostic). */
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::model_is(context.global_state, record_replay::IN_MEMORY) ||
                   record_replay::model_is(context.global_state, record_replay::IN_MEMORY_DENSE);
        }

        // Absent recordable_id = the dense harness replay (plain key); a
        // non-empty recordable_id selects the sparse absolute-time read.
        static auto defaults() { return std::tuple{arg<"recordable_id">(Str{""})}; }

        static void eval(Scalar<"key", Str> key, Scalar<"recordable_id", Str> recordable_id,
                         TraitsView traits, GlobalStateView gs, NodeScheduler sched, State<Int> index,
                         DateTime now, Out<TsVar<"S">> out)
        {
            if (recordable_id.value().empty())
            {
                // DENSE cycle-aligned (harness): plain key, index by cycle.
                const ValueView buffer = gs.get(key.value());
                if (!buffer.valid()) { return; }  // nothing seeded under this key
                const auto list = buffer.as_list();
                const auto i    = index.get();
                const auto size = static_cast<Int>(list.size());
                if (i < size)
                {
                    if (auto delta = testing::dense_entry_delta(list, static_cast<std::size_t>(i));
                        delta.has_value())
                    {
                        apply_delta(out, delta->view());
                    }
                }
                index.set(i + 1);
                if (i + 1 < size) { sched.schedule(MIN_TD); }  // re-arm for the next cycle
                return;
            }
            // SPARSE absolute-time (:memory:): (time, delta) entries, replayed at
            // their recorded times (component recover / cross-run reads).
            const std::string fq_key = record_replay_memory_detail::memory_recording_key(
                traits, recordable_id.value(), key.value());
            const ValueView buffer = gs.get(fq_key);
            if (!buffer.valid()) { return; }

            const auto entries = buffer.as_list();
            std::size_t current = static_cast<std::size_t>(index.get());
            while (current < entries.size())
            {
                const auto entry = entries.at(current).as_indexed_view();
                const DateTime when = entry.at(0).checked_as<DateTime>();
                if (when < now) { ++current; continue; }
                if (when > now) { break; }
                apply_delta(out, entry.at(1));
                ++current;
            }
            index.set(static_cast<Int>(current));
            if (current < entries.size())
            {
                const auto next = entries.at(current).as_indexed_view();
                const DateTime when = next.at(0).checked_as<DateTime>();
                if (when > now) { sched.schedule(when); }
            }
        }
    };

    /** In-memory compare follows the interactive Python contract: a
        mismatch fails the run instead of writing a deferred report. */
    struct memory_compare_impl
    {
        static constexpr auto name = "memory_compare";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::model_is(context.global_state, record_replay::IN_MEMORY);
        }

        static void eval(
            In<"lhs", TsVar<"S">, InputValidity::Unchecked> lhs,
            In<"rhs", TsVar<"S">, InputValidity::Unchecked> rhs,
            Scalar<"recordable_id", Str> recordable_id)
        {
            static_cast<void>(recordable_id);
            if (!lhs.valid() || !rhs.valid() || !lhs.value().equals(rhs.value()))
            {
                throw std::runtime_error("record/replay comparison failed");
            }
        }
    };

    /** Register the in-memory record/replay/compare overloads (both the dense
        harness backend and the sparse absolute-time backend). */
    void register_record_replay_memory_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_MEMORY_IMPL_H
