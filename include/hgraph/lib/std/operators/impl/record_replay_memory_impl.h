#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_MEMORY_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_MEMORY_IMPL_H

#include <hgraph/lib/std/operators/io.h>              // record / replay / compare markers
#include <hgraph/lib/std/value_util.h>                // ResolvedBindings (start-cached)
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
     * record/replay *backend* (``RecordReplayConfig::backend``); ``replay`` is a
     * SINGLE operator serving both.
     *
     * - ``dense_record_impl`` — the DENSE cycle-aligned harness recorder,
     *   selected under ``"testing"``: a plain-key ``List`` indexed by
     *   evaluation cycle (``MIN_ST + i*MIN_TD``; a hole = no tick), read back with
     *   ``get_recorded_values`` / ``Run.recorded``. It is the graph testing
     *   harness recorder.
     * - ``sparse_record_impl`` — the SPARSE absolute-time recorder, selected
     *   under the default ``"memory"``: a ``List`` of ``(evaluation_time, delta)``
     *   tuples under ``:memory:<fq_recordable_id>.<key>``, appended across runs
     *   and tolerant of arbitrary cross-cycle gaps (real-time alarms,
     *   ``@component`` persistence, RECOVER). Upstream ``_record_replay_in_memory``.
     * - ``replay_impl`` — the ONE in-memory replay.
     *   Replay is not split by the record backend: with no ``recordable_id`` it reads
     *   the seeded/recorded plain-key buffer (dense cycle-aligned); with an
     *   explicit ``recordable_id`` it reads the absolute-time
     *   ``:memory:<fq_recordable_id>.<key>`` recording (component recover /
     *   cross-run reads). Seeds (``set_replay_values`` / ``Run.set_replay``) are
     *   always the plain-key layout, so a bare ``replay(key)`` just replays them.
     *
     * The cycle-aligned buffer helpers and the seed/read API stay in
     * ``lib/testing/record_replay.h`` (the harness data layer these backends and
     * the tests share). The transitional frame backend is the sibling
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

        /** sparse_record's start-resolved recording plan: the fq key string
            and the delta binding (binding resolution locks the realization
            snapshot's counted mutex — start work, not per-tick work). */
        struct SparseRecordState
        {
            std::string  fq_key{};
            ValueTypeRef delta_binding{nullptr};
        };

        /** replay's cursor plus the start-resolved sparse fq key. */
        struct ReplayCursorState
        {
            Int         index{0};
            std::string fq_key{};
        };

        /** memory_compare's start-resolved summary key and counters.  An
            empty ``fq_key`` = a bare compare outside any recordable scope:
            it still throws on mismatch but publishes no summary. */
        struct MemoryCompareState
        {
            std::string fq_key{};
            Int         compared{0};
            Int         mismatches{0};
        };
    }  // namespace record_replay_memory_detail
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<stdlib::record_replay_memory_detail::SparseRecordState>
    {
        static constexpr std::string_view value{"stdlib.sparse_record_state"};
    };

    template <>
    struct scalar_name<stdlib::record_replay_memory_detail::ReplayCursorState>
    {
        static constexpr std::string_view value{"stdlib.replay_cursor_state"};
    };

    template <>
    struct scalar_name<stdlib::record_replay_memory_detail::MemoryCompareState>
    {
        static constexpr std::string_view value{"stdlib.memory_compare_state"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{

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

        /** Selected under the ``"testing"`` backend. */
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::effective_backend_is(context, record_replay::TESTING);
        }

        // hgraph parity: record(ts) defaults key to "out".
        static auto defaults()
        {
            return std::tuple{arg<"key">(Str{"out"}), arg<"sparse">(Bool{false}),
                              arg<"model">(Str{})};
        }

        static void start(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                          Scalar<"key", std::string> key, Scalar<"sparse", Bool>, Scalar<"model", Str>,
                          GlobalStateView gs, State<ResolvedBindings> bindings)
        {
            // Both buffer layouts are TYPED by the recorded delta schema;
            // resolve its binding here (the lookup locks the realization
            // snapshot's counted mutex). Buffer creation stays lazy on the
            // first tick (a never-ticking recording reads back empty either
            // way). A seeded state may contain the prior run's result under
            // this key.
            bindings.set(ResolvedBindings{
                .primary = testing::recording_binding_for(ts.base().schema()->delta_value_schema)});
            gs.erase(key.value());
        }

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, Scalar<"key", std::string> key,
                         Scalar<"sparse", Bool> sparse, Scalar<"model", Str>, GlobalStateView gs,
                         State<ResolvedBindings> bindings, DateTime now)
        {
            if (!ts.modified()) { return; }
            // The canonical per-tick delta, rebuilt as an owned value-layer Value (the
            // runtime's transient delta storage omits copy hooks).
            Value delta = capture_delta(ts.base());
            if (!delta_is_observable(ts.base(), delta.view())) { return; }
            if (sparse.value())
            {
                // SPARSE (the harness's __elide__): TYPED (time, delta)
                // tuple entries in evaluation order - one entry per tick
                // regardless of the gap, no Any boxing.
                const auto delta_binding = bindings.get().primary;
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
            const auto delta_binding = bindings.get().primary;
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
            return record_replay::effective_backend_is(context, record_replay::MEMORY);
        }

        // hgraph parity: bare ``record(ts)`` records under the default
        // recordable id ``nodes.record`` (matching ``get_recorded_value``'s
        // default read of ``:memory:nodes.record.<key>``); a component scope
        // or explicit ``recordable_id=`` overrides.
        static auto defaults()
        {
            return std::tuple{arg<"key">(Str{"out"}), arg<"recordable_id">(Str{"nodes.record"}),
                              arg<"model">(Str{})};
        }

        static void start(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                          Scalar<"key", Str> key, Scalar<"recordable_id", Str> recordable_id,
                          Scalar<"model", Str>, TraitsView traits,
                          State<record_replay_memory_detail::SparseRecordState> state)
        {
            state.set(record_replay_memory_detail::SparseRecordState{
                .fq_key = record_replay_memory_detail::memory_recording_key(
                    traits, recordable_id.value(), key.value()),
                .delta_binding = testing::recording_binding_for(
                    ts.base().schema()->delta_value_schema)});
        }

        static void eval(
            In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
            Scalar<"key", Str> /*resolved in start*/,
            Scalar<"recordable_id", Str> /*resolved in start*/, Scalar<"model", Str>,
            TraitsView /*resolved in start*/, GlobalStateView gs,
            State<record_replay_memory_detail::SparseRecordState> state, DateTime now)
        {
            if (!ts.modified()) { return; }
            const auto &resolved = state.ref();
            Value delta = capture_delta(ts.base());
            ValueView buffer = gs.get(resolved.fq_key);
            if (!buffer.valid())
            {
                gs.set(resolved.fq_key, testing::make_sparse_buffer(resolved.delta_binding));
                buffer = gs.get(resolved.fq_key);
            }
            auto mutation = buffer.as_list().begin_mutation();
            Value entry = testing::make_sparse_entry(
                resolved.delta_binding, now, std::move(delta));
            mutation.push_back(entry.view());
        }
    };

    /**
     * The single in-memory replay. Replay is not split by the record backend: it
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

        /** Selected under BOTH in-memory backends (replay serves both). */
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::effective_backend_is(context, record_replay::MEMORY) ||
                   record_replay::effective_backend_is(context, record_replay::TESTING);
        }

        // Absent recordable_id = the dense harness replay (plain key); a
        // non-empty recordable_id selects the sparse absolute-time read.
        static auto defaults()
        {
            return std::tuple{arg<"recordable_id">(Str{""}), arg<"model">(Str{})};
        }

        static void start(Scalar<"key", Str> key, Scalar<"recordable_id", Str> recordable_id,
                          Scalar<"model", Str>, TraitsView traits,
                          State<record_replay_memory_detail::ReplayCursorState> cursor)
        {
            // The sparse fq key is wiring-fixed; build the string once.
            auto current = cursor.get();
            if (!recordable_id.value().empty())
            {
                current.fq_key = record_replay_memory_detail::memory_recording_key(
                    traits, recordable_id.value(), key.value());
            }
            cursor.set(std::move(current));
        }

        static void eval(Scalar<"key", Str> key, Scalar<"recordable_id", Str> recordable_id,
                         Scalar<"model", Str>, TraitsView traits, GlobalStateView gs,
                         NodeScheduler sched,
                         State<record_replay_memory_detail::ReplayCursorState> cursor,
                         DateTime now, Out<TsVar<"S">> out)
        {
            static_cast<void>(traits);
            auto &state = cursor.modify();
            if (recordable_id.value().empty())
            {
                // DENSE cycle-aligned (harness): plain key, index by cycle.
                const ValueView buffer = gs.get(key.value());
                if (!buffer.valid()) { return; }  // nothing seeded under this key
                const auto list = buffer.as_list();
                const auto i    = state.index;
                const auto size = static_cast<Int>(list.size());
                if (i < size)
                {
                    if (auto delta = testing::dense_entry_delta(list, static_cast<std::size_t>(i));
                        delta.has_value())
                    {
                        apply_delta(out, delta->view());
                    }
                }
                state.index = i + 1;
                if (i + 1 < size) { sched.schedule(MIN_TD); }  // re-arm for the next cycle
                return;
            }
            // SPARSE absolute-time (:memory:): (time, delta) entries, replayed at
            // their recorded times (component recover / cross-run reads).
            const ValueView buffer = gs.get(state.fq_key);
            if (!buffer.valid()) { return; }

            const auto entries = buffer.as_list();
            std::size_t current = static_cast<std::size_t>(state.index);
            while (current < entries.size())
            {
                const auto entry = entries.at(current).as_indexed_view();
                const DateTime when = entry.at(0).checked_as<DateTime>();
                if (when < now) { ++current; continue; }
                if (when > now) { break; }
                apply_delta(out, entry.at(1));
                ++current;
            }
            state.index = static_cast<Int>(current);
            if (current < entries.size())
            {
                const auto next = entries.at(current).as_indexed_view();
                const DateTime when = next.at(0).checked_as<DateTime>();
                if (when > now) { sched.schedule(when); }
            }
        }
    };

    /** In-memory compare follows the interactive Python contract: a
        mismatch fails the run instead of writing a deferred report.  It
        still publishes the core-neutral ``ComparisonSummary`` (RFC 0025) —
        per tick, since the throw means stop may never see a final count —
        so both compare implementations answer the same summary query. */
    struct memory_compare_impl
    {
        static constexpr auto name = "memory_compare";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::effective_backend_is(context, record_replay::MEMORY);
        }

        static auto defaults() { return std::tuple{arg<"model">(Str{})}; }

        static void start(Scalar<"recordable_id", Str> recordable_id, TraitsView traits,
                          State<record_replay_memory_detail::MemoryCompareState> state)
        {
            // A bare compare outside any recordable scope has no id to
            // resolve; it keeps its throw-only contract (empty fq_key).
            std::string fq_key;
            if (!recordable_id.value().empty() ||
                traits.trait(record_replay::RECORDABLE_ID_TRAIT).valid())
            {
                fq_key = record_replay::fq_recordable_id(traits, recordable_id.value()) +
                         ".__compare__";
            }
            state.set(record_replay_memory_detail::MemoryCompareState{.fq_key = std::move(fq_key)});
        }

        static void eval(
            In<"lhs", TsVar<"S">, InputValidity::Unchecked> lhs,
            In<"rhs", TsVar<"S">, InputValidity::Unchecked> rhs,
            Scalar<"recordable_id", Str> recordable_id, Scalar<"model", Str>,
            State<record_replay_memory_detail::MemoryCompareState> state, GlobalStateView gs)
        {
            static_cast<void>(recordable_id);
            const bool equal = lhs.valid() && rhs.valid() && lhs.value().equals(rhs.value());
            auto &counters = state.modify();
            counters.compared += 1;
            if (!equal) { counters.mismatches += 1; }
            if (!counters.fq_key.empty())
            {
                // Published before the throw below so the failing tick is
                // visible in the summary.
                record_replay::publish_comparison_summary(
                    gs, counters.fq_key,
                    record_replay::ComparisonSummary{
                        static_cast<std::size_t>(counters.compared),
                        static_cast<std::size_t>(counters.mismatches)});
            }
            if (!equal)
            {
                throw std::runtime_error("record/replay comparison failed");
            }
        }
    };

    /** Register the in-memory record/replay/compare overloads (both the dense
        harness backend and the sparse absolute-time backend). */
    void register_record_replay_memory_operators();
}  // namespace hgraph::stdlib

namespace hgraph
{
    // The replay cursor state is also instantiated by the Python module's
    // harness wrappers; keep one exported plan/ops address.
    extern template HGRAPH_EXPORT const MemoryUtils::StoragePlan &
    MemoryUtils::plan_for<stdlib::record_replay_memory_detail::ReplayCursorState>() noexcept;
    extern template HGRAPH_EXPORT const ValueOps &
    ops_for<stdlib::record_replay_memory_detail::ReplayCursorState>() noexcept;
}  // namespace hgraph

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_MEMORY_IMPL_H
