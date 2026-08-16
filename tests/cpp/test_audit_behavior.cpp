// Native C++ coverage for the 2026-08-15 std-operator audit behavior fixes —
// the public-wiring equivalents of python/tests/test_audit_behavior_fixes.py,
// plus the restored-RecordableState regression the Python harness cannot
// drive (state seeding needs the native graph surface).

#include <catch2/catch_test_macros.hpp>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>

#include <initializer_list>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    [[nodiscard]] Value int_tuple(std::initializer_list<Int> values)
    {
        const auto *meta = scalar_descriptor<HomogeneousTuple<Int>>::value_meta();
        const auto binding = ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta());
        ListBuilder builder{binding};
        for (Int value : values) { builder.push_back(value); }
        ListStorage storage = builder.build_storage();
        return Value{compact_list_type(binding, *meta), &storage};
    }

    /** replay "x" -> take(N) -> record "out": the graph the restored-state
        regressions drive directly (start/evaluate on the mock root). */
    template <Int Count>
    struct TakeRestoreGraph
    {
        static constexpr auto name = "audit_take_restore_graph";

        static void compose(Wiring &w)
        {
            auto x = wire<stdlib::replay_impl, TS<Int>>(w, Str{"x"});
            wire<stdlib::dense_record_impl>(w, wire<stdlib::take>(w, x, Int{Count}), Str{"out"});
        }
    };

    /** The resettable form: replay "x" + reset "reset" -> take(ts, reset, N). */
    template <Int Count>
    struct TakeResetRestoreGraph
    {
        static constexpr auto name = "audit_take_reset_restore_graph";

        static void compose(Wiring &w)
        {
            auto x     = wire<stdlib::replay_impl, TS<Int>>(w, Str{"x"});
            auto reset = wire<stdlib::replay_impl, TS<Int>>(w, Str{"reset"});
            wire<stdlib::dense_record_impl>(w, wire<stdlib::take>(w, x, reset, Int{Count}),
                                            Str{"out"});
        }
    };

    /** The take node's ``ts`` input — child 0 of its input bundle. */
    [[nodiscard]] bool ts_input_active(const NodeView &node, DateTime when)
    {
        auto root   = node.input(when);
        auto bundle = root.as_bundle();
        return bundle[0].active();
    }

    /** Source whose eval writes through Out<TS<Int>>::set — the typed
        fast-write probe target. */
    struct FastWriteProbe
    {
        static constexpr auto name              = "audit_fast_write_probe";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<Int>> out) { out.set(Int{7}); }
    };

    [[nodiscard]] NodeView find_recordable_node(const GraphView &graph)
    {
        for (std::size_t index = 0; index < graph.node_count(); ++index)
        {
            auto node = graph.node_at(index);
            if (node.has_recordable_state()) { return node; }
        }
        return {};
    }
}  // namespace

TEST_CASE("audit: eq_ over TSDs stays silent until a source ticks")
{
    stdlib::register_standard_operators();
    // Two never-ticked dicts must not compare (upstream eq_tsds is
    // validity-gated); the pre-fix node emitted true at start.
    CHECK_OUTPUT((eval_node<stdlib::eq_, TSD<Int, TS<Int>>, TSD<Int, TS<Int>>>(
                     values<Value>(none, none), values<Value>(none, none))),
                 values<Bool>(none, none));
    // Once bound-and-ticked the comparison works, empty deltas included.
    CHECK_OUTPUT((eval_node<stdlib::eq_, TSD<Int, TS<Int>>, TSD<Int, TS<Int>>>(
                     values<Value>((dict_delta<Int, TS<Int>>({{0, 1}}))),
                     values<Value>((dict_delta<Int, TS<Int>>({{0, 1}}))))),
                 values<Bool>(true));
}

TEST_CASE("audit: container sum_/mean honour their default on an empty container")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT((eval_node<stdlib::sum_, TS<HomogeneousTuple<Int>>>(
                     values<Value>(int_tuple({})), values<Int>(42))),
                 values<Int>(42));
    CHECK_OUTPUT((eval_node<stdlib::mean, TS<HomogeneousTuple<Int>>>(
                     values<Value>(int_tuple({})), values<Float>(42.0))),
                 values<Float>(42.0));
    // Non-empty containers still aggregate; the default stays out of the way.
    CHECK_OUTPUT((eval_node<stdlib::sum_, TS<HomogeneousTuple<Int>>>(
                     values<Value>(int_tuple({1, 2, 3})), values<Int>(42))),
                 values<Int>(6));
    CHECK_OUTPUT((eval_node<stdlib::mean, TS<HomogeneousTuple<Int>>>(
                     values<Value>(int_tuple({2, 4})), values<Float>(42.0))),
                 values<Float>(3.0));
}

TEST_CASE("audit: a restored take counter derives passivation at start")
{
    stdlib::register_standard_operators();

    GraphBuilder gb = build_graph<TakeRestoreGraph<2>>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(10, 11, 12));

    MockRootGraph root{gb, MIN_ST, MAX_ET};
    auto          graph = root.graph();

    // Simulate a recovery restore: seed the take node's recordable counter
    // to its exhausted value BEFORE start. Activation is not recorded, so
    // the start hook must derive passivation from the restored state — the
    // pre-fix node forwarded indefinitely (take_reset) or leaked one tick
    // (take).
    NodeView take_node = find_recordable_node(graph);
    REQUIRE(take_node.valid());
    Out<TS<Int>>{take_node.recordable_state(MIN_ST), MIN_ST}.set(Int{2});

    graph.start(MIN_ST);
    // The start hook derived passivation from the restored counter.
    CHECK_FALSE(ts_input_active(take_node, MIN_ST));
    graph.evaluate(MIN_ST);
    graph.evaluate(MIN_ST + MIN_TD);
    graph.evaluate(MIN_ST + 2 * MIN_TD);
    graph.stop();

    // Exhausted before the run began: nothing forwards.
    CHECK_OUTPUT(get_recorded_values<Int>(graph.global_state(), "out"), values<Int>());
}

TEST_CASE("audit: a partially consumed restored take forwards only the remainder")
{
    stdlib::register_standard_operators();

    GraphBuilder gb = build_graph<TakeRestoreGraph<2>>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(10, 11, 12));

    MockRootGraph root{gb, MIN_ST, MAX_ET};
    auto          graph = root.graph();

    NodeView take_node = find_recordable_node(graph);
    REQUIRE(take_node.valid());
    Out<TS<Int>>{take_node.recordable_state(MIN_ST), MIN_ST}.set(Int{1});

    graph.start(MIN_ST);
    graph.evaluate(MIN_ST);
    graph.evaluate(MIN_ST + MIN_TD);
    graph.evaluate(MIN_ST + 2 * MIN_TD);
    graph.stop();

    // One of two already consumed: exactly one further tick forwards.
    CHECK_OUTPUT(get_recorded_values<Int>(graph.global_state(), "out"), values<Int>(10));
}

TEST_CASE("audit: a restored take(reset) counter re-arms on reset only")
{
    stdlib::register_standard_operators();

    GraphBuilder gb = build_graph<TakeResetRestoreGraph<2>>();
    // x ticks every cycle; reset ticks at t1 only.
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(10, 11, 12, 13, 14));
    set_replay_values<Int>(gb.global_state(), "reset", values<Int>(none, 1, none, none, none));

    MockRootGraph root{gb, MIN_ST, MAX_ET};
    auto          graph = root.graph();

    NodeView take_node = find_recordable_node(graph);
    REQUIRE(take_node.valid());
    // Restored EXHAUSTED: forwards nothing until the reset re-arms; the
    // reset cycle itself forwards (reset+tick together = cleared before the
    // new tick, the documented convention), then exactly one more — the
    // pre-fix == guard forwarded indefinitely.
    Out<TS<Int>>{take_node.recordable_state(MIN_ST), MIN_ST}.set(Int{2});

    graph.start(MIN_ST);
    CHECK_FALSE(ts_input_active(take_node, MIN_ST));
    for (int cycle = 0; cycle < 5; ++cycle) { graph.evaluate(MIN_ST + cycle * MIN_TD); }
    // Re-exhausted after forwarding count more: passive again.
    CHECK_FALSE(ts_input_active(take_node, MIN_ST + 4 * MIN_TD));
    graph.stop();

    CHECK_OUTPUT(get_recorded_values<Int>(graph.global_state(), "out"),
                 values<Int>(none, 11, 12));
}

TEST_CASE("audit: a reset does not resurrect a zero-count take")
{
    stdlib::register_standard_operators();

    GraphBuilder gb = build_graph<TakeResetRestoreGraph<0>>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(10, 11, 12));
    set_replay_values<Int>(gb.global_state(), "reset", values<Int>(none, 1, none));

    MockRootGraph root{gb, MIN_ST, MAX_ET};
    auto          graph = root.graph();

    NodeView take_node = find_recordable_node(graph);
    REQUIRE(take_node.valid());

    graph.start(MIN_ST);
    CHECK_FALSE(ts_input_active(take_node, MIN_ST));
    graph.evaluate(MIN_ST);
    graph.evaluate(MIN_ST + MIN_TD);   // the reset cycle
    // THE regression: the pre-fix eval re-armed ts here and then returned on
    // the zero-count guard, leaving the node permanently scheduled. Empty
    // output alone cannot distinguish that — the input must stay passive.
    CHECK_FALSE(ts_input_active(take_node, MIN_ST + MIN_TD));
    graph.evaluate(MIN_ST + 2 * MIN_TD);
    CHECK_FALSE(ts_input_active(take_node, MIN_ST + 2 * MIN_TD));
    graph.stop();

    CHECK_OUTPUT(get_recorded_values<Int>(graph.global_state(), "out"), values<Int>());
}

TEST_CASE("audit: zero-count take passivates at start and emits nothing")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::take>(values<Int>(1, 2, 3), Int{0}),
                 values<Int>(none, none, none));

    // The passivation itself (empty output alone also held pre-fix, when the
    // node stayed scheduled on every source tick doing nothing).
    GraphBuilder gb = build_graph<TakeRestoreGraph<0>>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(10, 11));

    MockRootGraph root{gb, MIN_ST, MAX_ET};
    auto          graph = root.graph();

    NodeView take_node = find_recordable_node(graph);
    REQUIRE(take_node.valid());

    graph.start(MIN_ST);
    CHECK_FALSE(ts_input_active(take_node, MIN_ST));
    graph.evaluate(MIN_ST);
    graph.evaluate(MIN_ST + MIN_TD);
    CHECK_FALSE(ts_input_active(take_node, MIN_ST + MIN_TD));
    graph.stop();

    CHECK_OUTPUT(get_recorded_values<Int>(graph.global_state(), "out"), values<Int>());
}

TEST_CASE("audit: the typed fast write engages for a pure-native scalar output")
{
    using namespace hgraph;

    auto node = NodeBuilder{}.label("probe").implementation<FastWriteProbe>().make_node();
    auto view = node.view();
    view.start(MIN_ST);
    view.evaluate(MIN_ST);
    CHECK(node.view().output(MIN_ST).value().checked_as<Int>() == 7);

    // The property the erased fallback silently hides: mutable_value() must
    // yield a MUTATION-access typed slot for a pure-native scalar output —
    // a Writable-tagged view makes try_mutable_as fail and every scalar set
    // pays the failed probe instead of the fast path (review finding on the
    // first cut of this change).
    auto output   = node.view().output(MIN_ST + MIN_TD);
    auto mutation = output.begin_mutation(MIN_ST + MIN_TD);
    auto direct   = mutation.mutable_value();
    REQUIRE(direct.valid());
    Int *slot = direct.try_mutable_as<Int>();
    REQUIRE(slot != nullptr);
    *slot = Int{11};
    mutation.mark_modified();
    CHECK(node.view().output(MIN_ST + MIN_TD).value().checked_as<Int>() == 11);
    CHECK(node.view().output(MIN_ST + MIN_TD).modified());
}
