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

    /** replay "x" -> take(2) -> record "out": the graph the restored-state
        regression drives directly (start/evaluate on the mock root). */
    struct TakeRestoreGraph
    {
        static constexpr auto name = "audit_take_restore_graph";

        static void compose(Wiring &w)
        {
            auto x = wire<stdlib::replay_impl, TS<Int>>(w, Str{"x"});
            wire<stdlib::dense_record_impl>(w, wire<stdlib::take>(w, x, Int{2}), Str{"out"});
        }
    };
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

    GraphBuilder gb = build_graph<TakeRestoreGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(10, 11, 12));

    MockRootGraph root{gb, MIN_ST, MAX_ET};
    auto          graph = root.graph();

    // Simulate a recovery restore: seed the take node's recordable counter
    // to its exhausted value BEFORE start. Activation is not recorded, so
    // the start hook must derive passivation from the restored state — the
    // pre-fix node forwarded indefinitely (take_reset) or leaked one tick
    // (take).
    NodeView take_node;
    for (std::size_t index = 0; index < graph.node_count(); ++index)
    {
        auto node = graph.node_at(index);
        if (node.has_recordable_state())
        {
            take_node = std::move(node);
            break;
        }
    }
    REQUIRE(take_node.valid());
    Out<TS<Int>>{take_node.recordable_state(MIN_ST), MIN_ST}.set(Int{2});

    graph.start(MIN_ST);
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

    GraphBuilder gb = build_graph<TakeRestoreGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(10, 11, 12));

    MockRootGraph root{gb, MIN_ST, MAX_ET};
    auto          graph = root.graph();

    NodeView take_node;
    for (std::size_t index = 0; index < graph.node_count(); ++index)
    {
        auto node = graph.node_at(index);
        if (node.has_recordable_state())
        {
            take_node = std::move(node);
            break;
        }
    }
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
