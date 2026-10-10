/**
 * RFC 0008 stage 6: resolved facts on prepared routes.
 *
 * A static node reading a native atomic input through a prepared route, and
 * writing its native atomic output through the prepared output route, must
 * behave exactly as the resolving paths do: validity, value, modified-ness
 * on ticking and non-ticking cycles, and observer notification of the write.
 */
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

namespace
{
    using namespace hgraph;

    // Ticks 1, 2, 3 on consecutive cycles, then stops scheduling.
    struct Pulse
    {
        static constexpr auto name              = "stage6_pulse";
        static constexpr bool schedule_on_start = true;

        static void start(State<Int> count) { count.set(Int{0}); }

        static void eval(NodeScheduler sched, State<Int> count, Out<TS<Int>> out)
        {
            Int &value = count.modify();
            value += 1;
            out.set(value);
            if (value < 3) { sched.schedule(MIN_TD); }
        }
    };

    // Doubles its input; its input route is native (TS<Int> bound to a node root output).
    struct Doubler
    {
        static constexpr auto name = "stage6_doubler";

        static void eval(In<"x", TS<Int>> x, Out<TS<Int>> out) { out.set(x.value() * 2); }
    };

    // Re-arms itself every cycle and encodes what it observes on its PASSIVE
    // input: bit 0 = valid, bit 1 = modified this cycle; accumulates the
    // per-cycle codes into a base-4 digit string in its state.
    struct Observer
    {
        static constexpr auto name              = "stage6_observer";
        static constexpr bool schedule_on_start = true;

        static void start(State<Int> trail) { trail.set(Int{0}); }

        static void eval(In<"x", TS<Int>, InputValidity::Unchecked, InputActivity::Passive> x,
                         NodeScheduler sched, State<Int> trail, Out<TS<Int>> out)
        {
            Int code = 0;
            if (x.valid()) { code += 1; }
            if (x.modified()) { code += 2; }
            Int &digits = trail.modify();
            digits = digits * 4 + code;
            out.set(digits);
            if (digits < 4 * 4 * 4 * 4) { sched.schedule(MIN_TD); }
        }
    };
    // Lifted kernels wired through the operator syntax: an Int add (a
    // trivially copyable native atomic result) and a Str add (a native
    // atomic whose assignment is not trivial).
    struct LiftedIntAddG
    {
        static constexpr auto name = "stage6_lifted_int_add_g";

        static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            using namespace hgraph::stdlib::syntax;
            return (lhs + rhs).as<TS<Int>>();
        }
    };

    struct LiftedStrAddG
    {
        static constexpr auto name = "stage6_lifted_str_add_g";

        static Port<TS<Str>> compose(Wiring &, Port<TS<Str>> lhs, Port<TS<Str>> rhs)
        {
            using namespace hgraph::stdlib::syntax;
            return (lhs + rhs).as<TS<Str>>();
        }
    };
}  // namespace

TEST_CASE("prepared routes: lifted kernels write through the prepared output route", "[rfc0008][stage6]")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    // One input ticking while the other is quiet re-evaluates the kernel
    // with the retained value; a cycle where neither ticks produces no
    // output tick (the route's record_modified is the same commit the
    // mutation scope performs).
    CHECK_OUTPUT((eval_node<LiftedIntAddG>(values<Int>(1, 2, none, none, 5),
                                           values<Int>(10, none, 30, none, none))),
                 values<Int>(11, 12, 32, none, 35));

    CHECK_OUTPUT((eval_node<LiftedStrAddG>(values<Str>(Str{"a"}, none, Str{"ccc"}),
                                           values<Str>(Str{"b"}, Str{"bb"}, none))),
                 values<Str>(Str{"ab"}, Str{"abb"}, Str{"cccbb"}));
}

TEST_CASE("prepared routes: native input reads and output writes match the resolving paths", "[rfc0008][stage6]")
{
    using namespace hgraph;
    reset_all_registries();

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.label("pulse").implementation<Pulse>())
        .add_node(NodeBuilder{}.label("double").implementation<Doubler>())
        .add_node(NodeBuilder{}.label("observe").implementation<Observer>())
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}})
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 2, .target_path = {0}});

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 3);
    const auto doubled = graph.node_at(1).output(MIN_ST + 2 * MIN_TD);
    CHECK(doubled.valid());
    CHECK(doubled.value().checked_as<Int>() == Int{6});          // 3 * 2 on the last pulse
    CHECK(doubled.last_modified_time() == MIN_ST + 2 * MIN_TD);  // written through the prepared output route

    // Observer cycles: t0 (pulse 1: valid+modified = 3), t1 (3), t2 (3),
    // t3 (valid, not modified = 1), t4 (1) -> digits 3,3,3,1,1 base 4.
    const auto trail = graph.node_at(2).output(MIN_ST + 4 * MIN_TD);
    CHECK(trail.valid());
    CHECK(trail.value().checked_as<Int>() == Int{(((3 * 4 + 3) * 4 + 3) * 4 + 1) * 4 + 1});
}
