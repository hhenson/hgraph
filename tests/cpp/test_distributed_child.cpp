// The boundary of a distributed child graph (RFC 0037, stage 2).
//
// A child in another process cannot bind its inputs to the caller's outputs,
// so each boundary argument becomes a local pull source the driver stages into
// and each result a local sink it reads back. Every test below is the same
// assertion the RFC makes central: a child driven that way produces exactly
// what the same graph produces run normally.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/distributed_child.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <optional>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::distributed;

    /** Per-key state lives in the child, so a lost or repeated cycle shows. */
    struct RunningTotal
    {
        static constexpr auto name = "child_running_total";

        static void eval(In<"ts", TS<Int>> ts, State<Int> total, Out<TS<Int>> out)
        {
            total.modify() += ts.value();
            out.set(total.get());
        }
    };

    /** The child as it runs in a worker: staged in, computed, captured out. */
    struct BoundaryChildGraph
    {
        static constexpr auto name = "boundary_child_graph";
        static void           compose(Wiring &w)
        {
            auto in    = wire<boundary_source_impl, TS<Int>>(w, Str{"in"});
            auto total = wire<RunningTotal>(w, in);
            wire<boundary_sink_impl>(w, total, Str{"out"});
        }
    };

    /** The same computation wired normally, as the reference. */
    struct LocalChildGraph
    {
        static constexpr auto name = "local_child_graph";
        static void           compose(Wiring &w)
        {
            auto src   = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto total = wire<RunningTotal>(w, src);
            wire<stdlib::dense_record_impl>(w, total, Str{"out"});
        }
    };

    const DateTime test_end = MIN_ST + TimeDelta{1000};

    std::vector<std::optional<Int>> run_local(const std::vector<std::optional<Int>> &values)
    {
        GraphBuilder gb = build_graph<LocalChildGraph>();
        testing::set_replay_values<Int>(gb.global_state(), "in", values);
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(test_end);
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();
        return testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out");
    }

    /** One cycle's worth of a boundary value, as the driver would send it. */
    Value int_delta(Int value)
    {
        // The staged form is the canonical delta Value for the schema, which is
        // exactly what capture_delta produces on the other side -- so what a
        // transport carries is this, and nothing else.
        return Value{value};
    }
}  // namespace

TEST_CASE("distributed child: staged inputs and collected outputs equal a local run")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    const std::vector<std::optional<Int>> values{Int{1}, Int{2}, Int{3}, Int{4}};

    DistributedChildHost host{build_graph<BoundaryChildGraph>(), test_end};
    host.start(MIN_ST);

    std::vector<std::optional<Int>> collected;
    DateTime                        when = MIN_ST;
    for (const auto &value : values)
    {
        host.stage("in", int_delta(*value).view());
        REQUIRE(host.step(when));
        Value out = host.collect("out");
        collected.push_back(out.has_value() ? std::optional<Int>{out.view().checked_as<Int>()}
                                        : std::nullopt);
        when = when + MIN_TD;
    }
    host.stop();

    CHECK(collected == run_local(values));
}

TEST_CASE("distributed child: the caller chooses the times, and they need not be dense")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    // A distributed child is evaluated when its CALLER has a cycle, which is
    // not on any grid the child would have chosen. The running total must
    // depend on the sequence of staged values and nothing else.
    DistributedChildHost host{build_graph<BoundaryChildGraph>(), test_end};
    host.start(MIN_ST);

    const std::vector<TimeDelta> gaps{TimeDelta{0}, TimeDelta{7}, TimeDelta{100}, TimeDelta{3}};
    std::vector<Int>             collected;
    DateTime                     when = MIN_ST;
    for (std::size_t i = 0; i < gaps.size(); ++i)
    {
        when = when + gaps[i];
        host.stage("in", int_delta(static_cast<Int>(i + 1)).view());
        REQUIRE(host.step(when));
        Value out = host.collect("out");
        REQUIRE(out.has_value());
        collected.push_back(out.view().checked_as<Int>());
    }
    host.stop();

    CHECK(collected == std::vector<Int>{1, 3, 6, 10});
}

TEST_CASE("distributed child: a cycle with nothing staged does not tick")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    // The caller evaluates the child because ITS graph had a cycle, often with
    // nothing for this child at all. Scheduling the boundary sources anyway
    // must not invent a tick -- which is why a stage is consumed on apply.
    DistributedChildHost host{build_graph<BoundaryChildGraph>(), test_end};
    host.start(MIN_ST);

    host.stage("in", int_delta(5).view());
    REQUIRE(host.step(MIN_ST));
    REQUIRE(host.collect("out").has_value());

    REQUIRE(host.step(MIN_ST + TimeDelta{1}));
    CHECK_FALSE(host.collect("out").has_value());   // no stage, no output
    REQUIRE(host.step(MIN_ST + TimeDelta{2}));
    CHECK_FALSE(host.collect("out").has_value());

    // and the state is intact: the next staged value continues the total.
    host.stage("in", int_delta(4).view());
    REQUIRE(host.step(MIN_ST + TimeDelta{3}));
    Value out = host.collect("out");
    REQUIRE(out.has_value());
    CHECK(out.view().checked_as<Int>() == 9);
    host.stop();
}

TEST_CASE("distributed child: the child's own schedule is reported and honoured")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    // With nothing staged and nothing self-scheduled, the child wants nothing;
    // preparing a cycle is what makes it due, and it is due at the time the
    // caller asked for.
    DistributedChildHost host{build_graph<BoundaryChildGraph>(), test_end};
    host.start(MIN_ST);
    CHECK(host.next_scheduled_time() == MAX_DT);

    host.stage("in", int_delta(1).view());
    REQUIRE(host.step(MIN_ST));
    CHECK(host.next_scheduled_time() == MAX_DT);   // nothing outstanding
    host.stop();
}
