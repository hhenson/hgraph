// Tests for the in-memory testing toolkit: the replay source and record sink,
// and their shared cycle-aligned List<Any> buffer over the GlobalState.
//
// The end-to-end test wires replay -> add_one -> record, seeds the input buffer
// on the builder, runs the executor, and reads the recorded per-cycle output back
// out of the graph's GlobalState.

#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;

    struct AddOne
    {
        static constexpr auto name = "add_one";
        static void           eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value() + 1); }
    };

    struct ReplayRecordGraph
    {
        static constexpr auto name = "replay_record_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, TS<Int>>(w, std::string{"in"});
            auto inc = wire<AddOne>(w, src);
            wire<stdlib::dense_record_impl>(w, inc, std::string{"out"});
        }
    };

    // A source that initiates its single tick 3 cycles after start using the
    // lightweight SingleShotScheduler (no per-node scheduler state).
    struct DelayedSource
    {
        static constexpr auto name = "delayed_source";
        static void           start(SingleShotScheduler sched) { sched.schedule(TimeDelta{3}); }
        static void           eval(Out<TS<Int>> out) { out.set(Int{99}); }
    };

    struct DelayedGraph
    {
        static constexpr auto name = "delayed_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<DelayedSource>(w);
            wire<stdlib::dense_record_impl>(w, src, std::string{"out"});
        }
    };
}  // namespace

TEST_CASE("testing helpers: set_replay_values / get_recorded_values round-trip")
{
    using namespace hgraph;

    GlobalState gs;
    testing::set_replay_values<Int>(gs.view(), "buf", {1, std::nullopt, 3});

    const auto back = testing::get_recorded_values<Int>(gs.view(), "buf");
    REQUIRE(back.size() == 3);
    CHECK(back[0] == std::optional<Int>{1});
    CHECK(back[1] == std::nullopt);
    CHECK(back[2] == std::optional<Int>{3});
}

TEST_CASE("testing: replay -> add_one -> record captures the per-cycle output")
{
    using namespace hgraph;

    GraphBuilder gb = build_graph<ReplayRecordGraph>();
    // Seed the input on the builder (carried onto the graph at make_graph):
    // tick 1 at cycle 0, no tick at cycle 1, tick 3 at cycle 2.
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, std::nullopt, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});

    GraphExecutorValue executor = eb.make_executor();
    auto               view     = executor.view();
    view.run();

    // add_one shifts each tick by one; the skipped cycle stays skipped.
    const auto out = testing::get_recorded_values<Int>(view.graph().global_state(), "out");
    REQUIRE(out.size() == 3);
    CHECK(out[0] == std::optional<Int>{2});
    CHECK(out[1] == std::nullopt);
    CHECK(out[2] == std::optional<Int>{4});
}

TEST_CASE("testing: SingleShotScheduler schedules a delayed first tick with no scheduler state")
{
    using namespace hgraph;

    GraphBuilder gb = build_graph<DelayedGraph>();

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue executor = eb.make_executor();
    auto               view     = executor.view();
    view.run();

    // The single tick lands at cycle offset 3 (start + 3); 0..2 are skipped.
    const auto out = testing::get_recorded_values<Int>(view.graph().global_state(), "out");
    REQUIRE(out.size() == 4);
    CHECK(out[0] == std::nullopt);
    CHECK(out[1] == std::nullopt);
    CHECK(out[2] == std::nullopt);
    CHECK(out[3] == std::optional<Int>{99});

    // SingleShotScheduler is stateless: the source carries no scheduler component.
    CHECK_FALSE(view.graph().node_at(0).has_scheduler());
}
