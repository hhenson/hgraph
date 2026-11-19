#include "../../src/hgraph/runtime/window_expiry_service.h"

#include <hgraph/manifest/graph_manifest.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/types/static_node.h>
#include <catch2/catch_test_macros.hpp>

#include <array>
#include <optional>

using namespace hgraph;

namespace
{
    struct Registration
    {
        explicit Registration(const GraphView &root)
            : alarm(std::in_place, root, this, expire, refresh) {}

        std::optional<WindowExpiryAlarm> alarm;
        Registration *cancel_with_self{nullptr};
        int fired{};
        int refreshed{};

        static void expire(void *owner, DateTime)
        {
            auto &self = *static_cast<Registration *>(owner);
            ++self.fired;
            if (self.cancel_with_self)
            {
                self.cancel_with_self->alarm.reset();
                self.alarm.reset();
            }
        }

        static void refresh(void *owner) { ++static_cast<Registration *>(owner)->refreshed; }
    };
}

TEST_CASE("window expiry queue reprioritizes live owners and cancels a due peer during dispatch")
{
    GraphExecutorBuilder builder;
    builder.graph_builder(GraphBuilder{}).mode(GraphExecutorMode::ExternallyDriven);
    auto executor = builder.make_executor();
    auto engine = executor.view();
    engine.start_external(MIN_ST);
    auto root = engine.graph();
    Registration first{root}, peer{root}, survivor{root};
    first.cancel_with_self = &peer;

    const auto due = MIN_ST + 2 * MIN_TD;
    const auto later = due + MIN_TD;
    first.alarm->set_deadline(due);
    peer.alarm->set_deadline(due);
    survivor.alarm->set_deadline(due - MIN_TD);
    REQUIRE(root.next_scheduled_time() == due - MIN_TD);
    survivor.alarm->set_deadline(later);
    REQUIRE(root.next_scheduled_time() == due);
    peer.alarm->request_refresh();

    REQUIRE(engine.step(due));
    CHECK(first.fired == 1);
    CHECK(peer.fired == 0);
    CHECK(first.refreshed == 0);
    CHECK(peer.refreshed == 0);
    REQUIRE(root.next_scheduled_time() == later);
    REQUIRE(engine.step(later));
    CHECK(survivor.fired == 1);
    CHECK(survivor.refreshed == 1);
    CHECK(root.next_scheduled_time() == MAX_DT);
    engine.stop_external();
}

namespace
{
    using Window = TSWDuration<Int, 2, 0>;

    struct ManualWindow
    {
        static constexpr auto name = "manual_expiry_window";
        static void eval(In<"a", TS<Int>>, In<"b", TS<Int>>, Out<Window>) {}
    };

    struct ReplayedWindow : ManualWindow
    {
        static constexpr bool replays_output_events = true;
    };
}

TEST_CASE("window replay policy remains attached to the builder through annotation and specialization")
{
    NodeBuilder node;
    node.implementation<ReplayedWindow>();
    REQUIRE(node.replays_output_events());
    node.label("source").checkpoint_identity(NodeCheckpointIdentity{.id = "source-id"});
    auto shared = node;
    shared.label("").label("clone").checkpoint_identity({}).checkpoint_identity(NodeCheckpointIdentity{.id = "clone-id"});
    CHECK(node.label() == "source");
    CHECK(node.checkpoint_identity().id == "source-id");
    CHECK(shared.replays_output_events());
    CHECK(shared.label() == "clone");
    CHECK(shared.checkpoint_identity().id == "clone-id");

    // A new implementation takes its own default. An explicitly set value
    // (including false) survives both concrete and resolved implementation changes.
    shared.implementation<ManualWindow>();
    CHECK_FALSE(shared.replays_output_events());
    CHECK(shared.checkpoint_identity().id == "clone-id");
    shared.replays_output_events(false).implementation<ReplayedWindow>(ResolutionMap{});
    CHECK_FALSE(shared.replays_output_events());
    shared.replays_output_events(true);
    const std::array<std::size_t, 1> passive{0};
    const auto rebound = shared.with_passive_inputs(passive).with_error_capture(node_error_ts_meta());
    CHECK(rebound.replays_output_events());
    CHECK(rebound.label() == "clone");
    CHECK(rebound.checkpoint_identity().id == "clone-id");
}

TEST_CASE("a shared graph type uses the running window owner's replay policy and forwarding mode")
{
    NodeBuilder native;
    native.implementation<ManualWindow>();
    auto replay = native;
    replay.replays_output_events(true);
    auto forwarded = native;
    forwarded.output_endpoint(TSEndpointSchema::peered(native.type().schema()->output_schema));

    for (bool forwarded_first : {false, true})
    {
        const auto label = forwarded_first ? "forwarded first" : "local first";
        const auto make_graph = [&](const NodeBuilder &node) {
            GraphBuilder graph;
            graph.label(label).add_node(node);
            return graph;
        };
        auto local = make_graph(native);
        auto upstream = make_graph(replay);
        auto borrowed = make_graph(forwarded);
        auto first_type = forwarded_first ? borrowed.type() : local.type();
        REQUIRE(first_type == local.type());
        REQUIRE(first_type == upstream.type());
        REQUIRE(first_type == borrowed.type());
        CHECK(manifest::capture(local).id() != manifest::capture(upstream).id());

        for (const GraphBuilder *graph : {&borrowed, &upstream, &local})
        {
            GraphExecutorBuilder builder;
            builder.graph_builder(*graph).mode(GraphExecutorMode::ExternallyDriven);
            auto executor = builder.make_executor();
            auto engine = executor.view();
            engine.start_external(MIN_ST);
            auto root = engine.graph();
            auto output = root.node_at(0).output(MIN_ST);
            CHECK(output.forwarding() == (graph == &borrowed));
            if (!output.forwarding())
            {
                Value sample{Int{1}};
                output.data_view().as_window().begin_mutation(MIN_ST).push(sample.view());
            }
            const auto deadline = MIN_ST + 3 * MIN_TD;
            REQUIRE(root.next_scheduled_time() == (graph == &local ? deadline : MAX_DT));
            if (graph == &local)
            {
                REQUIRE(engine.step(deadline));
                CHECK(output.data_view().as_window().empty());
            }
            engine.stop_external();
        }
    }
}
