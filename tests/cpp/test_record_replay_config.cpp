#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/registry_reset.h>

#include <catch2/catch_test_macros.hpp>

// Step 2 of the record/replay/table design record: explicit wiring-time
// RecordReplayConfig (P2), the mode scope (P3), and graph traits (P5) with
// parent-chained runtime lookup + fq recordable-id resolution.

namespace
{
    using namespace hgraph;

    struct TickSource
    {
        static constexpr auto name              = "trait_tick_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<Int>> out) { out.set(Int{1}); }
    };

    struct TraitGraph
    {
        static constexpr auto name = "trait_graph";

        static void compose(Wiring &w)
        {
            w.set_trait(std::string{record_replay::RECORDABLE_ID_TRAIT}, Value{Str{"book.trades"}});
            w.set_trait("region", Value{Str{"emea"}});
            auto src = wire<TickSource>(w);
            wire<stdlib::null_sink>(w, src);
        }
    };
}  // namespace

TEST_CASE("record/replay config: configuration belongs to the seeded GlobalState")
{
    using namespace hgraph::record_replay;
    hgraph::GlobalContext context;
    const auto            state = context.state().view();

    CHECK(config(state).model == std::string{IN_MEMORY});
    CHECK(config(state).date_key == "__date_time__");
    CHECK(model_is(state, IN_MEMORY));

    set_config(state, Config{.model = "Arrow", .date_key = "dt", .as_of_key = "asof", .as_of = MIN_ST});
    CHECK(config(state).model == "Arrow");
    CHECK(model_is(state, "Arrow"));
    CHECK_FALSE(model_is(state, IN_MEMORY));
    CHECK(config(state).as_of == MIN_ST);

    CHECK_THROWS_AS(set_config(state, Config{.model = ""}), std::invalid_argument);

    hgraph::GlobalState other;
    CHECK(config(other.view()).model == std::string{IN_MEMORY});
    CHECK_FALSE(config(other.view()).as_of.has_value());
}

TEST_CASE("record/replay mode scope: nesting shadows, nearest wins, pops restore")
{
    using namespace hgraph::record_replay;

    CHECK(current_scope().mode == Mode::None);
    {
        scope outer{Mode::Record, "outer"};
        CHECK(has_mode(current_scope().mode, Mode::Record));
        CHECK(current_scope().recordable_id == "outer");
        {
            scope inner{Mode::Replay | Mode::Compare, "inner"};
            CHECK(has_mode(current_scope().mode, Mode::Replay));
            CHECK(has_mode(current_scope().mode, Mode::Compare));
            CHECK_FALSE(has_mode(current_scope().mode, Mode::Record));
            CHECK(current_scope().recordable_id == "inner");
        }
        CHECK(current_scope().recordable_id == "outer");
    }
    CHECK(current_scope().mode == Mode::None);
    CHECK_FALSE(has_mode(Mode::None, Mode::None));   // None never "matches"
}

TEST_CASE("graph traits: wiring-time traits are readable on the running graph")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphExecutorValue executor = testing::run_graph(build_graph<TraitGraph>());
    const GraphView   &graph    = executor.view().graph();

    CHECK(graph.trait_or("region").checked_as<Str>() == Str{"emea"});
    CHECK_FALSE(graph.trait_or("missing").valid());

    CHECK(record_replay::has_recordable_id(graph));
    CHECK(record_replay::fq_recordable_id(graph, "") == "book.trades");
    CHECK(record_replay::fq_recordable_id(graph, "fills") == "book.trades.fills");
}

TEST_CASE("graph traits: nested graphs chain to the parent and can shadow")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphExecutorValue executor = testing::run_graph(build_graph<TraitGraph>());
    const GraphView   &root     = executor.view().graph();
    NodeView           anchor   = root.node_at(0);

    // A nested child with NO own traits inherits through the parent chain.
    GraphBuilder plain_child;
    GraphValue   inherited = plain_child.make_nested_graph(anchor.pointer());
    CHECK(inherited.view().trait("region").checked_as<Str>() == Str{"emea"});   // bubbles to the parent
    CHECK_FALSE(inherited.view().trait_or("region").valid());                    // own level only: absent
    CHECK(record_replay::fq_recordable_id(inherited.view(), "leg") == "book.trades.leg");

    // A child's own trait shadows the parent's.
    GraphBuilder shadowing_child;
    shadowing_child.trait(std::string{record_replay::RECORDABLE_ID_TRAIT}, Value{Str{"child"}});
    GraphValue shadowed = shadowing_child.make_nested_graph(anchor.pointer());
    CHECK(record_replay::fq_recordable_id(shadowed.view(), "leg") == "child.leg");
}

TEST_CASE("graph traits: fq id resolution without a parent trait requires a local id")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphBuilder gb;
    gb.trait("other", Value{Str{"x"}});
    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
    GraphExecutorValue executor = eb.make_executor();
    const GraphView   &graph    = executor.view().graph();

    CHECK_FALSE(record_replay::has_recordable_id(graph));
    CHECK(record_replay::fq_recordable_id(graph, "solo") == "solo");
    CHECK_THROWS_AS(record_replay::fq_recordable_id(graph, ""), std::runtime_error);
}
