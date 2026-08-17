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

    CHECK(config(state).backend == std::string{MEMORY});
    CHECK(backend_is(state, MEMORY));

    // Backend selection is OPEN (RFC 0025): unknown identifiers store as
    // given; a missing implementation surfaces at overload resolution.
    set_config(state, RecordReplayConfig{.backend = "acme.custom"});
    CHECK(config(state).backend == "acme.custom");
    CHECK(backend_is(state, "acme.custom"));
    CHECK_FALSE(backend_is(state, MEMORY));

    CHECK_THROWS_AS(set_config(state, RecordReplayConfig{.backend = ""}), std::invalid_argument);

    hgraph::GlobalState other;
    CHECK(config(other.view()).backend == std::string{MEMORY});
}

TEST_CASE("record/replay config: legacy model names normalise to backend ids")
{
    using namespace hgraph::record_replay;
    hgraph::GlobalContext context;
    const auto            state = context.state().view();

    set_config(state, RecordReplayConfig{.backend = "InMemory"});
    CHECK(config(state).backend == std::string{MEMORY});

    set_config(state, RecordReplayConfig{.backend = "InMemoryDense"});
    CHECK(config(state).backend == std::string{TESTING});

    set_config(state, RecordReplayConfig{.backend = "DataFrame"});
    CHECK(config(state).backend == "hgraph.persistence.frame");
}

TEST_CASE("comparison summary: core-neutral publication and total query")
{
    using namespace hgraph::record_replay;
    hgraph::GlobalContext context;
    const auto            state = context.state().view();

    // Nothing published: nullopt, never a throw (RFC 0025).
    CHECK_FALSE(comparison_summary(state, "calc.__compare__").has_value());

    publish_comparison_summary(state, "calc.__compare__",
                               ComparisonSummary{.compared = 7, .mismatches = 2});
    const auto summary = comparison_summary(state, "calc.__compare__");
    REQUIRE(summary.has_value());
    CHECK(summary->compared == 7);
    CHECK(summary->mismatches == 2);

    // Republishing overwrites: the summary is a running total, not a log.
    publish_comparison_summary(state, "calc.__compare__",
                               ComparisonSummary{.compared = 8, .mismatches = 2});
    CHECK(comparison_summary(state, "calc.__compare__")->compared == 8);

    // Keys are independent.
    CHECK_FALSE(comparison_summary(state, "other.__compare__").has_value());
}

TEST_CASE("recovery seeds: an unrecognised backend is a pointed error")
{
    using namespace hgraph::record_replay;
    hgraph::GlobalContext context;
    const auto            state = context.state().view();
    const auto *schema = hgraph::TypeRegistry::instance().ts(
        hgraph::scalar_descriptor<hgraph::Int>::value_meta());

    // Open selection (RFC 0025): the seed resolver dispatches over exactly
    // the ids core serves; an extension-owned or unknown identifier must
    // never silently read through the transitional frame path.
    set_config(state, RecordReplayConfig{.backend = "acme.custom"});
    CHECK_THROWS_AS((void)recorded_seed_resolver(state, "book.trades", schema, MIN_ST),
                    std::runtime_error);

    // The ids core serves keep resolving (no recording -> empty seed).
    set_config(state, RecordReplayConfig{.backend = std::string{MEMORY}});
    CHECK_FALSE(recorded_seed_resolver(state, "book.trades", schema, MIN_ST).has_value());
    set_config(state, RecordReplayConfig{.backend = std::string{TESTING}});
    CHECK_FALSE(recorded_seed_resolver(state, "book.trades", schema, MIN_ST).has_value());
}

TEST_CASE("comparison summary: registration survives a registry reset")
{
    using namespace hgraph::record_replay;

    // The per-test listener resets registries between cases, so each case
    // runs one process-per-case under ctest — a process-lifetime
    // registration flag passes there and breaks any same-process second
    // run. Reproduce the listener's cycle inside ONE case: registration is
    // generation-checked, so the post-reset publish must re-register.
    {
        hgraph::GlobalContext context;
        publish_comparison_summary(context.state().view(), "a.__compare__",
                                   ComparisonSummary{.compared = 1, .mismatches = 0});
    }
    hgraph::reset_all_registries();
    {
        hgraph::GlobalContext context;
        const auto            state = context.state().view();
        publish_comparison_summary(state, "b.__compare__",
                                   ComparisonSummary{.compared = 2, .mismatches = 1});
        const auto summary = comparison_summary(state, "b.__compare__");
        REQUIRE(summary.has_value());
        CHECK(summary->compared == 2);
        CHECK(summary->mismatches == 1);
    }
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
