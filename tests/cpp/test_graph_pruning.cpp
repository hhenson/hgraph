#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/subgraph_wiring.h>

#include <catch2/catch_test_macros.hpp>

#include <stdexcept>

namespace
{
    using namespace hgraph;

    struct KeepSink
    {
        static void eval(In<"value", TS<Int>>) {}
    };

    struct UnusedSource
    {
        static constexpr auto name = "unused_source";
        static constexpr bool schedule_on_start = true;
        static void start() { throw std::runtime_error("unused source started"); }
        static void eval(Out<TS<Int>>) { throw std::runtime_error("unused source evaluated"); }
        static void stop() { throw std::runtime_error("unused source stopped"); }
    };

    struct UnusedCompute
    {
        static constexpr auto name = "unused_compute";
        static void start() { throw std::runtime_error("unused compute started"); }
        static void eval(In<"value", TS<Int>>, Out<TS<Int>>)
        {
            throw std::runtime_error("unused compute evaluated");
        }
        static void stop() { throw std::runtime_error("unused compute stopped"); }
    };

    struct PrunedBranch
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            wire<UnusedCompute>(w, wire<UnusedSource>(w));
            wire<UnusedCompute>(w, value);
            return value;
        }
    };

    struct NestedPrunedBranch
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            return nested_<PrunedBranch>(w, value);
        }
    };

    struct SinkAndReturn
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            wire<UnusedSource>(w);
            wire<KeepSink>(w, value);
            return wire<stdlib::add_>(w, value, Int{1}).as<TS<Int>>();
        }
    };

    struct NestedSinkAndReturn
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            return nested_<SinkAndReturn>(w, value);
        }
    };
}

TEST_CASE("graph pruning: unused branches do not enter the runtime")
{
    CHECK_OUTPUT(hgraph::testing::eval_node<PrunedBranch>(hgraph::testing::values<hgraph::Int>(1, 2)),
                 hgraph::testing::values<hgraph::Int>(1, 2));
    CHECK_OUTPUT(hgraph::testing::eval_node<NestedPrunedBranch>(hgraph::testing::values<hgraph::Int>(1, 2)),
                 hgraph::testing::values<hgraph::Int>(1, 2));
}

TEST_CASE("graph pruning: nested returns and sinks both retain their producers")
{
    hgraph::stdlib::register_standard_operators();
    CHECK_OUTPUT(hgraph::testing::eval_node<NestedSinkAndReturn>(hgraph::testing::values<hgraph::Int>(1, 2)),
                 hgraph::testing::values<hgraph::Int>(2, 3));
}

TEST_CASE("graph pruning: no sinks produces an empty graph without consuming wiring")
{
    using namespace hgraph;
    Wiring w;
    auto unused = wire<UnusedCompute>(w, wire<UnusedSource>(w));
    CHECK(w.snapshot().node_count() == 0);
    wire<KeepSink>(w, unused);
    CHECK(w.snapshot().node_count() == 3);
    CHECK(std::move(w).finish().node_count() == 3);
}

TEST_CASE("graph pruning: inputs without rank dependencies still retain producers")
{
    using namespace hgraph;
    Wiring w;
    auto source = wire<UnusedSource>(w);
    const WiringInputRef input{.source = source.erased(), .rank_dependency = false};
    const auto sink = w.add_unique_node(
        typeid(KeepSink), NodeBuilder{}.implementation<KeepSink>(),
        std::span<const WiringInputRef>{&input, 1}, Value{});
    static_cast<void>(sink);
    CHECK(std::move(w).finish().node_count() == 2);
}

TEST_CASE("graph pruning: rank-only dependencies retain required nodes")
{
    using namespace hgraph;
    Wiring w;
    auto anchor = wire<UnusedSource>(w);
    wire<KeepSink>(w, anchor);
    auto dependency = wire<UnusedCompute>(w, anchor);
    w.add_rank_dependency(anchor.node(), dependency.node());
    // The retained dependency cycle still has to be diagnosed.
    CHECK_THROWS(std::move(w).finish());
}

TEST_CASE("graph pruning: unreachable dependency cycles are discarded")
{
    using namespace hgraph;
    Wiring w;
    auto source = wire<UnusedSource>(w);
    auto computation = wire<UnusedCompute>(w, source);
    w.add_rank_dependency(source.node(), computation.node());
    CHECK(std::move(w).finish().node_count() == 0);
}

TEST_CASE("graph pruning: unused push sources are absent from the runtime prefix")
{
    using namespace hgraph;
    Wiring w;
    auto source = w.add_unique_node(
        typeid(UnusedSource), make_push_source_node(*ts_type<TS<Int>>()),
        std::span<const WiringPortRef>{}, Value{});
    CHECK(w.snapshot().node_count() == 0);
    wire<KeepSink>(w, Port<TS<Int>>{w, source});
    auto graph = std::move(w).finish();
    REQUIRE(graph.node_count() == 2);
    CHECK(graph.nodes()[0].type().schema()->node_kind == NodeKind::PushSource);
}

TEST_CASE("graph pruning: delayed structural inputs retain their producers")
{
    using namespace hgraph;
    struct ListSink
    {
        static void eval(In<"values", TSL<TS<Int>, 2>>) {}
    };
    Wiring w;
    auto unused = wire<UnusedSource>(w);
    auto live = wire<UnusedCompute>(w, unused);
    auto delayed = delayed_binding<TS<Int>>(w);
    wire<ListSink>(w, {delayed(), live});
    delayed(unused);
    CHECK(std::move(w).finish().node_count() == 3);
}

TEST_CASE("graph pruning: discarded closure consumers do not capture outer producers")
{
    using namespace hgraph;
    Wiring outer;
    auto unused = wire<UnusedSource>(outer);
    Wiring child{WiringKind::SubGraph};
    wire<UnusedCompute>(child, unused);
    auto compiled = std::move(child).finish_subgraph(std::nullopt, {});
    CHECK(compiled.graph_builder.node_count() == 0);
    CHECK(compiled.captured_inputs.empty());
    CHECK(compiled.input_bindings.empty());
}

TEST_CASE("graph pruning: explicit outer captures are compacted after pruning")
{
    using namespace hgraph;
    Wiring outer;
    auto unused = wire<UnusedSource>(outer);
    auto live = wire<UnusedCompute>(outer, unused);
    Wiring child{WiringKind::SubGraph};
    auto unused_capture = child.capture_outer_source(unused.erased());
    auto live_capture = child.capture_outer_source(live.erased());
    wire<UnusedCompute>(child, Port<TS<Int>>{child, unused_capture});
    wire<KeepSink>(child, Port<TS<Int>>{child, live_capture});
    auto compiled = std::move(child).finish_subgraph(live_capture, {});
    REQUIRE(compiled.captured_inputs.size() == 1);
    CHECK(compiled.captured_inputs[0].same_source_as(live.erased()));
    REQUIRE(compiled.input_bindings.size() == 1);
    CHECK(compiled.input_bindings[0].source_path == std::vector<std::size_t>{0});
    REQUIRE(compiled.output_binding.has_value());
    CHECK(compiled.output_binding->parent_source_path == std::vector<std::size_t>{0});
}
