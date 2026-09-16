// Runtime diagnostics: GraphView::dump() and root-boundary node identity on
// escaping exceptions (docs/source/developer_guide/architecture.rst,
// Diagnostics). Exceptions caught by per-node capture / try_except_ keep their
// original message — that contract is covered by test_error_handling.cpp; here
// we cover the annotate-once-at-the-root path.

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <string>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct ThrowOnNegative
    {
        static constexpr auto name = "introspection_thrower";
        static void           eval(In<"x", TS<Int>> x, Out<TS<Int>> out)
        {
            if (x.value() < 0) { throw std::runtime_error("boom"); }
            out.set(x.value() * 2);
        }
    };

    struct ThrowingGraph
    {
        static constexpr auto name = "introspection_throwing_graph";
        static void           compose(Wiring &w)
        {
            auto x = wire<stdlib::replay_impl, TS<Int>>(w, Str{"x"});
            wire<stdlib::dense_record_impl>(w, wire<ThrowOnNegative>(w, x), Str{"out"});
        }
    };
}  // namespace

TEST_CASE("diagnostics: an exception escaping the root graph names the throwing node")
{
    GraphBuilder gb = build_graph<ThrowingGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(5, -3));

    using Catch::Matchers::ContainsSubstring;
    CHECK_THROWS_WITH(run_graph(std::move(gb), MIN_ST, MAX_ET),
                      ContainsSubstring("node[") && ContainsSubstring("introspection_thrower") &&
                          ContainsSubstring("boom"));
}

TEST_CASE("diagnostics: GraphView::dump lists every node with its schedule state")
{
    GraphBuilder gb = build_graph<ThrowingGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(5));

    GraphExecutorValue ex = run_graph(std::move(gb), MIN_ST, MAX_ET);

    const std::string dump = ex.view().graph().dump();
    using Catch::Matchers::ContainsSubstring;
    CHECK_THAT(dump, ContainsSubstring("nodes="));
    CHECK_THAT(dump, ContainsSubstring("node[0"));
    CHECK_THAT(dump, ContainsSubstring("introspection_thrower"));
    CHECK_THAT(dump, ContainsSubstring("scheduled="));
}

TEST_CASE("debug descriptors navigate graph node allocations")
{
    using namespace hgraph;

    GraphBuilder builder = build_graph<ThrowingGraph>();
    const GraphTypeRef graph_type = builder.root_type();
    const DebugDescriptor *graph_debug = graph_type.record()->debug;
    REQUIRE(graph_debug != nullptr);
    REQUIRE(graph_debug->valid());
    REQUIRE(graph_debug->layout == DebugLayoutKind::Graph);
    REQUIRE(graph_debug->field_count == builder.node_count());

    for (std::size_t index = 0; index < builder.node_count(); ++index)
    {
        const NodeTypeRef node_type = builder.nodes()[index].type();
        REQUIRE(graph_debug->fields[index].type == node_type.record());
        REQUIRE(graph_debug->fields[index].offset < graph_type.plan()->layout.size);
        REQUIRE(node_type.record()->debug != nullptr);
        REQUIRE(node_type.record()->debug->valid());
        REQUIRE(node_type.record()->debug->layout == DebugLayoutKind::Node);
    }
}

TEST_CASE("checkpoint preparation cleanup detaches unstarted runtime bindings without user hooks", "[checkpoint][graph]")
{
    using namespace hgraph;
    const auto *ts = schema_descriptor<TS<Int>>::ts_meta();
    const auto *ref = schema_descriptor<REF<TS<Int>>>::ts_meta();
    const auto *inputs = TypeRegistry::instance().un_named_tsb({{"ts", ts}});
    std::size_t starts = 0, stops = 0;
    NodeTypeDescriptor consumer;
    consumer.schema.display_name = "prepared_forwarding_consumer";
    consumer.schema.input_schema = inputs;
    consumer.schema.output_schema = ts;
    consumer.schema.output_endpoint_schema = forwarding_output_endpoint_schema(ts);
    consumer.callbacks.start = [&](const NodeView &, DateTime) { ++starts; };
    consumer.callbacks.stop = [&](const NodeView &, DateTime) { ++stops; };
    auto consumer_builder = NodeBuilder::from_descriptor(std::move(consumer));
    consumer_builder.input_endpoint(TSEndpointSchema::non_peered(inputs, {TSEndpointSchema::peered(ts)}));
    NodeTypeMetaData source;
    source.display_name = "prepared_source";
    source.output_schema = ts;
    NodeTypeMetaData reference;
    reference.display_name = "prepared_reference";
    reference.output_schema = ref;
    GraphBuilder builder;
    builder.add_node(std::move(consumer_builder));
    builder.add_node(NodeBuilder::native(std::move(source), {}));
    builder.add_node(NodeBuilder::native(std::move(reference), {}));
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder));
    auto executor = executor_builder.make_executor();
    auto graph = executor.view().graph();
    REQUIRE_FALSE(graph.started());
    auto producer = graph.node_at(1).output(MIN_ST);
    const Value initial{Int{7}};
    REQUIRE(producer.begin_mutation(MIN_ST).copy_value_from(initial.view()));
    auto input = graph.node_at(0).input(MIN_ST).indexed_child_at(0);
    input.bind_output(producer);
    auto forwarded = graph.node_at(0).output(MIN_ST);
    forwarded.bind_forwarding_target(producer);
    auto reference_output = graph.node_at(2).output(MIN_ST);
    const Value reference_value{TimeSeriesReference{producer}};
    REQUIRE(reference_output.begin_mutation(MIN_ST).copy_value_from(reference_value.view()));
    const auto alternative = reference_output.binding_for(*ts);
    REQUIRE(alternative.view(MIN_ST).valid());

    graph.discard_checkpoint_preparation(MIN_ST);
    CHECK_FALSE(input.bound());
    CHECK_FALSE(forwarded.forwarding_bound());
    CHECK_FALSE(alternative.view(MIN_ST).forwarding_bound());
    CHECK(producer.value().checked_as<Int>() == 7);
    CHECK(producer.last_modified_time() == MIN_ST);
    CHECK(starts == 0);
    CHECK(stops == 0);
    CHECK_FALSE(graph.started());
}
