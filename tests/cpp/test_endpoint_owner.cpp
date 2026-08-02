#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_registry.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <utility>

namespace
{
    hgraph::NodeBuilder source_node(const hgraph::TSValueTypeMetaData *output_schema,
                                    const char *name = "owner_source")
    {
        hgraph::NodeTypeMetaData schema;
        schema.display_name = name;
        schema.output_schema = output_schema;
        schema.node_kind = hgraph::NodeKind::PullSource;

        return hgraph::NodeBuilder::native(std::move(schema));
    }

    hgraph::NodeBuilder consumer_node(const hgraph::TSValueTypeMetaData *input_schema,
                                      const char *name = "owner_consumer")
    {
        hgraph::NodeTypeMetaData schema;
        schema.display_name = name;
        schema.input_schema = input_schema;
        schema.node_kind = hgraph::NodeKind::Compute;

        return hgraph::NodeBuilder::native(std::move(schema));
    }

    hgraph::NodeBuilder state_endpoint_node(const hgraph::TSValueTypeMetaData *ts_int)
    {
        hgraph::NodeTypeMetaData schema;
        schema.display_name = "owner_state_endpoints";
        schema.error_output_schema = ts_int;
        schema.recordable_state_schema = ts_int;

        return hgraph::NodeBuilder::native(std::move(schema));
    }
}

TEST_CASE("node output TSData can recover its owning node")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *output_schema = registry.tsb("EndpointOwnerOutput", {{"value", ts_int}});

    GraphBuilder builder;
    builder.add_node(source_node(output_schema));

    testing::MockRootGraph graph{builder};
    auto graph_view = graph.graph();
    auto source = graph_view.node_at(0);
    auto output = source.output(MIN_DT);

    REQUIRE(output.bound());
    CHECK(output.owner_port() == TSEndpointOwnerPort::Output);

    auto output_owner = output.owner_node();
    REQUIRE(output_owner.valid());
    CHECK(output_owner.node_index() == 0);
    CHECK(output_owner.graph_value() == source.graph_value());
    CHECK(output.owner_graph().data() == graph_view.data());

    auto root_owner = output.data_view().owner_node();
    REQUIRE(root_owner.valid());
    CHECK(root_owner.node_index() == 0);

    auto output_bundle = output.as_bundle();
    auto child = output_bundle.field("value");
    auto child_owner = child.data_view().owner_node();
    REQUIRE(child_owner.valid());
    CHECK(child_owner.node_index() == 0);
    CHECK(child.data_view().root_endpoint_owner().port() == TSEndpointOwnerPort::Output);
}

TEST_CASE("node input TSData can recover its consuming node")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *input_schema = registry.tsb("EndpointOwnerInput", {{"value", ts_int}});

    GraphBuilder builder;
    builder.add_node(consumer_node(input_schema));

    testing::MockRootGraph graph{builder};
    auto graph_view = graph.graph();
    auto consumer = graph_view.node_at(0);
    auto input = consumer.input(MIN_DT);

    REQUIRE(input.bound());
    CHECK(input.owner_port() == TSEndpointOwnerPort::Input);

    auto consumer_owner = input.consumer_node();
    REQUIRE(consumer_owner.valid());
    CHECK(consumer_owner.node_index() == 0);
    CHECK(consumer_owner.graph_value() == consumer.graph_value());
    CHECK(input.consumer_graph().data() == graph_view.data());

    auto root_owner = input.data_view().owner_node();
    REQUIRE(root_owner.valid());
    CHECK(root_owner.node_index() == 0);
    CHECK(input.data_view().root_endpoint_owner().port() == TSEndpointOwnerPort::Input);
}

TEST_CASE("time-series endpoint views report whether their schema is a reference")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);

    TSOutput value_output{*ts_int};
    TSOutput reference_output{*ref_int};
    CHECK_FALSE(value_output.view().is_reference());
    CHECK(reference_output.view().is_reference());

    TSInput value_input{
        TSInputBuilderFactory::checked_builder_for(*ts_int, TSEndpointSchema::peered(ts_int))};
    TSInput reference_input{
        TSInputBuilderFactory::checked_builder_for(*ref_int, TSEndpointSchema::peered(ref_int))};
    CHECK_FALSE(value_input.view().is_reference());
    CHECK(reference_input.view().is_reference());
}

TEST_CASE("bound input output owner remains the producer node")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *bundle_schema = registry.tsb("EndpointOwnerEdgeBundle", {{"value", ts_int}});

    GraphBuilder builder;
    builder.add_node(source_node(bundle_schema, "owner_edge_source"))
        .add_node(consumer_node(bundle_schema, "owner_edge_consumer"))
        .add_edge(GraphEdge{
            .source_node = 0,
            .source_path = {0},
            .target_node = 1,
            .target_path = {0},
        });

    testing::MockRootGraph graph{builder};
    auto graph_view = graph.graph();
    auto consumer = graph_view.node_at(1);
    auto input = consumer.input(MIN_DT);
    auto input_bundle = input.as_bundle();
    auto field = input_bundle.field("value");

    auto consumer_owner = field.consumer_node();
    REQUIRE(consumer_owner.valid());
    CHECK(consumer_owner.node_index() == 1);

    REQUIRE(field.bound());
    auto producer_output = field.bound_output();
    REQUIRE(producer_output.bound());

    auto producer_owner = producer_output.owner_node();
    REQUIRE(producer_owner.valid());
    CHECK(producer_owner.node_index() == 0);
    CHECK(producer_output.data_view().owner_node().node_index() == 0);
    CHECK(producer_output.owner_port() == TSEndpointOwnerPort::Output);

    auto resolved_data_owner = field.data_view().owner_node();
    REQUIRE(resolved_data_owner.valid());
    CHECK(resolved_data_owner.node_index() == 0);
}

TEST_CASE("error and recordable endpoints report their owner ports")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);

    GraphBuilder builder;
    builder.add_node(state_endpoint_node(ts_int));

    testing::MockRootGraph graph{builder};
    auto node = graph.graph().node_at(0);

    auto error_output = node.error_output(MIN_DT);
    CHECK(error_output.owner_port() == TSEndpointOwnerPort::ErrorOutput);
    REQUIRE(error_output.owner_node().valid());
    CHECK(error_output.owner_node().node_index() == 0);

    auto recordable_state = node.recordable_state(MIN_DT);
    CHECK(recordable_state.owner_port() == TSEndpointOwnerPort::RecordableState);
    REQUIRE(recordable_state.owner_node().valid());
    CHECK(recordable_state.owner_node().node_index() == 0);
}
