#include <hgraph/runtime/registry_snapshot.h>
#include <hgraph/runtime/runtime.h>

#include <catch2/catch_test_macros.hpp>

TEST_CASE("runtime registry snapshot reports retained type cardinalities")
{
    using namespace hgraph;

    const RuntimeRegistrySnapshot initial = runtime_registry_snapshot();

    NodeTypeMetaData schema;
    schema.display_name = "registry_snapshot_probe";
    const NodeBuilder node = NodeBuilder::native(std::move(schema));
    const RuntimeRegistrySnapshot after_node = runtime_registry_snapshot();
    CHECK(after_node.node_runtime_types == initial.node_runtime_types + 1);
    CHECK(after_node.type_records > initial.type_records);

    GraphBuilder graph;
    graph.label("registry_snapshot_graph").add_node(node);
    static_cast<void>(graph.root_type());
    const RuntimeRegistrySnapshot after_graph = runtime_registry_snapshot();
    CHECK(after_graph.graph_programs == initial.graph_programs + 1);
    CHECK(after_graph.graph_runtime_types == initial.graph_runtime_types + 2);
    CHECK(after_graph.type_records > after_node.type_records);

    GraphExecutorBuilder executor;
    executor.label("registry_snapshot_executor").graph_builder(std::move(graph));
    static_cast<void>(executor.type());
    const RuntimeRegistrySnapshot after_executor = runtime_registry_snapshot();
    CHECK(after_executor.executor_runtime_types ==
          initial.executor_runtime_types + 1);
    CHECK(after_executor.type_records > after_graph.type_records);
}
