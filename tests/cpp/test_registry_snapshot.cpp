#include <hgraph/lib/std/std_operators.h>
#include <hgraph/runtime/registry_snapshot.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/service_wiring.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <string>
#include <string_view>
#include <utility>

namespace {
using namespace hgraph;

struct RegistryCanonicalNode {
  static constexpr auto name = "registry_canonical_node";
  static constexpr std::string_view implementation_label =
      "test.registry.canonical";

  static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs,
                   Out<TS<Int>> out) {
    out.set(lhs.value() + rhs.value());
  }
};

GraphBuilder canonical_graph(std::string label = "registry_canonical_graph") {
  NodeBuilder node;
  node.implementation<RegistryCanonicalNode>();
  GraphBuilder graph;
  graph.label(std::move(label)).add_node(std::move(node));
  return graph;
}

struct RegistryServiceAdaptor : service_adaptor::interface {
  static constexpr std::string_view name{"registry_service_adaptor"};
  using input_schema = TS<Int>;
  using output_schema = TS<Int>;
};

struct RegistryServiceAdaptorSource {
  static constexpr auto name = "registry_service_adaptor_source";
  static constexpr bool schedule_on_start = true;

  static void eval(Out<TS<Int>> out) { out.set(Int{1}); }
};

struct RegistryServiceAdaptorImpl {
  static constexpr auto name = "registry_service_adaptor_impl";

  static void eval(
      In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests,
      Out<TSD<Int, TS<Int>>> out) {
    (void)requests;
    (void)out;
  }
};

struct RegistryServiceAdaptorGraph {
  static constexpr auto name = "registry_service_adaptor_graph";

  static void compose(Wiring &w) {
    const auto custom = service_adaptor::path("registry_reuse");
    service_adaptor::register_service_adaptor_impl<
        RegistryServiceAdaptor, RegistryServiceAdaptorImpl>(w, custom);
    const auto request = wire<RegistryServiceAdaptorSource>(w);
    static_cast<void>(wire<RegistryServiceAdaptor>(w, custom, request));
  }
};

struct RegistryMapSource {
  static constexpr auto name = "registry_map_source";
  static constexpr bool schedule_on_start = true;

  static void eval(Out<TSD<Int, TS<Int>>>) {}
};

struct RegistryMappedGraph {
  static constexpr auto name = "registry_mapped_graph";

  static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> value) {
    using namespace hgraph::stdlib::syntax;
    return (value + Int{1}).as<TS<Int>>();
  }
};

struct RegistryMapGraph {
  static constexpr auto name = "registry_map_graph";

  static void compose(Wiring &w) {
    const auto values = wire<RegistryMapSource>(w);
    static_cast<void>(wire<stdlib::map_>(w, fn<RegistryMappedGraph>(), values));
  }
};
} // namespace

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

TEST_CASE("static node runtime types are reused for equivalent descriptors") {
  using namespace hgraph;

  const RuntimeRegistrySnapshot initial = runtime_registry_snapshot();

  NodeBuilder first;
  first.implementation<RegistryCanonicalNode>();
  const RuntimeRegistrySnapshot after_first = runtime_registry_snapshot();
  REQUIRE(after_first.node_runtime_types == initial.node_runtime_types + 1);

  NodeBuilder repeated;
  repeated.implementation<RegistryCanonicalNode>();
  const RuntimeRegistrySnapshot after_repeated = runtime_registry_snapshot();
  CHECK(repeated.type() == first.type());
  CHECK(after_repeated.node_runtime_types == after_first.node_runtime_types);
  CHECK(after_repeated.type_records == after_first.type_records);

  const std::array<std::size_t, 1> passive_rhs{1};
  const NodeBuilder first_passive = first.with_passive_inputs(passive_rhs);
  const RuntimeRegistrySnapshot after_first_passive =
      runtime_registry_snapshot();
  CHECK(first_passive.type() != first.type());
  REQUIRE(after_first_passive.node_runtime_types ==
          after_repeated.node_runtime_types + 1);

  const NodeBuilder repeated_passive =
      repeated.with_passive_inputs(passive_rhs);
  const RuntimeRegistrySnapshot after_repeated_passive =
      runtime_registry_snapshot();
  CHECK(repeated_passive.type() == first_passive.type());
  CHECK(after_repeated_passive.node_runtime_types ==
        after_first_passive.node_runtime_types);
  CHECK(after_repeated_passive.type_records ==
        after_first_passive.type_records);
}

TEST_CASE(
    "native descriptors without a runtime type identity remain distinct") {
  using namespace hgraph;

  NodeTypeMetaData first_schema;
  first_schema.display_name = "uncanonical_native_node";
  const NodeBuilder first = NodeBuilder::native(std::move(first_schema));

  NodeTypeMetaData second_schema;
  second_schema.display_name = "uncanonical_native_node";
  const NodeBuilder second = NodeBuilder::native(std::move(second_schema));

  CHECK(first.type() != second.type());
}

TEST_CASE(
    "canonical descriptors require and honor a stable runtime type identity") {
  using namespace hgraph;

  static const std::byte runtime_type_id{};
  NodeTypeDescriptor first_descriptor;
  first_descriptor.schema.display_name = "canonical_native_node";
  const NodeBuilder first = NodeBuilder::from_canonical_descriptor(
      std::move(first_descriptor), &runtime_type_id);

  NodeTypeDescriptor repeated_descriptor;
  repeated_descriptor.schema.display_name = "canonical_native_node";
  const NodeBuilder repeated = NodeBuilder::from_canonical_descriptor(
      std::move(repeated_descriptor), &runtime_type_id);
  CHECK(repeated.type() == first.type());

  NodeTypeDescriptor null_identity_descriptor;
  CHECK_THROWS_AS(NodeBuilder::from_canonical_descriptor(
                      std::move(null_identity_descriptor), nullptr),
                  std::invalid_argument);
}

TEST_CASE("graph builders initialize an active global state with one copy") {
  using namespace hgraph;

  GlobalState state;
  GlobalContext context{state};
  const auto before_copy =
      runtime_registry_snapshot().type_system_lock_acquisitions;
  GlobalState copied{state};
  const auto after_copy =
      runtime_registry_snapshot().type_system_lock_acquisitions;
  const auto copy_acquisitions = after_copy - before_copy;

  GraphBuilder builder;

  const auto after = runtime_registry_snapshot().type_system_lock_acquisitions;
  CHECK(after - after_copy == copy_acquisitions);
}

TEST_CASE("global state binding refreshes after a registry reset") {
  using namespace hgraph;

  {
    GlobalState state;
    CHECK(state.view().size() == 0);
  }

  const auto generation = TypeRegistry::instance().reset_generation();
  reset_all_registries();
  REQUIRE(TypeRegistry::instance().reset_generation() != generation);

  GlobalState state;
  CHECK(state.view().size() == 0);
}

TEST_CASE("equivalent graph and executor runtime types are reused") {
  using namespace hgraph;

  GraphBuilder first_graph = canonical_graph();
  const GraphTypeRef first_root = first_graph.root_type();
  const GraphTypeRef first_nested = first_graph.nested_type();
  const RuntimeRegistrySnapshot after_first_graph = runtime_registry_snapshot();

  GraphBuilder repeated_graph = canonical_graph();
  CHECK(repeated_graph.root_type() == first_root);
  CHECK(repeated_graph.nested_type() == first_nested);
  const RuntimeRegistrySnapshot after_repeated_graph =
      runtime_registry_snapshot();
  CHECK(after_repeated_graph.graph_programs ==
        after_first_graph.graph_programs);
  CHECK(after_repeated_graph.graph_runtime_types ==
        after_first_graph.graph_runtime_types);

  GraphBuilder differently_named_graph =
      canonical_graph("registry_other_graph");
  CHECK(differently_named_graph.root_type() != first_root);
  const RuntimeRegistrySnapshot after_other_graph = runtime_registry_snapshot();
  CHECK(after_other_graph.graph_programs ==
        after_first_graph.graph_programs + 1);

  GraphExecutorBuilder first_executor;
  first_executor.label("registry_executor").graph_builder(canonical_graph());
  const ExecutorTypeRef first_executor_type = first_executor.type();
  const RuntimeRegistrySnapshot after_first_executor =
      runtime_registry_snapshot();

  GraphExecutorBuilder repeated_executor;
  repeated_executor.label("registry_executor").graph_builder(canonical_graph());
  CHECK(repeated_executor.type() == first_executor_type);
  const RuntimeRegistrySnapshot after_repeated_executor =
      runtime_registry_snapshot();
  CHECK(after_repeated_executor.executor_runtime_types ==
        after_first_executor.executor_runtime_types);

  GraphExecutorBuilder realtime_executor;
  realtime_executor.label("registry_executor")
      .mode(GraphExecutorMode::RealTime)
      .graph_builder(canonical_graph());
  CHECK(realtime_executor.type() != first_executor_type);
}

TEST_CASE("native service adaptor wiring reuses runtime types") {
  using namespace hgraph;

  stdlib::register_standard_operators();

  GraphBuilder first = build_graph<RegistryServiceAdaptorGraph>();
  static_cast<void>(first.root_type());
  const RuntimeRegistrySnapshot after_first = runtime_registry_snapshot();

  GraphBuilder repeated = build_graph<RegistryServiceAdaptorGraph>();
  static_cast<void>(repeated.root_type());
  const RuntimeRegistrySnapshot after_repeated = runtime_registry_snapshot();

  CHECK(after_repeated.node_runtime_types == after_first.node_runtime_types);
  CHECK(after_repeated.graph_programs == after_first.graph_programs);
  CHECK(after_repeated.graph_runtime_types ==
        after_first.graph_runtime_types);
  CHECK(after_repeated.executor_runtime_types ==
        after_first.executor_runtime_types);
  CHECK(after_repeated.type_records == after_first.type_records);
}

TEST_CASE("native map wiring reuses runtime types") {
  using namespace hgraph;

  stdlib::register_standard_operators();

  GraphBuilder first = build_graph<RegistryMapGraph>();
  static_cast<void>(first.root_type());
  const RuntimeRegistrySnapshot after_first = runtime_registry_snapshot();

  GraphBuilder repeated = build_graph<RegistryMapGraph>();
  static_cast<void>(repeated.root_type());
  const RuntimeRegistrySnapshot after_repeated = runtime_registry_snapshot();

  CHECK(after_repeated.node_runtime_types == after_first.node_runtime_types);
  CHECK(after_repeated.graph_programs == after_first.graph_programs);
  CHECK(after_repeated.graph_runtime_types ==
        after_first.graph_runtime_types);
  CHECK(after_repeated.executor_runtime_types ==
        after_first.executor_runtime_types);
  CHECK(after_repeated.type_records == after_first.type_records);
}
