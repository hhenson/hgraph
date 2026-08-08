#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <string>
#include <type_traits>
#include <utility>

namespace
{
    struct MoveCountingScalar
    {
        inline static int copy_constructs{0};
        inline static int copy_assigns{0};

        int value{0};

        MoveCountingScalar() = default;
        explicit MoveCountingScalar(int value_)
            : value(value_)
        {
        }
        MoveCountingScalar(const MoveCountingScalar &other)
            : value(other.value)
        {
            ++copy_constructs;
        }
        MoveCountingScalar(MoveCountingScalar &&) noexcept = default;

        MoveCountingScalar &operator=(const MoveCountingScalar &other)
        {
            value = other.value;
            ++copy_assigns;
            return *this;
        }
        MoveCountingScalar &operator=(MoveCountingScalar &&) noexcept = default;
    };

    void reset_move_counting_scalar_counts()
    {
        MoveCountingScalar::copy_constructs = 0;
        MoveCountingScalar::copy_assigns    = 0;
    }

    hgraph::NodeBuilder source_node(const hgraph::TSValueTypeMetaData *ts_int, std::int32_t value)
    {
        hgraph::NodeTypeMetaData schema;
        schema.display_name = "source";
        schema.output_schema = ts_int;
        schema.node_kind = hgraph::NodeKind::PullSource;
        schema.schedule_on_start = true;  // a source initiates itself at the start cycle

        hgraph::NodeCallbacks callbacks;
        callbacks.evaluate = [value](const hgraph::NodeView &view, hgraph::DateTime evaluation_time) {
            hgraph::testing::set_output_value(view, evaluation_time, value);
        };
        return hgraph::NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    hgraph::NodeBuilder add_one_node(const hgraph::TSValueTypeMetaData *input_schema,
                                     const hgraph::TSValueTypeMetaData *ts_int)
    {
        hgraph::NodeTypeMetaData schema;
        schema.display_name = "add_one";
        schema.input_schema = input_schema;
        schema.output_schema = ts_int;
        schema.node_kind = hgraph::NodeKind::Compute;

        hgraph::NodeCallbacks callbacks;
        callbacks.evaluate = [](const hgraph::NodeView &view, hgraph::DateTime evaluation_time) {
            auto root = view.input(evaluation_time);
            auto bundle = root.as_bundle();
            auto input = bundle[0];
            const std::int32_t value = input.value().checked_as<std::int32_t>();
            hgraph::testing::set_output_value(view, evaluation_time, value + 1);
        };

        auto endpoint = hgraph::TSEndpointSchema::non_peered(
            input_schema,
            {
                hgraph::TSEndpointSchema::peered(ts_int),
            });
        return hgraph::NodeBuilder::native(std::move(schema), std::move(callbacks), std::move(endpoint));
    }

    hgraph::NodeBuilder stateful_counter_node(const hgraph::ValueTypeMetaData *int_meta,
                                              const hgraph::TSValueTypeMetaData *ts_int)
    {
        hgraph::NodeTypeMetaData schema;
        schema.display_name = "counter";
        schema.output_schema = ts_int;
        schema.state_schema = int_meta;

        hgraph::NodeCallbacks callbacks;
        callbacks.evaluate = [](const hgraph::NodeView &view, hgraph::DateTime evaluation_time) {
            const std::int32_t next = view.state().checked_as<std::int32_t>() + 1;
            auto state = view.state().begin_mutation();
            state.set_scalar(next);
            hgraph::testing::set_output_value(view, evaluation_time, next);
        };

        return hgraph::NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    // A source whose emitted value is read from its per-instance scalar
    // configuration (the read-only `scalars` component), not hard-coded.
    hgraph::NodeBuilder scalar_source_node(const hgraph::ValueTypeMetaData *int_meta,
                                           const hgraph::TSValueTypeMetaData *ts_int)
    {
        hgraph::NodeTypeMetaData schema;
        schema.display_name = "scalar_source";
        schema.output_schema = ts_int;
        schema.scalar_schema = int_meta;
        schema.node_kind = hgraph::NodeKind::PullSource;

        hgraph::NodeCallbacks callbacks;
        callbacks.evaluate = [](const hgraph::NodeView &view, hgraph::DateTime evaluation_time) {
            hgraph::testing::set_output_value(
                view,
                evaluation_time,
                view.scalars().checked_as<std::int32_t>());
        };

        return hgraph::NodeBuilder::native(std::move(schema), std::move(callbacks));
    }
}

TEST_CASE("Node runtime types use canonical records and NodePtr views", "[type-erasure][node]")
{
    using namespace hgraph;

    NodeTypeMetaData schema;
    schema.display_name = "record-backed-compute";
    schema.node_kind = NodeKind::Compute;
    const NodeBuilder builder = NodeBuilder::native(std::move(schema));
    const NodeTypeRef type = builder.type();

    REQUIRE(type.valid());
    REQUIRE(type.record()->schema == &type.schema()->header);
    REQUIRE(type.record()->role == TypeRole::Runtime);
    REQUIRE(type.record()->ops_abi_version == NODE_OPS_ABI_VERSION);
    REQUIRE(type.record()->classification().family == TypeFamily::Node);
    REQUIRE(type.record()->classification().kind == static_cast<TypeKind>(NodeKind::Compute));
    REQUIRE(std::string{type.record()->semantic_name()} == "record-backed-compute");
    REQUIRE(has_capability(type.capabilities(), TypeCapabilities::Viewable));
    REQUIRE(has_capability(type.capabilities(), TypeCapabilities::Mutable));

    NodeValue node = builder.make_node();
    NodeView view = node.view();
    NodePtr pointer = view.pointer();
    AnyPtr generic = pointer;

    REQUIRE(pointer.valid());
    REQUIRE(pointer.record() == type.record());
    REQUIRE(pointer.data() == view.data());
    REQUIRE(pointer.writable_access());
    REQUIRE(generic.family() == TypeFamily::Node);
    REQUIRE(generic.role() == TypeRole::Runtime);
    REQUIRE(NodeTypeRef::checked(generic) == type);
    REQUIRE(NodePtr::checked(generic).same_state_as(pointer));
    REQUIRE(NodeView{NodePtr::checked(generic)}.type() == type);

    Value scalar{std::int32_t{1}};
    REQUIRE_THROWS_AS(NodeTypeRef::checked(scalar.binding().read_only(scalar.view().data())),
                      std::invalid_argument);
}

TEST_CASE("Graph executor and clock runtime families use canonical records", "[type-erasure][runtime]")
{
    using namespace hgraph;

    GraphBuilder graph_builder;
    graph_builder.label("record-backed-graph");
    const GraphTypeRef root_type = graph_builder.root_type();
    const GraphTypeRef nested_type = graph_builder.nested_type();

    REQUIRE(root_type.valid());
    REQUIRE(nested_type.valid());
    REQUIRE(root_type != nested_type);
    REQUIRE(root_type.schema() == nested_type.schema());
    REQUIRE(root_type.record()->schema == &root_type.schema()->header);
    REQUIRE(root_type.record()->classification().family == TypeFamily::Graph);
    REQUIRE(root_type.record()->role == TypeRole::Runtime);
    REQUIRE(std::string{root_type.record()->semantic_name()} == "record-backed-graph");
    REQUIRE(std::string{root_type.record()->implementation_name()} == "hgraph.graph.root");
    REQUIRE(std::string{nested_type.record()->implementation_name()} == "hgraph.graph.nested");
    REQUIRE(root_type.ops_ref().parent_kind == GraphParentKind::Root);
    REQUIRE(nested_type.ops_ref().parent_kind == GraphParentKind::Nested);
    REQUIRE(graph_builder.root_type() == root_type);
    REQUIRE(graph_builder.nested_type() == nested_type);

    GraphExecutorBuilder simulation_builder;
    simulation_builder.label("record-backed-executor")
        .graph_builder(graph_builder)
        .mode(GraphExecutorMode::Simulation);
    const ExecutorTypeRef simulation_type = simulation_builder.type();
    GraphExecutorValue simulation = simulation_builder.make_executor();
    GraphExecutorView simulation_view = simulation.view();
    EvaluationClockView simulation_clock = simulation_view.evaluation_clock();

    REQUIRE(simulation_type.valid());
    REQUIRE(simulation_view.type() == simulation_type);
    REQUIRE(simulation_view.pointer().record() == simulation_type.record());
    REQUIRE(simulation_type.record()->classification().family == TypeFamily::Executor);
    REQUIRE(simulation_type.record()->classification().kind ==
            static_cast<TypeKind>(GraphExecutorMode::Simulation));
    REQUIRE(std::string{simulation_type.record()->implementation_name()} ==
            "hgraph.executor.simulation");
    REQUIRE(simulation_clock.type().valid());
    REQUIRE(simulation_clock.pointer().record() == simulation_clock.type().record());
    REQUIRE(simulation_clock.type().record()->classification().family == TypeFamily::Clock);
    REQUIRE(simulation_clock.type().capabilities() == TypeCapabilities::Viewable);
    REQUIRE(std::string{simulation_clock.type().record()->implementation_name()} ==
            "hgraph.clock.simulation");

    GraphExecutorBuilder realtime_builder;
    realtime_builder.graph_builder(graph_builder).mode(GraphExecutorMode::RealTime);
    GraphExecutorValue realtime = realtime_builder.make_executor();
    EvaluationClockView realtime_clock = realtime.view().evaluation_clock();

    REQUIRE(realtime.view().type() != simulation_type);
    REQUIRE(realtime_clock.type() != simulation_clock.type());
    REQUIRE(realtime_clock.type().schema() == simulation_clock.type().schema());
    REQUIRE(std::string{realtime_clock.type().record()->implementation_name()} ==
            "hgraph.clock.realtime");

    const AnyPtr generic_graph = simulation_view.graph().pointer();
    const AnyPtr generic_executor = simulation_view.pointer();
    const AnyPtr generic_clock = simulation_clock.pointer();
    REQUIRE(GraphTypeRef::checked(generic_graph) == simulation_view.graph().type());
    REQUIRE(ExecutorTypeRef::checked(generic_executor) == simulation_type);
    REQUIRE(ClockTypeRef::checked(generic_clock) == simulation_clock.type());
    REQUIRE_THROWS_AS(GraphTypeRef::checked(generic_executor), std::invalid_argument);
}

TEST_CASE("NodeValue exposes a type-erased view over node storage")
{
    using namespace hgraph;

    static_assert(!std::is_copy_constructible_v<NodeView>);
    static_assert(!std::is_copy_assignable_v<NodeView>);
    static_assert(!std::is_copy_constructible_v<GraphView>);
    static_assert(!std::is_copy_assignable_v<GraphView>);
    static_assert(!std::is_copy_constructible_v<GraphExecutorView>);
    static_assert(!std::is_copy_assignable_v<GraphExecutorView>);

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);

    NodeValue node = source_node(ts_int, 41).make_node();
    const auto t1 = MIN_ST;

    auto view = node.view();
    REQUIRE(view.valid());
    REQUIRE(view.schema()->has_output());
    REQUIRE(view.type().checked_plan().find_component("runtime_storage") != nullptr);
    REQUIRE(view.type().checked_plan().find_component("output") != nullptr);
    REQUIRE(view.type().checked_plan().find_component("input") == nullptr);
    REQUIRE(view.type().checked_plan().find_component("state") == nullptr);
    REQUIRE(std::string{view.label()} == "source");
    REQUIRE_FALSE(view.started());

    view.start(t1);
    REQUIRE(view.started());
    view.evaluate(t1);

    auto output = node.view().output(t1);
    REQUIRE(output.valid());
    REQUIRE(output.modified());
    REQUIRE(output.value().checked_as<std::int32_t>() == 41);
}

TEST_CASE("testing set_output_value moves owned scalar values")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *value_meta = registry.register_scalar<MoveCountingScalar>("MoveCountingScalar");
    const auto *ts_value = registry.ts(value_meta);

    NodeTypeMetaData schema;
    schema.display_name = "move_counting_source";
    schema.output_schema = ts_value;
    schema.node_kind = NodeKind::PullSource;
    schema.schedule_on_start = true;

    NodeCallbacks callbacks;
    callbacks.evaluate = [](const NodeView &view, DateTime evaluation_time) {
        testing::set_output_value(view, evaluation_time, MoveCountingScalar{17});
    };

    NodeValue node = NodeBuilder::native(std::move(schema), std::move(callbacks)).make_node();
    const auto t1 = MIN_ST;

    reset_move_counting_scalar_counts();
    auto view = node.view();
    view.start(t1);
    view.evaluate(t1);

    auto        output_view  = node.view().output(t1).value();
    const auto &output_value = output_view.checked_as<MoveCountingScalar>();
    REQUIRE(output_value.value == 17);
    REQUIRE(MoveCountingScalar::copy_constructs == 0);
    REQUIRE(MoveCountingScalar::copy_assigns == 0);
}

TEST_CASE("NodeValue state is read-write value storage")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);

    NodeValue node = stateful_counter_node(int_meta, ts_int).make_node();
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};

    auto view = node.view();
    REQUIRE(view.valid());
    REQUIRE(view.schema()->has_state());
    REQUIRE(view.has_state());
    REQUIRE(view.type().checked_plan().find_component("runtime_storage") != nullptr);
    REQUIRE(view.type().checked_plan().find_component("output") != nullptr);
    REQUIRE(view.type().checked_plan().find_component("state") != nullptr);
    REQUIRE(view.type().checked_plan().find_component("input") == nullptr);
    REQUIRE(view.state().checked_as<std::int32_t>() == 0);

    view.start(t1);
    view.evaluate(t1);
    REQUIRE(node.view().state().checked_as<std::int32_t>() == 1);
    REQUIRE(node.view().output(t1).value().checked_as<std::int32_t>() == 1);

    node.view().evaluate(t2);
    REQUIRE(node.view().state().checked_as<std::int32_t>() == 2);
    REQUIRE(node.view().output(t2).value().checked_as<std::int32_t>() == 2);
}

TEST_CASE("NodeValue scalar configuration is read-only per-instance value storage")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);

    NodeValue node = scalar_source_node(int_meta, ts_int).scalars(Value{std::int32_t{7}}).make_node();
    const auto t1 = MIN_ST;

    auto view = node.view();
    REQUIRE(view.valid());
    REQUIRE(view.schema()->has_scalars());
    REQUIRE(view.has_scalars());
    REQUIRE(view.type().checked_plan().find_component("scalars") != nullptr);
    REQUIRE(view.type().checked_plan().find_component("state") == nullptr);
    REQUIRE(view.scalars().checked_as<std::int32_t>() == 7);

    view.start(t1);
    view.evaluate(t1);
    REQUIRE(node.view().output(t1).value().checked_as<std::int32_t>() == 7);
    // The scalar configuration is unchanged by evaluation.
    REQUIRE(node.view().scalars().checked_as<std::int32_t>() == 7);
}

TEST_CASE("GraphValue wires node views and evaluates scheduled notifications")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *input_schema = registry.tsb("RuntimeInput", {{"value", ts_int}});

    GraphBuilder builder;
    builder.label("runtime_graph")
        .add_node(source_node(ts_int, 42))
        .add_node(add_one_node(input_schema, ts_int))
        .add_edge(GraphEdge{
            .source_node = 0,
            .source_path = {},
            .target_node = 1,
            .target_path = {0},
        });

    testing::MockRootGraph graph{builder};
    auto graph_view = graph.graph();
    const auto t1 = MIN_ST;

    REQUIRE(graph_view.valid());
    REQUIRE(graph_view.schema()->nodes.size() == 2);
    REQUIRE(graph_view.node_count() == 2);
    const auto graph_type = graph_view.type();
    const auto &graph_plan = graph_type.checked_plan();
    const auto *node_storage = graph_plan.find_component("nodes");
    REQUIRE(node_storage != nullptr);
    REQUIRE(node_storage->plan->is_tuple());
    REQUIRE(node_storage->plan->component_count() == 2);
    REQUIRE(node_storage->plan->component(0).plan == graph_view.node_at(0).type().plan());
    REQUIRE(node_storage->plan->component(1).plan == graph_view.node_at(1).type().plan());
    const auto *schedule_storage = graph_plan.find_component("schedule");
    REQUIRE(schedule_storage != nullptr);
    REQUIRE(schedule_storage->plan->is_array());
    REQUIRE(schedule_storage->plan->array_count() == 2);
    REQUIRE(graph_view.node_at(1).type().checked_plan().find_component("input") != nullptr);
    REQUIRE(graph_view.node_at(1).type().checked_plan().find_component("output") != nullptr);
    REQUIRE(graph_view.node_at(1).type().checked_plan().find_component("state") == nullptr);

    graph_view.start(t1);
    REQUIRE(graph_view.started());
    REQUIRE(graph_view.next_scheduled_time() == t1);

    graph_view.evaluate(t1);
    REQUIRE(graph_view.evaluation_time() == t1);

    auto source_output = graph_view.node_at(0).output(t1);
    auto compute_output = graph_view.node_at(1).output(t1);
    REQUIRE(source_output.value().checked_as<std::int32_t>() == 42);
    REQUIRE(compute_output.value().checked_as<std::int32_t>() == 43);
    REQUIRE(source_output.modified());
    REQUIRE(compute_output.modified());

    // Delta cleanup is lazy: there is NO end-of-cycle sweep. The deltas produced this
    // cycle remain readable (modified() stays true at t1) until the producer's next
    // mutation, which is exactly what cross-graph / nested / mesh reads depend on. The
    // delta accessors are read-gated on modified(), so a stale delta is never observable
    // in the window before that next mutation reclaims it (slot-store prepare_delta).
    const auto t2 = t1 + TimeDelta{1};
    graph_view.evaluate(t2);
    REQUIRE(graph_view.evaluation_time() == t2);

    graph_view.stop();
    REQUIRE_FALSE(graph_view.started());
}

TEST_CASE("GraphExecutorValue runs the graph through the type-erased executor view")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *input_schema = registry.tsb("ExecutorInput", {{"value", ts_int}});

    GraphBuilder graph_builder;
    graph_builder
        .add_node(source_node(ts_int, 9))
        .add_node(add_one_node(input_schema, ts_int))
        .add_edge(GraphEdge{
            .source_node = 0,
            .source_path = {},
            .target_node = 1,
            .target_path = {0},
        });

    GraphExecutorBuilder executor_builder;
    executor_builder
        .label("executor")
        .graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{3});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto executor_view = executor.view();
    REQUIRE(executor_view.valid());
    REQUIRE(executor_view.schema()->mode == GraphExecutorMode::Simulation);

    executor_view.run();

    auto graph = executor_view.graph();
    auto output = graph.node_at(1).output(MIN_ST);
    REQUIRE(output.value().checked_as<std::int32_t>() == 10);
    REQUIRE_FALSE(graph.started());
}
