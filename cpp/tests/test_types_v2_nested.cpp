#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/child_graph.h>
#include <hgraph/types/graph_builder.h>
#include <hgraph/types/nested_clock.h>
#include <hgraph/types/nested_node_builder.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/value/type_registry.h>
#include <nanobind/nanobind.h>

#include <chrono>
#include <filesystem>

namespace hgraph::nested_test
{
    struct ChildTemplateRegistryResetGuard
    {
        ChildTemplateRegistryResetGuard() { ChildGraphTemplateRegistry::instance().reset(); }

        ~ChildTemplateRegistryResetGuard() { ChildGraphTemplateRegistry::instance().reset(); }
    };

    [[nodiscard]] engine_time_t tick(int offset) { return MIN_DT + std::chrono::microseconds{offset}; }

    // Minimal node implementations for testing child graph lifecycle.
    struct NoopNode
    {
        NoopNode()  = delete;
        ~NoopNode() = delete;
        static void eval() {}
    };

    struct CounterNode
    {
        CounterNode()  = delete;
        ~CounterNode() = delete;

        static void eval(State<int> state) {
            auto &count = state.view().template checked_as<int>();
            count++;
        }
    };

    struct SchedulingNode
    {
        SchedulingNode()  = delete;
        ~SchedulingNode() = delete;

        static void eval(NodeScheduler &scheduler, Out<TS<int>> out) {
            out.set(42);
            // Schedule self for next tick
            scheduler.schedule(MIN_TD);
        }
    };

    struct SourceNode
    {
        SourceNode()  = delete;
        ~SourceNode() = delete;

        static constexpr auto node_type = NodeTypeEnum::PULL_SOURCE_NODE;
        static void           eval(Out<TS<int>> out) { out.set(1); }
    };

    // Divides 100 by input. Throws on zero.
    struct DivideNode
    {
        DivideNode()  = delete;
        ~DivideNode() = delete;

        static void eval(In<"x", TS<int>> x, Out<TS<int>> out) {
            const int divisor = x.value();
            if (divisor == 0) { throw std::runtime_error("division by zero"); }
            out.set(100 / divisor);
        }
    };

    void ensure_python_hgraph_importable()
    {
        setenv("HGRAPH_USE_CPP", "0", 1);

        if (!Py_IsInitialized()) {
            PyConfig config;
            PyConfig_InitPythonConfig(&config);

            const auto throw_if_status_error = [&](PyStatus status, const char *action) {
                if (!PyStatus_Exception(status)) { return; }

                std::string message = action;
                if (status.err_msg != nullptr) {
                    message += ": ";
                    message += status.err_msg;
                }
                PyConfig_Clear(&config);
                throw std::runtime_error(message);
            };

#ifdef HGRAPH_TEST_PYTHON_EXECUTABLE
            throw_if_status_error(
                PyConfig_SetBytesString(&config, &config.program_name, HGRAPH_TEST_PYTHON_EXECUTABLE),
                "failed to configure Python executable");

            const auto python_home = std::filesystem::path{HGRAPH_TEST_PYTHON_HOME}.string();
            throw_if_status_error(
                PyConfig_SetBytesString(&config, &config.home, python_home.c_str()),
                "failed to configure Python home");
#endif

            const PyStatus status = Py_InitializeFromConfig(&config);
            if (PyStatus_Exception(status)) {
                std::string message = "failed to initialise Python";
                if (status.err_msg != nullptr) {
                    message += ": ";
                    message += status.err_msg;
                }
                PyConfig_Clear(&config);
                throw std::runtime_error(message);
            }

            PyConfig_Clear(&config);
        }

        nb::gil_scoped_acquire guard;
        nb::module_ sys = nb::module_::import_("sys");
        nb::list path = nb::borrow<nb::list>(sys.attr("path"));
        path.insert(0, "/Users/hhenson/CLionProjects/hgraph_2");
    }

}  // namespace hgraph::nested_test

// ============================================================================
// Nested Clock Tests
// ============================================================================

TEST_CASE("nested clock state initialises with expected defaults", "[v2][nested][clock]") {
    hgraph::NestedClockState state;
    CHECK(state.evaluation_time == hgraph::MIN_DT);
    CHECK(state.nested_next_scheduled == hgraph::MAX_DT);
    CHECK(state.parent_node == nullptr);
}

TEST_CASE("nested clock ops creates a valid EngineEvaluationClock facade", "[v2][nested][clock]") {
    hgraph::NestedClockState state;
    state.evaluation_time = hgraph::nested_test::tick(10);

    hgraph::EngineEvaluationClock clock{&state, &hgraph::nested_clock_ops()};

    CHECK(clock.valid());
    CHECK(clock.evaluation_time() == hgraph::nested_test::tick(10));
    CHECK(clock.next_scheduled_evaluation_time() == hgraph::MAX_DT);
}

TEST_CASE("nested clock set_evaluation_time updates the state", "[v2][nested][clock]") {
    hgraph::NestedClockState state;

    hgraph::EngineEvaluationClock clock{&state, &hgraph::nested_clock_ops()};
    clock.set_evaluation_time(hgraph::nested_test::tick(5));

    CHECK(state.evaluation_time == hgraph::nested_test::tick(5));
    CHECK(clock.evaluation_time() == hgraph::nested_test::tick(5));
}

TEST_CASE("nested clock update_next_scheduled skips when already evaluated past requested time", "[v2][nested][clock]") {
    hgraph::NestedClockState state;
    state.evaluation_time = hgraph::nested_test::tick(10);

    hgraph::EngineEvaluationClock clock{&state, &hgraph::nested_clock_ops()};

    // Request scheduling at tick(5), which is before evaluation_time tick(10)
    clock.update_next_scheduled_evaluation_time(hgraph::nested_test::tick(5));

    // Should not have changed — we already evaluated past this time
    CHECK(state.nested_next_scheduled == hgraph::MAX_DT);
}

TEST_CASE("nested clock update_next_scheduled computes proposed time correctly", "[v2][nested][clock]") {
    hgraph::NestedClockState state;
    // No evaluation yet — evaluation_time is MIN_DT
    // Floor = MIN_DT + MIN_TD = tick(1)

    hgraph::EngineEvaluationClock clock{&state, &hgraph::nested_clock_ops()};

    // Request scheduling at tick(5)
    // proposed = min(tick(5), max(MAX_DT, MIN_DT + MIN_TD)) = min(tick(5), MAX_DT) = tick(5)
    clock.update_next_scheduled_evaluation_time(hgraph::nested_test::tick(5));

    CHECK(state.nested_next_scheduled == hgraph::nested_test::tick(5));
}

TEST_CASE("nested clock update_next_scheduled coalesces to earliest time", "[v2][nested][clock]") {
    hgraph::NestedClockState state;

    hgraph::EngineEvaluationClock clock{&state, &hgraph::nested_clock_ops()};

    // First request at tick(10)
    clock.update_next_scheduled_evaluation_time(hgraph::nested_test::tick(10));
    CHECK(state.nested_next_scheduled == hgraph::nested_test::tick(10));

    // Second request at tick(5) — should coalesce to the earlier time
    // proposed = min(tick(5), max(tick(10), MIN_DT + MIN_TD)) = min(tick(5), tick(10)) = tick(5)
    clock.update_next_scheduled_evaluation_time(hgraph::nested_test::tick(5));
    CHECK(state.nested_next_scheduled == hgraph::nested_test::tick(5));
}

TEST_CASE("nested clock update_next_scheduled prevents scheduling before evaluation_time + MIN_TD", "[v2][nested][clock]") {
    hgraph::NestedClockState state;
    state.evaluation_time = hgraph::nested_test::tick(10);

    hgraph::EngineEvaluationClock clock{&state, &hgraph::nested_clock_ops()};

    // Request at tick(11) — the minimum possible is tick(10) + MIN_TD = tick(11)
    clock.update_next_scheduled_evaluation_time(hgraph::nested_test::tick(11));
    CHECK(state.nested_next_scheduled == hgraph::nested_test::tick(11));
}

TEST_CASE("nested clock update_next_scheduled delegates through parent scheduler", "[v2][nested][clock]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;

    auto       &value_registry = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry    = hgraph::TSTypeRegistry::instance();
    const auto *int_type       = value_registry.register_type<int>("int");
    const auto *scalar_ts      = ts_registry.ts(int_type);

    // Build a parent graph with a single noop node
    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.label("parent").output_schema(scalar_ts).uses_scheduler(true).implementation<NoopNode>());

    auto engine = EvaluationEngineBuilder{}.graph_builder(std::move(builder)).start_time(tick(0)).build();

    auto &graph = engine.graph();
    graph.start();

    // Set up nested clock state pointing at the parent node
    auto            &parent_node = graph.node_at(0);
    NestedClockState state;
    state.parent_node = &parent_node;

    EngineEvaluationClock clock{&state, &nested_clock_ops()};

    // Request scheduling — should propagate to parent graph
    clock.update_next_scheduled_evaluation_time(tick(5));
    CHECK(state.nested_next_scheduled == tick(5));
    CHECK(parent_node.scheduler().next_scheduled_time() == tick(5));

    // The parent graph should now have the node scheduled at tick(5)
    CHECK(graph.scheduled_time(0) == tick(5));
}

TEST_CASE("nested clock startup scheduling survives parent start", "[v2][nested][clock]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;

    auto       &value_registry = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry    = hgraph::TSTypeRegistry::instance();
    const auto *int_type       = value_registry.register_type<int>("int");
    const auto *scalar_ts      = ts_registry.ts(int_type);

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.label("parent").output_schema(scalar_ts).uses_scheduler(true).implementation<NoopNode>());

    auto engine = EvaluationEngineBuilder{}.graph_builder(std::move(builder)).start_time(tick(0)).build();

    auto &graph       = engine.graph();
    auto &parent_node = graph.node_at(0);

    NestedClockState state;
    state.parent_node = &parent_node;

    EngineEvaluationClock clock{&state, &nested_clock_ops()};

    // Simulate a child graph scheduling itself during the parent node's
    // start() path, before the parent node has flipped to started().
    clock.update_next_scheduled_evaluation_time(tick(5));
    CHECK(parent_node.scheduler().next_scheduled_time() == tick(5));
    CHECK(graph.scheduled_time(0) == hgraph::MIN_DT);

    graph.start();

    CHECK(graph.scheduled_time(0) == tick(5));
    CHECK(parent_node.scheduler().next_scheduled_time() == tick(5));
}

TEST_CASE("nested clock preserves same-cycle wakeups through parent scheduler", "[v2][nested][clock]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;

    auto       &value_registry = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry    = hgraph::TSTypeRegistry::instance();
    const auto *int_type       = value_registry.register_type<int>("int");
    const auto *scalar_ts      = ts_registry.ts(int_type);

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.label("parent").output_schema(scalar_ts).uses_scheduler(true).implementation<NoopNode>());

    auto engine = EvaluationEngineBuilder{}.graph_builder(std::move(builder)).start_time(tick(0)).build();

    auto &graph       = engine.graph();
    auto &parent_node = graph.node_at(0);
    graph.start();
    graph.evaluate(tick(5));

    NestedClockState state;
    state.parent_node     = &parent_node;
    state.evaluation_time = tick(4);

    EngineEvaluationClock clock{&state, &nested_clock_ops()};

    clock.update_next_scheduled_evaluation_time(tick(5));

    CHECK(state.nested_next_scheduled == tick(5));
    CHECK(parent_node.scheduler().is_scheduled_now());
    CHECK(parent_node.scheduler().next_scheduled_time() == tick(5));
    CHECK(graph.scheduled_time(0) == tick(5));
}

TEST_CASE("nested clock reset_next_scheduled resets to MAX_DT", "[v2][nested][clock]") {
    hgraph::NestedClockState state;
    state.nested_next_scheduled = hgraph::nested_test::tick(5);

    state.reset_next_scheduled();

    CHECK(state.nested_next_scheduled == hgraph::MAX_DT);
}

TEST_CASE("nested clock advance_to_next_scheduled_time moves evaluation_time", "[v2][nested][clock]") {
    hgraph::NestedClockState state;
    state.nested_next_scheduled = hgraph::nested_test::tick(5);

    hgraph::EngineEvaluationClock clock{&state, &hgraph::nested_clock_ops()};
    clock.advance_to_next_scheduled_time();

    CHECK(state.evaluation_time == hgraph::nested_test::tick(5));
    CHECK(state.nested_next_scheduled == hgraph::MAX_DT);
}

// ============================================================================
// ChildGraphTemplate Tests
// ============================================================================

TEST_CASE("ChildGraphTemplate::create stores builder and plan", "[v2][nested][template]") {
    using namespace hgraph;
    nested_test::ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry    = hgraph::TSTypeRegistry::instance();
    const auto *int_type       = value_registry.register_type<int>("int");
    const auto *scalar_ts      = ts_registry.ts(int_type);

    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("inner").output_schema(scalar_ts).implementation<nested_test::NoopNode>());

    BoundaryBindingPlan plan;
    plan.outputs.push_back(OutputBindingSpec{
        .mode             = OutputBindingMode::ALIAS_CHILD_OUTPUT,
        .child_node_index = 0,
    });

    auto tmpl = ChildGraphTemplate::create(std::move(child_builder), std::move(plan), "test_template",
                                           ChildGraphTemplateFlags{.has_output = true});

    REQUIRE(tmpl != nullptr);
    CHECK(tmpl->default_label == "test_template");
    CHECK(tmpl->flags.has_output == true);
    CHECK(tmpl->boundary_plan.outputs.size() == 1);
    CHECK(tmpl->boundary_plan.outputs[0].child_node_index == 0);
    CHECK(tmpl->graph_builder.node_builder_count() == 1);
    CHECK(ChildGraphTemplateRegistry::instance().size() == 1);
}

// ============================================================================
// ChildGraphInstance Lifecycle Tests
// ============================================================================

TEST_CASE("ChildGraphInstance lifecycle: initialise creates child graph", "[v2][nested][instance]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry    = hgraph::TSTypeRegistry::instance();
    const auto *int_type       = value_registry.register_type<int>("int");
    const auto *scalar_ts      = ts_registry.ts(int_type);

    // Create a parent graph with one noop node
    GraphBuilder parent_builder;
    parent_builder.add_node(NodeBuilder{}.label("parent").output_schema(scalar_ts).implementation<NoopNode>());

    auto  engine       = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &parent_graph = engine.graph();
    parent_graph.start();

    // Create a child graph template with one noop node
    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("inner").output_schema(scalar_ts).implementation<NoopNode>());

    auto tmpl = ChildGraphTemplate::create(std::move(child_builder), BoundaryBindingPlan{}, "child");

    // Initialise instance
    ChildGraphInstance instance;
    CHECK_FALSE(instance.is_initialised());
    CHECK_FALSE(instance.is_started());

    instance.initialise(*tmpl, parent_graph.node_at(0), {0}, "child_0");

    CHECK(instance.is_initialised());
    CHECK_FALSE(instance.is_started());
    CHECK(instance.graph() != nullptr);
    CHECK(instance.graph_id() == std::vector<int64_t>{0});
}

TEST_CASE("ChildGraphInstance lifecycle: start and stop", "[v2][nested][instance]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry    = hgraph::TSTypeRegistry::instance();
    const auto *int_type       = value_registry.register_type<int>("int");
    const auto *scalar_ts      = ts_registry.ts(int_type);

    // Parent graph
    GraphBuilder parent_builder;
    parent_builder.add_node(NodeBuilder{}.label("parent").output_schema(scalar_ts).implementation<NoopNode>());
    auto  engine       = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &parent_graph = engine.graph();
    parent_graph.start();

    // Child template
    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("inner").output_schema(scalar_ts).implementation<NoopNode>());
    auto tmpl = ChildGraphTemplate::create(std::move(child_builder), BoundaryBindingPlan{}, "child");

    ChildGraphInstance instance;
    instance.initialise(*tmpl, parent_graph.node_at(0), {0});

    CHECK_FALSE(instance.is_started());
    REQUIRE(instance.graph() != nullptr);

    // Verify the child graph has a valid evaluation engine
    auto child_clock = instance.graph()->engine_evaluation_clock();
    REQUIRE(child_clock.valid());

    instance.start(tick(1));
    CHECK(instance.is_started());
    CHECK(instance.clock_state().evaluation_time == tick(1));

    instance.stop(tick(2));
    CHECK_FALSE(instance.is_started());
}

TEST_CASE("ChildGraphInstance lifecycle: evaluate updates clock evaluation_time", "[v2][nested][instance]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry    = hgraph::TSTypeRegistry::instance();
    const auto *int_type       = value_registry.register_type<int>("int");
    const auto *scalar_ts      = ts_registry.ts(int_type);

    // Parent
    GraphBuilder parent_builder;
    parent_builder.add_node(NodeBuilder{}.label("parent").output_schema(scalar_ts).implementation<NoopNode>());
    auto  engine       = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &parent_graph = engine.graph();
    parent_graph.start();

    // Child with a source node that will schedule itself
    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("source").implementation<SourceNode>());
    auto tmpl = ChildGraphTemplate::create(std::move(child_builder), BoundaryBindingPlan{}, "child");

    ChildGraphInstance instance;
    instance.initialise(*tmpl, parent_graph.node_at(0), {0});
    instance.start(tick(1));

    CHECK(instance.clock_state().evaluation_time == tick(1));  // set during start

    // Schedule the source node for tick(1)
    instance.graph()->schedule_node(0, tick(1));

    instance.evaluate(tick(1));
    CHECK(instance.clock_state().evaluation_time == tick(1));
}

TEST_CASE("ChildGraphInstance lifecycle: dispose stops and marks disposed", "[v2][nested][instance]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry    = hgraph::TSTypeRegistry::instance();
    const auto *int_type       = value_registry.register_type<int>("int");
    const auto *scalar_ts      = ts_registry.ts(int_type);

    // Parent
    GraphBuilder parent_builder;
    parent_builder.add_node(NodeBuilder{}.label("parent").output_schema(scalar_ts).implementation<NoopNode>());
    auto  engine       = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &parent_graph = engine.graph();
    parent_graph.start();

    // Child
    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("inner").output_schema(scalar_ts).implementation<NoopNode>());
    auto tmpl = ChildGraphTemplate::create(std::move(child_builder), BoundaryBindingPlan{}, "child");

    ChildGraphInstance instance;
    instance.initialise(*tmpl, parent_graph.node_at(0), {0});
    instance.start(tick(1));

    CHECK(instance.is_started());
    CHECK_FALSE(instance.is_disposed());

    instance.dispose(tick(2));

    CHECK_FALSE(instance.is_started());
    CHECK(instance.is_disposed());
}

TEST_CASE("ChildGraphInstance rejects double initialise", "[v2][nested][instance]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry    = hgraph::TSTypeRegistry::instance();
    const auto *int_type       = value_registry.register_type<int>("int");
    const auto *scalar_ts      = ts_registry.ts(int_type);

    GraphBuilder parent_builder;
    parent_builder.add_node(NodeBuilder{}.label("parent").output_schema(scalar_ts).implementation<NoopNode>());
    auto  engine       = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &parent_graph = engine.graph();
    parent_graph.start();

    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("inner").output_schema(scalar_ts).implementation<NoopNode>());
    auto tmpl = ChildGraphTemplate::create(std::move(child_builder), BoundaryBindingPlan{}, "child");

    ChildGraphInstance instance;
    instance.initialise(*tmpl, parent_graph.node_at(0), {0});

    CHECK_THROWS_AS(instance.initialise(*tmpl, parent_graph.node_at(0), {0}), std::logic_error);
}

TEST_CASE("ChildGraphInstance rejects evaluate before start", "[v2][nested][instance]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry    = hgraph::TSTypeRegistry::instance();
    const auto *int_type       = value_registry.register_type<int>("int");
    const auto *scalar_ts      = ts_registry.ts(int_type);

    GraphBuilder parent_builder;
    parent_builder.add_node(NodeBuilder{}.label("parent").output_schema(scalar_ts).implementation<NoopNode>());
    auto  engine       = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &parent_graph = engine.graph();
    parent_graph.start();

    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("inner").output_schema(scalar_ts).implementation<NoopNode>());
    auto tmpl = ChildGraphTemplate::create(std::move(child_builder), BoundaryBindingPlan{}, "child");

    ChildGraphInstance instance;
    instance.initialise(*tmpl, parent_graph.node_at(0), {0});

    CHECK_THROWS_AS(instance.evaluate(tick(1)), std::logic_error);
}

TEST_CASE("ChildGraphInstance move semantics transfer ownership", "[v2][nested][instance]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry    = hgraph::TSTypeRegistry::instance();
    const auto *int_type       = value_registry.register_type<int>("int");
    const auto *scalar_ts      = ts_registry.ts(int_type);

    GraphBuilder parent_builder;
    parent_builder.add_node(NodeBuilder{}.label("parent").output_schema(scalar_ts).implementation<NoopNode>());
    auto  engine       = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &parent_graph = engine.graph();
    parent_graph.start();

    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("inner").output_schema(scalar_ts).implementation<NoopNode>());
    auto tmpl = ChildGraphTemplate::create(std::move(child_builder), BoundaryBindingPlan{}, "child");

    ChildGraphInstance instance;
    instance.initialise(*tmpl, parent_graph.node_at(0), {0});
    instance.start(tick(1));

    // Move construct
    ChildGraphInstance moved{std::move(instance)};
    CHECK(moved.is_initialised());
    CHECK(moved.is_started());
    CHECK(moved.graph() != nullptr);

    CHECK_FALSE(instance.is_initialised());
    CHECK_FALSE(instance.is_started());

    // Clean up
    moved.stop(tick(2));
}

TEST_CASE("nested clock scheduling propagates through child graph evaluation", "[v2][nested][integration]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry    = hgraph::TSTypeRegistry::instance();
    const auto *int_type       = value_registry.register_type<int>("int");
    const auto *scalar_ts      = ts_registry.ts(int_type);

    // Parent graph with one noop node (acts as the nested operator)
    GraphBuilder parent_builder;
    parent_builder.add_node(NodeBuilder{}.label("parent").output_schema(scalar_ts).implementation<NoopNode>());
    auto  engine       = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &parent_graph = engine.graph();
    parent_graph.start();

    // Child graph with a scheduling node that requests next tick each eval
    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("scheduler").implementation<SchedulingNode>());
    auto tmpl = ChildGraphTemplate::create(std::move(child_builder), BoundaryBindingPlan{}, "child");

    ChildGraphInstance instance;
    instance.initialise(*tmpl, parent_graph.node_at(0), {0});
    instance.start(tick(1));

    // Schedule the child's scheduling node for tick(1) and evaluate.
    // The SchedulingNode will produce output and request rescheduling for tick(2).
    instance.graph()->schedule_node(0, tick(1));
    instance.evaluate(tick(1));

    // The child node should have produced output
    auto child_output = instance.graph()->node_at(0).output_view(tick(1));
    CHECK(child_output.value().as_atomic().as<int>() == 42);

    // The nested clock should have scheduled the next evaluation at tick(2).
    // The scheduling node requests MIN_TD from current time, so tick(1) + 1 = tick(2).
    CHECK(instance.next_scheduled_time() == tick(2));

    // The parent graph should be scheduled at tick(2) via clock delegation.
    CHECK(parent_graph.scheduled_time(0).time_since_epoch().count() == 2);
}

// ============================================================================
// Boundary Binding: BIND_DIRECT End-to-End Test
// ============================================================================

namespace hgraph::nested_test
{
    // A simple compute node for the child graph: reads input, adds 10, writes output.
    struct AddTenNode
    {
        AddTenNode()  = delete;
        ~AddTenNode() = delete;

        static void eval(In<"x", TS<int>> x, Out<TS<int>> out) { out.set(x.value() + 10); }
    };

    struct IdentityIntNode
    {
        IdentityIntNode()  = delete;
        ~IdentityIntNode() = delete;

        static void eval(In<"x", TS<int>> x, Out<TS<int>> out) { out.set(x.value()); }
    };

    struct RefStateProbeNode
    {
        RefStateProbeNode()  = delete;
        ~RefStateProbeNode() = delete;

        static void eval(In<"ref", REF<TS<int>>> ref, Out<TS<int>> out)
        {
            const auto &value = ref.value();
            if (!ref.valid()) {
                out.set(-1);
            } else if (value.is_empty()) {
                out.set(1);
            } else if (value.is_peered()) {
                out.set(2);
            } else {
                out.set(3);
            }
        }
    };

    struct DereferenceRefNode
    {
        DereferenceRefNode()  = delete;
        ~DereferenceRefNode() = delete;

        static void eval(In<"ref", REF<TS<int>>> ref, EvaluationClock clock, Out<TS<int>> out)
        {
            const auto &value = ref.value();
            if (!value.is_peered()) { return; }
            out.set(value.target_view(clock.evaluation_time()).value().as_atomic().as<int>());
        }
    };

    // Helper: publish a scalar value to a node's root output and mark it modified.
    template <typename T> void publish_output(Node &node, T &&value, engine_time_t modified_time) {
        auto output = node.output_view(modified_time);
        output.value().set_scalar(std::forward<T>(value));
        LinkedTSContext context = output.linked_context();
        if (context.ts_state == nullptr) { throw std::logic_error("test output leaf has no state to mark modified"); }
        context.ts_state->mark_modified(modified_time);
    }
}  // namespace hgraph::nested_test

TEST_CASE("BoundaryBindingRuntime bind_keyed binds a multiplexed list element", "[v2][nested][boundary]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry     = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry        = hgraph::TSTypeRegistry::instance();
    const auto *int_type           = value_registry.register_type<int>("int");
    const auto *scalar_ts          = ts_registry.ts(int_type);
    const auto *list_ts            = ts_registry.tsl(scalar_ts, 2);
    const auto *parent_input_schema = ts_registry.tsb({{"xs", list_ts}}, "BoundaryListInputs");

    GraphBuilder parent_builder;
    parent_builder.add_node(NodeBuilder{}.label("source0").output_schema(scalar_ts).implementation<NoopNode>())
        .add_node(NodeBuilder{}.label("source1").output_schema(scalar_ts).implementation<NoopNode>())
        .add_node(NodeBuilder{}.label("parent_holder").input_schema(parent_input_schema).output_schema(scalar_ts).implementation<NoopNode>())
        .add_edge(Edge{.src_node = 0, .dst_node = 2, .input_path = {0, 0}})
        .add_edge(Edge{.src_node = 1, .dst_node = 2, .input_path = {0, 1}});

    auto  parent_engine = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &parent_graph  = parent_engine.graph();
    parent_graph.start();

    publish_output(parent_graph.node_at(0), 11, tick(1));
    publish_output(parent_graph.node_at(1), 22, tick(1));

    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("identity").implementation<IdentityIntNode>());

    BoundaryBindingPlan plan;
    plan.inputs.push_back(InputBindingSpec{
        .arg_name         = "xs",
        .mode             = InputBindingMode::BIND_MULTIPLEXED_ELEMENT,
        .child_node_index = 0,
        .child_input_path = {0},
    });

    const ChildGraphTemplate *tmpl = ChildGraphTemplate::create(std::move(child_builder), plan, "list_child");

    ChildGraphInstance child;
    child.initialise(*tmpl, parent_graph.node_at(2), {2, 0}, "keyed_child");
    hgraph::Value key_value{1};
    BoundaryBindingRuntime::bind_keyed(
        child.boundary_plan(), *child.graph(), parent_graph.node_at(2), parent_graph.node_at(2).output_view(tick(1)), key_value.view(), tick(1));
    child.start(tick(1));
    child.graph()->schedule_node(0, tick(1));
    child.evaluate(tick(1));

    CHECK(child.graph()->node_at(0).output_view(tick(1)).value().as_atomic().as<int>() == 22);
}

TEST_CASE("BoundaryBindingRuntime bind_keyed injects key values and detach_restore_blank clears them",
          "[v2][nested][boundary]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry    = hgraph::TSTypeRegistry::instance();
    const auto *int_type       = value_registry.register_type<int>("int");
    const auto *scalar_ts      = ts_registry.ts(int_type);

    GraphBuilder parent_builder;
    parent_builder.add_node(NodeBuilder{}.label("parent_holder").output_schema(scalar_ts).implementation<NoopNode>());

    auto  parent_engine = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &parent_graph  = parent_engine.graph();
    parent_graph.start();

    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("identity").implementation<IdentityIntNode>());

    BoundaryBindingPlan plan;
    plan.inputs.push_back(InputBindingSpec{
        .mode             = InputBindingMode::BIND_KEY_VALUE,
        .child_node_index = 0,
        .child_input_path = {0},
    });
    plan.inputs.push_back(InputBindingSpec{
        .mode             = InputBindingMode::DETACH_RESTORE_BLANK,
        .child_node_index = 0,
        .child_input_path = {0},
    });

    const ChildGraphTemplate *tmpl = ChildGraphTemplate::create(std::move(child_builder), plan, "key_child");

    ChildGraphInstance child;
    child.initialise(*tmpl, parent_graph.node_at(0), {0, 0}, "key_child");
    hgraph::Value key_value{7};
    BoundaryBindingRuntime::bind_keyed(
        child.boundary_plan(), *child.graph(), parent_graph.node_at(0), parent_graph.node_at(0).output_view(tick(1)), key_value.view(), tick(1));
    child.start(tick(1));
    child.graph()->schedule_node(0, tick(1));
    child.evaluate(tick(1));

    CHECK(child.graph()->node_at(0).output_view(tick(1)).value().as_atomic().as<int>() == 7);

    BoundaryBindingRuntime::unbind(child.boundary_plan(), *child.graph());
    auto child_input = child.graph()->node_at(0).input_view(hgraph::MIN_DT).as_bundle()[0];
    CHECK_FALSE(child_input.valid());
}

TEST_CASE("BoundaryBindingRuntime clone_ref_binding preserves peered reference semantics", "[v2][nested][boundary]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry      = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry         = hgraph::TSTypeRegistry::instance();
    const auto *int_type            = value_registry.register_type<int>("int");
    const auto *scalar_ts           = ts_registry.ts(int_type);
    const auto *ref_scalar_ts       = ts_registry.ref(scalar_ts);
    const auto *parent_input_schema = ts_registry.tsb({{"ref", ref_scalar_ts}}, "BoundaryRefInputs");

    GraphBuilder parent_builder;
    parent_builder.add_node(NodeBuilder{}.label("source").output_schema(scalar_ts).implementation<NoopNode>())
        .add_node(NodeBuilder{}.label("parent_holder").input_schema(parent_input_schema).output_schema(scalar_ts).implementation<NoopNode>())
        .add_edge(Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto  parent_engine = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &parent_graph  = parent_engine.graph();
    parent_graph.start();

    publish_output(parent_graph.node_at(0), 41, tick(1));

    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("probe").implementation<RefStateProbeNode>());

    BoundaryBindingPlan plan;
    plan.inputs.push_back(InputBindingSpec{
        .arg_name         = "ref",
        .mode             = InputBindingMode::CLONE_REF_BINDING,
        .child_node_index = 0,
        .child_input_path = {0},
    });

    const ChildGraphTemplate *tmpl = ChildGraphTemplate::create(std::move(child_builder), plan, "ref_child");

    ChildGraphInstance child;
    child.initialise(*tmpl, parent_graph.node_at(1), {1, 0}, "ref_child");
    BoundaryBindingRuntime::bind(child.boundary_plan(), *child.graph(), parent_graph.node_at(1), tick(1));
    child.start(tick(1));
    child.graph()->schedule_node(0, tick(1));
    child.evaluate(tick(1));

    CHECK(child.graph()->node_at(0).output_view(tick(1)).value().as_atomic().as<int>() == 2);
}

TEST_CASE("BoundaryBindingRuntime rebind updates cloned REF targets", "[v2][nested][boundary]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry      = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry         = hgraph::TSTypeRegistry::instance();
    const auto *int_type            = value_registry.register_type<int>("int");
    const auto *scalar_ts           = ts_registry.ts(int_type);
    const auto *ref_scalar_ts       = ts_registry.ref(scalar_ts);
    const auto *parent_input_schema = ts_registry.tsb({{"ref", ref_scalar_ts}}, "BoundaryRefRebindInputs");

    GraphBuilder parent_builder;
    parent_builder.add_node(NodeBuilder{}.label("source0").output_schema(scalar_ts).implementation<NoopNode>())
        .add_node(NodeBuilder{}.label("source1").output_schema(scalar_ts).implementation<NoopNode>())
        .add_node(NodeBuilder{}.label("parent_holder").input_schema(parent_input_schema).output_schema(scalar_ts).implementation<NoopNode>());

    auto  parent_engine = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &parent_graph  = parent_engine.graph();
    parent_graph.start();

    publish_output(parent_graph.node_at(0), 10, tick(1));
    publish_output(parent_graph.node_at(1), 20, tick(2));

    auto parent_input = parent_graph.node_at(2).input_view(tick(1)).as_bundle()[0];
    parent_input.bind_output(parent_graph.node_at(0).output_view(tick(1)));

    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("deref").implementation<DereferenceRefNode>());

    BoundaryBindingPlan plan;
    plan.inputs.push_back(InputBindingSpec{
        .arg_name         = "ref",
        .mode             = InputBindingMode::CLONE_REF_BINDING,
        .child_node_index = 0,
        .child_input_path = {0},
    });

    const ChildGraphTemplate *tmpl = ChildGraphTemplate::create(std::move(child_builder), plan, "ref_rebind_child");

    ChildGraphInstance child;
    child.initialise(*tmpl, parent_graph.node_at(2), {2, 0}, "ref_rebind_child");
    BoundaryBindingRuntime::bind(child.boundary_plan(), *child.graph(), parent_graph.node_at(2), tick(1));
    child.start(tick(1));
    child.graph()->schedule_node(0, tick(1));
    child.evaluate(tick(1));

    CHECK(child.graph()->node_at(0).output_view(tick(1)).value().as_atomic().as<int>() == 10);

    parent_input = parent_graph.node_at(2).input_view(tick(2)).as_bundle()[0];
    parent_input.bind_output(parent_graph.node_at(1).output_view(tick(2)));
    BoundaryBindingRuntime::rebind(child.boundary_plan(), *child.graph(), parent_graph.node_at(2), "ref", tick(2));
    child.graph()->schedule_node(0, tick(2));
    child.evaluate(tick(2));

    CHECK(child.graph()->node_at(0).output_view(tick(2)).value().as_atomic().as<int>() == 20);
}

// ============================================================================
// Full Nested Operator: nested_graph_implementation end-to-end
// ============================================================================

TEST_CASE("nested_graph_implementation creates a working nested operator node", "[v2][nested][operator]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry      = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry         = hgraph::TSTypeRegistry::instance();
    const auto *int_type            = value_registry.register_type<int>("int");
    const auto *scalar_ts           = ts_registry.ts(int_type);
    const auto *nested_input_schema = ts_registry.tsb({{"x", scalar_ts}}, "NestedOpInputs");

    // ---- Build the child graph template ----
    // Child node 0: AddTenNode (input: TSB{x: TS<int>}, output: TS<int>)
    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("adder").implementation<AddTenNode>());

    BoundaryBindingPlan plan;
    plan.inputs.push_back(InputBindingSpec{
        .arg_name         = "x",
        .mode             = InputBindingMode::BIND_DIRECT,
        .child_node_index = 0,
        .child_input_path = {0},  // First field of adder's TSB input ("x")
    });
    plan.outputs.push_back(OutputBindingSpec{
        .mode               = OutputBindingMode::ALIAS_CHILD_OUTPUT,
        .child_node_index   = 0,
        .child_output_path  = {},
        .parent_output_path = {},
    });

    const ChildGraphTemplate *tmpl = ChildGraphTemplate::create(std::move(child_builder), std::move(plan), "add_ten",
                                                                ChildGraphTemplateFlags{.has_output = true});

    // ---- Build the parent graph ----
    // Node 0: source (NoopNode, output: TS<int>)
    // Node 1: nested operator (nested_graph_implementation, input: TSB{x: TS<int>}, output: TS<int>)
    // Edge: source → nested_op input field 0
    GraphBuilder parent_builder;

    NodeBuilder nested_op_builder;
    nested_op_builder.label("nested_op").input_schema(nested_input_schema).output_schema(scalar_ts);
    nested_graph_implementation(nested_op_builder, tmpl);

    parent_builder.add_node(NodeBuilder{}.label("source").output_schema(scalar_ts).implementation<NoopNode>())
        .add_node(std::move(nested_op_builder))
        .add_edge(Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto  engine = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &graph  = engine.graph();

    // ---- Start the parent graph (which starts the nested operator, which starts the child graph) ----
    graph.start();

    // ---- Publish a value to the source and evaluate ----
    publish_output(graph.node_at(0), 5, tick(1));

    // Schedule the nested operator for evaluation
    graph.schedule_node(1, tick(1));

    graph.evaluate(tick(1));

    // ---- Verify: nested operator output should be 15 (5 + 10) ----
    auto nested_output = graph.node_at(1).output_view(tick(1));
    CHECK(nested_output.value().as_atomic().as<int>() == 15);
    CHECK(nested_output.modified());
}

// ============================================================================
// Automatic Scheduling: nested operator triggers when parent input changes
// ============================================================================

TEST_CASE("nested operator is automatically scheduled when its input changes", "[v2][nested][scheduling]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;

    auto       &value_registry      = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry         = hgraph::TSTypeRegistry::instance();
    const auto *int_type            = value_registry.register_type<int>("int");
    const auto *scalar_ts           = ts_registry.ts(int_type);
    const auto *nested_input_schema = ts_registry.tsb({{"x", scalar_ts}}, "AutoSchedInputs");

    // Child template: adder (adds 10)
    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("adder").implementation<AddTenNode>());

    BoundaryBindingPlan plan;
    plan.inputs.push_back(InputBindingSpec{
        .arg_name         = "x",
        .mode             = InputBindingMode::BIND_DIRECT,
        .child_node_index = 0,
        .child_input_path = {0},
    });
    plan.outputs.push_back(OutputBindingSpec{
        .mode             = OutputBindingMode::ALIAS_CHILD_OUTPUT,
        .child_node_index = 0,
    });

    auto *tmpl = ChildGraphTemplate::create(std::move(child_builder), std::move(plan), "auto_sched",
                                            ChildGraphTemplateFlags{.has_output = true});

    // Parent graph: source → nested_op (NO manual schedule_node)
    GraphBuilder parent_builder;
    NodeBuilder  nested_op_builder;
    nested_op_builder.label("nested_op").input_schema(nested_input_schema).output_schema(scalar_ts);
    nested_graph_implementation(nested_op_builder, tmpl);

    parent_builder.add_node(NodeBuilder{}.label("source").output_schema(scalar_ts).implementation<NoopNode>())
        .add_node(std::move(nested_op_builder))
        .add_edge(Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto  engine = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &graph  = engine.graph();
    graph.start();

    // Publish a value to the source — this should automatically schedule
    // the nested operator via the notification chain:
    //   source output mark_modified → nested_op input notified → Node::notify → schedule_node
    publish_output(graph.node_at(0), 7, tick(1));

    // Verify the nested operator was automatically scheduled
    CHECK(graph.scheduled_time(1).time_since_epoch().count() == 1);  // tick(1)

    // Evaluate the parent graph — nested_op should run automatically
    graph.evaluate(tick(1));

    // Verify: nested operator output should be 17 (7 + 10)
    auto nested_output = graph.node_at(1).output_view(tick(1));
    CHECK(nested_output.value().as_atomic().as<int>() == 17);
    CHECK(nested_output.modified());

    // Second cycle: publish another value
    publish_output(graph.node_at(0), 3, tick(2));
    CHECK(graph.scheduled_time(1).time_since_epoch().count() == 2);

    graph.evaluate(tick(2));
    CHECK(graph.node_at(1).output_view(tick(2)).value().as_atomic().as<int>() == 13);
}

// ============================================================================
// try_except operator tests
// ============================================================================

TEST_CASE("try_except forwards child output to .out field on success", "[v2][nested][try_except]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;
    ensure_python_hgraph_importable();

    auto       &value_registry  = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry     = hgraph::TSTypeRegistry::instance();
    const auto *int_type        = value_registry.register_type<int>("int");
    const auto *node_error_type = value_registry.register_type<nanobind::object>("NodeError");
    const auto *scalar_ts       = ts_registry.ts(int_type);
    const auto *node_error_ts   = ts_registry.ts(node_error_type);

    // Output schema: TSB{exception: TS[NodeError], out: TS[int]}
    const auto *output_schema       = ts_registry.tsb({{"exception", node_error_ts}, {"out", scalar_ts}}, "TryExceptResult");
    const auto *nested_input_schema = ts_registry.tsb({{"x", scalar_ts}}, "TryExceptInputs");

    // Child: DivideNode (100 / x)
    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("divider").implementation<DivideNode>());

    BoundaryBindingPlan plan;
    plan.inputs.push_back(InputBindingSpec{
        .arg_name         = "x",
        .mode             = InputBindingMode::BIND_DIRECT,
        .child_node_index = 0,
        .child_input_path = {0},
    });
    plan.outputs.push_back(OutputBindingSpec{
        .mode             = OutputBindingMode::ALIAS_CHILD_OUTPUT,
        .child_node_index = 0,
    });

    auto *tmpl = ChildGraphTemplate::create(std::move(child_builder), std::move(plan), "try_except_child",
                                            ChildGraphTemplateFlags{.has_output = true});

    // Parent: source → try_except_op
    GraphBuilder parent_builder;
    NodeBuilder  try_op_builder;
    try_op_builder.label("try_except_op").input_schema(nested_input_schema).output_schema(output_schema);
    try_except_graph_implementation(try_op_builder, tmpl);

    parent_builder.add_node(NodeBuilder{}.label("source").output_schema(scalar_ts).implementation<NoopNode>())
        .add_node(std::move(try_op_builder))
        .add_edge(Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto  engine = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &graph  = engine.graph();
    graph.start();

    // ---- Normal path: 100 / 5 = 20 ----
    publish_output(graph.node_at(0), 5, tick(1));
    graph.evaluate(tick(1));

    // Output is TSB{exception, out}. Success should only modify the "out" field.
    auto output          = graph.node_at(1).output_view(tick(1));
    auto exception_field = output.as_bundle().field("exception");
    auto out_field       = output.as_bundle().field("out");
    CHECK_FALSE(exception_field.modified());
    CHECK(out_field.value().as_atomic().as<int>() == 20);
    CHECK(out_field.modified());
}

TEST_CASE("try_except catches exception, stops child, and subsequent evals are no-ops", "[v2][nested][try_except]") {
    using namespace hgraph;
    using namespace hgraph::nested_test;
    ChildTemplateRegistryResetGuard registry_guard;
    ensure_python_hgraph_importable();

    auto       &value_registry      = hgraph::value::TypeRegistry::instance();
    auto       &ts_registry         = hgraph::TSTypeRegistry::instance();
    const auto *int_type            = value_registry.register_type<int>("int");
    const auto *node_error_type     = value_registry.register_type<nanobind::object>("NodeError");
    const auto *scalar_ts           = ts_registry.ts(int_type);
    const auto *node_error_ts       = ts_registry.ts(node_error_type);
    const auto *output_schema       = ts_registry.tsb({{"exception", node_error_ts}, {"out", scalar_ts}}, "TryExceptResult2");
    const auto *nested_input_schema = ts_registry.tsb({{"x", scalar_ts}}, "TryExceptInputs2");

    // Child: DivideNode (throws on zero)
    GraphBuilder child_builder;
    child_builder.add_node(NodeBuilder{}.label("divider").implementation<DivideNode>());

    BoundaryBindingPlan plan;
    plan.inputs.push_back(InputBindingSpec{
        .arg_name         = "x",
        .mode             = InputBindingMode::BIND_DIRECT,
        .child_node_index = 0,
        .child_input_path = {0},
    });
    plan.outputs.push_back(OutputBindingSpec{
        .mode             = OutputBindingMode::ALIAS_CHILD_OUTPUT,
        .child_node_index = 0,
    });

    auto *tmpl = ChildGraphTemplate::create(std::move(child_builder), std::move(plan), "try_except_err",
                                            ChildGraphTemplateFlags{.has_output = true});

    GraphBuilder parent_builder;
    NodeBuilder  try_op_builder;
    try_op_builder.label("try_except_op").input_schema(nested_input_schema).output_schema(output_schema);
    try_except_graph_implementation(try_op_builder, tmpl);

    parent_builder.add_node(NodeBuilder{}.label("source").output_schema(scalar_ts).implementation<NoopNode>())
        .add_node(std::move(try_op_builder))
        .add_edge(Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto  engine = EvaluationEngineBuilder{}.graph_builder(std::move(parent_builder)).start_time(tick(0)).build();
    auto &graph  = engine.graph();
    graph.start();

    // ---- First cycle: normal path, 100 / 5 = 20 ----
    publish_output(graph.node_at(0), 5, tick(1));
    graph.evaluate(tick(1));

    auto output_tick_1   = graph.node_at(1).output_view(tick(1)).as_bundle();
    auto out_field       = output_tick_1.field("out");
    auto exception_field = output_tick_1.field("exception");
    CHECK(out_field.value().as_atomic().as<int>() == 20);
    CHECK_FALSE(exception_field.modified());

    // ---- Second cycle: error path, 100 / 0 throws ----
    // nested_node_eval catches the exception, stops the child graph, and
    // absorbs it (no error_output configured). The evaluation doesn't crash.
    publish_output(graph.node_at(0), 0, tick(2));
    REQUIRE_NOTHROW(graph.evaluate(tick(2)));

    // The exception field should be populated and the success field should stay untouched.
    auto output_tick_2 = graph.node_at(1).output_view(tick(2)).as_bundle();
    CHECK(output_tick_2.field("exception").modified());
    CHECK_FALSE(output_tick_2.field("out").modified());

    // ---- Third cycle: child is stopped, eval is a no-op ----
    publish_output(graph.node_at(0), 10, tick(3));
    REQUIRE_NOTHROW(graph.evaluate(tick(3)));
    auto output_tick_3 = graph.node_at(1).output_view(tick(3)).as_bundle();
    CHECK_FALSE(output_tick_3.field("exception").modified());
    CHECK_FALSE(output_tick_3.field("out").modified());
}
