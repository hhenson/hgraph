// Static node authoring: declare compute / source / sink nodes as stateless
// structs with a typed eval(In<>, Out<>, State<>) signature, and have
// NodeBuilder::implementation<T>() build the runtime node. This is the C++
// static wiring coverage derived from the earlier implementation and adjusted
// to the current type-erased runtime (see docs: Wiring, Schemas > Static Schema).

#include <hgraph/runtime/runtime.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdint>

#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

namespace
{
    using namespace hgraph;

    // Source: Out only, no In -> kind inferred as PullSource. Writes a constant.
    struct ConstantSource
    {
        static constexpr auto name = "constant_source";

        static constexpr bool schedule_on_start = true;
        static void           eval(Out<TS<Int>> out) { out.set(Int{41}); }
    };

    // Compute: In + Out -> kind inferred as Compute.
    struct AddOne
    {
        static constexpr auto name = "add_one";

        static void eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value() + 1); }
    };

    // Compute with two named inputs.
    struct Sum
    {
        static constexpr auto name = "sum";
        static constexpr std::string_view implementation_label = "test.static.sum";

        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    struct PolicyProbe
    {
        static constexpr auto name = "policy_probe";

        static void eval(In<"lhs", TS<Int>> lhs,
                         In<"rhs", TS<Int>, InputActivity::Passive, InputValidity::Unchecked> rhs,
                         In<"strict", TSL<TS<Int>, 2>, InputValidity::AllValid> strict,
                         In<"keys", TSD<Int, TS<Int>>, InputActivity::Structural,
                            InputValidity::Unchecked> keys,
                         Out<TS<Int>> out)
        {
            (void)rhs;
            (void)strict;
            (void)keys;
            out.set(lhs.value());
        }
    };

    struct AllValidProbe
    {
        static constexpr auto name = "all_valid_probe";

        static void eval(In<"values", TSL<TS<Int>, 2>, InputValidity::AllValid>,
                         Out<TS<Bool>> out)
        {
            out.set(true);
        }
    };

    struct ActiveRelay
    {
        static constexpr auto name = "active_relay";

        static void eval(In<"in", TS<Int>, InputValidity::Unchecked> in, Out<TS<Int>> out)
        {
            out.set(in.valid() ? in.value() : Int{-1});
        }
    };

    struct PassiveRelay
    {
        static constexpr auto name = "passive_relay";

        static void eval(In<"in", TS<Int>, InputActivity::Passive, InputValidity::Unchecked> in, Out<TS<Int>> out)
        {
            out.set(in.valid() ? in.value() : Int{-1});
        }
    };

    struct DefaultMissingRhs
    {
        static constexpr auto name = "default_missing_rhs";

        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    struct UncheckedMissingRhs
    {
        static constexpr auto name = "unchecked_missing_rhs";

        static void eval(In<"lhs", TS<Int>> lhs,
                         In<"rhs", TS<Int>, InputValidity::Unchecked> rhs,
                         Out<TS<Int>> out)
        {
            out.set(lhs.value() + (rhs.valid() ? rhs.value() : Int{100}));
        }
    };

    // Stateful source (Out only, no In -> PullSource) exercising State<Int>.
    struct Counter
    {
        static constexpr auto name = "counter";

        static void start(State<Int> state) { state.set(Int{0}); }

        static void eval(State<Int> state, Out<TS<Int>> out)
        {
            const Int next = state.get() + 1;
            state.set(next);
            out.set(next);
        }
    };

    using LastSeenState = TSB<"LastSeenState", Field<"last", TS<Int>>>;

    using ClockSnapshot = TSB<"ClockSnapshot",
                              Field<"evaluation_time", TS<DateTime>>,
                              Field<"now", TS<DateTime>>,
                              Field<"next_cycle", TS<DateTime>>,
                              Field<"cycle_time_us", TS<Int>>>;

    struct ClockProbe
    {
        static constexpr auto name              = "clock_probe";
        static constexpr bool schedule_on_start = true;

        static void eval(EvaluationClockView clock, DateTime evaluation_time, Out<ClockSnapshot> out)
        {
            out.field<"evaluation_time">().set(clock.evaluation_time());
            out.field<"now">().set(clock.now());
            out.field<"next_cycle">().set(clock.next_cycle_evaluation_time());
            out.field<"cycle_time_us">().set(Int{clock.cycle_time().count()});

            if (clock.evaluation_time() != evaluation_time)
            {
                throw std::logic_error("DateTime injection does not match EvaluationClockView");
            }
        }
    };

    struct GlobalStateProbe
    {
        static constexpr auto name = "global_state_probe";

        static void eval(GlobalStateView state, Out<TS<Int>> out)
        {
            out.set(state.get_as<Int>("seed"));
        }
    };

    struct RecordablePreviousValue
    {
        static constexpr auto name = "recordable_previous_value";

        static void eval(In<"in", TS<Int>> in,
                         RecordableState<LastSeenState> state,
                         Out<TS<Int>> out)
        {
            auto last = state.field<"last">();
            const Int previous = last.valid() ? last.value().checked_as<Int>() : Int{-1};
            out.set(previous);
            last.set(in.value());
        }
    };

    // Source configured by a scalar input (no time-series inputs -> PullSource).
    // Emits its configured value; the scalar is read-only wiring-time config.
    struct ScaledSource
    {
        static constexpr auto name = "scaled_source";

        static void eval(Scalar<"value", Int> value, Out<TS<Int>> out) { out.set(value.value()); }
    };

    // Compute node mixing a time-series input with a scalar input. The scalar
    // is configuration (not part of the input TSB); Out is last by convention.
    struct Shift
    {
        static constexpr auto name = "shift";

        static void eval(In<"in", TS<Int>> in, Scalar<"delta", Int> delta, Out<TS<Int>> out)
        {
            out.set(in.value() + delta.value());
        }
    };

    struct LifecycleConfiguredSource
    {
        static constexpr auto name = "lifecycle_configured_source";
        static constexpr bool schedule_on_start = true;
        inline static Int     stopped_value{-1};
        using signature_args =
            std::tuple<Scalar<"value", Int>, Scalar<"lifecycle", Int>, State<Int>, Out<TS<Int>>>;

        static void start(Scalar<"lifecycle", Int> lifecycle, State<Int> state)
        {
            state.set(lifecycle.value());
        }

        static void eval(Scalar<"value", Int> value, State<Int> state, Out<TS<Int>> out)
        {
            out.set(value.value() + state.get());
        }

        static void stop(Scalar<"lifecycle", Int> lifecycle, State<Int> state)
        {
            stopped_value = state.get() + lifecycle.value();
        }
    };

    struct DuplicateInputNames
    {
        static void eval(In<"x", TS<Int>>, In<"x", TS<Int>>, Out<TS<Int>>);
    };

    struct DuplicateScalarNames
    {
        static void eval(Scalar<"x", Int>, Scalar<"x", Int>, Out<TS<Int>>);
    };

    struct MultipleStateSlots
    {
        static void eval(State<Int>, State<Int>, Out<TS<Int>>);
    };

    struct MultipleRecordableStateSlots
    {
        static void eval(RecordableState<LastSeenState>, RecordableState<LastSeenState>, Out<TS<Int>>);
    };

    // Build a single-field compound scalar configuration {field: value}.
    Value int_scalar_config(std::string_view field, Int value)
    {
        auto       &registry    = TypeRegistry::instance();
        const auto *int_meta    = registry.register_scalar<Int>("int");
        const auto *bundle_meta = registry.un_named_bundle({{std::string{field}, int_meta}});
        const auto binding     = ValuePlanFactory::instance().type_for(bundle_meta);

        Value scalars{binding};
        {
            auto mutation = scalars.as_bundle().begin_mutation();
            mutation[field].checked_mutable_as<Int>() = value;
        }
        return scalars;
    }
}  // namespace

TEST_CASE("static node: node kind is inferred from In/Out selectors")
{
    using namespace hgraph;

    auto source = NodeBuilder{}.label("src").implementation<ConstantSource>().make_node();
    CHECK(source.view().node_kind() == NodeKind::PullSource);
    CHECK(source.view().has_output());
    CHECK_FALSE(source.view().has_input());

    auto compute = NodeBuilder{}.label("inc").implementation<AddOne>().make_node();
    CHECK(compute.view().node_kind() == NodeKind::Compute);
    CHECK(compute.view().has_input());
    CHECK(compute.view().has_output());
}

TEST_CASE("static node: implementation identity is carried by the node type record")
{
    using namespace hgraph;

    NodeBuilder builder;
    builder.implementation<Sum>();

    REQUIRE(builder.type().valid());
    CHECK(std::string{builder.type().record()->semantic_name()} == "sum");
    CHECK(std::string{builder.type().record()->implementation_name()} == "test.static.sum");

    const std::array<std::size_t, 1> passive{0};
    const NodeBuilder derived = builder.with_passive_inputs(passive);
    CHECK(std::string{derived.type().record()->implementation_name()} == "test.static.sum");
}

TEST_CASE("static node: signature detects ambiguous selector contracts")
{
    using namespace hgraph;

    STATIC_REQUIRE(StaticNodeSignature<Sum>::input_names_unique());
    STATIC_REQUIRE(StaticNodeSignature<Shift>::scalar_names_unique());
    STATIC_REQUIRE(StaticNodeSignature<Counter>::state_count() == 1);
    STATIC_REQUIRE(StaticNodeSignature<RecordablePreviousValue>::state_count() == 0);
    STATIC_REQUIRE(StaticNodeSignature<RecordablePreviousValue>::recordable_state_count() == 1);

    STATIC_REQUIRE_FALSE(StaticNodeSignature<DuplicateInputNames>::input_names_unique());
    STATIC_REQUIRE_FALSE(StaticNodeSignature<DuplicateScalarNames>::scalar_names_unique());
    STATIC_REQUIRE(StaticNodeSignature<MultipleStateSlots>::state_count() == 2);
    STATIC_REQUIRE(StaticNodeSignature<MultipleRecordableStateSlots>::recordable_state_count() == 2);
}

TEST_CASE("static node: input policy flags are reflected into node metadata")
{
    using namespace hgraph;

    STATIC_REQUIRE(In<"x", TS<Int>, InputValidity::Unchecked, InputActivity::Passive>::activity ==
                   InputActivity::Passive);
    STATIC_REQUIRE(In<"x", TS<Int>, InputValidity::Unchecked, InputActivity::Passive>::validity ==
                   InputValidity::Unchecked);
    STATIC_REQUIRE(In<"x", TS<Int>, InputActivity::Passive, InputValidity::Unchecked>::activity ==
                   InputActivity::Passive);
    STATIC_REQUIRE(In<"x", TS<Int>, InputActivity::Passive, InputValidity::Unchecked>::validity ==
                   InputValidity::Unchecked);

    auto node = NodeBuilder{}.label("policy").implementation<PolicyProbe>().make_node();
    auto view = node.view();

    REQUIRE(view.schema()->active_inputs.has_value());
    REQUIRE(view.schema()->valid_inputs.has_value());
    CHECK(*view.schema()->active_inputs == std::vector<std::size_t>{0, 2});
    CHECK(view.schema()->structural_inputs == std::vector<std::size_t>{3});
    CHECK(*view.schema()->valid_inputs == std::vector<std::size_t>{0});
    CHECK(view.schema()->all_valid_inputs == std::vector<std::size_t>{2});
}

TEST_CASE("static node: all-valid inputs guard evaluation through public eval_node")
{
    using namespace hgraph;

    const std::vector<std::optional<Value>> input{
        list_delta<TS<Int>>({{0, 1}}),
        list_delta<TS<Int>>({{1, 2}}),
    };
    CHECK_OUTPUT(testing::eval_node<AllValidProbe>(input), {std::nullopt, Bool{true}});
}

TEST_CASE("static node: set_delta construction survives value registry resets")
{
    using namespace hgraph;

    {
        (void)TypeRegistry::instance().register_scalar<Int>("int");
        const Value first = set_delta<Int>({1}, {});  // canonical Bundle{added:{1}, removed:{}}
        CHECK(first.equals(set_delta<Int>({1}, {})));
    }

    reset_all_registries();

    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const Value second = set_delta<Int>({2}, {1});
    CHECK(second.equals(set_delta<Int>({2}, {1})));       // order-independent, type-erased
    CHECK_FALSE(second.equals(set_delta<Int>({2}, {})));  // different removed -> not equal
}

TEST_CASE("static node: source -> compute graph runs in simulation mode")
{
    using namespace hgraph;

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.label("src").implementation<ConstantSource>())
        .add_node(NodeBuilder{}.label("inc").implementation<AddOne>())
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}});

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{42});
}

TEST_CASE("static node: two sources feed a two-input compute node")
{
    using namespace hgraph;

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.label("a").implementation<ConstantSource>())
        .add_node(NodeBuilder{}.label("b").implementation<ConstantSource>())
        .add_node(NodeBuilder{}.label("sum").implementation<Sum>())
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 2, .target_path = {0}})
        .add_edge(GraphEdge{.source_node = 1, .source_path = {}, .target_node = 2, .target_path = {1}});

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Int>() == Int{82});
}

TEST_CASE("static node: passive inputs do not activate compute nodes")
{
    using namespace hgraph;

    GraphBuilder active_builder;
    active_builder.add_node(NodeBuilder{}.label("src").implementation<ConstantSource>())
        .add_node(NodeBuilder{}.label("active").implementation<ActiveRelay>())
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}});

    GraphExecutorBuilder active_executor_builder;
    active_executor_builder.graph_builder(std::move(active_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue active_executor      = active_executor_builder.make_executor();
    auto               active_executor_view = active_executor.view();
    active_executor_view.run();
    CHECK(active_executor_view.graph().node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{41});

    GraphBuilder passive_builder;
    passive_builder.add_node(NodeBuilder{}.label("src").implementation<ConstantSource>())
        .add_node(NodeBuilder{}.label("passive").implementation<PassiveRelay>())
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}});

    GraphExecutorBuilder passive_executor_builder;
    passive_executor_builder.graph_builder(std::move(passive_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue passive_executor      = passive_executor_builder.make_executor();
    auto               passive_executor_view = passive_executor.view();
    passive_executor_view.run();
    CHECK_FALSE(passive_executor_view.graph().node_at(1).output(MIN_ST).valid());
}

TEST_CASE("static node: unchecked inputs do not block readiness")
{
    using namespace hgraph;

    GraphBuilder default_builder;
    default_builder.add_node(NodeBuilder{}.label("src").implementation<ConstantSource>())
        .add_node(NodeBuilder{}.label("default").implementation<DefaultMissingRhs>())
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}});

    GraphExecutorBuilder default_executor_builder;
    default_executor_builder.graph_builder(std::move(default_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue default_executor      = default_executor_builder.make_executor();
    auto               default_executor_view = default_executor.view();
    default_executor_view.run();
    CHECK_FALSE(default_executor_view.graph().node_at(1).output(MIN_ST).valid());

    GraphBuilder unchecked_builder;
    unchecked_builder.add_node(NodeBuilder{}.label("src").implementation<ConstantSource>())
        .add_node(NodeBuilder{}.label("unchecked").implementation<UncheckedMissingRhs>())
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}});

    GraphExecutorBuilder unchecked_executor_builder;
    unchecked_executor_builder.graph_builder(std::move(unchecked_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue unchecked_executor      = unchecked_executor_builder.make_executor();
    auto               unchecked_executor_view = unchecked_executor.view();
    unchecked_executor_view.run();
    CHECK(unchecked_executor_view.graph().node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{141});
}

TEST_CASE("static node: Scalar<> configures a source from per-instance values")
{
    using namespace hgraph;

    auto node = NodeBuilder{}
                    .label("scaled")
                    .implementation<ScaledSource>()
                    .scalars(int_scalar_config("value", Int{7}))
                    .make_node();

    auto view = node.view();
    REQUIRE(view.node_kind() == NodeKind::PullSource);   // Scalar is config, not a TS input
    REQUIRE(view.schema()->has_scalars());
    REQUIRE(view.has_scalars());
    REQUIRE_FALSE(view.has_input());
    REQUIRE(view.scalars().as_bundle().field("value").checked_as<Int>() == Int{7});

    const auto t1 = MIN_ST;
    view.start(t1);
    view.evaluate(t1);
    CHECK(node.view().output(t1).value().checked_as<Int>() == Int{7});
}

TEST_CASE("static node: Scalar<> coexists with a time-series input")
{
    using namespace hgraph;

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.label("src").implementation<ConstantSource>())
        .add_node(NodeBuilder{}.label("shift").implementation<Shift>().scalars(int_scalar_config("delta", Int{5})))
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}});

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    // Compute node: Compute kind (In present), one TS input field, scalar excluded.
    REQUIRE(graph.node_at(1).node_kind() == NodeKind::Compute);
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{46});   // 41 + 5
}

TEST_CASE("static node: explicit signature keeps lifecycle-only selectors out of eval")
{
    using namespace hgraph;

    NodeBuilder builder;
    builder.implementation<LifecycleConfiguredSource>();
    REQUIRE(builder.type().schema()->scalar_schema != nullptr);
    REQUIRE(builder.type().schema()->scalar_schema->field_count == 2);
    CHECK(std::string{builder.type().schema()->scalar_schema->fields[0].name} == "value");
    CHECK(std::string{builder.type().schema()->scalar_schema->fields[1].name} == "lifecycle");

    LifecycleConfiguredSource::stopped_value = Int{-1};
    CHECK_OUTPUT(testing::eval_node<LifecycleConfiguredSource>(
                     arg<"value">(Int{7}), arg<"lifecycle">(Int{5})),
                 {Int{12}});
    CHECK(LifecycleConfiguredSource::stopped_value == Int{10});
}

TEST_CASE("static node: State<Int> is constructed and mutated across evaluations")
{
    using namespace hgraph;

    auto       node = NodeBuilder{}.label("counter").implementation<Counter>().make_node();
    const auto t1   = MIN_ST;
    const auto t2   = t1 + TimeDelta{1};

    auto view = node.view();
    REQUIRE(view.has_state());

    view.start(t1);
    view.evaluate(t1);
    CHECK(node.view().state().checked_as<Int>() == 1);
    CHECK(node.view().output(t1).value().checked_as<Int>() == 1);

    node.view().evaluate(t2);
    CHECK(node.view().state().checked_as<Int>() == 2);
    CHECK(node.view().output(t2).value().checked_as<Int>() == 2);
}

TEST_CASE("static node: EvaluationClockView is injected as a read-only clock view")
{
    using namespace hgraph;

    GraphBuilder graph_builder;
    graph_builder.label("clock_probe_graph")
        .add_node(NodeBuilder{}.label("clock").implementation<ClockProbe>());

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 1);
    auto output_view = graph.node_at(0).output(MIN_ST);
    auto output      = output_view.as_bundle();

    const DateTime evaluation_time = output.field("evaluation_time").value().checked_as<DateTime>();
    const DateTime now             = output.field("now").value().checked_as<DateTime>();
    const DateTime next_cycle      = output.field("next_cycle").value().checked_as<DateTime>();
    const Int      cycle_time_us   = output.field("cycle_time_us").value().checked_as<Int>();

    CHECK(evaluation_time == MIN_ST);
    CHECK(next_cycle == MIN_ST + MIN_TD);
    CHECK(now >= evaluation_time);
    CHECK(cycle_time_us >= Int{0});
}

TEST_CASE("static node: EvaluationClockView cache storage is allocated only when injected")
{
    using namespace hgraph;

    NodeBuilder clock_builder;
    clock_builder.implementation<ClockProbe>();
    CHECK(clock_builder.type().schema()->uses_evaluation_clock);
    CHECK(clock_builder.type().checked_plan().find_component("evaluation_clock") != nullptr);

    NodeBuilder ordinary_builder;
    ordinary_builder.implementation<Counter>();
    CHECK_FALSE(ordinary_builder.type().schema()->uses_evaluation_clock);
    CHECK(ordinary_builder.type().checked_plan().find_component("evaluation_clock") == nullptr);
}

TEST_CASE("static node: GlobalStateView cache storage is allocated only when injected")
{
    using namespace hgraph;

    NodeBuilder state_builder;
    state_builder.implementation<GlobalStateProbe>();
    CHECK(state_builder.type().schema()->uses_global_state);
    CHECK(state_builder.type().checked_plan().find_component("global_state") != nullptr);

    NodeBuilder ordinary_builder;
    ordinary_builder.implementation<Counter>();
    CHECK_FALSE(ordinary_builder.type().schema()->uses_global_state);
    CHECK(ordinary_builder.type().checked_plan().find_component("global_state") == nullptr);
}

TEST_CASE("graph executor graph is constructed as a root graph")
{
    using namespace hgraph;

    GraphBuilder graph_builder;
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder));

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               executor_view = executor.view();
    auto               graph = executor_view.graph();
    auto               root = graph.as_root();
    auto               graph_executor = root.executor();

    CHECK(graph.parent_kind() == GraphParentKind::Root);
    CHECK(graph.is_root());
    CHECK_FALSE(graph.is_nested());
    REQUIRE(graph_executor.valid());
    CHECK(graph_executor.type() == executor_view.type());
    CHECK(graph_executor.data() == executor_view.data());
    CHECK_THROWS_AS(graph.as_nested(), std::logic_error);
    CHECK_THROWS_AS(GraphBuilder{}.make_root_graph(ExecutorPtr{}), std::invalid_argument);
}

TEST_CASE("static node: RecordableState<TSB> is hidden output-backed state")
{
    using namespace hgraph;

    auto       source = NodeBuilder{}.label("source").implementation<Counter>().make_node();
    auto       node   = NodeBuilder{}.label("previous").implementation<RecordablePreviousValue>().make_node();
    const auto t1   = MIN_ST;
    const auto t2   = t1 + TimeDelta{1};

    auto view = node.view();
    REQUIRE(view.node_kind() == NodeKind::Compute);
    REQUIRE(view.has_input());
    REQUIRE(view.has_output());
    REQUIRE_FALSE(view.has_state());
    REQUIRE(view.has_recordable_state());
    REQUIRE(view.schema()->recordable_state_schema == schema_descriptor<LastSeenState>::ts_meta());
    REQUIRE(view.schema()->output_schema == schema_descriptor<TS<Int>>::ts_meta());
    REQUIRE(view.type().checked_plan().find_component("recordable_state") != nullptr);

    source.view().start(t1);
    view.start(t1);

    source.view().evaluate(t1);
    {
        auto root   = view.input(t1);
        auto bundle = root.as_bundle();
        auto input  = bundle.field("in");
        input.bind_output(source.view().output(t1));
    }
    view.evaluate(t1);
    CHECK(node.view().output(t1).value().checked_as<Int>() == Int{-1});
    {
        auto recordable = node.view().recordable_state(t1);
        auto bundle     = recordable.as_bundle();
        auto last       = bundle.field("last");
        REQUIRE(last.valid());
        CHECK(last.value().checked_as<Int>() == Int{1});
    }

    source.view().evaluate(t2);
    node.view().evaluate(t2);
    CHECK(node.view().output(t2).value().checked_as<Int>() == Int{1});
    {
        auto recordable = node.view().recordable_state(t2);
        auto bundle     = recordable.as_bundle();
        auto last       = bundle.field("last");
        REQUIRE(last.valid());
        CHECK(last.value().checked_as<Int>() == Int{2});
        CHECK(last.modified());
    }
}
