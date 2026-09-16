#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using CounterState = TSB<"checkpoint_counter", Field<"total", TS<Int>>>;
    std::vector<Int> starts;

    struct Counter
    {
        static constexpr auto name = "checkpoint_counter";
        static void start(RecordableState<CounterState> state)
        {
            auto total = state.field<"total">();
            starts.push_back(total.valid() ? total.value().checked_as<Int>() : Int{-1});
            if (!total.valid()) { total.set(Int{0}); }
        }
        static void eval(In<"ts", TS<Int>> input, RecordableState<CounterState> state, Out<TS<Int>> out)
        {
            if (input.value() == -999) { throw std::runtime_error("failed day computation"); }
            auto total = state.field<"total">();
            const auto value = total.value().checked_as<Int>() + input.value();
            total.set(value);
            if (input.value() != 5) { out.set(value); }
        }
        static void stop(RecordableState<CounterState> state)
        {
            if (state.field<"total">().value().checked_as<Int>() < 0)
            {
                throw std::runtime_error("failed day stop");
            }
        }
    };
    struct Strategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> input)
        {
            return wire<Counter>(w, input);
        }
    };
    struct Component
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            return stdlib::component<Strategy>(w, "strategy", input);
        }
    };
    struct PassThrough
    {
        static constexpr auto name = "checkpoint_upstream_compute";
        static void eval(In<"ts", TS<Int>> input, Out<TS<Int>> out) { out.set(input.value()); }
    };
    struct OutsideConsumer
    {
        static constexpr auto name = "checkpoint_outside_consumer";
        static void eval(In<"ts", TS<Int>>) {}
    };
    struct ComputedIngress
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            return stdlib::component<Strategy>(w, "strategy", wire<PassThrough>(w, input));
        }
    };
    struct SharedIngress
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            (void)wire<OutsideConsumer>(w, input);
            return stdlib::component<Strategy>(w, "strategy", input);
        }
    };
    struct CapturedFailureStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> input)
        {
            auto output = wire<Counter>(w, input);
            (void)exception_time_series(output);
            return output;
        }
    };
    struct CapturedFailureComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            return stdlib::component<CapturedFailureStrategy>(w, "strategy", input);
        }
    };
    struct StartSource
    {
        static constexpr auto name = "checkpoint_start_source";
        static void start(NodeView node, DateTime now) { node.graph().schedule_node(node.node_index(), now); }
        static void eval(Scalar<"value", Int> value, Out<TS<Int>> out) { out.set(value.value()); }
    };
    template <Int FirstEvent> struct StartIngress
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>>)
        {
            return stdlib::component<Strategy>(w, "strategy", wire<StartSource>(w, Int{FirstEvent}));
        }
    };
    struct UntilTrueStrategy
    {
        static Port<TS<Bool>> compose(Wiring &w, NamedPort<"ts", TS<Bool>> input)
        {
            return wire<stdlib::until_true>(w, input).as<TS<Bool>>();
        }
    };
    struct UntilTrueComponent
    {
        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Bool>> input)
        {
            return stdlib::component<UntilTrueStrategy>(w, "strategy", input);
        }
    };
    struct FreezeStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"predicate", TS<Bool>> predicate,
                                     NamedPort<"ts", TS<Int>> input)
        {
            return wire<stdlib::freeze>(w, predicate, input).as<TS<Int>>();
        }
    };
    struct FreezeComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> predicate, Port<TS<Int>> input)
        {
            return stdlib::component<FreezeStrategy>(w, "strategy", predicate, input);
        }
    };
    struct ObserveMembershipAfterFirstValue
    {
        static void eval(In<"ts", TSD<Int, TS<Int>>> input, Out<TS<Int>> output)
        {
            output.set(static_cast<Int>(input.size()));
            input.base().make_structural_active();
        }
    };
    struct MembershipStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TSD<Int, TS<Int>>> input)
        {
            return wire<ObserveMembershipAfterFirstValue>(w, input);
        }
    };
    struct MembershipComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> input)
        {
            return stdlib::component<MembershipStrategy>(w, "strategy", input);
        }
    };
    EvalNodeRunOptions interval(Int begin, Int end)
    {
        return {.start_time = MIN_ST + MIN_TD * begin, .end_time = MIN_ST + MIN_TD * end};
    }
}

TEST_CASE("component checkpoint preserves until_true passivation across completed days", "[checkpoint][component]")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .load = [&] { return completed; },
        .commit = [&](const auto &image) { completed = image; }});
    CHECK_OUTPUT(eval_node_with_options<UntilTrueComponent>(interval(0, 2), values<Bool>(false, true)),
                 values<Bool>(false, true));
    REQUIRE(completed);
    CHECK_OUTPUT(eval_node_with_options<UntilTrueComponent>(interval(2, 4), values<Bool>(false, true)),
                 values<Bool>(none, none));
}

TEST_CASE("component checkpoint preserves freeze passivation across completed days", "[checkpoint][component]")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .load = [&] { return completed; },
        .commit = [&](const auto &image) { completed = image; }});
    CHECK_OUTPUT(eval_node_with_options<FreezeComponent>(interval(0, 2), values<Bool>(false, true), values<Int>(1, 2)),
                 values<Int>(1, 2));
    REQUIRE(completed);
    CHECK_OUTPUT(eval_node_with_options<FreezeComponent>(interval(2, 4), values<Bool>(false, true), values<Int>(3, 4)),
                 values<Int>(none, none));
}

TEST_CASE("component checkpoint preserves dynamically selected structural input activity", "[checkpoint][component]")
{
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .load = [&] { return completed; },
        .commit = [&](const auto &image) { completed = image; }});
    CHECK_OUTPUT(eval_node_with_options<MembershipComponent>(interval(0, 1),
        values<Value>(dict_delta<Int, TS<Int>>({{1, 10}}))), values<Int>(1));
    REQUIRE(completed);
    CHECK_OUTPUT(eval_node_with_options<MembershipComponent>(interval(1, 3),
        values<Value>(dict_delta<Int, TS<Int>>({{1, 20}}), dict_delta<Int, TS<Int>>({{2, 30}}))),
        values<Int>(none, 2));
}

TEST_CASE("component checkpoint restores hidden state before start without counting old input", "[checkpoint][component]")
{
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .commit = [&](const auto &image) { completed = image; }});
    starts.clear();
    CHECK_OUTPUT(eval_node_with_options<Component>(interval(0, 2), values<Int>(1, 2)), {1, 3});
    REQUIRE(completed);
    CHECK(completed->completed_until == MIN_ST + 2 * MIN_TD);
    CHECK(starts == std::vector<Int>{-1});
    const auto prior = *completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .load = [&] { return std::optional{prior}; },
        .commit = [&](const auto &image) { completed = image; }});
    CHECK_OUTPUT(eval_node_with_options<Component>(interval(2, 5), values<Int>(none, 3, 4)), {none, 6, 10});
    CHECK(starts == std::vector<Int>{-1, 3});
    REQUIRE(completed);
    CHECK(completed->completed_until == MIN_ST + 5 * MIN_TD);
}

TEST_CASE("component checkpoint saves hidden changes without public output and preserves prior output", "[checkpoint][component]")
{
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .commit = [&](const auto &image) { completed = image; }});
    CHECK_OUTPUT(eval_node_with_options<Component>(interval(0, 2), values<Int>(1, 5)), {1, none});
    REQUIRE(completed);
    bool saw_output = false;
    for (const auto &node : completed->graph.nodes)
    {
        if (node.recordable_state)
        {
            REQUIRE(node.output);
            CHECK(node.output->payload.view().checked_as<Int>() == 1);
            saw_output = true;
        }
    }
    CHECK(saw_output);
    const auto prior = *completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .load = [&] { return std::optional{prior}; },
        .commit = [&](const auto &image) { completed = image; }});
    CHECK_OUTPUT(eval_node_with_options<Component>(interval(2, 3), values<Int>(2)), {8});
}

TEST_CASE("component checkpoint never commits a failed evaluation or stop", "[checkpoint][component]")
{
    GlobalContext context;
    std::size_t commits = 0;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .commit = [&](const auto &) { ++commits; }});
    CHECK_THROWS_WITH(eval_node_with_options<Component>(interval(0, 2), values<Int>(1, -999)),
                      Catch::Matchers::ContainsSubstring("failed day computation"));
    CHECK(commits == 0);
    CHECK_THROWS_WITH(eval_node_with_options<Component>(interval(0, 1), values<Int>(-1)),
                      Catch::Matchers::ContainsSubstring("failed day stop"));
    CHECK(commits == 0);
}

TEST_CASE("component checkpoint refuses computed and shared external ingress before starting", "[checkpoint][component]")
{
    GlobalContext context;
    std::size_t commits = 0;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .commit = [&](const auto &) { ++commits; }});
    starts.clear();
    CHECK_THROWS_WITH(eval_node_with_options<ComputedIngress>(interval(0, 1), values<Int>(1)),
                      Catch::Matchers::ContainsSubstring("direct owned pull-source"));
    CHECK_THROWS_WITH(eval_node_with_options<SharedIngress>(interval(0, 1), values<Int>(1)),
                      Catch::Matchers::ContainsSubstring("consumed outside"));
    CHECK(starts.empty());
    CHECK(commits == 0);
}

TEST_CASE("component checkpoint refuses swallowed node failures before starting", "[checkpoint][component]")
{
    GlobalContext context;
    std::size_t commits = 0;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .commit = [&](const auto &) { ++commits; }});
    starts.clear();
    CHECK_THROWS_WITH(eval_node_with_options<CapturedFailureComponent>(interval(0, 1), values<Int>(-999)),
                      Catch::Matchers::ContainsSubstring("error capture"));
    CHECK(starts.empty());
    CHECK(commits == 0);
}

TEST_CASE("component checkpoint preserves source start scheduling for a new ingress event", "[checkpoint][component]")
{
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .load = [&] { return completed; },
        .commit = [&](const auto &image) { completed = image; }});
    starts.clear();
    CHECK_OUTPUT(eval_node_with_options<StartIngress<1>>(interval(0, 1), values<Int>(none)), {1});
    REQUIRE(completed);
    bool saved_ingress = false;
    for (const auto &node : completed->graph.nodes)
    {
        if (node.ingress)
        {
            CHECK(node.ingress->payload.view().checked_as<Int>() == 1);
            saved_ingress = true;
        }
    }
    CHECK(saved_ingress);
    CHECK_OUTPUT(eval_node_with_options<StartIngress<2>>(interval(1, 2), values<Int>(none)), {3});
    CHECK(starts == std::vector<Int>{-1, 1});
}

TEST_CASE("component checkpoint validates revision and recovery interval before starting", "[checkpoint][component]")
{
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .commit = [&](const auto &image) { completed = image; }});
    (void)eval_node_with_options<Component>(interval(0, 1), values<Int>(1));
    REQUIRE(completed);
    const auto prior = *completed;
    starts.clear();
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .revision = "2", .load = [&] { return std::optional{prior}; },
        .commit = [](const auto &) {}});
    CHECK_THROWS_WITH(eval_node_with_options<Component>(interval(1, 2), values<Int>(2)),
                      Catch::Matchers::ContainsSubstring("incompatible"));
    CHECK(starts.empty());
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .load = [&] { return std::optional{prior}; },
        .commit = [](const auto &) {}});
    CHECK_THROWS_WITH(eval_node_with_options<Component>(interval(0, 1), values<Int>(2)),
                      Catch::Matchers::ContainsSubstring("recovery must start"));
    CHECK(starts.empty());
}
