#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/runtime/checkpoint_codec.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

namespace
{
    using namespace hgraph;
    struct DerivedCache
    {
        std::vector<Int> values;
        std::shared_ptr<Int> lifetime;
        friend bool operator==(const DerivedCache &, const DerivedCache &) = default;
    };
}

namespace hgraph
{
    template <> struct scalar_descriptor<DerivedCache>
    {
        static constexpr bool is_concrete() noexcept { return true; }
        static const ValueTypeMetaData *value_meta()
        {
            return TypeRegistry::instance().register_scalar<DerivedCache>("checkpoint_derived_cache");
        }
    };
}

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
    std::weak_ptr<Int> cache_lifetime;
    bool fail_cache_start = false;
    std::vector<Int> cache_stops;

    struct CachedCounter
    {
        static constexpr auto name = "checkpoint_cached_counter";
        static void start(State<DerivedCache> cache, RecordableState<CounterState> state)
        {
            REQUIRE(cache.ref().values.empty());
            REQUIRE_FALSE(cache.ref().lifetime);
            cache.modify().lifetime = std::make_shared<Int>(42);
            cache_lifetime = cache.ref().lifetime;
            auto total = state.field<"total">();
            starts.push_back(total.valid() ? total.value().checked_as<Int>() : Int{-1});
            if (!total.valid()) { total.set(Int{0}); }
            cache.modify().values = {state.field<"total">().value().checked_as<Int>()};
            if (fail_cache_start) { throw std::runtime_error("cache start failed"); }
        }
        static void eval(In<"ts", TS<Int>> input, State<DerivedCache> cache,
                         RecordableState<CounterState> state, Out<TS<Int>> out)
        {
            REQUIRE(cache.ref().values.at(0) == state.field<"total">().value().checked_as<Int>());
            if (input.value() == -999) { throw std::runtime_error("failed day computation"); }
            auto total = state.field<"total">();
            const auto value = total.value().checked_as<Int>() + input.value();
            total.set(value);
            if (input.value() != 5) { out.set(value); }
            cache.modify().values[0] = state.field<"total">().value().checked_as<Int>();
        }
        static void stop(State<DerivedCache> cache, RecordableState<CounterState> state)
        {
            cache_stops.push_back(cache.ref().values.at(0));
            Counter::stop(std::move(state));
        }
    };
    struct CachedSink
    {
        static void start(State<DerivedCache> cache, RecordableState<CounterState> state)
        {
            CachedCounter::start(std::move(cache), std::move(state));
        }
        static void eval(In<"ts", TS<Int>> input, State<DerivedCache> cache, RecordableState<CounterState> state)
        {
            REQUIRE(cache.ref().values[0] == state.field<"total">().value().checked_as<Int>());
            cache.modify().values[0] += input.value();
            state.field<"total">().set(cache.ref().values[0]);
        }
        static void stop(State<DerivedCache> cache, RecordableState<CounterState> state)
        {
            CachedCounter::stop(std::move(cache), std::move(state));
        }
    };
    struct CachedSinkStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> input)
        {
            (void)wire<CachedSink>(w, input);
            return wire<Counter>(w, input);
        }
    };
    struct CachedSinkComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            return stdlib::component<CachedSinkStrategy>(w, "strategy", input);
        }
    };
    struct CachedStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> input)
        {
            return wire<CachedCounter>(w, input);
        }
    };
    struct CachedComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            return stdlib::component<CachedStrategy>(w, "strategy", input);
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
    struct PendingAlarms
    {
        static void start(NodeScheduler scheduler)
        {
            scheduler.schedule(MIN_TD * 2, "first");
            scheduler.schedule(MIN_TD * 4, "second");
            scheduler.schedule(MIN_TD * 6, "cancelled");
            scheduler.un_schedule("cancelled");
        }
        static void eval(In<"ts", TS<Int>> input, NodeScheduler scheduler, Out<TS<Int>> out)
        {
            if (input.modified() && input.value() == -1)
            {
                scheduler.un_schedule("second");
                scheduler.schedule(MIN_TD, "first");
            }
            if (scheduler.tag_is_scheduled_now("first")) { out.set(input.value()); }
            if (scheduler.tag_is_scheduled_now("second")) { out.set(input.value() * 2); }
        }
    };
    struct CachedAlarms
    {
        static void start(State<DerivedCache> cache, RecordableState<CounterState> state,
                          NodeScheduler scheduler)
        {
            CachedCounter::start(std::move(cache), std::move(state));
            scheduler.schedule(MIN_TD * 2, "publish");
        }
        static void eval(In<"ts", TS<Int>> input, State<DerivedCache> cache,
                         RecordableState<CounterState> state, NodeScheduler scheduler, Out<TS<Int>> out)
        {
            REQUIRE(cache.ref().values.at(0) == state.field<"total">().value().checked_as<Int>());
            if (input.modified())
            {
                cache.modify().values[0] += input.value();
                state.field<"total">().set(cache.ref().values[0]);
            }
            if (scheduler.tag_is_scheduled_now("publish")) { out.set(cache.ref().values[0]); }
        }
    };
    struct CachedAlarmStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> input)
        {
            return wire<CachedAlarms>(w, input);
        }
    };
    struct CachedAlarmComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            return stdlib::component<CachedAlarmStrategy>(w, "strategy", input);
        }
    };
    struct AlarmStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> input)
        {
            return wire<PendingAlarms>(w, input);
        }
    };
    struct AlarmComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            return stdlib::component<AlarmStrategy>(w, "strategy", input);
        }
    };
    struct ImmediateAlarmInput
    {
        static void start(Scalar<"value", Int> value, Out<TS<Int>> out) { out.set(value.value()); }
        static void eval(Scalar<"value", Int>, Out<TS<Int>>) {}
    };
    template <Int Value> struct ImmediateAlarmComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>>)
        {
            return stdlib::component<AlarmStrategy>(w, "strategy", wire<ImmediateAlarmInput>(w, Int{Value}));
        }
    };
    struct MappedAlarms
    {
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w, NamedPort<"ts", TSD<Str, TS<Int>>> input)
        {
            return wire<stdlib::map_>(w, fn<PendingAlarms>(), input).as<TSD<Str, TS<Int>>>();
        }
    };
    struct MappedAlarmComponent
    {
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w, Port<TSD<Str, TS<Int>>> input)
        {
            return stdlib::component<MappedAlarms>(w, "strategy", input);
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

TEST_CASE("component checkpoint restores pending alarms independently of recordable state", "[checkpoint][component][scheduler]")
{
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .load = [&] { return completed; },
        .commit = [&](const auto &image) {
            std::string bytes;
            encode_component_checkpoint(image, bytes);
            completed = decode_component_checkpoint(bytes);
        }});
    CHECK_OUTPUT(eval_node_with_options<AlarmComponent>(interval(0, 2), values<Int>(7, none)), values<Int>(none, none));
    REQUIRE(completed);
    const auto alarm = std::ranges::find_if(completed->graph.nodes, [](const auto &node) { return node.scheduler.has_value(); });
    REQUIRE(alarm != completed->graph.nodes.end());
    CHECK_FALSE(alarm->recordable_state);
    REQUIRE(alarm->scheduler->events.size() == 2);
    CHECK(alarm->scheduler->events.front() == std::pair<DateTime, std::string>{MIN_ST + MIN_TD * 2, "first"});

    SECTION("deadlines at restart and later survive repeated checkpoints")
    {
        CHECK_OUTPUT(eval_node_with_options<AlarmComponent>(interval(2, 4), values<Int>(none, none)), values<Int>(7, none));
        CHECK_OUTPUT(eval_node_with_options<AlarmComponent>(interval(4, 5), values<Int>(none)), values<Int>(14));
        // An empty image clears the next run's bootstrap alarms too.
        CHECK_OUTPUT(eval_node_with_options<AlarmComponent>(interval(5, 12), values<Int>(none, none, none, none, none, none, none)), values<Int>(none, none, none, none, none, none, none));
    }
    SECTION("restored tags can be replaced and cancelled by fresh input")
    {
        CHECK_OUTPUT(eval_node_with_options<AlarmComponent>(interval(2, 7), values<Int>(-1, none, none, none, none)), values<Int>(none, -1, none, none, none));
    }
    SECTION("a restart after a pending deadline is refused")
    {
        CHECK_THROWS_WITH(eval_node_with_options<AlarmComponent>(interval(3, 5), values<Int>(none)),
                          Catch::Matchers::ContainsSubstring("deadline precedes restart"));
    }
}

TEST_CASE("component checkpoint propagates restored child alarms to the enclosing graph", "[checkpoint][component][scheduler]")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .load = [&] { return completed; },
        .commit = [&](const auto &image) { completed = image; }});
    CHECK_OUTPUT(eval_node_with_options<MappedAlarmComponent>(interval(0, 2),
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 7}}), none)), values<Value>(dict_delta<Str, TS<Int>>({}), none));
    CHECK_OUTPUT(eval_node_with_options<MappedAlarmComponent>(interval(2, 5), values<Value>(none)),
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 7}}), none, dict_delta<Str, TS<Int>>({{"a", 14}})));
}

TEST_CASE("restored future alarms preserve fresh input ticks at restart", "[checkpoint][component][scheduler]")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .load = [&] { return completed; },
        .commit = [&](const auto &image) { completed = image; }});
    SECTION("input published during start")
    {
        CHECK_OUTPUT(eval_node_with_options<ImmediateAlarmComponent<7>>(interval(0, 1), values<Int>(none)), values<Int>(none));
        CHECK_OUTPUT(eval_node_with_options<ImmediateAlarmComponent<-1>>(interval(1, 6),
            values<Int>(none, none, none, none, none)), values<Int>(none, -1, none, none, none));
    }
    SECTION("root node")
    {
        CHECK_OUTPUT(eval_node_with_options<AlarmComponent>(interval(0, 1), values<Int>(7)), values<Int>(none));
        // The restart tick must cancel the second alarm and replace the first,
        // even though neither saved alarm is due in this cycle.
        CHECK_OUTPUT(eval_node_with_options<AlarmComponent>(interval(1, 6), values<Int>(-1, none, none, none, none)),
                     values<Int>(none, -1, none, none, none));
    }
    SECTION("mapped child")
    {
        CHECK_OUTPUT(eval_node_with_options<MappedAlarmComponent>(interval(0, 1),
            values<Value>(dict_delta<Str, TS<Int>>({{"a", 7}}))), values<Value>(dict_delta<Str, TS<Int>>({})));
        CHECK_OUTPUT(eval_node_with_options<MappedAlarmComponent>(interval(1, 6),
            values<Value>(dict_delta<Str, TS<Int>>({{"a", -1}}), none, none, none, none)),
            values<Value>(none, dict_delta<Str, TS<Int>>({{"a", -1}}), none, none, none));
    }
}

TEST_CASE("component checkpoint rebuilds local cache after restoring durable state", "[checkpoint][component][cache]")
{
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .load = [&] { return completed; },
        .commit = [&](const auto &image) { completed = image; }});
    starts.clear();
    cache_stops.clear();
    CHECK_OUTPUT(eval_node_with_options<CachedComponent>(interval(0, 2), values<Int>(1, 5)), {1, none});
    REQUIRE(completed);
    CHECK(cache_lifetime.expired());
    CHECK_OUTPUT(eval_node_with_options<CachedComponent>(interval(2, 4), values<Int>(none, 2)), {none, 8});
    CHECK(cache_lifetime.expired());
    CHECK_OUTPUT(eval_node_with_options<CachedComponent>(interval(4, 5), values<Int>(3)), {11});
    CHECK(starts == std::vector<Int>{-1, 6, 8});
    CHECK(cache_stops == std::vector<Int>{6, 8, 11});
    CHECK(cache_lifetime.expired());
}

TEST_CASE("mixed cache state is destroyed on lifecycle failures", "[checkpoint][component][cache]")
{
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .commit = [&](const auto &image) { completed = image; }});
    cache_stops.clear();
    SECTION("start")
    {
        fail_cache_start = true;
        CHECK_THROWS_WITH(eval_node_with_options<CachedComponent>(interval(0, 1), values<Int>(1)),
                          Catch::Matchers::ContainsSubstring("cache start failed"));
        fail_cache_start = false;
        CHECK(cache_stops.empty());
    }
    SECTION("eval")
    {
        CHECK_THROWS_WITH(eval_node_with_options<CachedComponent>(interval(0, 1), values<Int>(-999)),
                          Catch::Matchers::ContainsSubstring("failed day computation"));
    }
    SECTION("stop")
    {
        CHECK_THROWS_WITH(eval_node_with_options<CachedComponent>(interval(0, 1), values<Int>(-1)),
                          Catch::Matchers::ContainsSubstring("failed day stop"));
    }
    CHECK_FALSE(completed);
    CHECK(cache_lifetime.expired());
}

TEST_CASE("mixed state keeps independent planned slots and checkpoint service restrictions", "[checkpoint][cache]")
{
    auto node = NodeBuilder{}.implementation<CachedCounter>().make_node();
    const auto view = node.view();
    CHECK(view.has_state());
    CHECK(view.has_recordable_state());
    CHECK(view.type().checked_plan().find_component("state") != nullptr);
    CHECK(view.type().checked_plan().find_component("recordable_state") != nullptr);
    auto schema = *view.schema();
    CHECK(schema.checkpoints_without_ops());
    schema.recordable_state_schema = nullptr;
    CHECK_FALSE(schema.checkpoints_without_ops());
    schema = *view.schema();
    schema.uses_scheduler = true;
    CHECK(schema.checkpoints_without_ops());
    schema = *view.schema();
    schema.uses_global_state = true;
    CHECK_FALSE(schema.checkpoints_without_ops());
    schema = *view.schema();
    schema.uses_evaluation_clock = true;
    CHECK_FALSE(schema.checkpoints_without_ops());
}

TEST_CASE("recordable sink rebuilds local cache before resumed input", "[checkpoint][component][cache]")
{
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .load = [&] { return completed; },
        .commit = [&](const auto &image) { completed = image; }});
    cache_stops.clear();
    CHECK_OUTPUT(eval_node_with_options<CachedSinkComponent>(interval(0, 2), values<Int>(1, 2)), {1, 3});
    CHECK(cache_lifetime.expired());
    CHECK_OUTPUT(eval_node_with_options<CachedSinkComponent>(interval(2, 4), values<Int>(none, 4)), {none, 7});
    CHECK(cache_stops == std::vector<Int>{3, 7});
    CHECK(cache_lifetime.expired());
}

TEST_CASE("component checkpoint restores alarms beside rebuilt cache and recordable state", "[checkpoint][component][cache][scheduler]")
{
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "strategy", .load = [&] { return completed; },
        .commit = [&](const auto &image) {
            std::string bytes;
            encode_component_checkpoint(image, bytes);
            completed = decode_component_checkpoint(bytes);
        }});
    CHECK_OUTPUT(eval_node_with_options<CachedAlarmComponent>(interval(0, 1), values<Int>(3)), values<Int>(none));
    REQUIRE(completed);
    CHECK(cache_lifetime.expired());
    CHECK_OUTPUT(eval_node_with_options<CachedAlarmComponent>(interval(1, 3), values<Int>(none, none)), values<Int>(none, 3));
    CHECK(cache_lifetime.expired());
    CHECK_OUTPUT(eval_node_with_options<CachedAlarmComponent>(interval(3, 6), values<Int>(none, none, none)), values<Int>(none, none, none));
    CHECK(cache_lifetime.expired());
}
