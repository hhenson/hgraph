// Public native behavior for parent-clocked asynchronous sink graphs.
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/runtime/spawn.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <array>
#include <chrono>
#include <thread>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct Sample
    {
        DateTime time;
        Value value;
        Value delta;
        bool valid;
        std::thread::id thread;
        std::vector<DateTime> window_times{};
    };
    // Each trace has one writer and is read only after the run joins its child.
    struct Trace
    {
        std::vector<Sample> samples;
        int starts{0};
        int stops{0};
    };
}
namespace hgraph::static_schema_detail
{
    template <> struct scalar_name<Trace *>
    { static constexpr std::string_view value{"SpawnTestTrace"}; };
}
namespace
{
    void prepare()
    {
        stdlib::register_standard_operators();
        TypeRegistry::instance().register_scalar<Trace *>("SpawnTestTrace");
    }
    WiringArg input_arg(WiringPortRef port, std::string name = {})
    {
        WiringArg arg;
        arg.port = port;
        arg.name = std::move(name);
        return arg;
    }
    template <typename S>
    struct Capture
    {
        static void start(Scalar<"trace", Trace *> trace) { ++trace.value()->starts; }
        static void stop(Scalar<"trace", Trace *> trace) { ++trace.value()->stops; }
        static void eval(In<"value", S, InputValidity::Unchecked> value,
                         Scalar<"trace", Trace *> trace, DateTime time)
        {
            const auto &base = value.base();
            trace.value()->samples.push_back({time,
                base.valid() ? Value{base.value()} : Value{},
                base.modified() ? Value{base.delta_value()} : Value{},
                base.valid(), std::this_thread::get_id()});
            if constexpr (requires { value.value_times(); })
                for (const auto time_value : value.value_times())
                    trace.value()->samples.back().window_times.push_back(time_value);
        }
    };
    template <typename S>
    struct Sink
    {
        static void compose(Wiring &w, Port<S> value, Scalar<"trace", Trace *> trace)
        { wire<Capture<S>>(w, value, arg<"trace">(trace.value())); }
    };
    template <typename S>
    struct Identity
    {
        static Port<S> compose(Wiring &, NamedPort<"value", S> value) { return value; }
    };
    template <typename S, bool Spawn, bool Pipeline = false>
    struct CaptureGraph
    {
        static Port<S> compose(Wiring &w, Port<S> value, Scalar<"trace", Trace *> trace)
        {
            if constexpr (Spawn)
            {
                std::vector<SpawnStage> stages;
                if constexpr (Pipeline)
                {
                    stages.push_back(spawn_fn<Identity<S>>());
                    stages.push_back(spawn_fn<Identity<S>>());
                }
                stages.push_back(spawn_fn<Sink<S>>(arg<"trace">(trace.value())));
                std::array arguments{input_arg(value.erased())};
                wire_spawn(w, pipeline_(std::move(stages)), arguments, SpawnConfig{1, 1024 * 1024});
            }
            else { wire<Sink<S>>(w, value, arg<"trace">(trace.value())); }
            return value;
        }
    };
    void compare(const Trace &actual, const Trace &expected)
    {
        REQUIRE(actual.starts == 1);
        REQUIRE(actual.stops == 1);
        REQUIRE(actual.samples.size() == expected.samples.size());
        for (std::size_t i = 0; i < actual.samples.size(); ++i)
        {
            CAPTURE(i);
            CHECK(actual.samples[i].time == expected.samples[i].time);
            CHECK(actual.samples[i].valid == expected.samples[i].valid);
            CHECK(actual.samples[i].window_times == expected.samples[i].window_times);
            CHECK(actual.samples[i].value.equals(expected.samples[i].value));
            CHECK(actual.samples[i].delta.equals(expected.samples[i].delta));
            CHECK(actual.samples[i].thread != std::this_thread::get_id());
        }
    }
    template <typename S, typename T>
    void boundary_oracle(const std::vector<std::optional<T>> &input)
    {
        Trace expected, direct, pipeline;
        (void)eval_node<CaptureGraph<S, false>>(input, arg<"trace">(&expected));
        (void)eval_node<CaptureGraph<S, true>>(input, arg<"trace">(&direct));
        (void)eval_node<CaptureGraph<S, true, true>>(input, arg<"trace">(&pipeline));
        compare(direct, expected);
        compare(pipeline, expected);
    }
    struct Add
    {
        static void eval(In<"value", TS<Int>> value, In<"offset", TS<Int>> offset,
                         Scalar<"factor", Int> factor, Out<TS<Int>> out)
        { out.set(value.value() * factor.value() + offset.value()); }
    };
    struct AddGraph
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value, NamedPort<"offset", TS<Int>> offset,
                                    Scalar<"factor", Int> factor)
        { return wire<Add>(w, value, offset, arg<"factor">(factor.value())); }
    };
    template <bool Spawn>
    struct BoundGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value, Port<TS<Int>> offset,
                                    Scalar<"trace", Trace *> trace)
        {
            if constexpr (Spawn)
            {
                auto transform = bind_(spawn_fn<AddGraph>(arg<"factor">(Int{2})),
                                       {{"offset", offset.erased()}});
                std::array arguments{input_arg(value.erased(), "value")};
                wire_spawn(w, pipeline_({spawn_fn<Identity<TS<Int>>>(), std::move(transform),
                    spawn_fn<Sink<TS<Int>>>(arg<"trace">(trace.value()))}), arguments, SpawnConfig{1, 1024});
            }
            else
            {
                auto out = wire<AddGraph>(w, value, offset, arg<"factor">(Int{2}));
                wire<Sink<TS<Int>>>(w, out, arg<"trace">(trace.value()));
            }
            return value;
        }
    };
    struct TimerSink
    {
        static void start(NodeScheduler scheduler, Scalar<"trace", Trace *> trace)
        { ++trace.value()->starts; scheduler.schedule(MIN_TD); }
        static void eval(NodeScheduler scheduler, Scalar<"trace", Trace *> trace, DateTime time)
        {
            trace.value()->samples.push_back({time, Value{Int{1}}, Value{Int{1}}, true,
                                              std::this_thread::get_id()});
            if (trace.value()->samples.size() < 3) scheduler.schedule(MIN_TD);
        }
        static void stop(Scalar<"trace", Trace *> trace) { ++trace.value()->stops; }
    };
    struct TimerGraph
    {
        static void compose(Wiring &w, Scalar<"trace", Trace *> trace)
        { wire<TimerSink>(w, arg<"trace">(trace.value())); }
    };
    struct SpawnTimer
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input, Scalar<"trace", Trace *> trace)
        {
            wire_spawn(w, pipeline_({spawn_fn<TimerGraph>(arg<"trace">(trace.value()))}));
            return input;
        }
    };
    struct Silent
    {
        static void eval(In<"value", TS<Int>>, Out<TS<Int>>) {}
    };
    struct InputTimer
    {
        static void start(NodeScheduler scheduler, Scalar<"trace", Trace *> trace)
        { TimerSink::start(scheduler, trace); }
        static void eval(In<"value", TS<Int>, InputValidity::Unchecked>, NodeScheduler scheduler,
                         Scalar<"trace", Trace *> trace, DateTime time)
        { TimerSink::eval(scheduler, trace, time); }
        static void stop(Scalar<"trace", Trace *> trace) { TimerSink::stop(trace); }
    };
    struct IdlePipeline
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input, Scalar<"trace", Trace *> trace)
        {
            std::array arguments{input_arg(input.erased())};
            wire_spawn(w, pipeline_({spawn_fn<Silent>(), spawn_fn<Identity<TS<Int>>>(),
                spawn_fn<InputTimer>(arg<"trace">(trace.value()))}), arguments);
            return input;
        }
    };
    struct Failure
    {
        static void eval(In<"value", TS<Int>>) { throw std::runtime_error("spawn test failure"); }
    };
    struct SpawnFailure
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            std::array arguments{input_arg(input.erased())};
            wire_spawn(w, fn<Failure>(), arguments);
            return input;
        }
    };
    struct SelectReference
    {
        static void eval(In<"choose_rhs", TS<Bool>> choose,
                         In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs,
                         Out<REF<TS<Int>>> out)
        { out.set(choose.value() ? rhs.reference() : lhs.reference()); }
    };
    template <bool Spawn>
    struct ReferenceGraph
    {
        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Bool>> choose, Port<TS<Int>> lhs,
                                     Port<TS<Int>> rhs, Scalar<"trace", Trace *> trace)
        {
            auto reference = wire<SelectReference>(w, choose, lhs, rhs);
            if constexpr (Spawn)
            {
                std::array arguments{input_arg(reference.erased())};
                wire_spawn(w, pipeline_({spawn_fn<Identity<TS<Int>>>(),
                    spawn_fn<Sink<TS<Int>>>(arg<"trace">(trace.value()))}), arguments);
            }
            else wire<Sink<TS<Int>>>(w, reference, arg<"trace">(trace.value()));
            return choose;
        }
    };
    struct RequestStop
    {
        static void eval(In<"value", TS<Int>>, EngineControlView engine) { engine.request_stop(); }
    };
    struct SpawnStop
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            std::array arguments{input_arg(input.erased())};
            wire_spawn(w, fn<RequestStop>(), arguments);
            return input;
        }
    };
    struct StopOnStart
    {
        static void start(EngineControlView engine) { engine.request_stop(); }
        static void eval(In<"value", TS<Int>>) {}
    };
    struct SpawnStopOnStart
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            std::array arguments{input_arg(input.erased())};
            wire_spawn(w, fn<StopOnStart>(), arguments);
            return input;
        }
    };
    struct FailOwnerCycle
    {
        static void eval(In<"value", TS<Int>>, EngineControlView engine)
        {
            engine.add_after_evaluation_notification([] { throw std::runtime_error("owner cycle failed"); });
        }
    };
    struct FailingOwner
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input, Scalar<"trace", Trace *> trace)
        {
            std::array arguments{input_arg(input.erased())};
            wire_spawn(w, pipeline_({spawn_fn<Sink<TS<Int>>>(arg<"trace">(trace.value()))}), arguments);
            wire<FailOwnerCycle>(w, input);
            return input;
        }
    };
    enum class Invalid { Output, Empty, Missing, Duplicate, Ambiguous, IntermediateSink, ZeroCapacity, Oversize };
    template <Invalid Case>
    struct InvalidGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input, Scalar<"trace", Trace *> trace)
        {
            std::array arguments{input_arg(input.erased())};
            auto sink = spawn_fn<Sink<TS<Int>>>(arg<"trace">(trace.value()));
            if constexpr (Case == Invalid::Output) wire_spawn(w, fn<Identity<TS<Int>>>(), arguments);
            if constexpr (Case == Invalid::Empty) wire_spawn(w, pipeline_({}), arguments);
            if constexpr (Case == Invalid::Missing) wire_spawn(w, pipeline_({sink}));
            if constexpr (Case == Invalid::Duplicate)
                wire_spawn(w, pipeline_({bind_(sink, {{"value", input.erased()}, {"value", input.erased()}})}));
            if constexpr (Case == Invalid::Ambiguous)
                wire_spawn(w, pipeline_({spawn_fn<Identity<TS<Int>>>(),
                    spawn_fn<AddGraph>(arg<"factor">(Int{1})), sink}), arguments);
            if constexpr (Case == Invalid::IntermediateSink)
                wire_spawn(w, pipeline_({sink, sink}), arguments);
            if constexpr (Case == Invalid::ZeroCapacity)
                wire_spawn(w, pipeline_({sink}), arguments, SpawnConfig{0, 1024});
            if constexpr (Case == Invalid::Oversize)
                wire_spawn(w, pipeline_({sink}), arguments, SpawnConfig{1, 1});
            return input;
        }
    };
    struct AddOne
    {
        static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out) { out.set(value.value() + 1); }
    };
    using Dict = TSD<Str, TS<Int>>;
    enum class NestedKind { Map, Reduce, Mesh };
    template <NestedKind Kind>
    struct Nested
    {
        static Port<TS<Int>> compose(Wiring &w, Port<Dict> value)
        {
            if constexpr (Kind == NestedKind::Reduce)
                return wire<stdlib::reduce_>(w, fn<stdlib::add_>(), value).template as<TS<Int>>();
            else
            {
                auto mapped = [&] {
                    if constexpr (Kind == NestedKind::Map)
                        return wire<stdlib::map_>(w, fn<AddOne>(), value).template as<Dict>();
                    else return wire<stdlib::mesh_>(w, fn<AddOne>(), value).template as<Dict>();
                }();
                return wire<stdlib::reduce_>(w, fn<stdlib::add_>(), mapped).template as<TS<Int>>();
            }
        }
    };
    template <NestedKind Kind, bool Spawn>
    struct NestedGraph
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> value, Scalar<"trace", Trace *> trace)
        {
            if constexpr (Spawn)
            {
                std::array arguments{input_arg(value.erased())};
                wire_spawn(w, pipeline_({spawn_fn<Nested<Kind>>(),
                    spawn_fn<Sink<TS<Int>>>(arg<"trace">(trace.value()))}), arguments);
            }
            else wire<Sink<TS<Int>>>(w, wire<Nested<Kind>>(w, value), arg<"trace">(trace.value()));
            return value;
        }
    };
}

using namespace hgraph;
using namespace hgraph::testing;

TEST_CASE("spawn: direct sink and pass-through pipeline preserve scalar and signal traces", "[spawn]")
{
    prepare();
    boundary_oracle<TS<Int>>(values<Int>(1, none, 3, 4, none, 6));
    boundary_oracle<SIGNAL>(values<bool>(true, none, true));
}

TEST_CASE("spawn: collection boundaries preserve values deltas and membership", "[spawn]")
{
    prepare();
    SECTION("set")
    { boundary_oracle<TSS<Int>>(values<Value>(set_delta<Int>({1, 2}, {}), none,
                                             set_delta<Int>({3}, {1}), set_delta<Int>({}, {2, 3}))); }
    SECTION("dictionary")
    { boundary_oracle<Dict>(values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}),
        dict_delta<Str, TS<Int>>({}, {"a"}), dict_delta<Str, TS<Int>>({{"a", 3}}))); }
    SECTION("fixed list")
    { boundary_oracle<TSL<TS<Int>, 2>>(values<Value>(list_delta<TS<Int>>({1, std::nullopt}),
        list_delta<TS<Int>>({std::nullopt, 2}), list_delta<TS<Int>>({3, 4}))); }
    SECTION("dynamic list")
    { boundary_oracle<TSL<TS<Int>>>(values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {3, 4}}),
        dynamic_list_delta<TS<Int>>({}, {3}), dynamic_list_delta<TS<Int>>({{3, 5}}))); }
    SECTION("partial nested bundle")
    {
        using Row = TSB<"SpawnTestRow", Field<"value", TS<Int>>, Field<"label", TS<Str>>>;
        boundary_oracle<Row>(values<Value>(tsb_delta<Row>(Int{1}, std::nullopt),
            tsb_delta<Row>(std::nullopt, Str{"one"}), tsb_delta<Row>(Int{2}, Str{"two"})));
        boundary_oracle<TSD<Str, TSS<Int>>>(values<Value>(
            dict_delta<Str, TSS<Int>>({{"x", set_delta<Int>({1, 2}, {})}}),
            dict_delta<Str, TSS<Int>>({{"x", set_delta<Int>({3}, {1})}}),
            dict_delta<Str, TSS<Int>>({}, {"x"})));
    }
}

TEST_CASE("spawn: count and duration windows preserve history through stage boundaries", "[spawn]")
{
    prepare();
    boundary_oracle<TSW<Int, 3, 1>>(values<Int>(1, 2, none, 4, 5, 6));
    boundary_oracle<TSWDuration<Int, 3, 0>>(values<Int>(1, none, 3, none, none, none, 7));
}

TEST_CASE("spawn: named external inputs align with flow at original timestamps", "[spawn]")
{
    prepare();
    Trace expected, actual;
    auto flow = values<Int>(1, none, 3, none, 5);
    auto side = values<Int>(10, 20, 30, 40, 50);
    (void)eval_node<BoundGraph<false>>(flow, side, arg<"trace">(&expected));
    (void)eval_node<BoundGraph<true>>(flow, side, arg<"trace">(&actual));
    compare(actual, expected);
}

TEST_CASE("spawn: no-input child requests scheduled work after parent inputs finish", "[spawn]")
{
    prepare();
    Trace trace;
    (void)eval_node<SpawnTimer>(values<Int>(7), arg<"trace">(&trace));
    REQUIRE(trace.starts == 1);
    REQUIRE(trace.stops == 1);
    REQUIRE(trace.samples.size() == 3);
    for (std::size_t i = 0; i < 3; ++i)
    {
        CHECK(trace.samples[i].time == MIN_ST + MIN_TD * static_cast<Int>(i + 1));
        CHECK(trace.samples[i].thread != std::this_thread::get_id());
    }
}

TEST_CASE("spawn: failures propagate and invalid plans fail closed", "[spawn]")
{
    prepare();
    CHECK_THROWS((eval_node<SpawnFailure>(values<Int>(1))));
    Trace trace;
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::Output>>(values<Int>(1), arg<"trace">(&trace))));
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::Empty>>(values<Int>(1), arg<"trace">(&trace))));
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::Missing>>(values<Int>(1), arg<"trace">(&trace))));
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::Duplicate>>(values<Int>(1), arg<"trace">(&trace))));
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::Ambiguous>>(values<Int>(1), arg<"trace">(&trace))));
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::IntermediateSink>>(values<Int>(1), arg<"trace">(&trace))));
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::ZeroCapacity>>(values<Int>(1), arg<"trace">(&trace))));
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::Oversize>>(values<Int>(1), arg<"trace">(&trace))));
}

TEST_CASE("spawn: ordinary map reduce and mesh retain keyed semantics inside a stage", "[spawn]")
{
    prepare();
    const auto input = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}),
        dict_delta<Str, TS<Int>>({{"b", 4}}), dict_delta<Str, TS<Int>>({}, {"a"}),
        dict_delta<Str, TS<Int>>({{"a", 5}}));
    auto check = [&]<NestedKind Kind>() {
        Trace expected, actual;
        (void)eval_node<NestedGraph<Kind, false>>(input, arg<"trace">(&expected));
        (void)eval_node<NestedGraph<Kind, true>>(input, arg<"trace">(&actual));
        compare(actual, expected);
    };
    check.template operator()<NestedKind::Map>();
    check.template operator()<NestedKind::Reduce>();
    check.template operator()<NestedKind::Mesh>();
}

TEST_CASE("spawn: reference retargeting materializes values at each boundary", "[spawn]")
{
    prepare();
    Trace expected, actual;
    const auto choose = values<Bool>(false, none, true, none, false);
    const auto lhs = values<Int>(1, 2, 3, 4, 5);
    const auto rhs = values<Int>(10, 20, 30, 40, 50);
    (void)eval_node<ReferenceGraph<false>>(choose, lhs, rhs, arg<"trace">(&expected));
    (void)eval_node<ReferenceGraph<true>>(choose, lhs, rhs, arg<"trace">(&actual));
    compare(actual, expected);
}

TEST_CASE("spawn: downstream timers request progress through idle stages", "[spawn]")
{
    prepare();
    Trace trace;
    (void)eval_node<IdlePipeline>(values<Int>(7), arg<"trace">(&trace));
    REQUIRE(trace.samples.size() == 3);
    for (std::size_t i = 0; i < 3; ++i)
        CHECK(trace.samples[i].time == MIN_ST + MIN_TD * static_cast<Int>(i + 1));
    CHECK(trace.starts == 1);
    CHECK(trace.stops == 1);
}

TEST_CASE("spawn: no-input timers respect the enclosing exclusive end time", "[spawn]")
{
    prepare();
    Trace trace;
    (void)eval_node_with_options<SpawnTimer>(
        {MIN_ST, MIN_ST + MIN_TD * 2}, values<Int>(7), arg<"trace">(&trace));
    REQUIRE(trace.samples.size() == 1);
    CHECK(trace.samples.front().time == MIN_ST + MIN_TD);
    CHECK(trace.starts == 1);
    CHECK(trace.stops == 1);
}

TEST_CASE("spawn: child stop fails the owning run instead of dropping accepted input", "[spawn]")
{
    prepare();
    CHECK_THROWS_WITH((eval_node<SpawnStop>(values<Int>(1, 2, 3))),
                      Catch::Matchers::ContainsSubstring("child requested stop"));
}

TEST_CASE("spawn: failed owner cycle never grants its pending input frame", "[spawn]")
{
    prepare();
    Trace trace;
    CHECK_THROWS_WITH((eval_node<FailingOwner>(values<Int>(1), arg<"trace">(&trace))),
                      Catch::Matchers::ContainsSubstring("owner cycle failed"));
    CHECK(trace.samples.empty());
    CHECK(trace.starts == 1);
    CHECK(trace.stops == 1);
}

TEST_CASE("spawn: child stop during start fails and joins the worker", "[spawn]")
{
    prepare();
    CHECK_THROWS_WITH((eval_node<SpawnStopOnStart>(values<Int>(1))),
                      Catch::Matchers::ContainsSubstring("child requested stop during start"));
}
