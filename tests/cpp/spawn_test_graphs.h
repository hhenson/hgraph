#pragma once
// Public native behavior for parent-clocked asynchronous sink graphs.
#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/spawn.h>
#include <hgraph/types/static_node.h>


#include <array>
#include <chrono>
#include <thread>
#include <vector>

#include <fstream>
#include <iomanip>
#include <filesystem>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct Sample
    {
        DateTime time;
        std::string value;
        std::string delta;
        bool valid;
        long long pid;
        std::vector<DateTime> window_times{};
    };
    long long process_id()
    {
#ifdef _WIN32
        return _getpid();
#else
        return getpid();
#endif
    }
    struct Trace
    {
        mutable std::vector<Sample> samples;
        mutable int starts{0};
        mutable int stops{0};
        bool remote{false};
        Str path{(std::filesystem::temp_directory_path() / ("hgraph-spawn-" + std::to_string(process_id()) + "-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()))).string()};
        mutable bool loaded{false};
        ~Trace() { if (!remote) { std::error_code error; std::filesystem::remove(path, error); } }
        void save() const
        {
            if (!remote) return;
            std::ofstream file{path, std::ios::binary | std::ios::trunc};
            file << starts << ' ' << stops << '\n';
            for (const auto &sample : samples)
            {
                file << sample.time.time_since_epoch().count() << ' ' << sample.valid << ' ' << sample.pid << ' '
                     << std::quoted(sample.value) << ' ' << std::quoted(sample.delta) << ' ' << sample.window_times.size();
                for (auto time : sample.window_times) file << ' ' << time.time_since_epoch().count();
                file << '\n';
            }
            if (!file) throw std::runtime_error("cannot write spawn trace");
        }
        void load() const
        {
            if (loaded || !std::filesystem::exists(path)) return;
            loaded = true;
            std::ifstream file{path, std::ios::binary};
            file >> starts >> stops;
            Sample sample;
            Int time{};
            std::size_t count{};
            while (file >> time >> sample.valid >> sample.pid >> std::quoted(sample.value) >> std::quoted(sample.delta) >> count)
            {
                sample.time = DateTime{TimeDelta{time}};
                sample.window_times.clear();
                for (std::size_t i = 0; i < count; ++i) { file >> time; sample.window_times.emplace_back(TimeDelta{time}); }
                samples.push_back(sample);
            }
        }
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
        static void start(Scalar<"trace", Trace *> trace) { ++trace.value()->starts; trace.value()->save(); }
        static void stop(Scalar<"trace", Trace *> trace) { ++trace.value()->stops; trace.value()->save(); }
        static void eval(In<"value", S, InputValidity::Unchecked> value,
                         Scalar<"trace", Trace *> trace, DateTime time)
        {
            const auto &base = value.base();
            trace.value()->samples.push_back({time,
                base.valid() ? base.value().to_string() : "<invalid>",
                base.modified() ? base.delta_value().to_string() : "<invalid>",
                base.valid(), process_id()});
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
    // --- recovery (RFC 0039) -------------------------------------------------
    // What recovers is a component inside a stage. Everything else in the
    // stage is processed, the pipeline's sink first of all: it acts in a worker
    // process, and no checkpoint could replay that. The sinks in these tests
    // are the plain ``Capture`` above. It has no recordable state, so it is
    // transient wherever it sits -- outside the component, or inside one.
    using SpawnRunningState = TSB<"SpawnRunningState", Field<"total", TS<Int>>>;
    /** State that a lost, repeated or re-ticked frame changes. */
    struct SpawnAccumulate
    {
        static void eval(In<"value", TS<Int>> value, RecordableState<SpawnRunningState> state, Out<TS<Int>> out)
        {
            auto      total = state.field<"total">();
            const Int next  = (total.valid() ? total.value().checked_as<Int>() : 0) + value.value();
            total.set(next);
            out.set(next);
        }
    };
    struct AccumulateStage
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value)
        { return wire<SpawnAccumulate>(w, value).as<TS<Int>>(); }
    };
    /**
     * Counts every tick of a side input. A restored pipeline already holds its
     * input baselines; re-sending one would tick ``offset`` again and show here.
     */
    struct SpawnSideAccumulate
    {
        static void eval(In<"value", TS<Int>, InputValidity::Unchecked> value, In<"offset", TS<Int>, InputValidity::Unchecked> offset,
                         RecordableState<SpawnRunningState> state, Out<TS<Int>> out)
        {
            auto total = state.field<"total">();
            Int  next  = total.valid() ? total.value().checked_as<Int>() : 0;
            if (offset.modified()) { next += offset.value(); }
            total.set(next);
            if (value.modified()) { out.set(next * 1000 + value.value()); }
        }
    };
    struct SideStage
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value, NamedPort<"offset", TS<Int>> offset)
        { return wire<SpawnSideAccumulate>(w, value, offset).as<TS<Int>>(); }
    };
    /** The recoverable unit: a component, wired inside a stage. */
    inline constexpr const char *spawn_component_id = "spawn-accumulate";
    struct AccumulateBody
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value)
        { return wire<SpawnAccumulate>(w, value).as<TS<Int>>(); }
    };
    /** A stage that is nothing but the component; the sink is the next stage. */
    struct ComponentStage
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value)
        { return stdlib::component<AccumulateBody>(w, spawn_component_id, value); }
    };
    /** One stage holding both: the component, and beside it the plain sink
        every other spawn test uses, which declares nothing. */
    struct ComponentAndSinkStage
    {
        static void compose(Wiring &w, Port<TS<Int>> value, Scalar<"trace", Trace *> trace)
        {
            auto total = stdlib::component<AccumulateBody>(w, spawn_component_id, value);
            wire<Capture<TS<Int>>>(w, total, arg<"trace">(trace.value()));
        }
    };
    struct SideBody
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value, NamedPort<"offset", TS<Int>> offset)
        { return wire<SpawnSideAccumulate>(w, value, offset).as<TS<Int>>(); }
    };
    struct SideComponentStage
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value, NamedPort<"offset", TS<Int>> offset)
        { return stdlib::component<SideBody>(w, spawn_component_id, value, offset); }
    };
    /** A hosted component over a KEYED input: per-key totals, folded to one number. */
    using SpawnKeyed = TSD<Str, TS<Int>>;
    struct SpawnKeyedDigest
    {
        static void eval(In<"d", SpawnKeyed> d, Out<TS<Int>> out)
        {
            Int total = 0;
            for (auto &&[key, child] : d.valid_items()) { total += child.value(); }
            out.set(total);
        }
    };
    struct KeyedBody
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", SpawnKeyed> value)
        {
            auto totals = wire<stdlib::map_>(w, fn<SpawnAccumulate>(), value).as<SpawnKeyed>();
            return wire<SpawnKeyedDigest>(w, totals).as<TS<Int>>();
        }
    };
    struct KeyedComponentStage
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", SpawnKeyed> value)
        { return stdlib::component<KeyedBody>(w, spawn_component_id, value); }
    };
    /** Inside the stage, something computes the component's input OUTSIDE it. */
    struct SpawnDoubled
    {
        static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out) { out.set(value.value() * 2); }
    };
    struct PreprocessedComponentStage
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value)
        { return stdlib::component<AccumulateBody>(w, spawn_component_id, wire<SpawnDoubled>(w, value).as<TS<Int>>()); }
    };
    /** The same component id over another body: another contract. */
    struct OtherBody
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value)
        { return wire<SpawnAccumulate>(w, wire<SpawnAccumulate>(w, value)).as<TS<Int>>(); }
    };
    struct OtherComponentStage
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value)
        { return stdlib::component<OtherBody>(w, spawn_component_id, value); }
    };
    struct ForgetfulBody
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value);
    };
    /** Holds its total in ordinary ``State``, which no checkpoint can see. */
    struct SpawnForgetful
    {
        static void eval(In<"value", TS<Int>> value, State<Int> total, Out<TS<Int>> out)
        {
            total.modify() += value.value();
            out.set(total.get());
        }
    };
    struct ForgetfulStage
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value)
        { return wire<SpawnForgetful>(w, value).as<TS<Int>>(); }
    };
    inline Port<TS<Int>> ForgetfulBody::compose(Wiring &w, NamedPort<"value", TS<Int>> value)
    { return wire<SpawnForgetful>(w, value).as<TS<Int>>(); }
    /** A component that cannot be recovered, hosted in a stage. */
    struct ForgetfulComponentStage
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value)
        { return stdlib::component<ForgetfulBody>(w, spawn_component_id, value); }
    };

    template <typename S>
    struct Identity
    {
        static Port<S> compose(Wiring &, NamedPort<"value", S> value) { return value; }
    };
    template <typename Graph>
    SpawnStage test_stage()
    { return process_stage(spawn_fn<Graph>(), typeid(Graph).name()); }
    template <typename Graph>
    SpawnStage test_stage(Trace *trace)
    { return process_stage(spawn_fn<Graph>(arg<"trace">(trace)), typeid(Graph).name(), trace->path); }
    template <typename Graph>
    SpawnStage test_stage(Int factor)
    { return process_stage(spawn_fn<Graph>(arg<"factor">(factor)), typeid(Graph).name(), std::to_string(factor)); }
    SpawnConfig process_config(std::size_t frames = 256, std::size_t bytes = 64 * 1024 * 1024)
    {
        SpawnConfig config{frames, bytes};
#ifdef HGRAPH_TEST_WORKER_PROGRAM
        config.worker_program = HGRAPH_TEST_WORKER_PROGRAM;
#endif
        config.worker_timeout = std::chrono::seconds{5};
        return config;
    }
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
                    stages.push_back(test_stage<Identity<S>>());
                    stages.push_back(test_stage<Identity<S>>());
                }
                stages.push_back(test_stage<Sink<S>>(trace.value()));
                std::array arguments{input_arg(value.erased())};
                wire_spawn(w, pipeline_(std::move(stages)), arguments, process_config(1, 1024 * 1024));
            }
            else { wire<Sink<S>>(w, value, arg<"trace">(trace.value())); }
            return value;
        }
    };
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
                auto transform = bind_(test_stage<AddGraph>(Int{2}),
                                       {{"offset", offset.erased()}});
                std::array arguments{input_arg(value.erased(), "value")};
                wire_spawn(w, pipeline_({test_stage<Identity<TS<Int>>>(), std::move(transform),
                    test_stage<Sink<TS<Int>>>(trace.value())}), arguments, process_config(1, 1024));
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
        { ++trace.value()->starts; trace.value()->save(); scheduler.schedule(MIN_TD); }
        static void eval(NodeScheduler scheduler, Scalar<"trace", Trace *> trace, DateTime time)
        {
            trace.value()->samples.push_back({time, "1", "1", true,
                                              process_id()});
            if (trace.value()->samples.size() < 3) scheduler.schedule(MIN_TD);
        }
        static void stop(Scalar<"trace", Trace *> trace) { ++trace.value()->stops; trace.value()->save(); }
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
            wire_spawn(w, pipeline_({test_stage<TimerGraph>(trace.value())}), {}, process_config());
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
            wire_spawn(w, pipeline_({test_stage<Silent>(), test_stage<Identity<TS<Int>>>(),
                test_stage<InputTimer>(trace.value())}), arguments, process_config());
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
            wire_spawn(w, pipeline_({test_stage<Failure>()}), arguments, process_config());
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
                wire_spawn(w, pipeline_({test_stage<Identity<TS<Int>>>(),
                    test_stage<Sink<TS<Int>>>(trace.value())}), arguments, process_config());
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
            wire_spawn(w, pipeline_({test_stage<RequestStop>()}), arguments, process_config());
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
            wire_spawn(w, pipeline_({test_stage<StopOnStart>()}), arguments, process_config());
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
            wire_spawn(w, pipeline_({test_stage<Sink<TS<Int>>>(trace.value())}), arguments, process_config());
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
            auto sink = test_stage<Sink<TS<Int>>>(trace.value());
            if constexpr (Case == Invalid::Output) wire_spawn(w, spawn_fn<Identity<TS<Int>>>(), arguments);
            if constexpr (Case == Invalid::Empty) wire_spawn(w, pipeline_({}), arguments, process_config());
            if constexpr (Case == Invalid::Missing) wire_spawn(w, pipeline_({sink}));
            if constexpr (Case == Invalid::Duplicate)
                wire_spawn(w, pipeline_({bind_(sink, {{"value", input.erased()}, {"value", input.erased()}})}));
            if constexpr (Case == Invalid::Ambiguous)
                wire_spawn(w, pipeline_({test_stage<Identity<TS<Int>>>(),
                    test_stage<AddGraph>(Int{1}), sink}), arguments, process_config());
            if constexpr (Case == Invalid::IntermediateSink)
                wire_spawn(w, pipeline_({sink, sink}), arguments, process_config());
            if constexpr (Case == Invalid::ZeroCapacity)
                wire_spawn(w, pipeline_({sink}), arguments, process_config(0, 1024));
            if constexpr (Case == Invalid::Oversize)
                wire_spawn(w, pipeline_({sink}), arguments, process_config(1, 1));
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
                wire_spawn(w, pipeline_({test_stage<Nested<Kind>>(),
                    test_stage<Sink<TS<Int>>>(trace.value())}), arguments, process_config());
            }
            else wire<Sink<TS<Int>>>(w, wire<Nested<Kind>>(w, value), arg<"trace">(trace.value()));
            return value;
        }
    };
}


namespace
{
    struct Crash
    {
        static void eval(In<"value", TS<Int>>) { std::_Exit(23); }
    };
    struct Hang
    {
        static void eval(In<"value", TS<Int>>) { std::this_thread::sleep_for(std::chrono::seconds{30}); }
    };
    struct StartFailure
    {
        static void start() { throw std::runtime_error("spawn start failure"); }
        static void eval(In<"value", TS<Int>>) {}
    };
    struct StopFailure
    {
        static void stop() { throw std::runtime_error("spawn stop failure"); }
        static void eval(In<"value", TS<Int>>) {}
    };
    template <typename Child>
    struct ProcessFailureGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            std::array arguments{input_arg(input.erased())};
            auto config = process_config(1);
            config.worker_timeout = std::chrono::milliseconds{500};
            wire_spawn(w, pipeline_({test_stage<Child>()}), arguments, config);
            return input;
        }
    };
}

namespace
{
    struct MappedTimerChild
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            return wire<SpawnTimer>(w, input,
                arg<"trace">(w.operator_state().get_as<Trace *>("spawn_test_trace")));
        }
    };
    struct MappedSpawnTimer
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> value, Scalar<"trace", Trace *> trace)
        {
            w.global_state().set("spawn_test_trace", Value{trace.value()});
            return wire<stdlib::map_>(w, fn<MappedTimerChild>(), value).as<Dict>();
        }
    };
    struct ReschedulingSink
    {
        static void start(NodeScheduler scheduler, Scalar<"trace", Trace *> trace)
        { TimerSink::start(scheduler, trace); }
        static void eval(In<"value", TS<Int>>, NodeScheduler scheduler,
                         Scalar<"trace", Trace *> trace, DateTime time)
        {
            // Fail promptly if termination regresses instead of hanging the suite.
            if (trace.value()->samples.size() >= 2048)
                throw std::runtime_error("child escaped immediate drain limit");
            trace.value()->samples.push_back({time, "1", "1", true, process_id()});
            scheduler.schedule(MIN_TD);
        }
        static void stop(Scalar<"trace", Trace *> trace) { TimerSink::stop(trace); }
    };
    struct SpawnRescheduling
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input, Scalar<"trace", Trace *> trace)
        {
            std::array arguments{input_arg(input.erased())};
            wire_spawn(w, test_stage<ReschedulingSink>(trace.value()), arguments, process_config());
            return input;
        }
    };
    struct IdleCrash
    {
        static void start()
        {
            // Test-only asynchronous process death, after the startup reply.
            std::thread([] {
                std::this_thread::sleep_for(std::chrono::milliseconds{200});
                std::_Exit(31);
            }).detach();
        }
        static void eval(In<"value", TS<Int>>) {}
    };
}
