// Recovery through ``spawn_`` (RFC 0039, "Recovery of worker-hosted graphs").
//
// What recovers is a COMPONENT INSIDE A STAGE. The recovery configuration names
// it; the ``spawn_`` that hosts it stands in for it in the owner graph; and each
// stage's image covers the component and the runtime's own boundary nodes.
// Everything else in the stage is processed, not recovered -- the pipeline's
// sink first of all, which acts in a worker process and whose effect no
// checkpoint could replay. So the sinks here are the plain ones every other
// spawn test uses: they declare nothing.
//
// The sink's trace IS the observable result, so a restarted run is compared
// sample for sample with an uninterrupted one.

#include "spawn_test_graphs.h"

#include <hgraph/lib/std/component.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/runtime/component_checkpoint.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <memory>

namespace
{
    /** The component in one stage, the sink in the next. ``spawn_`` is in no component. */
    template <typename Stage> struct HostedPipeline
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value, Scalar<"trace", Trace *> trace)
        {
            std::array arguments{input_arg(value.erased())};
            wire_spawn(w, pipeline_({test_stage<Stage>(), test_stage<Sink<TS<Int>>>(trace.value())}), arguments,
                       process_config());
            return value;
        }
    };
    /** The component and the sink side by side in ONE stage graph. */
    struct HostedSingleStage
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value, Scalar<"trace", Trace *> trace)
        {
            std::array arguments{input_arg(value.erased())};
            wire_spawn(w, test_stage<ComponentAndSinkStage>(trace.value()), arguments, process_config());
            return value;
        }
    };
    /** A hosted component with a bound side input that ticks once, on the first day. */
    struct HostedSideInput
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value, Port<TS<Int>> offset, Scalar<"trace", Trace *> trace)
        {
            auto first = bind_(test_stage<SideComponentStage>(), {{"offset", offset.erased()}});
            std::array arguments{input_arg(value.erased(), "value")};
            wire_spawn(w, pipeline_({std::move(first), test_stage<Sink<TS<Int>>>(trace.value())}), arguments,
                       process_config());
            return value;
        }
    };
    /** The other nesting: ``spawn_`` is itself a member of a recoverable component. */
    template <typename Stage> struct MemberPipeline
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value, Scalar<"trace", Trace *> trace)
        {
            // The body is a lambda because the trace is test plumbing, not a
            // time series a component boundary could carry.
            const std::array inputs{WiringNamedPortRef{"value", value.erased()}};
            const auto       out = stdlib::component(w, "spawn-pipeline", inputs,
                [&](std::span<const WiringPortRef> ports) -> WiringPortRef {
                    std::array arguments{input_arg(ports[0])};
                    wire_spawn(w, pipeline_({test_stage<Identity<TS<Int>>>(), test_stage<Stage>(),
                                             test_stage<RecoverableSink>(trace.value())}),
                               arguments, process_config());
                    return ports[0];
                });
            return Port<void>{w, out}.template as<TS<Int>>();
        }
    };

    EvalNodeRunOptions interval(std::size_t begin, std::size_t end)
    {
        return {.start_time = MIN_ST + MIN_TD * static_cast<Int>(begin),
                .end_time   = MIN_ST + MIN_TD * static_cast<Int>(end)};
    }

    using Observed = std::vector<std::tuple<DateTime, std::string, std::string, bool>>;
    void append(Observed &observed, const Trace &trace)
    {
        trace.load();
        for (const auto &sample : trace.samples)
        {
            REQUIRE(sample.pid != process_id());
            observed.emplace_back(sample.time, sample.value, sample.delta, sample.valid);
        }
    }

    using Ticks = std::vector<std::optional<Int>>;

    /** One day of ``Graph`` over ``inputs[begin, end)``, recovering ``component`` when one is named. */
    template <typename Graph, typename... Input>
    void day(std::size_t begin, std::size_t end, const char *component, std::optional<ComponentCheckpoint> &completed,
             Observed &observed, const Input &...inputs)
    {
        GlobalContext context;
        if (component != nullptr)
        {
            configure_component_recovery(context.state().view(), {
                .component_id = component, .load = [&] { return completed; },
                .commit = [&](const auto &image) { completed = image; }});
        }
        Trace trace;
        (void)eval_node_with_options<Graph>(interval(begin, end), Ticks{inputs.begin() + begin, inputs.begin() + end}...,
                                            arg<"trace">(&trace));
        append(observed, trace);
        if (component != nullptr) { REQUIRE(completed); }
    }

    template <typename Graph, typename... Input> Observed uninterrupted(const Ticks &first, const Input &...rest)
    {
        std::optional<ComponentCheckpoint> unused;
        Observed                           observed;
        day<Graph>(0, first.size(), nullptr, unused, observed, first, rest...);
        return observed;
    }

    /** ``split`` is the first day's length, or the input's size for a restart on every tick. */
    template <typename Graph, typename... Input>
    Observed restarted(std::size_t split, const char *component, const Ticks &first, const Input &...rest)
    {
        std::optional<ComponentCheckpoint> completed;
        Observed                           observed;
        for (std::size_t begin = 0; begin < first.size();)
        {
            const auto end = split == first.size() ? begin + 1 : (begin == 0 ? split : first.size());
            day<Graph>(begin, end, component, completed, observed, first, rest...);
            begin = end;
        }
        return observed;
    }

    template <typename Graph> void every_boundary(const char *component)
    {
        const Ticks ticks    = values<Int>(1, 2, none, 3, 4, none, 5);
        const auto  expected = uninterrupted<Graph>(ticks);
        // Pinned so the comparison cannot pass on silence: five ticks, running totals.
        REQUIRE(expected.size() == 5);
        REQUIRE(std::get<1>(expected.back()) == "15");
        for (std::size_t split = 1; split <= ticks.size(); ++split)
        {
            CAPTURE(split);
            CHECK(restarted<Graph>(split, component, ticks) == expected);
            // The control: the same restarts without recovery lose the running
            // total, and the sink sees it. Without this the comparison above
            // would pass for a pipeline that carried nothing across a boundary.
            if (split < ticks.size()) { CHECK(restarted<Graph>(split, nullptr, ticks) != expected); }
        }
    }
}  // namespace

TEST_CASE("spawn recovery: a component inside a stage restarts invisibly, and the sink after it declares nothing",
          "[checkpoint][spawn]")
{
    prepare();
    every_boundary<HostedPipeline<ComponentStage>>(spawn_component_id);
}

TEST_CASE("spawn recovery: the component and a plain sink can share one stage", "[checkpoint][spawn]")
{
    prepare();
    every_boundary<HostedSingleStage>(spawn_component_id);
}

TEST_CASE("spawn recovery: a restored pipeline does not re-send the baselines it already holds", "[checkpoint][spawn]")
{
    prepare();
    // ``offset`` ticks once, on the first day. A fresh pipeline sends every
    // input in full on its first capture; a restored one must not, or the
    // component counts the offset again on every restart.
    const Ticks value    = values<Int>(1, 2, 3, 4);
    const Ticks offset   = values<Int>(7, none, none, none);
    const auto  expected = uninterrupted<HostedSideInput>(value, offset);
    REQUIRE(expected.size() == 4);
    REQUIRE(std::get<1>(expected.back()) == "7004");
    CHECK(restarted<HostedSideInput>(value.size(), spawn_component_id, value, offset) == expected);
}

TEST_CASE("spawn recovery: what is outside the component is processed, recoverable or not", "[checkpoint][spawn]")
{
    prepare();
    // A stage after the component keeps ordinary ``State``, which no
    // checkpoint can see. It is outside the component, so it is neither part
    // of the contract nor restored: the pipeline wires, every day completes,
    // and that stage simply starts again -- so the trace differs from an
    // uninterrupted run exactly where its forgotten total shows.
    struct Pipeline
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value, Scalar<"trace", Trace *> trace)
        {
            std::array arguments{input_arg(value.erased())};
            wire_spawn(w, pipeline_({test_stage<ComponentStage>(), test_stage<ForgetfulStage>(),
                                     test_stage<Sink<TS<Int>>>(trace.value())}),
                       arguments, process_config());
            return value;
        }
    };
    const Ticks ticks    = values<Int>(1, 2, 3);
    const auto  expected = uninterrupted<Pipeline>(ticks);
    const auto  actual   = restarted<Pipeline>(2, spawn_component_id, ticks);
    REQUIRE(expected.size() == 3);
    REQUIRE(actual.size() == 3);
    // Day one is identical. On day two the component resumes at 6; the stage
    // after it has forgotten 1 + 3 and reports 6 where the unbroken run says 10.
    CHECK(actual[0] == expected[0]);
    CHECK(actual[1] == expected[1]);
    CHECK(std::get<1>(expected[2]) == "10");
    CHECK(std::get<1>(actual[2]) == "6");
}

TEST_CASE("spawn recovery: an unrecoverable node INSIDE the hosted component is refused at wiring",
          "[checkpoint][spawn]")
{
    prepare();
    using Graph = HostedPipeline<ForgetfulComponentStage>;
    {
        GlobalContext context;
        configure_component_recovery(context.state().view(), {
            .component_id = spawn_component_id, .load = [] { return std::optional<ComponentCheckpoint>{}; },
            .commit = [](const auto &) {}});
        Trace trace;
        REQUIRE_THROWS_WITH((eval_node_with_options<Graph>(interval(0, 1), values<Int>(1), arg<"trace">(&trace))),
                            Catch::Matchers::ContainsSubstring("spawn_ worker 0") &&
                                Catch::Matchers::ContainsSubstring("cannot be recovered"));
    }
    // Recovery not configured: the same pipeline wires and runs, as most do.
    const auto observed = uninterrupted<Graph>(values<Int>(1, 2));
    REQUIRE(observed.size() == 2);
    CHECK(std::get<1>(observed.back()) == "3");
}

TEST_CASE("spawn recovery: a configured component that no stage hosts is still reported as not wired",
          "[checkpoint][spawn]")
{
    prepare();
    GlobalContext context;
    configure_component_recovery(context.state().view(), {
        .component_id = "somewhere-else", .load = [] { return std::optional<ComponentCheckpoint>{}; },
        .commit = [](const auto &) {}});
    Trace trace;
    REQUIRE_THROWS_WITH((eval_node_with_options<HostedPipeline<ComponentStage>>(interval(0, 1), values<Int>(1),
                                                                                arg<"trace">(&trace))),
                        Catch::Matchers::ContainsSubstring("configured component was not wired"));
}

TEST_CASE("spawn recovery: another component body is another contract", "[checkpoint][spawn]")
{
    prepare();
    std::optional<ComponentCheckpoint> completed;
    Observed                           observed;
    const Ticks                        ticks = values<Int>(1, 2);
    day<HostedPipeline<ComponentStage>>(0, 1, spawn_component_id, completed, observed, ticks);
    REQUIRE_THROWS_WITH(day<HostedPipeline<OtherComponentStage>>(1, 2, spawn_component_id, completed, observed, ticks),
                        Catch::Matchers::ContainsSubstring("incompatible"));
}

TEST_CASE("spawn recovery: a spawn_ that is itself a component member saves its stages whole", "[checkpoint][spawn]")
{
    prepare();
    every_boundary<MemberPipeline<AccumulateStage>>("spawn-pipeline");
}
