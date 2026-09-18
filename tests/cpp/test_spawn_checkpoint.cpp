// Recovery through ``spawn_`` (RFC 0039, "Recovery of worker-hosted graphs").
//
// A pipeline's stages are graphs in other processes, so the owner's checkpoint
// state is one image per stage. At the completed-day boundary the executor has
// already settled every stage, which is what makes the images consistent
// without a fence of their own.
//
// The pipeline ends in a sink that acts in a worker process: its trace IS the
// observable result, so a restarted run is compared sample for sample with an
// uninterrupted one.

#include "spawn_test_graphs.h"

#include <hgraph/lib/std/component.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/runtime/component_checkpoint.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <memory>

namespace
{
    template <typename Stage> struct PipelineComponent
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

    /** A first stage with a bound side input that ticks once, on the first day. */
    struct SideInputComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value, Port<TS<Int>> offset, Scalar<"trace", Trace *> trace)
        {
            const std::array inputs{WiringNamedPortRef{"value", value.erased()}, WiringNamedPortRef{"offset", offset.erased()}};
            const auto       out = stdlib::component(w, "spawn-pipeline", inputs,
                [&](std::span<const WiringPortRef> ports) -> WiringPortRef {
                    auto first = bind_(test_stage<SideStage>(), {{"offset", ports[1]}});
                    std::array arguments{input_arg(ports[0], "value")};
                    wire_spawn(w, pipeline_({std::move(first), test_stage<RecoverableSink>(trace.value())}),
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

    template <typename Graph> Observed uninterrupted(const std::vector<std::optional<Int>> &input)
    {
        GlobalContext context;
        Trace         trace;
        (void)eval_node_with_options<Graph>(interval(0, input.size()), input, arg<"trace">(&trace));
        Observed observed;
        append(observed, trace);
        return observed;
    }

    /** ``split`` is the first day's length, or ``input.size()`` for a restart on every tick. */
    template <typename Graph>
    Observed restarted(const std::vector<std::optional<Int>> &input, std::size_t split, bool recover = true)
    {
        std::optional<ComponentCheckpoint> completed;
        Observed                           observed;
        std::size_t                        begin = 0;
        while (begin < input.size())
        {
            const auto    end = split == input.size() ? begin + 1 : (begin == 0 ? split : input.size());
            GlobalContext context;
            if (recover)
            {
                configure_component_recovery(context.state().view(), {
                    .component_id = "spawn-pipeline", .load = [&] { return completed; },
                    .commit = [&](const auto &image) { completed = image; }});
            }
            Trace trace;
            const std::vector<std::optional<Int>> day{input.begin() + begin, input.begin() + end};
            (void)eval_node_with_options<Graph>(interval(begin, end), day, arg<"trace">(&trace));
            append(observed, trace);
            if (recover) { REQUIRE(completed); }
            begin = end;
        }
        return observed;
    }

    const std::vector<std::optional<Int>> ticks = values<Int>(1, 2, none, 3, 4, none, 5);
}  // namespace

TEST_CASE("spawn recovery: a restart at any completed day is invisible to the pipeline's sink", "[checkpoint][spawn]")
{
    prepare();
    using Graph         = PipelineComponent<AccumulateStage>;
    const auto expected = uninterrupted<Graph>(ticks);
    // Pinned so the comparison cannot pass on silence: five ticks, running totals.
    REQUIRE(expected.size() == 5);
    REQUIRE(std::get<1>(expected.back()) == "15");

    for (std::size_t split = 1; split <= ticks.size(); ++split)
    {
        CAPTURE(split);
        CHECK(restarted<Graph>(ticks, split) == expected);
        // The control: the same restarts without recovery lose the running
        // total, and the sink sees it. Without this the comparison above
        // would pass for a pipeline that carried nothing across a boundary.
        if (split < ticks.size()) { CHECK(restarted<Graph>(ticks, split, false) != expected); }
    }
}

TEST_CASE("spawn recovery: an unrecoverable stage is refused where a component learns everything else",
          "[checkpoint][spawn]")
{
    prepare();
    using Graph = PipelineComponent<ForgetfulStage>;
    {
        GlobalContext context;
        configure_component_recovery(context.state().view(), {
            .component_id = "spawn-pipeline", .load = [] { return std::optional<ComponentCheckpoint>{}; },
            .commit = [](const auto &) {}});
        Trace trace;
        REQUIRE_THROWS_WITH((eval_node_with_options<Graph>(interval(0, 1), values<Int>(1), arg<"trace">(&trace))),
                            Catch::Matchers::ContainsSubstring("spawn_ worker 1") &&
                                Catch::Matchers::ContainsSubstring("cannot be recovered"));
    }
    // Recovery not configured: the same pipeline wires and runs, as most do.
    GlobalContext context;
    Trace         trace;
    (void)eval_node_with_options<Graph>(interval(0, 2), values<Int>(1, 2), arg<"trace">(&trace));
    Observed observed;
    append(observed, trace);
    REQUIRE(observed.size() == 2);
    CHECK(std::get<1>(observed.back()) == "3");
}

TEST_CASE("spawn recovery: another pipeline is another contract", "[checkpoint][spawn]")
{
    prepare();
    std::optional<ComponentCheckpoint> completed;
    const auto configure = [&](GlobalContext &context) {
        configure_component_recovery(context.state().view(), {
            .component_id = "spawn-pipeline", .load = [&] { return completed; },
            .commit = [&](const auto &image) { completed = image; }});
    };
    {
        GlobalContext context;
        configure(context);
        Trace trace;
        (void)eval_node_with_options<PipelineComponent<AccumulateStage>>(interval(0, 1), values<Int>(1), arg<"trace">(&trace));
        REQUIRE(completed);
    }
    GlobalContext context;
    configure(context);
    Trace trace;
    REQUIRE_THROWS_WITH((eval_node_with_options<PipelineComponent<Identity<TS<Int>>>>(
                            interval(1, 2), values<Int>(2), arg<"trace">(&trace))),
                        Catch::Matchers::ContainsSubstring("incompatible"));
}

TEST_CASE("spawn recovery: a restored pipeline does not re-send the baselines it already holds", "[checkpoint][spawn]")
{
    prepare();
    // ``offset`` ticks once, on the first day. A fresh pipeline sends every
    // input in full on its first capture; a restored one must not, or the
    // stage counts the offset again on every restart.
    const auto value  = values<Int>(1, 2, 3, 4);
    const auto offset = values<Int>(7, none, none, none);
    const auto run    = [&](std::size_t begin, std::size_t end, std::optional<ComponentCheckpoint> &completed, Observed &observed) {
        GlobalContext context;
        configure_component_recovery(context.state().view(), {
            .component_id = "spawn-pipeline", .load = [&] { return completed; },
            .commit = [&](const auto &image) { completed = image; }});
        Trace trace;
        const std::vector<std::optional<Int>> day_value{value.begin() + begin, value.begin() + end};
        const std::vector<std::optional<Int>> day_offset{offset.begin() + begin, offset.begin() + end};
        (void)eval_node_with_options<SideInputComponent>(interval(begin, end), day_value, day_offset, arg<"trace">(&trace));
        append(observed, trace);
    };
    Observed expected;
    {
        std::optional<ComponentCheckpoint> unused;
        run(0, value.size(), unused, expected);
    }
    REQUIRE(expected.size() == 4);
    REQUIRE(std::get<1>(expected.back()) == "7004");

    std::optional<ComponentCheckpoint> completed;
    Observed                           observed;
    for (std::size_t day = 0; day < value.size(); ++day) { run(day, day + 1, completed, observed); }
    CHECK(observed == expected);
}
