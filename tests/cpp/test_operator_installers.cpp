#include <hgraph/lib/std/operators/io.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <catch2/catch_test_macros.hpp>

// The registration-installer contract (RFC 0025, checkpoint 3): a registry
// reset clears the overload TABLE but keeps registration INTENT, so one
// rebuild call replays every installer — an extension's exactly as core's.

namespace
{
    using namespace hgraph;

    /** A minimal external record backend registered through public headers
        only — the shape an installed extension uses. */
    struct probe_record_impl
    {
        static constexpr auto name = "probe_record";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::effective_backend_is(context, "test.probe.mem");
        }

        static auto defaults()
        {
            return std::tuple{arg<"key">(Str{"out"}), arg<"model">(Str{})};
        }

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         Scalar<"key", Str> key, Scalar<"model", Str>, GlobalStateView gs)
        {
            if (!ts.modified()) { return; }
            gs.set(":probe:" + key.value(), Value{ts.value()});
        }
    };

    /** The matching replay half: emits the recorded values cycle-aligned. */
    struct probe_replay_impl
    {
        static constexpr auto name              = "probe_replay";
        static constexpr bool schedule_on_start = true;

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::effective_backend_is(context, "test.probe.mem");
        }

        static auto defaults()
        {
            return std::tuple{arg<"recordable_id">(Str{""}), arg<"model">(Str{})};
        }

        static void eval(Scalar<"key", Str> key, Scalar<"recordable_id", Str>,
                         Scalar<"model", Str>, GlobalStateView gs, NodeScheduler sched,
                         Out<TsVar<"O">> out)
        {
            const auto stored = gs.get(":probe:" + key.value());
            if (!stored.valid()) { return; }
            apply_delta(out, stored);
            static_cast<void>(sched);
        }
    };

    void install_probe_backend()
    {
        register_overload<stdlib::record, probe_record_impl>();
        register_overload<stdlib::replay, probe_replay_impl>();
    }

    [[nodiscard]] std::size_t record_overload_count()
    {
        return OperatorRegistry::instance().overload_signatures("record").size();
    }

    struct ProbeRoundTrip
    {
        static constexpr auto name = "probe_round_trip";

        static void compose(Wiring &w)
        {
            record_replay::set_config(
                w.global_state(),
                record_replay::RecordReplayConfig{.backend = "test.probe.mem"});
            auto source = wire<stdlib::replay, TS<Int>>(w, Str{"in"});
            wire<stdlib::record>(w, source, Str{"out"});
        }
    };
}  // namespace

TEST_CASE("installers: a reset-and-rebuild replays extensions exactly as core")
{
    stdlib::register_standard_operators();
    auto &registry = OperatorRegistry::instance();

    const std::size_t core_count = record_overload_count();
    REQUIRE(core_count > 0);

    // The probe installer registers the way an installed extension does.
    // (Its guard names a backend id nothing else selects, so the entry it
    // leaves behind for later same-process cases is inert.)
    registry.register_installer("test.probe", &install_probe_backend);
    registry.run_installers();
    CHECK(record_overload_count() == core_count + 1);

    // Reset empties the table; intent survives.
    reset_all_registries();
    CHECK(record_overload_count() == 0);

    // ONE core rebuild call replays the probe alongside core.
    stdlib::register_standard_operators();
    CHECK(record_overload_count() == core_count + 1);

    // Idempotent between resets: repeated rebuild calls never duplicate.
    stdlib::register_standard_operators();
    registry.run_installers();
    CHECK(record_overload_count() == core_count + 1);

    // Re-registering an applied key replaces the callback without
    // re-applying it.
    registry.register_installer("test.probe", &install_probe_backend);
    registry.run_installers();
    CHECK(record_overload_count() == core_count + 1);
}

TEST_CASE("installers: an external backend records and replays through the core markers")
{
    stdlib::register_standard_operators();
    auto &registry = OperatorRegistry::instance();
    registry.register_installer("test.probe", &install_probe_backend);
    registry.run_installers();

    GlobalContext context;
    GraphBuilder  graph_builder = build_graph<ProbeRoundTrip>();
    graph_builder.global_state().set(":probe:in", Value{Int{42}});

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{4});
    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();
    view.run();

    // The value travelled probe replay -> probe record through registry
    // resolution of the CORE markers — the extension seam end to end.
    const auto state = view.graph().global_state();
    REQUIRE(state.get(":probe:out").valid());
    CHECK(state.get(":probe:out").checked_as<Int>() == 42);
}
