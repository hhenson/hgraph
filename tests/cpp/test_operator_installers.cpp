#include <hgraph/lib/std/operators/io.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/util/scope.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <stdexcept>

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

    struct provider_probe : Operator<"provider_lifecycle_probe", Out<TS<Int>>>
    {
    };

    struct leased_provider_probe : Operator<"leased_provider_probe", Out<TS<Int>>>
    {
    };

    struct provider_lifted_add : Operator<"provider_lifted_add",
                                          In<"lhs", TS<Int>>,
                                          In<"rhs", TS<Int>>,
                                          Out<TS<Int>>>
    {
    };

    struct provider_probe_impl
    {
        static constexpr auto name              = "provider_probe_impl";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<Int>> out) { out.set(1); }
    };

    void install_provider_probe()
    {
        register_overload<provider_probe, provider_probe_impl>();
    }

    void install_leased_provider_probe()
    {
        register_overload<leased_provider_probe, provider_probe_impl>();
    }

    void install_provider_lifted_add()
    {
        register_overload<provider_lifted_add, lift<stdlib::scalar_add<Int>>>();
    }

    [[nodiscard]] std::size_t provider_probe_count()
    {
        return OperatorRegistry::instance().overload_signatures("provider_lifecycle_probe").size();
    }
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

TEST_CASE("installers: a throwing installer stays unapplied and retries")
{
    auto &registry = OperatorRegistry::instance();

    int attempts = 0;
    registry.register_installer("test.flaky", [&attempts] {
        ++attempts;
        if (attempts == 1) { throw std::runtime_error("flaky installer"); }
    });

    // The failure propagates and the entry is NOT marked applied.
    CHECK_THROWS_AS(registry.run_installers(), std::runtime_error);
    CHECK(attempts == 1);

    // The next rebuild retries it; success marks it applied.
    registry.run_installers();
    CHECK(attempts == 2);
    registry.run_installers();
    CHECK(attempts == 2);

    // Neutralise before the local capture dies: the installer list outlives
    // this case, and the listener's reset would otherwise replay a dangling
    // callback in a later same-process case.
    registry.register_installer("test.flaky", [] {});
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

TEST_CASE("installers: provider removal erases only its candidates and reset intent")
{
    auto &registry = OperatorRegistry::instance();

    // A directly registered candidate has no provider and must survive
    // removal of a provider contributing to the same overload set.
    register_overload<provider_probe, provider_probe_impl>();
    OperatorProviderHandle provider =
        registry.register_installer("test.removable-provider", &install_provider_probe);
    CHECK(provider.valid());
    CHECK(provider.active());
    CHECK(provider.key() == "test.removable-provider");
    CHECK(provider.live_leases() == 0);

    registry.run_installers();
    CHECK(provider_probe_count() == 2);

    CHECK(registry.remove_provider(provider));
    CHECK_FALSE(provider.active());
    CHECK(provider_probe_count() == 1);
    CHECK_FALSE(registry.remove_provider(provider));

    // The direct candidate is reset, while removed installer intent is not
    // replayed. Reusing the key creates a new generation which the stale
    // handle cannot remove.
    reset_all_registries();
    registry.run_installers();
    CHECK(provider_probe_count() == 0);

    OperatorProviderHandle replacement =
        registry.register_installer("test.removable-provider", &install_provider_probe);
    registry.run_installers();
    CHECK(replacement.active());
    CHECK(provider_probe_count() == 1);
    CHECK_FALSE(registry.remove_provider(provider));
    CHECK(provider_probe_count() == 1);
    CHECK(registry.remove_provider(replacement));
}

TEST_CASE("installers: a throwing provider rolls back candidates before retry")
{
    auto &registry = OperatorRegistry::instance();
    int   attempts = 0;
    OperatorProviderHandle provider = registry.register_installer(
        "test.transactional-provider", [&] {
            register_overload<provider_probe, provider_probe_impl>();
            ++attempts;
            if (attempts == 1) { throw std::runtime_error("provider install failed"); }
        });

    CHECK_THROWS_AS(registry.run_installers(), std::runtime_error);
    CHECK(attempts == 1);
    CHECK(provider_probe_count() == 0);

    registry.run_installers();
    CHECK(attempts == 2);
    CHECK(provider_probe_count() == 1);
    CHECK(registry.remove_provider(provider));
}

TEST_CASE("installers: targeted provider activation leaves unrelated intent pending")
{
    auto &registry = OperatorRegistry::instance();
    int   selected_attempts = 0;
    int   unrelated_attempts = 0;
    OperatorProviderHandle selected = registry.register_installer("test.targeted-provider", [&] {
        ++selected_attempts;
        register_overload<provider_probe, provider_probe_impl>();
    });
    OperatorProviderHandle unrelated = registry.register_installer("test.unrelated-pending-provider", [&] {
        ++unrelated_attempts;
        throw std::runtime_error("unrelated provider must remain pending");
    });

    registry.activate_provider(selected);
    CHECK(selected_attempts == 1);
    CHECK(unrelated_attempts == 0);
    CHECK(provider_probe_count() == 1);
    CHECK(registry.remove_provider(selected));

    CHECK_THROWS_AS(registry.activate_provider(unrelated), std::runtime_error);
    CHECK(unrelated_attempts == 1);
    CHECK(registry.remove_provider(unrelated));
}

TEST_CASE("installers: targeted provider activation nests under an aggregate installer")
{
    auto &registry = OperatorRegistry::instance();
    int   aggregate_attempts = 0;
    int   nested_attempts    = 0;
    OperatorProviderHandle nested;
    OperatorProviderHandle aggregate = registry.register_installer("test.aggregate-provider", [&] {
        ++aggregate_attempts;
        nested = registry.register_installer("test.nested-provider", [&] {
            ++nested_attempts;
            register_overload<provider_probe, provider_probe_impl>();
        });
        registry.activate_provider(nested);
    });

    registry.activate_provider(aggregate);
    CHECK(aggregate_attempts == 1);
    CHECK(nested_attempts == 1);
    CHECK(provider_probe_count() == 1);

    CHECK(registry.remove_provider(nested));
    CHECK(registry.remove_provider(aggregate));
}

TEST_CASE("installers: nested activation failure preserves the error and removes its provider")
{
    auto &registry = OperatorRegistry::instance();
    OperatorProviderHandle nested;
    OperatorProviderHandle aggregate = registry.register_installer("test.failing-aggregate-provider", [&] {
        nested = registry.register_installer("test.failing-nested-provider", [&] {
            register_overload<provider_probe, provider_probe_impl>();
            throw std::runtime_error("nested provider failed");
        });
        auto rollback = make_scope_exit<true>([&] { static_cast<void>(registry.remove_provider(nested)); });
        registry.activate_provider(nested);
        rollback.release();
    });

    CHECK_THROWS_WITH(registry.activate_provider(aggregate), "nested provider failed");
    CHECK(nested.valid());
    CHECK_FALSE(nested.active());
    CHECK(provider_probe_count() == 0);
    CHECK(registry.remove_provider(aggregate));
}

TEST_CASE("installers: provider leases follow graph plan and runtime lifetimes")
{
    auto &registry = OperatorRegistry::instance();
    OperatorProviderHandle provider =
        registry.register_installer("test.leased-provider", &install_leased_provider_probe);
    registry.run_installers();

    GraphBuilder graph_builder;
    {
        Wiring wiring;
        static_cast<void>(wire<leased_provider_probe, TS<Int>>(wiring));
        CHECK(provider.live_leases() == 1);
        CHECK_THROWS_AS(registry.remove_provider(provider), OperatorProviderInUseError);
        graph_builder = std::move(wiring).finish();
    }
    CHECK(provider.live_leases() == 1);
    CHECK_THROWS_AS(registry.remove_provider(provider), OperatorProviderInUseError);
    CHECK(provider.active());

    GraphExecutorValue executor;
    {
        GraphExecutorBuilder executor_builder;
        executor_builder.graph_builder(std::move(graph_builder));
        executor = executor_builder.make_executor();
        CHECK(provider.live_leases() == 1);
        CHECK_THROWS_AS(registry.remove_provider(provider), OperatorProviderInUseError);
    }

    // The reusable builder is gone, but the runtime graph independently
    // retains the same lease until its executor is destroyed.
    CHECK(provider.live_leases() == 1);
    CHECK_THROWS_WITH(registry.remove_provider(provider),
                      Catch::Matchers::ContainsSubstring("test.leased-provider") &&
                          Catch::Matchers::ContainsSubstring("live lease"));
    executor = GraphExecutorValue{};
    CHECK(provider.live_leases() == 0);
    CHECK(registry.remove_provider(provider));
}

TEST_CASE("installers: lifted-kernel plans retain their operator provider")
{
    stdlib::register_standard_operators();
    auto &registry = OperatorRegistry::instance();
    OperatorProviderHandle provider =
        registry.register_installer("test.lifted-provider", &install_provider_lifted_add);
    registry.run_installers();

    {
        GraphBuilder graph_builder;
        {
            Wiring wiring;
            auto values = wire<stdlib::const_, TSL<TS<Int>, 3>>(
                wiring, stdlib::make_list<Int>({Int{1}, Int{2}, Int{3}}));
            static_cast<void>(wire<stdlib::reduce_>(
                wiring, fn<provider_lifted_add>(), values));
            CHECK(provider.live_leases() > 0);
            CHECK_THROWS_AS(registry.remove_provider(provider), OperatorProviderInUseError);
            graph_builder = std::move(wiring).finish();
        }
        CHECK(provider.live_leases() > 0);
        CHECK_THROWS_AS(registry.remove_provider(provider), OperatorProviderInUseError);
    }

    CHECK(provider.live_leases() == 0);
    CHECK(registry.remove_provider(provider));
}
