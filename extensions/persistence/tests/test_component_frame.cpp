#include <hgraph/lib/std/component.h>
#include <hgraph/persistence/recording_store.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <string>

// Step 5 of the record/replay/table design record: component<G> - Python's
// @component as a wiring function over the mode scope + the record/replay
// operators (whatever backend the active model selects).

namespace polymorphic_record_replay_repro
{
    struct Event
    {};
}

namespace hgraph
{
    template <>
    struct scalar_descriptor<polymorphic_record_replay_repro::Event>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return true; }
        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            auto &registry = TypeRegistry::instance();
            return registry.bundle(
                "tests.record_replay", "Event",
                {{"event_id", registry.value_type("str")}}, {}, true);
        }
    };
}

namespace hgraph::testing
{
    template <>
    struct ts_harness<TS<polymorphic_record_replay_repro::Event>>
        : bundle_ts_harness<TS<polymorphic_record_replay_repro::Event>>
    {};
}

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using record_replay::Mode;
    using PolymorphicEvent = polymorphic_record_replay_repro::Event;

    struct PolymorphicEventIdentityGraph
    {
        [[maybe_unused]] static constexpr auto name = "polymorphic_event_identity_graph";

        static Port<TS<PolymorphicEvent>> compose(
            Wiring &, NamedPort<"events", TS<PolymorphicEvent>> events)
        {
            return events;
        }
    };

    struct PolymorphicEventComponentHarness
    {
        [[maybe_unused]] static constexpr auto name = "polymorphic_event_component_harness";

        static Port<TS<PolymorphicEvent>> compose(
            Wiring &w, Port<TS<PolymorphicEvent>> events)
        {
            return stdlib::component<PolymorphicEventIdentityGraph>(
                w, "polymorphic_events", events);
        }
    };

    struct SumGraph
    {
        [[maybe_unused]] static constexpr auto name = "component_sum_graph";

        static Port<TS<Int>> compose(Wiring &w, NamedPort<"lhs", TS<Int>> lhs, NamedPort<"rhs", TS<Int>> rhs)
        {
            return wire<stdlib::add_>(w, lhs, rhs).as<TS<Int>>();
        }
    };

    struct RecordingHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_record_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            record_replay::scope mode{Mode::Record};
            return stdlib::component<SumGraph>(w, "calc", lhs, rhs);
        }
    };

    struct ReplayHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_replay_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            // Live inputs are wired but REPLACED by the recordings.
            record_replay::scope mode{Mode::Replay};
            return stdlib::component<SumGraph>(w, "calc", lhs, rhs);
        }
    };

    struct ReplayOutputHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_replay_output_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            record_replay::scope mode{Mode::ReplayOutput};
            return stdlib::component<SumGraph>(w, "calc", lhs, rhs);
        }
    };

    struct InnerDouble
    {
        [[maybe_unused]] static constexpr auto name = "component_inner_double";

        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts)
        {
            return wire<stdlib::add_>(w, ts, ts).as<TS<Int>>();
        }
    };

    struct OuterGraph
    {
        [[maybe_unused]] static constexpr auto name = "component_outer_graph";

        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts)
        {
            return stdlib::component<InnerDouble>(w, "inner", Port<TS<Int>>{ts});
        }
    };

    struct NestedHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_nested_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            record_replay::scope mode{Mode::Record};
            return stdlib::component<OuterGraph>(w, "outer", ts);
        }
    };

    struct RecoverHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_recover_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            record_replay::scope mode{Mode::Recover};
            return stdlib::component<SumGraph>(w, "calc", lhs, rhs);
        }
    };

    // The same shape as SumGraph but a DIFFERENT computation - the
    // regression a Compare run must catch.
    struct ProductGraph
    {
        [[maybe_unused]] static constexpr auto name = "component_product_graph";

        static Port<TS<Int>> compose(Wiring &w, NamedPort<"lhs", TS<Int>> lhs, NamedPort<"rhs", TS<Int>> rhs)
        {
            return wire<stdlib::mul_>(w, lhs, rhs).as<TS<Int>>();
        }
    };

    template <typename G>
    struct CompareHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_compare_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            record_replay::scope mode{Mode::Compare};
            return stdlib::component<G>(w, "calc", lhs, rhs);
        }
    };

    struct DirectCompareGraph
    {
        [[maybe_unused]] static constexpr auto name = "direct_compare_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            wire<stdlib::compare>(w, lhs, rhs, arg<"recordable_id">(Str{"sided"}));
            return lhs;
        }
    };

    struct NoIdHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_no_id_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            record_replay::scope mode{Mode::Record};   // no id anywhere
            return stdlib::component<SumGraph>(w, "", lhs, rhs);
        }
    };

    struct PlainHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_plain_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            // No ambient mode: the component is a plain wire<G>.
            return stdlib::component<SumGraph>(w, "calc", lhs, rhs);
        }
    };

    using DedupState = TSB<"ComponentDedupState", Field<"last", TS<Int>>>;

    struct DedupNode
    {
        static constexpr auto name = "component_dedup_node";

        static void eval(In<"ts", TS<Int>> ts,
                         RecordableState<DedupState> state,
                         Out<TS<Int>> out)
        {
            auto last = state.field<"last">();
            if (!last.valid() || last.value().checked_as<Int>() != ts.value())
            {
                last.set(ts.value());
                out.set(ts.value());
            }
        }
    };

    struct DedupGraph
    {
        static constexpr auto name = "component_dedup_graph";

        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts)
        {
            return wire<DedupNode>(w, ts);
        }
    };

    struct DedupHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_dedup_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            return stdlib::component<DedupGraph>(w, "dedup", ts);
        }
    };
}  // namespace

// The FRAME-backend component behaviours (RFC 0025 checkpoint 4): recorded
// through this extension's durable backend, moved here with the backend.
// The in-memory component behaviours (and the fixtures' source of truth)
// remain in core tests/cpp/test_component.cpp.
TEST_CASE("component: Record mode records the named inputs and __out__ while passing values through")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    CHECK_OUTPUT(eval_node<RecordingHarness>(values<Int>(1, none, 3), values<Int>(10, 20, none)),
                 values<Int>(11, 21, 23));

    CHECK(persistence::store_contains(context.state().view(), "calc.lhs"));
    CHECK(persistence::store_contains(context.state().view(), "calc.rhs"));
    CHECK(persistence::store_contains(context.state().view(), "calc.__out__"));
    CHECK(frame_rows(persistence::store_read(context.state().view(), "calc.lhs")) == 2);
    CHECK(frame_rows(persistence::store_read(context.state().view(), "calc.__out__")) == 3);
}

TEST_CASE("component: Replay mode replaces live inputs with the recordings")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    (void)eval_node<RecordingHarness>(values<Int>(1, none, 3), values<Int>(10, 20, none));

    // Different live inputs - the recorded ones win.
    CHECK_OUTPUT(eval_node<ReplayHarness>(values<Int>(100, 100, 100), values<Int>(100, 100, 100)),
                 values<Int>(11, 21, 23));
}

TEST_CASE("component: ReplayOutput mode replays the recorded output directly")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    (void)eval_node<RecordingHarness>(values<Int>(1, none, 3), values<Int>(10, 20, none));

    CHECK_OUTPUT(eval_node<ReplayOutputHarness>(values<Int>(100), values<Int>(100)),
                 values<Int>(11, 21, 23));
}

TEST_CASE("component: nested components chain fully-qualified ids through the scope")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    CHECK_OUTPUT(eval_node<NestedHarness>(values<Int>(5)), values<Int>(10));
    CHECK(persistence::store_contains(context.state().view(), "outer.ts"));
    CHECK(persistence::store_contains(context.state().view(), "outer.__out__"));
    CHECK(persistence::store_contains(context.state().view(), "outer.inner.ts"));
    CHECK(persistence::store_contains(context.state().view(), "outer.inner.__out__"));
}

TEST_CASE("component: Recover seeds inputs at start from the recordings; live ticks override")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    // Record: lhs ticks 1 @t0 and 3 @t2; rhs ticks 10 @t0 and 20 @t1.
    (void)eval_node<RecordingHarness>(values<Int>(1, none, 3), values<Int>(10, 20, none));

    // Recover (same start time): the seeds are the values recorded AT OR
    // BEFORE the start time - lhs=1, rhs=10 - so cycle 0 computes 11 with
    // SILENT live inputs; the live lhs=100 @t1 then overrides (100+10).
    CHECK_OUTPUT(eval_node<RecoverHarness>(values<Int>(none, 100), values<Int>(none, none)),
                 values<Int>(11, 110));
}

TEST_CASE("component: Compare recomputes from recorded inputs and records per-tick equality")
{
    stdlib::register_standard_operators();
    {
        GlobalContext context;
        record_replay::set_config(context.state().view(),
                                  record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

        (void)eval_node<RecordingHarness>(values<Int>(1, none, 3), values<Int>(10, 20, none));

        // Same computation: every recomputed tick matches the recording.
        (void)eval_node<CompareHarness<SumGraph>>(values<Int>(100), values<Int>(100));
        auto matched = record_replay::comparison_summary(context.state().view(), "calc.__compare__");
        REQUIRE(matched.has_value());
        CHECK(matched->compared == 3);
        CHECK(matched->mismatches == 0);

        // The query is total: an unknown key is nullopt, not an error.
        CHECK_FALSE(record_replay::comparison_summary(context.state().view(), "nowhere.__compare__")
                        .has_value());
    }

    // A comparison result is itself an immutable per-run recording. Start a
    // new run, with its own graph-scoped store, to compare the regressed
    // computation (product instead of sum) against the same baseline.
    {
        GlobalContext context;
        record_replay::set_config(context.state().view(),
                                  record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

        (void)eval_node<RecordingHarness>(values<Int>(1, none, 3), values<Int>(10, 20, none));
        (void)eval_node<CompareHarness<ProductGraph>>(values<Int>(100), values<Int>(100));
        auto regressed = record_replay::comparison_summary(context.state().view(), "calc.__compare__");
        REQUIRE(regressed.has_value());
        CHECK(regressed->compared == 3);
        CHECK(regressed->mismatches == 3);
    }
}

TEST_CASE("compare: a one-sided value is recorded as a mismatch")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    // cycle 0: lhs=1, rhs invalid -> mismatch. cycle 1: rhs=1 arrives and
    // lhs still holds 1 -> equal. cycle 2: lhs=5 vs rhs=1 -> mismatch.
    (void)eval_node<DirectCompareGraph>(values<Int>(1, none, 5), values<Int>(none, 1, none));

    auto summary = record_replay::comparison_summary(context.state().view(), "sided.__compare__");
    REQUIRE(summary.has_value());
    CHECK(summary->compared == 3);
    CHECK(summary->mismatches == 2);
}

