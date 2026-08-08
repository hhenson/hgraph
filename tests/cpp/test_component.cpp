#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/record_replay.h>

#include <catch2/catch_test_macros.hpp>

// Step 5 of the record/replay/table design record: component<G> - Python's
// @component as a wiring function over the mode scope + the record/replay
// operators (whatever backend the active model selects).

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using record_replay::Mode;

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
        static constexpr auto name = "component_dedup_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            return stdlib::component<DedupGraph>(w, "dedup", ts);
        }
    };
}  // namespace

TEST_CASE("component: C++ recordable state works through eval_node")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<DedupHarness>(values<Int>(1, 2, 3, 3, 4)),
                 values<Int>(1, 2, 3, none, 4));
}

TEST_CASE("component: Record mode records the named inputs and __out__ while passing values through")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    CHECK_OUTPUT(eval_node<RecordingHarness>(values<Int>(1, none, 3), values<Int>(10, 20, none)),
                 values<Int>(11, 21, 23));

    CHECK(record_replay::store_contains("calc.lhs"));
    CHECK(record_replay::store_contains("calc.rhs"));
    CHECK(record_replay::store_contains("calc.__out__"));
    CHECK(frame_rows(record_replay::store_read("calc.lhs")) == 2);
    CHECK(frame_rows(record_replay::store_read("calc.__out__")) == 3);
}

TEST_CASE("component: in-memory mode records timestamped values and replays them")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(
        context.state().view(),
        record_replay::Config{.model = std::string{record_replay::IN_MEMORY}});

    {
        record_replay::scope mode{Mode::Record};
        CHECK_OUTPUT(
            eval_node<RecordingHarness>(
                values<Int>(1, none, 3), values<Int>(10, 20, none)),
            values<Int>(11, 21, 23));
    }

    const auto state = context.state().view();
    REQUIRE(state.contains(":memory:calc.lhs"));
    REQUIRE(state.contains(":memory:calc.rhs"));
    REQUIRE(state.contains(":memory:calc.__out__"));
    CHECK(state.get(":memory:calc.lhs").as_list().size() == 2);
    CHECK(state.get(":memory:calc.rhs").as_list().size() == 2);
    CHECK(state.get(":memory:calc.__out__").as_list().size() == 3);

    {
        record_replay::scope mode{Mode::Replay};
        CHECK_OUTPUT(
            eval_node<ReplayHarness>(values<Int>(), values<Int>()),
            values<Int>(11, 21, 23));
    }

    CHECK_THROWS_AS(
        (void)eval_node<CompareHarness<ProductGraph>>(
            values<Int>(), values<Int>()),
        std::runtime_error);
}

TEST_CASE("component: Replay mode replaces live inputs with the recordings")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

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
                              record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    (void)eval_node<RecordingHarness>(values<Int>(1, none, 3), values<Int>(10, 20, none));

    CHECK_OUTPUT(eval_node<ReplayOutputHarness>(values<Int>(100), values<Int>(100)),
                 values<Int>(11, 21, 23));
}

TEST_CASE("component: nested components chain fully-qualified ids through the scope")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    CHECK_OUTPUT(eval_node<NestedHarness>(values<Int>(5)), values<Int>(10));
    CHECK(record_replay::store_contains("outer.ts"));
    CHECK(record_replay::store_contains("outer.__out__"));
    CHECK(record_replay::store_contains("outer.inner.ts"));
    CHECK(record_replay::store_contains("outer.inner.__out__"));
}

TEST_CASE("component: no ambient mode wires plainly; active mode without an id throws")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<PlainHarness>(values<Int>(2), values<Int>(3)), values<Int>(5));
    CHECK_THROWS_AS((void)eval_node<NoIdHarness>(values<Int>(1), values<Int>(1)), std::invalid_argument);
}

TEST_CASE("component: Recover seeds inputs at start from the recordings; live ticks override")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

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
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    (void)eval_node<RecordingHarness>(values<Int>(1, none, 3), values<Int>(10, 20, none));

    // Same computation: every recomputed tick matches the recording.
    (void)eval_node<CompareHarness<SumGraph>>(values<Int>(100), values<Int>(100));
    auto matched = record_replay::comparison_summary(context.state().view(), "calc.__compare__");
    CHECK(matched.compared == 3);
    CHECK(matched.mismatches == 0);

    // A regressed computation (product instead of sum) mismatches every tick.
    (void)eval_node<CompareHarness<ProductGraph>>(values<Int>(100), values<Int>(100));
    auto regressed = record_replay::comparison_summary(context.state().view(), "calc.__compare__");
    CHECK(regressed.compared == 3);
    CHECK(regressed.mismatches == 3);

    // No comparison recorded under an unknown key.
    CHECK_THROWS_AS((void)record_replay::comparison_summary(context.state().view(), "nowhere.__compare__"),
                    std::runtime_error);
}

TEST_CASE("compare: a one-sided value is recorded as a mismatch")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    // cycle 0: lhs=1, rhs invalid -> mismatch. cycle 1: rhs=1 arrives and
    // lhs still holds 1 -> equal. cycle 2: lhs=5 vs rhs=1 -> mismatch.
    (void)eval_node<DirectCompareGraph>(values<Int>(1, none, 5), values<Int>(none, 1, none));

    auto summary = record_replay::comparison_summary(context.state().view(), "sided.__compare__");
    CHECK(summary.compared == 3);
    CHECK(summary.mismatches == 2);
}

namespace
{
    /** Two components claiming the SAME recordable id in one wiring. */
    struct DuplicateIdHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_duplicate_id_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            auto first  = stdlib::component<SumGraph>(w, "calc", lhs, rhs);
            auto second = stdlib::component<SumGraph>(w, "calc", lhs, rhs);
            return wire<stdlib::add_>(w, first, second).template as<TS<Int>>();
        }
    };
}  // namespace

TEST_CASE("component: a duplicate recordable id in one wiring throws (python parity)")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    // Mode-independent: the id claim is structural, not a record/replay
    // concern (upstream raises on the duplicate wiring itself).
    CHECK_THROWS_AS((void)eval_node<DuplicateIdHarness>(values<Int>(1), values<Int>(2)),
                    std::invalid_argument);
}
