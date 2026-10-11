#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/utils/counted_mutex.h>

#include <array>
#include <catch2/catch_test_macros.hpp>

// User-visible REF ports through the full executor (closes the review gap:
// prior REF coverage was view/binding-level plus std operators). A
// user-authored node publishes ``Out<REF<TS<Int>>>`` retargeting between two
// INDEPENDENT upstream producers mid-run; an ordinary ``TS<Int>`` consumer
// follows the reference (REF-transparent input adaptation + alternative-store
// rebinds) across executor cycles.

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct RefSelector
    {
        static constexpr auto name = "ref_selector";

        static void eval(In<"pick_rhs", TS<Bool>> pick_rhs,
                         In<"lhs", TS<Int>, InputValidity::Unchecked> lhs,
                         In<"rhs", TS<Int>, InputValidity::Unchecked> rhs,
                         Out<REF<TS<Int>>> out)
        {
            if (!pick_rhs.modified()) { return; }  // retarget only on a selection change
            out.set(pick_rhs.value() ? rhs.reference() : lhs.reference());
        }
    };

    struct RefSelectGraph
    {
        [[maybe_unused]] static constexpr auto name = "ref_select_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> pick_rhs, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            // The REF<TS<Int>> port is consumed as a plain TS<Int> — the
            // REF-transparent binding dereferences whatever output the
            // reference currently designates.
            return wire<RefSelector>(w, pick_rhs, lhs, rhs).as<TS<Int>>();
        }
    };

    struct ForwardDowncastRefGraph
    {
        static constexpr auto name = "forward_downcast_ref_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> source)
        {
            auto source_ref = source.template as<REF<TS<Int>>>();
            return wire<stdlib::downcast_ref, REF<TS<Int>>>(w, source_ref).template as<TS<Int>>();
        }
    };

    struct NestedStructuralRefProbe
    {
        static constexpr auto name = "nested_structural_ref_probe";

        static void eval(In<"refs", TSL<REF<TSL<TS<Int>, 2>>, 2>> refs,
                         Out<TS<Bool>> out)
        {
            const auto first = refs[0].value();
            const auto second = refs[1].value();
            const auto *target = schema_descriptor<TSL<TS<Int>, 2>>::ts_meta();
            out.set(first.is_non_peered() && second.is_non_peered() &&
                    time_series_schema_equivalent(first.target_schema(), target) &&
                    time_series_schema_equivalent(second.target_schema(), target) &&
                    first.items().size() == 2 && second.items().size() == 2);
        }
    };

    struct NestedStructuralRefGraph
    {
        static constexpr auto name = "nested_structural_ref_graph";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Int>> b,
                                      Port<TS<Int>> c, Port<TS<Int>> d)
        {
            auto lhs = stdlib::to_tsl<TSL<TS<Int>, 2>>(w, a, b);
            auto rhs = stdlib::to_tsl<TSL<TS<Int>, 2>>(w, c, d);
            auto refs = stdlib::to_tsl<TSL<TSL<TS<Int>, 2>, 2>>(w, lhs, rhs);
            return wire<NestedStructuralRefProbe>(w, refs);
        }
    };

    std::size_t   owned_export_evaluations{0};
    std::uint64_t pair_delta_locks{0};

    struct OwnedExportValue
    {
        static constexpr auto name = "owned_export_value";
        static void           eval(In<"value", TS<Int>, InputValidity::Unchecked> value, Out<TS<Int>> out) {
            ++owned_export_evaluations;
            if (value.valid() && value.modified()) { out.set(value.value()); }
        }
    };

    struct ExportReference
    {
        static constexpr auto name = "export_reference";
        static void           eval(In<"value", REF<TS<Int>>, InputValidity::Unchecked> value, Out<REF<TS<Int>>> out) {
            if (value.valid() && value.modified()) { out.set(value.value()); }
        }
    };

    struct ExportFirstOwned
    {
        static Port<REF<TS<Int>>> compose(Wiring &w, Port<TS<Int>> first, Port<TS<Int>>) {
            auto child = wire<OwnedExportValue>(w, first);
            return wire<ExportReference>(w, child.as<REF<TS<Int>>>());
        }
    };

    struct ExportSecondOwned
    {
        static Port<REF<TS<Int>>> compose(Wiring &w, Port<TS<Int>>, Port<TS<Int>> second) {
            auto child = wire<OwnedExportValue>(w, second);
            return wire<ExportReference>(w, child.as<REF<TS<Int>>>());
        }
    };

    struct FollowOwnedExport
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> enabled, Port<TS<Int>> first, Port<TS<Int>> second) {
            auto selected = wire<stdlib::switch_>(
                w, enabled, stdlib::switch_cases({{Value{true}, fn<ExportFirstOwned>()}, {Value{false}, fn<ExportSecondOwned>()}}),
                first, second);
            return wire<OwnedExportValue>(w, selected.as<TS<Int>>());
        }
    };

    struct HoldFirstExport
    {
        static void eval(In<"value", REF<TS<Int>>> value, State<Bool> captured, Out<REF<TS<Int>>> out) {
            if (!captured.get()) {
                captured.set(true);
                out.set(value.value());
            }
        }
    };

    struct ObserveHeldExport
    {
        static void eval(In<"held", TS<Int>, InputValidity::Unchecked> held, In<"step", TS<Int>> step, Out<TS<Int>> out) {
            if (step.modified()) { out.set(held.valid() ? held.value() : Int{-1}); }
        }
    };

    struct PublishSavedExportLater
    {
        static void eval(In<"held", REF<TS<Int>>, InputValidity::Unchecked> held, In<"step", TS<Int>> step,
                         Scalar<"begin", Int> begin, Out<REF<TS<Int>>> out) {
            if (step.modified() && step.value() >= begin.value() && held.valid()) { out.set(held.value()); }
        }
    };

    struct ObserveLateOwnedExport
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> enabled, Port<TS<Int>> first, Port<TS<Int>> second,
                                     Port<TS<Int>> step, Scalar<"begin", Int> begin) {
            auto selected = wire<stdlib::switch_>(
                w, enabled, stdlib::switch_cases({{Value{true}, fn<ExportFirstOwned>()}, {Value{false}, fn<ExportSecondOwned>()}}),
                first, second);
            auto held = wire<HoldFirstExport>(w, selected.as<REF<TS<Int>>>());
            auto late = wire<PublishSavedExportLater>(w, held, step, begin);
            return wire<ObserveHeldExport>(w, late.as<TS<Int>>(), step);
        }
    };

    struct ObserveOwnedExport
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> enabled, Port<TS<Int>> first, Port<TS<Int>> second,
                                     Port<TS<Int>> step) {
            auto selected = wire<stdlib::switch_>(
                w, enabled, stdlib::switch_cases({{Value{true}, fn<ExportFirstOwned>()}, {Value{false}, fn<ExportSecondOwned>()}}),
                first, second);
            auto held = wire<HoldFirstExport>(w, selected.as<REF<TS<Int>>>());
            return wire<ObserveHeldExport>(w, held.as<TS<Int>>(), step);
        }
    };

    std::vector<Int>    manual_values{};
    TimeSeriesReference manual_saved{};
    struct SaveManualValue
    {
        static void eval(In<"value", TS<Int>> value) { manual_values.push_back(value.value()); }
    };
    struct SaveManualReference
    {
        static void eval(In<"held", REF<TS<Int>>> held) { manual_saved = held.value(); }
    };
    struct ManualObserveOwnedExport
    {
        static void compose(Wiring &w) {
            auto enabled  = wire<stdlib::replay_impl, TS<Bool>>(w, Str{"enabled"});
            auto first    = wire<stdlib::replay_impl, TS<Int>>(w, Str{"first"});
            auto second   = wire<stdlib::replay_impl, TS<Int>>(w, Str{"second"});
            auto step     = wire<stdlib::replay_impl, TS<Int>>(w, Str{"step"});
            auto selected = wire<stdlib::switch_>(
                w, enabled, stdlib::switch_cases({{Value{true}, fn<ExportFirstOwned>()}, {Value{false}, fn<ExportSecondOwned>()}}),
                first, second);
            auto held = wire<HoldFirstExport>(w, selected.as<REF<TS<Int>>>());
            wire<SaveManualReference>(w, held);
            wire<SaveManualValue>(w, wire<ObserveHeldExport>(w, held.as<TS<Int>>(), step));
        }
    };

    struct ExportFirstParent
    {
        static Port<REF<TS<Int>>> compose(Wiring &w, Port<TS<Int>> first, Port<TS<Int>>) {
            return wire<ExportReference>(w, first.as<REF<TS<Int>>>());
        }
    };

    struct ExportSecondParent
    {
        static Port<REF<TS<Int>>> compose(Wiring &w, Port<TS<Int>>, Port<TS<Int>> second) {
            return wire<ExportReference>(w, second.as<REF<TS<Int>>>());
        }
    };

    struct FollowHeldParentExport
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> enabled, Port<TS<Int>> first, Port<TS<Int>> second) {
            auto selected = wire<stdlib::switch_>(
                w, enabled,
                stdlib::switch_cases({{Value{true}, fn<ExportFirstParent>()}, {Value{false}, fn<ExportSecondParent>()}}), first,
                second);
            return wire<OwnedExportValue>(w, wire<HoldFirstExport>(w, selected.as<REF<TS<Int>>>()).as<TS<Int>>());
        }
    };

    using ExportPair = UnNamedTSB<Field<"left", TS<Int>>, Field<"right", TS<Int>>>;
    struct ExportPairReference
    {
        static void eval(In<"value", REF<ExportPair>> value, Out<REF<ExportPair>> out) {
            if (value.modified()) { out.set(value.value()); }
        }
    };
    struct ExportDuplicatePair
    {
        static Port<REF<ExportPair>> compose(Wiring &w, Port<TS<Int>> left, Port<TS<Int>>) {
            return wire<ExportPairReference>(w, stdlib::to_tsb<ExportPair>(w, left, left));
        }
    };
    struct ExportSeparatePair
    {
        static Port<REF<ExportPair>> compose(Wiring &w, Port<TS<Int>> left, Port<TS<Int>> right) {
            return wire<ExportPairReference>(w, stdlib::to_tsb<ExportPair>(w, left, right));
        }
    };
    struct FollowPairRight
    {
        static void eval(In<"value", ExportPair, InputValidity::Unchecked> value, Out<TS<Int>> out) {
            const auto right = value.field<"right">();
            if (right.valid() && right.modified()) { out.set(right.value()); }
        }
    };
    struct ForwardPairDelta
    {
        static void eval(In<"value", TsVar<"S">, InputValidity::Unchecked> value, Out<TsVar<"S">> out) {
            const auto before = type_system_lock_count();
            if (value.modified() && value.valid()) { apply_delta(out, value.delta_value()); }
            pair_delta_locks += type_system_lock_count() - before;
        }
    };
    struct FollowChangedPairExport
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> duplicate_left, Port<TS<Int>> left, Port<TS<Int>> right) {
            auto selected = wire<stdlib::switch_>(
                w, duplicate_left,
                stdlib::switch_cases({{Value{true}, fn<ExportDuplicatePair>()}, {Value{false}, fn<ExportSeparatePair>()}}), left,
                right);
            return wire<FollowPairRight>(w, wire<ForwardPairDelta>(w, selected.as<ExportPair>()).as<ExportPair>());
        }
    };
    template <bool First> struct ExportOwnedPair
    {
        static Port<REF<ExportPair>> compose(Wiring &w, Port<TS<Bool>> duplicate_left, Port<TS<Int>> first_left,
                                             Port<TS<Int>> first_right, Port<TS<Int>> second_left, Port<TS<Int>> second_right) {
            auto left  = wire<OwnedExportValue>(w, First ? first_left : second_left);
            auto right = wire<OwnedExportValue>(w, First ? first_right : second_right);
            return wire<stdlib::switch_>(
                       w, duplicate_left,
                       stdlib::switch_cases({{Value{true}, fn<ExportDuplicatePair>()}, {Value{false}, fn<ExportSeparatePair>()}}),
                       left, right)
                .as<REF<ExportPair>>();
        }
    };
    struct FollowChangedOwnedPairExport
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> enabled, Port<TS<Bool>> duplicate_left, Port<TS<Int>> first_left,
                                     Port<TS<Int>> first_right, Port<TS<Int>> second_left, Port<TS<Int>> second_right) {
            auto selected = wire<stdlib::switch_>(
                w, enabled,
                stdlib::switch_cases({{Value{true}, fn<ExportOwnedPair<true>>()}, {Value{false}, fn<ExportOwnedPair<false>>()}}),
                duplicate_left, first_left, first_right, second_left, second_right);
            return wire<FollowPairRight>(w, wire<ForwardPairDelta>(w, selected.as<ExportPair>()).as<ExportPair>());
        }
    };
}  // namespace

TEST_CASE("REF owner export: following a changed branch samples its actual child endpoint") {
    stdlib::register_standard_operators();
    for (int run = 0; run < 2; ++run) {
        CHECK_OUTPUT(eval_node<FollowOwnedExport>(values<Bool>(true, true, false, false, true, none),
                                                  values<Int>(7, 8, 9, 10, 11, 12), values<Int>(20, none, none, 21, none, none)),
                     values<Int>(7, 8, 20, 21, 11, 12));
    }
}

TEST_CASE("REF owner export: stopped targets survive the removal cycle and expire before the next") {
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ObserveOwnedExport>(values<Bool>(true, true, false, false, true, true, false, true, none),
                                               values<Int>(7, 8, 9, 10, 11, 12, 13, 14, 15),
                                               values<Int>(20, none, none, 21, none, none, 22, none, 23),
                                               values<Int>(1, 1, 1, 1, 1, 1, 1, 1, 1)),
                 values<Int>(7, 8, 8, -1, -1, -1, -1, -1, -1));
}

TEST_CASE("REF owner export: stopping a forwarding owner preserves the captured parent endpoint") {
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<FollowHeldParentExport>(values<Bool>(true, true, false, false, true, true, false, true, none),
                                                   values<Int>(7, 8, 9, 10, 11, 12, 13, 14, 15),
                                                   values<Int>(20, none, none, 21, none, none, 22, none, 23)),
                 values<Int>(7, 8, 9, 10, 11, 12, 13, 14, 15));
}

TEST_CASE("REF owner export: binding a saved stopped designation obeys the cycle boundary") {
    stdlib::register_standard_operators();
    const auto run = [](Int begin) {
        return eval_node<ObserveLateOwnedExport>(values<Bool>(true, true, false, false, true, none),
                                                 values<Int>(7, 8, 9, 10, 11, 12), values<Int>(20, none, none, 21, none, none),
                                                 values<Int>(1, 2, 3, 4, 5, 6), begin);
    };
    CHECK_OUTPUT(run(3), values<Int>(-1, -1, 8, -1, -1, -1));
    CHECK_OUTPUT(run(4), values<Int>(-1, -1, -1, -1, -1, -1));
}

TEST_CASE("REF owner export: changing a tree designation samples its changed child") {
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<FollowChangedPairExport>(values<Bool>(false, none, none, true, none, false, none),
                                                    values<Int>(7, 8, none, none, 9, none, none),
                                                    values<Int>(20, none, 21, none, none, none, 22)),
                 values<Int>(20, none, 21, 8, 9, 21, 22));
}

TEST_CASE("REF owner export: a composed owned tree samples a changed retained child") {
    stdlib::register_standard_operators();
    pair_delta_locks = 0;
    CHECK_OUTPUT(
        eval_node<FollowChangedOwnedPairExport>(values<Bool>(true, none, none, none, none, false, none, true, none, none),
                                                values<Bool>(false, none, none, true, none, false, none, false, none, none),
                                                values<Int>(7, 8, none, none, 9, 10, none, 7, none, none),
                                                values<Int>(20, none, 21, none, none, 22, none, 30, 31, none),
                                                values<Int>(9, none, none, none, none, 9, 10, none, none, none),
                                                values<Int>(40, none, none, none, none, 40, none, none, none)),
        values<Int>(20, none, 21, 8, 9, 40, none, 30, 31, none));
    CHECK(pair_delta_locks == 0);
}

TEST_CASE("REF owner export: an independently awakened consumer reads an idle owned endpoint") {
    stdlib::register_standard_operators();
    for (int run = 0; run < 2; ++run) {
        owned_export_evaluations = 0;
        CHECK_OUTPUT(eval_node<ObserveOwnedExport>(values<Bool>(true, none, none, none), values<Int>(7, none, none, none),
                                                   values<Int>(20, none, none, none), values<Int>(1, 1, 1, 1)),
                     values<Int>(7, 7, 7, 7));
        CHECK(owned_export_evaluations == 1);
    }
}

TEST_CASE("REF owner export: manual root cycles expire retired owners and teardown cancels pending work") {
    stdlib::register_standard_operators();
    for (const auto cycle_count : {3, 4}) {
        manual_values.clear();
        manual_saved = TimeSeriesReference{};
        {
            auto builder = build_graph<ManualObserveOwnedExport>();
            set_replay_values<Bool>(builder.global_state(), "enabled", values<Bool>(true, true, false, false));
            set_replay_values<Int>(builder.global_state(), "first", values<Int>(7, 8, 9, 10));
            set_replay_values<Int>(builder.global_state(), "second", values<Int>(20, none, none, 21));
            set_replay_values<Int>(builder.global_state(), "step", values<Int>(1, 1, 1, 1));
            MockRootGraph root{builder};
            auto          graph = root.graph();
            REQUIRE(graph.node_count() != 0);
            graph.start(MIN_ST);
            const std::array<Int, 4> expected{7, 8, 8, -1};
            for (int cycle = 0; cycle < cycle_count; ++cycle) {
                const auto time = MIN_ST + MIN_TD * cycle;
                REQUIRE(graph.evaluate(time));
                CHECK(root.executor().evaluation_clock().evaluation_time() == time);
                REQUIRE(manual_values.size() == static_cast<std::size_t>(cycle + 1));
                CHECK(manual_values.back() == expected[cycle]);
            }
            const auto time = MIN_ST + MIN_TD * (cycle_count - 1);
            graph.stop(time);
            CHECK(manual_saved.has_output() == (cycle_count == 3));
        }
        CHECK_FALSE(manual_saved.has_output());
    }
}

TEST_CASE("REF executor: a user REF output retargets between independent producers mid-run")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<RefSelectGraph>(values<Bool>(true, none, false, none, none),
                                           values<Int>(1, 2, none, 4, none),
                                           values<Int>(10, none, 30, none, 50)),
                 // cycle 0: ref -> rhs, sees 10. cycle 1: rhs silent (lhs's 2
                 // is not observed — the ref points at rhs). cycle 2: retarget
                 // to lhs, whose current value (2) is observed on rebind.
                 // cycle 3: lhs ticks 4. cycle 4: rhs's 50 is not observed.
                 values<Int>(10, none, 2, 4, none));
}

TEST_CASE("REF executor: a reference retarget to an already-valid producer samples its current value")
{
    stdlib::register_standard_operators();

    // lhs ticks only at cycle 0; the retarget at cycle 2 must still deliver
    // that value to the consumer (rebind-time sampling, matching the Python
    // REF semantics), not wait for the producer's next tick.
    CHECK_OUTPUT(eval_node<RefSelectGraph>(values<Bool>(true, none, false, none),
                                           values<Int>(7, none, none, none),
                                           values<Int>(10, 20, none, none)),
                 values<Int>(10, 20, 7, none));
}

TEST_CASE("downcast_ref: C++ wiring still forwards the referenced output")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ForwardDowncastRefGraph>(values<Int>(1, 2, 3)),
                 values<Int>(1, 2, 3));
}

TEST_CASE("REF executor: nested structural sources adapt to fixed REF children")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<NestedStructuralRefGraph>(values<Int>(1, 10), values<Int>(2, 20),
                                                      values<Int>(3, 30), values<Int>(4, 40)),
                 values<Bool>(true, none));
}
