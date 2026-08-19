// Wiring-level sub-graph compilation (compile_subgraph<G> / nested_<G>).
//
// These tests drive the developer-guide *Nested Graphs* design: a sub-graph
// ``compose`` runs against boundary placeholder ports (no stub nodes), is
// compiled into a CompiledSubGraph, and is owned at runtime by the existing
// single_nested_graph_node. Sources/sinks are the standard ``replay`` /
// ``record`` testing nodes; the child bodies use the lib/std operators.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/map_node.h>
#include <hgraph/runtime/mesh_node.h>
#include <hgraph/runtime/ordered_reduce_node.h>
#include <hgraph/runtime/reduce_node.h>
#include <hgraph/runtime/switch_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>

#include <catch2/catch_test_macros.hpp>

#include <stdexcept>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    using IntPair = TSL<TS<Int>, 2>;
    using StructuralPair =
        UnNamedTSB<Field<"lhs", TS<Int>>, Field<"rhs", TS<Int>>>;

    // ---------------- sub-graph definitions under test ----------------

    struct AddOneSubGraph
    {
        static constexpr auto name = "add_one_subgraph";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> in)
        {
            using namespace hgraph::stdlib::syntax;
            return (in + Int{1}).as<TS<Int>>();
        }
    };

    struct SumSubGraph
    {
        static constexpr auto name = "sum_subgraph";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            using namespace hgraph::stdlib::syntax;
            return (lhs + rhs).as<TS<Int>>();
        }
    };

    // One boundary arg consumed by two child input endpoints.
    struct FanOutSubGraph
    {
        static constexpr auto name = "fan_out_subgraph";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> in)
        {
            using namespace hgraph::stdlib::syntax;
            return (in + in).as<TS<Int>>();
        }
    };

    struct ScaledSubGraph
    {
        static constexpr auto name = "scaled_subgraph";
        static Port<TS<Int>>  compose(Wiring &, NamedPort<"in", TS<Int>> in, Scalar<"factor", Int> factor)
        {
            using namespace hgraph::stdlib::syntax;
            return (in * factor.value()).as<TS<Int>>();
        }
    };

    // A sub-graph that is itself a source: its child schedule must drive the
    // outer graph through the nested node's schedule propagation (replay is a
    // self-rescheduling source).
    struct SourceOnlySubGraph
    {
        static constexpr auto name = "source_only_subgraph";
        static Port<TS<Int>>  compose(Wiring &w) { return wire<stdlib::replay_impl, TS<Int>>(w, Str{"src"}); }
    };

    // A sink sub-graph: compose returns void; the nested node becomes a sink.
    struct RecordSubGraph
    {
        static constexpr auto name = "record_subgraph";
        static void           compose(Wiring &w, Port<TS<Int>> in) { wire<stdlib::dense_record_impl>(w, in, Str{"out"}); }
    };

    // Returning a boundary input directly is the pass-through mode
    // (alias_parent_input): the outer output aliases the outer input's source.
    struct PassThroughSubGraph
    {
        static constexpr auto name = "pass_through_subgraph";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> in) { return in; }
    };

    struct IntIdentity
    {
        static constexpr auto name = "int_identity";

        static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out)
        {
            out.set(value.value());
        }
    };

    struct RefSelector
    {
        static constexpr auto name = "nested_ref_selector";

        static void eval(In<"pick_rhs", TS<Bool>> pick_rhs,
                         In<"lhs", TS<Int>, InputValidity::Unchecked> lhs,
                         In<"rhs", TS<Int>, InputValidity::Unchecked> rhs,
                         Out<REF<TS<Int>>> out)
        {
            if (pick_rhs.modified()) { out.set(pick_rhs.value() ? rhs.reference() : lhs.reference()); }
        }
    };

    // The declared REF boundary forces the outer plain source through the
    // to-REF alternative before it is bound into the child graph.
    struct RefInputSubGraph
    {
        static constexpr auto name = "ref_input_subgraph";
        static Port<TS<Int>>  compose(Wiring &w, Port<REF<TS<Int>>> in)
        {
            return wire<IntIdentity>(w, in);
        }
    };

    // A plain boundary fed by a REF source exercises the inverse adaptation.
    struct PlainInputSubGraph
    {
        static constexpr auto name = "plain_input_subgraph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in)
        {
            return wire<IntIdentity>(w, in);
        }
    };

    // The REF is produced inside the child and forwarded through the nested
    // node before an outer plain consumer dereferences it.
    struct RefOutputSubGraph
    {
        static constexpr auto name = "ref_output_subgraph";
        static Port<REF<TS<Int>>> compose(Wiring &w,
                                          Port<TS<Bool>> pick_rhs,
                                          Port<TS<Int>> lhs,
                                          Port<TS<Int>> rhs)
        {
            return wire<RefSelector>(w, pick_rhs, lhs, rhs);
        }
    };

    struct RefPassThroughSubGraph
    {
        static constexpr auto name = "ref_pass_through_subgraph";
        static Port<REF<TS<Int>>> compose(Wiring &, Port<REF<TS<Int>>> in) { return in; }
    };

    // The callable exposes a plain logical result while its actual terminal
    // remains a REF-producing node. CompiledSubGraph must retain both views.
    struct DereferencedRefOutputSubGraph
    {
        static constexpr auto name = "dereferenced_ref_output_subgraph";

        static Port<TS<Int>> compose(Wiring &w,
                                     Port<TS<Bool>> pick_rhs,
                                     Port<TS<Int>> lhs,
                                     Port<TS<Int>> rhs)
        {
            return wire<RefSelector>(w, pick_rhs, lhs, rhs).as<TS<Int>>();
        }
    };

    struct PairSum
    {
        static constexpr auto name = "pair_sum";

        static void eval(In<"values", IntPair> values, Out<TS<Int>> out)
        {
            out.set(values[0].value() + values[1].value());
        }
    };

    struct StructuralRefInputSubGraph
    {
        static constexpr auto name = "structural_ref_input_subgraph";
        static Port<TS<Int>>  compose(Wiring &w, Port<REF<IntPair>> in)
        {
            return wire<PairSum>(w, in);
        }
    };

    struct StructuralPairSubGraph
    {
        static constexpr auto name = "structural_pair_subgraph";

        static Port<StructuralPair> compose(
            Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            return stdlib::to_tsb<StructuralPair>(w, lhs, rhs);
        }
    };

    struct StructuralListSubGraph
    {
        static constexpr auto name = "structural_list_subgraph";

        static Port<IntPair> compose(
            Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            return stdlib::to_tsl<IntPair>(w, lhs, rhs).as<IntPair>();
        }
    };

    struct DynamicListPassThroughSubGraph
    {
        static constexpr auto name = "dynamic_list_pass_through_subgraph";

        static Port<TSL<TS<Int>>> compose(
            Wiring &, Port<TSL<TS<Int>>> values)
        {
            return values;
        }
    };

    struct StructuralPairPassThroughSubGraph
    {
        static constexpr auto name = "structural_pair_pass_through_subgraph";

        static Port<StructuralPair> compose(
            Wiring &w, Port<StructuralPair> value, Port<TS<Int>> trigger)
        {
            static_cast<void>(wire<stdlib::null_sink>(w, trigger));
            return value;
        }
    };

    struct EmptyStructuralPairReferenceSource
    {
        static constexpr auto name = "empty_structural_pair_reference_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<REF<StructuralPair>> out)
        {
            out.set(TimeSeriesReference::empty(
                schema_descriptor<StructuralPair>::ts_meta()));
        }
    };

    // A structural-initializer boundary: the outer arg is a non-peered TSL whose
    // shape is mirrored into the child compile (boundary leaves), so the child
    // consumer binds leaf-wise.
    struct TslRecordSubGraph
    {
        static constexpr auto name = "tsl_record_subgraph";
        static void           compose(Wiring &w, Port<TSL<TS<Int>, 2>> in) { wire<stdlib::dense_record_impl>(w, in, Str{"out"}); }
    };

    struct GlobalStateSeedingSubGraph
    {
        static constexpr auto name = "global_state_seeding_subgraph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in)
        {
            using namespace hgraph::stdlib::syntax;
            w.global_state().set("seeded", Value{Int{1}});
            return (in + Int{1}).as<TS<Int>>();
        }
    };

    // ---------------- top-level test graphs ----------------

    // Lightweight wrappers with declared inputs/outputs, driven through eval_node.
    struct NestedAddOneGraph
    {
        static constexpr auto name = "nested_add_one_graph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in) { return nested_<AddOneSubGraph>(w, in); }
    };

    struct NestedSumGraph
    {
        static constexpr auto name = "nested_sum_graph";
        static Port<TS<Int>>  compose(Wiring &w, NamedPort<"a", TS<Int>> a, NamedPort<"b", TS<Int>> b)
        {
            return nested_<SumSubGraph>(w, a, b);
        }
    };

    struct NestedFanOutGraph
    {
        static constexpr auto name = "nested_fan_out_graph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in) { return nested_<FanOutSubGraph>(w, in); }
    };

    struct NestedSourceOnlyGraph
    {
        static constexpr auto name = "nested_source_only_graph";
        static void           compose(Wiring &w)
        {
            wire<stdlib::dense_record_impl>(w, nested_<SourceOnlySubGraph>(w), Str{"out"});
        }
    };

    struct NestedScaledGraph
    {
        static constexpr auto name = "nested_scaled_graph";
        static void           compose(Wiring &w)
        {
            auto in = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});

            auto a = nested_<ScaledSubGraph>(w, in, Int{2});
            auto b = nested_<ScaledSubGraph>(w, in, Int{2});
            auto c = nested_<ScaledSubGraph>(w, in, Int{3});

            // Equal scalars intern to one nested node; distinct scalars do not.
            CHECK(a.node() == b.node());
            CHECK(a.node() != c.node());

            wire<stdlib::dense_record_impl>(w, a, Str{"doubled"});
            wire<stdlib::dense_record_impl>(w, c, Str{"tripled"});
        }
    };

    struct NestedRecordGraph
    {
        static constexpr auto name = "nested_record_graph";
        static void           compose(Wiring &w)
        {
            auto in = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            nested_<RecordSubGraph>(w, in);
        }
    };

    struct NestedPassThroughGraph
    {
        static constexpr auto name = "nested_pass_through_graph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in)
        {
            return nested_<PassThroughSubGraph>(w, in);
        }
    };

    struct NestedRefInputGraph
    {
        static constexpr auto name = "nested_ref_input_graph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in)
        {
            return nested_<RefInputSubGraph>(w, in);
        }
    };

    struct NestedPlainInputFromRefGraph
    {
        static constexpr auto name = "nested_plain_input_from_ref_graph";
        static Port<TS<Int>> compose(Wiring &w,
                                     Port<TS<Bool>> pick_rhs,
                                     Port<TS<Int>> lhs,
                                     Port<TS<Int>> rhs)
        {
            return nested_<PlainInputSubGraph>(w, wire<RefSelector>(w, pick_rhs, lhs, rhs));
        }
    };

    struct NestedRefOutputGraph
    {
        static constexpr auto name = "nested_ref_output_graph";
        static Port<TS<Int>> compose(Wiring &w,
                                     Port<TS<Bool>> pick_rhs,
                                     Port<TS<Int>> lhs,
                                     Port<TS<Int>> rhs)
        {
            auto ref = nested_<RefOutputSubGraph>(w, pick_rhs, lhs, rhs);
            return wire<IntIdentity>(w, ref);
        }
    };

    struct NestedRefPassThroughGraph
    {
        static constexpr auto name = "nested_ref_pass_through_graph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in)
        {
            auto ref = nested_<RefPassThroughSubGraph>(w, in);
            return wire<IntIdentity>(w, ref);
        }
    };

    struct NestedStructuralRefInputGraph
    {
        static constexpr auto name = "nested_structural_ref_input_graph";
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            return nested_<StructuralRefInputSubGraph>(w, {lhs, rhs});
        }
    };

    struct NestedStructuralPairGraph
    {
        static constexpr auto name = "nested_structural_pair_graph";

        static Port<StructuralPair> compose(
            Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            return nested_<StructuralPairSubGraph>(w, lhs, rhs);
        }
    };

    struct NestedStructuralListGraph
    {
        static constexpr auto name = "nested_structural_list_graph";

        static Port<IntPair> compose(
            Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            return nested_<StructuralListSubGraph>(w, lhs, rhs);
        }
    };

    struct NestedDynamicListPassThroughGraph
    {
        static constexpr auto name = "nested_dynamic_list_pass_through_graph";

        static Port<TSL<TS<Int>>> compose(
            Wiring &w, Port<TSL<TS<Int>>> values)
        {
            return nested_<DynamicListPassThroughSubGraph>(w, values);
        }
    };

    struct NestedStructuralPairUnbindGraph
    {
        static constexpr auto name = "nested_structural_pair_unbind_graph";

        static Port<StructuralPair> compose(
            Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs,
            Port<TS<Int>> choice, Port<TS<Int>> trigger)
        {
            auto value = stdlib::to_tsb<StructuralPair>(w, lhs, rhs);
            auto empty = wire<EmptyStructuralPairReferenceSource>(w);
            auto choices = stdlib::to_tsl<TSL<StructuralPair, 2>>(
                w, value, empty);
            auto selected = wire<stdlib::getitem_>(w, choices, choice)
                                .as<StructuralPair>();
            return nested_<StructuralPairPassThroughSubGraph>(
                w, selected, trigger);
        }
    };

    struct NestedTslGraph
    {
        static constexpr auto name = "nested_tsl_graph";
        static void           compose(Wiring &w)
        {
            auto a = wire<stdlib::replay_impl, TS<Int>>(w, Str{"a"});
            auto b = wire<stdlib::replay_impl, TS<Int>>(w, Str{"b"});
            nested_<TslRecordSubGraph>(w, {a, b});
        }
    };

    struct NestedAllocationGraph
    {
        static constexpr auto name = "nested_allocation_graph";

        static void compose(Wiring &w)
        {
            auto scalar = wire<stdlib::const_, TS<Int>>(w, Int{3});
            static_cast<void>(nested_<AddOneSubGraph>(w, scalar));

            auto dict = wire<stdlib::const_, TSD<Str, TS<Int>>>(
                w, stdlib::make_map<Str, Int>({{Str{"a"}, Int{1}}, {Str{"b"}, Int{2}}, {Str{"c"}, Int{3}}}));
            static_cast<void>(wire<stdlib::map_>(w, fn<AddOneSubGraph>(), dict));
            static_cast<void>(wire<stdlib::mesh_>(w, fn<AddOneSubGraph>(), dict));
            static_cast<void>(wire<stdlib::reduce_>(w, fn<stdlib::add_>(), dict));

            auto ordered_dict = wire<stdlib::const_, TSD<Int, TS<Int>>>(
                w, stdlib::make_map<Int, Int>({{Int{0}, Int{1}}, {Int{1}, Int{2}}, {Int{2}, Int{3}}}));
            auto ordered_zero = wire<stdlib::const_, TS<Int>>(w, Int{0});
            static_cast<void>(wire<stdlib::reduce_>(
                w, fn<stdlib::add_>(), ordered_dict, ordered_zero, Bool{false}));

            auto key = wire<stdlib::const_, TS<Str>>(w, Str{"double"});
            static_cast<void>(wire<stdlib::switch_>(
                w, key,
                stdlib::switch_cases({{Value{Str{"double"}}, fn<AddOneSubGraph>()}}),
                scalar));
        }
    };

    template <typename Graph, typename Seed>
    GraphExecutorValue run_seeded(Seed seed)
    {
        GraphBuilder gb = build_graph<Graph>();
        seed(gb.global_state());
        return run_graph(std::move(gb), MIN_ST, MIN_ST + TimeDelta{10});
    }
}  // namespace

TEST_CASE("nested wiring: nested_<G> binds an outer input into the child graph and forwards its output")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<NestedAddOneGraph>(values<Int>(1, 2, 3)), values<Int>(2, 3, 4));
}

TEST_CASE("nested wiring: compiled structural results keep logical and terminal schemas distinct")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    const auto *bundle_schema = schema_descriptor<StructuralPair>::ts_meta();
    const CompiledSubGraph bundle = compile_subgraph<StructuralPairSubGraph>();
    CHECK(bundle.output_schema == bundle_schema);
    CHECK(bundle.terminal_output_schema ==
          TypeRegistry::instance().ref(bundle_schema));
    CHECK(bundle.terminal_is_structural_adapter);

    const auto *list_schema = schema_descriptor<IntPair>::ts_meta();
    const CompiledSubGraph list = compile_subgraph<StructuralListSubGraph>();
    CHECK(list.output_schema == list_schema);
    CHECK(list.terminal_output_schema == TypeRegistry::instance().ref(list_schema));
    CHECK(list.terminal_is_structural_adapter);

    const auto *plain_schema = schema_descriptor<TS<Int>>::ts_meta();
    const CompiledSubGraph dereferenced_ref =
        compile_subgraph<DereferencedRefOutputSubGraph>();
    CHECK(dereferenced_ref.output_schema == plain_schema);
    CHECK(dereferenced_ref.terminal_output_schema ==
          TypeRegistry::instance().ref(plain_schema));
    CHECK_FALSE(dereferenced_ref.terminal_is_structural_adapter);
}

TEST_CASE("nested wiring: newly composed fixed structural results retain their public schemas")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(
        (eval_node<NestedStructuralPairGraph>(
            values<Int>(1, 2), values<Int>(10, 20))),
        values<Value>(tsb_delta<StructuralPair>(Int{1}, Int{10}),
                      tsb_delta<StructuralPair>(Int{2}, Int{20})));
    CHECK_OUTPUT(
        (eval_node<NestedStructuralListGraph>(
            values<Int>(1, 2), values<Int>(10, 20))),
        values<Value>(list_delta<TS<Int>>({1, 10}),
                      list_delta<TS<Int>>({2, 20})));
}

TEST_CASE("nested wiring: a dynamic TSL pass-through binds before the producer grows the list")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    const auto input = values<Value>(
        list_delta<TS<Int>>({}),
        list_delta<TS<Int>>({{0, 1}, {1, 2}}),
        list_delta<TS<Int>>({{1, 3}}));
    CHECK_OUTPUT(
        eval_node<NestedDynamicListPassThroughGraph>(input),
        values<Value>(none,
                      list_delta<TS<Int>>({{0, 1}, {1, 2}}),
                      list_delta<TS<Int>>({{1, 3}})));
}

TEST_CASE("nested wiring: structural pass-through leaves clear while their source is unbound")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(
        (eval_node<NestedStructuralPairUnbindGraph>(
            values<Int>(1, none, 2, none),
            values<Int>(10, none, 20, none),
            values<Int>(0, 1, none, 0),
            values<Int>(0, 1, 2, 3))),
        values<Value>(
            tsb_delta<StructuralPair>(Int{1}, Int{10}),
            none,
            none,
            tsb_delta<StructuralPair>(Int{2}, Int{20})));
}

TEST_CASE("nested wiring: every dynamic child graph uses planned or stable in-place storage")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphExecutorValue executor = run_graph(build_graph<NestedAllocationGraph>());
    auto graph = executor.view().graph();

    bool saw_nested = false;
    bool saw_map    = false;
    bool saw_mesh   = false;
    bool saw_reduce = false;
    bool saw_ordered_reduce = false;
    bool saw_switch = false;
    for (std::size_t index = 0; index < graph.node_count(); ++index)
    {
        auto node = graph.node_at(index);
        if (node.is<SingleNestedGraphNodeView>())
        {
            auto nested = node.as<SingleNestedGraphNodeView>();
            REQUIRE(nested.child_graph_value().has_value());
            CHECK(nested.child_graph_value().uses_external_storage());
            saw_nested = true;
        }
        else if (node.is<MapNodeView>())
        {
            auto map = node.as<MapNodeView>();
            REQUIRE(map.child_graph_count() > 0);
            CHECK(map.child_graphs_use_in_place_storage());
            saw_map = true;
        }
        else if (node.is<MeshNodeView>())
        {
            auto mesh = node.as<MeshNodeView>();
            REQUIRE(mesh.child_graph_count() > 0);
            CHECK(mesh.child_graphs_use_in_place_storage());
            saw_mesh = true;
        }
        else if (node.is<ReduceNodeView>())
        {
            auto reduce = node.as<ReduceNodeView>();
            REQUIRE(reduce.combiner_count() > 0);
            CHECK(reduce.child_graphs_use_in_place_storage());
            saw_reduce = true;
        }
        else if (node.is<OrderedReduceNodeView>())
        {
            auto reduce = node.as<OrderedReduceNodeView>();
            REQUIRE(reduce.child_graph_count() > 0);
            CHECK(reduce.child_graphs_use_in_place_storage());
            saw_ordered_reduce = true;
        }
        else if (node.is<SwitchNodeView>())
        {
            auto switch_view = node.as<SwitchNodeView>();
            REQUIRE(switch_view.stored_graph_count() > 0);
            CHECK(switch_view.child_graphs_use_in_place_storage());
            saw_switch = true;
        }
    }

    CHECK(saw_nested);
    CHECK(saw_map);
    CHECK(saw_mesh);
    CHECK(saw_reduce);
    CHECK(saw_ordered_reduce);
    CHECK(saw_switch);
}

TEST_CASE("nested wiring: node builders expose every immediate compiled child through one contract")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphBuilder graph = build_graph<NestedAllocationGraph>();
    struct Inspection
    {
        std::size_t children{0};
        std::size_t owners{0};
        bool all_children_non_empty{true};
    } inspection;

    const auto visitor = [](void *raw, const GraphBuilder &child) {
        auto &state = *static_cast<Inspection *>(raw);
        ++state.children;
        state.all_children_non_empty = state.all_children_non_empty && child.node_count() > 0;
    };

    for (const NodeBuilder &node : graph.nodes())
    {
        const std::size_t before = inspection.children;
        node.visit_child_graphs(&inspection, visitor);
        if (inspection.children != before) { ++inspection.owners; }
    }

    // nested_, map_, mesh_, associative reduce, ordered reduce and switch_
    // each own one immediate compiled child in this graph. Ordinary nodes use
    // the same contract and visit nothing.
    CHECK(inspection.owners == 6);
    CHECK(inspection.children == 6);
    CHECK(inspection.all_children_non_empty);
}

TEST_CASE("nested wiring: a sub-graph with multiple boundary inputs binds each arg")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<NestedSumGraph>(values<Int>(1, 2, 3), values<Int>(10, 20, 30)),
                 values<Int>(11, 22, 33));
    CHECK_OUTPUT(eval_node<NestedSumGraph>(arg<"a">(values<Int>(1, 2, 3)),
                                           arg<"b">(values<Int>(10, 20, 30))),
                 values<Int>(11, 22, 33));
}

TEST_CASE("nested wiring: one boundary arg feeds multiple child input endpoints")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<NestedFanOutGraph>(values<Int>(1, 2, 3)), values<Int>(2, 4, 6));
}

TEST_CASE("nested wiring: a source-only sub-graph drives the outer graph via schedule propagation")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // The child replay reads its buffer through the root GlobalState (nested
    // graphs delegate state to the root) and re-schedules itself each cycle;
    // the nested node's schedule propagation must keep the outer graph running
    // with no outer source at all.
    auto ex = run_seeded<NestedSourceOnlyGraph>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "src", values<Int>(5, 6, 7));
    });
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "out"), values<Int>(5, 6, 7));
}

TEST_CASE("nested wiring: scalar parameters configure the child graph and partition interning")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    auto ex = run_seeded<NestedScaledGraph>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "in", values<Int>(1, 2, 3));
    });
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "doubled"), values<Int>(2, 4, 6));
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "tripled"), values<Int>(3, 6, 9));
}

TEST_CASE("nested wiring: graph eval_node accepts keyword wrappers around inputs and scalars")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ScaledSubGraph>(arg<"in">(values<Int>(1, 2, 3)), arg<"factor">(Int{4})),
                 values<Int>(4, 8, 12));
    CHECK_OUTPUT(eval_node<ScaledSubGraph>(arg<"factor">(Int{4}), arg<"in">(values<Int>(1, 2, 3))),
                 values<Int>(4, 8, 12));
}

TEST_CASE("nested wiring: a sink sub-graph wires as a nested sink node")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    auto ex = run_seeded<NestedRecordGraph>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "in", values<Int>(1, 2, 3));
    });
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "out"), values<Int>(1, 2, 3));
}

TEST_CASE("nested wiring: a pass-through sub-graph aliases the outer input (alias_parent_input)")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<NestedPassThroughGraph>(values<Int>(1, 2, 3)), values<Int>(1, 2, 3));
}

TEST_CASE("nested wiring: a plain outer source adapts to a child REF input")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<NestedRefInputGraph>(values<Int>(1, 2, 3)), values<Int>(1, 2, 3));
}

TEST_CASE("nested wiring: a retargeting REF source adapts to a child plain input")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<NestedPlainInputFromRefGraph>(values<Bool>(false, none, true, none, false),
                                                         values<Int>(1, 2, none, 4, none),
                                                         values<Int>(10, none, 30, none, 50)),
                 values<Int>(1, 2, 30, none, 4));
}

TEST_CASE("nested wiring: a child REF output retargets an outer plain consumer")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<NestedRefOutputGraph>(values<Bool>(false, none, true, none, false),
                                                  values<Int>(1, 2, none, 4, none),
                                                  values<Int>(10, none, 30, none, 50)),
                 values<Int>(1, 2, 30, none, 4));
}

TEST_CASE("nested wiring: a REF pass-through samples and follows the plain parent source")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<NestedRefPassThroughGraph>(values<Int>(1, 2, 3)), values<Int>(1, 2, 3));
}

TEST_CASE("nested wiring: a fixed structural source adapts to a child REF input")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<NestedStructuralRefInputGraph>(values<Int>(1, 2, 3),
                                                          values<Int>(10, 20, 30)),
                 values<Int>(11, 22, 33));
}

TEST_CASE("nested wiring: a structural initializer boundary binds leaf-wise into the child")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    auto ex = run_seeded<NestedTslGraph>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "a", values<Int>(1, 2));
        set_replay_values<Int>(gs, "b", values<Int>(10, 20));
    });
    const auto recorded = get_recorded_deltas(ex.view().graph().global_state(), "out");
    REQUIRE(recorded.size() == 2);
    REQUIRE(recorded[0].has_value());
    REQUIRE(recorded[1].has_value());
    CHECK(recorded[0]->equals(list_delta<TS<Int>>({1, 10})));
    CHECK(recorded[1]->equals(list_delta<TS<Int>>({2, 20})));
}

TEST_CASE("nested wiring: a sub-graph compose must not seed GlobalState")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    Wiring w;
    auto   in = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
    CHECK_THROWS_AS((void)nested_<GlobalStateSeedingSubGraph>(w, in), std::invalid_argument);
}
