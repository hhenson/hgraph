// Graph wiring (slice 1): author a graph as a struct with a static wire(Wiring&)
// body, compose nodes with wire<T>(w, ports...), and build it with
// build_graph<G>() — no node indices or edges by hand. See docs: Graph Wiring.

#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <tuple>
#include <utility>
#include <vector>

namespace
{
    using namespace hgraph;

    struct OutputOnlyGraphEdgeLayout
    {
        std::size_t source_node{0};
        std::vector<std::size_t> source_path{};
        std::size_t target_node{0};
        std::vector<std::size_t> target_path{};
    };

    template <typename TSchema>
    [[nodiscard]] const TSOutputView &erased_output(const Out<TSchema> &out)
    {
        if constexpr (std::is_base_of_v<TSOutputView, Out<TSchema>>)
        {
            return out;
        }
        else
        {
            return out.base();
        }
    }

    struct ConstantSource
    {
        static constexpr auto name              = "source";
        static constexpr bool schedule_on_start = true;
        static void           eval(Out<TS<Int>> out) { out.set(Int{41}); }
    };

    struct AddOne
    {
        static constexpr auto name = "add_one";
        static void           eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value() + 1); }
    };

    struct PythonValueSource
    {
        static constexpr auto name = "python_value_source";
        static constexpr bool uses_python_values = true;
        static void eval(Out<TS<Int>> out) { out.set(Int{1}); }
    };

    struct PythonValueSink
    {
        static constexpr auto name = "python_value_sink";
        static constexpr bool uses_python_values = true;
        static void eval(In<"in", TS<Int>> in) { static_cast<void>(in); }
    };

    struct NativeValueSink
    {
        static constexpr auto name = "native_value_sink";
        static void eval(In<"in", TS<Int>> in) { static_cast<void>(in); }
    };

    struct PythonOnlyStorageGraph
    {
        static void compose(Wiring &w)
        {
            wire<PythonValueSink>(w, wire<PythonValueSource>(w));
        }
    };

    struct NativeCachedStorageGraph
    {
        static void compose(Wiring &w)
        {
            wire<PythonValueSink>(w, wire<ConstantSource>(w));
        }
    };

    struct MixedStorageGraph
    {
        static void compose(Wiring &w)
        {
            auto source = wire<PythonValueSource>(w);
            wire<PythonValueSink>(w, source);
            wire<NativeValueSink>(w, source);
        }
    };

    struct PythonTerminalSubGraph
    {
        static Port<TS<Int>> compose(Wiring &w)
        {
            return wire<PythonValueSource>(w);
        }
    };

    // source -> add_one, wired declaratively.
    struct AddOneGraph
    {
        static constexpr auto name = "add_one_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<AddOne>(w, source);
        }
    };

    // A sub-graph: TS<Int> -> TS<Int>, adding two via two add_one nodes.
    struct PlusTwo
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> x)
        {
            return wire<AddOne>(w, wire<AddOne>(w, x));
        }
    };

    // Top-level: source(41) -> PlusTwo -> 43. The sub-graph flattens into the parent.
    struct PlusTwoGraph
    {
        static constexpr auto name = "plus_two_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<PlusTwo>(w, source);
        }
    };

    // Two-input compute node (exercises compile-time per-port schema matching).
    struct Sum
    {
        static constexpr auto name = "sum";
        static void           eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    struct SumGraph
    {
        static constexpr auto name = "sum_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);   // interns to one node
            wire<Sum>(w, source, source);            // 41 + 41
        }
    };

    // Source configured by a scalar argument (no TS inputs -> PullSource).
    struct ScaledSource
    {
        static constexpr auto name              = "scaled_source";
        static constexpr bool schedule_on_start = true;
        static void           eval(Scalar<"value", Int> value, Out<TS<Int>> out) { out.set(value.value()); }
    };

    struct ScaledSourceGraph
    {
        static constexpr auto name = "scaled_source_graph";
        static void           compose(Wiring &w) { wire<ScaledSource>(w, Int{7}); }
    };

    // Compute node mixing a TS input port with a scalar argument; wire args are
    // given in eval-parameter order: the port, then the scalar.
    struct Shift
    {
        static constexpr auto name = "shift";
        static void           eval(In<"in", TS<Int>> in, Scalar<"delta", Int> delta, Out<TS<Int>> out)
        {
            out.set(in.value() + delta.value());
        }
    };

    struct DefaultShift
    {
        static constexpr auto name = "default_shift";
        static auto           defaults() { return std::tuple{arg<"delta">(Int{5})}; }
        static void           eval(In<"in", TS<Int>> in, Scalar<"delta", Int> delta, Out<TS<Int>> out)
        {
            out.set(in.value() + delta.value());
        }
    };

    struct ShiftGraph
    {
        static constexpr auto name = "shift_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);   // 41
            wire<Shift>(w, source, Int{5});          // 41 + 5 = 46
        }
    };

    // Top-level graph that takes a Scalar parameter, threaded into a node scalar.
    struct ConfiguredSourceGraph
    {
        static constexpr auto name = "configured_source_graph";
        static void           compose(Wiring &w, Scalar<"value", Int> value)
        {
            wire<ScaledSource>(w, value.value());
        }
    };

    // Top-level graph whose scalar parameter offsets a constant source.
    struct OffsetGraph
    {
        static constexpr auto name = "offset_graph";
        static void           compose(Wiring &w, Scalar<"offset", Int> offset)
        {
            auto source = wire<ConstantSource>(w);    // 41
            wire<Shift>(w, source, offset.value());   // 41 + offset
        }
    };

    struct ConfiguredPairSourceGraph
    {
        static constexpr auto name = "configured_pair_source_graph";
        static void           compose(Wiring &w, Scalar<"lhs", Int> lhs, Scalar<"rhs", Int> rhs)
        {
            wire<ScaledSource>(w, lhs.value() * Int{10} + rhs.value());
        }
    };

    struct DefaultConfiguredSourceGraph
    {
        static constexpr auto name = "default_configured_source_graph";
        static auto           defaults() { return std::tuple{arg<"value">(Int{12})}; }
        static void           compose(Wiring &w, Scalar<"value", Int> value)
        {
            wire<ScaledSource>(w, value.value());
        }
    };

    // Sub-graph with a port input AND a scalar parameter: (TS<Int>, by) -> TS<Int>.
    // The received Scalar<"by", Int> is forwarded straight to wire<Shift> (whose
    // scalar parameter is Scalar<"delta", Int>) — no .value() needed; the wiring
    // layer unpacks the Scalar and re-applies it (names need not match).
    struct ShiftBy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"x", TS<Int>> x, Scalar<"by", Int> by)
        {
            return wire<Shift>(w, x, by);
        }
    };

    // Top-level: source(41) -> ShiftBy(by=5) -> 46. The literal 5 is auto-wrapped
    // into the sub-graph's Scalar<"by", Int> parameter, and ShiftBy flattens.
    struct ShiftBySubGraph
    {
        static constexpr auto name = "shift_by_sub_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<ShiftBy>(w, source, Int{5});
        }
    };

    struct NamedNodeWireGraph
    {
        static constexpr auto name = "named_node_wire_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<Shift>(w, arg<"delta">(Int{5}), arg<"in">(source));
        }
    };

    struct DefaultNodeWireGraph
    {
        static constexpr auto name = "default_node_wire_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<DefaultShift>(w, source);
        }
    };

    struct NamedSubGraphWireGraph
    {
        static constexpr auto name = "named_sub_graph_wire_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<ShiftBy>(w, arg<"by">(Int{5}), arg<"x">(source));
        }
    };

    struct AddOneSubGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> x)
        {
            return wire<AddOne>(w, x);
        }
    };

    struct GenericSourceIntoTypedSubGraph
    {
        static constexpr auto name = "generic_source_into_typed_sub_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<stdlib::const_>(w, Int{41});  // erased Port<void>, resolved to TS<Int>
            wire<AddOneSubGraph>(w, source);
        }
    };

    struct CountSignal
    {
        static constexpr auto name = "count_signal";
        static void           eval(In<"pulse", SIGNAL> pulse, State<Int> count, Out<TS<Int>> out)
        {
            if (pulse.ticked())
            {
                const Int next = count.get() + 1;
                count.set(next);
                out.set(next);
            }
        }
    };

    struct CountSignalSubGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<SIGNAL> pulse)
        {
            return wire<CountSignal>(w, pulse);
        }
    };

    struct SignalSubGraphFromTsPort
    {
        static constexpr auto name = "signal_sub_graph_from_ts_port";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<CountSignalSubGraph>(w, source);
        }
    };

    struct RefProbe
    {
        static constexpr auto name              = "ref_probe";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ref", REF<TS<Int>>> ref, Out<TS<Bool>> out)
        {
            const TimeSeriesReference value = ref.value();
            out.set(value.is_peered() &&
                    time_series_schema_equivalent(value.target_schema(), ts_type<TS<Int>>()));
        }
    };

    struct StructuralListRefProbe
    {
        static constexpr auto name              = "structural_list_ref_probe";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ref", REF<TSL<TS<Int>, 2>>> ref, Out<TS<Bool>> out)
        {
            const TimeSeriesReference value = ref.value();
            out.set(value.is_non_peered() &&
                    time_series_schema_equivalent(value.target_schema(), ts_type<TSL<TS<Int>, 2>>()) &&
                    value.items().size() == 2 &&
                    value[0].is_peered() &&
                    value[1].is_peered());
        }
    };

    struct RefCopy
    {
        static constexpr auto name              = "ref_copy";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ref", REF<TS<Int>>> ref, Out<REF<TS<Int>>> out)
        {
            out.set(ref.value());
        }
    };

    struct RefDeref
    {
        static constexpr auto name              = "ref_deref";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out)
        {
            out.set(value.value());
        }
    };

    struct RefProbeGraph
    {
        static constexpr auto name = "ref_probe_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<RefProbe>(w, source);
        }
    };

    struct StructuralListRefProbeGraph
    {
        static constexpr auto name = "structural_list_ref_probe_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            auto list   = stdlib::to_tsl<TSL<TS<Int>, 2>>(w, source, source);
            wire<StructuralListRefProbe>(w, list);
        }
    };

    struct RefRoundTripGraph
    {
        static constexpr auto name = "ref_round_trip_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            auto ref    = wire<RefCopy>(w, source);
            wire<RefDeref>(w, ref);
        }
    };

    template <typename TSchema>
    struct RefCopyFor
    {
        static constexpr auto name              = "ref_copy_for";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ref", REF<TSchema>> ref, Out<REF<TSchema>> out)
        {
            out.set(ref.value());
        }
    };

    template <typename TSchema>
    struct RefDerefPass
    {
        static constexpr auto name              = "ref_deref_pass";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"value", TSchema> value, Out<TSchema> out)
        {
            Value delta = capture_delta(value.base());
            apply_delta(erased_output(out), delta.view());
        }
    };

    template <typename TSource, typename TSchema>
    struct RefRoundTripFor
    {
        static constexpr auto name = "ref_round_trip_for";

        static void compose(Wiring &w)
        {
            auto source = wire<TSource>(w);
            auto ref    = wire<RefCopyFor<TSchema>>(w, source);
            wire<RefDerefPass<TSchema>>(w, ref);
        }
    };

    struct SetSource
    {
        static constexpr auto name              = "set_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TSS<Int>> out)
        {
            out.add(Int{1});
            out.add(Int{2});
        }
    };

    struct DictSource
    {
        static constexpr auto name              = "dict_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TSD<Int, TS<Int>>> out)
        {
            out.set(Int{1}, Int{10});
            out.set(Int{2}, Int{20});
        }
    };

    struct ListSource
    {
        static constexpr auto name              = "list_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TSL<TS<Int>, 2>> out)
        {
            out.set(0, Int{7});
            out.set(1, Int{8});
        }
    };

    using RefRoundTripBundle = TSB<"RefRoundTripBundle",
                                   Field<"a", TS<Int>>,
                                   Field<"b", TS<Int>>>;

    struct BraceListInputSum
    {
        static constexpr auto name = "brace_list_input_sum";

        static void eval(In<"tsl", TSL<TS<Int>, 2>> tsl, Out<TS<Int>> out)
        {
            if (tsl[0].valid() && tsl[1].valid()) { out.set(tsl[0].value() + tsl[1].value()); }
        }
    };

    struct BraceListInputGraph
    {
        static constexpr auto name = "brace_list_input_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<BraceListInputSum>(w, {source, source});
        }
    };

    struct BraceBundleInputSum
    {
        static constexpr auto name = "brace_bundle_input_sum";

        static void eval(In<"tsb", RefRoundTripBundle> tsb, Out<TS<Int>> out)
        {
            auto a = tsb.field<"a">();
            auto b = tsb.field<"b">();
            if (a.valid() && b.valid()) { out.set(a.value() + b.value()); }
        }
    };

    struct BraceBundleInputGraph
    {
        static constexpr auto name = "brace_bundle_input_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<BraceBundleInputSum>(w, {source, source});
        }
    };

    struct BraceBundleNamedInputGraph
    {
        static constexpr auto name = "brace_bundle_named_input_graph";

        static void compose(Wiring &w)
        {
            auto a = wire<ScaledSource>(w, Int{3});
            auto b = wire<ScaledSource>(w, Int{40});
            wire<BraceBundleInputSum>(w, {{"b", b}, {"a", a}});
        }
    };

    struct BraceBundlePartialInputProbe
    {
        static constexpr auto name = "brace_bundle_partial_input_probe";

        static void eval(In<"tsb", RefRoundTripBundle> tsb, Out<TS<Int>> out)
        {
            auto a = tsb.field<"a">();
            auto b = tsb.field<"b">();
            if (a.valid() && !b.valid()) { out.set(a.value()); }
        }
    };

    struct BraceBundlePartialInputGraph
    {
        static constexpr auto name = "brace_bundle_partial_input_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<BraceBundlePartialInputProbe>(w, {{"a", source}});
        }
    };

    struct PackedTslTailSum
    {
        static constexpr auto name = "packed_tsl_tail_sum";

        static void eval(In<"base", TS<Int>> base,
                         In<"values", Args<>> values,
                         Scalar<"offset", Int> offset,
                         Out<TS<Int>> out)
        {
            Int total = base.value() + offset.value();
            for (std::size_t i = 0; i < values.size(); ++i)
            {
                auto child = values[i];
                if (child.valid()) { total += child.value().checked_as<Int>(); }
            }
            out.set(total);
        }
    };

    struct PackedTslTailGraph
    {
        [[maybe_unused]] static constexpr auto name = "packed_tsl_tail_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> base, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            return wire<PackedTslTailSum>(w, base, lhs, rhs, arg<"offset">(Int{5})).template as<TS<Int>>();
        }
    };

    struct PackedKwargsSum
    {
        static constexpr auto name = "packed_kwargs_sum";

        static void eval(In<"values", Kwargs<>> values, Out<TS<Int>> out)
        {
            auto lhs = values.field("lhs");
            auto rhs = values.field("rhs");
            out.set(lhs.value().checked_as<Int>() + rhs.value().checked_as<Int>());
        }
    };

    struct PackedKwargsGraph
    {
        [[maybe_unused]] static constexpr auto name = "packed_kwargs_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            return wire<PackedKwargsSum>(w, arg<"lhs">(lhs), arg<"rhs">(rhs)).template as<TS<Int>>();
        }
    };

    struct PackedNumberedFormat
    {
        static constexpr auto name = "packed_numbered_format";

        static void eval(In<"values", Kwargs<>> values, Out<TS<Str>> out)
        {
            auto count = values.field("_1");
            auto label = values.field("_2");
            out.set(Str{std::to_string(count.value().checked_as<Int>()) + ":" +
                        label.value().checked_as<Str>()});
        }
    };

    struct PackedNumberedGraph
    {
        [[maybe_unused]] static constexpr auto name = "packed_numbered_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> count, Port<TS<Str>> label)
        {
            return wire<PackedNumberedFormat>(w, count, label).template as<TS<Str>>();
        }
    };

    struct PackedGenericTsbSum
    {
        static constexpr auto name = "packed_generic_tsb_sum";

        static void eval(In<"values", Kwargs<>> values, Out<TS<Int>> out)
        {
            auto lhs = values.field("lhs");
            auto rhs = values.field("rhs");
            out.set(lhs.value().checked_as<Int>() + rhs.value().checked_as<Int>());
        }
    };

    struct PackedGenericTsbGraph
    {
        [[maybe_unused]] static constexpr auto name = "packed_generic_tsb_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            return wire<PackedGenericTsbSum>(w, arg<"lhs">(lhs), arg<"rhs">(rhs)).template as<TS<Int>>();
        }
    };

    static_assert(graph_wiring_detail::input_pack_kind<In<"values", Args<>>>() ==
                  graph_wiring_detail::node_collection_pack_kind::tsl);
    static_assert(graph_wiring_detail::input_pack_kind<In<"values", Args<TS<Int>>>>() ==
                  graph_wiring_detail::node_collection_pack_kind::tsl);
    static_assert(graph_wiring_detail::input_pack_kind<In<"values", Kwargs<>>>() ==
                  graph_wiring_detail::node_collection_pack_kind::tsb);
    static_assert(graph_wiring_detail::input_pack_kind<In<"values", TSL<TS<Int>, SIZE<"N">>>>() ==
                  graph_wiring_detail::node_collection_pack_kind::none);
    static_assert(graph_wiring_detail::input_pack_kind<
                      In<"values", UnNamedTSB<Field<"lhs", TS<Int>>, Field<"rhs", TS<Int>>>>>() ==
                  graph_wiring_detail::node_collection_pack_kind::none);

    using RecordedStateBundle = TSB<"RecordedStateBundle", Field<"last", TS<Int>>>;

    struct RecordablePreviousValue
    {
        static constexpr auto name = "recordable_previous_value";

        static void eval(In<"in", TS<Int>> in,
                         RecordableState<RecordedStateBundle> state,
                         Out<TS<Int>> out)
        {
            auto      last     = state.field<"last">();
            const Int previous = last.valid() ? last.value().checked_as<Int>() : Int{-1};
            out.set(previous);
            last.set(in.value());
        }
    };

    struct RecordedStateLast
    {
        static constexpr auto name = "recorded_state_last";

        static void eval(In<"state", RecordedStateBundle> state, Out<TS<Int>> out)
        {
            auto last = state.field<"last">();
            if (last.valid()) { out.set(last.value()); }
        }
    };

    struct RecordableStatePortGraph
    {
        static constexpr auto name = "recordable_state_port_graph";

        static void compose(Wiring &w)
        {
            auto source   = wire<ConstantSource>(w);
            auto previous = wire<RecordablePreviousValue>(w, source);
            wire<RecordedStateLast>(w, recordable_state(previous));
        }
    };

    struct RecordableStatePassthrough
    {
        static constexpr auto name = "recordable_state_passthrough";

        static Port<RecordedStateBundle> compose(Wiring &, Port<RecordedStateBundle> state)
        {
            return state;
        }
    };

    struct NestedRecordableStatePortGraph
    {
        static constexpr auto name = "nested_recordable_state_port_graph";

        static void compose(Wiring &w)
        {
            auto source   = wire<ConstantSource>(w);
            auto previous = wire<RecordablePreviousValue>(w, source);
            auto state = nested_<RecordableStatePassthrough>(w, recordable_state(previous));
            wire<RecordedStateLast>(w, state);
        }
    };

    struct StructuralBundleRefProbe
    {
        static constexpr auto name              = "structural_bundle_ref_probe";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ref", REF<RefRoundTripBundle>> ref, Out<TS<Bool>> out)
        {
            const TimeSeriesReference value = ref.value();
            out.set(value.is_non_peered() &&
                    time_series_schema_equivalent(value.target_schema(), ts_type<RefRoundTripBundle>()) &&
                    value.items().size() == 2 &&
                    value[0].is_peered() &&
                    value[1].is_peered());
        }
    };

    struct BundleSource
    {
        static constexpr auto name              = "bundle_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<RefRoundTripBundle> out)
        {
            out.field<"a">().set(Int{11});
            out.field<"b">().set(Int{12});
        }
    };

    struct StructuralBundleRefProbeGraph
    {
        static constexpr auto name = "structural_bundle_ref_probe_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            auto bundle = stdlib::to_tsb<RefRoundTripBundle>(w, source, source);
            wire<StructuralBundleRefProbe>(w, bundle);
        }
    };

    struct NestedListSetSource
    {
        static constexpr auto name              = "nested_list_set_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TSL<TSS<Int>, 2>> out)
        {
            out[0].add(Int{3});
            out[0].add(Int{4});
            out[1].add(Int{5});
        }
    };

    NodeBuilder static_node_builder_for_add_one()
    {
        NodeBuilder builder;
        builder.implementation<AddOne>();
        return builder;
    }

    NodeBuilder static_node_builder_for_constant_source()
    {
        NodeBuilder builder;
        builder.implementation<ConstantSource>();
        return builder;
    }

    NodeBuilder nested_add_one_builder()
    {
        const auto *ts_int = ts_type<TS<Int>>();

        GraphBuilder child;
        child.label("nested_add_one_child").add_node(static_node_builder_for_add_one());

        NodeTypeMetaData meta;
        meta.display_name = "nested_add_one";
        meta.input_schema = TypeRegistry::instance().un_named_tsb({{"in", ts_int}});
        meta.output_schema = ts_int;

        SingleNestedGraphNodeSpec spec;
        spec.graph_builder = std::move(child);
        spec.input_bindings.push_back(NestedGraphInputBinding{
            .source_path = {0},
            .target = NestedGraphEndpoint{.node = 0, .path = {0}},
        });
        spec.output_binding = NestedGraphOutputBinding{.source = NestedGraphEndpoint{.node = 0}};
        return single_nested_graph_node(std::move(meta), std::move(spec));
    }

    NodeBuilder nested_constant_builder()
    {
        const auto *ts_int = ts_type<TS<Int>>();

        GraphBuilder child;
        child.label("nested_constant_child").add_node(static_node_builder_for_constant_source());

        NodeTypeMetaData meta;
        meta.display_name  = "nested_constant";
        meta.output_schema = ts_int;

        SingleNestedGraphNodeSpec spec;
        spec.graph_builder = std::move(child);
        spec.output_binding = NestedGraphOutputBinding{.source = NestedGraphEndpoint{.node = 0}};
        return single_nested_graph_node(std::move(meta), std::move(spec));
    }

    void write_int(const NodeView &view, DateTime evaluation_time, Int value)
    {
        Value wrapped{value};
        auto  mutation = view.output(evaluation_time).begin_mutation(evaluation_time);
        REQUIRE(mutation.copy_value_from(wrapped.view()));
    }

    struct ErrorOutputSourceTag
    {
    };

    NodeBuilder error_output_source_builder()
    {
        const auto *ts_int = ts_type<TS<Int>>();

        NodeTypeMetaData meta;
        meta.display_name        = "error_output_source";
        meta.output_schema       = ts_int;
        meta.error_output_schema = ts_int;
        meta.node_kind           = NodeKind::PullSource;
        meta.schedule_on_start   = true;

        NodeCallbacks callbacks;
        callbacks.evaluate = [](const NodeView &view, DateTime evaluation_time) {
            write_int(view, evaluation_time, Int{1});

            Value error_value{Int{99}};
            auto  mutation = view.error_output(evaluation_time).begin_mutation(evaluation_time);
            REQUIRE(mutation.copy_value_from(error_value.view()));
        };
        return NodeBuilder::native(std::move(meta), std::move(callbacks));
    }

    struct ErrorOutputPortGraph
    {
        static constexpr auto name = "error_output_port_graph";

        static void compose(Wiring &w)
        {
            WiringPortRef source_ref = w.add_node(std::type_index(typeid(ErrorOutputSourceTag)),
                                                  error_output_source_builder(),
                                                  std::span<const WiringPortRef>{},
                                                  Value{});
            Port<TS<Int>> source{w, std::move(source_ref)};
            wire<AddOne>(w, error_output(source));
        }
    };

    // A self-rescheduling source emitting 0, 1, ..., count-1 over successive cycles
    // (the callback-based analogue of TickingSource in test_simulation_execution).
    NodeBuilder ticking_int_source(const TSValueTypeMetaData *ts_int, int count)
    {
        NodeTypeMetaData meta;
        meta.display_name      = "ticking_source";
        meta.output_schema     = ts_int;
        meta.node_kind         = NodeKind::PullSource;
        meta.schedule_on_start = true;

        auto          emitted = std::make_shared<int>(0);
        NodeCallbacks callbacks;
        callbacks.evaluate = [emitted, count](const NodeView &view, DateTime evaluation_time) {
            const int n = *emitted;
            write_int(view, evaluation_time, Int{n});
            *emitted = n + 1;
            if (n + 1 < count && view.graph_value() != nullptr)
            {
                view.graph_value()->schedule_node(view.node_index(), evaluation_time + MIN_TD);
            }
        };
        return NodeBuilder::native(std::move(meta), std::move(callbacks));
    }

    // A pass-through compute node that records how many times it is evaluated.
    NodeBuilder counting_passthrough(const TSValueTypeMetaData *ts_int, std::shared_ptr<int> evals)
    {
        NodeTypeMetaData meta;
        meta.display_name  = "counting_passthrough";
        meta.input_schema  = TypeRegistry::instance().un_named_tsb({{"in", ts_int}});
        meta.output_schema = ts_int;
        meta.node_kind     = NodeKind::Compute;

        NodeCallbacks callbacks;
        callbacks.evaluate = [evals](const NodeView &view, DateTime evaluation_time) {
            ++*evals;
            auto root   = view.input(evaluation_time);
            auto bundle = root.as_bundle();
            auto in     = bundle[0];
            write_int(view, evaluation_time, in.value().checked_as<Int>());
        };
        return NodeBuilder::native(std::move(meta), std::move(callbacks));
    }

    // A sink (no output) used to exercise that identical sinks are not interned.
    struct DropInt
    {
        static constexpr auto name = "drop_int";
        static void           eval(In<"in", TS<Int>> in) { static_cast<void>(in); }
    };

    struct TwoSinksGraph
    {
        static constexpr auto name = "two_sinks_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<DropInt>(w, source);
            wire<DropInt>(w, source);   // identical sink: must stay a distinct node
        }
    };
}  // namespace

TEST_CASE("graph wiring: GraphEdge source kind is packed into source_node")
{
    using namespace hgraph;

    STATIC_REQUIRE(sizeof(GraphEdge) == sizeof(OutputOnlyGraphEdgeLayout));

    const GraphEdge ordinary{.source_node = 5};
    CHECK(graph_edge_source_node(ordinary.source_node) == 5);
    CHECK(graph_edge_source_kind(ordinary.source_node) == GraphEdgeSourceKind::Output);

    const std::size_t special = make_graph_edge_source(5, GraphEdgeSourceKind::RecordableState);
    CHECK(graph_edge_source_node(special) == 5);
    CHECK(graph_edge_source_kind(special) == GraphEdgeSourceKind::RecordableState);
}

TEST_CASE("graph wiring: build_graph wires source -> add_one and runs in simulation")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<AddOneGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);
    // The rank pass orders source (no inputs) before add_one, so node 1 is add_one.
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{42});
}

TEST_CASE("graph wiring: Python output storage is selected from complete readership")
{
    using namespace hgraph;

    SECTION("Python producer with only Python readers requests Python-only storage")
    {
        const GraphBuilder graph = build_graph<PythonOnlyStorageGraph>();
        REQUIRE(graph.node_count() == 2);
        CHECK(graph.nodes()[0].output_value_storage() ==
              ValueStorageVariant::PythonOnly);
    }

    SECTION("native producer read by Python keeps native storage and requests a cache")
    {
        const GraphBuilder graph = build_graph<NativeCachedStorageGraph>();
        REQUIRE(graph.node_count() == 2);
        CHECK(graph.nodes()[0].output_value_storage() ==
              ValueStorageVariant::NativeWithPythonCache);
    }

    SECTION("one native reader prevents Python-only storage")
    {
        const GraphBuilder graph = build_graph<MixedStorageGraph>();
        REQUIRE(graph.node_count() == 3);
        CHECK(graph.nodes()[0].output_value_storage() ==
              ValueStorageVariant::NativeWithPythonCache);
    }

    SECTION("a subgraph return is an escaping native boundary")
    {
        const CompiledSubGraph compiled =
            compile_subgraph<PythonTerminalSubGraph>();
        REQUIRE(compiled.graph_builder.node_count() == 1);
        CHECK(compiled.graph_builder.nodes()[0].output_value_storage() ==
              ValueStorageVariant::Native);
    }
}

TEST_CASE("graph wiring: identical nodes are interned to one")
{
    using namespace hgraph;

    Wiring w;
    auto   a = wire<ConstantSource>(w);
    auto   b = wire<ConstantSource>(w);

    CHECK(a.node() != nullptr);
    CHECK(a.node() == b.node());   // same interned wiring instance

    GraphBuilder            graph_builder = std::move(w).finish();
    testing::MockRootGraph  graph{graph_builder};
    CHECK(graph.graph().node_count() == 1);   // deduped to a single runtime node
}

TEST_CASE("graph wiring: push sources form the runtime prefix across implementations")
{
    using namespace hgraph;

    struct FirstPushSourceTag {};
    struct InterleavedSinkTag {};
    struct SecondPushSourceTag {};

    const auto *schema = ts_type<TS<Int>>();
    Wiring w;
    (void)w.add_unique_node(
        std::type_index(typeid(FirstPushSourceTag)),
        make_push_source_node(*schema),
        std::span<const WiringPortRef>{},
        Value{});

    NodeTypeMetaData sink_schema;
    sink_schema.display_name = "interleaved_sink";
    sink_schema.node_kind = NodeKind::Sink;
    (void)w.add_unique_node(
        std::type_index(typeid(InterleavedSinkTag)),
        NodeBuilder::native(std::move(sink_schema), {}),
        std::span<const WiringPortRef>{},
        Value{});

    (void)w.add_unique_node(
        std::type_index(typeid(SecondPushSourceTag)),
        make_push_source_node(*schema),
        std::span<const WiringPortRef>{},
        Value{});

    GraphBuilder graph = std::move(w).finish();
    REQUIRE(graph.node_count() == 3);
    CHECK(graph.nodes()[0].type().schema()->node_kind == NodeKind::PushSource);
    CHECK(graph.nodes()[1].type().schema()->node_kind == NodeKind::PushSource);
    CHECK(graph.nodes()[2].type().schema()->node_kind == NodeKind::Sink);
}

TEST_CASE("graph wiring: sub-graph composition inlines (flattens) into the parent")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<PlusTwoGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 3);   // source + two add_one (the PlusTwo sub-graph flattened)
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Int>() == Int{43});
}

TEST_CASE("graph wiring: sub-graph typed input accepts an erased generic source port")
{
    using namespace hgraph;
    stdlib::register_standard_operators();   // const_ is an operator

    GraphBuilder graph_builder = build_graph<GenericSourceIntoTypedSubGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();
    view.run();

    auto graph = view.graph();
    REQUIRE(graph.node_count() == 2);
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{42});
}

TEST_CASE("graph wiring: sub-graph SIGNAL input accepts any time-series port")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<SignalSubGraphFromTsPort>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();
    view.run();

    auto graph = view.graph();
    REQUIRE(graph.node_count() == 2);
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{1});
}

TEST_CASE("graph wiring: TS output can bind to a REF input")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<RefProbeGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Bool>());
}

TEST_CASE("graph wiring: structural TSL source can bind to a REF input as non-peered")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphBuilder graph_builder = build_graph<StructuralListRefProbeGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 3);
    REQUIRE(graph.node_at(2).output(MIN_ST).valid());
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Bool>());
}

TEST_CASE("graph wiring: structural TSB source can bind to a REF input as non-peered")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphBuilder graph_builder = build_graph<StructuralBundleRefProbeGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 3);
    REQUIRE(graph.node_at(2).output(MIN_ST).valid());
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Bool>());
}

TEST_CASE("graph wiring: brace initializer wires fixed TSL input as non-peered structure")
{
    using namespace hgraph;

    GraphExecutorValue executor = testing::run_graph(build_graph<BraceListInputGraph>());

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 2);
    REQUIRE(graph.node_at(1).output(MIN_ST).valid());
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{82});
}

TEST_CASE("graph wiring: brace initializer wires TSB input as non-peered structure")
{
    using namespace hgraph;

    GraphExecutorValue executor = testing::run_graph(build_graph<BraceBundleInputGraph>());

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 2);
    REQUIRE(graph.node_at(1).output(MIN_ST).valid());
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{82});
}

TEST_CASE("graph wiring: named brace initializer wires TSB fields by name")
{
    using namespace hgraph;

    GraphExecutorValue executor = testing::run_graph(build_graph<BraceBundleNamedInputGraph>());

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 3);
    REQUIRE(graph.node_at(2).output(MIN_ST).valid());
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Int>() == Int{43});
}

TEST_CASE("graph wiring: partial named TSB initializer fills missing fields with null sources")
{
    using namespace hgraph;

    GraphExecutorValue executor = testing::run_graph(build_graph<BraceBundlePartialInputGraph>());

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 2);
    REQUIRE(graph.node_at(1).output(MIN_ST).valid());
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{41});
}

TEST_CASE("graph wiring: node call packs surplus positional ports into an Args input")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_OUTPUT(testing::eval_node<PackedTslTailGraph>(testing::values<Int>(1, 2),
                                                        testing::values<Int>(10, 20),
                                                        testing::values<Int>(100, 200)),
                 testing::values<Int>(116, 227));
}

TEST_CASE("graph wiring: node call packs unknown keyword ports into a Kwargs input")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_OUTPUT(testing::eval_node<PackedKwargsGraph>(testing::values<Int>(1, 2),
                                                       testing::values<Int>(10, 20)),
                 testing::values<Int>(11, 22));
}

TEST_CASE("graph wiring: node call packs heterogeneous positional ports as numbered TSB fields")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<Str>("str");

    CHECK_OUTPUT(testing::eval_node<PackedNumberedGraph>(testing::values<Int>(3, 4),
                                                         testing::values<Str>(Str{"a"}, Str{"b"})),
                 testing::values<Str>(Str{"3:a"}, Str{"4:b"}));
}

TEST_CASE("graph wiring: node call packs kwargs into a default generic Kwargs input")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_OUTPUT(testing::eval_node<PackedGenericTsbGraph>(testing::values<Int>(5, 6),
                                                           testing::values<Int>(50, 60)),
                 testing::values<Int>(55, 66));
}

TEST_CASE("graph wiring: recordable_state exposes the hidden recordable-state port")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<RecordableStatePortGraph>();
    REQUIRE(graph_builder.edges().size() == 2);
    CHECK(graph_edge_source_node(graph_builder.edges()[1].source_node) == 1);
    CHECK(graph_edge_source_kind(graph_builder.edges()[1].source_node) == GraphEdgeSourceKind::RecordableState);

    GraphExecutorValue executor = testing::run_graph(std::move(graph_builder));

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 3);
    REQUIRE(graph.node_at(2).output(MIN_ST).valid());
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Int>() == Int{41});
}

TEST_CASE("graph wiring: recordable state passes through a compiled nested boundary")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<NestedRecordableStatePortGraph>();
    REQUIRE(graph_builder.edges().size() == 3);
    CHECK(graph_edge_source_kind(graph_builder.edges()[1].source_node) ==
          GraphEdgeSourceKind::RecordableState);

    GraphExecutorValue executor = testing::run_graph(std::move(graph_builder));

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 4);
    REQUIRE(graph.node_at(3).output(MIN_ST).valid());
    CHECK(graph.node_at(3).output(MIN_ST).value().checked_as<Int>() == Int{41});
}

TEST_CASE("graph wiring: error_output exposes the hidden error-output port")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<ErrorOutputPortGraph>();
    REQUIRE(graph_builder.edges().size() == 1);
    CHECK(graph_edge_source_node(graph_builder.edges()[0].source_node) == 0);
    CHECK(graph_edge_source_kind(graph_builder.edges()[0].source_node) == GraphEdgeSourceKind::ErrorOutput);

    GraphExecutorValue executor = testing::run_graph(std::move(graph_builder));

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 2);
    REQUIRE(graph.node_at(1).output(MIN_ST).valid());
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{100});
}

TEST_CASE("graph wiring: special output helpers validate source capabilities")
{
    using namespace hgraph;

    Wiring w;
    auto   source = wire<ConstantSource>(w);

    CHECK_THROWS_AS(recordable_state(source), std::logic_error);
    CHECK_THROWS_AS(error_output(source), std::logic_error);
}

TEST_CASE("graph wiring: nested node binds outer input into child graph input")
{
    using namespace hgraph;

    GraphBuilder graph_builder;
    graph_builder.label("outer_nested_add_one")
        .add_node(static_node_builder_for_constant_source())
        .add_node(nested_add_one_builder())
        .add_edge(GraphEdge{.source_node = 0, .target_node = 1, .target_path = {0}});

    GraphExecutorValue executor = testing::run_graph(std::move(graph_builder));

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 2);
    REQUIRE(graph.node_at(1).output(MIN_ST).valid());
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{42});

    auto nested_node = graph.node_at(1);
    auto nested_view = nested_node.as<SingleNestedGraphNodeView>();
    auto child_graph = nested_view.child_graph();
    CHECK(child_graph.parent_kind() == GraphParentKind::Nested);
    CHECK(child_graph.is_nested());
    CHECK_FALSE(child_graph.is_root());

    auto parent_node = child_graph.as_nested().parent_node();
    CHECK(parent_node.type() == nested_node.type());
    CHECK(parent_node.data() == nested_node.data());

    auto root_graph = child_graph.root();
    CHECK(root_graph.type() == graph.type());
    CHECK(root_graph.data() == graph.data());
    CHECK(root_graph.executor().data() == executor.view().data());

    child_graph.global_state().set("nested_state_is_root_state", Value{Int{99}});
    CHECK(graph.global_state().get_as<Int>("nested_state_is_root_state") == Int{99});
}

TEST_CASE("graph wiring: nested node forwards child output to downstream input")
{
    using namespace hgraph;

    GraphBuilder graph_builder;
    graph_builder.label("outer_nested_add_two")
        .add_node(static_node_builder_for_constant_source())
        .add_node(nested_add_one_builder())
        .add_node(static_node_builder_for_add_one())
        .add_edge(GraphEdge{.source_node = 0, .target_node = 1, .target_path = {0}})
        .add_edge(GraphEdge{.source_node = 1, .target_node = 2, .target_path = {0}});

    GraphExecutorValue executor = testing::run_graph(std::move(graph_builder));

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 3);
    REQUIRE(graph.node_at(2).output(MIN_ST).valid());
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Int>() == Int{43});
}

TEST_CASE("graph wiring: nested node propagates child graph schedule")
{
    using namespace hgraph;

    GraphBuilder graph_builder;
    graph_builder.label("outer_nested_constant").add_node(nested_constant_builder());

    GraphExecutorValue executor = testing::run_graph(std::move(graph_builder));

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 1);
    REQUIRE(graph.node_at(0).output(MIN_ST).valid());
    CHECK(graph.node_at(0).output(MIN_ST).value().checked_as<Int>() == Int{41});
}

TEST_CASE("graph wiring: an out-of-band child graph schedule pushes through to the parent node")
{
    using namespace hgraph;

    GraphBuilder graph_builder;
    graph_builder.label("outer_nested_constant").add_node(nested_constant_builder());

    testing::MockRootGraph root{graph_builder};
    auto graph = root.graph();
    graph.start(MIN_ST);
    graph.evaluate(MIN_ST);   // child evaluated; nothing further scheduled

    auto nested_view = graph.node_at(0).as<SingleNestedGraphNodeView>();
    REQUIRE(graph.next_scheduled_time() == MAX_DT);

    // Schedule a child node directly while the child graph is idle (the path a
    // notification or wall-clock alarm takes between parent evaluations): the
    // push half of the scheduling delegation must wake the parent node at the
    // same time, without waiting for the next pull-after-evaluate.
    const DateTime when = MIN_ST + TimeDelta{7};
    nested_view.child_graph_value().schedule_node(0, when);

    CHECK(graph.next_scheduled_time() == when);
    graph.stop();
}

TEST_CASE("graph wiring: nested graph evaluation propagates cached child next schedule")
{
    using namespace hgraph;

    const auto *ts_int = ts_type<TS<Int>>();

    GraphBuilder child;
    child.label("nested_cached_next_child").add_node(ticking_int_source(ts_int, 2));

    NodeTypeMetaData meta;
    meta.display_name  = "nested_cached_next";
    meta.output_schema = ts_int;

    SingleNestedGraphNodeSpec spec;
    spec.graph_builder = std::move(child);
    spec.output_binding = NestedGraphOutputBinding{.source = NestedGraphEndpoint{.node = 0}};

    GraphBuilder graph_builder;
    graph_builder.label("outer_nested_cached_next")
        .add_node(single_nested_graph_node(std::move(meta), std::move(spec)));

    testing::MockRootGraph root{graph_builder};
    auto graph = root.graph();

    graph.start(MIN_ST);
    REQUIRE(graph.next_scheduled_time() == MIN_ST);

    graph.evaluate(MIN_ST);

    CHECK(graph.node_at(0).output(MIN_ST).value().checked_as<Int>() == Int{0});
    CHECK(graph.next_scheduled_time() == MIN_ST + MIN_TD);

    graph.stop();
}

TEST_CASE("graph wiring: nested node re-evaluates its child across multiple cycles")
{
    using namespace hgraph;

    const auto *ts_int           = ts_type<TS<Int>>();
    auto        downstream_evals = std::make_shared<int>(0);

    GraphBuilder graph_builder;
    graph_builder.label("multi_cycle_nested")
        .add_node(ticking_int_source(ts_int, 3))   // emits 0, 1, 2 over three cycles
        .add_node(nested_add_one_builder())         // child forwards add_one(n)
        .add_node(counting_passthrough(ts_int, downstream_evals))
        .add_edge(GraphEdge{.source_node = 0, .target_node = 1, .target_path = {0}})
        .add_edge(GraphEdge{.source_node = 1, .target_node = 2, .target_path = {0}});

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{10});

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 3);
    // The nested child re-evaluated on every source tick and forwarded a fresh
    // value, so the downstream node ran once per cycle...
    CHECK(*downstream_evals == 3);
    // ...and the last forwarded value is add_one(2) = 3.
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Int>() == Int{3});
}

TEST_CASE("graph wiring: identical sink nodes are not interned")
{
    using namespace hgraph;

    GraphBuilder            graph_builder = build_graph<TwoSinksGraph>();
    testing::MockRootGraph  graph{graph_builder};

    // The source is deduped to one node, but the two identical sinks stay distinct:
    // a sink runs for its side effect, so each must remain its own runtime node.
    CHECK(graph.graph().node_count() == 3);
}

TEST_CASE("graph wiring: REF output can bind back to a dereferenced TS input")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<RefRoundTripGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 3);
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Int>() == Int{41});
}

TEST_CASE("graph wiring: REF round trip supports TSS")
{
    using namespace hgraph;

    GraphExecutorValue executor = testing::run_graph(build_graph<RefRoundTripFor<SetSource, TSS<Int>>>());

    auto graph  = executor.view().graph();
    auto output = graph.node_at(2).output(MIN_ST);
    auto set    = output.as_set();
    Value one{Int{1}};
    Value two{Int{2}};

    REQUIRE(graph.node_count() == 3);
    CHECK(set.size() == 2);
    CHECK(set.contains(one.view()));
    CHECK(set.contains(two.view()));
}

TEST_CASE("graph wiring: REF round trip supports TSD")
{
    using namespace hgraph;

    GraphExecutorValue executor = testing::run_graph(build_graph<RefRoundTripFor<DictSource, TSD<Int, TS<Int>>>>());

    auto graph  = executor.view().graph();
    auto output = graph.node_at(2).output(MIN_ST);
    auto dict   = output.as_dict();
    Value one{Int{1}};
    Value two{Int{2}};

    REQUIRE(graph.node_count() == 3);
    REQUIRE(dict.contains(one.view()));
    REQUIRE(dict.contains(two.view()));
    CHECK(dict.at(one.view()).value().checked_as<Int>() == Int{10});
    CHECK(dict.at(two.view()).value().checked_as<Int>() == Int{20});
}

TEST_CASE("graph wiring: REF round trip supports fixed TSL")
{
    using namespace hgraph;

    GraphExecutorValue executor = testing::run_graph(build_graph<RefRoundTripFor<ListSource, TSL<TS<Int>, 2>>>());

    auto graph  = executor.view().graph();
    auto output = graph.node_at(2).output(MIN_ST);
    auto list   = output.as_list();

    REQUIRE(graph.node_count() == 3);
    REQUIRE(list.size() == 2);
    CHECK(list.at(0).value().checked_as<Int>() == Int{7});
    CHECK(list.at(1).value().checked_as<Int>() == Int{8});
}

TEST_CASE("graph wiring: REF round trip supports TSB")
{
    using namespace hgraph;

    GraphExecutorValue executor = testing::run_graph(build_graph<RefRoundTripFor<BundleSource, RefRoundTripBundle>>());

    auto graph  = executor.view().graph();
    auto output = graph.node_at(2).output(MIN_ST);
    auto bundle = output.as_bundle();

    REQUIRE(graph.node_count() == 3);
    CHECK(bundle.field("a").value().checked_as<Int>() == Int{11});
    CHECK(bundle.field("b").value().checked_as<Int>() == Int{12});
}

TEST_CASE("graph wiring: REF round trip supports nested collection children")
{
    using namespace hgraph;

    GraphExecutorValue executor =
        testing::run_graph(build_graph<RefRoundTripFor<NestedListSetSource, TSL<TSS<Int>, 2>>>());

    auto graph         = executor.view().graph();
    auto output        = graph.node_at(2).output(MIN_ST);
    auto list          = output.as_list();
    auto first_child   = list.at(0);
    auto second_child  = list.at(1);
    auto first_values  = first_child.as_set();
    auto second_values = second_child.as_set();
    Value three{Int{3}};
    Value four{Int{4}};
    Value five{Int{5}};

    REQUIRE(graph.node_count() == 3);
    CHECK(first_values.size() == 2);
    CHECK(first_values.contains(three.view()));
    CHECK(first_values.contains(four.view()));
    CHECK(second_values.size() == 1);
    CHECK(second_values.contains(five.view()));
}

TEST_CASE("graph wiring: multi-input node wires and type-checks its ports")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<SumGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);   // one interned source + sum
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{82});
}

TEST_CASE("graph wiring: a scalar argument configures a wired node")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<ScaledSourceGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 1);
    CHECK(graph.node_at(0).output(MIN_ST).value().checked_as<Int>() == Int{7});
}

TEST_CASE("graph wiring: a scalar argument coexists with a time-series input port")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<ShiftGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);   // source + shift
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{46});
}

TEST_CASE("graph wiring: scalar values participate in node interning")
{
    using namespace hgraph;

    Wiring w;
    auto   a = wire<ScaledSource>(w, Int{7});
    auto   b = wire<ScaledSource>(w, Int{7});   // equal scalar -> same interned instance
    auto   c = wire<ScaledSource>(w, Int{8});   // different scalar -> distinct instance

    CHECK(a.node() == b.node());
    CHECK(a.node() != c.node());

    GraphBuilder            graph_builder = std::move(w).finish();
    testing::MockRootGraph  graph{graph_builder};
    CHECK(graph.graph().node_count() == 2);   // {7} deduped, {8} distinct
}

TEST_CASE("graph wiring: StaticGraphSignature reflects a graph's compose parameters")
{
    using namespace hgraph;

    using bare = StaticGraphSignature<AddOneGraph>;   // compose(Wiring &)
    STATIC_REQUIRE(bare::param_count() == 0);
    STATIC_REQUIRE(bare::input_count() == 0);
    STATIC_REQUIRE(bare::scalar_count() == 0);

    using configured = StaticGraphSignature<ConfiguredSourceGraph>;   // compose(Wiring &, Scalar<...>)
    STATIC_REQUIRE(configured::param_count() == 1);
    STATIC_REQUIRE(configured::input_count() == 0);
    STATIC_REQUIRE(configured::scalar_count() == 1);
}

TEST_CASE("graph wiring: a top-level graph takes a scalar parameter via build_graph")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<ConfiguredSourceGraph>(Int{9});

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 1);
    CHECK(graph.node_at(0).output(MIN_ST).value().checked_as<Int>() == Int{9});
}

TEST_CASE("graph wiring: build_graph scalar parameters accept keyword arguments")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<ConfiguredPairSourceGraph>(arg<"rhs">(Int{2}), arg<"lhs">(Int{4}));

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 1);
    CHECK(graph.node_at(0).output(MIN_ST).value().checked_as<Int>() == Int{42});
}

TEST_CASE("graph wiring: build_graph uses defaulted scalar parameters")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<DefaultConfiguredSourceGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 1);
    CHECK(graph.node_at(0).output(MIN_ST).value().checked_as<Int>() == Int{12});
}

TEST_CASE("graph wiring: build_graph rejects invalid keyword scalar calls")
{
    using namespace hgraph;

    CHECK_THROWS_WITH(build_graph<ConfiguredPairSourceGraph>(arg<"lhs">(Int{1}), arg<"lhs">(Int{2})),
                      Catch::Matchers::ContainsSubstring("multiple values for scalar parameter 'lhs'"));
    CHECK_THROWS_WITH(build_graph<ConfiguredPairSourceGraph>(arg<"unknown">(Int{1}), arg<"rhs">(Int{2})),
                      Catch::Matchers::ContainsSubstring("unexpected keyword argument 'unknown'"));
    CHECK_THROWS_WITH(build_graph<ConfiguredPairSourceGraph>(arg<"rhs">(Int{2}), Int{4}),
                      Catch::Matchers::ContainsSubstring("positional argument follows a named argument"));
}

TEST_CASE("graph wiring: a graph scalar parameter threads into a node's scalar")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<OffsetGraph>(Int{5});   // 41 + 5

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{46});
}

TEST_CASE("graph wiring: direct node wiring accepts keyword arguments")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<NamedNodeWireGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{46});
}

TEST_CASE("graph wiring: direct node wiring uses defaulted scalar parameters")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<DefaultNodeWireGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{46});
}

TEST_CASE("graph wiring: wire<G> auto-wraps a scalar literal for a sub-graph parameter")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<ShiftBySubGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);   // source + shift (ShiftBy flattened away)
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{46});
}

TEST_CASE("graph wiring: flattened sub-graph wiring accepts keyword arguments")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<NamedSubGraphWireGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);   // source + shift (ShiftBy flattened away)
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{46});
}

TEST_CASE("graph wiring: catch-all service resolvers compose once")
{
    using namespace hgraph;

    Wiring wiring;
    int invocations = 0;
    wiring.register_catch_all_service_implementation_candidate(
        "test catch-all", [&](Wiring &target) {
            ++invocations;
            const auto built = target.built_service_paths();
            for (const auto &[path, kind] : target.service_client_paths())
            {
                if (std::ranges::none_of(built, [&](const auto &entry) {
                        return entry.first == path;
                    }))
                {
                    target.register_built_service_path(path, kind);
                }
            }
        });

    wiring.register_service_client_path("service://first", "test service");
    wiring.build_services();
    CHECK(invocations == 1);

    wiring.register_service_client_path("service://second", "test service");
    wiring.build_services();
    CHECK(invocations == 1);
    CHECK(wiring.built_service_paths().size() == 1);
}

TEST_CASE("graph wiring: isolated sub-graphs reject implementation registration")
{
    using namespace hgraph;

    Wiring wiring{WiringKind::SubGraph};
    const auto message = Catch::Matchers::ContainsSubstring(
        "cannot be registered inside an isolated sub-graph");

    CHECK_THROWS_WITH(
        wiring.register_service_implementation_candidate(
            {"service://exact"}, "exact", [](Wiring &) {}),
        message);
    CHECK_THROWS_WITH(
        wiring.register_default_service_implementation_candidate(
            "service://", "/default", "default",
            [](Wiring &, std::string_view) {}),
        message);
    CHECK_THROWS_WITH(
        wiring.register_catch_all_service_implementation_candidate(
            "catch-all", [](Wiring &) {}),
        message);
}

TEST_CASE("graph wiring: top-level wiring accepts implementation registration")
{
    using namespace hgraph;

    Wiring wiring;
    CHECK_NOTHROW(wiring.register_service_implementation_candidate(
        {"service://exact"}, "exact", [](Wiring &) {}));
}

TEST_CASE("graph wiring: structured service client reflection retains interface identity")
{
    using namespace hgraph;

    Wiring wiring;
    wiring.register_service_client_path(
        "adaptor://prices[VALUE=int]/quote", "adaptor", "quote", "VALUE=int");

    const auto records = wiring.service_client_records();
    REQUIRE(records.size() == 1);
    CHECK(records[0].base_path == "adaptor://prices[VALUE=int]/quote");
    CHECK(records[0].endpoint_path == records[0].base_path);
    CHECK(records[0].kind == "adaptor");
    CHECK(records[0].interface_name == "quote");
    CHECK(records[0].specialization == "VALUE=int");
    CHECK(records[0].receive);
}

TEST_CASE("graph wiring: conflicting structured service client identity is rejected")
{
    using namespace hgraph;

    Wiring wiring;
    wiring.register_service_client_path(
        "adaptor://prices[VALUE=int]/quote", "adaptor", "quote", "VALUE=int");

    CHECK_THROWS_WITH(
        wiring.register_service_client_path(
            "adaptor://prices[VALUE=int]/quote", "adaptor", "trade", "VALUE=int"),
        Catch::Matchers::ContainsSubstring("conflicting service/adaptor client identity"));
}

TEST_CASE("graph wiring: partial implementation scope reports only used endpoints")
{
    using namespace hgraph;

    Wiring wiring;
    auto scope = wiring.service_implementation_scope(
        "partial", std::vector<WiringServiceImplementationEndpoint>{
            {"adaptor://first/quote/from_graph", {}},
            {"adaptor://second/quote/from_graph", {}},
        }, false);
    wiring.register_service_implementation_stub(
        "adaptor://first/quote/from_graph", "adaptor");

    CHECK(wiring.service_implementation_used_endpoints() ==
          std::vector<std::string>{"adaptor://first/quote/from_graph"});
    CHECK_NOTHROW(scope.complete());
}

TEST_CASE("graph wiring: exact and selective catch-all implementations coexist")
{
    using namespace hgraph;

    Wiring wiring;
    wiring.register_service_client_path("adaptor://exact/quote", "adaptor");
    wiring.register_service_client_path("adaptor://fallback/quote", "adaptor");
    wiring.register_service_implementation_candidate(
        {"adaptor://exact/quote"}, "exact",
        [](Wiring &target) {
            target.register_built_service_path("adaptor://exact/quote", "adaptor");
        });
    wiring.register_catch_all_service_implementation_candidate(
        "selective catch-all", [](Wiring &target) {
            target.register_built_service_path("adaptor://fallback/quote", "adaptor");
        });

    CHECK_NOTHROW(wiring.build_services());
    CHECK(wiring.built_service_paths() ==
          std::vector<std::pair<std::string, std::string>>{
              {"adaptor://exact/quote", "adaptor"},
              {"adaptor://fallback/quote", "adaptor"},
          });
}

TEST_CASE("graph wiring: default service candidates serve concrete paths lazily")
{
    using namespace hgraph;

    Wiring wiring;
    std::vector<std::string> materialized;
    wiring.register_default_service_implementation_candidate(
        "service://", "/quote", "default quote",
        [&](Wiring &target, std::string_view requested_path) {
            materialized.emplace_back(requested_path);
            target.register_built_service_path(std::string{requested_path}, "quote");
        });

    wiring.build_services();
    CHECK(materialized.empty());

    wiring.register_service_client_path("service://custom/quote", "quote");
    wiring.build_services();
    CHECK(materialized == std::vector<std::string>{"service://custom/quote"});

    wiring.build_services();
    CHECK(materialized.size() == 1);
}

TEST_CASE("graph wiring: exact service candidates take precedence over defaults")
{
    using namespace hgraph;

    Wiring wiring;
    int default_materializations = 0;
    int exact_materializations = 0;
    wiring.register_default_service_implementation_candidate(
        "service://", "/quote", "default quote",
        [&](Wiring &, std::string_view) { ++default_materializations; });
    wiring.register_service_implementation_candidate(
        {"service://custom/quote"}, "custom quote", [&](Wiring &target) {
            ++exact_materializations;
            target.register_built_service_path("service://custom/quote", "quote");
        });

    wiring.register_service_client_path("service://custom/quote", "quote");
    wiring.build_services();

    CHECK(exact_materializations == 1);
    CHECK(default_materializations == 0);
}

TEST_CASE("graph wiring: default multi-interface candidates materialize atomically")
{
    using namespace hgraph;

    Wiring wiring;
    int materializations = 0;
    wiring.register_default_service_implementation_candidate(
        {{"ref_svc://", "/prices"}, {"reqrepl_svc://", "/orders"}},
        "default backend", [&](Wiring &target, std::string_view) {
            ++materializations;
            target.register_built_service_path("ref_svc://prod/prices", "prices");
            target.register_built_service_path("reqrepl_svc://prod/orders", "orders");
        });

    wiring.register_service_client_path("reqrepl_svc://prod/orders", "orders");
    wiring.build_services();
    CHECK(materializations == 1);

    wiring.register_service_client_path("ref_svc://prod/prices", "prices");
    wiring.build_services();
    CHECK(materializations == 1);
}

TEST_CASE("graph wiring: default multi-interface candidates reject exact overlap")
{
    using namespace hgraph;

    Wiring wiring;
    wiring.register_default_service_implementation_candidate(
        {{"ref_svc://", "/prices"}, {"reqrepl_svc://", "/orders"}},
        "default backend", [](Wiring &, std::string_view) {});
    wiring.register_service_implementation_candidate(
        {"ref_svc://prod/prices"}, "production prices", [](Wiring &target) {
            target.register_built_service_path("ref_svc://prod/prices", "prices");
        });
    wiring.register_service_client_path("reqrepl_svc://prod/orders", "orders");

    CHECK_THROWS_WITH(
        wiring.build_services(),
        Catch::Matchers::ContainsSubstring("default backend") &&
            Catch::Matchers::ContainsSubstring("production prices") &&
            Catch::Matchers::ContainsSubstring("ref_svc://prod/prices"));
}
