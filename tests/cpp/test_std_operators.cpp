// Phase 3: real lib/std operator families. One name
// collects several per-type implementations; the most specific is selected at wiring.
//
// These exercise the "operator signature is a suggestion" principle: the operators
// declare independent type variables for lhs / rhs / result, so a single name covers
// homogeneous (int + int), mixed (int + float -> float), heterogeneous
// (datetime + timedelta -> datetime), and result-differs cases (div int / int -> float;
// datetime - datetime -> timedelta).
//
// Operators are evaluated through the type-erased ``eval_node<Op>`` harness: the output
// schema is the one operator dispatch resolves at wiring time, so results come back as
// per-cycle ``Value`` deltas. The expected sequence is written with the same
// ``values<T>(...)`` helper used for the inputs; ``CHECK_OUTPUT`` boxes it and compares
// with ``Value`` equality.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/operators/impl/arithmetic_impl.h>
#include <hgraph/lib/std/operators/impl/collection_impl.h>
#include <hgraph/lib/std/operators/impl/string_impl.h>
#include <hgraph/lib/std/standard_types.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/util/date_time.h>

#include <arrow/api.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <numbers>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using namespace std::chrono;

    [[nodiscard]] DateTime dt(std::int64_t micros) { return DateTime{microseconds{micros}}; }
    [[nodiscard]] Date ymd(int y, unsigned m, unsigned d)
    {
        return Date{year{y} / month{m} / day{d}};
    }

    [[nodiscard]] Value int_tuple(std::initializer_list<Int> values)
    {
        const auto *meta = scalar_descriptor<HomogeneousTuple<Int>>::value_meta();
        const auto binding = ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta());
        ListBuilder builder{binding};
        for (Int value : values) { builder.push_back(value); }
        ListStorage storage = builder.build_storage();
        return Value{compact_list_type(binding, *meta), &storage};
    }

    [[nodiscard]] Value nullable_int_tuple(std::initializer_list<std::optional<Int>> values)
    {
        const auto *meta = scalar_descriptor<HomogeneousTuple<Int>>::value_meta();
        const auto binding = ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta());
        ListBuilder builder{binding};
        for (const auto value : values)
        {
            if (value.has_value()) { builder.push_back(*value); }
            else { builder.push_back_unset(); }
        }
        ListStorage storage = builder.build_storage();
        return Value{compact_list_type(binding, *meta), &storage};
    }

    [[nodiscard]] Value int_set(std::initializer_list<Int> values)
    {
        const auto binding =
            ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta());
        SetBuilder builder{binding};
        for (Int value : values) { builder.insert(value); }
        return builder.build();
    }

    [[nodiscard]] Value int_map(std::initializer_list<std::pair<Int, Int>> values)
    {
        const auto binding =
            ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta());
        MapBuilder builder{binding, binding};
        for (const auto &[key, value] : values) { builder.set_item(key, value); }
        return builder.build();
    }

    [[nodiscard]] Series int_series(std::initializer_list<std::optional<Int>> values)
    {
        arrow::Int64Builder builder;
        for (const auto value : values)
        {
            const arrow::Status status = value.has_value() ? builder.Append(*value) : builder.AppendNull();
            if (!status.ok()) { throw std::runtime_error(status.ToString()); }
        }
        std::shared_ptr<arrow::Array> array;
        const auto status = builder.Finish(&array);
        if (!status.ok()) { throw std::runtime_error(status.ToString()); }
        return Series{std::move(array)};
    }

    [[nodiscard]] Series float_series(std::initializer_list<std::optional<Float>> values)
    {
        arrow::DoubleBuilder builder;
        for (const auto value : values)
        {
            const arrow::Status status = value.has_value() ? builder.Append(*value) : builder.AppendNull();
            if (!status.ok()) { throw std::runtime_error(status.ToString()); }
        }
        std::shared_ptr<arrow::Array> array;
        const auto status = builder.Finish(&array);
        if (!status.ok()) { throw std::runtime_error(status.ToString()); }
        return Series{std::move(array)};
    }

    void check_series_output(const std::vector<std::optional<Series>> &actual,
                             const std::vector<std::optional<Series>> &expected)
    {
        REQUIRE(actual.size() == expected.size());
        for (std::size_t index = 0; index < actual.size(); ++index)
        {
            INFO("series output index " << index);
            REQUIRE(actual[index].has_value() == expected[index].has_value());
            if (actual[index].has_value())
            {
                REQUIRE(actual[index]->array != nullptr);
                REQUIRE(expected[index]->array != nullptr);
                CHECK(actual[index]->array->Equals(expected[index]->array));
            }
        }
    }

    [[nodiscard]] WiringArg scalar_arg(Value value)
    {
        WiringArg arg;
        arg.kind         = WiringArg::Kind::Scalar;
        arg.scalar_value = std::move(value);
        arg.scalar_meta  = arg.scalar_value.schema();
        return arg;
    }

    template <typename GraphT, typename Out>
    [[nodiscard]] std::vector<std::optional<Out>>
    eval_runtime_schema_graph(const TSValueTypeMetaData *input_schema,
                              const std::vector<std::optional<Value>> &input)
    {
        Wiring w;
        std::array<WiringArg, 1> replay_args{scalar_arg(Value{Str{"eval_node::in"}})};
        OperatorWireResult replay =
            wire_operator(w, "replay", std::span<const WiringArg>{replay_args}, true, input_schema);
        if (!replay.has_output) { throw std::logic_error("runtime-schema replay did not produce an output"); }

        Port<TS<Out>> out = GraphT::compose(w, replay.output);
        wire<stdlib::dense_record_impl>(w, out, std::string{"eval_node::out"});

        GraphBuilder gb = std::move(w).finish();
        set_replay_deltas(gb.global_state(), "eval_node::in", input);

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
        GraphExecutorValue executor = eb.make_executor();
        auto               view     = executor.view();
        view.run();

        auto recorded = get_recorded_values<Out>(view.graph().global_state(), "eval_node::out");
        if (recorded.size() < input.size()) { recorded.resize(input.size()); }
        return recorded;
    }

    struct SeriesToTupleGraph
    {
        static constexpr auto name = "series_to_tuple_graph";

        static Port<TS<HomogeneousTuple<Int>>> compose(Wiring &w, Port<TS<SeriesOf<Int>>> ts)
        {
            return wire<stdlib::convert, TS<HomogeneousTuple<Int>>>(w, ts);
        }
    };

    struct SeriesAddGraph
    {
        static constexpr auto name = "series_add_graph";
        static Port<TS<SeriesOf<Int>>> compose(Wiring &w, Port<TS<SeriesOf<Int>>> lhs,
                                               Port<TS<SeriesOf<Int>>> rhs)
        {
            return wire<stdlib::add_>(w, lhs, rhs).as<TS<SeriesOf<Int>>>();
        }
    };

    struct SeriesMixedAddGraph
    {
        static constexpr auto name = "series_mixed_add_graph";
        static Port<TS<SeriesOf<Float>>> compose(Wiring &w, Port<TS<SeriesOf<Int>>> lhs,
                                                 Port<TS<Float>> rhs)
        {
            return wire<stdlib::add_>(w, lhs, rhs).as<TS<SeriesOf<Float>>>();
        }
    };

    struct SeriesDivGraph
    {
        static constexpr auto name = "series_div_graph";
        static Port<TS<SeriesOf<Float>>> compose(Wiring &w, Port<TS<SeriesOf<Int>>> lhs,
                                                 Port<TS<SeriesOf<Int>>> rhs)
        {
            return wire<stdlib::div_>(w, lhs, rhs).as<TS<SeriesOf<Float>>>();
        }
    };

    struct SeriesGetItemGraph
    {
        static constexpr auto name = "series_get_item_graph";
        static Port<TS<Int>> compose(Wiring &w, Port<TS<SeriesOf<Int>>> ts,
                                     Port<TS<Int>> index)
        {
            return wire<stdlib::getitem_>(w, ts, index).as<TS<Int>>();
        }
    };

    struct SeriesContainsGraph
    {
        static constexpr auto name = "series_contains_graph";
        static Port<TS<Bool>> compose(Wiring &w, Port<TS<SeriesOf<Int>>> ts,
                                      Port<TS<Int>> item)
        {
            return wire<stdlib::contains_>(w, ts, item).as<TS<Bool>>();
        }
    };

    // Lightweight graphs with declared inputs/outputs, driven through eval_node.
    struct SyntaxArithmeticGraph
    {
        static constexpr auto name = "syntax_arithmetic_graph";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> a, Port<TS<Int>> b)
        {
            using namespace hgraph::stdlib::syntax;
            return (a + b * Int{2}).as<TS<Int>>();
        }
    };

    struct SyntaxComparisonGraph
    {
        static constexpr auto name = "syntax_comparison_graph";
        static Port<TS<Bool>> compose(Wiring &, Port<TS<Int>> a, Port<TS<Float>> b)
        {
            using namespace hgraph::stdlib::syntax;
            return ((a < b) || !(a == Int{0})).as<TS<Bool>>();
        }
    };

    struct SyntaxNamedHelperGraph
    {
        static constexpr auto name = "syntax_named_helper_graph";
        static Port<TS<Float>> compose(Wiring &, Port<TS<Int>> a)
        {
            using namespace hgraph::stdlib::syntax;
            return (pow(abs(-a), Int{2}) / Int{2}).as<TS<Float>>();
        }
    };

    struct SyntaxBadCastGraph
    {
        static constexpr auto name = "syntax_bad_cast_graph";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> a, Port<TS<Int>> b)
        {
            using namespace hgraph::stdlib::syntax;
            return (a / b).as<TS<Int>>();   // int / int -> float: the cast must throw
        }
    };

    struct WindowSnapshot
    {
        static constexpr auto name = "window_snapshot";

        static void eval(In<"window", TsVar<"W">, InputValidity::Unchecked> input,
                         Out<TS<Int>> out)
        {
            auto window = input.base().as_window();
            Int  total  = 0;
            for (const ValueView value : window.values()) { total += value.checked_as<Int>(); }
            out.set(static_cast<Int>(window.size()) * 100 + total);
        }
    };

    struct ResettableTickWindowGraph
    {
        static constexpr auto name = "resettable_tick_window_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts, Port<SIGNAL> reset)
        {
            auto window = wire<stdlib::to_window>(w, ts, Int{3}, Int{1}, reset);
            return wire<WindowSnapshot, TS<Int>>(w, window);
        }
    };

    struct ResettableDurationWindowGraph
    {
        static constexpr auto name = "resettable_duration_window_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts, Port<SIGNAL> reset)
        {
            auto window = wire<stdlib::to_window>(w, ts, MIN_TD * 10, MIN_TD, reset);
            return wire<WindowSnapshot, TS<Int>>(w, window);
        }
    };

    struct WindowStdDdofGraph
    {
        static constexpr auto name = "window_std_ddof_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto window = wire<stdlib::to_window>(w, ts, Int{3}, Int{3});
            return wire<stdlib::std_>(
                       w, window, arg<"ddof">(Int{1}))
                .as<TS<Float>>();
        }
    };

    // Scalar-container TS schemas are runtime metadata today. The wrapper graph
    // pins the output type expectation while eval_runtime_schema_graph supplies
    // the input schema at replay.
    struct ScalarContainerMinGraph
    {
        static Port<TS<Int>>  compose(Wiring &w, Port<void> ts)
        {
            return wire<stdlib::min_>(w, ts).as<TS<Int>>();
        }
    };

    struct ScalarContainerSumGraph
    {
        static Port<TS<Int>>  compose(Wiring &w, Port<void> ts)
        {
            return wire<stdlib::sum_>(w, ts).as<TS<Int>>();
        }
    };

    struct ScalarContainerMeanGraph
    {
        static Port<TS<Float>> compose(Wiring &w, Port<void> ts)
        {
            return wire<stdlib::mean>(w, ts).as<TS<Float>>();
        }
    };

    struct ScalarContainerStdGraph
    {
        static Port<TS<Float>> compose(Wiring &w, Port<void> ts)
        {
            return wire<stdlib::std_>(w, ts).as<TS<Float>>();
        }
    };

    struct SplitToPairGraph
    {
        static constexpr auto  name = "split_to_pair_graph";
        static Port<TSL<TS<Str>, 2>> compose(Wiring &w, Port<TS<Str>> s)
        {
            return wire<stdlib::split, TSL<TS<Str>, 2>>(w, s, Str{","});
        }
    };

    struct JoinDefaultGraph
    {
        static constexpr auto name = "join_default_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TSL<TS<Str>, 3>> strings)
        {
            return wire<stdlib::join>(w, strings, Str{","}).as<TS<Str>>();
        }
    };

    struct JoinStrictGraph
    {
        static constexpr auto name = "join_strict_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TSL<TS<Str>, 3>> strings)
        {
            return wire<stdlib::join>(w, strings, Str{","}, arg<"__strict__">(Bool{true})).as<TS<Str>>();
        }
    };

    struct FormatArgsGraph
    {
        static constexpr auto name = "format_args_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> fmt, Port<TS<Int>> ts1, Port<TS<Str>> ts2)
        {
            return wire<stdlib::format_>(w, fmt, ts1, ts2).as<TS<Str>>();
        }
    };

    struct FormatNoArgsGraph
    {
        static constexpr auto name = "format_no_args_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> fmt)
        {
            return wire<stdlib::format_>(w, fmt).as<TS<Str>>();
        }
    };

    struct FormatBoolGraph
    {
        static constexpr auto name = "format_bool_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Bool>> value)
        {
            return wire<stdlib::format_>(w, Str{"value={}"}, value).as<TS<Str>>();
        }
    };

    struct FormatRefArgumentGraph
    {
        static constexpr auto name = "format_ref_argument_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Bool>> choose_minimum, Port<TS<Int>> lhs,
                                     Port<TS<Int>> rhs)
        {
            auto minimum   = wire<stdlib::min_>(w, lhs, rhs);
            auto maximum   = wire<stdlib::max_>(w, lhs, rhs);
            auto selected  = wire<stdlib::if_then_else>(w, choose_minimum, minimum, maximum);
            auto remainder = wire<stdlib::mod_>(w, lhs, rhs);
            return wire<stdlib::format_>(w, Str{"{}:{}"}, selected, remainder).as<TS<Str>>();
        }
    };

    struct ValidOverRefSelectionGraph
    {
        static constexpr auto name = "valid_over_ref_selection_graph";
        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Bool>> choose_minimum, Port<TS<Int>> lhs,
                                      Port<TS<Int>> rhs)
        {
            auto minimum  = wire<stdlib::min_>(w, lhs, rhs);
            auto maximum  = wire<stdlib::max_>(w, lhs, rhs);
            auto selected = wire<stdlib::if_then_else>(w, choose_minimum, minimum, maximum);
            return wire<stdlib::valid>(w, selected).as<TS<Bool>>();
        }
    };

    struct DedupIntConstGraph
    {
        static constexpr auto name = "dedup_int_const_graph";
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> driver)
        {
            (void)driver;
            return wire<stdlib::dedup>(w, Int{2}).as<TS<Int>>();
        }
    };

    struct DedupIntToleranceGraph
    {
        static constexpr auto name = "dedup_int_tolerance_graph";
        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> ts)
        {
            return wire<stdlib::dedup>(w, ts, Int{1}).as<TS<Float>>();
        }
    };

    struct FormatKwargsGraph
    {
        static constexpr auto name = "format_kwargs_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> fmt, Port<TS<Int>> ts1, Port<TS<Str>> ts2)
        {
            return wire<stdlib::format_>(w, fmt, arg<"ts1">(ts1), arg<"ts2">(ts2)).as<TS<Str>>();
        }
    };

    struct FormatMixedGraph
    {
        static constexpr auto name = "format_mixed_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> fmt, Port<TS<Float>> ts,
                                     Port<TS<Int>> ts1, Port<TS<Str>> ts2)
        {
            return wire<stdlib::format_>(w, fmt, ts, arg<"ts1">(ts1), arg<"ts2">(ts2)).as<TS<Str>>();
        }
    };

    struct FormatSampledGraph
    {
        static constexpr auto name = "format_sampled_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> fmt, Port<TS<Int>> ts1, Port<TS<Str>> ts2)
        {
            return wire<stdlib::format_>(w, fmt, ts1, ts2, arg<"__sample__">(Int{3})).as<TS<Str>>();
        }
    };

    using ContainerAccessBundle = UnNamedTSB<Field<"a", TS<Int>>, Field<"b", TS<Str>>>;
    using ContainerAccessReferenceBundle =
        UnNamedTSB<Field<"a", REF<TS<Int>>>, Field<"b", REF<TS<Str>>>>;
    using NamedContainerAccessBundle =
        TSB<"NamedContainerAccessBundle", Field<"a", TS<Int>>, Field<"b", TS<Str>>>;
    using HandlerOutputBundle =
        UnNamedTSB<Field<"response", TS<Int>>, Field<"audit", TS<Str>>>;
    using NumericTsbBundle      = UnNamedTSB<Field<"a", TS<Int>>, Field<"b", TS<Float>>>;
    using FloatTsbBundle        = UnNamedTSB<Field<"a", TS<Float>>, Field<"b", TS<Float>>>;
    using IntTsbBundle          = UnNamedTSB<Field<"a", TS<Int>>, Field<"b", TS<Int>>>;
    using IfIntRefBundle        = UnNamedTSB<Field<"true", REF<TS<Int>>>, Field<"false", REF<TS<Int>>>>;
    using IfIntTsdRefBundle = UnNamedTSB<Field<"true", REF<TSD<Int, TS<Int>>>>,
                                         Field<"false", REF<TSD<Int, TS<Int>>>>>;
    using RoutedIntRefList      = TSL<REF<TS<Int>>, 3>;
    using IntTslPair            = TSL<TS<Int>, 2>;
    using IntTsd                = TSD<Int, TS<Int>>;

    struct ForwardReference
    {
        static constexpr auto name = "forward_reference";

        static void eval(In<"ts", REF<TS<Int>>> ts, Out<REF<TS<Int>>> out)
        {
            out.set(ts.reference());
        }
    };

    struct ForwardReferenceGraph
    {
        static constexpr auto name = "forward_reference_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            return wire<ForwardReference>(w, ts).as<TS<Int>>();
        }
    };

    struct CombineTsdReferenceTopologyGraph
    {
        static constexpr auto name = "combine_tsd_reference_topology_graph";

        static Port<TSD<Str, TS<Int>>> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Int>> b)
        {
            return wire<stdlib::combine_tsd>(
                       w, stdlib::make_list<Str>({Str{"a"}, Str{"b"}}), a, b)
                .as<TSD<Str, TS<Int>>>();
        }
    };

    template <typename Schema>
    struct EmptyReferenceSource
    {
        static constexpr auto name              = "empty_reference_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<REF<Schema>> out)
        {
            out.set(TimeSeriesReference::empty(ts_type<Schema>()));
        }
    };

    struct TslReferenceFlipGraph
    {
        static constexpr auto name = "tsl_reference_flip_graph";

        static Port<IntTslPair> compose(Wiring &w,
                                        Port<IntTslPair> first,
                                        Port<IntTslPair> second,
                                        Port<TS<Int>> index)
        {
            auto empty = wire<EmptyReferenceSource<IntTslPair>>(w);
            auto choices = stdlib::to_tsl<TSL<IntTslPair, 3>>(w, first, second, empty);
            return wire<stdlib::getitem_>(w, choices, index).as<IntTslPair>();
        }
    };

    struct FixedTslFirstGraph
    {
        static constexpr auto name = "fixed_tsl_first_graph";

        static Port<TS<Int>> compose(Wiring &, Port<IntTslPair> ts)
        {
            return tsl_element(ts, 0);
        }
    };

    struct TsbReferenceFlipGraph
    {
        static constexpr auto name = "tsb_reference_flip_graph";

        static Port<ContainerAccessBundle> compose(Wiring &w,
                                                   Port<ContainerAccessBundle> first,
                                                   Port<ContainerAccessBundle> second,
                                                   Port<TS<Int>> index)
        {
            auto empty = wire<EmptyReferenceSource<ContainerAccessBundle>>(w);
            auto choices = stdlib::to_tsl<TSL<ContainerAccessBundle, 3>>(w, first, second, empty);
            return wire<stdlib::getitem_>(w, choices, index).as<ContainerAccessBundle>();
        }
    };

    struct DereferenceTsbReferenceGraph
    {
        static constexpr auto name = "dereference_tsb_reference_graph";

        static Port<ContainerAccessBundle> compose(Wiring &w,
                                                   Port<ContainerAccessBundle> first,
                                                   Port<ContainerAccessBundle> second,
                                                   Port<TS<Int>> index)
        {
            auto empty = wire<EmptyReferenceSource<ContainerAccessBundle>>(w);
            auto choices = stdlib::to_tsl<TSL<ContainerAccessBundle, 3>>(w, first, second, empty);
            auto selected = wire<stdlib::getitem_>(w, choices, index).as<REF<ContainerAccessBundle>>();
            auto references = wire<stdlib::dereference>(w, selected);
            if (references.erased().is_structural_source())
            {
                throw std::logic_error("dereference did not materialize a node output");
            }
            if (references.erased().schema != ts_type<ContainerAccessReferenceBundle>())
            {
                throw std::logic_error("dereference did not resolve a TSB of field references");
            }

            auto materialized = references.as<ContainerAccessReferenceBundle>();
            auto a = wire<stdlib::getattr_>(w, materialized, Str{"a"}).as<REF<TS<Int>>>();
            auto b = wire<stdlib::getattr_>(w, materialized, Str{"b"}).as<REF<TS<Str>>>();
            return stdlib::to_tsb<ContainerAccessBundle>(w, a, b);
        }
    };

    struct MakeContainerAccessBundle
    {
        static constexpr auto name = "make_container_access_bundle";

        static void eval(In<"a", TS<Int>> a, In<"b", TS<Str>> b, Out<ContainerAccessBundle> out)
        {
            out.field<"a">().set(a.value());
            out.field<"b">().set(b.value());
        }
    };

    struct StructuralTsbContainerAccessGraph
    {
        static constexpr auto name = "structural_tsb_container_access_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Str>> b)
        {
            auto bundle     = stdlib::to_tsb<ContainerAccessBundle>(w, a, b);
            auto a_by_name  = wire<stdlib::getitem_>(w, bundle, Str{"a"}).as<TS<Int>>();
            auto b_by_index = wire<stdlib::getitem_>(w, bundle, Int{-1}).as<TS<Str>>();
            auto b_len      = wire<stdlib::len_>(w, b_by_index).as<TS<Int>>();
            return wire<stdlib::add_>(w, a_by_name, b_len).as<TS<Int>>();
        }
    };

    struct PeeredTsbContainerAccessGraph
    {
        static constexpr auto name = "peered_tsb_container_access_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Str>> b)
        {
            auto bundle     = wire<MakeContainerAccessBundle>(w, a, b);
            auto a_by_index = wire<stdlib::getitem_>(w, bundle, Int{0}).as<TS<Int>>();
            auto b_by_attr  = wire<stdlib::getattr_>(w, bundle, Str{"b"}).as<TS<Str>>();
            auto b_len      = wire<stdlib::len_>(w, b_by_attr).as<TS<Int>>();
            return wire<stdlib::add_>(w, a_by_index, b_len).as<TS<Int>>();
        }
    };

    struct KeyedHandlerResponseProjectionGraph
    {
        static Port<TSD<Int, TS<Int>>> compose(Wiring &w,
                                               Port<TSD<Int, HandlerOutputBundle>> responses)
        {
            return wire<stdlib::getattr_>(w, responses, Str{"response"})
                .as<TSD<Int, TS<Int>>>();
        }
    };

    struct TsbContainerMetadataGraph
    {
        static constexpr auto name = "tsb_container_metadata_graph";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Str>> b)
        {
            auto bundle        = stdlib::to_tsb<ContainerAccessBundle>(w, a, b);
            auto length        = wire<stdlib::len_>(w, bundle).as<TS<Int>>();
            auto empty         = wire<stdlib::is_empty>(w, bundle).as<TS<Bool>>();
            auto length_is_two = wire<stdlib::eq_>(w, length, Int{2}).as<TS<Bool>>();
            auto not_empty     = wire<stdlib::not_>(w, empty).as<TS<Bool>>();
            return wire<stdlib::and_>(w, length_is_two, not_empty).as<TS<Bool>>();
        }
    };

    struct TsbAddGraph
    {
        static constexpr auto name = "tsb_add_graph";

        static Port<NumericTsbBundle> compose(Wiring &w, Port<TS<Int>> a1, Port<TS<Float>> b1,
                                              Port<TS<Int>> a2, Port<TS<Float>> b2)
        {
            auto lhs = stdlib::to_tsb<NumericTsbBundle>(w, a1, b1);
            auto rhs = stdlib::to_tsb<NumericTsbBundle>(w, a2, b2);
            return wire<stdlib::add_>(w, lhs, rhs).as<NumericTsbBundle>();
        }
    };

    struct TsbDivGraph
    {
        static constexpr auto name = "tsb_div_graph";

        static Port<FloatTsbBundle> compose(Wiring &w, Port<TS<Int>> a1, Port<TS<Float>> b1,
                                            Port<TS<Int>> a2, Port<TS<Float>> b2)
        {
            auto lhs = stdlib::to_tsb<NumericTsbBundle>(w, a1, b1);
            auto rhs = stdlib::to_tsb<NumericTsbBundle>(w, a2, b2);
            return wire<stdlib::div_>(w, lhs, rhs).as<FloatTsbBundle>();
        }
    };

    struct TsbUnaryGraph
    {
        static constexpr auto name = "tsb_unary_graph";

        static Port<NumericTsbBundle> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Float>> b)
        {
            auto ts = stdlib::to_tsb<NumericTsbBundle>(w, a, b);
            return wire<stdlib::neg_>(w, ts).as<NumericTsbBundle>();
        }
    };

    struct TsbDedupGraph
    {
        static constexpr auto name = "tsb_dedup_graph";

        static Port<ContainerAccessBundle> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Str>> b)
        {
            auto ts = stdlib::to_tsb<ContainerAccessBundle>(w, a, b);
            return wire<stdlib::dedup>(w, ts).as<ContainerAccessBundle>();
        }
    };

    struct TsbProxyLagGraph
    {
        static constexpr auto name = "tsb_proxy_lag_graph";

        static Port<NumericTsbBundle> compose(Wiring &w, Port<TS<Int>> a,
                                              Port<TS<Float>> b, Port<TS<Bool>> proxy)
        {
            auto ts = stdlib::to_tsb<NumericTsbBundle>(w, a, b);
            return wire<stdlib::lag>(w, ts, Int{2}, proxy).as<NumericTsbBundle>();
        }
    };

    using RebasedDict = TSD<Str, TS<Int>>;
    using RebasedBundle =
        TSB<"RebasedBundle",
            Field<"unit_values", RebasedDict>,
            Field<"target_units", RebasedDict>>;
    using RebasedList = TSL<RebasedDict, std::size_t{2}>;

    struct SelectRebasedPrice
    {
        static constexpr auto name = "select_rebased_price";

        static Port<TS<Int>> compose(Wiring &, Port<TS<Int>>, Port<TS<Int>> price)
        {
            return price;
        }
    };

    struct TsbNestedTsdProxyLagFeedbackGraph
    {
        static constexpr auto name = "tsb_nested_tsd_proxy_lag_feedback_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<RebasedDict> target,
                                     Port<TS<Bool>> rebase, Port<RebasedDict> prices,
                                     Port<TS<Int>> trigger)
        {
            auto state_feedback = stdlib::feedback<RebasedBundle>(w);
            auto empty_values = wire<stdlib::const_, RebasedDict>(
                w, stdlib::make_map<Str, Int>({}));
            auto empty_targets = wire<stdlib::const_, RebasedDict>(
                w, stdlib::make_map<Str, Int>({}));
            auto initial_state =
                stdlib::to_tsb<RebasedBundle>(w, empty_values, empty_targets);
            auto state =
                wire<stdlib::default_>(
                    w,
                    wire<stdlib::lag>(w, state_feedback(), Int{1}, trigger),
                    initial_state)
                    .as<RebasedBundle>();

            auto prior_target =
                wire<stdlib::getitem_>(w, state, Str{"target_units"})
                    .as<RebasedDict>();
            auto target_units =
                wire<stdlib::if_then_else>(w, rebase, target, prior_target)
                    .as<RebasedDict>();
            auto unit_values =
                wire<stdlib::map_>(w, fn<SelectRebasedPrice>(), target_units,
                                   stdlib::no_key(prices))
                    .as<RebasedDict>();
            auto next_state =
                stdlib::to_tsb<RebasedBundle>(w, unit_values, target_units);
            state_feedback(next_state);

            auto prior_values =
                wire<stdlib::getitem_>(w, state, Str{"unit_values"})
                    .as<RebasedDict>();
            auto next =
                wire<stdlib::getitem_>(w, prior_values, Str{"next"}).as<TS<Int>>();
            auto missing = wire<stdlib::const_, TS<Int>>(w, Int{-1});
            return wire<stdlib::sample>(
                       w, trigger, wire<stdlib::default_>(w, next, missing))
                .as<TS<Int>>();
        }
    };

    struct TslNestedTsdProxyLagFeedbackGraph
    {
        static constexpr auto name = "tsl_nested_tsd_proxy_lag_feedback_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<RebasedDict> target,
                                     Port<TS<Bool>> rebase, Port<RebasedDict> prices,
                                     Port<TS<Int>> trigger)
        {
            auto state_feedback = stdlib::feedback<RebasedList>(w);
            auto empty_values = wire<stdlib::const_, RebasedDict>(
                w, stdlib::make_map<Str, Int>({}));
            auto empty_targets = wire<stdlib::const_, RebasedDict>(
                w, stdlib::make_map<Str, Int>({}));
            auto initial_state =
                stdlib::to_tsl<RebasedList>(w, empty_values, empty_targets);
            auto state =
                wire<stdlib::default_>(
                    w,
                    wire<stdlib::lag>(w, state_feedback(), Int{1}, trigger),
                    initial_state)
                    .as<RebasedList>();

            auto target_index = wire<stdlib::const_, TS<Int>>(w, Int{1});
            auto prior_target =
                wire<stdlib::getitem_>(w, state, target_index).as<RebasedDict>();
            auto target_units =
                wire<stdlib::if_then_else>(w, rebase, target, prior_target)
                    .as<RebasedDict>();
            auto unit_values =
                wire<stdlib::map_>(w, fn<SelectRebasedPrice>(), target_units,
                                   stdlib::no_key(prices))
                    .as<RebasedDict>();
            auto next_state =
                stdlib::to_tsl<RebasedList>(w, unit_values, target_units);
            state_feedback(next_state);

            auto values_index = wire<stdlib::const_, TS<Int>>(w, Int{0});
            auto prior_values =
                wire<stdlib::getitem_>(w, state, values_index).as<RebasedDict>();
            auto next =
                wire<stdlib::getitem_>(w, prior_values, Str{"next"}).as<TS<Int>>();
            auto missing = wire<stdlib::const_, TS<Int>>(w, Int{-1});
            return wire<stdlib::sample>(
                       w, trigger, wire<stdlib::default_>(w, next, missing))
                .as<TS<Int>>();
        }
    };

    struct NamedTsbDedupGraph
    {
        static constexpr auto name = "named_tsb_dedup_graph";

        static Port<NamedContainerAccessBundle> compose(
            Wiring &w, Port<TS<Int>> a, Port<TS<Str>> b)
        {
            auto ts = stdlib::to_tsb<NamedContainerAccessBundle>(w, a, b);
            auto out = wire<stdlib::dedup>(w, ts);
            if (out.erased().schema != ts_type<NamedContainerAccessBundle>())
            {
                throw std::logic_error("dedup did not preserve the named TSB schema");
            }
            return out.as<NamedContainerAccessBundle>();
        }
    };

    struct RefNamedTsbDedupGraph
    {
        static constexpr auto name = "ref_named_tsb_dedup_graph";

        static Port<NamedContainerAccessBundle> compose(
            Wiring &w, Port<TS<Int>> a, Port<TS<Str>> b)
        {
            auto ts = stdlib::to_tsb<NamedContainerAccessBundle>(w, a, b);
            auto selected = wire<stdlib::default_>(w, ts, ts);
            if (selected.erased().schema == nullptr ||
                selected.erased().schema->kind != TSTypeKind::REF)
            {
                throw std::logic_error("default did not expose its reference output");
            }
            return wire<stdlib::dedup>(w, selected).as<NamedContainerAccessBundle>();
        }
    };

    struct TsbBitwiseGraph
    {
        static constexpr auto name = "tsb_bitwise_graph";

        static Port<IntTsbBundle> compose(Wiring &w, Port<TS<Int>> a1, Port<TS<Int>> b1,
                                          Port<TS<Int>> a2, Port<TS<Int>> b2)
        {
            auto lhs = stdlib::to_tsb<IntTsbBundle>(w, a1, b1);
            auto rhs = stdlib::to_tsb<IntTsbBundle>(w, a2, b2);
            return wire<stdlib::bit_xor>(w, lhs, rhs).as<IntTsbBundle>();
        }
    };

    struct TsbMeanGraph
    {
        static constexpr auto name = "tsb_mean_graph";

        static Port<FloatTsbBundle> compose(Wiring &w, Port<TS<Int>> a1, Port<TS<Float>> b1,
                                            Port<TS<Int>> a2, Port<TS<Float>> b2)
        {
            auto lhs = stdlib::to_tsb<NumericTsbBundle>(w, a1, b1);
            auto rhs = stdlib::to_tsb<NumericTsbBundle>(w, a2, b2);
            return wire<stdlib::mean>(w, lhs, rhs).as<FloatTsbBundle>();
        }
    };

    struct TsbMinGraph
    {
        static constexpr auto name = "tsb_min_graph";

        static Port<ContainerAccessBundle> compose(Wiring &w, Port<TS<Int>> a1, Port<TS<Str>> b1,
                                                   Port<TS<Int>> a2, Port<TS<Str>> b2)
        {
            auto lhs = stdlib::to_tsb<ContainerAccessBundle>(w, a1, b1);
            auto rhs = stdlib::to_tsb<ContainerAccessBundle>(w, a2, b2);
            return wire<stdlib::min_>(w, lhs, rhs).as<ContainerAccessBundle>();
        }
    };

    struct TsbMaxGraph
    {
        static constexpr auto name = "tsb_max_graph";

        static Port<ContainerAccessBundle> compose(Wiring &w, Port<TS<Int>> a1, Port<TS<Str>> b1,
                                                   Port<TS<Int>> a2, Port<TS<Str>> b2)
        {
            auto lhs = stdlib::to_tsb<ContainerAccessBundle>(w, a1, b1);
            auto rhs = stdlib::to_tsb<ContainerAccessBundle>(w, a2, b2);
            return wire<stdlib::max_>(w, lhs, rhs).as<ContainerAccessBundle>();
        }
    };

    struct RaceGraph
    {
        static constexpr auto name = "race_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            return wire<stdlib::race>(w, lhs, rhs).as<TS<Int>>();
        }
    };

    struct IfTrueRouteGraph
    {
        static constexpr auto name = "if_true_route_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> condition, Port<TS<Int>> ts)
        {
            auto routed = wire<stdlib::if_, IfIntRefBundle>(w, condition, ts).as<IfIntRefBundle>();
            return wire<stdlib::getitem_>(w, routed, Str{"true"}).as<TS<Int>>();
        }
    };

    struct IfFalseRouteGraph
    {
        static constexpr auto name = "if_false_route_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> condition, Port<TS<Int>> ts)
        {
            auto routed = wire<stdlib::if_, IfIntRefBundle>(w, condition, ts).as<IfIntRefBundle>();
            return wire<stdlib::getitem_>(w, routed, Str{"false"}).as<TS<Int>>();
        }
    };

    struct SampleIfTrueRouteGraph
    {
        static constexpr auto name = "sample_if_true_route_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> condition, Port<TS<Int>> ts)
        {
            auto routed = wire<stdlib::if_, IfIntRefBundle>(w, condition, ts).as<IfIntRefBundle>();
            auto signal = wire<stdlib::getitem_>(w, routed, Str{"true"}).as<TS<Int>>();
            auto one = wire<stdlib::const_, TS<Int>>(w, Int{1});
            return wire<stdlib::sample>(w, signal, one).as<TS<Int>>();
        }
    };

    struct IfTrueTsdFilterGraph
    {
        static constexpr auto name = "if_true_tsd_filter_graph";

        static Port<IntTsd> compose(Wiring &w, Port<IntTsd> tsd, Port<TS<Bool>> condition)
        {
            auto routed = wire<stdlib::if_, IfIntTsdRefBundle>(w, condition, tsd).as<IfIntTsdRefBundle>();
            auto branch = wire<stdlib::getitem_>(w, routed, Str{"true"}).as<IntTsd>();
            auto enabled = wire<stdlib::const_, TS<Bool>>(w, Bool{true});
            return wire<stdlib::filter_>(w, enabled, branch).as<IntTsd>();
        }
    };

    struct IfTrueTsdKeySetGraph
    {
        static constexpr auto name = "if_true_tsd_key_set_graph";

        static Port<TSS<Int>> compose(Wiring &w, Port<IntTsd> tsd, Port<TS<Bool>> condition)
        {
            auto routed = wire<stdlib::if_, IfIntTsdRefBundle>(w, condition, tsd).as<IfIntTsdRefBundle>();
            auto branch = wire<stdlib::getitem_>(w, routed, Str{"true"}).as<IntTsd>();
            return wire<stdlib::keys_>(w, branch).as<TSS<Int>>();
        }
    };

    struct CreateInvalidTsdChild
    {
        static constexpr auto name = "create_invalid_tsd_child";

        static void eval(In<"erase", TS<Bool>> erase, Out<IntTsd> out)
        {
            static_cast<void>(out.at(Int{9}));
            if (erase.value()) { static_cast<void>(out.erase(Int{9})); }
        }
    };

    struct InvalidTsdChildUnbindGraph
    {
        static constexpr auto name = "invalid_tsd_child_unbind_graph";

        static Port<IntTsd> compose(Wiring &w, Port<TS<Bool>> erase, Port<TS<Bool>> condition)
        {
            auto source = wire<CreateInvalidTsdChild>(w, erase).as<IntTsd>();
            auto routed = wire<stdlib::if_, IfIntTsdRefBundle>(w, condition, source).as<IfIntTsdRefBundle>();
            auto branch = wire<stdlib::getitem_>(w, routed, Str{"true"}).as<IntTsd>();
            auto enabled = wire<stdlib::const_, TS<Bool>>(w, Bool{true});
            return wire<stdlib::filter_>(w, enabled, branch).as<IntTsd>();
        }
    };

    struct RouteByIndexSlotTwoGraph
    {
        static constexpr auto name = "route_by_index_slot_two_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> index, Port<TS<Int>> ts)
        {
            auto routed = wire<stdlib::route_by_index, RoutedIntRefList>(w, index, ts).as<RoutedIntRefList>();
            return tsl_element(routed, 2).as<TS<Int>>();
        }
    };

    struct DynamicTslGetitemGraph
    {
        static constexpr auto name = "dynamic_tsl_getitem_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<IntTslPair> ts, Port<TS<Int>> key)
        {
            return wire<stdlib::getitem_>(w, ts, key).as<TS<Int>>();
        }
    };

    struct DynamicTsdGetitemGraph
    {
        static constexpr auto name = "dynamic_tsd_getitem_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<IntTsd> ts, Port<TS<Int>> key)
        {
            return wire<stdlib::getitem_>(w, ts, key).as<TS<Int>>();
        }
    };

    struct StructuralTsdActivityProbe
    {
        static constexpr auto name = "structural_tsd_activity_probe";

        static void start(State<Int> invocations) { invocations.set(Int{0}); }

        static void eval(In<"ts", IntTsd, InputActivity::Structural, InputValidity::Unchecked>,
                         State<Int> invocations, Out<TS<Int>> out)
        {
            const Int next = invocations.get() + 1;
            invocations.set(next);
            out.set(next);
        }
    };

    struct StructuralTsdActivityGraph
    {
        static constexpr auto name = "structural_tsd_activity_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<IntTsd> ts)
        {
            return wire<StructuralTsdActivityProbe>(w, ts);
        }
    };

    struct ForwardTsdReference
    {
        static constexpr auto name = "forward_tsd_reference";

        static void eval(In<"ts", IntTsd> ts, Out<REF<IntTsd>> out)
        {
            out.set(ts.base().reference());
        }
    };

    struct StructuralLinkedTsdActivityGraph
    {
        static constexpr auto name = "structural_linked_tsd_activity_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<IntTsd> ts)
        {
            auto linked = wire<ForwardTsdReference>(w, ts).as<IntTsd>();
            return wire<StructuralTsdActivityProbe>(w, linked);
        }
    };

    struct TextToBytesGraph
    {
        static constexpr auto name = "text_to_bytes_graph";

        static Port<TS<Bytes>> compose(Wiring &w, Port<TS<Str>> ts)
        {
            return wire<stdlib::convert, TS<Bytes>>(w, ts);
        }
    };

    struct BytesToTextGraph
    {
        static constexpr auto name = "bytes_to_text_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Bytes>> ts)
        {
            return wire<stdlib::convert, TS<Str>>(w, ts);
        }
    };

    struct AnyNumericRoundTripGraph
    {
        static constexpr auto name = "any_numeric_round_trip_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto boxed = wire<stdlib::convert, TS<AnyValue>>(w, ts);
            return wire<stdlib::convert, TS<Float>>(w, boxed);
        }
    };

    struct AnyDateRoundTripGraph
    {
        static constexpr auto name = "any_date_round_trip_graph";

        static Port<TS<DateTime>> compose(Wiring &w, Port<TS<Date>> ts)
        {
            auto boxed = wire<stdlib::convert, TS<AnyValue>>(w, ts);
            return wire<stdlib::convert, TS<DateTime>>(w, boxed);
        }
    };

    struct TripleValue
    {
        static constexpr auto name = "triple_value";
        [[nodiscard]] static Int apply(Int value) { return value * 3; }
    };

    struct AddValues
    {
        static constexpr auto name = "add_values";
        [[nodiscard]] static Int apply(Int lhs, Int rhs) { return lhs + rhs; }
    };

    struct CaptureValue
    {
        static constexpr auto name = "capture_value";
        inline static Int captured = 0;

        [[nodiscard]] static Int apply(Int value)
        {
            captured = value;
            return value;
        }
    };

    struct SameParity
    {
        static constexpr auto name = "same_parity";
        [[nodiscard]] static Bool apply(Int lhs, Int rhs) { return lhs % 2 == rhs % 2; }
    };

    struct AtLeastThree
    {
        static constexpr auto name = "at_least_three";
        [[nodiscard]] static Bool apply(Int value) { return value >= 3; }
    };

    struct ApplyValueCallableGraph
    {
        static constexpr auto name = "apply_value_callable_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto callable = wire<stdlib::const_, TS<ValueCallable>>(w, value_fn<TripleValue>());
            return wire<stdlib::apply_op, TS<Int>>(w, callable, ts);
        }
    };

    struct CallValueCallableGraph
    {
        static constexpr auto name = "call_value_callable_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto callable = wire<stdlib::const_, TS<ValueCallable>>(w, value_fn<CaptureValue>());
            wire<stdlib::call_op>(w, callable, ts);
            return ts;
        }
    };

    struct ApplyVariadicValueCallableGraph
    {
        static constexpr auto name = "apply_variadic_value_callable_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs,
                                     Port<TS<Int>> rhs)
        {
            auto callable = wire<stdlib::const_, TS<ValueCallable>>(w, value_fn<AddValues>());
            return wire<stdlib::apply_op, TS<Int>>(w, callable, lhs, rhs);
        }
    };

}  // namespace

TEST_CASE("std operators: add_ selects the int implementation for TS<Int> operands")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::add_>(values<Int>(1, 2, 3), values<Int>(10, 20, 30)), values<Int>(11, 22, 33));
}

TEST_CASE("std operators: convert round trips numeric values through native Any")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<AnyNumericRoundTripGraph>(values<Int>(1, -2, 3)),
                 values<Float>(1.0, -2.0, 3.0));
}

TEST_CASE("std operators: convert dispatches from native Any by its contained schema")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<AnyDateRoundTripGraph>(values<Date>(ymd(2024, 1, 2), ymd(2025, 12, 31))),
                 values<DateTime>(DateTime{sys_days{ymd(2024, 1, 2)}},
                                  DateTime{sys_days{ymd(2025, 12, 31)}}));
}

TEST_CASE("std operators: apply invokes a native runtime value callable")
{
    stdlib::register_standard_operators();
    const auto names = OperatorRegistry::instance().registered_names();
    CHECK(std::ranges::find(names, std::string{stdlib::apply_op::name}) != names.end());
    CHECK_OUTPUT(eval_node<ApplyValueCallableGraph>(values<Int>(2, -3, 5)),
                 values<Int>(6, -9, 15));
}

TEST_CASE("std operators: call invokes a native runtime value callable for side effects")
{
    stdlib::register_standard_operators();
    CaptureValue::captured = 0;
    CHECK_OUTPUT(eval_node<CallValueCallableGraph>(values<Int>(2, 7)), values<Int>(2, 7));
    CHECK(CaptureValue::captured == 7);
}

TEST_CASE("std operators: apply packs multiple native runtime callable arguments")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ApplyVariadicValueCallableGraph>(values<Int>(2, -3, 5),
                                                            values<Int>(4, 7, -2)),
                 values<Int>(6, 4, 3));
}

TEST_CASE("std operators: tuple subtraction accepts a native erased comparator")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT((eval_node<stdlib::sub_, TS<HomogeneousTuple<Int>>>(
                     values<Value>(int_tuple({1, 2, 3, 4, 5})), values<Int>(1),
                     value_fn<SameParity>())),
                 values<Value>(int_tuple({2, 4})));
}

TEST_CASE("std operators: len_ visits scalar tuple, set, and map values")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::len_, TS<HomogeneousTuple<Int>>>(
                     values<Value>(int_tuple({1, 2, 3}), int_tuple({4})))),
                 values<Int>(3, 1));
    CHECK_OUTPUT((eval_node<stdlib::len_, TS<Set<Int>>>(
                     values<Value>(int_set({1, 2, 2}), int_set({})))),
                 values<Int>(2, 0));
    CHECK_OUTPUT((eval_node<stdlib::len_, TS<Map<Int, Int>>>(
                     values<Value>(int_map({{1, 10}, {2, 20}}), int_map({{3, 30}})))),
                 values<Int>(2, 1));
}

TEST_CASE("stdlib nonthrowing schema helpers reject malformed compact kinds")
{
    using namespace hgraph;

    ValueTypeMetaData malformed{ValueTypeKind::Atomic, ValueTypeFlags::None, "malformed"};
    malformed.header.kind = static_cast<TypeKind>(ValueTypeKind::Any) + 1;

    REQUIRE(stdlib::arithmetic_impl_detail::container_agg_element_meta<ValueTypeKind::Map>(&malformed) == nullptr);
    REQUIRE(stdlib::collection_impl_detail::nested_map_meta(&malformed) == nullptr);
}

TEST_CASE("std operators: add_ supports mixed numeric operands (int + float -> float)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::add_>(values<Int>(1, 2, 3), values<Float>(0.5, 1.5, 2.5)),
                 values<Float>(1.5, 3.5, 5.5));
}

TEST_CASE("std operators: add_ supports string concatenation")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::add_>(values<Str>(Str{"a"}, Str{"h"}), values<Str>(Str{"b"}, Str{"g"})),
                 values<Str>(Str{"ab"}, Str{"hg"}));
}

TEST_CASE("std operators: add_ supports datetime + timedelta -> datetime")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::add_>(values<DateTime>(dt(1'000'000), dt(2'000'000)),
                                         values<TimeDelta>(microseconds{500'000}, microseconds{1'500'000})),
                 values<DateTime>(dt(1'500'000), dt(3'500'000)));
}

namespace
{
    struct LenOverWindowGraph
    {
        static constexpr auto name = "len_over_window_graph";
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto window = wire<stdlib::to_window>(w, ts, Int{3});
            return wire<stdlib::len_>(w, window).as<TS<Int>>();
        }
    };

    struct LenOverTslOfTssGraph
    {
        static constexpr auto name = "len_over_tsl_of_tss_graph";
        static Port<TS<Int>> compose(Wiring &w, Port<TSS<Int>> a, Port<TSS<Int>> b)
        {
            auto list = stdlib::to_tsl<TSL<TSS<Int>, 2>>(w, a, b);
            return wire<stdlib::len_>(w, list).as<TS<Int>>();
        }
    };
}  // namespace

TEST_CASE("std operators: len_ covers windows and composite-element lists (issue #81)")
{
    stdlib::register_standard_operators();

    // TSW: the current buffer length — grows until the window fills, then
    // stays put (no-change means no tick).
    CHECK_OUTPUT(eval_node<LenOverWindowGraph>(values<Int>(1, 2, 3, 4, 5)),
                 values<Int>(1, 2, 3, none, none));

    // A TSL whose element is composite (TSS children here) has a size too:
    // len_tsl accepts any element kind.
    CHECK_OUTPUT(eval_node<LenOverTslOfTssGraph>(values<Value>(set_delta<Int>({1}, {})),
                                                 values<Value>(set_delta<Int>({2}, {}))),
                 values<Int>(2));
}

TEST_CASE("std operators: timedelta and datetime attribute operators (issue #82)")
{
    stdlib::register_standard_operators();

    // Python's normalized decomposition: -1 day + 1s + 5us has days=-1,
    // seconds=1, microseconds=5; total_seconds is the exact float.
    const TimeDelta negative = duration_cast<TimeDelta>(days{-1}) + seconds{1} + microseconds{5};
    CHECK_OUTPUT(eval_node<stdlib::days>(values<TimeDelta>(negative)), values<Int>(-1));
    CHECK_OUTPUT(eval_node<stdlib::seconds>(values<TimeDelta>(negative)), values<Int>(1));
    CHECK_OUTPUT(eval_node<stdlib::microseconds>(values<TimeDelta>(negative)), values<Int>(5));
    CHECK_OUTPUT(eval_node<stdlib::total_seconds>(
                     values<TimeDelta>(duration_cast<TimeDelta>(days{1}) + seconds{30})),
                 values<Float>(86430.0));

    // 2026-07-27T13:05:09.123456 is a Monday.
    const DateTime stamp = DateTime{sys_days{ymd(2026, 7, 27)}} + hours{13} + minutes{5} +
                           seconds{9} + microseconds{123'456};
    CHECK_OUTPUT(eval_node<stdlib::year>(values<DateTime>(stamp)), values<Int>(2026));
    CHECK_OUTPUT(eval_node<stdlib::month>(values<DateTime>(stamp)), values<Int>(7));
    CHECK_OUTPUT(eval_node<stdlib::day>(values<DateTime>(stamp)), values<Int>(27));
    CHECK_OUTPUT(eval_node<stdlib::hour>(values<DateTime>(stamp)), values<Int>(13));
    CHECK_OUTPUT(eval_node<stdlib::minute>(values<DateTime>(stamp)), values<Int>(5));
    CHECK_OUTPUT(eval_node<stdlib::second>(values<DateTime>(stamp)), values<Int>(9));
    CHECK_OUTPUT(eval_node<stdlib::microsecond>(values<DateTime>(stamp)), values<Int>(123'456));
    CHECK_OUTPUT(eval_node<stdlib::weekday>(values<DateTime>(stamp)), values<Int>(0));
    CHECK_OUTPUT(eval_node<stdlib::isoweekday>(values<DateTime>(stamp)), values<Int>(1));
    // timestamp(): FRACTIONAL epoch seconds (python parity) — the half
    // second survives, before and after the epoch.
    CHECK_OUTPUT(eval_node<stdlib::timestamp>(values<DateTime>(DateTime{microseconds{500'000}})),
                 values<Float>(0.5));
    CHECK_OUTPUT(eval_node<stdlib::timestamp>(values<DateTime>(DateTime{microseconds{-500'000}})),
                 values<Float>(-0.5));
    CHECK_OUTPUT(eval_node<stdlib::timestamp>(values<DateTime>(stamp)),
                 values<Float>(std::chrono::duration<Float>(stamp.time_since_epoch()).count()));

    CHECK_OUTPUT(eval_node<stdlib::hour>(values<Time>(time_of_day(13, 5, 9))), values<Int>(13));
    CHECK_OUTPUT(eval_node<stdlib::minute>(values<Time>(time_of_day(13, 5, 9))), values<Int>(5));
    CHECK_OUTPUT(eval_node<stdlib::second>(values<Time>(time_of_day(13, 5, 9))), values<Int>(9));
}

TEST_CASE("std operators: date and timedelta arithmetic matches Python normalized days")
{
    stdlib::register_standard_operators();
    const TimeDelta two_days  = duration_cast<TimeDelta>(days{2});
    const TimeDelta five_days = duration_cast<TimeDelta>(days{5});

    CHECK_OUTPUT(eval_node<stdlib::add_>(values<Date>(ymd(2020, 1, 1), ymd(2020, 1, 10)),
                                         values<TimeDelta>(two_days, five_days)),
                 values<Date>(ymd(2020, 1, 3), ymd(2020, 1, 15)));

    CHECK_OUTPUT(
        eval_node<stdlib::add_>(
            values<Date>(ymd(2020, 1, 2), ymd(2020, 1, 2)),
            values<TimeDelta>(hours{23}, microseconds{-1})),
        values<Date>(ymd(2020, 1, 2), ymd(2020, 1, 1)));
    CHECK_OUTPUT(
        eval_node<stdlib::sub_>(
            values<Date>(ymd(2020, 1, 2), ymd(2020, 1, 2)),
            values<TimeDelta>(hours{23}, microseconds{-1})),
        values<Date>(ymd(2020, 1, 2), ymd(2020, 1, 3)));
}

TEST_CASE("std operators: div_ produces a different result type (int / int -> float)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::div_>(values<Int>(7, 9), values<Int>(2, 3)), values<Float>(3.5, 3.0));
}

TEST_CASE("std operators: sub_ of two datetimes yields a timedelta (result differs from both operands)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::sub_>(values<DateTime>(dt(3'000'000), dt(5'000'000)),
                                         values<DateTime>(dt(1'000'000), dt(2'000'000))),
                 values<TimeDelta>(microseconds{2'000'000}, microseconds{3'000'000}));
}

TEST_CASE("std operators: sub_ supports mixed numeric operands")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::sub_>(values<Int>(5, 7), values<Float>(0.5, 2.25)),
                 values<Float>(4.5, 4.75));
}

TEST_CASE("std operators: mul_ supports numeric operands and string repetition")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::mul_>(values<Int>(2, 3), values<Int>(4, 5)), values<Int>(8, 15));
    CHECK_OUTPUT(eval_node<stdlib::mul_>(values<Int>(2, 3), values<Float>(0.5, 1.5)), values<Float>(1.0, 4.5));
    CHECK_OUTPUT(eval_node<stdlib::mul_>(values<Str>(Str{"a"}, Str{"bc"}), values<Int>(3, 2)),
                 values<Str>(Str{"aaa"}, Str{"bcbc"}));
}

TEST_CASE("std operators: eq_ resolves its TS<Bool> output independently of the operand type")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::eq_>(values<Int>(1, 2, 3), values<Int>(1, 5, 3)),
                 values<Bool>(true, false, true));
}

TEST_CASE("std operators: comparison operators support ordering and cmp_")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::ne_>(values<Int>(1, 2), values<Int>(1, 3)), values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<stdlib::lt_>(values<Int>(1, 5), values<Float>(2.0, 4.0)), values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<stdlib::ge_>(values<Str>(Str{"b"}, Str{"a"}), values<Str>(Str{"a"}, Str{"a"})),
                 values<Bool>(true, true));
    CHECK_OUTPUT(eval_node<stdlib::cmp_>(values<Int>(1, 2, 3), values<Int>(2, 2, 1)),
                 values<stdlib::CmpResult>(stdlib::CmpResult::LT, stdlib::CmpResult::EQ, stdlib::CmpResult::GT));
}

TEST_CASE("std operators: min_ and max_ support binary scalar operands")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::min_>(values<Int>(3, 1), values<Int>(2, 5)), values<Int>(2, 1));
    CHECK_OUTPUT(eval_node<stdlib::max_>(values<Int>(3, 1), values<Float>(2.5, 5.5)), values<Float>(3.0, 5.5));
    CHECK_OUTPUT(eval_node<stdlib::min_>(values<Str>(Str{"b"}, Str{"a"}), values<Str>(Str{"a"}, Str{"c"})),
                 values<Str>(Str{"a"}, Str{"a"}));
    CHECK_OUTPUT(eval_node<stdlib::max_>(values<Date>(ymd(2020, 1, 1), ymd(2020, 1, 10)),
                                         values<Date>(ymd(2020, 1, 3), ymd(2020, 1, 5))),
                 values<Date>(ymd(2020, 1, 3), ymd(2020, 1, 10)));
}

TEST_CASE("std operators: scalar container aggregate overloads resolve by kind and element type")
{
    const auto types = stdlib::register_standard_types();
    stdlib::register_standard_operators();

    auto &registry = TypeRegistry::instance();

    CHECK_OUTPUT((eval_runtime_schema_graph<ScalarContainerMinGraph, Int>(
                     registry.ts(registry.set(types.int_type)),
                     values<Value>(stdlib::make_set<Int>({}),
                                   stdlib::make_set<Int>({Int{1}, Int{2}, Int{-1}})))),
                 values<Int>(none, Int{-1}));

    CHECK_OUTPUT((eval_runtime_schema_graph<ScalarContainerSumGraph, Int>(
                     registry.ts(registry.map(types.str_type, types.int_type)),
                     values<Value>(stdlib::make_map<Str, Int>({}),
                                   stdlib::make_map<Str, Int>({{Str{"a"}, Int{1}}, {Str{"b"}, Int{2}}})))),
                 values<Int>(Int{0}, Int{3}));

    const auto mean_out = eval_runtime_schema_graph<ScalarContainerMeanGraph, Float>(
        registry.ts(registry.list(types.int_type)),
        values<Value>(stdlib::make_list<Int>({}), stdlib::make_list<Int>({Int{1}, Int{2}})));
    REQUIRE(mean_out.size() == 2);
    REQUIRE(mean_out[0].has_value());
    CHECK(std::isnan(*mean_out[0]));
    REQUIRE(mean_out[1].has_value());
    CHECK(*mean_out[1] == Float{1.5});

    CHECK_OUTPUT((eval_runtime_schema_graph<ScalarContainerStdGraph, Float>(
                     registry.ts(registry.set(types.float_type)),
                     values<Value>(stdlib::make_set<Float>({}),
                                   stdlib::make_set<Float>({Float{1.0}}),
                                   stdlib::make_set<Float>({Float{1.0}, Float{2.0}})))),
                 values<Float>(Float{0.0}, Float{0.0}, std::sqrt(Float{0.5})));
}

TEST_CASE("std operators: eq_ works for strings")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::eq_>(values<Str>(Str{"x"}, Str{"y"}), values<Str>(Str{"x"}, Str{"z"})),
                 values<Bool>(true, false));
}

TEST_CASE("std operators: eq_ uses epsilon for float-involved comparisons")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::eq_>(values<Float>(1.0, 1.0), values<Float>(1.0 + 1e-11, 1.0 + 1e-5)),
                 values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<stdlib::eq_>(values<Int>(1, 1), values<Float>(1.0 + 1e-11, 1.0 + 1e-5)),
                 values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<stdlib::eq_>(values<Float>(1.0 + 1e-5), values<Int>(1), arg<"epsilon">(Float{1e-4})),
                 values<Bool>(true));
}

TEST_CASE("std operators: zero_ emits the op-aware zero for standard scalar outputs")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::zero_, TS<Int>>(fn<stdlib::add_>())), values<Int>(0));
    CHECK_OUTPUT((eval_node<stdlib::zero_, TS<Int>>(fn<stdlib::mul_>())), values<Int>(1));
    CHECK_OUTPUT((eval_node<stdlib::zero_, TS<Int>>(fn<stdlib::min_>())),
                 values<Int>(std::numeric_limits<Int>::max()));
    CHECK_OUTPUT((eval_node<stdlib::zero_, TS<Float>>(fn<stdlib::add_>())), values<Float>(Float{0}));
    CHECK_OUTPUT((eval_node<stdlib::zero_, TS<Float>>(fn<stdlib::max_>())),
                 values<Float>(-std::numeric_limits<Float>::infinity()));
    CHECK_OUTPUT((eval_node<stdlib::zero_, TS<Str>>(fn<stdlib::add_>())), values<Str>(Str{}));
}

TEST_CASE("std operators: default_ substitutes the default until ts first ticks")
{
    stdlib::register_standard_operators();

    // ts invalid for two cycles: the default (9) holds, then ts takes over.
    CHECK_OUTPUT(eval_node<stdlib::default_>(values<Int>(none, none, 3, 4), values<Int>(9)),
                 values<Int>(9, none, 3, 4));

    // ts valid from the first cycle: the default never shows.
    CHECK_OUTPUT(eval_node<stdlib::default_>(values<Int>(1, 2), values<Int>(9)), values<Int>(1, 2));
}

TEST_CASE("std operators: syntax sugar wires arithmetic expressions through standard overloads")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<SyntaxArithmeticGraph>(values<Int>(1, 2, 3), values<Int>(10, 20, 30)),
                 values<Int>(21, 42, 63));
}

TEST_CASE("std operators: syntax sugar composes comparisons and logical operators")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<SyntaxComparisonGraph>(values<Int>(1, 0, 5), values<Float>(2.0, -1.0, 4.0)),
                 values<Bool>(true, false, true));
}

TEST_CASE("std operators: syntax helpers cover non-overloadable arithmetic operators")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<SyntaxNamedHelperGraph>(values<Int>(-2, 3)), values<Float>(2.0, 4.5));
}

TEST_CASE("std operators: syntax port cast validates the resolved runtime schema")
{
    stdlib::register_standard_operators();
    REQUIRE_THROWS_AS(eval_node<SyntaxBadCastGraph>(values<Int>(7), values<Int>(2)), std::logic_error);
}

TEST_CASE("std operators: floordiv_ and mod_ use floor semantics")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::floordiv_>(values<Int>(7, -7), values<Int>(3, 3)), values<Int>(2, -3));
    CHECK_OUTPUT(eval_node<stdlib::mod_>(values<Int>(7, -7), values<Int>(3, 3)), values<Int>(1, 2));
    CHECK_OUTPUT(eval_node<stdlib::floordiv_>(values<Float>(7.5, -7.5), values<Int>(2, 2)), values<Float>(3.0, -4.0));
}

TEST_CASE("std operators: divmod_ returns quotient and remainder as a two-element list")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::divmod_>(values<Int>(5, -7), values<Int>(2, 3)),
                 values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 1}}),
                               list_delta<TS<Int>>({{0, -3}, {1, 2}})));
    CHECK_OUTPUT(eval_node<stdlib::divmod_>(values<Float>(5.0), values<Int>(2)),
                 values<Value>(list_delta<TS<Float>>({{0, 2.0}, {1, 1.0}})));
    CHECK_OUTPUT(eval_node<stdlib::divmod_>(values<Int>(5), values<Float>(2.0)),
                 values<Value>(list_delta<TS<Float>>({{0, 2.0}, {1, 1.0}})));
}

TEST_CASE("std operators: pow_ preserves homogeneous integer results")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::pow_>(values<Int>(2, 9), values<Int>(3, 2)), values<Int>(8, 81));
    CHECK_OUTPUT(eval_node<stdlib::pow_>(values<Int>(2, 3), Int{3}), values<Int>(8, 27));
    CHECK_OUTPUT(eval_node<stdlib::pow_>(Int{2}, values<Int>(3, 4)), values<Int>(8, 16));
    CHECK_OUTPUT(eval_node<stdlib::pow_>(values<Float>(4.0, 9.0), values<Float>(0.5, 0.5)), values<Float>(2.0, 3.0));
    CHECK_OUTPUT(eval_node<stdlib::pow_>(values<Int>(4), values<Float>(0.5)), values<Float>(2.0));
}

TEST_CASE("std operators: pow_ takes divide-by-zero policy for zero raised to a negative power")
{
    using DBZ = stdlib::DivideByZero;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::pow_>(values<Float>(0.0, 2.0), values<Int>(-1, 3), DBZ::Inf),
                 values<Float>(std::numeric_limits<Float>::infinity(), 8.0));
    CHECK_OUTPUT(eval_node<stdlib::pow_>(values<Float>(0.0, 2.0), values<Int>(-1, 3), DBZ::NoTick),
                 values<Float>(none, 8.0));
    CHECK_OUTPUT(eval_node<stdlib::pow_>(values<Float>(0.0), values<Int>(-1), DBZ::Zero), values<Float>(0.0));
    CHECK_OUTPUT(eval_node<stdlib::pow_>(values<Float>(0.0), values<Int>(-1), DBZ::One), values<Float>(1.0));
    CHECK_OUTPUT(eval_node<stdlib::pow_>(values<Int>(0), values<Int>(-1), DBZ::NoTick), values<Int>(none));
    REQUIRE_THROWS(eval_node<stdlib::pow_>(values<Float>(0.0), values<Int>(-1)));
    REQUIRE_THROWS(eval_node<stdlib::pow_>(values<Int>(2), values<Int>(-1)));
}

TEST_CASE("std operators: unary numeric operators support neg pos abs sign and ln")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::neg_>(values<Int>(1, -2)), values<Int>(-1, 2));
    CHECK_OUTPUT(eval_node<stdlib::pos_>(values<Float>(-1.5, 2.5)), values<Float>(-1.5, 2.5));
    CHECK_OUTPUT(eval_node<stdlib::abs_>(values<Int>(-3, 4)), values<Int>(3, 4));
    CHECK_OUTPUT(eval_node<stdlib::sign>(values<Int>(-3, 0, 4)), values<Int>(-1, 1, 1));
    CHECK_OUTPUT(eval_node<stdlib::sign>(values<Float>(-3.5, -0.0, 4.25)), values<Float>(-1.0, 1.0, 1.0));
    CHECK_OUTPUT(eval_node<stdlib::ln>(values<Float>(1.0, std::numbers::e)), values<Float>(0.0, 1.0));
}

TEST_CASE("std operators: fixed TSL arithmetic maps elementwise")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::add_, TSL<TS<Int>, 4>, TSL<TS<Int>, 4>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {2, 2}, {3, 5}})),
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 3}, {3, 6}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 3}, {3, 11}})));
    CHECK_OUTPUT((eval_node<stdlib::sub_, TSL<TS<Int>, 2>, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})),
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 3}})))),
                 values<Value>(list_delta<TS<Int>>({{0, -1}, {1, -1}})));
    CHECK_OUTPUT((eval_node<stdlib::mul_, TSL<TS<Int>, 2>, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})),
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 3}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 6}})));
    CHECK_OUTPUT((eval_node<stdlib::div_, TSL<TS<Float>, 2>, TSL<TS<Float>, 2>>(
                     values<Value>(list_delta<TS<Float>>({{0, 1.0}, {1, 2.0}})),
                     values<Value>(list_delta<TS<Float>>({{0, 2.0}, {1, 1.0}})))),
                 values<Value>(list_delta<TS<Float>>({{0, 0.5}, {1, 2.0}})));
    CHECK_OUTPUT((eval_node<stdlib::floordiv_, TSL<TS<Int>, 3>, TSL<TS<Int>, 3>>(
                     values<Value>(list_delta<TS<Int>>({{0, 3}, {1, 2}, {2, 100}})),
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 2}, {2, 10}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 1}, {2, 10}})));
    CHECK_OUTPUT((eval_node<stdlib::mod_, TSL<TS<Int>, 3>, TSL<TS<Int>, 3>>(
                     values<Value>(list_delta<TS<Int>>({{0, 3}, {1, 2}, {2, 105}})),
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 2}, {2, 10}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 0}, {2, 5}})));
    CHECK_OUTPUT((eval_node<stdlib::pow_, TSL<TS<Int>, 3>, TSL<TS<Int>, 3>>(
                     values<Value>(list_delta<TS<Int>>({{0, 3}, {1, 4}, {2, 5}})),
                     values<Value>(list_delta<TS<Int>>({{0, 3}, {1, 0}, {2, 1}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 27}, {1, 1}, {2, 5}})));
}

TEST_CASE("std operators: fixed TSL arithmetic broadcasts scalar time-series")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::add_, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})), values<Int>(2))),
                 values<Value>(list_delta<TS<Int>>({{0, 3}, {1, 4}})));
    CHECK_OUTPUT((eval_node<stdlib::sub_, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})), values<Int>(2))),
                 values<Value>(list_delta<TS<Int>>({{0, -1}, {1, 0}})));
    CHECK_OUTPUT((eval_node<stdlib::sub_, TSL<TS<Int>, 2>>(
                     values<Int>(10), values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 9}, {1, 8}})));
    CHECK_OUTPUT((eval_node<stdlib::mul_, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})), values<Int>(2))),
                 values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 4}})));
    CHECK_OUTPUT((eval_node<stdlib::div_, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})), values<Int>(2))),
                 values<Value>(list_delta<TS<Float>>({{0, 0.5}, {1, 1.0}})));
    CHECK_OUTPUT((eval_node<stdlib::floordiv_, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})), values<Int>(2))),
                 values<Value>(list_delta<TS<Int>>({{0, 0}, {1, 1}})));
    CHECK_OUTPUT((eval_node<stdlib::mod_, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 3}, {1, 2}})), values<Int>(2))),
                 values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 0}})));
    CHECK_OUTPUT((eval_node<stdlib::pow_, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 3}, {1, 2}})), values<Int>(2))),
                 values<Value>(list_delta<TS<Int>>({{0, 9}, {1, 4}})));
    CHECK_OUTPUT((eval_node<stdlib::div_, TSL<TS<Int>, 2>>(
                     values<Int>(12), values<Value>(list_delta<TS<Int>>({{0, 3}, {1, 4}})))),
                 values<Value>(list_delta<TS<Float>>({{0, 4.0}, {1, 3.0}})));
}

TEST_CASE("std operators: fixed TSL unary arithmetic maps elementwise")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::neg_, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})))),
                 values<Value>(list_delta<TS<Int>>({{0, -1}, {1, -2}})));
    CHECK_OUTPUT((eval_node<stdlib::pos_, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, -2}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 1}, {1, -2}})));
    CHECK_OUTPUT((eval_node<stdlib::abs_, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, -2}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})));
}

TEST_CASE("std operators: logical and bitwise operators support standard scalars")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::not_>(values<Bool>(true, false)), values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<stdlib::and_>(values<Int>(1, 0), values<Int>(2, 3)), values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<stdlib::or_>(values<Str>(Str{}, Str{"x"}), values<Str>(Str{}, Str{})),
                 values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<stdlib::bit_and>(values<Int>(6, 5), values<Int>(3, 1)), values<Int>(2, 1));
    CHECK_OUTPUT(eval_node<stdlib::bit_or>(values<Bool>(true, false), values<Bool>(false, false)),
                 values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<stdlib::invert_>(values<Int>(0, 1)), values<Int>(~Int{0}, ~Int{1}));
    CHECK_OUTPUT(eval_node<stdlib::invert_>(values<Bool>(true, false)), values<Int>(-2, -1));
    CHECK_OUTPUT(eval_node<stdlib::lshift_>(values<Int>(1, 2), values<Int>(3, 2)), values<Int>(8, 8));
    CHECK_OUTPUT(eval_node<stdlib::rshift_>(values<Int>(8, 9), values<Int>(1, 2)), values<Int>(4, 2));
}

TEST_CASE("std operators: fixed TSL bitwise operators map elementwise")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::invert_, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, -2}})))),
                 values<Value>(list_delta<TS<Int>>({{0, -2}, {1, 1}})));
    CHECK_OUTPUT((eval_node<stdlib::bit_and, TSL<TS<Int>, 3>, TSL<TS<Int>, 3>>(
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 8}, {2, 7}})),
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 9}, {2, 5}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 0}, {1, 8}, {2, 5}})));
    CHECK_OUTPUT((eval_node<stdlib::bit_or, TSL<TS<Int>, 3>, TSL<TS<Int>, 3>>(
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 8}, {2, 7}})),
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 9}, {2, 5}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 3}, {1, 9}, {2, 7}})));
    CHECK_OUTPUT((eval_node<stdlib::bit_xor, TSL<TS<Int>, 3>, TSL<TS<Int>, 3>>(
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 8}, {2, 7}})),
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 9}, {2, 5}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 3}, {1, 1}, {2, 2}})));
    CHECK_OUTPUT((eval_node<stdlib::lshift_, TSL<TS<Int>, 3>, TSL<TS<Int>, 3>>(
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 10}, {2, 8}})),
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}, {2, 3}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 4}, {1, 40}, {2, 64}})));
    CHECK_OUTPUT((eval_node<stdlib::rshift_, TSL<TS<Int>, 3>, TSL<TS<Int>, 3>>(
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 10}, {2, 1024}})),
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}, {2, 3}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}, {2, 128}})));
    CHECK_OUTPUT((eval_node<stdlib::bit_and, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 3}, {1, 6}})), values<Int>(2))),
                 values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 2}})));
    CHECK_OUTPUT((eval_node<stdlib::lshift_, TSL<TS<Int>, 2>>(
                     values<Int>(1), values<Value>(list_delta<TS<Int>>({{0, 3}, {1, 4}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 8}, {1, 16}})));
}

TEST_CASE("std operators: fixed TSL comparisons and extrema match Python contracts")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::eq_, TSL<TS<Int>, 2>, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})),
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 3}})))),
                 values<Bool>(false));
    CHECK_OUTPUT((eval_node<stdlib::eq_, TSL<TS<Float>, 2>, TSL<TS<Float>, 2>>(
                     values<Value>(list_delta<TS<Float>>({{0, 1.0}, {1, 2.0}})),
                     values<Value>(list_delta<TS<Float>>({{0, 1.0 + 1e-11}, {1, 2.0}})))),
                 values<Bool>(true));
    CHECK_OUTPUT((eval_node<stdlib::ne_, TSL<TS<Int>, 2>, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})),
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})))),
                 values<Bool>(false));
    CHECK_OUTPUT((eval_node<stdlib::min_, TSL<TS<Int>, 2>, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})),
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 3}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})));
    CHECK_OUTPUT((eval_node<stdlib::max_, TSL<TS<Float>, 2>, TSL<TS<Float>, 2>>(
                     values<Value>(list_delta<TS<Float>>({{0, 1.0}, {1, 2.0}})),
                     values<Value>(list_delta<TS<Float>>({{0, 2.0}, {1, 3.0}})))),
                 values<Value>(list_delta<TS<Float>>({{0, 2.0}, {1, 3.0}})));
}

TEST_CASE("std operators: fixed TSL binary analytics map elementwise")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::sum_, TSL<TS<Int>, 2>, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})),
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 3}})))),
                 values<Value>(list_delta<TS<Int>>({{0, 3}, {1, 5}})));
    CHECK_OUTPUT((eval_node<stdlib::mean, TSL<TS<Int>, 2>, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})),
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 3}})))),
                 values<Value>(list_delta<TS<Float>>({{0, 1.5}, {1, 2.5}})));
    CHECK_OUTPUT((eval_node<stdlib::var_, TSL<TS<Float>, 2>, TSL<TS<Float>, 2>>(
                     values<Value>(list_delta<TS<Float>>({{0, 1.0}, {1, 2.0}})),
                     values<Value>(list_delta<TS<Float>>({{0, 2.0}, {1, 3.0}})))),
                 values<Value>(list_delta<TS<Float>>({{0, 0.5}, {1, 0.5}})));
    CHECK_OUTPUT((eval_node<stdlib::std_, TSL<TS<Int>, 2>, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})),
                     values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 3}})))),
                 values<Value>(list_delta<TS<Float>>({{0, std::sqrt(0.5)}, {1, std::sqrt(0.5)}})));
}

TEST_CASE("std operators: string operators support replace substr and container basics")
{
    stdlib::register_standard_operators();

    // hgraph shape: TSB{is_match: TS[bool], groups: TS[tuple[str, ...]]}.
    const auto match_delta = [](Bool flag, std::optional<std::vector<Str>> groups) {
        const auto *schema = stdlib::match_impl::result_schema();
        BundleBuilder builder{ValuePlanFactory::instance().type_for(schema->delta_value_schema)};
        builder.set("is_match", Value{flag});
        if (groups.has_value())
        {
            const auto *groups_meta = schema->fields()[1].type->value_schema;
            ListBuilder items{ValuePlanFactory::instance().type_for(groups_meta->element_type)};
            for (const Str &group : *groups) { items.push_back(group); }
            builder.set("groups", items.build());
        }
        return builder.build();
    };
    // a match with no capture groups still writes groups = () (hgraph shape)
    CHECK_OUTPUT(eval_node<stdlib::match_>(values<Str>(Str{"a"}), values<Str>(Str{"a"})),
                 values<Value>(match_delta(true, std::vector<Str>{})));
    CHECK_OUTPUT(eval_node<stdlib::match_>(values<Str>(Str{"(a)"}), values<Str>(Str{"aa"})),
                 values<Value>(match_delta(true, std::vector<Str>{Str{"a"}})));
    CHECK_OUTPUT(eval_node<stdlib::match_>(values<Str>(Str{"a"}), values<Str>(Str{"b"})),
                 values<Value>(match_delta(false, std::nullopt)));
    CHECK_OUTPUT(eval_node<stdlib::replace>(values<Str>(Str{"a"}, Str{"^a"}),
                                            values<Str>(Str{"z"}, Str{"z"}),
                                            values<Str>(Str{"abcabcabc"}, Str{"abcabcabc"})),
                 values<Str>(Str{"zbczbczbc"}, Str{"zbcabcabc"}));
    CHECK_OUTPUT(eval_node<stdlib::substr>(values<Str>(Str{"abcdef"}, Str{"abcdef"}, Str{"abcdef"}),
                                           values<Int>(0, 2, 1),
                                           values<Int>(3, 4, 5)),
                 values<Str>(Str{"abc"}, Str{"cd"}, Str{"bcde"}));
    CHECK_OUTPUT(eval_node<stdlib::contains_>(values<Str>(Str{"abc"}, none, Str{}),
                                              values<Str>(Str{"z"}, Str{"bc"}, Str{})),
                 values<Bool>(false, true, true));
    CHECK_OUTPUT(eval_node<stdlib::len_>(values<Str>(Str{}, Str{"abc"})), values<Int>(0, 3));
    CHECK_OUTPUT(eval_node<stdlib::is_empty>(values<Str>(Str{}, Str{"abc"})), values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<stdlib::getitem_>(values<Str>(Str{"abc"}, Str{"abc"}), values<Int>(1, -1)),
                 values<Str>(Str{"b"}, Str{"c"}));

    CHECK_OUTPUT(eval_node<SplitToPairGraph>(values<Str>(Str{"a,b,c"})),
                 values<Value>(list_delta<TS<Str>>({{0, Str{"a"}}, {1, Str{"b,c"}}})));

    WiringArg split_source;
    split_source.kind        = WiringArg::Kind::TimeSeries;
    split_source.port.schema = ts_type<TS<Str>>();

    WiringArg split_separator;
    split_separator.kind         = WiringArg::Kind::Scalar;
    split_separator.scalar_value = Value{Str{","}};
    split_separator.scalar_meta  = split_separator.scalar_value.schema();

    std::array<WiringArg, 2> unresolved_split_args{split_source, split_separator};
    // hgraph parity: split WITHOUT an expected type resolves to the default
    // TS[tuple[str, ...]] shape (all splits).
    {
        ResolvedOperatorCall default_split = OperatorRegistry::instance().resolve(
            "split", std::span<const WiringArg>{unresolved_split_args.data(), unresolved_split_args.size()}, true);
        const auto *out = default_split.map.find_ts("O");
        REQUIRE(out != nullptr);
        CHECK(out->value_schema->value_kind() == ValueTypeKind::List);
        CHECK(out->value_schema->has(ValueTypeFlags::VariadicTuple));
    }

    ResolvedOperatorCall resolved_split = OperatorRegistry::instance().resolve(
        "split", std::span<const WiringArg>{unresolved_split_args.data(), unresolved_split_args.size()}, true,
        ts_type<TSL<TS<Str>, 2>>());
    REQUIRE(resolved_split.map.find_size("N").has_value());
    CHECK(*resolved_split.map.find_size("N") == 2);
    CHECK(ts_pattern_resolve(resolved_split.impl->output, resolved_split.map) == ts_type<TSL<TS<Str>, 2>>());

    CHECK_OUTPUT(eval_node<JoinDefaultGraph>(values<Value>(list_delta<TS<Str>>({{0, Str{"a"}}, {2, Str{"c"}}}),
                                                           list_delta<TS<Str>>({{1, Str{"b"}}}))),
                 values<Str>(Str{"a,c"}, Str{"a,b,c"}));
    CHECK_OUTPUT(eval_node<JoinStrictGraph>(values<Value>(list_delta<TS<Str>>({{0, Str{"a"}}, {2, Str{"c"}}}),
                                                          list_delta<TS<Str>>({{1, Str{"b"}}}))),
                 values<Str>(none, Str{"a,b,c"}));

    CHECK_OUTPUT(eval_node<FormatArgsGraph>(values<Str>(Str{"{} is a test {}"}, none),
                                            values<Int>(1, 2),
                                            values<Str>(Str{"a"}, Str{"b"})),
                 values<Str>(Str{"1 is a test a"}, Str{"2 is a test b"}));
    CHECK_OUTPUT(eval_node<FormatNoArgsGraph>(values<Str>(Str{"plain"}, Str{"escaped {{brace}}"})),
                 values<Str>(Str{"plain"}, Str{"escaped {brace}"}));
    CHECK_OUTPUT(eval_node<FormatBoolGraph>(values<Bool>(true, false)),
                 values<Str>(Str{"value=True"}, Str{"value=False"}));
    CHECK_OUTPUT(eval_node<FormatKwargsGraph>(values<Str>(Str{"{ts1} is a test {ts2}"}, none),
                                              values<Int>(1, 2),
                                              values<Str>(Str{"a"}, Str{"b"})),
                 values<Str>(Str{"1 is a test a"}, Str{"2 is a test b"}));
    CHECK_OUTPUT(eval_node<FormatMixedGraph>(values<Str>(Str{"{ts1} is a test {ts2}"}, none),
                                             values<Float>(1.1, 1.2),
                                             values<Int>(1, 2),
                                             values<Str>(Str{"a"}, Str{"b"})),
                 values<Str>(Str{"1 is a test a"}, Str{"2 is a test b"}));
    CHECK_OUTPUT(eval_node<FormatSampledGraph>(values<Str>(Str{"{} is a test {}"}, none, none, none),
                                               values<Int>(1, 2, 3, 4),
                                               values<Str>(Str{"a"}, Str{"b"}, Str{"c"}, Str{"d"})),
                 values<Str>(none, none, Str{"3 is a test c"}, none));
}

TEST_CASE("std operators: collection container operators support TSS TSD and fixed TSL")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::len_, TSS<Int>>(values<Value>(set_delta<Int>({}, {}),
                                                                  set_delta<Int>({1}, {}),
                                                                  set_delta<Int>({2, 3}, {}),
                                                                  set_delta<Int>({}, {1})))),
                 values<Int>(0, 1, 3, 2));
    CHECK_OUTPUT((eval_node<stdlib::is_empty, TSS<Int>>(values<Value>(none,
                                                                      set_delta<Int>({1}, {}),
                                                                      set_delta<Int>({2}, {}),
                                                                      set_delta<Int>({}, {1}),
                                                                      set_delta<Int>({}, {2})))),
                 values<Bool>(true, false, none, none, true));
    CHECK_OUTPUT((eval_node<stdlib::contains_, TSS<Int>>(values<Value>(set_delta<Int>({1, 2, 3}, {})),
                                                         values<Int>(1, 4))),
                 values<Bool>(true, false));
    CHECK_OUTPUT((eval_node<stdlib::contains_, TSS<Int>>(
                     values<Value>(none, set_delta<Int>({1}, {})),
                     values<Int>(1, 1))),
                 values<Bool>(false, true));
    CHECK_OUTPUT((eval_node<stdlib::contains_, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({1, 2, 3}, {})),
                     values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({4}, {1, 2})))),
                 values<Bool>(true, false));

    CHECK_OUTPUT((eval_node<stdlib::len_, TSD<Int, TS<Int>>>(values<Value>(dict_delta<Int, TS<Int>>({}),
                                                                           dict_delta<Int, TS<Int>>({{0, 1}}),
                                                                           dict_delta<Int, TS<Int>>({}, {0})))),
                 values<Int>(0, 1, 0));

    // A NEVER-VALID input (upstream parity): len_ stays silent until the
    // first real delta (issue #116 family); contains_ SEEDS False — upstream
    // initializes the contains ref-output before the container first ticks
    // (issue #149); is_empty (above) keeps its True start tick per the
    // upstream port.
    CHECK_OUTPUT((eval_node<stdlib::len_, TSS<Int>>(values<Value>(none, set_delta<Int>({1}, {})))),
                 values<Int>(none, 1));
    CHECK_OUTPUT((eval_node<stdlib::len_, TSD<Int, TS<Int>>>(
                     values<Value>(none, dict_delta<Int, TS<Int>>({{1, 1}})))),
                 values<Int>(none, 1));
    CHECK_OUTPUT((eval_node<stdlib::contains_, TSD<Int, TS<Int>>>(
                     values<Value>(none, dict_delta<Int, TS<Int>>({{1, 10}})),
                     values<Int>(1, none))),
                 values<Bool>(false, true));
    CHECK_OUTPUT((eval_node<stdlib::contains_, TSS<Int>, TSS<Int>>(
                     values<Value>(none, set_delta<Int>({1, 2}, {})),
                     values<Value>(set_delta<Int>({1}, {}), none))),
                 values<Bool>(false, true));

    // Removal-driven re-ticks over EXPLICIT removal deltas — plain correct
    // behaviour on which both runtimes agree (the #120/#129 divergences were
    // the harness None-convention, hhenson/hgraph#363, not len semantics):
    // a partial removal shrinks len_, emptying flips is_empty, removing the
    // probed key flips contains_.
    CHECK_OUTPUT((eval_node<stdlib::len_, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 1}, {2, 2}}),
                                   dict_delta<Int, TS<Int>>({}, {1})))),
                 values<Int>(2, 1));
    CHECK_OUTPUT((eval_node<stdlib::len_, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 1}, {2, 2}}),
                                   dict_delta<Int, TS<Int>>({{2, 5}}, {1})))),
                 values<Int>(2, 1));
    CHECK_OUTPUT((eval_node<stdlib::is_empty, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}),
                                   dict_delta<Int, TS<Int>>({}, {1}),
                                   dict_delta<Int, TS<Int>>({{2, 2}})))),
                 values<Bool>(false, true, false));
    CHECK_OUTPUT((eval_node<stdlib::contains_, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 10}}),
                                   dict_delta<Int, TS<Int>>({}, {1})),
                     values<Int>(1, none))),
                 values<Bool>(true, false));
    CHECK_OUTPUT((eval_node<stdlib::is_empty, TSD<Int, TS<Int>>>(values<Value>(none,
                                                                               dict_delta<Int, TS<Int>>({{1, 1}}),
                                                                               dict_delta<Int, TS<Int>>({{2, 2}}),
                                                                               dict_delta<Int, TS<Int>>({}, {1}),
                                                                               dict_delta<Int, TS<Int>>({}, {2})))),
                 values<Bool>(true, false, none, none, true));
    CHECK_OUTPUT((eval_node<stdlib::contains_, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 20}})), values<Int>(1, 3))),
                 values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<stdlib::make_tsd>(
                     Str{"key"}, values<Int>(1, 2)),
                 values<Value>(dict_delta<Str, TS<Int>>({{Str{"key"}, 1}}),
                               dict_delta<Str, TS<Int>>({{Str{"key"}, 2}})));

    CHECK_OUTPUT((eval_node<stdlib::len_, TSL<TS<Int>, 2>>(values<Value>(list_delta<TS<Int>>({}),
                                                                         list_delta<TS<Int>>({{0, 1}}),
                                                                         list_delta<TS<Int>>({{1, 2}})))),
                 values<Int>(2, none, none));
    CHECK_OUTPUT((eval_node<stdlib::len_, TSL<TS<Int>, 4>>(values<Value>(list_delta<TS<Int>>({1, 2, 3, 4})))),
                 values<Int>(4));
    CHECK_OUTPUT(eval_node<DynamicTslGetitemGraph>(values<Value>(list_delta<TS<Int>>({1, 10}),
                                                                  list_delta<TS<Int>>({2, 20}),
                                                                  list_delta<TS<Int>>({3, 30}),
                                                                  list_delta<TS<Int>>({4, 40})),
                                                    values<Int>(0, none, 1, none)),
                 values<Int>(1, 2, 30, 40));
    CHECK_OUTPUT(eval_node<DynamicTsdGetitemGraph>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 20}}),
                                   dict_delta<Int, TS<Int>>({{1, 11}}),
                                   dict_delta<Int, TS<Int>>({}, {1}),
                                   dict_delta<Int, TS<Int>>({{1, 12}}),
                                   dict_delta<Int, TS<Int>>({{2, 22}})),
                     values<Int>(1, none, none, none, 2)),
                 values<Int>(10, 11, none, 12, 22));
    CHECK_OUTPUT((eval_node<stdlib::index_of, TSL<TS<Int>, 3>>(
                     values<Value>(list_delta<TS<Int>>({1, 2, 3}),
                                   none,
                                   list_delta<TS<Int>>({2, 3, 4}),
                                   list_delta<TS<Int>>({-1, 0, 1})),
                     values<Int>(2, 1))),
                 values<Int>(1, 0, -1, 2));
}

TEST_CASE("static input activity: TSD structural subscriptions ignore child value ticks")
{
    const auto input = values<Value>(dict_delta<Int, TS<Int>>({{1, 10}}),
                                     dict_delta<Int, TS<Int>>({{1, 11}}),
                                     dict_delta<Int, TS<Int>>({{2, 20}}),
                                     dict_delta<Int, TS<Int>>({{1, 12}}),
                                     dict_delta<Int, TS<Int>>({}, {2}));
    const auto expected = values<Int>(1, none, 2, none, 3);

    CHECK_OUTPUT(eval_node<StructuralTsdActivityGraph>(input), expected);
    CHECK_OUTPUT(eval_node<StructuralLinkedTsdActivityGraph>(input), expected);
}

TEST_CASE("std operators: TSB container access projects structural and peered fields")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<StructuralTsbContainerAccessGraph>(values<Int>(1, 10),
                                                              values<Str>(Str{"xy"}, Str{"abcd"})),
                 values<Int>(3, 14));
    CHECK_OUTPUT(eval_node<PeeredTsbContainerAccessGraph>(values<Int>(2, 20),
                                                          values<Str>(Str{"abc"}, Str{"z"})),
                 values<Int>(5, 21));
}

TEST_CASE("std operators: keyed bundle projection exposes the response field")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(
        eval_node<KeyedHandlerResponseProjectionGraph>(
            values<Value>(
                dict_delta<Int, HandlerOutputBundle>(
                    {{1, tsb_delta<HandlerOutputBundle>(Int{10}, Str{"first"})},
                     {2, tsb_delta<HandlerOutputBundle>(Int{20}, Str{"second"})}}),
                dict_delta<Int, HandlerOutputBundle>(
                    {{1, tsb_delta<HandlerOutputBundle>(Int{11}, std::nullopt)}}),
                dict_delta<Int, HandlerOutputBundle>({}, {2}))),
        values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 20}}),
                      dict_delta<Int, TS<Int>>({{1, 11}}),
                      dict_delta<Int, TS<Int>>({}, {2})));
}

TEST_CASE("std operators: active reference topology receives one explicit startup sample")
{
    stdlib::register_standard_operators();

    // ForwardReference has no hand-written schedule_on_start flag. Static C++
    // authoring infers the startup sample from its active REF input, after
    // which target value ticks flow through the published reference directly.
    CHECK_OUTPUT(eval_node<ForwardReferenceGraph>(values<Int>(1, 2)), values<Int>(1, 2));

    CHECK_OUTPUT(eval_node<CombineTsdReferenceTopologyGraph>(values<Int>(1, 2), values<Int>(3, none)),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 3}}),
                               dict_delta<Str, TS<Int>>({{"a", 2}})));
}

TEST_CASE("std operators: fixed TSL scalar indexing is a structural projection")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<FixedTslFirstGraph>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}}),
                                   list_delta<TS<Int>>({{0, 3}, {1, 4}}),
                                   list_delta<TS<Int>>({{1, 5}}))),
                 values<Int>(1, 3, none));
}

TEST_CASE("std operators: TSB len and is_empty are schema metadata")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<TsbContainerMetadataGraph>(values<Int>(1), values<Str>(Str{"x"})), values<Bool>(true));
}

TEST_CASE("std operators: TSB itemwise arithmetic maps each field through the scalar operators")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<TsbAddGraph>(values<Int>(1, 10),
                                        values<Float>(2.5, 1.0),
                                        values<Int>(3, 4),
                                        values<Float>(4.5, 2.0)),
                 values<Value>(tsb_delta<NumericTsbBundle>(Int{4}, Float{7.0}),
                               tsb_delta<NumericTsbBundle>(Int{14}, Float{3.0})));
    CHECK_OUTPUT(eval_node<TsbDivGraph>(values<Int>(8),
                                        values<Float>(9.0),
                                        values<Int>(4),
                                        values<Float>(3.0)),
                 values<Value>(tsb_delta<FloatTsbBundle>(Float{2.0}, Float{3.0})));
    CHECK_OUTPUT(eval_node<TsbUnaryGraph>(values<Int>(3), values<Float>(-4.5)),
                 values<Value>(tsb_delta<NumericTsbBundle>(Int{-3}, Float{4.5})));
}

TEST_CASE("std operators: dedup suppresses unchanged TSB fields independently")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<TsbDedupGraph>(values<Int>(1, 1, 2, 2),
                                          values<Str>(Str{"x"}, Str{"y"}, Str{"y"}, Str{"y"})),
                 values<Value>(tsb_delta<ContainerAccessBundle>(Int{1}, Str{"x"}),
                               tsb_delta<ContainerAccessBundle>(std::nullopt, Str{"y"}),
                               tsb_delta<ContainerAccessBundle>(Int{2}, std::nullopt),
                               none));
}

TEST_CASE("std operators: dedup preserves a named TSB schema")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<NamedTsbDedupGraph>(values<Int>(1, 1, 2),
                                               values<Str>(Str{"x"}, Str{"y"}, Str{"y"})),
                 values<Value>(tsb_delta<NamedContainerAccessBundle>(Int{1}, Str{"x"}),
                               tsb_delta<NamedContainerAccessBundle>(std::nullopt, Str{"y"}),
                               tsb_delta<NamedContainerAccessBundle>(Int{2}, std::nullopt)));
}

TEST_CASE("std operators: dedup projects a named TSB through REF")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<RefNamedTsbDedupGraph>(values<Int>(1, 1, 2),
                                                  values<Str>("x", "x", "y")),
                 values<Value>(tsb_delta<NamedContainerAccessBundle>(Int{1}, Str{"x"}),
                               none,
                               tsb_delta<NamedContainerAccessBundle>(Int{2}, Str{"y"})));
}

TEST_CASE("std operators: TSB itemwise bitwise and analytics reuse field operator resolution")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<TsbBitwiseGraph>(values<Int>(7),
                                            values<Int>(8),
                                            values<Int>(5),
                                            values<Int>(9)),
                 values<Value>(tsb_delta<IntTsbBundle>(Int{2}, Int{1})));
    CHECK_OUTPUT(eval_node<TsbMeanGraph>(values<Int>(7),
                                         values<Float>(8.0),
                                         values<Int>(5),
                                         values<Float>(9.0)),
                 values<Value>(tsb_delta<FloatTsbBundle>(Float{6.0}, Float{8.5})));
    CHECK_OUTPUT(eval_node<TsbMinGraph>(values<Int>(7),
                                        values<Str>(Str{"8"}),
                                        values<Int>(5),
                                        values<Str>(Str{"9"})),
                 values<Value>(tsb_delta<ContainerAccessBundle>(Int{5}, Str{"8"})));
    CHECK_OUTPUT(eval_node<TsbMaxGraph>(values<Int>(7),
                                        values<Str>(Str{"8"}),
                                        values<Int>(5),
                                        values<Str>(Str{"9"})),
                 values<Value>(tsb_delta<ContainerAccessBundle>(Int{7}, Str{"9"})));
}

TEST_CASE("std operators: convert copies an Arrow Series into a native variadic tuple")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<SeriesToTupleGraph>(
                     values<Series>(int_series({}), int_series({Int{1}}),
                                    int_series({Int{2}, std::nullopt, Int{3}}))),
                 values<Value>(int_tuple({}), int_tuple({Int{1}}),
                               nullable_int_tuple({Int{2}, std::nullopt, Int{3}})));
}

TEST_CASE("std operators: Arrow Series arithmetic and access use public typed wiring")
{
    stdlib::register_standard_operators();

    check_series_output(
        eval_node<SeriesAddGraph>(values<Series>(int_series({1, 2, 3})),
                                  values<Series>(int_series({4, 5, 6}))),
        values<Series>(int_series({5, 7, 9})));
    check_series_output(
        eval_node<SeriesMixedAddGraph>(values<Series>(int_series({1, 2, 3})),
                                       values<Float>(0.5)),
        values<Series>(float_series({1.5, 2.5, 3.5})));
    check_series_output(
        eval_node<SeriesDivGraph>(values<Series>(int_series({4, 6, 9})),
                                  values<Series>(int_series({2, 4, 3}))),
        values<Series>(float_series({2.0, 1.5, 3.0})));
    CHECK_OUTPUT(eval_node<SeriesGetItemGraph>(values<Series>(int_series({1, 2, 3})),
                                               values<Int>(2)),
                 values<Int>(3));
    CHECK_OUTPUT(eval_node<SeriesContainsGraph>(values<Series>(int_series({1, 2, 3}),
                                                                int_series({1, 2, 3})),
                                                values<Int>(2, 4)),
                 values<Bool>(true, false));
}

TEST_CASE("std operators: str_ converts scalar time-series values to strings")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::str_>(values<Int>(3, -2)), values<Str>(Str{"3"}, Str{"-2"}));
    CHECK_OUTPUT(eval_node<stdlib::str_>(values<Bool>(true, false)), values<Str>(Str{"true"}, Str{"false"}));
}

TEST_CASE("std operators: convert preserves UTF-8 payloads between text and bytes")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<TextToBytesGraph>(
                     values<Str>(Str{"plain"}, Str{"caf\xC3\xA9"}))),
                 values<Bytes>(bytes_("plain"), bytes_("caf\xC3\xA9")));
    CHECK_OUTPUT((eval_node<BytesToTextGraph>(
                     values<Bytes>(bytes_("plain"), bytes_("caf\xC3\xA9")))),
                 values<Str>(Str{"plain"}, Str{"caf\xC3\xA9"}));
    CHECK_THROWS((eval_node<BytesToTextGraph>(
        values<Bytes>(bytes_(std::string_view{"\xFF", 1})))));
}

TEST_CASE("std operators: stream operators cover sampling filtering slicing and scalar analytics")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::sample>(values<Bool>(none, true, none, true),
                                           values<Int>(1, 2, 3, 4, 5)),
                 values<Int>(none, 2, none, 4, none));

    CHECK_OUTPUT(eval_node<stdlib::filter_>(values<Bool>(true, false, false, true, true, none),
                                            values<Int>(1, 2, 3, none, none, 4)),
                 values<Int>(1, none, none, 3, none, 4));
    CHECK_OUTPUT((eval_node<stdlib::filter_, TSS<Int>>(
                     values<Bool>(true, false, none, true),
                     values<Value>(set_delta<Int>({1}, {}),
                                   set_delta<Int>({2}, {}),
                                   set_delta<Int>({}, {1}),
                                   set_delta<Int>({3}, {})))),
                 values<Value>(set_delta<Int>({1}, {}),
                               none,
                               none,
                               set_delta<Int>({2, 3}, {1})));
    CHECK_OUTPUT((eval_node<stdlib::filter_, TSD<Int, TS<Int>>>(
                     values<Bool>(true, false, none, true),
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}),
                                   dict_delta<Int, TS<Int>>({{1, 2}, {2, 2}}),
                                   dict_delta<Int, TS<Int>>({}, {1}),
                                   dict_delta<Int, TS<Int>>({{3, 3}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}),
                               none,
                               none,
                               dict_delta<Int, TS<Int>>({{2, 2}, {3, 3}}, {1})));

    CHECK_OUTPUT(eval_node<stdlib::lag>(values<Int>(1, 2, 3, 4), Int{1}),
                 values<Int>(none, 1, 2, 3));
    CHECK_OUTPUT(eval_node<stdlib::lag>(values<Int>(1, 2, 3, 4), Int{2}),
                 values<Int>(none, none, 1, 2));
    CHECK_OUTPUT((eval_node<stdlib::lag, TSS<Int>>(
                     values<Value>(set_delta<Int>({1}, {}),
                                   set_delta<Int>({2}, {}),
                                   set_delta<Int>({3}, {})),
                     Int{1})),
                 values<Value>(none, set_delta<Int>({1}, {}), set_delta<Int>({2}, {})));

    CHECK_OUTPUT(eval_node<stdlib::until_true>(values<Bool>(false, false, true, false)),
                 values<Bool>(false, false, true, none));

    CHECK_OUTPUT(eval_node<stdlib::freeze>(values<Bool>(false, false, true, false),
                                           values<Int>(1, 2, 3, 4)),
                 values<Int>(1, 2, 3, none));

    // The C++ PREDICATE forms (hgraph's callable predicate as a WiredFn):
    // until_true(fn, ts) inlines the predicate; freeze(fn, ts) freezes once
    // it fires.
    CHECK_OUTPUT(eval_node<stdlib::until_true>(fn<stdlib::not_>(),
                                               values<Bool>(true, true, false, true)),
                 values<Bool>(false, false, true, none));
    CHECK_OUTPUT(eval_node<stdlib::freeze>(fn<stdlib::not_>(),
                                           values<Bool>(true, true, false, true)),
                 values<Bool>(true, true, false, none));
    // Runtime value callables execute inside the native node. Python
    // callables use this same overload through the bridge.
    CHECK_OUTPUT(eval_node<stdlib::until_true>(value_fn<AtLeastThree>(),
                                               values<Int>(1, 2, 3, 4)),
                 values<Bool>(false, false, true, none));
    CHECK_OUTPUT(eval_node<stdlib::freeze>(value_fn<AtLeastThree>(),
                                           values<Int>(1, 2, 3, 4)),
                 values<Int>(1, 2, 3, none));
    CHECK_OUTPUT(eval_node<stdlib::gate>(values<Bool>(false, false, true, true),
                                         values<Int>(1, 2, 3, none),
                                         Int{8}),
                 values<Int>(none, none, 1, 2, 3));
    // hgraph semantics: a tick landing on the cycle the window releases
    // MERGES into that release (upstream throttle accumulates before the
    // scheduled drain), so t2 emits 3 (not the buffered 2) and t4 emits 5.
    CHECK_OUTPUT(eval_node<stdlib::throttle>(values<Int>(1, 2, 3, 4, 5),
                                             values<TimeDelta>(MIN_TD * 2,
                                                               none,
                                                               none,
                                                               none,
                                                               none)),
                 values<Int>(1, none, 3, none, 5));
    CHECK_OUTPUT(eval_node<stdlib::throttle>(values<Int>(1, 2, 0, 4, 0),
                                             values<TimeDelta>(MIN_TD * 2,
                                                               none,
                                                               none,
                                                               none,
                                                               none)),
                 values<Int>(1, none, 0, none, 0));
    CHECK_OUTPUT(eval_node<stdlib::throttle>(
                     values<Str>(Str{"1"}, Str{"2"}, Str{}, Str{"4"}, Str{}),
                     values<TimeDelta>(MIN_TD * 2, none, none, none, none)),
                 values<Str>(Str{"1"}, none, Str{}, none, Str{}));

    CHECK_OUTPUT(eval_node<stdlib::take>(values<Int>(1, 2, 3, 4, 5), Int{3}),
                 values<Int>(1, 2, 3, none, none));
    CHECK_OUTPUT(eval_node<stdlib::drop>(values<Int>(1, 2, 3, 4, 5), Int{3}),
                 values<Int>(none, none, none, 4, 5));
    CHECK_OUTPUT(eval_node<stdlib::step>(values<Int>(1, 2, 3, 4, 5, 6, 7, 8), Int{2}),
                 values<Int>(1, none, 3, none, 5, none, 7, none));
    CHECK_OUTPUT(eval_node<stdlib::slice_>(values<Int>(0, 1, 2, 3, 4, 5, 6, 7, 8), Int{2}, Int{-1}, Int{2}),
                 values<Int>(none, none, 2, none, 4, none, 6, none, 8));
    CHECK_OUTPUT(eval_node<stdlib::slice_>(values<Int>(0, 1, 2, 3), Int{-1}, Int{-1}, Int{2}),
                 values<Int>(none, none, none, none));

    CHECK_OUTPUT(eval_node<stdlib::to_window>(values<Int>(1, 2, 3, 4, 5), MIN_TD * 2),
                 values<Value>(Value{Int{1}}, Value{Int{2}}, Value{Int{3}},
                               Value{Int{4}}, Value{Int{5}}));
    // rolling_average is a public graph operator in both C++ and Python.  Keep
    // native coverage for both scalar-policy overloads so the ported upstream
    // tests do not merely prove the Python facade.
    CHECK_OUTPUT(eval_node<stdlib::rolling_average>(values<Int>(1, 2, 3, 4, 5),
                                                     Int{3}),
                 values<Float>(none, none, none, 3.0, 4.0));
    CHECK_OUTPUT(eval_node<stdlib::rolling_average>(values<Int>(1, 2, 3, 4, 5),
                                                     Int{3}, Int{2}),
                 values<Float>(none, 1.5, 2.0, 3.0, 4.0));
    auto duration_average =
        eval_node<stdlib::rolling_average>(values<Int>(1, 2, 3, 4, 5), MIN_TD * 3);
    REQUIRE(duration_average.size() == 8);
    REQUIRE(duration_average.back().has_value());
    CHECK(std::isnan(duration_average.back()->view().checked_as<Float>()));
    duration_average.pop_back();
    CHECK_OUTPUT(duration_average,
                 values<Float>(none, none, none, 3.0, 4.0, 4.5, 5.0));
    CHECK_OUTPUT(eval_node<ResettableTickWindowGraph>(
                     values<Int>(1, 2, none, 3, 4),
                     values<Bool>(none, none, true, none, none)),
                 values<Int>(101, 203, 0, 103, 207));
    CHECK_OUTPUT(eval_node<ResettableTickWindowGraph>(
                     values<Int>(1, 2, 3),
                     values<Bool>(none, true, none)),
                 values<Int>(101, 102, 205));
    CHECK_OUTPUT(eval_node<ResettableDurationWindowGraph>(
                     values<Int>(1, 2, none, 3),
                     values<Bool>(none, none, true, none)),
                 values<Int>(101, 203, 0, 103));
    CHECK_OUTPUT(eval_node<WindowStdDdofGraph>(
                     values<Int>(1, 2, 3, 4, 5)),
                 values<Float>(none, none, 1.0, 1.0, 1.0));

    CHECK_OUTPUT(eval_node<stdlib::count>(values<Int>(3, none, 2, 1)), values<Int>(1, none, 2, 3));
    CHECK_OUTPUT(eval_node<stdlib::dedup>(values<Int>(1, 2, 2, 3, 3, 3, 4)),
                 values<Int>(1, 2, none, 3, none, none, 4));
    CHECK_OUTPUT((eval_node<stdlib::dedup, TSS<Int>>(
                     values<Value>(set_delta<Int>({1}, {}),
                                   set_delta<Int>({1}, {}),
                                   set_delta<Int>({}, {1}),
                                   set_delta<Int>({}, {1})))),
                 values<Value>(set_delta<Int>({1}, {}), none,
                               set_delta<Int>({}, {1}), none));
    CHECK_OUTPUT(eval_node<stdlib::diff>(values<Int>(1, 2, 4, 7)), values<Int>(none, 1, 2, 3));
    CHECK_OUTPUT(eval_node<stdlib::diff>(values<Float>(1.0, 1.5, 3.0)), values<Float>(none, 0.5, 1.5));
    CHECK_OUTPUT(eval_node<stdlib::clip>(values<Float>(-1.0, 0.5, 2.0), Float{0.0}, Float{1.0}),
                 values<Float>(0.0, 0.5, 1.0));
    CHECK_OUTPUT(eval_node<stdlib::ewma>(values<Float>(1.0, 2.0, 3.0, 4.0), Float{0.5}),
                 values<Float>(1.0, 1.5, 2.25, 3.125));
}

TEST_CASE("std operators: TSB proxy lag preserves sparse field deltas")
{
    stdlib::register_standard_operators();

    const auto bundle_delta = [](std::optional<Int> a, std::optional<Float> b) {
        const auto *schema = ts_type<NumericTsbBundle>();
        BundleBuilder builder{
            ValuePlanFactory::instance().type_for(schema->delta_value_schema)};
        if (a.has_value()) { builder.set("a", Value{*a}); }
        if (b.has_value()) { builder.set("b", Value{*b}); }
        return builder.build();
    };

    CHECK_OUTPUT(eval_node<TsbProxyLagGraph>(
                     values<Int>(1, 2, 3, 4, 5),
                     values<Float>(1.0, 2.0, none, none, none),
                     values<Bool>(true, none, true, true, true)),
                 values<Value>(none, none, none,
                               bundle_delta(Int{2}, Float{2.0}),
                               bundle_delta(Int{3}, std::nullopt)));
}

TEST_CASE("std operators: structural proxy lag recursively preserves nested TSD keys")
{
    using namespace std::string_literals;

    stdlib::register_standard_operators();

    const auto target =
        values<Value>(dict_delta<Str, TS<Int>>({{"next"s, Int{1}}}), none, none, none);
    const auto rebase = values<Bool>(true, false, false, false);
    const auto prices =
        values<Value>(dict_delta<Str, TS<Int>>({{"next"s, Int{10}}}), none, none, none);
    const auto trigger = values<Int>(0, 1, 2, 3);
    const auto feedback_expected = values<Int>(-1, -1, 10, 10);

    CHECK_OUTPUT(
        eval_node<TsbNestedTsdProxyLagFeedbackGraph>(
            target, rebase, prices, trigger),
        feedback_expected);
    CHECK_OUTPUT(
        eval_node<TslNestedTsdProxyLagFeedbackGraph>(
            target, rebase, prices, trigger),
        feedback_expected);
}

TEST_CASE("std operators: schedule and resample are bounded by executor end time")
{
    stdlib::register_standard_operators();

    {
        Wiring w;
        auto   ticks = wire<stdlib::schedule>(w, MIN_TD * 2);
        wire<stdlib::dense_record_impl>(w, ticks, Str{"schedule_out"});

        GraphBuilder graph_builder = std::move(w).finish();
        GraphExecutorBuilder executor_builder;
        executor_builder.graph_builder(std::move(graph_builder))
            .start_time(MIN_ST)
            .end_time(MIN_ST + MIN_TD * 6);
        GraphExecutorValue executor = executor_builder.make_executor();
        auto               view     = executor.view();
        view.run();

        CHECK_OUTPUT(get_recorded_values<Bool>(view.graph().global_state(), "schedule_out"),
                     values<Bool>(none, none, true, none, true));
    }

    {
        Wiring w;
        auto   ts  = wire<stdlib::replay_impl, TS<Int>>(w, Str{"resample_in"});
        auto   out = wire<stdlib::resample>(w, ts, MIN_TD * 2);
        wire<stdlib::dense_record_impl>(w, out, Str{"resample_out"});

        GraphBuilder graph_builder = std::move(w).finish();
        set_replay_values<Int>(graph_builder.global_state(),
                               "resample_in",
                               values<Int>(1, none, 3));

        GraphExecutorBuilder executor_builder;
        executor_builder.graph_builder(std::move(graph_builder))
            .start_time(MIN_ST)
            .end_time(MIN_ST + MIN_TD * 6);
        GraphExecutorValue executor = executor_builder.make_executor();
        auto               view     = executor.view();
        view.run();

        CHECK_OUTPUT(get_recorded_values<Int>(view.graph().global_state(), "resample_out"),
                     values<Int>(none, none, 3, none, 3));
    }
}

TEST_CASE("std operators: control operators cover variadic booleans merge and selection")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::all_>(values<Bool>(none, true, false)),
                 values<Bool>(false, true, false));
    CHECK_OUTPUT(eval_node<stdlib::any_>(values<Bool>(none, true, false)),
                 values<Bool>(false, true, false));
    CHECK_OUTPUT(eval_node<stdlib::all_>(values<Bool>(true, true, true),
                                         values<Bool>(true, false, true),
                                         values<Bool>(true, true, none)),
                 values<Bool>(true, false, true));
    CHECK_OUTPUT(eval_node<stdlib::any_>(values<Bool>(false, false, none),
                                         values<Bool>(false, true, false),
                                         values<Bool>(false, false, none)),
                 values<Bool>(false, true, false));
    CHECK_OUTPUT((eval_node<stdlib::all_, TSD<Int, TS<Bool>>>(
                     values<Value>(dict_delta<Int, TS<Bool>>({}),
                                   dict_delta<Int, TS<Bool>>({{1, false}}),
                                   dict_delta<Int, TS<Bool>>({{2, true}}),
                                   dict_delta<Int, TS<Bool>>({{1, true}}),
                                   dict_delta<Int, TS<Bool>>({{2, false}}),
                                   dict_delta<Int, TS<Bool>>({}, {2}),
                                   dict_delta<Int, TS<Bool>>({}, {1})))),
                 values<Bool>(true, false, false, true, false, true, true));
    CHECK_OUTPUT((eval_node<stdlib::any_, TSD<Int, TS<Bool>>>(
                     values<Value>(dict_delta<Int, TS<Bool>>({}),
                                   dict_delta<Int, TS<Bool>>({{1, false}}),
                                   dict_delta<Int, TS<Bool>>({{2, false}}),
                                   dict_delta<Int, TS<Bool>>({{1, true}}),
                                   dict_delta<Int, TS<Bool>>({{2, true}}),
                                   dict_delta<Int, TS<Bool>>({{2, false}}),
                                   dict_delta<Int, TS<Bool>>({}, {1})))),
                 values<Bool>(false, false, false, true, true, true, false));

    CHECK_OUTPUT(eval_node<stdlib::merge>(values<Int>(none, 2, none, none, 6),
                                          values<Int>(1, none, 4, none, none),
                                          values<Int>(none, 3, 5, none, none)),
                 values<Int>(1, 2, 4, none, 6));
    CHECK_OUTPUT(eval_node<RaceGraph>(values<Int>(none, 1, 10, 11),
                                      values<Int>(2, 3, 4, 5)),
                 values<Int>(2, 3, 4, 5));
    CHECK_OUTPUT(eval_node<IfTrueRouteGraph>(values<Bool>(true, true, false, false, true),
                                             values<Int>(1, 2, 3, 4, 5)),
                 values<Int>(1, 2, none, none, 5));
    CHECK_OUTPUT(eval_node<IfFalseRouteGraph>(values<Bool>(true, true, false, false, true),
                                              values<Int>(1, 2, 3, 4, 5)),
                 values<Int>(none, none, 3, 4, none));
    CHECK_OUTPUT(eval_node<SampleIfTrueRouteGraph>(values<Bool>(false, false, true, false, true),
                                                   values<Int>(1, 2, 3, 4, 5)),
                 values<Int>(none, none, 1, none, 1));
    CHECK_OUTPUT(eval_node<RouteByIndexSlotTwoGraph>(values<Int>(0, 2, none, 1, 2),
                                                     values<Int>(10, 20, 30, 40, 50)),
                 values<Int>(none, 20, 30, none, 50));

    CHECK_OUTPUT(eval_node<stdlib::if_true>(values<Bool>(true, false, true)),
                 values<Bool>(true, none, true));
    CHECK_OUTPUT(eval_node<stdlib::if_true>(values<Bool>(true, false, true), Bool{true}),
                 values<Bool>(true, none, none));
    CHECK_OUTPUT(eval_node<stdlib::if_then_else>(values<Bool>(true, false, true),
                                                 values<Int>(1, 2, 3),
                                                 values<Int>(4, 5, 6)),
                 values<Int>(1, 5, 3));
    CHECK_OUTPUT(eval_node<stdlib::if_cmp>(values<stdlib::CmpResult>(stdlib::CmpResult::LT,
                                                                     stdlib::CmpResult::EQ,
                                                                     stdlib::CmpResult::GT),
                                           values<Int>(1, 2, 3),
                                           values<Int>(10, 20, 30),
                                           values<Int>(100, 200, 300)),
                 values<Int>(1, 20, 300));
}

TEST_CASE("std operators: structural REF unbind publishes only previously visible removals")
{
    stdlib::register_standard_operators();

    const auto source = values<Value>(none,
                                      dict_delta<Int, TS<Int>>({{1, 1}, {2, 2}}),
                                      dict_delta<Int, TS<Int>>({{3, 3}}, {2}),
                                      none);
    const auto condition = values<Bool>(true, true, false, true);

    CHECK_OUTPUT(eval_node<IfTrueTsdFilterGraph>(source, condition),
                 values<Value>(none,
                               dict_delta<Int, TS<Int>>({{1, 1}, {2, 2}}),
                               dict_delta<Int, TS<Int>>({}, {1, 2}),
                               dict_delta<Int, TS<Int>>({{1, 1}, {3, 3}})));
    CHECK_OUTPUT(eval_node<IfTrueTsdKeySetGraph>(source, condition),
                 values<Value>(none,
                               set_delta<Int>({1, 2}, {}),
                               set_delta<Int>({}, {1, 2}),
                               set_delta<Int>({1, 3}, {})));
    CHECK_OUTPUT(eval_node<InvalidTsdChildUnbindGraph>(values<Bool>(false, true),
                                                       values<Bool>(true, false)),
                 values<Value>(none, none));
}

TEST_CASE("std operators: fixed TSL REF composition flips through an empty reference")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<TslReferenceFlipGraph>(
                     values<Value>(list_delta<TS<Int>>({1, 1}), none, none, none),
                     values<Value>(list_delta<TS<Int>>({2, 2}), none, none, none),
                     values<Int>(0, 2, 1, 2)),
                 values<Value>(list_delta<TS<Int>>({1, 1}), none,
                               list_delta<TS<Int>>({2, 2}), none));
}

TEST_CASE("std operators: TSB REF composition flips through an empty reference")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<TsbReferenceFlipGraph>(
                     values<Value>(tsb_delta<ContainerAccessBundle>(Int{1}, std::nullopt),
                                   tsb_delta<ContainerAccessBundle>(std::nullopt, Str{"a"}),
                                   none, none),
                     values<Value>(tsb_delta<ContainerAccessBundle>(Int{2}, std::nullopt),
                                   tsb_delta<ContainerAccessBundle>(std::nullopt, Str{"b"}),
                                   none, none),
                     values<Int>(0, 2, 1, 2)),
                 values<Value>(tsb_delta<ContainerAccessBundle>(Int{1}, std::nullopt),
                               none,
                               tsb_delta<ContainerAccessBundle>(Int{2}, Str{"b"}),
                               none));
}

TEST_CASE("std operators: dereference materializes REF TSB fields as references")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<DereferenceTsbReferenceGraph>(
                     values<Value>(tsb_delta<ContainerAccessBundle>(Int{1}, std::nullopt),
                                   tsb_delta<ContainerAccessBundle>(std::nullopt, Str{"a"}),
                                   none, none, none),
                     values<Value>(tsb_delta<ContainerAccessBundle>(Int{2}, std::nullopt),
                                   tsb_delta<ContainerAccessBundle>(std::nullopt, Str{"b"}),
                                   none, none, none),
                     values<Int>(0, none, 2, 1, 2)),
                 values<Value>(tsb_delta<ContainerAccessBundle>(Int{1}, std::nullopt),
                               tsb_delta<ContainerAccessBundle>(std::nullopt, Str{"a"}),
                               none,
                               tsb_delta<ContainerAccessBundle>(Int{2}, Str{"b"}),
                               none));
}

TEST_CASE("std operators: date component operators extract day month year and explode")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::day_of_month>(values<Date>(ymd(2020, 1, 3), ymd(2021, 12, 31))),
                 values<Int>(3, 31));
    CHECK_OUTPUT(eval_node<stdlib::month_of_year>(values<Date>(ymd(2020, 1, 3), ymd(2021, 12, 31))),
                 values<Int>(1, 12));
    CHECK_OUTPUT(eval_node<stdlib::year>(values<Date>(ymd(2020, 1, 3), ymd(2021, 12, 31))), values<Int>(2020, 2021));
    CHECK_OUTPUT(eval_node<stdlib::explode>(values<Date>(ymd(2024, 1, 1), ymd(2024, 1, 2),
                                                          ymd(2024, 2, 2), ymd(2025, 2, 2))),
                 values<Value>(list_delta<TS<Int>>({{0, 2024}, {1, 1}, {2, 1}}),
                               list_delta<TS<Int>>({{2, 2}}),
                               list_delta<TS<Int>>({{1, 2}}),
                               list_delta<TS<Int>>({{0, 2025}})));
}

TEST_CASE("std operators: time-series property operators report valid modified and last-modified")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::valid>(values<Int>(none, 1)), values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<stdlib::modified>(values<Int>(none, 1, none, none, 2, none)),
                 values<Bool>(false, true, false, none, true, false));
    CHECK_OUTPUT(eval_node<stdlib::last_modified_time>(values<Int>(1, none, 2)),
                 values<DateTime>(MIN_ST, none, MIN_ST + 2 * MIN_TD));

    const Date start_date{std::chrono::floor<std::chrono::days>(MIN_ST)};
    CHECK_OUTPUT(eval_node<stdlib::last_modified_date>(values<Int>(1, none, 2)),
                 values<Date>(start_date, none, start_date));

    const std::vector<std::optional<Value>> wall_clock =
        eval_node<stdlib::last_modified_wall_clock_time>(values<Int>(1, none, 2));
    REQUIRE(wall_clock.size() == 3);
    REQUIRE(wall_clock[0].has_value());
    CHECK(wall_clock[0]->view().checked_as<DateTime>() != MIN_DT);
    CHECK_FALSE(wall_clock[1].has_value());
    REQUIRE(wall_clock[2].has_value());
    CHECK(wall_clock[2]->view().checked_as<DateTime>() != MIN_DT);
}

TEST_CASE("std operators: float dedup applies the upstream default tolerance")
{
    stdlib::register_standard_operators();

    // parity issue #69: upstream's float dedup overload defaults
    // abs_tol=1e-15, so a sub-tolerance change from the last emitted value
    // does not tick; a real change still does.
    CHECK_OUTPUT(eval_node<stdlib::dedup>(values<Float>(0.0, 9.395605309808467e-37, 1.0)),
                 values<Float>(0.0, none, 1.0));
}

TEST_CASE("std operators: format renders REF arguments dereferenced")
{
    stdlib::register_standard_operators();

    // parity issue #72: a REF-valued argument (if_then_else selection)
    // formats its referenced VALUE, never the reference itself.
    CHECK_OUTPUT(eval_node<FormatRefArgumentGraph>(values<Bool>(true), values<Int>(8), values<Int>(-6)),
                 values<Str>("-6:-4"));
}

TEST_CASE("std operators: valid over a REF source stays silent until the reference arrives")
{
    stdlib::register_standard_operators();

    // parity issue #70: upstream's valid_impl requires the REF input valid,
    // so a REF-valued source that never ticks produces NO output; once the
    // selection resolves, validity publishes.
    CHECK_OUTPUT(eval_node<ValidOverRefSelectionGraph>(values<Bool>(none), values<Int>(none), values<Int>(none)),
                 values<Bool>(none));
    CHECK_OUTPUT(eval_node<ValidOverRefSelectionGraph>(values<Bool>(true), values<Int>(8), values<Int>(-6)),
                 values<Bool>(true));
}

TEST_CASE("std operators: an int const dedup stays int")
{
    stdlib::register_standard_operators();

    // parity issue #74: an int scalar auto-const is NOT a match for a
    // ``TS<Float>`` input (cross-family coercion), so the generic dedup
    // overload wins; the float-tolerance overload (single-arg callable since
    // the #69 fix) can never capture it.
    CHECK_OUTPUT(eval_node<DedupIntConstGraph>(values<Int>(none)), values<Int>(2));

    // The same rule as a hard reject: an int tolerance for the float
    // overload's ``TS<Float>`` abs_tol leaves no matching candidate.
    REQUIRE_THROWS_AS(eval_node<DedupIntToleranceGraph>(values<Float>(1.0)), OperatorResolutionError);
}

TEST_CASE("std operators: an operand combination with no registered implementation raises")
{
    stdlib::register_standard_operators();   // bool arithmetic is deliberately not registered
    REQUIRE_THROWS_AS(eval_node<stdlib::add_>(values<Bool>(true), values<Bool>(false)), OperatorResolutionError);
}

TEST_CASE("std operators: div_ takes an optional divide-by-zero policy scalar")
{
    using DBZ = stdlib::DivideByZero;
    stdlib::register_standard_operators();

    const Float inf = std::numeric_limits<Float>::infinity();

    // Non-zero divisors are unaffected by the policy.
    CHECK_OUTPUT(eval_node<stdlib::div_>(values<Int>(6, 9), values<Int>(2, 3), DBZ::Inf), values<Float>(3.0, 3.0));

    // A zero divisor takes the policy's value.
    CHECK_OUTPUT(eval_node<stdlib::div_>(values<Int>(1, 1), values<Int>(2, 0), DBZ::Inf), values<Float>(0.5, inf));
    CHECK_OUTPUT(eval_node<stdlib::div_>(values<Int>(1, 1), values<Int>(2, 0), DBZ::Zero), values<Float>(0.5, 0.0));
    CHECK_OUTPUT(eval_node<stdlib::div_>(values<Int>(1, 1), values<Int>(2, 0), DBZ::One), values<Float>(0.5, 1.0));

    // NoTick produces a gap (no tick) on the zero-divisor cycle.
    CHECK_OUTPUT(eval_node<stdlib::div_>(values<Int>(1, 1, 1), values<Int>(2, 0, 4), DBZ::NoTick),
                 values<Float>(0.5, none, 0.25));

    CHECK_OUTPUT(eval_node<stdlib::floordiv_>(values<Int>(5, 5, 5), values<Int>(2, 0, 4), DBZ::NoTick),
                 values<Int>(2, none, 1));
    CHECK_OUTPUT(eval_node<stdlib::mod_>(values<Int>(5, 5, 5), values<Int>(2, 0, 4), DBZ::NoTick),
                 values<Int>(1, none, 1));
}

TEST_CASE("std operators: div_ NaN policy emits NaN on a zero divisor")
{
    using DBZ = stdlib::DivideByZero;
    stdlib::register_standard_operators();

    const std::vector<std::optional<Value>> out =
        eval_node<stdlib::div_>(values<Int>(1, 1), values<Int>(2, 0), DBZ::Nan);
    REQUIRE(out.size() == 2);
    REQUIRE(out[0].has_value());
    CHECK(out[0]->view().checked_as<Float>() == Float{0.5});
    REQUIRE(out[1].has_value());
    CHECK(std::isnan(out[1]->view().checked_as<Float>()));
}

TEST_CASE("std operators: div_ Error policy raises on a zero divisor")
{
    using DBZ = stdlib::DivideByZero;
    stdlib::register_standard_operators();

    REQUIRE_THROWS(eval_node<stdlib::div_>(values<Int>(1), values<Int>(0), DBZ::Error));
}

TEST_CASE("std operators: div_ without a policy defaults to Error and raises on a zero divisor")
{
    stdlib::register_standard_operators();
    REQUIRE_THROWS(eval_node<stdlib::div_>(values<Int>(1), values<Int>(0)));
}

TEST_CASE("std operators: request_id uses the native service identifier allocator")
{
    stdlib::register_standard_operators();

    const auto first = eval_node<stdlib::request_id>(Int{1});
    const auto second = eval_node<stdlib::request_id>(Int{1});
    REQUIRE(first.size() == 1);
    REQUIRE(second.size() == 1);
    REQUIRE(first[0].has_value());
    REQUIRE(second[0].has_value());
    CHECK(first[0]->view().checked_as<Int>() != second[0]->view().checked_as<Int>());
}
