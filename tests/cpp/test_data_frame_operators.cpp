#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/operators/impl/data_frame_impl.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value_builder.h>

#include <arrow/api.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    using Row = Bundle<"tests.data_frame::Row", Field<"a", Int>, Field<"b", Int>>;
    using FixedRows = Tuple<Row, Row>;
    using MixedFixedRows = Tuple<Row, Int>;
    using FrameMetaDetails = Bundle<"tests.data_frame::FrameMetaDetails",
                                    Field<"desk", Str>>;
    using FrameMeta = Bundle<"tests.data_frame::FrameMeta",
                             Field<"revision", Int>, Field<"as_of", DateTime>,
                             Field<"source", Str>, Field<"details", FrameMetaDetails>>;
    using JoinedRow = Bundle<"tests.data_frame::JoinedRow", Field<"a", Int>,
                             Field<"b", Int>, Field<"b_right", Int>>;
    using PredicateTSB = UnNamedTSB<Field<"a", TS<Int>>, Field<"b", TS<Int>>>;
    using BColumnTSB = UnNamedTSB<Field<"b", TS<Int>>>;
    using CColumnTSB = UnNamedTSB<Field<"c", TS<Int>>>;
    using CSeriesColumnTSB = UnNamedTSB<Field<"c", TS<SeriesOf<Int>>>>;
    using ProjectedRow = Bundle<"tests.data_frame::ProjectedRow", Field<"a", Int>,
                                Field<"c", Int>>;
    using KeyedRow = Bundle<"tests.data_frame::KeyedRow", Field<"a", Int>,
                            Field<"b", Int>, Field<"key", Str>>;
    using ScalarRefFrameRow =
        UnNamedBundle<Field<"date", DateTime>, Field<"key", Str>, Field<"value", Float>>;
    using RefBundle = UnNamedTSB<Field<"a", TS<Int>>, Field<"b", TS<Str>>>;
    using SqlRow = UnNamedTSB<Field<"name", TS<Str>>, Field<"age", TS<Int>>>;
    using BundleRefFrameRow =
        UnNamedBundle<Field<"date", DateTime>, Field<"key", Str>, Field<"a", Int>,
                      Field<"b", Str>>;

    void require_arrow(const arrow::Status &status)
    {
        if (!status.ok()) { throw std::runtime_error(status.ToString()); }
    }

    [[nodiscard]] Frame frame(std::vector<std::int64_t> a, std::vector<std::int64_t> b)
    {
        arrow::Int64Builder a_builder;
        arrow::Int64Builder b_builder;
        require_arrow(a_builder.AppendValues(a));
        require_arrow(b_builder.AppendValues(b));
        std::shared_ptr<arrow::Array> a_array;
        std::shared_ptr<arrow::Array> b_array;
        require_arrow(a_builder.Finish(&a_array));
        require_arrow(b_builder.Finish(&b_array));
        return Frame{arrow::Table::Make(
            arrow::schema({arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())}),
            {std::move(a_array), std::move(b_array)})};
    }

    [[nodiscard]] bool equals(const Frame &lhs, const Frame &rhs)
    {
        return lhs.has_value() == rhs.has_value() &&
               (!lhs.has_value() || lhs.table->Equals(*rhs.table));
    }

    [[nodiscard]] Frame joined_frame(std::vector<std::int64_t> a,
                                     std::vector<std::int64_t> b,
                                     std::vector<std::optional<std::int64_t>> b_right)
    {
        arrow::Int64Builder a_builder;
        arrow::Int64Builder b_builder;
        arrow::Int64Builder right_builder;
        require_arrow(a_builder.AppendValues(a));
        require_arrow(b_builder.AppendValues(b));
        for (const auto value : b_right)
        {
            require_arrow(value.has_value() ? right_builder.Append(*value)
                                            : right_builder.AppendNull());
        }
        std::shared_ptr<arrow::Array> a_array;
        std::shared_ptr<arrow::Array> b_array;
        std::shared_ptr<arrow::Array> right_array;
        require_arrow(a_builder.Finish(&a_array));
        require_arrow(b_builder.Finish(&b_array));
        require_arrow(right_builder.Finish(&right_array));
        return Frame{arrow::Table::Make(
            arrow::schema({arrow::field("a", arrow::int64()),
                           arrow::field("b", arrow::int64()),
                           arrow::field("b_right", arrow::int64())}),
            {std::move(a_array), std::move(b_array), std::move(right_array)})};
    }

    [[nodiscard]] Value row_value(std::optional<Int> a, std::optional<Int> b)
    {
        BundleBuilder builder{
            ValuePlanFactory::instance().type_for(scalar_descriptor<Row>::value_meta())};
        if (a.has_value()) { builder.set(0, Value{*a}); }
        if (b.has_value()) { builder.set(1, Value{*b}); }
        return builder.build();
    }

    [[nodiscard]] Value fixed_rows(Int first_a, Int first_b, Int second_a, Int second_b)
    {
        const auto binding = ValuePlanFactory::instance().type_for(
            scalar_descriptor<FixedRows>::value_meta());
        Value value{binding};
        auto  tuple = value.as_tuple().begin_mutation();
        const std::array<Value, 2> rows{
            row_value(first_a, first_b), row_value(second_a, second_b)};
        for (std::size_t index = 0; index < rows.size(); ++index)
        {
            auto destination = tuple.at(index);
            destination.binding().ops_ref().copy_assign_from(
                destination.binding(), destination.mutable_data(),
                rows[index].binding(), rows[index].view().data());
        }
        return value;
    }

    [[nodiscard]] Value metadata_value(Int revision)
    {
        BundleBuilder details{
            ValuePlanFactory::instance().type_for(
                scalar_descriptor<FrameMetaDetails>::value_meta())};
        details.set(0, Value{Str{"systematic"}});
        BundleBuilder builder{
            ValuePlanFactory::instance().type_for(scalar_descriptor<FrameMeta>::value_meta())};
        builder.set(0, Value{revision});
        builder.set(1, Value{DateTime{std::chrono::microseconds{123456}}});
        builder.set(2, Value{Str{"fixture"}});
        builder.set(3, details.build());
        return builder.build();
    }

    [[nodiscard]] Frame metadata_frame(std::vector<std::int64_t> a,
                                       std::vector<std::int64_t> b, Int revision)
    {
        Frame result = frame(std::move(a), std::move(b));
        result.table = result.table->ReplaceSchemaMetadata(
            arrow::key_value_metadata({"external.owner"}, {"research"}));
        return with_frame_metadata(std::move(result), metadata_value(revision));
    }

    [[nodiscard]] Frame keyed_frame(std::vector<std::int64_t> a,
                                    std::vector<std::int64_t> b,
                                    std::vector<std::string> keys)
    {
        arrow::Int64Builder a_builder;
        arrow::Int64Builder b_builder;
        arrow::StringBuilder key_builder;
        require_arrow(a_builder.AppendValues(a));
        require_arrow(b_builder.AppendValues(b));
        require_arrow(key_builder.AppendValues(keys));
        std::shared_ptr<arrow::Array> a_array;
        std::shared_ptr<arrow::Array> b_array;
        std::shared_ptr<arrow::Array> key_array;
        require_arrow(a_builder.Finish(&a_array));
        require_arrow(b_builder.Finish(&b_array));
        require_arrow(key_builder.Finish(&key_array));
        return Frame{arrow::Table::Make(
            arrow::schema({arrow::field("a", arrow::int64()),
                           arrow::field("b", arrow::int64()),
                           arrow::field("key", arrow::utf8())}),
            {std::move(a_array), std::move(b_array), std::move(key_array)})};
    }

    [[nodiscard]] Series int_series(std::vector<std::int64_t> values)
    {
        arrow::Int64Builder builder;
        require_arrow(builder.AppendValues(values));
        std::shared_ptr<arrow::Array> array;
        require_arrow(builder.Finish(&array));
        return Series{std::move(array)};
    }

    [[nodiscard]] Frame array_frame(DateTime when)
    {
        arrow::TimestampBuilder date_builder{
            arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool()};
        auto value_builder = std::make_unique<arrow::Int64Builder>();
        auto *values       = value_builder.get();
        arrow::ListBuilder list_builder{arrow::default_memory_pool(),
                                        std::move(value_builder)};
        require_arrow(date_builder.Append(when.time_since_epoch().count()));
        require_arrow(list_builder.Append());
        require_arrow(values->AppendValues({Int{10}, Int{20}}));

        std::shared_ptr<arrow::Array> date_array;
        std::shared_ptr<arrow::Array> list_array;
        require_arrow(date_builder.Finish(&date_array));
        require_arrow(list_builder.Finish(&list_array));
        return Frame{arrow::Table::Make(
            arrow::schema({
                arrow::field("date", arrow::timestamp(arrow::TimeUnit::MICRO)),
                arrow::field("value", arrow::list(arrow::int64()))}),
            {std::move(date_array), std::move(list_array)})};
    }

    [[nodiscard]] Frame sql_integer_frame()
    {
        arrow::TimestampBuilder date_builder{
            arrow::timestamp(arrow::TimeUnit::MICRO),
            arrow::default_memory_pool()};
        arrow::StringBuilder name_builder;
        arrow::Int32Builder age_builder;
        require_arrow(date_builder.Append(MIN_ST.time_since_epoch().count()));
        require_arrow(name_builder.Append("Alice"));
        require_arrow(age_builder.Append(25));
        std::shared_ptr<arrow::Array> date_array;
        std::shared_ptr<arrow::Array> name_array;
        std::shared_ptr<arrow::Array> age_array;
        require_arrow(date_builder.Finish(&date_array));
        require_arrow(name_builder.Finish(&name_array));
        require_arrow(age_builder.Finish(&age_array));
        return Frame{arrow::Table::Make(
            arrow::schema({
                arrow::field("date", arrow::timestamp(arrow::TimeUnit::MICRO)),
                arrow::field("name", arrow::utf8()),
                arrow::field("age", arrow::int32())}),
            {std::move(date_array), std::move(name_array),
             std::move(age_array)})};
    }

    [[nodiscard]] Frame matrix_frame(DateTime when)
    {
        arrow::TimestampBuilder date_builder{
            arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool()};
        auto item_builder = std::make_unique<arrow::Int64Builder>();
        auto *items       = item_builder.get();
        auto row_builder  = std::make_unique<arrow::ListBuilder>(
            arrow::default_memory_pool(), std::move(item_builder));
        auto *rows = row_builder.get();
        arrow::ListBuilder matrix_builder{arrow::default_memory_pool(),
                                          std::move(row_builder)};

        require_arrow(date_builder.Append(when.time_since_epoch().count()));
        require_arrow(matrix_builder.Append());
        require_arrow(rows->Append());
        require_arrow(items->AppendValues({Int{1}, Int{2}}));
        require_arrow(rows->Append());
        require_arrow(items->AppendValues({Int{3}, Int{4}}));

        std::shared_ptr<arrow::Array> date_array;
        std::shared_ptr<arrow::Array> matrix_array;
        require_arrow(date_builder.Finish(&date_array));
        require_arrow(matrix_builder.Finish(&matrix_array));
        return Frame{arrow::Table::Make(
            arrow::schema({
                arrow::field("date", arrow::timestamp(arrow::TimeUnit::MICRO)),
                arrow::field("value", arrow::list(arrow::list(arrow::int64())))}),
            {std::move(date_array), std::move(matrix_array)})};
    }

    [[nodiscard]] Frame scalar_batch(std::vector<DateTime> times,
                                     std::vector<Int> values)
    {
        arrow::TimestampBuilder date_builder{
            arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool()};
        arrow::Int64Builder value_builder;
        for (const DateTime when : times)
        {
            require_arrow(date_builder.Append(when.time_since_epoch().count()));
        }
        require_arrow(value_builder.AppendValues(values));
        std::shared_ptr<arrow::Array> date_array;
        std::shared_ptr<arrow::Array> value_array;
        require_arrow(date_builder.Finish(&date_array));
        require_arrow(value_builder.Finish(&value_array));
        return Frame{arrow::Table::Make(
            arrow::schema({
                arrow::field("date", arrow::timestamp(arrow::TimeUnit::MICRO)),
                arrow::field("value", arrow::int64())}),
            {std::move(date_array), std::move(value_array)})};
    }

    struct FromDataFrameBatchesGraph
    {
        static constexpr auto name = "from_data_frame_batches_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Frame>> frames)
        {
            return wire<stdlib::from_data_frame_batches, TS<Int>>(
                w, frames, Str{"date"}, Str{"key"}, Str{"value"}, TimeDelta{});
        }
    };

    struct SortFrameGraph
    {
        static constexpr auto name = "sort_frame_graph";

        static Port<TS<FrameOf<Row>>> compose(Wiring &w, Port<TS<FrameOf<Row>>> ts,
                                              Scalar<"by", Str> by,
                                              Scalar<"descending", Bool> descending)
        {
            return wire<stdlib::sorted_>(w, ts, by, descending).as<TS<FrameOf<Row>>>();
        }
    };

    struct ConvertCompoundFrameGraph
    {
        static constexpr auto name = "convert_compound_frame_graph";

        static Port<TS<FrameOf<Row>>> compose(Wiring &w, Port<TS<Row>> ts)
        {
            return wire<stdlib::convert, TS<FrameOf<Row>>>(w, ts);
        }
    };

    struct ConvertFixedTupleFrameGraph
    {
        static constexpr auto name = "convert_fixed_tuple_frame_graph";

        static Port<TS<FrameOf<Row>>> compose(Wiring &w, Port<TS<FixedRows>> ts)
        {
            return wire<stdlib::convert, TS<FrameOf<Row>>>(w, ts);
        }
    };

    struct ConvertMixedFixedTupleFrameGraph
    {
        static constexpr auto name = "convert_mixed_fixed_tuple_frame_graph";

        static Port<TS<FrameOf<Row>>> compose(Wiring &w, Port<TS<MixedFixedRows>> ts)
        {
            return wire<stdlib::convert, TS<FrameOf<Row>>>(w, ts);
        }
    };

    struct SortMetadataFrameGraph
    {
        static constexpr auto name = "sort_metadata_frame_graph";
        static Port<TS<FrameOf<Row, FrameMeta>>> compose(
            Wiring &w, Port<TS<FrameOf<Row, FrameMeta>>> ts,
            Scalar<"by", Str> by, Scalar<"descending", Bool> descending)
        {
            return wire<stdlib::sorted_>(w, ts, by, descending)
                .as<TS<FrameOf<Row, FrameMeta>>>();
        }
    };

    struct ConcatFrameGraph
    {
        static constexpr auto name = "concat_frame_graph";

        static Port<TS<FrameOf<Row>>> compose(Wiring &w, Port<TS<FrameOf<Row>>> lhs,
                                              Port<TS<FrameOf<Row>>> rhs)
        {
            return wire<stdlib::concat>(w, lhs, rhs).as<TS<FrameOf<Row>>>();
        }
    };

    struct GroupFrameGraph
    {
        static constexpr auto name = "group_frame_graph";

        static Port<TSD<Int, TS<FrameOf<Row>>>> compose(Wiring &w,
                                                        Port<TS<FrameOf<Row>>> ts,
                                                        Scalar<"by", Str> by)
        {
            return wire<stdlib::group_by>(w, ts, by).as<TSD<Int, TS<FrameOf<Row>>>>();
        }
    };

    struct JoinFrameGraph
    {
        static constexpr auto name = "join_frame_graph";

        static Port<TS<FrameOf<JoinedRow>>> compose(
            Wiring &w, Port<TS<FrameOf<Row>>> lhs, Port<TS<FrameOf<Row>>> rhs,
            Scalar<"on", Str> on, Scalar<"how", Str> how, Scalar<"suffix", Str> suffix)
        {
            auto joined = wire<stdlib::data_frame::join, TS<FrameOf<JoinedRow>>>(
                w, lhs, rhs, on, how, suffix);
            return wire<stdlib::sorted_>(w, joined, Str{"a"}, Bool{false})
                .as<TS<FrameOf<JoinedRow>>>();
        }
    };

    struct FilterFrameGraph
    {
        static constexpr auto name = "filter_frame_graph";

        static Port<TS<FrameOf<Row>>> compose(
            Wiring &w, Port<TS<FrameOf<Row>>> ts, Port<TS<Int>> a,
            Port<TS<Int>> b)
        {
            auto predicate = stdlib::to_tsb<PredicateTSB>(w, a, b);
            return wire<stdlib::data_frame::filter_frame>(
                w, ts, predicate).as<TS<FrameOf<Row>>>();
        }
    };

    struct FilterCsGraph
    {
        static constexpr auto name = "filter_cs_graph";

        static Port<TS<FrameOf<Row>>> compose(
            Wiring &w, Port<TS<FrameOf<Row>>> ts, Port<TS<Row>> predicate)
        {
            return wire<stdlib::data_frame::filter_cs>(
                w, ts, predicate).as<TS<FrameOf<Row>>>();
        }
    };

    struct ReplaceColumnGraph
    {
        static constexpr auto name = "replace_column_graph";

        static Port<TS<FrameOf<Row>>> compose(
            Wiring &w, Port<TS<FrameOf<Row>>> ts, Port<TS<Int>> b)
        {
            auto columns = stdlib::to_tsb<BColumnTSB>(w, b);
            return wire<stdlib::data_frame::with_columns, TS<FrameOf<Row>>>(
                w, ts, columns);
        }
    };

    struct ProjectColumnGraph
    {
        static constexpr auto name = "project_column_graph";

        static Port<TS<FrameOf<ProjectedRow>>> compose(
            Wiring &w, Port<TS<FrameOf<Row>>> ts, Port<TS<Int>> c)
        {
            auto columns = stdlib::to_tsb<CColumnTSB>(w, c);
            return wire<stdlib::data_frame::with_columns,
                        TS<FrameOf<ProjectedRow>>>(w, ts, columns);
        }
    };

    struct UngroupFrameGraph
    {
        static constexpr auto name = "ungroup_frame_graph";

        static Port<TS<FrameOf<Row>>> compose(
            Wiring &w, Port<TSD<Str, TS<FrameOf<Row>>>> ts)
        {
            return wire<stdlib::data_frame::ungroup>(w, ts)
                .as<TS<FrameOf<Row>>>();
        }
    };

    struct ProjectSeriesColumnGraph
    {
        static constexpr auto name = "project_series_column_graph";

        static Port<TS<FrameOf<ProjectedRow>>> compose(
            Wiring &w, Port<TS<FrameOf<Row>>> ts,
            Port<TS<SeriesOf<Int>>> c)
        {
            auto columns = stdlib::to_tsb<CSeriesColumnTSB>(w, c);
            return wire<stdlib::data_frame::with_columns,
                        TS<FrameOf<ProjectedRow>>>(w, ts, columns);
        }
    };

    struct FrameAttributeGraph
    {
        static constexpr auto name = "frame_attribute_graph";

        static Port<TS<SeriesOf<Int>>> compose(Wiring &w, Port<TS<FrameOf<Row>>> ts)
        {
            return wire<stdlib::getattr_>(w, ts, Str{"a"}).as<TS<SeriesOf<Int>>>();
        }
    };

    struct FrameColumnGraph
    {
        static constexpr auto name = "frame_column_graph";

        static Port<TS<SeriesOf<Int>>> compose(Wiring &w, Port<TS<FrameOf<Row>>> ts)
        {
            return wire<stdlib::getitem_>(w, ts, Str{"b"}).as<TS<SeriesOf<Int>>>();
        }
    };

    struct FrameRowGraph
    {
        static constexpr auto name = "frame_row_graph";

        static Port<TS<Row>> compose(Wiring &w, Port<TS<FrameOf<Row>>> ts,
                                     Scalar<"index", Int> index)
        {
            return wire<stdlib::getitem_>(w, ts, index.value()).as<TS<Row>>();
        }
    };

    struct FrameRowDynamicGraph
    {
        static constexpr auto name = "frame_row_dynamic_graph";

        static Port<TS<Row>> compose(Wiring &w, Port<TS<FrameOf<Row>>> ts,
                                     Port<TS<Int>> index)
        {
            return wire<stdlib::getitem_>(w, ts, index).as<TS<Row>>();
        }
    };

    struct MissingFrameColumnGraph
    {
        static constexpr auto name = "missing_frame_column_graph";

        static Port<TS<SeriesOf<Int>>> compose(Wiring &w, Port<TS<FrameOf<Row>>> ts)
        {
            return wire<stdlib::getattr_>(w, ts, Str{"missing"}).as<TS<SeriesOf<Int>>>();
        }
    };

    struct UngroupKeyedFrameGraph
    {
        static constexpr auto name = "ungroup_keyed_frame_graph";

        static Port<TS<FrameOf<KeyedRow>>> compose(
            Wiring &w, Port<TSD<Str, TS<FrameOf<Row>>>> ts)
        {
            return wire<stdlib::data_frame::ungroup_with_keys,
                        TS<FrameOf<KeyedRow>>>(w, ts, Str{"key"});
        }
    };

    struct SelectScalarRefDict
    {
        static constexpr auto name = "select_scalar_ref_dict";

        static void eval(In<"pick_rhs", TS<Bool>> pick_rhs,
                         In<"lhs", REF<TS<Float>>> lhs,
                         In<"rhs", REF<TS<Float>>> rhs,
                         Out<TSD<Str, REF<TS<Float>>>> out)
        {
            out.set(Str{"selected"}, pick_rhs.value() ? rhs.value() : lhs.value());
        }
    };

    struct ScalarRefToFrameGraph
    {
        static constexpr auto name = "scalar_ref_to_frame_graph";

        static Port<TS<FrameOf<ScalarRefFrameRow>>> compose(
            Wiring &w, Port<TS<Bool>> pick_rhs, Port<TS<Float>> lhs, Port<TS<Float>> rhs)
        {
            auto refs = wire<SelectScalarRefDict>(w, pick_rhs, lhs, rhs);
            return wire<stdlib::to_data_frame, TS<FrameOf<ScalarRefFrameRow>>>(
                w, refs, Str{"date"}, Str{"key"}, Str{"value"});
        }
    };

    struct SelectBundleRefDict
    {
        static constexpr auto name = "select_bundle_ref_dict";

        static void eval(In<"pick_rhs", TS<Bool>> pick_rhs,
                         In<"lhs", REF<RefBundle>> lhs,
                         In<"rhs", REF<RefBundle>> rhs,
                         Out<TSD<Str, REF<RefBundle>>> out)
        {
            out.set(Str{"selected"}, pick_rhs.value() ? rhs.value() : lhs.value());
        }
    };

    struct BundleRefToFrameGraph
    {
        static constexpr auto name = "bundle_ref_to_frame_graph";

        static Port<TS<FrameOf<BundleRefFrameRow>>> compose(
            Wiring &w, Port<TS<Bool>> pick_rhs, Port<TS<Int>> lhs_a, Port<TS<Str>> lhs_b,
            Port<TS<Int>> rhs_a, Port<TS<Str>> rhs_b)
        {
            auto lhs  = stdlib::to_tsb<RefBundle>(w, lhs_a, lhs_b);
            auto rhs  = stdlib::to_tsb<RefBundle>(w, rhs_a, rhs_b);
            auto refs = wire<SelectBundleRefDict>(w, pick_rhs, lhs, rhs);
            return wire<stdlib::to_data_frame, TS<FrameOf<BundleRefFrameRow>>>(
                w, refs, Str{"date"}, Str{"key"}, Str{"value"});
        }
    };
}

TEST_CASE("data frame operators: to_data_frame dereferences scalar TSD values across repoints")
{
    stdlib::register_standard_operators();
    const auto result = eval_node<ScalarRefToFrameGraph>(
        values<Bool>(false, true, false),
        values<Float>(1.5, none, none),
        values<Float>(2.5, none, none));

    REQUIRE(result.size() == 3);
    const std::array<Float, 3> expected{1.5, 2.5, 1.5};
    for (std::size_t index = 0; index < result.size(); ++index)
    {
        REQUIRE(result[index].has_value());
        CHECK(frame_rows(*result[index]) == 1);
        CHECK(frame_cell(*result[index], "key", scalar_descriptor<Str>::value_meta(), 0)
                  .view()
                  .checked_as<Str>() == "selected");
        CHECK(frame_cell(*result[index], "value", scalar_descriptor<Float>::value_meta(), 0)
                  .view()
                  .checked_as<Float>() == expected[index]);
    }
}

TEST_CASE("data frame operators: to_data_frame dereferences TSB-valued TSD references")
{
    stdlib::register_standard_operators();
    const auto result = eval_node<BundleRefToFrameGraph>(
        values<Bool>(false, true),
        values<Int>(1, none), values<Str>("one", none),
        values<Int>(2, none), values<Str>("two", none));

    REQUIRE(result.size() == 2);
    for (std::size_t index = 0; index < result.size(); ++index)
    {
        REQUIRE(result[index].has_value());
        CHECK(frame_rows(*result[index]) == 1);
        CHECK(frame_cell(*result[index], "key", scalar_descriptor<Str>::value_meta(), 0)
                  .view()
                  .checked_as<Str>() == "selected");
    }
    CHECK(frame_cell(*result[0], "a", scalar_descriptor<Int>::value_meta(), 0)
              .view()
              .checked_as<Int>() == 1);
    CHECK(frame_cell(*result[0], "b", scalar_descriptor<Str>::value_meta(), 0)
              .view()
              .checked_as<Str>() == "one");
    CHECK(frame_cell(*result[1], "a", scalar_descriptor<Int>::value_meta(), 0)
              .view()
              .checked_as<Int>() == 2);
    CHECK(frame_cell(*result[1], "b", scalar_descriptor<Str>::value_meta(), 0)
              .view()
              .checked_as<Str>() == "two");
}

TEST_CASE("data frame operators: sorted_ orders rows through the native wiring path")
{
    stdlib::register_standard_operators();
    const auto input    = frame({2, 1, 3}, {20, 10, 30});
    const auto expected = frame({3, 2, 1}, {30, 20, 10});
    const auto result   = eval_node<SortFrameGraph>(values<Frame>(input), Str{"a"}, Bool{true});

    REQUIRE(result.size() == 1);
    REQUIRE(result[0].has_value());
    CHECK(equals(*result[0], expected));
}

TEST_CASE("data frame operators: convert compound scalars to rows")
{
    stdlib::register_standard_operators();
    const auto single = eval_node<ConvertCompoundFrameGraph>(
        values<Value>(row_value(1, 10)));

    REQUIRE(single.size() == 1);
    REQUIRE(single[0].has_value());
    CHECK(equals(*single[0], frame({1}, {10})));

    const auto result = eval_node<ConvertFixedTupleFrameGraph>(
        values<Value>(fixed_rows(1, 10, 2, 20)));

    REQUIRE(result.size() == 1);
    REQUIRE(result[0].has_value());
    CHECK(equals(*result[0], frame({1, 2}, {10, 20})));

    CHECK_THROWS_AS(
        (void)eval_node<ConvertMixedFixedTupleFrameGraph>(values<Value>()),
        OperatorRequirementsError);
}

TEST_CASE("data frame operators: Arrow schema metadata survives row-preserving operations")
{
    stdlib::register_standard_operators();
    const Frame input = metadata_frame({2, 1}, {20, 10}, 7);
    const auto &arrow_metadata = input.table->schema()->metadata();
    REQUIRE(arrow_metadata != nullptr);
    CHECK(*arrow_metadata->Get(frame_metadata_schema_key) ==
          std::string{scalar_descriptor<FrameMeta>::value_meta()->name()});
    CHECK(*arrow_metadata->Get(frame_metadata_version_key) == "1");
    CHECK(*arrow_metadata->Get("hgraph.metadata.field.revision") == "7");
    CHECK(*arrow_metadata->Get("hgraph.metadata.field.as_of") ==
          "1970-01-01T00:00:00.123456Z");
    CHECK(*arrow_metadata->Get("hgraph.metadata.field.source") == "fixture");
    CHECK(*arrow_metadata->Get("hgraph.metadata.field.details") ==
          R"({"desk": "systematic"})");
    CHECK(frame_metadata(input, scalar_descriptor<FrameMeta>::value_meta()) ==
          metadata_value(7));
    CHECK(frame_metadata(input) == metadata_value(7));

    Frame markerless = input;
    auto markerless_metadata = markerless.table->schema()->metadata()->Copy();
    for (std::int64_t index = markerless_metadata->size(); index-- > 0;)
    {
        if (markerless_metadata->key(index) != frame_metadata_schema_key) { continue; }
        REQUIRE(markerless_metadata->Delete(index).ok());
    }
    markerless.table = markerless.table->ReplaceSchemaMetadata(
        std::move(markerless_metadata));
    CHECK(markerless.has_metadata());
    CHECK(frame_metadata(markerless, scalar_descriptor<FrameMeta>::value_meta()) ==
          metadata_value(7));
    CHECK_THROWS_AS(frame_metadata(markerless), std::invalid_argument);
    CHECK(frame_metadata_equal(markerless, input));

    Frame unknown_field = input;
    auto unknown_field_metadata =
        unknown_field.table->schema()->metadata()->Copy();
    unknown_field_metadata->Append("hgraph.metadata.field.unknown", "value");
    unknown_field.table = unknown_field.table->ReplaceSchemaMetadata(
        std::move(unknown_field_metadata));
    CHECK_THROWS_AS(
        frame_metadata(unknown_field, scalar_descriptor<FrameMeta>::value_meta()),
        std::invalid_argument);

    const auto result = eval_node<SortMetadataFrameGraph>(
        values<Frame>(input), Str{"a"}, Bool{false});
    REQUIRE(result.size() == 1);
    REQUIRE(result[0]);
    REQUIRE(result[0]->has_metadata());
    CHECK(frame_metadata_equal(*result[0], input));
    CHECK(result[0]->table->Equals(*frame({1, 2}, {10, 20}).table));

    const Frame same_revision = metadata_frame({3}, {30}, 7);
    const Frame combined = stdlib::data_frame_detail::concat_frames(input, same_revision);
    CHECK(frame_metadata_equal(combined, input));
    CHECK(frame_rows(combined) == 3);
    const Frame markerless_combined =
        stdlib::data_frame_detail::concat_frames(markerless, same_revision);
    CHECK(frame_metadata_equal(markerless_combined, input));
    CHECK(frame_rows(markerless_combined) == 3);

    const Frame other_revision = metadata_frame({3}, {30}, 8);
    CHECK_THROWS_AS(stdlib::data_frame_detail::concat_frames(input, other_revision),
                    std::invalid_argument);
    const Frame stripped = without_frame_metadata(input);
    CHECK_FALSE(stripped.has_metadata());
    REQUIRE(stripped.table->schema()->metadata() != nullptr);
    CHECK(*stripped.table->schema()->metadata()->Get("external.owner") == "research");
}

TEST_CASE("data frame operators: concat appends rows through the native wiring path")
{
    stdlib::register_standard_operators();
    const auto lhs      = frame({1, 2}, {10, 20});
    const auto rhs      = frame({3}, {30});
    const auto expected = frame({1, 2, 3}, {10, 20, 30});
    const auto result   = eval_node<ConcatFrameGraph>(values<Frame>(lhs), values<Frame>(rhs));

    REQUIRE(result.size() == 1);
    REQUIRE(result[0].has_value());
    CHECK(equals(*result[0], expected));
}

TEST_CASE("data frame operators: group_by publishes groups and removed keys natively")
{
    stdlib::register_standard_operators();
    const auto first  = frame({1, 1, 2}, {10, 11, 20});
    const auto second = frame({1}, {12});
    const auto result = eval_node<GroupFrameGraph>(values<Frame>(first, second), Str{"a"});

    REQUIRE(result.size() == 2);
    REQUIRE(result[0].has_value());
    const auto first_delta = result[0]->as_bundle();
    const auto first_modified = first_delta["modified"].as_map();
    CHECK(first_modified.size() == 2);
    const Value one{Int{1}};
    const Value two{Int{2}};
    CHECK(equals(first_modified[one.view()].checked_as<Frame>(), frame({1, 1}, {10, 11})));
    CHECK(equals(first_modified[two.view()].checked_as<Frame>(), frame({2}, {20})));

    REQUIRE(result[1].has_value());
    const auto second_delta = result[1]->as_bundle();
    CHECK(second_delta["removed"].as_set().contains(two.view()));
    const auto second_modified = second_delta["modified"].as_map();
    CHECK(second_modified.size() == 1);
    CHECK(equals(second_modified[one.view()].checked_as<Frame>(), frame({1}, {12})));
}

TEST_CASE("data frame operators: join resolves and executes through native C++ wiring")
{
    stdlib::register_standard_operators();
    const auto lhs = frame({1, 2}, {10, 20});
    const auto rhs = frame({2, 3}, {200, 300});
    const auto expected = joined_frame({1, 2}, {10, 20}, {std::nullopt, 200});
    const auto result = eval_node<JoinFrameGraph>(
        values<Frame>(lhs), values<Frame>(rhs), Str{"a"}, Str{"left"},
        Str{"_right"});

    REQUIRE(result.size() == 1);
    REQUIRE(result[0].has_value());
    CHECK(equals(*result[0], expected));
}

TEST_CASE("data frame operators: structural and compound predicates filter natively")
{
    stdlib::register_standard_operators();
    const auto input = frame({1, 2, 2}, {10, 20, 30});

    const auto structural = eval_node<FilterFrameGraph>(
        values<Frame>(input), values<Int>(2), values<Int>(none));
    REQUIRE(structural.size() == 1);
    REQUIRE(structural[0].has_value());
    CHECK(equals(*structural[0], frame({2, 2}, {20, 30})));

    const auto compound = eval_node<FilterCsGraph>(
        values<Frame>(input), values<Value>(row_value(Int{2}, Int{20})));
    REQUIRE(compound.size() == 1);
    REQUIRE(compound[0].has_value());
    CHECK(equals(*compound[0], frame({2}, {20})));
}

TEST_CASE("data frame operators: from_data_frame preserves shaped array bindings")
{
    stdlib::register_standard_operators();

    const auto array = eval_node<stdlib::from_data_frame, TS<ArrayOf<Int, 2>>>(
        array_frame(MIN_ST), Str{"date"}, Str{"key"}, Str{"value"}, TimeDelta{});
    REQUIRE(array.size() == 1);
    REQUIRE(array[0].has_value());
    CHECK(array[0]->binding() ==
          ValuePlanFactory::instance().type_for(
              scalar_descriptor<ArrayOf<Int, 2>>::value_meta()));
    CHECK(array[0]->as_list().size() == 2);
    CHECK(array[0]->as_list().at(0).checked_as<Int>() == Int{10});
    CHECK(array[0]->as_list().at(1).checked_as<Int>() == Int{20});

    const auto matrix = eval_node<stdlib::from_data_frame, TS<ArrayOf<Int, 2, 2>>>(
        matrix_frame(MIN_ST), Str{"date"}, Str{"key"}, Str{"value"}, TimeDelta{});
    REQUIRE(matrix.size() == 1);
    REQUIRE(matrix[0].has_value());
    CHECK(matrix[0]->binding() ==
          ValuePlanFactory::instance().type_for(
              scalar_descriptor<ArrayOf<Int, 2, 2>>::value_meta()));
    REQUIRE(matrix[0]->as_list().size() == 2);
    CHECK(matrix[0]->as_list().at(0).as_list().at(0).checked_as<Int>() == Int{1});
    CHECK(matrix[0]->as_list().at(1).as_list().at(1).checked_as<Int>() == Int{4});
}

TEST_CASE("data frame operators: SQL-width integers feed native bundle outputs")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(
        (eval_node<stdlib::from_data_frame, SqlRow>(
            sql_integer_frame(), Str{"date"}, Str{"key"}, Str{"value"},
            TimeDelta{})),
        values<Value>(tsb_delta<SqlRow>(Str{"Alice"}, Int{25})));
}

TEST_CASE("data frame operators: frame batches stream through one native source")
{
    stdlib::register_standard_operators();
    const auto first = scalar_batch({MIN_ST, MIN_ST + TimeDelta{1}}, {1, 2});
    const auto second = scalar_batch({MIN_ST + TimeDelta{2}}, {3});
    CHECK_OUTPUT(eval_node<FromDataFrameBatchesGraph>(
                     values<Frame>(first, none, second)),
                 values<Int>(1, 2, 3));
}

TEST_CASE("data frame operators: throttle forwards Arrow frame values unchanged")
{
    stdlib::register_standard_operators();
    const auto first  = frame({1}, {10});
    const auto second = frame({2}, {20});
    const auto third  = frame({3}, {30});

    CHECK_OUTPUT(eval_node<stdlib::throttle>(values<Frame>(first, second, third),
                                             MIN_TD * 2),
                 values<Frame>(first, none, third));
}

TEST_CASE("data frame operators: from_data_frame skips rows before evaluation start")
{
    stdlib::register_standard_operators();
    const auto input = scalar_batch(
        {MIN_ST - TimeDelta{1}, MIN_ST}, {0, 1});
    const auto result = eval_node<stdlib::from_data_frame, TS<Int>>(
        input, Str{"date"}, Str{"key"}, Str{"value"}, TimeDelta{});
    REQUIRE(result.size() == 1);
    REQUIRE(result[0].has_value());
    CHECK(result[0]->view().checked_as<Int>() == Int{1});
}

TEST_CASE("data frame operators: with_columns replaces and projects through C++ wiring")
{
    stdlib::register_standard_operators();
    const auto input = frame({1, 2}, {10, 20});

    const auto replaced = eval_node<ReplaceColumnGraph>(
        values<Frame>(input), values<Int>(99));
    REQUIRE(replaced.size() == 1);
    REQUIRE(replaced[0].has_value());
    CHECK(equals(*replaced[0], frame({1, 2}, {99, 99})));

    const auto projected = eval_node<ProjectColumnGraph>(
        values<Frame>(input), values<Int>(7));
    REQUIRE(projected.size() == 1);
    REQUIRE(projected[0].has_value());
    const auto projected_values = frame({1, 2}, {7, 7});
    const auto expected = projected_values.table->RenameColumns({"a", "c"});
    REQUIRE(expected.ok());
    CHECK(projected[0]->table->Equals(**expected));

    const auto series = eval_node<ProjectSeriesColumnGraph>(
        values<Frame>(input), values<Series>(int_series({7, 8})));
    REQUIRE(series.size() == 1);
    REQUIRE(series[0].has_value());
    const auto series_values = frame({1, 2}, {7, 8});
    const auto series_expected = series_values.table->RenameColumns({"a", "c"});
    REQUIRE(series_expected.ok());
    INFO("actual:\n" << series[0]->table->ToString()
                       << "\nexpected:\n" << (*series_expected)->ToString());
    CHECK(series[0]->table->Equals(**series_expected));
}

TEST_CASE("data frame operators: Frame column and row access use the declared row schema")
{
    stdlib::register_standard_operators();
    const auto input = frame({1, 2}, {10, 20});

    auto attribute = eval_node<FrameAttributeGraph>(values<Frame>(input));
    REQUIRE(attribute.size() == 1);
    REQUIRE(attribute[0].has_value());
    CHECK(attribute[0]->array->Equals(int_series({1, 2}).array));

    auto column = eval_node<FrameColumnGraph>(values<Frame>(input));
    REQUIRE(column.size() == 1);
    REQUIRE(column[0].has_value());
    CHECK(column[0]->array->Equals(int_series({10, 20}).array));

    CHECK_OUTPUT(eval_node<FrameRowGraph>(values<Frame>(input), Int{0}),
                 values<Value>(row_value(1, 10)));
    CHECK_OUTPUT(eval_node<FrameRowGraph>(values<Frame>(input), Int{-1}),
                 values<Value>(row_value(2, 20)));
    CHECK_OUTPUT(eval_node<FrameRowDynamicGraph>(values<Frame>(input), values<Int>(0, 1)),
                 values<Value>(row_value(1, 10), row_value(2, 20)));

    REQUIRE_THROWS(eval_node<FrameRowGraph>(values<Frame>(input), Int{2}));
    REQUIRE_THROWS(eval_node<MissingFrameColumnGraph>(values<Frame>(input)));
}

TEST_CASE("data frame operators: ungroup concatenates keyed frames and materializes keys natively")
{
    stdlib::register_standard_operators();
    const auto one = frame({1, 2}, {10, 20});
    const auto two = frame({3}, {30});
    const auto input = values<Value>(
        dict_delta<Str, TS<FrameOf<Row>>>({{Str{"one"}, one}, {Str{"two"}, two}}));

    const auto plain = eval_node<UngroupFrameGraph>(input);
    REQUIRE(plain.size() == 1);
    REQUIRE(plain[0].has_value());
    CHECK(equals(*plain[0], frame({1, 2, 3}, {10, 20, 30})));

    const auto keyed = eval_node<UngroupKeyedFrameGraph>(input);
    REQUIRE(keyed.size() == 1);
    REQUIRE(keyed[0].has_value());
    CHECK(equals(*keyed[0],
                 keyed_frame({1, 2, 3}, {10, 20, 30},
                             {"one", "one", "two"})));
}
