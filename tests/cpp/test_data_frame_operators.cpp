#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/operators/impl/data_frame_impl.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value_builder.h>

#include <arrow/api.h>

#include <catch2/catch_test_macros.hpp>

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

    record_replay::store_write("tests.data_frame.metadata", markerless);
    const Frame stored =
        record_replay::store_read("tests.data_frame.metadata");
    CHECK(frame_metadata_equal(stored, markerless));
    CHECK(frame_metadata(stored, scalar_descriptor<FrameMeta>::value_meta()) ==
          metadata_value(7));

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
