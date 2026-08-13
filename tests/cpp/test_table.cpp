#include <hgraph/lib/std/operators/impl/table_impl.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/temporal.h>
#include <hgraph/types/value/table_codec.h>
#include <hgraph/types/value/value_builder.h>

#include <arrow/api.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>

// Step 3 of the record/replay/table design record: the Arrow-backed Frame
// value kind + the interned per-schema TableConverter (bitemporal
// [date, as_of, *columns] rows written directly into Arrow builders) and the
// to_table / from_table operators.

struct ExtensionTableScalar
{
    std::int64_t value{0};
    bool         operator==(const ExtensionTableScalar &) const = default;
};

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<ExtensionTableScalar>
    {
        static constexpr std::string_view value{"tests.ExtensionTableScalar"};
    };
}  // namespace hgraph::static_schema_detail

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    int extension_describes{0};
    int extension_emits{0};
    int extension_applies{0};

    void describe_extension_scalar(TableLayout &layout, const TSValueTypeMetaData *schema,
                                   std::string, std::vector<std::size_t>, std::size_t)
    {
        ++extension_describes;
        layout.leaf_ts = schema;
        layout.value_col_start = layout.keys.size();
        layout.keys.push_back("value");
        layout.col_metas.push_back(scalar_descriptor<Int>::value_meta());
        layout.value_cols.push_back(
            TableLayout::Column{.name = "value", .leaf = scalar_descriptor<Int>::value_meta()});
    }

    void emit_extension_scalar(const TableLayout &, const TSInputView &ts, Int, DateTime now,
                               DateTime as_of, bool, const TableRowSink &sink, bool)
    {
        ++extension_emits;
        const Value when{now};
        const Value revision{as_of};
        const Value cell{ts.value().checked_as<ExtensionTableScalar>().value};
        sink.cell(sink.context, 0, when.view());
        sink.cell(sink.context, 1, revision.view());
        sink.cell(sink.context, 2, cell.view());
        sink.end_row(sink.context);
    }

    void apply_extension_scalar(const TableLayout &, const TableRowSource &source,
                                const TSOutputView &out)
    {
        ++extension_applies;
        const Value cell = source.cell(source.context, 0, 2);
        const Value wrapped{ExtensionTableScalar{cell.view().checked_as<Int>()}};
        apply_current_value(out, wrapped.view());
    }

    const TableTypeOps extension_table_ops{&describe_extension_scalar, &emit_extension_scalar,
                                           &apply_extension_scalar};

    [[nodiscard]] std::int64_t timestamp_at(const Frame &frame, const std::string &column,
                                            std::int64_t row)
    {
        const auto chunked = frame.table->GetColumnByName(column);
        REQUIRE(chunked != nullptr);
        return static_cast<const arrow::TimestampArray &>(*chunked->chunk(0)).Value(row);
    }
}  // namespace

TEST_CASE("table codec: an atomic value round-trips through a bitemporal "
          "single-row frame")
{
    const auto &registry = TypeRegistry::instance();
    static_cast<void>(registry);
    const auto &converter = table_converter(scalar_descriptor<Int>::value_meta());

    CHECK(converter.date_key == "__date_time__");
    CHECK(converter.as_of_key == "__as_of__");
    REQUIRE(converter.columns.size() == 1);
    CHECK(converter.columns[0].name == "value");
    CHECK(converter.arrow_schema->num_fields() == 3);
    CHECK(converter.arrow_schema->field(0)->type()->ToString() == "timestamp[us, tz=UTC]");
    REQUIRE(converter.arrow_schema->metadata() != nullptr);
    CHECK(converter.arrow_schema->metadata()->Get("hgraph.temporal.version").ValueOr("") == "2");

    const Value value{Int{42}};
    const Frame frame = single_row_frame(converter, MIN_ST, MIN_ST + TimeDelta{5}, value.view());
    REQUIRE(frame_rows(frame) == 1);
    CHECK(timestamp_at(frame, "__date_time__", 0) == MIN_ST.time_since_epoch().count());
    CHECK(timestamp_at(frame, "__as_of__", 0) ==
          (MIN_ST + TimeDelta{5}).time_since_epoch().count());

    const Value back = read_row(converter, frame, 0);
    CHECK(back.view().checked_as<Int>() == Int{42});
}

TEST_CASE("table codec: temporal version 2 Arrow mappings round-trip")
{
    using namespace std::chrono;
    const CivilDate day{year{2025}, month{11}, std::chrono::day{2}};

    const auto check_round_trip = [](const auto &value) {
        using T = std::decay_t<decltype(value)>;
        const auto &converter = table_converter(scalar_descriptor<T>::value_meta());
        const Value wrapped{value};
        const Frame frame = single_row_frame(converter, MIN_ST, MIN_ST, wrapped.view());
        CHECK(read_row(converter, frame, 0).view().template checked_as<T>() == value);
        return frame;
    };

    CHECK(check_round_trip(Instant{microseconds{123}})
              .table->schema()
              ->field(2)
              ->type()
              ->ToString() == "timestamp[us, tz=UTC]");
    CHECK(check_round_trip(Duration{123}).table->schema()->field(2)->type()->ToString() ==
          "duration[us]");
    CHECK(
        check_round_trip(CivilDateTime{day, 1, 30}).table->schema()->field(2)->type()->ToString() ==
        "timestamp[us]");
    CHECK(check_round_trip(Period{1, -2, 3}).table->schema()->field(2)->type()->ToString() ==
          "month_day_nano_interval");
    CHECK(check_round_trip(ZoneId{"America/New_York"})
              .table->schema()
              ->field(2)
              ->type()
              ->ToString() == "string");

    const auto  provider = make_time_zone_provider();
    const auto  zoned = resolve(CivilDateTime{day, 1, 30}, ZoneId{"America/New_York"}, *provider,
                                AmbiguousTimePolicy::Latest);
    const Frame zoned_frame = check_round_trip(zoned);
    CHECK(zoned_frame.table->schema()->field(2)->type()->id() == arrow::Type::STRUCT);
    REQUIRE(zoned_frame.table->schema()->metadata() != nullptr);
    CHECK_FALSE(
        zoned_frame.table->schema()->metadata()->Get("hgraph.tzdb.version").ValueOr("").empty());
    auto mismatched_metadata = zoned_frame.table->schema()->metadata()->Copy();
    REQUIRE(mismatched_metadata->Set("hgraph.tzdb.version", "deliberately-different").ok());
    const Frame mismatched_zoned{
        zoned_frame.table->ReplaceSchemaMetadata(std::move(mismatched_metadata))};
    CHECK_THROWS_AS(read_row(table_converter(scalar_descriptor<ZonedDateTime>::value_meta()),
                             mismatched_zoned, 0),
                    std::invalid_argument);

    const InstantRange range =
        InstantRange::bounded(Instant{microseconds{0}}, Instant{microseconds{10}});
    CHECK(check_round_trip(range).table->schema()->field(2)->type()->id() == arrow::Type::STRUCT);
    const InstantRangeSet ranges{
        range, InstantRange::bounded(Instant{microseconds{20}}, Instant{microseconds{30}})};
    CHECK(check_round_trip(ranges).table->schema()->field(2)->type()->id() ==
          arrow::Type::FIXED_SIZE_LIST);
}

TEST_CASE("table codec: schema-declared legacy Instant reads but version 2 "
          "rejects it")
{
    auto builder = std::make_shared<arrow::TimestampBuilder>(
        arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
    REQUIRE(builder->Append(42).ok());
    std::shared_ptr<arrow::Array> values;
    REQUIRE(builder->Finish(&values).ok());

    const auto legacy_schema =
        arrow::schema({arrow::field("value", arrow::timestamp(arrow::TimeUnit::MICRO))});
    const Frame legacy{arrow::Table::Make(legacy_schema, {values}, 1)};
    CHECK(frame_cell(legacy, "value", scalar_descriptor<Instant>::value_meta(), 0)
              .view()
              .checked_as<Instant>() == Instant{std::chrono::microseconds{42}});

    const auto version_two_schema = arrow::schema(
        legacy_schema->fields(), arrow::key_value_metadata({"hgraph.temporal.version"}, {"2"}));
    const Frame invalid{arrow::Table::Make(version_two_schema, {values}, 1)};
    CHECK_THROWS_AS(frame_cell(invalid, "value", scalar_descriptor<Instant>::value_meta(), 0),
                    std::invalid_argument);
}

TEST_CASE("table codec: Arrow utf8_view arrays decode as native strings")
{
    arrow::StringViewBuilder builder;
    const Str                expected{"a string-view value stored outside the inline view"};
    REQUIRE(builder.Append(expected).ok());

    std::shared_ptr<arrow::Array> values;
    REQUIRE(builder.Finish(&values).ok());
    REQUIRE(values->type_id() == arrow::Type::STRING_VIEW);

    const Value decoded = array_cell(*values, scalar_descriptor<Str>::value_meta(), 0);
    CHECK(decoded.view().checked_as<Str>() == expected);
}

TEST_CASE("table codec: Arrow numeric widths widen to Python-compatible native "
          "scalars")
{
    arrow::Int32Builder int_builder;
    REQUIRE(int_builder.Append(42).ok());
    std::shared_ptr<arrow::Array> integers;
    REQUIRE(int_builder.Finish(&integers).ok());
    CHECK(array_cell(*integers, scalar_descriptor<Int>::value_meta(), 0).view().checked_as<Int>() ==
          Int{42});

    arrow::FloatBuilder float_builder;
    REQUIRE(float_builder.Append(1.25F).ok());
    std::shared_ptr<arrow::Array> floats;
    REQUIRE(float_builder.Finish(&floats).ok());
    CHECK(
        array_cell(*floats, scalar_descriptor<Float>::value_meta(), 0).view().checked_as<Float>() ==
        Float{1.25});

    arrow::UInt64Builder overflow_builder;
    REQUIRE(overflow_builder.Append(std::numeric_limits<std::uint64_t>::max()).ok());
    std::shared_ptr<arrow::Array> overflow;
    REQUIRE(overflow_builder.Finish(&overflow).ok());
    CHECK_THROWS_AS(array_cell(*overflow, scalar_descriptor<Int>::value_meta(), 0),
                    std::out_of_range);
}

TEST_CASE("table codec: bytes and variadic tuples are Arrow leaf values")
{
    auto &registry = TypeRegistry::instance();

    const auto &bytes_converter = table_converter(scalar_descriptor<Bytes>::value_meta());
    const Bytes bytes_value{std::string{"a\0b", 3}};
    const Value bytes{bytes_value};
    const Frame bytes_frame = single_row_frame(bytes_converter, MIN_ST, MIN_ST, bytes.view());
    CHECK(read_row(bytes_converter, bytes_frame, 0).view().checked_as<Bytes>() == bytes_value);

    const auto *int_meta = scalar_descriptor<Int>::value_meta();
    const auto *tuple_meta = registry.list(int_meta, 0, true);
    const auto  int_binding = ValuePlanFactory::instance().type_for(int_meta);
    ListBuilder tuple_builder{int_binding};
    tuple_builder.push_back(Int{1});
    tuple_builder.push_back(Int{4});
    ListStorage tuple_storage = tuple_builder.build_storage();
    const Value tuple{compact_list_type(int_binding, *tuple_meta), &tuple_storage};

    const auto &tuple_converter = table_converter(tuple_meta);
    const Frame tuple_frame = single_row_frame(tuple_converter, MIN_ST, MIN_ST, tuple.view());
    const Value tuple_back = read_row(tuple_converter, tuple_frame, 0);
    REQUIRE(tuple_back.view().as_list().size() == 2);
    CHECK(tuple_back.view().as_list().at(0).checked_as<Int>() == Int{1});
    CHECK(tuple_back.view().as_list().at(1).checked_as<Int>() == Int{4});
}

TEST_CASE("table codec: a depth-1 bundle flattens to named columns and round-trips")
{
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *bundle_meta = registry.un_named_bundle({{"qty", int_meta}, {"symbol", str_meta}});

    const auto &converter = table_converter(bundle_meta);
    REQUIRE(converter.columns.size() == 2);
    CHECK(converter.columns[0].name == "qty");
    CHECK(converter.columns[1].name == "symbol");

    const auto    binding = ValuePlanFactory::instance().type_for(bundle_meta);
    BundleBuilder builder{binding};
    builder.set("qty", Value{Int{7}});
    builder.set("symbol", Value{Str{"VOD"}});
    const Value value = builder.build();

    const Frame frame = single_row_frame(converter, MIN_ST, MIN_ST, value.view());
    const Value back = read_row(converter, frame, 0);
    CHECK(back.view().as_bundle().at(0).checked_as<Int>() == Int{7});
    CHECK(back.view().as_bundle().at(1).checked_as<Str>() == Str{"VOD"});
}

TEST_CASE("table codec: unset bundle fields round-trip as Arrow nulls")
{
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *bundle_meta = registry.un_named_bundle({{"qty", int_meta}, {"symbol", str_meta}});

    const auto    binding = ValuePlanFactory::instance().type_for(bundle_meta);
    BundleBuilder builder{binding};
    builder.set("qty", Value{Int{7}});
    const Value value = builder.build();

    const auto &converter = table_converter(bundle_meta);
    const Frame frame = single_row_frame(converter, MIN_ST, MIN_ST, value.view());
    const auto  symbol_column = frame.table->GetColumnByName("symbol");
    REQUIRE(symbol_column != nullptr);
    REQUIRE(symbol_column->chunk(0)->IsNull(0));

    const Value back = read_row(converter, frame, 0);
    auto        bundle = back.view().as_bundle();
    CHECK(bundle.at("qty").checked_as<Int>() == Int{7});
    CHECK_FALSE(bundle.at("symbol").has_value());
}

TEST_CASE("table codec: input schemas are a minimum - extra columns pass, "
          "missing columns throw")
{
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *wide_meta = registry.un_named_bundle({{"qty", int_meta}, {"symbol", str_meta}});
    const auto *narrow_meta = registry.un_named_bundle({{"qty", int_meta}});
    const auto *other_meta = registry.un_named_bundle({{"price", int_meta}});

    const auto    binding = ValuePlanFactory::instance().type_for(wide_meta);
    BundleBuilder builder{binding};
    builder.set("qty", Value{Int{9}});
    builder.set("symbol", Value{Str{"BP"}});
    const Frame wide =
        single_row_frame(table_converter(wide_meta), MIN_ST, MIN_ST, builder.build().view());

    // Narrow reader over a wide frame: the extra column is ignored.
    const Value narrow = read_row(table_converter(narrow_meta), wide, 0);
    CHECK(narrow.view().as_bundle().at(0).checked_as<Int>() == Int{9});

    // A required column that is absent throws.
    CHECK_THROWS_AS((void)read_row(table_converter(other_meta), wide, 0), std::invalid_argument);
}

namespace
{
    struct TableRoundTripGraph
    {
        [[maybe_unused]] static constexpr auto name = "table_round_trip_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> ts)
        {
            auto frame = wire<stdlib::to_table>(w, ts);
            return wire<stdlib::from_table, TS<Float>>(w, frame).as<TS<Float>>();
        }
    };

    struct ExtensionTableRoundTripGraph
    {
        [[maybe_unused]] static constexpr auto name = "extension_table_round_trip_graph";

        static Port<TS<ExtensionTableScalar>> compose(Wiring &w, Port<TS<ExtensionTableScalar>> ts)
        {
            auto rows = wire<stdlib::to_table>(w, ts);
            return wire<stdlib::from_table, TS<ExtensionTableScalar>>(w, rows)
                .as<TS<ExtensionTableScalar>>();
        }
    };

    struct ExtensionRecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "extension_record_graph";

        static Port<TS<ExtensionTableScalar>> compose(Wiring &w, Port<TS<ExtensionTableScalar>> ts)
        {
            wire<stdlib::record>(w, ts, Str{"values"}, arg<"recordable_id">(Str{"extension"}));
            return ts;
        }
    };

    struct ExtensionReplayGraph
    {
        [[maybe_unused]] static constexpr auto name = "extension_replay_graph";

        static Port<TS<ExtensionTableScalar>> compose(Wiring &w)
        {
            return wire<stdlib::replay, TS<ExtensionTableScalar>>(
                       w, Str{"values"}, arg<"recordable_id">(Str{"extension"}))
                .as<TS<ExtensionTableScalar>>();
        }
    };
}  // namespace

TEST_CASE("table operators: to_table emits one bitemporal tuple row per tick")
{
    stdlib::register_standard_operators();

    // The tuple-row parity protocol (design record step 6): TS<int> emits
    // TS<tuple[datetime, datetime, int]>; eval_node<Operator> returns
    // type-erased Values.
    auto rows = eval_node<stdlib::to_table>(values<Int>(42, none, -1));
    REQUIRE(rows.size() == 3);
    REQUIRE(rows[0].has_value());
    CHECK_FALSE(rows[1].has_value());
    REQUIRE(rows[2].has_value());

    const auto first = rows[0]->view();
    const auto last = rows[2]->view();
    REQUIRE(first.is_tuple());
    CHECK(first.as_tuple().at(2).checked_as<Int>() == Int{42});
    CHECK(last.as_tuple().at(2).checked_as<Int>() == Int{-1});
    // The date cell carries the tick's evaluation time (cycle 2 = start + 2
    // ticks).
    CHECK(last.as_tuple().at(0).checked_as<DateTime>() == MIN_ST + TimeDelta{2});
}

TEST_CASE("table operators: to_table honours the configured as_of override")
{
    stdlib::register_standard_operators();
    GlobalContext  context;
    const DateTime fixed = MIN_ST + TimeDelta{123456};
    record_replay::set_config(context.state().view(), record_replay::Config{.as_of = fixed});

    auto rows = eval_node<stdlib::to_table>(values<Int>(1));
    REQUIRE(rows[0].has_value());
    CHECK(rows[0]->view().as_tuple().at(1).checked_as<DateTime>() == fixed);
}

TEST_CASE("table layouts are isolated by seeded column configuration")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(
        context.state().view(),
        record_replay::Config{.date_key = "event_time", .as_of_key = "observed_at"});

    const auto &layout = stdlib::table_ts_detail::ts_table_layout(
        TypeRegistry::instance().ts(scalar_descriptor<Int>::value_meta()), "event_time",
        "observed_at");
    REQUIRE(layout.keys.size() == 3);
    CHECK(layout.keys[0] == "event_time");
    CHECK(layout.keys[1] == "observed_at");
    CHECK(layout.keys[2] == "value");

    auto rows = eval_node<stdlib::to_table>(values<Int>(1));
    REQUIRE(rows[0].has_value());
    CHECK(rows[0]->view().is_tuple());
}

TEST_CASE("table operators: to_table -> from_table round-trips through a graph")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<TableRoundTripGraph>(values<Float>(1.5, none, -0.25)),
                 values<Float>(1.5, none, -0.25));
}

TEST_CASE("table type ops: an extension supplies describe emit and apply once "
          "per plan")
{
    stdlib::register_standard_operators();
    const auto *schema =
        TypeRegistry::instance().ts(scalar_descriptor<ExtensionTableScalar>::value_meta());
    extension_describes = 0;
    extension_emits = 0;
    extension_applies = 0;
    register_table_type_ops(schema, extension_table_ops);

    CHECK_OUTPUT(
        eval_node<ExtensionTableRoundTripGraph>(
            values<ExtensionTableScalar>(ExtensionTableScalar{1}, none, ExtensionTableScalar{3})),
        values<ExtensionTableScalar>(ExtensionTableScalar{1}, none, ExtensionTableScalar{3}));

    CHECK(extension_describes == 1);
    CHECK(extension_emits == 2);
    CHECK(extension_applies == 2);
}

TEST_CASE("table type ops: stored replay dispatches through the extension "
          "operation")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(
        context.state().view(),
        record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});
    const auto *schema =
        TypeRegistry::instance().ts(scalar_descriptor<ExtensionTableScalar>::value_meta());
    extension_describes = 0;
    extension_emits = 0;
    extension_applies = 0;
    register_table_type_ops(schema, extension_table_ops);

    const auto expected =
        values<ExtensionTableScalar>(ExtensionTableScalar{2}, none, ExtensionTableScalar{5});
    (void)eval_node<ExtensionRecordGraph>(expected);
    CHECK_OUTPUT(eval_node<ExtensionReplayGraph>(), expected);

    CHECK(extension_describes == 1);
    CHECK(extension_emits == 2);
    CHECK(extension_applies == 2);
}

TEST_CASE("table type ops: registry reset withdraws exact-schema overrides")
{
    const auto *schema =
        TypeRegistry::instance().ts(scalar_descriptor<ExtensionTableScalar>::value_meta());
    register_table_type_ops(schema, extension_table_ops);
    CHECK(&table_type_ops(schema) == &extension_table_ops);

    reset_all_registries();

    const auto *reseeded_schema =
        TypeRegistry::instance().ts(scalar_descriptor<ExtensionTableScalar>::value_meta());
    CHECK(&table_type_ops(reseeded_schema) != &extension_table_ops);
}
