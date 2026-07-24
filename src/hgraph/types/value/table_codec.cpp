#include <hgraph/types/value/table_codec.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/temporal.h>
#include <hgraph/types/value/value_builder.h>

#include <arrow/api.h>

#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        using Column = TableConverter::Column;

        [[noreturn]] void fail_status(const arrow::Status &status, const char *what)
        {
            throw std::runtime_error(fmt::format("table codec: {} failed: {}", what, status.ToString()));
        }

        void check(const arrow::Status &status, const char *what)
        {
            if (!status.ok()) { fail_status(status, what); }
        }

        void append_null(arrow::ArrayBuilder &builder)
        {
            check(builder.AppendNull(), "append null");
        }

        // ---------------------------------------------------------------
        // Per-leaf append / read thunks (fn-ptrs; first param the column)
        // ---------------------------------------------------------------

        template <typename Builder, typename Get>
        void append_with(arrow::ArrayBuilder &builder, const Get &get, const char *what)
        {
            check(static_cast<Builder &>(builder).Append(get()), what);
        }

        void append_bool(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            append_with<arrow::BooleanBuilder>(builder, [&] { return leaf.checked_as<Bool>(); }, "append bool");
        }

        void append_int(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            append_with<arrow::Int64Builder>(builder, [&] { return leaf.checked_as<Int>(); }, "append int");
        }

        void append_float(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            append_with<arrow::DoubleBuilder>(builder, [&] { return leaf.checked_as<Float>(); }, "append float");
        }

        void append_str(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            check(static_cast<arrow::StringBuilder &>(builder).Append(leaf.checked_as<Str>()), "append str");
        }

        void append_bytes(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            check(static_cast<arrow::BinaryBuilder &>(builder).Append(leaf.checked_as<Bytes>().data),
                  "append bytes");
        }

        void append_date(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            const auto days =
                std::chrono::sys_days{leaf.checked_as<Date>()}.time_since_epoch().count();
            append_with<arrow::Date32Builder>(builder, [&] { return static_cast<std::int32_t>(days); },
                                              "append date");
        }

        void append_datetime(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            append_with<arrow::TimestampBuilder>(
                builder, [&] { return leaf.checked_as<DateTime>().time_since_epoch().count(); },
                "append datetime");
        }

        void append_timedelta(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            append_with<arrow::DurationBuilder>(builder, [&] { return leaf.checked_as<TimeDelta>().count(); },
                                                "append timedelta");
        }

        void append_time(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            append_with<arrow::Time64Builder>(builder, [&] { return leaf.checked_as<Time>().microseconds; },
                                              "append time");
        }

        void append_civil_datetime(const Column &, const ValueView &leaf,
                                   arrow::ArrayBuilder &builder)
        {
            append_with<arrow::TimestampBuilder>(
                builder,
                [&] {
                    return leaf.checked_as<CivilDateTime>()
                        .epoch_microseconds();
                },
                "append civil datetime");
        }

        void append_period(const Column &, const ValueView &leaf,
                           arrow::ArrayBuilder &builder)
        {
            const Period value = leaf.checked_as<Period>();
            check(static_cast<arrow::MonthDayNanoIntervalBuilder &>(builder)
                      .Append({value.total_months(), value.days(), 0}),
                  "append period");
        }

        void append_zone_id(const Column &, const ValueView &leaf,
                            arrow::ArrayBuilder &builder)
        {
            check(static_cast<arrow::StringBuilder &>(builder).Append(
                      leaf.checked_as<ZoneId>().name()),
                  "append zone id");
        }

        [[nodiscard]] std::shared_ptr<arrow::DataType> zoned_datetime_type()
        {
            static const auto type = arrow::struct_({
                arrow::field(
                    "instant",
                    arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
                arrow::field("zone", arrow::utf8()),
                arrow::field("offset_seconds", arrow::int32()),
            });
            return type;
        }

        [[nodiscard]] std::shared_ptr<arrow::DataType> instant_range_type()
        {
            static const auto type = arrow::struct_({
                arrow::field(
                    "start",
                    arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
                arrow::field(
                    "end",
                    arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
                arrow::field("lower_closed", arrow::boolean()),
                arrow::field("upper_closed", arrow::boolean()),
                arrow::field("empty", arrow::boolean()),
            });
            return type;
        }

        [[nodiscard]] std::shared_ptr<arrow::DataType> civil_date_range_type()
        {
            static const auto type = arrow::struct_({
                arrow::field("start", arrow::date32()),
                arrow::field("end", arrow::date32()),
                arrow::field("lower_closed", arrow::boolean()),
                arrow::field("upper_closed", arrow::boolean()),
                arrow::field("empty", arrow::boolean()),
            });
            return type;
        }

        template <typename Range, typename AppendEndpoint>
        void append_range_value(const Range &range,
                                arrow::StructBuilder &builder,
                                AppendEndpoint &&append_endpoint)
        {
            check(builder.Append(), "append range");
            auto &start = *builder.field_builder(0);
            auto &end = *builder.field_builder(1);
            if (range.lower_bounded())
            {
                append_endpoint(start, range.lower_value());
            }
            else { append_null(start); }
            if (range.upper_bounded())
            {
                append_endpoint(end, range.upper_value());
            }
            else { append_null(end); }
            append_with<arrow::BooleanBuilder>(
                *builder.field_builder(2),
                [&] {
                    return range.lower_boundary() == Boundary::Closed;
                },
                "append lower range boundary");
            append_with<arrow::BooleanBuilder>(
                *builder.field_builder(3),
                [&] {
                    return range.upper_boundary() == Boundary::Closed;
                },
                "append upper range boundary");
            append_with<arrow::BooleanBuilder>(
                *builder.field_builder(4), [&] { return range.empty(); },
                "append empty range marker");
        }

        void append_instant_range_value(
            const InstantRange &range, arrow::StructBuilder &builder)
        {
            append_range_value(
                range, builder,
                [](arrow::ArrayBuilder &endpoint, Instant value) {
                    append_with<arrow::TimestampBuilder>(
                        endpoint,
                        [&] { return value.time_since_epoch().count(); },
                        "append instant range endpoint");
                });
        }

        void append_civil_date_range_value(
            const CivilDateRange &range, arrow::StructBuilder &builder)
        {
            append_range_value(
                range, builder,
                [](arrow::ArrayBuilder &endpoint, CivilDate value) {
                    const auto days =
                        std::chrono::sys_days{value}
                            .time_since_epoch()
                            .count();
                    append_with<arrow::Date32Builder>(
                        endpoint,
                        [&] {
                            return static_cast<std::int32_t>(days);
                        },
                        "append civil date range endpoint");
                });
        }

        void append_zoned_datetime(const Column &, const ValueView &leaf,
                                   arrow::ArrayBuilder &builder)
        {
            const auto value = leaf.checked_as<ZonedDateTime>();
            auto &structure = static_cast<arrow::StructBuilder &>(builder);
            check(structure.Append(), "append zoned datetime");
            append_with<arrow::TimestampBuilder>(
                *structure.field_builder(0),
                [&] {
                    return value.instant().time_since_epoch().count();
                },
                "append zoned instant");
            check(static_cast<arrow::StringBuilder &>(
                      *structure.field_builder(1))
                      .Append(value.zone().name()),
                  "append zoned zone id");
            append_with<arrow::Int32Builder>(
                *structure.field_builder(2),
                [&] { return value.offset_seconds(); },
                "append zoned offset");
        }

        void append_instant_range(const Column &, const ValueView &leaf,
                                  arrow::ArrayBuilder &builder)
        {
            append_instant_range_value(
                leaf.checked_as<InstantRange>(),
                static_cast<arrow::StructBuilder &>(builder));
        }

        void append_civil_date_range(const Column &, const ValueView &leaf,
                                     arrow::ArrayBuilder &builder)
        {
            append_civil_date_range_value(
                leaf.checked_as<CivilDateRange>(),
                static_cast<arrow::StructBuilder &>(builder));
        }

        template <typename RangeSet, typename AppendRange>
        void append_range_set_value(const RangeSet &value,
                                    arrow::ArrayBuilder &builder,
                                    AppendRange &&append_range)
        {
            auto &list =
                static_cast<arrow::FixedSizeListBuilder &>(builder);
            check(list.Append(), "append fixed range set");
            auto &ranges =
                static_cast<arrow::StructBuilder &>(*list.value_builder());
            for (std::size_t index = 0; index < RangeSet::capacity(); ++index)
            {
                if (index < value.size())
                {
                    append_range(value[index], ranges);
                }
                else
                {
                    check(ranges.AppendNull(),
                          "append unused fixed range slot");
                }
            }
        }

        void append_instant_range_set(const Column &, const ValueView &leaf,
                                      arrow::ArrayBuilder &builder)
        {
            append_range_set_value(
                leaf.checked_as<InstantRangeSet>(), builder,
                [](const InstantRange &range,
                   arrow::StructBuilder &ranges) {
                    append_instant_range_value(range, ranges);
                });
        }

        void append_civil_date_range_set(const Column &,
                                         const ValueView &leaf,
                                         arrow::ArrayBuilder &builder)
        {
            append_range_set_value(
                leaf.checked_as<CivilDateRangeSet>(), builder,
                [](const CivilDateRange &range,
                   arrow::StructBuilder &ranges) {
                    append_civil_date_range_value(range, ranges);
                });
        }

        Value read_bool(const Column &, const arrow::Array &array, std::int64_t row)
        {
            return Value{Bool{static_cast<const arrow::BooleanArray &>(array).Value(row)}};
        }

        Value read_int(const Column &, const arrow::Array &array, std::int64_t row)
        {
            return Value{Int{static_cast<const arrow::Int64Array &>(array).Value(row)}};
        }

        Value read_float(const Column &, const arrow::Array &array, std::int64_t row)
        {
            return Value{Float{static_cast<const arrow::DoubleArray &>(array).Value(row)}};
        }

        Value read_str(const Column &, const arrow::Array &array, std::int64_t row)
        {
            // polars-built tables carry large_utf8 (64-bit offsets); reading
            // one through StringArray misreads the offset buffer.
            if (array.type_id() == arrow::Type::LARGE_STRING)
            {
                return Value{Str{static_cast<const arrow::LargeStringArray &>(array).GetView(row)}};
            }
            return Value{Str{static_cast<const arrow::StringArray &>(array).GetView(row)}};
        }

        Value read_bytes(const Column &, const arrow::Array &array, std::int64_t row)
        {
            if (array.type_id() == arrow::Type::LARGE_BINARY)
            {
                return Value{
                    Bytes{std::string{static_cast<const arrow::LargeBinaryArray &>(array).GetView(row)}}};
            }
            return Value{Bytes{std::string{static_cast<const arrow::BinaryArray &>(array).GetView(row)}}};
        }

        Value read_date(const Column &, const arrow::Array &array, std::int64_t row)
        {
            const auto days = static_cast<const arrow::Date32Array &>(array).Value(row);
            return Value{Date{std::chrono::sys_days{std::chrono::days{days}}}};
        }

        Value read_datetime(const Column &, const arrow::Array &array, std::int64_t row)
        {
            const auto micros = static_cast<const arrow::TimestampArray &>(array).Value(row);
            return Value{DateTime{std::chrono::microseconds{micros}}};
        }

        Value read_timedelta(const Column &, const arrow::Array &array, std::int64_t row)
        {
            return Value{TimeDelta{static_cast<const arrow::DurationArray &>(array).Value(row)}};
        }

        Value read_time(const Column &, const arrow::Array &array, std::int64_t row)
        {
            return Value{Time{static_cast<const arrow::Time64Array &>(array).Value(row)}};
        }

        Value read_civil_datetime(const Column &, const arrow::Array &array,
                                  std::int64_t row)
        {
            const auto micros =
                static_cast<const arrow::TimestampArray &>(array).Value(row);
            return Value{
                CivilDateTime::from_epoch_microseconds(micros)};
        }

        Value read_period(const Column &, const arrow::Array &array,
                          std::int64_t row)
        {
            const auto value =
                static_cast<const arrow::MonthDayNanoIntervalArray &>(array)
                    .Value(row);
            if (value.nanoseconds != 0)
            {
                throw std::invalid_argument(
                    "table codec: period interval nanoseconds must be zero");
            }
            return Value{Period::from_total(value.months, value.days)};
        }

        Value read_zone_id(const Column &, const arrow::Array &array,
                           std::int64_t row)
        {
            if (array.type_id() == arrow::Type::LARGE_STRING)
            {
                return Value{ZoneId{
                    static_cast<const arrow::LargeStringArray &>(array)
                        .GetView(row)}};
            }
            return Value{ZoneId{
                static_cast<const arrow::StringArray &>(array).GetView(row)}};
        }

        Value read_zoned_datetime(const Column &, const arrow::Array &array,
                                  std::int64_t row)
        {
            const auto &structure =
                static_cast<const arrow::StructArray &>(array);
            const auto instant = static_cast<const arrow::TimestampArray &>(
                                     *structure.field(0))
                                     .Value(row);
            const auto zone_text =
                static_cast<const arrow::StringArray &>(*structure.field(1))
                    .GetView(row);
            const auto offset =
                static_cast<const arrow::Int32Array &>(*structure.field(2))
                    .Value(row);
            const ZoneId zone{zone_text};
            static const auto provider = make_time_zone_provider();
            const auto value =
                at_zone(Instant{Duration{instant}}, zone, *provider);
            if (value.offset_seconds() != offset)
            {
                throw std::invalid_argument(
                    "table codec: zoned datetime offset disagrees with provider");
            }
            return Value{value};
        }

        template <typename Range, typename ReadEndpoint>
        [[nodiscard]] Range read_range_value(
            const arrow::StructArray &structure, std::int64_t row,
            ReadEndpoint &&read_endpoint)
        {
            const auto &empty =
                static_cast<const arrow::BooleanArray &>(*structure.field(4));
            if (empty.Value(row)) { return Range::make_empty(); }
            const auto &lower =
                static_cast<const arrow::BooleanArray &>(*structure.field(2));
            const auto &upper =
                static_cast<const arrow::BooleanArray &>(*structure.field(3));
            const Boundary lower_boundary =
                lower.Value(row) ? Boundary::Closed : Boundary::Open;
            const Boundary upper_boundary =
                upper.Value(row) ? Boundary::Closed : Boundary::Open;
            const auto &start = *structure.field(0);
            const auto &end = *structure.field(1);
            if (!start.IsNull(row) && !end.IsNull(row))
            {
                return Range::bounded(
                    read_endpoint(start, row), read_endpoint(end, row),
                    lower_boundary, upper_boundary);
            }
            if (!start.IsNull(row))
            {
                return Range::from(read_endpoint(start, row),
                                   lower_boundary);
            }
            if (!end.IsNull(row))
            {
                return Range::until(read_endpoint(end, row),
                                    upper_boundary);
            }
            return Range::all();
        }

        [[nodiscard]] InstantRange read_instant_range_value(
            const arrow::StructArray &structure, std::int64_t row)
        {
            return read_range_value<InstantRange>(
                structure, row,
                [](const arrow::Array &endpoint, std::int64_t index) {
                    return Instant{Duration{
                        static_cast<const arrow::TimestampArray &>(endpoint)
                            .Value(index)}};
                });
        }

        [[nodiscard]] CivilDateRange read_civil_date_range_value(
            const arrow::StructArray &structure, std::int64_t row)
        {
            return read_range_value<CivilDateRange>(
                structure, row,
                [](const arrow::Array &endpoint, std::int64_t index) {
                    return CivilDate{std::chrono::sys_days{
                        std::chrono::days{
                            static_cast<const arrow::Date32Array &>(endpoint)
                                .Value(index)}}};
                });
        }

        Value read_instant_range(const Column &, const arrow::Array &array,
                                 std::int64_t row)
        {
            return Value{read_instant_range_value(
                static_cast<const arrow::StructArray &>(array), row)};
        }

        Value read_civil_date_range(const Column &, const arrow::Array &array,
                                    std::int64_t row)
        {
            return Value{read_civil_date_range_value(
                static_cast<const arrow::StructArray &>(array), row)};
        }

        template <typename Range, typename RangeSet, typename ReadRange>
        Value read_range_set_value(const arrow::Array &array,
                                   std::int64_t row,
                                   ReadRange &&read_range)
        {
            const auto &list =
                static_cast<const arrow::FixedSizeListArray &>(array);
            const auto &ranges =
                static_cast<const arrow::StructArray &>(*list.values());
            std::array<Range, 2> values{};
            std::size_t size = 0;
            const auto offset = list.value_offset(row);
            for (std::int64_t index = offset;
                 index < offset + list.value_length(row); ++index)
            {
                if (!ranges.IsNull(index))
                {
                    values[size++] = read_range(ranges, index);
                }
            }
            return Value{RangeSet{
                std::span<const Range>{values.data(), size}}};
        }

        Value read_instant_range_set(const Column &,
                                     const arrow::Array &array,
                                     std::int64_t row)
        {
            return read_range_set_value<InstantRange, InstantRangeSet>(
                array, row,
                [](const arrow::StructArray &ranges, std::int64_t index) {
                    return read_instant_range_value(ranges, index);
                });
        }

        Value read_civil_date_range_set(const Column &,
                                        const arrow::Array &array,
                                        std::int64_t row)
        {
            return read_range_set_value<CivilDateRange, CivilDateRangeSet>(
                array, row,
                [](const arrow::StructArray &ranges, std::int64_t index) {
                    return read_civil_date_range_value(ranges, index);
                });
        }

        struct LeafOps
        {
            std::shared_ptr<arrow::DataType> type;
            Column::AppendFn                 append;
            Column::ReadFn                   read;
        };

        void append_sequence(const Column &, const ValueView &, arrow::ArrayBuilder &);
        Value read_sequence(const Column &, const arrow::Array &, std::int64_t);

        [[nodiscard]] LeafOps leaf_ops_for(const ValueTypeMetaData *meta)
        {
            if (meta == scalar_descriptor<Bool>::value_meta())
            {
                return {arrow::boolean(), &append_bool, &read_bool};
            }
            if (meta == scalar_descriptor<Int>::value_meta()) { return {arrow::int64(), &append_int, &read_int}; }
            if (meta == scalar_descriptor<Float>::value_meta())
            {
                return {arrow::float64(), &append_float, &read_float};
            }
            if (meta == scalar_descriptor<Str>::value_meta()) { return {arrow::utf8(), &append_str, &read_str}; }
            if (meta == scalar_descriptor<Bytes>::value_meta())
            {
                return {arrow::binary(), &append_bytes, &read_bytes};
            }
            if (meta == scalar_descriptor<Date>::value_meta())
            {
                return {arrow::date32(), &append_date, &read_date};
            }
            if (meta == scalar_descriptor<DateTime>::value_meta())
            {
                return {arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                        &append_datetime, &read_datetime};
            }
            if (meta == scalar_descriptor<TimeDelta>::value_meta())
            {
                return {arrow::duration(arrow::TimeUnit::MICRO), &append_timedelta, &read_timedelta};
            }
            if (meta == scalar_descriptor<Time>::value_meta())
            {
                return {arrow::time64(arrow::TimeUnit::MICRO), &append_time, &read_time};
            }
            if (meta == scalar_descriptor<CivilDateTime>::value_meta())
            {
                return {arrow::timestamp(arrow::TimeUnit::MICRO),
                        &append_civil_datetime, &read_civil_datetime};
            }
            if (meta == scalar_descriptor<Period>::value_meta())
            {
                return {arrow::month_day_nano_interval(), &append_period,
                        &read_period};
            }
            if (meta == scalar_descriptor<ZoneId>::value_meta())
            {
                return {arrow::utf8(), &append_zone_id, &read_zone_id};
            }
            if (meta == scalar_descriptor<ZonedDateTime>::value_meta())
            {
                return {zoned_datetime_type(), &append_zoned_datetime,
                        &read_zoned_datetime};
            }
            if (meta == scalar_descriptor<InstantRange>::value_meta())
            {
                return {instant_range_type(), &append_instant_range,
                        &read_instant_range};
            }
            if (meta == scalar_descriptor<CivilDateRange>::value_meta())
            {
                return {civil_date_range_type(), &append_civil_date_range,
                        &read_civil_date_range};
            }
            if (meta == scalar_descriptor<InstantRangeSet>::value_meta())
            {
                return {
                    arrow::fixed_size_list(instant_range_type(), 2),
                    &append_instant_range_set, &read_instant_range_set};
            }
            if (meta == scalar_descriptor<CivilDateRangeSet>::value_meta())
            {
                return {
                    arrow::fixed_size_list(civil_date_range_type(), 2),
                    &append_civil_date_range_set,
                    &read_civil_date_range_set};
            }
            if ((meta->value_kind() == ValueTypeKind::List ||
                 (meta->value_kind() == ValueTypeKind::Tuple && meta->has(ValueTypeFlags::VariadicTuple))) &&
                meta->element_type != nullptr)
            {
                return {arrow::list(leaf_ops_for(meta->element_type).type), &append_sequence, &read_sequence};
            }
            throw std::logic_error(fmt::format("table codec: unsupported leaf scalar '{}'",
                                               meta != nullptr && !meta->name().empty() ? meta->name()
                                                                                      : std::string_view{"?"}));
        }

        void validate_array_type(const arrow::Array &array,
                                 const ValueTypeMetaData *leaf,
                                 std::string_view source)
        {
            const LeafOps ops = leaf_ops_for(leaf);
            const auto actual = array.type();
            const bool compatible = actual->Equals(ops.type) ||
                                    (leaf ==
                                         scalar_descriptor<DateTime>::value_meta() &&
                                     actual->Equals(arrow::timestamp(
                                         arrow::TimeUnit::MICRO))) ||
                                    (ops.type->id() == arrow::Type::STRING &&
                                     actual->id() == arrow::Type::LARGE_STRING) ||
                                    (ops.type->id() == arrow::Type::BINARY &&
                                     actual->id() == arrow::Type::LARGE_BINARY);
            if (!compatible)
            {
                throw std::invalid_argument(fmt::format(
                    "table codec: {} has Arrow type '{}', expected '{}' for native scalar '{}'",
                    source, actual->ToString(), ops.type->ToString(),
                    leaf != nullptr && !leaf->name().empty()
                        ? leaf->name()
                        : std::string_view{"?"}));
            }
        }

        [[nodiscard]] bool temporal_version_two(
            const arrow::Schema &schema) noexcept
        {
            const auto &metadata = schema.metadata();
            if (!metadata) { return false; }
            const int index =
                metadata->FindKey("hgraph.temporal.version");
            return index >= 0 && metadata->value(index) == "2";
        }

        void validate_versioned_array_type(
            const arrow::Array &array, const ValueTypeMetaData *leaf,
            const arrow::Schema &schema, std::string_view source)
        {
            validate_array_type(array, leaf, source);
            if (leaf == scalar_descriptor<DateTime>::value_meta() &&
                temporal_version_two(schema))
            {
                const auto &timestamp =
                    static_cast<const arrow::TimestampType &>(*array.type());
                if (timestamp.timezone() != "UTC")
                {
                    throw std::invalid_argument(fmt::format(
                        "table codec: {} is a version-2 Instant but has "
                        "timezone-free Arrow type '{}'",
                        source, array.type()->ToString()));
                }
            }
            if (leaf == scalar_descriptor<ZonedDateTime>::value_meta())
            {
                const auto &metadata = schema.metadata();
                const int version_index =
                    metadata != nullptr
                        ? metadata->FindKey("hgraph.tzdb.version")
                        : -1;
                if (version_index < 0)
                {
                    throw std::invalid_argument(fmt::format(
                        "table codec: {} is a ZonedDateTime but the Arrow "
                        "schema has no hgraph.tzdb.version",
                        source));
                }
                static const auto provider = make_time_zone_provider();
                const std::string_view encoded_version =
                    metadata->value(version_index);
                if (encoded_version != provider->version())
                {
                    throw std::invalid_argument(fmt::format(
                        "table codec: {} uses TZDB version '{}', but the "
                        "active provider uses '{}'",
                        source, encoded_version, provider->version()));
                }
            }
        }

        void append_sequence(const Column &column, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            auto &list_builder = static_cast<arrow::ListBuilder &>(builder);
            check(list_builder.Append(), "append sequence");
            auto       &value_builder = *list_builder.value_builder();
            const auto *element_meta  = column.leaf_meta->element_type;
            const auto  element_ops   = leaf_ops_for(element_meta);
            const Column element_column{.leaf_meta = element_meta, .type = element_ops.type};

            const auto append_values = [&](const auto &sequence) {
                for (const ValueView value : sequence.values())
                {
                    if (value.has_value()) { element_ops.append(element_column, value, value_builder); }
                    else { append_null(value_builder); }
                }
            };
            if (leaf.schema()->value_kind() == ValueTypeKind::Tuple) { append_values(leaf.as_tuple()); }
            else { append_values(leaf.as_list()); }
        }

        Value read_sequence(const Column &column, const arrow::Array &array, std::int64_t row)
        {
            const auto &list         = static_cast<const arrow::ListArray &>(array);
            const auto *sequence_meta = column.leaf_meta;
            const auto *element_meta  = sequence_meta->element_type;
            const auto  element_binding = ValuePlanFactory::instance().type_for(element_meta);
            if (element_binding == nullptr)
            {
                throw std::logic_error("table codec: sequence element has no value binding");
            }

            const auto  element_ops = leaf_ops_for(element_meta);
            const Column element_column{.leaf_meta = element_meta, .type = element_ops.type};
            const auto  &values = *list.values();
            const auto   begin  = list.value_offset(row);
            const auto   end    = begin + list.value_length(row);
            ListBuilder builder{element_binding};
            for (std::int64_t index = begin; index < end; ++index)
            {
                if (values.IsNull(index))
                {
                    builder.push_back_unset();
                    continue;
                }
                Value value = element_ops.read(element_column, values, index);
                builder.push_back_copy(value.view().data());
            }
            ListStorage storage = builder.build_storage();
            return Value{compact_list_type(element_binding, *sequence_meta), &storage};
        }

        // ---------------------------------------------------------------
        // Synthesis + interning. NO locks: wiring and evaluation are
        // single-threaded (the OperatorRegistry precedent) — push senders,
        // the only cross-thread entry, never touch converters.
        // ---------------------------------------------------------------

        struct ConverterKey
        {
            const ValueTypeMetaData *meta{nullptr};
            std::string              date_key{};
            std::string              as_of_key{};

            bool operator==(const ConverterKey &) const = default;
        };

        struct ConverterKeyHash
        {
            std::size_t operator()(const ConverterKey &key) const noexcept
            {
                std::size_t seed = std::hash<const ValueTypeMetaData *>{}(key.meta);
                seed ^= std::hash<std::string>{}(key.date_key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                seed ^= std::hash<std::string>{}(key.as_of_key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                return seed;
            }
        };

        std::unordered_map<ConverterKey, std::unique_ptr<TableConverter>, ConverterKeyHash> g_converters;

        [[nodiscard]] const TableConverter *build_converter(const ValueTypeMetaData *meta,
                                                            std::string_view date_key,
                                                            std::string_view as_of_key)
        {
            if (meta == nullptr) { throw std::logic_error("table codec: null value schema"); }

            auto converter  = std::make_unique<TableConverter>();
            converter->meta = meta;
            converter->date_key = date_key;
            converter->as_of_key = as_of_key;

            const auto add_leaf = [&](std::string name, const ValueTypeMetaData *leaf,
                                      std::vector<std::size_t> path) {
                LeafOps ops = leaf_ops_for(leaf);
                converter->columns.push_back(Column{.name      = std::move(name),
                                                    .leaf_meta = leaf,
                                                    .path      = std::move(path),
                                                    .type      = ops.type,
                                                    .append    = ops.append,
                                                    .read      = ops.read});
            };

            switch (meta->value_kind())
            {
                case ValueTypeKind::Atomic: add_leaf("value", meta, {}); break;
                case ValueTypeKind::List:
                    if (!meta->has(ValueTypeFlags::VariadicTuple))
                    {
                        throw std::logic_error("table codec: only variadic tuple list leaves are supported");
                    }
                    add_leaf("value", meta, {});
                    break;
                case ValueTypeKind::Bundle: {
                    for (std::size_t i = 0; i < meta->field_count; ++i)
                    {
                        const auto &field = meta->fields[i];
                        if (field.type->value_kind() != ValueTypeKind::Atomic)
                        {
                            throw std::logic_error(
                                "table codec: nested compound bundle fields are not supported yet "
                                "(depth-1 bundles only; see the design record)");
                        }
                        add_leaf(field.name != nullptr ? field.name : fmt::format("f{}", i), field.type, {i});
                    }
                    break;
                }
                default:
                    throw std::logic_error(fmt::format(
                        "table codec: unsupported value kind for '{}' (atomics and depth-1 bundles in v1)",
                        meta->name()));
            }

            arrow::FieldVector fields;
            fields.reserve(converter->columns.size() + 2);
            fields.push_back(arrow::field(
                converter->date_key,
                arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")));
            fields.push_back(arrow::field(
                converter->as_of_key,
                arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")));
            for (const auto &column : converter->columns) { fields.push_back(arrow::field(column.name, column.type)); }
            std::vector<std::string> metadata_keys{
                "hgraph.temporal.version"};
            std::vector<std::string> metadata_values{"2"};
            if (std::any_of(
                    converter->columns.begin(), converter->columns.end(),
                    [](const Column &column) {
                        return column.leaf_meta ==
                               scalar_descriptor<ZonedDateTime>::value_meta();
                    }))
            {
                metadata_keys.emplace_back("hgraph.tzdb.version");
                metadata_values.emplace_back(
                    make_time_zone_provider()->version());
            }
            auto metadata = arrow::key_value_metadata(
                std::move(metadata_keys), std::move(metadata_values));
            converter->arrow_schema =
                arrow::schema(std::move(fields), std::move(metadata));

            const auto *raw = converter.get();
            g_converters.emplace(ConverterKey{meta, std::string{date_key}, std::string{as_of_key}},
                                 std::move(converter));
            return raw;
        }

        [[nodiscard]] std::unique_ptr<arrow::ArrayBuilder> make_builder(const std::shared_ptr<arrow::DataType> &type)
        {
            std::unique_ptr<arrow::ArrayBuilder> builder;
            check(arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder), "make builder");
            return builder;
        }

        [[nodiscard]] std::shared_ptr<arrow::Array> finish(arrow::ArrayBuilder &builder)
        {
            std::shared_ptr<arrow::Array> array;
            check(builder.Finish(&array), "finish array");
            return array;
        }

        void append_column(const Column &column, const ValueView &value, arrow::ArrayBuilder &builder)
        {
            if (!value.has_value())
            {
                append_null(builder);
                return;
            }
            column.append(column, value, builder);
        }
    }  // namespace

    struct FrameRecorder::Impl
    {
        const TableConverter                             *converter{nullptr};
        std::unique_ptr<arrow::ArrayBuilder>              date_builder{};
        std::unique_ptr<arrow::ArrayBuilder>              as_of_builder{};
        std::vector<std::unique_ptr<arrow::ArrayBuilder>> column_builders{};
        std::int64_t                                      rows{0};
    };

    FrameRecorder::FrameRecorder(const TableConverter &converter) : impl_(std::make_unique<Impl>())
    {
        impl_->converter     = &converter;
        impl_->date_builder  = make_builder(
            arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"));
        impl_->as_of_builder = make_builder(
            arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"));
        impl_->column_builders.reserve(converter.columns.size());
        for (const auto &column : converter.columns) { impl_->column_builders.push_back(make_builder(column.type)); }
    }

    FrameRecorder::FrameRecorder(FrameRecorder &&) noexcept            = default;
    FrameRecorder &FrameRecorder::operator=(FrameRecorder &&) noexcept = default;
    FrameRecorder::~FrameRecorder()                                    = default;

    void FrameRecorder::append(DateTime value_time, DateTime as_of, const ValueView &value)
    {
        check(static_cast<arrow::TimestampBuilder &>(*impl_->date_builder)
                  .Append(value_time.time_since_epoch().count()),
              "append value time");
        check(static_cast<arrow::TimestampBuilder &>(*impl_->as_of_builder).Append(as_of.time_since_epoch().count()),
              "append as-of");
        const auto &columns = impl_->converter->columns;
        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            const auto &column = columns[i];
            if (column.path.empty()) { append_column(column, value, *impl_->column_builders[i]); }
            else { append_column(column, value.as_bundle().at(column.path.front()), *impl_->column_builders[i]); }
        }
        ++impl_->rows;
    }

    Frame FrameRecorder::finish()
    {
        arrow::ArrayVector arrays;
        arrays.reserve(impl_->converter->columns.size() + 2);
        arrays.push_back(hgraph::finish(*impl_->date_builder));
        arrays.push_back(hgraph::finish(*impl_->as_of_builder));
        for (auto &builder : impl_->column_builders) { arrays.push_back(hgraph::finish(*builder)); }
        return Frame{arrow::Table::Make(impl_->converter->arrow_schema, std::move(arrays), impl_->rows)};
    }

    DateTime frame_value_time(const TableConverter &converter, const Frame &frame, std::int64_t row)
    {
        const auto chunked = frame.table->GetColumnByName(converter.date_key);
        if (chunked == nullptr)
        {
            throw std::invalid_argument("table codec: frame is missing its value-time column");
        }
        validate_versioned_array_type(
            *chunked->chunk(0),
            scalar_descriptor<DateTime>::value_meta(),
            *frame.table->schema(), "value-time column");
        const auto &array = static_cast<const arrow::TimestampArray &>(*chunked->chunk(0));
        return DateTime{std::chrono::microseconds{array.Value(row)}};
    }

    const TableConverter &table_converter(const ValueTypeMetaData *meta, std::string_view date_key,
                                          std::string_view as_of_key)
    {
        // Composed + interned once per schema. Per-tick operator paths do NOT
        // call this: nodes resolve their converter in ``start`` and carry it
        // in node State (the lifecycle "compose once" contract).
        ConverterKey key{meta, std::string{date_key}, std::string{as_of_key}};
        if (const auto it = g_converters.find(key); it != g_converters.end()) { return *it->second; }
        return *build_converter(meta, date_key, as_of_key);
    }

    void clear_table_converters() noexcept { g_converters.clear(); }

    Frame single_row_frame(const TableConverter &converter, DateTime value_time, DateTime as_of,
                           const ValueView &value)
    {
        arrow::ArrayVector arrays;
        arrays.reserve(converter.columns.size() + 2);

        const auto append_timestamp = [&](DateTime when) {
            auto builder = make_builder(
                arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"));
            check(static_cast<arrow::TimestampBuilder &>(*builder).Append(when.time_since_epoch().count()),
                  "append timestamp");
            arrays.push_back(finish(*builder));
        };
        append_timestamp(value_time);
        append_timestamp(as_of);

        for (const auto &column : converter.columns)
        {
            auto builder = make_builder(column.type);
            // v1 paths are depth <= 1 (atomic or depth-1 bundle field).
            if (column.path.empty()) { append_column(column, value, *builder); }
            else { append_column(column, value.as_bundle().at(column.path.front()), *builder); }
            arrays.push_back(finish(*builder));
        }

        return Frame{arrow::Table::Make(converter.arrow_schema, std::move(arrays), 1)};
    }

    Frame frame_from_rows(const TableConverter &converter, std::span<const ValueView> rows,
                          std::size_t first_column)
    {
        const auto        &columns = converter.columns;
        arrow::FieldVector fields;
        fields.reserve(columns.size());
        for (const auto &column : columns) { fields.push_back(arrow::field(column.name, column.type)); }
        const auto schema = arrow::schema(
            std::move(fields), converter.arrow_schema->metadata());

        std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
        builders.reserve(columns.size());
        for (const auto &column : columns) { builders.push_back(make_builder(column.type)); }

        for (const ValueView &row : rows)
        {
            const auto tuple = row.as_tuple();
            for (std::size_t i = 0; i < columns.size(); ++i)
            {
                append_column(columns[i], tuple.at(first_column + i), *builders[i]);
            }
        }

        arrow::ArrayVector arrays;
        arrays.reserve(columns.size());
        for (auto &builder : builders) { arrays.push_back(finish(*builder)); }
        return Frame{arrow::Table::Make(schema, std::move(arrays),
                                        static_cast<std::int64_t>(rows.size()))};
    }

    Frame frame_from_values(const TableConverter &converter, std::span<const Value> rows)
    {
        const auto        &columns = converter.columns;
        arrow::FieldVector fields;
        fields.reserve(columns.size());
        for (const auto &column : columns) { fields.push_back(arrow::field(column.name, column.type)); }
        const auto schema = arrow::schema(
            std::move(fields), converter.arrow_schema->metadata());

        std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
        builders.reserve(columns.size());
        for (const auto &column : columns) { builders.push_back(make_builder(column.type)); }

        for (const Value &row : rows)
        {
            const ValueView view = row.view();
            for (std::size_t i = 0; i < columns.size(); ++i)
            {
                const auto &column = columns[i];
                if (column.path.empty()) { append_column(column, view, *builders[i]); }
                else { append_column(column, view.as_bundle().at(column.path.front()), *builders[i]); }
            }
        }

        arrow::ArrayVector arrays;
        arrays.reserve(columns.size());
        for (auto &builder : builders) { arrays.push_back(finish(*builder)); }
        return Frame{arrow::Table::Make(schema, std::move(arrays),
                                        static_cast<std::int64_t>(rows.size()))};
    }

    std::vector<std::string> frame_column_names(const Frame &frame)
    {
        std::vector<std::string> names;
        if (!frame.has_value()) { return names; }
        for (const auto &field : frame.table->schema()->fields()) { names.push_back(field->name()); }
        return names;
    }

    Value array_cell(const arrow::Array &array, const ValueTypeMetaData *leaf,
                     std::int64_t row)
    {
        if (leaf == nullptr) { throw std::invalid_argument("table codec: Arrow array cell has no native schema"); }
        if (row < 0 || row >= array.length())
        {
            throw std::out_of_range("table codec: Arrow array row is out of range");
        }
        if (array.IsNull(row)) { return Value{}; }

        validate_array_type(array, leaf, "Arrow array");
        const LeafOps ops = leaf_ops_for(leaf);
        const Column temp{.leaf_meta = leaf, .type = ops.type};
        return ops.read(temp, array, row);
    }

    arrow::Datum arrow_scalar(const ValueView &value, const ValueTypeMetaData *leaf)
    {
        if (leaf == nullptr) { throw std::invalid_argument("table codec: null scalar schema"); }
        const LeafOps ops = leaf_ops_for(leaf);
        const Column column{.leaf_meta = leaf, .type = ops.type};
        auto builder = make_builder(ops.type);
        if (value.has_value()) { ops.append(column, value, *builder); }
        else { append_null(*builder); }
        const auto array = finish(*builder);
        auto scalar = array->GetScalar(0);
        if (!scalar.ok()) { fail_status(scalar.status(), "read encoded scalar"); }
        return arrow::Datum{std::move(*scalar)};
    }

    Value frame_cell(const Frame &frame, std::string_view column, const ValueTypeMetaData *leaf,
                     std::int64_t row)
    {
        if (!frame.has_value()) { throw std::invalid_argument("table codec: cannot read an empty frame"); }
        const auto chunked = frame.table->GetColumnByName(std::string{column});
        if (chunked == nullptr)
        {
            throw std::invalid_argument(fmt::format("table codec: frame is missing column '{}'", column));
        }
        std::shared_ptr<arrow::Array> array;
        if (chunked->num_chunks() != 1)
        {
            const auto combined = arrow::Concatenate(chunked->chunks());
            if (!combined.ok()) { fail_status(combined.status(), "concatenate chunks"); }
            array = *combined;
        }
        else { array = chunked->chunk(0); }
        validate_versioned_array_type(
            *array, leaf, *frame.table->schema(),
            fmt::format("column '{}'", column));
        return array_cell(*array, leaf, row);
    }

    Frame frame_rename_columns(const Frame &frame,
                               std::span<const std::pair<std::string, std::string>> renames)
    {
        if (!frame.has_value()) { return frame; }
        std::vector<std::string> names;
        for (const auto &field : frame.table->schema()->fields())
        {
            std::string name = field->name();
            for (const auto &[from, to] : renames)
            {
                if (name == from)
                {
                    name = to;
                    break;
                }
            }
            names.push_back(std::move(name));
        }
        auto renamed = frame.table->RenameColumns(names);
        if (!renamed.ok()) { fail_status(renamed.status(), "rename columns"); }
        return Frame{*renamed};
    }

    Value read_row(const TableConverter &converter, const Frame &frame, std::int64_t row)
    {
        if (!frame.has_value()) { throw std::invalid_argument("table codec: cannot read from an empty frame"); }
        const arrow::Table &table = *frame.table;
        if (row < 0 || row >= table.num_rows())
        {
            throw std::out_of_range("table codec: row index out of range");
        }

        const auto column_array = [&](const std::string &name) -> std::shared_ptr<arrow::Array> {
            const auto chunked = table.GetColumnByName(name);
            if (chunked == nullptr)
            {
                // The input-minimum rule: required columns must be present
                // (extra frame columns are simply never asked for).
                throw std::invalid_argument(fmt::format("table codec: frame is missing column '{}'", name));
            }
            if (chunked->num_chunks() != 1)
            {
                const auto combined = arrow::Concatenate(chunked->chunks());
                if (!combined.ok()) { fail_status(combined.status(), "concatenate chunks"); }
                return *combined;
            }
            return chunked->chunk(0);
        };

        if (converter.meta->value_kind() != ValueTypeKind::Bundle)
        {
            const auto &column = converter.columns.front();
            auto array = column_array(column.name);
            validate_versioned_array_type(
                *array, column.leaf_meta, *table.schema(),
                fmt::format("column '{}'", column.name));
            if (array->IsNull(row)) { return Value{*converter.meta}; }
            return column.read(column, *array, row);
        }

        const auto binding = ValuePlanFactory::instance().type_for(converter.meta);
        BundleBuilder builder{binding};
        for (const auto &column : converter.columns)
        {
            auto array = column_array(column.name);
            validate_versioned_array_type(
                *array, column.leaf_meta, *table.schema(),
                fmt::format("column '{}'", column.name));
            if (array->IsNull(row)) { continue; }
            builder.set(column.path.front(), column.read(column, *array, row));
        }
        return builder.build();
    }
}  // namespace hgraph
