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
#include <limits>
#include <memory>
#include <mutex>
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
            switch (array.type_id())
            {
                case arrow::Type::INT8:
                    return Value{Int{static_cast<const arrow::Int8Array &>(array).Value(row)}};
                case arrow::Type::INT16:
                    return Value{Int{static_cast<const arrow::Int16Array &>(array).Value(row)}};
                case arrow::Type::INT32:
                    return Value{Int{static_cast<const arrow::Int32Array &>(array).Value(row)}};
                case arrow::Type::INT64:
                    return Value{Int{static_cast<const arrow::Int64Array &>(array).Value(row)}};
                case arrow::Type::UINT8:
                    return Value{Int{static_cast<const arrow::UInt8Array &>(array).Value(row)}};
                case arrow::Type::UINT16:
                    return Value{Int{static_cast<const arrow::UInt16Array &>(array).Value(row)}};
                case arrow::Type::UINT32:
                    return Value{Int{static_cast<const arrow::UInt32Array &>(array).Value(row)}};
                case arrow::Type::UINT64: {
                    const auto value = static_cast<const arrow::UInt64Array &>(array).Value(row);
                    if (value > static_cast<std::uint64_t>(std::numeric_limits<Int>::max()))
                    {
                        throw std::out_of_range(
                            "table codec: unsigned integer does not fit native scalar 'int'");
                    }
                    return Value{Int{static_cast<Int>(value)}};
                }
                default:
                    throw std::logic_error("table codec: non-integer array reached integer reader");
            }
        }

        Value read_float(const Column &, const arrow::Array &array, std::int64_t row)
        {
            switch (array.type_id())
            {
                case arrow::Type::FLOAT:
                    return Value{Float{static_cast<const arrow::FloatArray &>(array).Value(row)}};
                case arrow::Type::DOUBLE:
                    return Value{Float{static_cast<const arrow::DoubleArray &>(array).Value(row)}};
                default:
                    throw std::logic_error("table codec: non-floating array reached float reader");
            }
        }

        Value read_str(const Column &, const arrow::Array &array, std::int64_t row)
        {
            // polars-built tables carry large_utf8 (64-bit offsets); reading
            // one through StringArray misreads the offset buffer. Arrow's
            // utf8_view representation likewise uses a distinct array layout.
            if (array.type_id() == arrow::Type::STRING_VIEW)
            {
                return Value{Str{static_cast<const arrow::StringViewArray &>(array).GetView(row)}};
            }
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

/**
 * The atomic leaves the table codec can carry, as ONE list: the dispatch in
 * ``leaf_ops_for`` and the inventory ``table_atomic_leaf_metas`` reports are
 * both generated from it, so a leaf cannot be added to one without the other.
 * That matters because the inventory is what the round-trip test sizes itself
 * against - a hand-kept second copy would let a new leaf ship with the test
 * still claiming full coverage.
 */
#define HGRAPH_TABLE_ATOMIC_LEAVES(X)                                             \
    X(Bool, arrow::boolean(), bool)                                               \
    X(Int, arrow::int64(), int)                                                   \
    X(Float, arrow::float64(), float)                                             \
    X(Str, arrow::utf8(), str)                                                    \
    X(Bytes, arrow::binary(), bytes)                                              \
    X(Date, arrow::date32(), date)                                                \
    X(DateTime, arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), datetime)        \
    X(TimeDelta, arrow::duration(arrow::TimeUnit::MICRO), timedelta)              \
    X(Time, arrow::time64(arrow::TimeUnit::MICRO), time)                          \
    X(CivilDateTime, arrow::timestamp(arrow::TimeUnit::MICRO), civil_datetime)    \
    X(Period, arrow::month_day_nano_interval(), period)                           \
    X(ZoneId, arrow::utf8(), zone_id)                                             \
    X(ZonedDateTime, zoned_datetime_type(), zoned_datetime)                       \
    X(InstantRange, instant_range_type(), instant_range)                          \
    X(CivilDateRange, civil_date_range_type(), civil_date_range)                  \
    X(InstantRangeSet, arrow::fixed_size_list(instant_range_type(), 2),           \
      instant_range_set)                                                          \
    X(CivilDateRangeSet, arrow::fixed_size_list(civil_date_range_type(), 2),      \
      civil_date_range_set)

        [[nodiscard]] LeafOps leaf_ops_for(const ValueTypeMetaData *meta)
        {
#define HGRAPH_TABLE_LEAF_DISPATCH(Type, ArrowType, Suffix)                       \
    if (meta == scalar_descriptor<Type>::value_meta())                            \
    {                                                                             \
        return {ArrowType, &append_##Suffix, &read_##Suffix};                     \
    }
            HGRAPH_TABLE_ATOMIC_LEAVES(HGRAPH_TABLE_LEAF_DISPATCH)
#undef HGRAPH_TABLE_LEAF_DISPATCH
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
            const auto integer_compatible = [&] {
                if (leaf != scalar_descriptor<Int>::value_meta()) { return false; }
                switch (actual->id())
                {
                    case arrow::Type::INT8:
                    case arrow::Type::INT16:
                    case arrow::Type::INT32:
                    case arrow::Type::INT64:
                    case arrow::Type::UINT8:
                    case arrow::Type::UINT16:
                    case arrow::Type::UINT32:
                    case arrow::Type::UINT64: return true;
                    default: return false;
                }
            }();
            const bool floating_compatible =
                leaf == scalar_descriptor<Float>::value_meta() &&
                (actual->id() == arrow::Type::FLOAT ||
                 actual->id() == arrow::Type::DOUBLE);
            const bool compatible = actual->Equals(ops.type) ||
                                    integer_compatible || floating_compatible ||
                                    (leaf ==
                                         scalar_descriptor<DateTime>::value_meta() &&
                                     actual->Equals(arrow::timestamp(
                                         arrow::TimeUnit::MICRO))) ||
                                    (ops.type->id() == arrow::Type::STRING &&
                                     (actual->id() == arrow::Type::LARGE_STRING ||
                                      actual->id() == arrow::Type::STRING_VIEW)) ||
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

        /**
         * The schema metadata a recorded table must carry.
         *
         * Shared by both writers, because the reader below REJECTS a table
         * that lacks it: a ZonedDateTime column without hgraph.tzdb.version
         * fails validation, so a writer that omits it produces recordings its
         * own replay refuses. TableRecorder omitted it and only atomic-leaf
         * tests covered that path, so nothing caught it.
         */
        [[nodiscard]] std::shared_ptr<arrow::KeyValueMetadata> table_schema_metadata(
            std::span<const ValueTypeMetaData *const> leaf_metas)
        {
            std::vector<std::string> keys{"hgraph.temporal.version"};
            std::vector<std::string> values{"2"};
            if (std::any_of(leaf_metas.begin(), leaf_metas.end(),
                            [](const ValueTypeMetaData *leaf) {
                                return leaf == scalar_descriptor<ZonedDateTime>::value_meta();
                            }))
            {
                keys.emplace_back("hgraph.tzdb.version");
                values.emplace_back(make_time_zone_provider()->version());
            }
            return arrow::key_value_metadata(std::move(keys), std::move(values));
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

            if (sequence_meta->is_shaped_array() && sequence_meta->fixed_size != 0)
            {
                const auto sequence_binding =
                    ValuePlanFactory::instance().type_for(sequence_meta);
                if (sequence_binding == nullptr)
                {
                    throw std::logic_error(
                        "table codec: shaped array has no value binding");
                }

                Value result{sequence_binding};
                auto  output = result.as_list().begin_mutation();
                output.resize(static_cast<std::size_t>(end - begin));
                for (std::int64_t index = begin; index < end; ++index)
                {
                    if (values.IsNull(index)) { continue; }
                    Value value = element_ops.read(element_column, values, index);
                    output.at(static_cast<std::size_t>(index - begin))
                        .copy_from(value.view());
                }
                return result;
            }

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

        std::mutex g_converters_mutex;
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
            std::vector<const ValueTypeMetaData *> leaf_metas;
            leaf_metas.reserve(converter->columns.size());
            for (const auto &column : converter->columns) { leaf_metas.push_back(column.leaf_meta); }
            converter->arrow_schema =
                arrow::schema(std::move(fields), table_schema_metadata(leaf_metas));

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


    struct TableRecorder::Impl
    {
        std::vector<Column>                              columns{};
        std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders{};
        std::shared_ptr<arrow::Schema>                   schema{};
        std::int64_t                                     rows{0};
        /** Row index each column last received a cell for; -1 for none. Cheaper
            than clearing a mask per row, and it makes a duplicate delivery
            detectable. */
        std::vector<std::int64_t>                        written{};
    };

    TableRecorder::TableRecorder(std::span<const std::string> names,
                                 std::span<const ValueTypeMetaData *const> leaf_metas)
        : impl_(std::make_unique<Impl>())
    {
        if (names.size() != leaf_metas.size())
        {
            throw std::invalid_argument("table recorder: column names and leaf metadata differ in length");
        }
        arrow::FieldVector fields;
        fields.reserve(names.size());
        impl_->columns.reserve(names.size());
        impl_->builders.reserve(names.size());
        for (std::size_t i = 0; i < names.size(); ++i)
        {
            if (leaf_metas[i] == nullptr)
            {
                throw std::invalid_argument(
                    fmt::format("table recorder: column '{}' has no leaf metadata", names[i]));
            }
            const LeafOps ops = leaf_ops_for(leaf_metas[i]);
            impl_->columns.push_back(Column{.name      = names[i],
                                            .leaf_meta = leaf_metas[i],
                                            .path      = {},
                                            .type      = ops.type,
                                            .append    = ops.append,
                                            .read      = ops.read});
            impl_->builders.push_back(make_builder(ops.type));
            impl_->written.push_back(-1);
            fields.push_back(arrow::field(names[i], ops.type));
        }
        // The same metadata the converter writes: replay validates against it,
        // so a recording without it is one this codec will not read back.
        impl_->schema = arrow::schema(std::move(fields), table_schema_metadata(leaf_metas));
    }

    TableRecorder::TableRecorder(TableRecorder &&) noexcept            = default;
    TableRecorder &TableRecorder::operator=(TableRecorder &&) noexcept = default;
    TableRecorder::~TableRecorder()                                    = default;

    void TableRecorder::append_cell(std::size_t column, const ValueView &value)
    {
        if (column >= impl_->columns.size())
        {
            throw std::invalid_argument(fmt::format("table recorder: column {} of {}", column,
                                                    impl_->columns.size()));
        }
        if (impl_->written[column] == impl_->rows)
        {
            throw std::invalid_argument(fmt::format(
                "table recorder: column '{}' delivered twice in one row", impl_->columns[column].name));
        }
        if (!value.has_value()) { return; }   // absent; end_row makes it a null
        append_column(impl_->columns[column], value, *impl_->builders[column]);
        impl_->written[column] = impl_->rows;
    }

    void TableRecorder::end_row()
    {
        for (std::size_t i = 0; i < impl_->columns.size(); ++i)
        {
            if (impl_->written[i] == impl_->rows) { continue; }
            check(impl_->builders[i]->AppendNull(), "append null");
        }
        ++impl_->rows;
    }

    std::int64_t TableRecorder::rows() const noexcept { return impl_->rows; }

    const std::shared_ptr<arrow::Schema> &TableRecorder::arrow_schema() const noexcept { return impl_->schema; }

    Frame TableRecorder::finish()
    {
        arrow::ArrayVector arrays;
        arrays.reserve(impl_->builders.size());
        for (auto &builder : impl_->builders) { arrays.push_back(hgraph::finish(*builder)); }
        const std::int64_t rows = impl_->rows;
        impl_->rows             = 0;
        std::fill(impl_->written.begin(), impl_->written.end(), -1);
        return Frame{arrow::Table::Make(impl_->schema, std::move(arrays), rows)};
    }

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

    Value read_table_cell(const ValueTypeMetaData *leaf_meta, const arrow::Array &array,
                          const arrow::Schema &schema, std::int64_t row)
    {
        if (leaf_meta == nullptr) { throw std::invalid_argument("table codec: cell has no leaf metadata"); }
        if (array.IsNull(row)) { return Value{}; }
        // BEFORE dispatching: the readers use unchecked derived-array casts
        // (the Boolean reader casts straight to arrow::BooleanArray), so a
        // stored column whose Arrow type disagrees with the leaf metadata is
        // undefined behaviour rather than a diagnosable error. read_row has
        // always validated; this path did not, and a frame can now come from a
        // Python-supplied store that the runtime never wrote.
        validate_versioned_array_type(array, leaf_meta, schema, "table cell");
        const LeafOps ops = leaf_ops_for(leaf_meta);
        const Column  column{.name      = {},
                             .leaf_meta = leaf_meta,
                             .path      = {},
                             .type      = ops.type,
                             .append    = ops.append,
                             .read      = ops.read};
        return ops.read(column, array, row);
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
        std::scoped_lock lock{g_converters_mutex};
        ConverterKey key{meta, std::string{date_key}, std::string{as_of_key}};
        if (const auto it = g_converters.find(key); it != g_converters.end()) { return *it->second; }
        return *build_converter(meta, date_key, as_of_key);
    }

    void clear_table_converters() noexcept
    {
        std::scoped_lock lock{g_converters_mutex};
        g_converters.clear();
    }

    std::vector<const ValueTypeMetaData *> table_atomic_leaf_metas()
    {
        // Built fresh rather than cached in a static: the metas are registry
        // -owned, and a reset frees them.
        std::vector<const ValueTypeMetaData *> metas;
#define HGRAPH_TABLE_LEAF_META(Type, ArrowType, Suffix)                           \
    metas.push_back(scalar_descriptor<Type>::value_meta());
        HGRAPH_TABLE_ATOMIC_LEAVES(HGRAPH_TABLE_LEAF_META)
#undef HGRAPH_TABLE_LEAF_META
        return metas;
    }

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

    Value frame_cell_at(const Frame &frame, int column, const ValueTypeMetaData *leaf,
                        std::int64_t row)
    {
        if (!frame.has_value()) { throw std::invalid_argument("table codec: cannot read an empty frame"); }
        if (column < 0 || column >= frame.table->num_columns())
        {
            throw std::out_of_range(
                fmt::format("table codec: column index {} is outside the frame", column));
        }
        const auto chunked = frame.table->column(column);
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
            fmt::format("column '{}'", frame.table->schema()->field(column)->name()));
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
