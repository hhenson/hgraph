#ifndef HGRAPH_LIB_STD_STANDARD_TYPES_H
#define HGRAPH_LIB_STD_STANDARD_TYPES_H

#include <hgraph/types/frame.h>
#include <hgraph/types/series.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/temporal.h>

#include <cstdint>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <string_view>

namespace hgraph::stdlib
{
    /**
     * Pointers for the standard scalar schemas and their common ``TS`` / ``TSS``
     * schemas after registration.
     *
     * User-facing names intentionally follow the Python-facing vocabulary:
     * ``int`` is ``Int``, ``float`` is ``Float``, ``date`` is ``Date``,
     * ``datetime`` is ``DateTime``, ``timedelta`` is ``TimeDelta``, and
     * ``str`` is ``Str``.
     * Explicit fixed-width aliases such as ``int8`` and
     * ``uint32`` are also registered for callers that need narrower storage.
     */
    struct RegisteredStandardTypes
    {
        const ValueTypeMetaData *bool_type{nullptr};
        const ValueTypeMetaData *int_type{nullptr};
        const ValueTypeMetaData *float_type{nullptr};
        const ValueTypeMetaData *date_type{nullptr};
        const ValueTypeMetaData *datetime_type{nullptr};
        const ValueTypeMetaData *timedelta_type{nullptr};
        const ValueTypeMetaData *time_type{nullptr};
        const ValueTypeMetaData *str_type{nullptr};
        const ValueTypeMetaData *bytes_type{nullptr};
        const ValueTypeMetaData *frame_type{nullptr};
        const ValueTypeMetaData *series_type{nullptr};
        const ValueTypeMetaData *period_type{nullptr};
        const ValueTypeMetaData *civil_datetime_type{nullptr};
        const ValueTypeMetaData *zone_id_type{nullptr};
        const ValueTypeMetaData *zoned_datetime_type{nullptr};
        const ValueTypeMetaData *instant_range_type{nullptr};
        const ValueTypeMetaData *civil_date_range_type{nullptr};
        const ValueTypeMetaData *instant_range_set_type{nullptr};
        const ValueTypeMetaData *civil_date_range_set_type{nullptr};
        const ValueTypeMetaData *month_end_policy_type{nullptr};
        const ValueTypeMetaData *ambiguous_time_policy_type{nullptr};
        const ValueTypeMetaData *nonexistent_time_policy_type{nullptr};
        const ValueTypeMetaData *boundary_type{nullptr};

        const ValueTypeMetaData *int8_type{nullptr};
        const ValueTypeMetaData *int16_type{nullptr};
        const ValueTypeMetaData *int32_type{nullptr};
        const ValueTypeMetaData *int64_type{nullptr};
        const ValueTypeMetaData *uint8_type{nullptr};
        const ValueTypeMetaData *uint16_type{nullptr};
        const ValueTypeMetaData *uint32_type{nullptr};
        const ValueTypeMetaData *uint64_type{nullptr};
        const ValueTypeMetaData *float32_type{nullptr};
        const ValueTypeMetaData *float64_type{nullptr};

        const TSValueTypeMetaData *ts_bool{nullptr};
        const TSValueTypeMetaData *ts_int{nullptr};
        const TSValueTypeMetaData *ts_float{nullptr};
        const TSValueTypeMetaData *ts_date{nullptr};
        const TSValueTypeMetaData *ts_datetime{nullptr};
        const TSValueTypeMetaData *ts_timedelta{nullptr};
        const TSValueTypeMetaData *ts_time{nullptr};
        const TSValueTypeMetaData *ts_str{nullptr};
        const TSValueTypeMetaData *ts_bytes{nullptr};
        const TSValueTypeMetaData *ts_frame{nullptr};
        const TSValueTypeMetaData *ts_series{nullptr};
        const TSValueTypeMetaData *ts_period{nullptr};
        const TSValueTypeMetaData *ts_civil_datetime{nullptr};
        const TSValueTypeMetaData *ts_zone_id{nullptr};
        const TSValueTypeMetaData *ts_zoned_datetime{nullptr};
        const TSValueTypeMetaData *ts_instant_range{nullptr};
        const TSValueTypeMetaData *ts_civil_date_range{nullptr};
        const TSValueTypeMetaData *ts_instant_range_set{nullptr};
        const TSValueTypeMetaData *ts_civil_date_range_set{nullptr};

        const TSValueTypeMetaData *tss_bool{nullptr};
        const TSValueTypeMetaData *tss_int{nullptr};
        const TSValueTypeMetaData *tss_float{nullptr};
        const TSValueTypeMetaData *tss_date{nullptr};
        const TSValueTypeMetaData *tss_datetime{nullptr};
        const TSValueTypeMetaData *tss_timedelta{nullptr};
        const TSValueTypeMetaData *tss_time{nullptr};
        const TSValueTypeMetaData *tss_str{nullptr};
        const TSValueTypeMetaData *tss_bytes{nullptr};
        const TSValueTypeMetaData *tss_period{nullptr};
        const TSValueTypeMetaData *tss_civil_datetime{nullptr};
        const TSValueTypeMetaData *tss_zone_id{nullptr};
        const TSValueTypeMetaData *tss_zoned_datetime{nullptr};
        const TSValueTypeMetaData *tss_instant_range{nullptr};
        const TSValueTypeMetaData *tss_civil_date_range{nullptr};
        const TSValueTypeMetaData *tss_instant_range_set{nullptr};
        const TSValueTypeMetaData *tss_civil_date_range_set{nullptr};
    };

    namespace standard_types_detail
    {
        template <typename T>
        [[nodiscard]] inline const ValueTypeMetaData *
        register_scalar_aliases(TypeRegistry &registry, std::initializer_list<std::string_view> names)
        {
            const ValueTypeMetaData *meta = nullptr;
            for (std::string_view name : names)
            {
                const ValueTypeMetaData *registered = registry.register_scalar<T>(name);
                if (meta == nullptr)
                {
                    meta = registered;
                }
                else if (registered != meta)
                {
                    throw std::logic_error("standard scalar alias resolved to a different schema");
                }
            }
            return meta;
        }

        inline void register_ts_aliases(TypeRegistry                  &registry,
                                        const ValueTypeMetaData       *scalar,
                                        std::initializer_list<std::string_view> names,
                                        const TSValueTypeMetaData    *&ts_out,
                                        const TSValueTypeMetaData    *&tss_out)
        {
            const TSValueTypeMetaData *ts  = registry.ts(scalar);
            const TSValueTypeMetaData *tss = registry.tss(scalar);
            for (std::string_view name : names)
            {
                registry.register_time_series_type_alias(std::string{"TS["} + std::string{name} + "]", ts);
                registry.register_time_series_type_alias(std::string{"TSS["} + std::string{name} + "]", tss);
            }
            ts_out  = ts;
            tss_out = tss;
        }
    }  // namespace standard_types_detail

    /**
     * Register the standard scalar vocabulary and pre-intern the common ``TS`` /
     * ``TSS`` schemas.
     *
     * Primary aliases:
     *
     * - ``bool`` -> ``Bool``
     * - ``int`` -> ``Int``
     * - ``float`` -> ``Float``
     * - ``date`` -> ``Date``
     * - ``datetime`` -> ``DateTime``
     * - ``timedelta`` -> ``TimeDelta``
     * - ``time`` -> ``Time`` (time of day)
     * - ``str`` -> ``Str``
     * - ``bytes`` -> ``Bytes``
     *
     * Explicit aliases include ``int8``/``int16``/``int32``/``int64``,
     * ``uint8``/``uint16``/``uint32``/``uint64``, ``float32`` and
     * ``float64``. ``double`` and ``string`` are accepted compatibility aliases
     * for ``float64`` and ``str``.
     */
    [[nodiscard]] inline RegisteredStandardTypes register_standard_types(
        TypeRegistry &registry = TypeRegistry::instance())
    {
        RegisteredStandardTypes types{};

        types.bool_type      = standard_types_detail::register_scalar_aliases<Bool>(registry, {"bool"});
        (void)registry.json();   // seeds the JSON meta + its "JSON" alias
        types.int_type       = standard_types_detail::register_scalar_aliases<Int>(registry, {"int", "int64"});
        types.float_type     = standard_types_detail::register_scalar_aliases<Float>(registry, {"float", "float64", "double"});
        types.date_type      = standard_types_detail::register_scalar_aliases<Date>(registry, {"date"});
        types.datetime_type  = standard_types_detail::register_scalar_aliases<DateTime>(registry, {"datetime"});
        types.timedelta_type = standard_types_detail::register_scalar_aliases<TimeDelta>(registry, {"timedelta"});
        types.time_type      = standard_types_detail::register_scalar_aliases<Time>(registry, {"time"});
        types.str_type       = standard_types_detail::register_scalar_aliases<Str>(registry, {"str", "string"});
        types.bytes_type     = standard_types_detail::register_scalar_aliases<Bytes>(registry, {"bytes"});
        types.frame_type     = standard_types_detail::register_scalar_aliases<Frame>(registry, {"frame"});
        types.series_type    = standard_types_detail::register_scalar_aliases<Series>(registry, {"series"});
        types.period_type = standard_types_detail::register_scalar_aliases<Period>(
            registry, {"period"});
        types.civil_datetime_type =
            standard_types_detail::register_scalar_aliases<CivilDateTime>(
                registry, {"civil_datetime"});
        types.zone_id_type =
            standard_types_detail::register_scalar_aliases<ZoneId>(
                registry, {"zone_id"});
        types.zoned_datetime_type =
            standard_types_detail::register_scalar_aliases<ZonedDateTime>(
                registry, {"zoned_datetime"});
        types.instant_range_type =
            standard_types_detail::register_scalar_aliases<InstantRange>(
                registry, {"instant_range"});
        types.civil_date_range_type =
            standard_types_detail::register_scalar_aliases<CivilDateRange>(
                registry, {"civil_date_range"});
        types.instant_range_set_type =
            standard_types_detail::register_scalar_aliases<InstantRangeSet>(
                registry, {"instant_range_set"});
        types.civil_date_range_set_type =
            standard_types_detail::register_scalar_aliases<CivilDateRangeSet>(
                registry, {"civil_date_range_set"});
        types.month_end_policy_type =
            standard_types_detail::register_scalar_aliases<MonthEndPolicy>(
                registry, {"month_end_policy"});
        types.ambiguous_time_policy_type =
            standard_types_detail::register_scalar_aliases<AmbiguousTimePolicy>(
                registry, {"ambiguous_time_policy"});
        types.nonexistent_time_policy_type =
            standard_types_detail::register_scalar_aliases<NonexistentTimePolicy>(
                registry, {"nonexistent_time_policy"});
        types.boundary_type =
            standard_types_detail::register_scalar_aliases<Boundary>(
                registry, {"boundary"});

        types.int8_type    = standard_types_detail::register_scalar_aliases<std::int8_t>(registry, {"int8"});
        types.int16_type   = standard_types_detail::register_scalar_aliases<std::int16_t>(registry, {"int16"});
        types.int32_type   = standard_types_detail::register_scalar_aliases<std::int32_t>(registry, {"int32"});
        types.int64_type   = types.int_type;
        types.uint8_type   = standard_types_detail::register_scalar_aliases<std::uint8_t>(registry, {"uint8"});
        types.uint16_type  = standard_types_detail::register_scalar_aliases<std::uint16_t>(registry, {"uint16"});
        types.uint32_type  = standard_types_detail::register_scalar_aliases<std::uint32_t>(registry, {"uint32"});
        types.uint64_type  = standard_types_detail::register_scalar_aliases<std::uint64_t>(registry, {"uint64"});
        types.float32_type = standard_types_detail::register_scalar_aliases<float>(registry, {"float32"});
        types.float64_type = types.float_type;

        const TSValueTypeMetaData *unused_ts  = nullptr;
        const TSValueTypeMetaData *unused_tss = nullptr;
        standard_types_detail::register_ts_aliases(registry, types.bool_type, {"bool"}, types.ts_bool, types.tss_bool);
        standard_types_detail::register_ts_aliases(registry, types.int_type, {"int", "int64"}, types.ts_int, types.tss_int);
        standard_types_detail::register_ts_aliases(registry, types.float_type, {"float", "float64", "double"},
                                                   types.ts_float, types.tss_float);
        standard_types_detail::register_ts_aliases(registry, types.date_type, {"date"}, types.ts_date, types.tss_date);
        standard_types_detail::register_ts_aliases(registry, types.datetime_type, {"datetime"}, types.ts_datetime,
                                                   types.tss_datetime);
        standard_types_detail::register_ts_aliases(registry, types.timedelta_type, {"timedelta"}, types.ts_timedelta,
                                                   types.tss_timedelta);
        standard_types_detail::register_ts_aliases(registry, types.time_type, {"time"}, types.ts_time,
                                                   types.tss_time);
        standard_types_detail::register_ts_aliases(registry, types.str_type, {"str", "string"}, types.ts_str,
                                                   types.tss_str);
        standard_types_detail::register_ts_aliases(registry, types.bytes_type, {"bytes"}, types.ts_bytes,
                                                   types.tss_bytes);
        standard_types_detail::register_ts_aliases(
            registry, types.period_type, {"period"}, types.ts_period,
            types.tss_period);
        standard_types_detail::register_ts_aliases(
            registry, types.civil_datetime_type, {"civil_datetime"},
            types.ts_civil_datetime, types.tss_civil_datetime);
        standard_types_detail::register_ts_aliases(
            registry, types.zone_id_type, {"zone_id"}, types.ts_zone_id,
            types.tss_zone_id);
        standard_types_detail::register_ts_aliases(
            registry, types.zoned_datetime_type, {"zoned_datetime"},
            types.ts_zoned_datetime, types.tss_zoned_datetime);
        standard_types_detail::register_ts_aliases(
            registry, types.instant_range_type, {"instant_range"},
            types.ts_instant_range, types.tss_instant_range);
        standard_types_detail::register_ts_aliases(
            registry, types.civil_date_range_type, {"civil_date_range"},
            types.ts_civil_date_range, types.tss_civil_date_range);
        standard_types_detail::register_ts_aliases(
            registry, types.instant_range_set_type, {"instant_range_set"},
            types.ts_instant_range_set, types.tss_instant_range_set);
        standard_types_detail::register_ts_aliases(
            registry, types.civil_date_range_set_type, {"civil_date_range_set"},
            types.ts_civil_date_range_set, types.tss_civil_date_range_set);
        standard_types_detail::register_ts_aliases(
            registry, types.month_end_policy_type, {"month_end_policy"},
            unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(
            registry, types.ambiguous_time_policy_type,
            {"ambiguous_time_policy"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(
            registry, types.nonexistent_time_policy_type,
            {"nonexistent_time_policy"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(
            registry, types.boundary_type, {"boundary"}, unused_ts, unused_tss);
        {
            const TSValueTypeMetaData *unused_frame_tss = nullptr;
            standard_types_detail::register_ts_aliases(registry, types.frame_type, {"frame"}, types.ts_frame,
                                                       unused_frame_tss);
        }
        {
            const TSValueTypeMetaData *unused_series_tss = nullptr;
            standard_types_detail::register_ts_aliases(registry, types.series_type, {"series"}, types.ts_series,
                                                       unused_series_tss);
        }

        standard_types_detail::register_ts_aliases(registry, types.int8_type, {"int8"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(registry, types.int16_type, {"int16"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(registry, types.int32_type, {"int32"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(registry, types.uint8_type, {"uint8"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(registry, types.uint16_type, {"uint16"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(registry, types.uint32_type, {"uint32"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(registry, types.uint64_type, {"uint64"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(registry, types.float32_type, {"float32"}, unused_ts, unused_tss);

        return types;
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_STANDARD_TYPES_H
