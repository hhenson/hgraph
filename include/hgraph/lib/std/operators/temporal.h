#ifndef HGRAPH_LIB_STD_OPERATORS_TEMPORAL_H
#define HGRAPH_LIB_STD_OPERATORS_TEMPORAL_H

#include <hgraph/lib/std/operators/comparison.h>   // CmpResult (evaluation_time_in_range)
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/temporal.h>

namespace hgraph::stdlib
{
    /**
     * Date / time-series-property operator **definitions** (markers only). Mirrors the
     * Python ``hgraph`` date operators (``_date_operators.py``) and the time-series
     * introspection operators (``_time_series_properties.py``).
     */

    // ---- Date component extraction ----

    /** ``day_of_month`` — the day-of-month of a ``TS<Date>``. */
    struct day_of_month : Operator<"day_of_month", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    /** ``month_of_year`` — the month-of-year of a ``TS<Date>``. */
    struct month_of_year : Operator<"month_of_year", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    /** ``year`` — the year of a ``TS<Date>``. */
    struct year : Operator<"year", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    /** ``explode`` — the (year, month, day) of a ``TS<Date>`` as a 3-element list. */
    /** hgraph's date ATTRIBUTES (port.month / .day / .weekday / .isoweekday). */
    struct month : Operator<"month", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    struct day : Operator<"day", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    struct weekday : Operator<"weekday", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    struct isoweekday : Operator<"isoweekday", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    /** ``evaluation_time_in_range`` — where the evaluation time sits
        relative to [start, end]: LT / EQ / GT, self-scheduling at the
        boundaries (datetime / date / daily-recurring time overloads). */
    struct evaluation_time_in_range
        : Operator<"evaluation_time_in_range", In<"start_time", TsVar<"A">>, In<"end_time", TsVar<"B">>,
                   Out<TS<CmpResult>>>
    {
    };

    struct isoformat : Operator<"isoformat", In<"ts", TS<Date>>, Out<TS<Str>>>
    {
    };

    struct explode : Operator<"explode", In<"ts", TS<Date>>, Out<TsVar<"O">>>
    {
    };

    // ---- Time-series introspection ----

    /** ``valid`` — ``True`` while ``ts`` is valid, ``False`` otherwise. */
    struct valid : Operator<"valid", In<"ts", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``modified`` — ``True`` in the cycle ``ts`` is modified (a live, ticking property). */
    struct modified : Operator<"modified", In<"ts", SIGNAL>, Out<TS<Bool>>>
    {
    };

    /** ``last_modified_time`` — the evaluation time ``ts`` was last modified. */
    struct last_modified_time : Operator<"last_modified_time", In<"ts", SIGNAL>, Out<TS<DateTime>>>
    {
    };

    /** ``last_modified_wall_clock_time`` — the wall-clock time ``ts`` was last modified. */
    struct last_modified_wall_clock_time
        : Operator<"last_modified_wall_clock_time", In<"ts", SIGNAL>, Out<TS<DateTime>>>
    {
    };

    /** ``last_modified_date`` — the date component of the last-modified time. */
    struct last_modified_date : Operator<"last_modified_date", In<"ts", SIGNAL>, Out<TS<Date>>>
    {
    };

    struct at_zone
        : Operator<"at_zone", In<"instant", TS<Instant>>,
                   In<"zone", TS<ZoneId>>, Out<TS<ZonedDateTime>>>
    {
    };

    struct resolve_civil
        : Operator<"resolve_civil", In<"local", TS<CivilDateTime>>,
                   In<"zone", TS<ZoneId>>,
                   Scalar<"ambiguous", AmbiguousTimePolicy>,
                   Scalar<"nonexistent", NonexistentTimePolicy>,
                   Out<TS<ZonedDateTime>>>
    {
    };

    struct convert_zone
        : Operator<"convert_zone", In<"value", TS<ZonedDateTime>>,
                   In<"zone", TS<ZoneId>>, Out<TS<ZonedDateTime>>>
    {
    };

    struct to_instant
        : Operator<"to_instant", In<"value", TS<ZonedDateTime>>,
                   Out<TS<Instant>>>
    {
    };

    struct to_civil
        : Operator<"to_civil", In<"value", TS<ZonedDateTime>>,
                   Out<TS<CivilDateTime>>>
    {
    };

    struct range_contains
        : Operator<"range_contains", In<"range", TsVar<"R">>,
                   In<"value", TsVar<"V">>, Out<TS<Bool>>>
    {
    };

    struct range_intersection
        : Operator<"range_intersection", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TsVar<"R">>>
    {
    };

    struct range_overlaps
        : Operator<"range_overlaps", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TS<Bool>>>
    {
    };

    struct range_touches
        : Operator<"range_touches", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TS<Bool>>>
    {
    };

    struct range_adjacent
        : Operator<"range_adjacent", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TS<Bool>>>
    {
    };

    struct range_mergeable
        : Operator<"range_mergeable", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TS<Bool>>>
    {
    };

    struct range_difference
        : Operator<"range_difference", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    struct range_union
        : Operator<"range_union", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    struct range_merge
        : Operator<"range_merge", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TsVar<"R">>>
    {
    };

    struct range_hull
        : Operator<"range_hull", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TsVar<"R">>>
    {
    };

    struct range_shift
        : Operator<"range_shift", In<"range", TsVar<"R">>,
                   In<"delta", TsVar<"D">>,
                   Scalar<"month_end_policy", MonthEndPolicy>,
                   Out<TsVar<"R">>>
    {
    };

    struct range_extent
        : Operator<"range_extent", In<"range", TS<InstantRange>>,
                   Out<TS<Duration>>>
    {
    };

    struct temporal_floor
        : Operator<"temporal_floor", In<"value", TsVar<"T">>,
                   In<"quantum", TS<Duration>>, Out<TsVar<"T">>>
    {
    };

    struct temporal_ceil
        : Operator<"temporal_ceil", In<"value", TsVar<"T">>,
                   In<"quantum", TS<Duration>>, Out<TsVar<"T">>>
    {
    };

    struct temporal_round
        : Operator<"temporal_round", In<"value", TsVar<"T">>,
                   In<"quantum", TS<Duration>>, Out<TsVar<"T">>>
    {
    };

    struct temporal_bucket
        : Operator<"temporal_bucket", In<"value", TS<Instant>>,
                   In<"width", TS<Duration>>, Out<TS<InstantRange>>>
    {
    };

}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_TEMPORAL_H
