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

    /** ``day`` — the day-of-month attribute of a date or datetime. */
    struct day : Operator<"day", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    /** ``weekday`` — the day of the week using Monday as zero. */
    struct weekday : Operator<"weekday", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    /** ``isoweekday`` — the ISO day of the week using Monday as one. */
    struct isoweekday : Operator<"isoweekday", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    /** hgraph's timedelta ATTRIBUTES (port.days / .seconds / .microseconds)
        and ``total_seconds()`` — issue #82. Python's normalization: ``days``
        floors toward -inf; ``seconds`` / ``microseconds`` are the
        non-negative remainders. */
    struct days : Operator<"days", In<"ts", TS<TimeDelta>>, Out<TS<Int>>>
    {
    };

    /** ``seconds`` — the non-negative whole-second remainder of a timedelta. */
    struct seconds : Operator<"seconds", In<"ts", TS<TimeDelta>>, Out<TS<Int>>>
    {
    };

    /** ``microseconds`` — the non-negative microsecond remainder of a timedelta. */
    struct microseconds : Operator<"microseconds", In<"ts", TS<TimeDelta>>, Out<TS<Int>>>
    {
    };

    /** ``total_seconds`` — convert a timedelta to fractional seconds. */
    struct total_seconds : Operator<"total_seconds", In<"ts", TS<TimeDelta>>, Out<TS<Float>>>
    {
    };

    /** hgraph's datetime / time ATTRIBUTES (port.hour / .minute / .second /
        .microsecond) plus the datetime methods (``weekday()`` /
        ``isoweekday()`` / ``timestamp()``) — issue #82. The datetime
        overloads register under the existing ``year`` / ``month`` / ``day``
        / ``weekday`` / ``isoweekday`` markers. */
    struct hour : Operator<"hour", In<"ts", TS<DateTime>>, Out<TS<Int>>>
    {
    };

    /** ``minute`` — the minute component of a datetime or time. */
    struct minute : Operator<"minute", In<"ts", TS<DateTime>>, Out<TS<Int>>>
    {
    };

    /** ``second`` — the second component of a datetime or time. */
    struct second : Operator<"second", In<"ts", TS<DateTime>>, Out<TS<Int>>>
    {
    };

    /** ``microsecond`` — the microsecond component of a datetime or time. */
    struct microsecond : Operator<"microsecond", In<"ts", TS<DateTime>>, Out<TS<Int>>>
    {
    };

    /** ``timestamp`` — FRACTIONAL seconds since the Unix epoch (Python's
        ``datetime.timestamp()`` returns a float). hgraph datetimes are UTC
        by convention, so this is the UTC epoch count (upstream's naive
        ``datetime.timestamp()`` is local-tz dependent; recorded deviation). */
    struct timestamp : Operator<"timestamp", In<"ts", TS<DateTime>>, Out<TS<Float>>>
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

    /** ``isoformat`` — format a date, datetime, or time as an ISO 8601 string. */
    struct isoformat : Operator<"isoformat", In<"ts", TS<Date>>, Out<TS<Str>>>
    {
    };

    /** ``explode`` — split a date into a fixed list of year, month, and day. */
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

    /** ``at_zone`` — represent an instant in the supplied time zone. */
    struct at_zone
        : Operator<"at_zone", In<"instant", TS<Instant>>,
                   In<"zone", TS<ZoneId>>, Out<TS<ZonedDateTime>>>
    {
    };

    /** ``resolve_civil`` — resolve a local civil time using explicit daylight-saving policies. */
    struct resolve_civil
        : Operator<"resolve_civil", In<"local", TS<CivilDateTime>>,
                   In<"zone", TS<ZoneId>>,
                   Scalar<"ambiguous", AmbiguousTimePolicy>,
                   Scalar<"nonexistent", NonexistentTimePolicy>,
                   Out<TS<ZonedDateTime>>>
    {
    };

    /** ``convert_zone`` — view a zoned datetime in another zone without changing its instant. */
    struct convert_zone
        : Operator<"convert_zone", In<"value", TS<ZonedDateTime>>,
                   In<"zone", TS<ZoneId>>, Out<TS<ZonedDateTime>>>
    {
    };

    /** ``to_instant`` — extract the absolute instant from a zoned datetime. */
    struct to_instant
        : Operator<"to_instant", In<"value", TS<ZonedDateTime>>,
                   Out<TS<Instant>>>
    {
    };

    /** ``to_civil`` — extract the local civil date and time from a zoned datetime. */
    struct to_civil
        : Operator<"to_civil", In<"value", TS<ZonedDateTime>>,
                   Out<TS<CivilDateTime>>>
    {
    };

    /** ``range_contains`` — test whether a temporal range contains a value or another range. */
    struct range_contains
        : Operator<"range_contains", In<"range", TsVar<"R">>,
                   In<"value", TsVar<"V">>, Out<TS<Bool>>>
    {
    };

    /** ``range_intersection`` — return the common portion of two temporal ranges. */
    struct range_intersection
        : Operator<"range_intersection", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TsVar<"R">>>
    {
    };

    /** ``range_overlaps`` — test whether two temporal ranges share any included instant. */
    struct range_overlaps
        : Operator<"range_overlaps", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TS<Bool>>>
    {
    };

    /** ``range_touches`` — test whether finite endpoint values coincide, regardless of openness. */
    struct range_touches
        : Operator<"range_touches", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TS<Bool>>>
    {
    };

    /** ``range_adjacent`` — test whether endpoints coincide with exactly one of them closed. */
    struct range_adjacent
        : Operator<"range_adjacent", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TS<Bool>>>
    {
    };

    /** ``range_mergeable`` — test whether two ranges overlap or are adjacent. */
    struct range_mergeable
        : Operator<"range_mergeable", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TS<Bool>>>
    {
    };

    /** ``range_difference`` — subtract the right-hand range from the left-hand range. */
    struct range_difference
        : Operator<"range_difference", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``range_union`` — return the normalized union of two temporal ranges. */
    struct range_union
        : Operator<"range_union", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``range_merge`` — merge two mergeable ranges into one range. */
    struct range_merge
        : Operator<"range_merge", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TsVar<"R">>>
    {
    };

    /** ``range_hull`` — return the smallest range spanning both inputs. */
    struct range_hull
        : Operator<"range_hull", In<"lhs", TsVar<"R">>,
                   In<"rhs", TsVar<"R">>, Out<TsVar<"R">>>
    {
    };

    /** ``range_shift`` — move both range boundaries by a duration or calendar period. */
    struct range_shift
        : Operator<"range_shift", In<"range", TsVar<"R">>,
                   In<"delta", TsVar<"D">>,
                   Scalar<"month_end_policy", MonthEndPolicy>,
                   Out<TsVar<"R">>>
    {
    };

    /** ``range_extent`` — return the duration between an instant range's boundaries. */
    struct range_extent
        : Operator<"range_extent", In<"range", TS<InstantRange>>,
                   Out<TS<Duration>>>
    {
    };

    /** ``temporal_floor`` — round a temporal value down to a quantum boundary. */
    struct temporal_floor
        : Operator<"temporal_floor", In<"value", TsVar<"T">>,
                   In<"quantum", TS<Duration>>, Out<TsVar<"T">>>
    {
    };

    /** ``temporal_ceil`` — round a temporal value up to a quantum boundary. */
    struct temporal_ceil
        : Operator<"temporal_ceil", In<"value", TsVar<"T">>,
                   In<"quantum", TS<Duration>>, Out<TsVar<"T">>>
    {
    };

    /** ``temporal_round`` — round a temporal value to its nearest quantum boundary. */
    struct temporal_round
        : Operator<"temporal_round", In<"value", TsVar<"T">>,
                   In<"quantum", TS<Duration>>, Out<TsVar<"T">>>
    {
    };

    /** ``temporal_bucket`` — return the fixed-width instant range containing a value. */
    struct temporal_bucket
        : Operator<"temporal_bucket", In<"value", TS<Instant>>,
                   In<"width", TS<Duration>>, Out<TS<InstantRange>>>
    {
    };

}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_TEMPORAL_H
