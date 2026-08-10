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

    /** Track whether an input currently has a usable value.
        This emits on validity transitions rather than mirroring every source tick.
        @param ts Input whose validity is observed.
        @return True while valid and false while invalid.
        @par Python example
        @code{.py}
        has_price = hg.valid(price)
        @endcode */
    struct valid : Operator<"valid", In<"ts", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** Produce a live boolean pulse describing source modification in the current cycle.
        @param ts Signal whose tick timing is observed; its value is ignored.
        @return True in a modification cycle and false in subsequent evaluation cycles.
        @par Python example
        @code{.py}
        price_ticked = hg.modified(price)
        @endcode */
    struct modified : Operator<"modified", In<"ts", SIGNAL>, Out<TS<Bool>>>
    {
    };

    /** Report the engine evaluation time of each source modification.
        @param ts Signal whose modification time is observed.
        @return The current graph evaluation timestamp whenever ``ts`` ticks.
        @par Python example
        @code{.py}
        event_time = hg.last_modified_time(price)
        @endcode */
    struct last_modified_time : Operator<"last_modified_time", In<"ts", SIGNAL>, Out<TS<DateTime>>>
    {
    };

    /** Report wall-clock time when each source modification is evaluated.
        Unlike ``last_modified_time``, this observes real elapsed time rather than the
        graph's logical evaluation clock.
        @param ts Signal whose modification time is observed.
        @return Wall-clock timestamp for each source tick.
        @par Python example
        @code{.py}
        received_at = hg.last_modified_wall_clock_time(price)
        @endcode */
    struct last_modified_wall_clock_time
        : Operator<"last_modified_wall_clock_time", In<"ts", SIGNAL>, Out<TS<DateTime>>>
    {
    };

    /** ``last_modified_date`` — the date component of the last-modified time. */
    struct last_modified_date : Operator<"last_modified_date", In<"ts", SIGNAL>, Out<TS<Date>>>
    {
    };

    /** Represent an absolute instant in a time zone without changing that instant.
        @param instant Absolute UTC-line timestamp.
        @param zone IANA time-zone identifier used for local representation.
        @return A zoned datetime carrying both instant and zone.
        @par Python example
        @code{.py}
        local_view = hg.at_zone(instant, hg.ZoneId("Europe/London"))
        @endcode */
    struct at_zone
        : Operator<"at_zone", In<"instant", TS<Instant>>,
                   In<"zone", TS<ZoneId>>, Out<TS<ZonedDateTime>>>
    {
    };

    /** Resolve a timezone-free local civil datetime to an absolute zoned instant.
        Daylight-saving overlaps and gaps require explicit, fixed wiring-time policies so
        ambiguous data cannot silently select an instant.
        @param local Local date and time without an offset.
        @param zone IANA zone whose transition rules interpret ``local``.
        @param ambiguous Policy for a local time that occurs twice.
        @param nonexistent Policy for a local time skipped by a clock transition.
        @return The resolved zoned datetime.
        @par Python example
        @code{.py}
        resolved = hg.resolve_civil(local, zone, ambiguous=hg.AmbiguousTimePolicy.EARLIEST,
                                    nonexistent=hg.NonexistentTimePolicy.NEXT_VALID)
        @endcode */
    struct resolve_civil
        : Operator<"resolve_civil", In<"local", TS<CivilDateTime>>,
                   In<"zone", TS<ZoneId>>,
                   Scalar<"ambiguous", AmbiguousTimePolicy>,
                   Scalar<"nonexistent", NonexistentTimePolicy>,
                   Out<TS<ZonedDateTime>>>
    {
    };

    /** Change the display zone of a zoned datetime while preserving its absolute instant.
        @param value Zoned datetime to convert.
        @param zone Destination IANA time zone.
        @return The same instant represented in ``zone``.
        @par Python example
        @code{.py}
        new_york_time = hg.convert_zone(london_time, hg.ZoneId("America/New_York"))
        @endcode */
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

    /** Move both finite range boundaries by a duration or calendar period.
        Open/closed boundary flags and unbounded endpoints are preserved.
        @param range Temporal range to move.
        @param delta Fixed duration or calendar-relative period added to each finite endpoint.
        @param month_end_policy Policy for calendar shifts whose target month lacks the source day.
        @return The shifted range.
        @par Python example
        @code{.py}
        next_month = hg.range_shift(window, hg.Period(months=1))
        @endcode */
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

    /** Round a temporal value down to the preceding fixed-duration boundary.
        @param value Instant or supported temporal value to round.
        @param quantum Positive fixed duration defining the boundary grid.
        @param origin Optional origin from which boundaries are measured.
        @return The greatest boundary not after ``value``.
        @par Python example
        @code{.py}
        minute = hg.temporal_floor(instant, timedelta(minutes=1))
        @endcode */
    struct temporal_floor
        : Operator<"temporal_floor", In<"value", TsVar<"T">>,
                   In<"quantum", TS<Duration>>, Out<TsVar<"T">>>
    {
    };

    /** Round a temporal value up to the following fixed-duration boundary.
        An exact boundary remains unchanged.
        @param value Instant or supported temporal value to round.
        @param quantum Positive fixed duration defining the boundary grid.
        @param origin Optional origin from which boundaries are measured.
        @return The least boundary not before ``value``.
        @par Python example
        @code{.py}
        next_minute = hg.temporal_ceil(instant, timedelta(minutes=1))
        @endcode */
    struct temporal_ceil
        : Operator<"temporal_ceil", In<"value", TsVar<"T">>,
                   In<"quantum", TS<Duration>>, Out<TsVar<"T">>>
    {
    };

    /** Round a temporal value to the nearest fixed-duration boundary.
        @param value Instant or supported temporal value to round.
        @param quantum Positive fixed duration defining the boundary grid.
        @param origin Optional origin from which boundaries are measured.
        @return The nearest boundary using the selected tie behaviour.
        @par Python example
        @code{.py}
        nearest_minute = hg.temporal_round(instant, timedelta(minutes=1))
        @endcode */
    struct temporal_round
        : Operator<"temporal_round", In<"value", TsVar<"T">>,
                   In<"quantum", TS<Duration>>, Out<TsVar<"T">>>
    {
    };

    /** Return the half-open fixed-width range containing an instant.
        @param value Instant to classify.
        @param width Positive bucket duration.
        @param origin Optional origin anchoring bucket boundaries.
        @return The containing ``[start, end)`` instant range.
        @par Python example
        @code{.py}
        minute_bucket = hg.temporal_bucket(instant, timedelta(minutes=1))
        @endcode */
    struct temporal_bucket
        : Operator<"temporal_bucket", In<"value", TS<Instant>>,
                   In<"width", TS<Duration>>, Out<TS<InstantRange>>>
    {
    };

}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_TEMPORAL_H
