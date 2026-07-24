#include <hgraph/lib/std/operators/impl/temporal_impl.h>

#include <hgraph/lib/std/operators/arithmetic.h>   // sub_ (date - timedelta)

namespace hgraph::stdlib
{
    void register_temporal_operators()
    {
        register_overload<day_of_month, day_of_month_impl>();
        register_overload<day, day_of_month_impl>();
        register_overload<sub_, sub_date_timedelta_impl>();
        register_overload<isoformat, isoformat_impl>();
        register_overload<evaluation_time_in_range, evaluation_time_in_range_datetime_impl>();
        register_overload<evaluation_time_in_range, evaluation_time_in_range_date_impl>();
        register_overload<evaluation_time_in_range, evaluation_time_in_range_time_impl>();
        register_overload<month, month_of_year_impl>();
        register_overload<weekday, weekday_impl>();
        register_overload<isoweekday, isoweekday_impl>();
        register_overload<month_of_year, month_of_year_impl>();
        register_overload<year, year_impl>();
        register_overload<explode, explode_date_impl>();
        register_overload<valid, valid_impl>();
        register_graph_overload<valid, valid_ref_graph_impl>();
        register_overload<modified, modified_impl>();
        register_overload<last_modified_time, last_modified_time_impl>();
        register_overload<last_modified_wall_clock_time, last_modified_wall_clock_time_impl>();
        register_overload<last_modified_date, last_modified_date_impl>();
        register_overload<at_zone, at_zone_impl>();
        register_overload<
            resolve_civil,
            resolve_civil_impl<AmbiguousTimePolicy::Reject,
                               NonexistentTimePolicy::Reject>>();
        register_overload<
            resolve_civil,
            resolve_civil_impl<AmbiguousTimePolicy::Reject,
                               NonexistentTimePolicy::NextValid>>();
        register_overload<
            resolve_civil,
            resolve_civil_impl<AmbiguousTimePolicy::Reject,
                               NonexistentTimePolicy::PreviousValid>>();
        register_overload<
            resolve_civil,
            resolve_civil_impl<AmbiguousTimePolicy::Earliest,
                               NonexistentTimePolicy::Reject>>();
        register_overload<
            resolve_civil,
            resolve_civil_impl<AmbiguousTimePolicy::Earliest,
                               NonexistentTimePolicy::NextValid>>();
        register_overload<
            resolve_civil,
            resolve_civil_impl<AmbiguousTimePolicy::Earliest,
                               NonexistentTimePolicy::PreviousValid>>();
        register_overload<
            resolve_civil,
            resolve_civil_impl<AmbiguousTimePolicy::Latest,
                               NonexistentTimePolicy::Reject>>();
        register_overload<
            resolve_civil,
            resolve_civil_impl<AmbiguousTimePolicy::Latest,
                               NonexistentTimePolicy::NextValid>>();
        register_overload<
            resolve_civil,
            resolve_civil_impl<AmbiguousTimePolicy::Latest,
                               NonexistentTimePolicy::PreviousValid>>();
        register_overload<convert_zone, convert_zone_impl>();
        register_overload<to_instant, to_instant_impl>();
        register_overload<to_civil, to_civil_impl>();
        register_overload<range_contains,
                          range_contains_value_impl<InstantRange, Instant>>();
        register_overload<range_contains,
                          range_contains_value_impl<CivilDateRange, CivilDate>>();
        register_overload<range_contains,
                          range_contains_range_impl<InstantRange>>();
        register_overload<range_contains,
                          range_contains_range_impl<CivilDateRange>>();
        register_overload<range_intersection,
                          range_intersection_impl<InstantRange>>();
        register_overload<range_intersection,
                          range_intersection_impl<CivilDateRange>>();
        register_overload<range_overlaps,
                          range_relation_impl<InstantRange, 0>>();
        register_overload<range_overlaps,
                          range_relation_impl<CivilDateRange, 0>>();
        register_overload<range_touches,
                          range_relation_impl<InstantRange, 1>>();
        register_overload<range_touches,
                          range_relation_impl<CivilDateRange, 1>>();
        register_overload<range_adjacent,
                          range_relation_impl<InstantRange, 2>>();
        register_overload<range_adjacent,
                          range_relation_impl<CivilDateRange, 2>>();
        register_overload<range_mergeable,
                          range_relation_impl<InstantRange, 3>>();
        register_overload<range_mergeable,
                          range_relation_impl<CivilDateRange, 3>>();
        register_overload<range_difference,
                          range_difference_impl<InstantRange,
                                                InstantRangeSet>>();
        register_overload<range_difference,
                          range_difference_impl<CivilDateRange,
                                                CivilDateRangeSet>>();
        register_overload<range_union,
                          range_union_impl<InstantRange, InstantRangeSet>>();
        register_overload<range_union,
                          range_union_impl<CivilDateRange,
                                           CivilDateRangeSet>>();
        register_overload<range_merge, range_merge_impl<InstantRange>>();
        register_overload<range_merge, range_merge_impl<CivilDateRange>>();
        register_overload<range_hull,
                          range_hull_impl<InstantRange>>();
        register_overload<range_hull,
                          range_hull_impl<CivilDateRange>>();
        register_overload<range_shift, instant_range_shift_impl>();
        register_overload<
            range_shift,
            civil_date_range_shift_impl<MonthEndPolicy::Reject>>();
        register_overload<
            range_shift,
            civil_date_range_shift_impl<MonthEndPolicy::Clamp>>();
        register_overload<
            range_shift,
            civil_date_range_shift_impl<
                MonthEndPolicy::PreserveEndOfMonth>>();
        register_overload<range_extent, instant_range_extent_impl>();
        register_overload<temporal_floor,
                          quantize_impl<Duration, -1>>();
        register_overload<temporal_floor,
                          quantize_instant_impl<-1>>();
        register_overload<temporal_ceil,
                          quantize_impl<Duration, 1>>();
        register_overload<temporal_ceil,
                          quantize_instant_impl<1>>();
        register_overload<temporal_round,
                          quantize_impl<Duration, 0>>();
        register_overload<temporal_round,
                          quantize_instant_impl<0>>();
        register_overload<temporal_bucket, temporal_bucket_impl>();
    }
}  // namespace hgraph::stdlib
