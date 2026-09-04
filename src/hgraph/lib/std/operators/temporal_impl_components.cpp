#include <hgraph/lib/std/operators/impl/temporal_impl.h>

#include <hgraph/lib/std/operators/arithmetic.h>   // sub_ (date - timedelta)

namespace hgraph::stdlib
{
    // Calendar components of Date / DateTime / Time / TimeDelta values,
    // evaluation-time predicates and the modified / valid introspection
    // operators.
    void register_temporal_components_overloads()
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
        // Issue #82: timedelta attributes and the datetime/time overloads of
        // the attribute operators (upstream's getattr_ tables).
        register_overload<days, days_timedelta_impl>();
        register_overload<seconds, seconds_timedelta_impl>();
        register_overload<microseconds, microseconds_timedelta_impl>();
        register_overload<total_seconds, total_seconds_timedelta_impl>();
        register_overload<year, year_datetime_impl>();
        register_overload<month, month_datetime_impl>();
        register_overload<month_of_year, month_datetime_impl>();
        register_overload<day, day_datetime_impl>();
        register_overload<day_of_month, day_datetime_impl>();
        register_overload<weekday, weekday_datetime_impl>();
        register_overload<isoweekday, isoweekday_datetime_impl>();
        register_overload<hour, hour_datetime_impl>();
        register_overload<minute, minute_datetime_impl>();
        register_overload<second, second_datetime_impl>();
        register_overload<microsecond, microsecond_datetime_impl>();
        register_overload<datepart, datepart_datetime_impl>();
        register_overload<timestamp, timestamp_datetime_impl>();
        register_overload<hour, hour_time_impl>();
        register_overload<minute, minute_time_impl>();
        register_overload<second, second_time_impl>();
        register_overload<microsecond, microsecond_time_impl>();
        register_overload<explode, explode_date_impl>();
        register_overload<valid, valid_impl>();
        register_graph_overload<valid, valid_ref_graph_impl>();
        register_overload<modified, modified_impl>();
        register_overload<last_modified_time, last_modified_time_impl>();
        register_overload<last_modified_wall_clock_time, last_modified_wall_clock_time_impl>();
        register_overload<last_modified_date, last_modified_date_impl>();
    }
}  // namespace hgraph::stdlib
