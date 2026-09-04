#include <hgraph/lib/std/operators/impl/arithmetic_impl.h>

namespace hgraph::stdlib
{
    // Arithmetic over TimeDelta / DateTime / Date and the RFC 0002 Duration /
    // Instant / Period / CivilDateTime / ZonedDateTime types.
    void register_arithmetic_temporal_overloads()
    {
        // add_ — homogeneous and heterogeneous temporal.
        register_overload<add_, checked_add_durations>();                                      // TimeDelta + TimeDelta
        register_overload<add_, checked_add_instant_duration>();                               // DateTime + TimeDelta
        register_overload<add_, checked_add_duration_instant>();                               // TimeDelta + DateTime
        register_overload<add_, add_date_timedelta>();                                           // Date + TimeDelta -> Date
        register_overload<add_, checked_add_periods>();
        register_overload<add_, checked_add_civil_datetime_duration>();
        register_overload<add_, combine_civil_date_time>();
        register_overload<add_, checked_add_zoned_duration>();
        register_overload<add_, checked_add_duration_zoned>();
        register_overload<add_, apply_period_add_impl<
                                    Date, MonthEndPolicy::Reject>>();
        register_overload<add_, apply_period_add_impl<
                                    Date, MonthEndPolicy::Clamp>>();
        register_overload<add_, apply_period_add_impl<
                                    Date,
                                    MonthEndPolicy::PreserveEndOfMonth>>();
        register_overload<add_, apply_period_add_impl<
                                    CivilDateTime,
                                    MonthEndPolicy::Reject>>();
        register_overload<add_, apply_period_add_impl<
                                    CivilDateTime,
                                    MonthEndPolicy::Clamp>>();
        register_overload<add_, apply_period_add_impl<
                                    CivilDateTime,
                                    MonthEndPolicy::PreserveEndOfMonth>>();

        // sub_ — note the result type that differs from the operands.
        register_overload<sub_, checked_sub_durations>();                                      // TimeDelta - TimeDelta
        register_overload<sub_, checked_sub_instant_duration>();                               // DateTime - TimeDelta
        register_overload<sub_, checked_sub_instants>();                                       // DateTime - DateTime -> TimeDelta
        register_overload<sub_, sub_dates>();                                                    // Date - Date -> TimeDelta
        register_overload<sub_, checked_sub_periods>();
        register_overload<sub_, checked_sub_civil_datetime_duration>();
        register_overload<sub_, checked_sub_civil_datetimes>();
        register_overload<sub_, checked_sub_zoned_duration>();
        register_overload<sub_, apply_period_sub_impl<
                                    Date, MonthEndPolicy::Reject>>();
        register_overload<sub_, apply_period_sub_impl<
                                    Date, MonthEndPolicy::Clamp>>();
        register_overload<sub_, apply_period_sub_impl<
                                    Date,
                                    MonthEndPolicy::PreserveEndOfMonth>>();
        register_overload<sub_, apply_period_sub_impl<
                                    CivilDateTime,
                                    MonthEndPolicy::Reject>>();
        register_overload<sub_, apply_period_sub_impl<
                                    CivilDateTime,
                                    MonthEndPolicy::Clamp>>();
        register_overload<sub_, apply_period_sub_impl<
                                    CivilDateTime,
                                    MonthEndPolicy::PreserveEndOfMonth>>();

        // Scaling and quotients.
        register_overload<mul_, arithmetic_impl_detail::timedelta_scale_impl>();
        register_overload<mul_, arithmetic_impl_detail::timedelta_scale_float_impl>();
        register_overload<mul_, arithmetic_impl_detail::period_scale_impl>();
        register_overload<mul_, arithmetic_impl_detail::int_scale_period_impl>();
        register_overload<div_, arithmetic_impl_detail::timedelta_div_impl>();
        register_overload<div_, arithmetic_impl_detail::timedelta_div_float_impl>();
        register_overload<div_, div_timedeltas>();                    // TimeDelta / TimeDelta -> Float

        // Unary.
        register_overload<neg_, arithmetic_impl_detail::negate_duration_impl>();
        register_overload<neg_, arithmetic_impl_detail::negate_period_impl>();
        register_overload<pos_, lift<scalar_pos<TimeDelta>>>();
        register_overload<abs_, abs_timedelta>();
    }
}  // namespace hgraph::stdlib
