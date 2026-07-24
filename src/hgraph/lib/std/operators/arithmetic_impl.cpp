#include <hgraph/lib/std/operators/impl/arithmetic_impl.h>

namespace hgraph::stdlib
{
    void register_arithmetic_operators()
    {
        using tsl_itemwise_impl_detail::tsl_binary_map;
        using tsl_itemwise_impl_detail::tsl_lhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_rhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_unary_map;
        using tsb_itemwise_impl_detail::tsb_binary_map;
        using tsb_itemwise_impl_detail::tsb_unary_map;

        // add_ — homogeneous numeric / temporal, mixed numeric, and heterogeneous temporal.
        register_overload<add_, lift<scalar_add<Int>>>();                                      // int + int -> int
        register_overload<add_, lift<scalar_add<Float>>>();                                    // float + float -> float
        register_overload<add_, lift<scalar_add<Str>>>();                                      // string concatenation
        register_overload<add_, checked_add_durations>();                                      // TimeDelta + TimeDelta
        register_overload<add_, lift<scalar_add<Int, Float, Float>>>();                        // int + float -> float
        register_overload<add_, lift<scalar_add<Float, Int, Float>>>();                        // float + int -> float
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
        register_graph_overload<add_, tsl_binary_map<add_>>();
        register_graph_overload<add_, tsl_rhs_broadcast_map<add_>>();
        register_graph_overload<add_, tsl_lhs_broadcast_map<add_>>();
        register_graph_overload<add_, tsb_binary_map<add_>>();

        // sub_ — note the result type that differs from the operands.
        register_overload<sub_, lift<scalar_sub<Int>>>();                                      // int - int -> int
        register_overload<sub_, lift<scalar_sub<Float>>>();                                    // float - float -> float
        register_overload<sub_, checked_sub_durations>();                                      // TimeDelta - TimeDelta
        register_overload<sub_, lift<scalar_sub<Int, Float, Float>>>();                        // int - float -> float
        register_overload<sub_, lift<scalar_sub<Float, Int, Float>>>();                        // float - int -> float
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
        register_graph_overload<sub_, tsl_binary_map<sub_>>();
        register_graph_overload<sub_, tsl_rhs_broadcast_map<sub_>>();
        register_graph_overload<sub_, tsl_lhs_broadcast_map<sub_>>();
        register_graph_overload<sub_, tsb_binary_map<sub_>>();

        // mul_ — numeric products and string repetition.
        register_overload<mul_, lift<scalar_mul<Int>>>();
        register_overload<mul_, lift<scalar_mul<Float>>>();
        register_overload<mul_, lift<scalar_mul<Int, Float, Float>>>();
        register_overload<mul_, lift<scalar_mul<Float, Int, Float>>>();
        register_overload<mul_, repeat_string_right>();
        register_overload<mul_, repeat_string_left>();
        register_graph_overload<mul_, tsl_binary_map<mul_>>();
        register_graph_overload<mul_, tsl_rhs_broadcast_map<mul_>>();
        register_graph_overload<mul_, tsl_lhs_broadcast_map<mul_>>();
        register_graph_overload<mul_, tsb_binary_map<mul_>>();

        // div_ — the two-argument form defaults to DivideByZero::Error; the three-argument
        // form takes an explicit policy. Arity selects between them.
        register_overload<div_, lift<scalar_div<Int>>>();        // int / int -> float
        register_overload<div_, lift<scalar_div<Float>>>();      // float / float -> float
        register_overload<div_, lift<scalar_div<Int, Float>>>();
        register_overload<div_, lift<scalar_div<Float, Int>>>();
        register_overload<div_, div_numbers<Int, Int>>();             // int / int -> float (with policy)

        register_overload<add_, arithmetic_impl_detail::concat_lists_impl>();
        register_overload<sub_, arithmetic_impl_detail::set_op_impl<arithmetic_impl_detail::SetOpKind::Difference>>();
        register_overload<bit_and,
                          arithmetic_impl_detail::set_op_impl<arithmetic_impl_detail::SetOpKind::Intersection>>();
        register_overload<bit_or, arithmetic_impl_detail::set_op_impl<arithmetic_impl_detail::SetOpKind::Union>>();
        register_overload<bit_xor,
                          arithmetic_impl_detail::set_op_impl<arithmetic_impl_detail::SetOpKind::SymmetricDifference>>();
        register_overload<bit_or, arithmetic_impl_detail::merge_maps_impl>();
        register_overload<sub_, arithmetic_impl_detail::diff_maps_impl>();
        register_overload<sub_, arithmetic_impl_detail::remove_list_items_impl>();
        register_overload<mul_, arithmetic_impl_detail::timedelta_scale_impl>();
        register_overload<mul_, arithmetic_impl_detail::timedelta_scale_float_impl>();
        register_overload<mul_, arithmetic_impl_detail::period_scale_impl>();
        register_overload<mul_, arithmetic_impl_detail::int_scale_period_impl>();
        register_overload<div_, arithmetic_impl_detail::timedelta_div_impl>();
        register_overload<div_, arithmetic_impl_detail::timedelta_div_float_impl>();
        register_overload<getitem_, arithmetic_impl_detail::getitem_map_scalar_impl>();
        register_overload<and_, arithmetic_impl_detail::container_truthy_impl<true>>();
        register_overload<or_, arithmetic_impl_detail::container_truthy_impl<false>>();
        register_overload<len_, arithmetic_impl_detail::len_container_impl>();

        {
            arithmetic_impl_detail::register_container_aggregates<ValueTypeKind::Map>();
            arithmetic_impl_detail::register_container_aggregates<ValueTypeKind::Set>();
            arithmetic_impl_detail::register_container_aggregates<ValueTypeKind::List>();
        }
        register_overload<sum_, arithmetic_impl_detail::running_sum_impl<Int>>();
        register_overload<sum_, arithmetic_impl_detail::running_sum_impl<Float>>();
        register_overload<sum_, arithmetic_impl_detail::running_sum_reset_impl<Int>>();
        register_overload<sum_, arithmetic_impl_detail::running_sum_reset_impl<Float>>();
        register_graph_overload<sum_, arithmetic_impl_detail::multi_sum_impl<false>>();
        register_overload<mean, arithmetic_impl_detail::running_mean_impl<Int>>();
        register_overload<mean, arithmetic_impl_detail::running_mean_impl<Float>>();
        register_graph_overload<mean, arithmetic_impl_detail::multi_sum_impl<true>>();
        register_overload<std_, arithmetic_impl_detail::running_moments_impl<Int, true>>();
        register_overload<std_, arithmetic_impl_detail::running_moments_impl<Float, true>>();
        register_overload<var_, arithmetic_impl_detail::running_moments_impl<Int, false>>();
        register_overload<var_, arithmetic_impl_detail::running_moments_impl<Float, false>>();
        register_overload<div_, div_numbers<Float, Float>>();         // float / float -> float (with policy)
        register_overload<div_, div_numbers<Int, Float>>();
        register_overload<div_, div_numbers<Float, Int>>();
        register_overload<div_, div_timedeltas>();                    // TimeDelta / TimeDelta -> Float
        register_graph_overload<div_, tsl_binary_map<div_>>();
        register_graph_overload<div_, tsl_rhs_broadcast_map<div_>>();
        register_graph_overload<div_, tsl_lhs_broadcast_map<div_>>();
        register_graph_overload<div_, tsb_binary_map<div_>>();

        // floordiv_ / mod_ — integer outputs for int operands, Float otherwise.
        register_overload<floordiv_, lift<scalar_floordiv<Int>>>();
        register_overload<floordiv_, lift<scalar_floordiv<Float>>>();
        register_overload<floordiv_, lift<scalar_floordiv<Int, Float>>>();
        register_overload<floordiv_, lift<scalar_floordiv<Float, Int>>>();
        register_overload<floordiv_, floordiv_ints>();
        register_overload<floordiv_, floordiv_numbers<Float, Float>>();
        register_overload<floordiv_, floordiv_numbers<Int, Float>>();
        register_overload<floordiv_, floordiv_numbers<Float, Int>>();
        register_graph_overload<floordiv_, tsl_binary_map<floordiv_>>();
        register_graph_overload<floordiv_, tsl_rhs_broadcast_map<floordiv_>>();
        register_graph_overload<floordiv_, tsl_lhs_broadcast_map<floordiv_>>();
        register_graph_overload<floordiv_, tsb_binary_map<floordiv_>>();

        register_overload<mod_, lift<scalar_mod<Int>>>();
        register_overload<mod_, lift<scalar_mod<Float>>>();
        register_overload<mod_, lift<scalar_mod<Int, Float>>>();
        register_overload<mod_, lift<scalar_mod<Float, Int>>>();
        register_overload<mod_, mod_ints>();
        register_overload<mod_, mod_numbers<Float, Float>>();
        register_overload<mod_, mod_numbers<Int, Float>>();
        register_overload<mod_, mod_numbers<Float, Int>>();
        register_graph_overload<mod_, tsl_binary_map<mod_>>();
        register_graph_overload<mod_, tsl_rhs_broadcast_map<mod_>>();
        register_graph_overload<mod_, tsl_lhs_broadcast_map<mod_>>();
        register_graph_overload<mod_, tsb_binary_map<mod_>>();

        // divmod_ — mirrors floordiv_ / mod_ result typing.
        register_overload<divmod_, divmod_ints>();
        register_overload<divmod_, divmod_numbers<Float, Float>>();
        register_overload<divmod_, divmod_numbers<Int, Float>>();
        register_overload<divmod_, divmod_numbers<Float, Int>>();

        // pow_ — numeric power is explicitly Float-valued in C++.
        register_overload<pow_, lift<scalar_pow<Int>>>();
        register_overload<pow_, lift<scalar_pow<Float>>>();
        register_overload<pow_, lift<scalar_pow<Int, Float>>>();
        register_overload<pow_, lift<scalar_pow<Float, Int>>>();
        register_overload<pow_, pow_numbers<Int, Int>>();
        register_overload<pow_, pow_numbers<Float, Float>>();
        register_overload<pow_, pow_numbers<Int, Float>>();
        register_overload<pow_, pow_numbers<Float, Int>>();
        register_graph_overload<pow_, tsl_binary_map<pow_>>();
        register_graph_overload<pow_, tsl_rhs_broadcast_map<pow_>>();
        register_graph_overload<pow_, tsl_lhs_broadcast_map<pow_>>();
        register_graph_overload<pow_, tsb_binary_map<pow_>>();

        register_overload<round_, round_float_impl>();

        register_overload<neg_, lift<scalar_neg<Int>>>();
        register_overload<neg_, lift<scalar_neg<Float>>>();
        register_overload<neg_, arithmetic_impl_detail::negate_duration_impl>();
        register_overload<neg_, arithmetic_impl_detail::negate_period_impl>();
        register_graph_overload<neg_, tsl_unary_map<neg_>>();
        register_graph_overload<neg_, tsb_unary_map<neg_>>();
        register_overload<pos_, lift<scalar_pos<Int>>>();
        register_overload<pos_, lift<scalar_pos<Float>>>();
        register_overload<pos_, lift<scalar_pos<TimeDelta>>>();
        register_graph_overload<pos_, tsl_unary_map<pos_>>();
        register_graph_overload<pos_, tsb_unary_map<pos_>>();
        register_overload<abs_, lift<scalar_abs<Int>>>();
        register_overload<abs_, lift<scalar_abs<Float>>>();
        register_overload<abs_, abs_timedelta>();
        register_graph_overload<abs_, tsl_unary_map<abs_>>();
        register_graph_overload<abs_, tsb_unary_map<abs_>>();
        register_overload<sign, lift<scalar_sign<Int>>>();
        register_overload<sign, lift<scalar_sign<Float>>>();
        register_overload<ln, lift<scalar_ln>>();
    }
}  // namespace hgraph::stdlib
