#include <hgraph/lib/std/operators/impl/comparison_impl.h>

namespace hgraph::stdlib
{
    void register_comparison_operators()
    {
        using tsl_itemwise_impl_detail::tsl_binary_map;
        using tsl_itemwise_impl_detail::tsl_lhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_rhs_broadcast_map;
        using tsb_itemwise_impl_detail::tsb_binary_map;

        register_overload<eq_, lift<scalar_eq<Bool>>>();
        register_overload<eq_, lift<scalar_eq<Int>>>();
        register_overload<eq_, lift<scalar_eq<Str>>>();
        register_overload<eq_, lift<scalar_eq<Date>>>();
        register_overload<eq_, lift<scalar_eq<DateTime>>>();
        register_overload<eq_, lift<scalar_eq<TimeDelta>>>();
        register_overload<eq_, eq_numeric_epsilon<Float, Float>>();
        register_overload<eq_, eq_numeric_epsilon<Int, Float>>();
        register_overload<eq_, eq_numeric_epsilon<Float, Int>>();
        register_graph_overload<eq_, comparison_impl_detail::eq_tsl>();

        register_overload<ne_, lift<scalar_ne<Bool>>>();
        register_overload<ne_, lift<scalar_ne<Int>>>();
        register_overload<ne_, lift<scalar_ne<Float>>>();
        register_overload<ne_, lift<scalar_ne<Str>>>();
        register_overload<ne_, lift<scalar_ne<Date>>>();
        register_overload<ne_, lift<scalar_ne<DateTime>>>();
        register_overload<ne_, lift<scalar_ne<TimeDelta>>>();
        register_mixed_numeric_comparisons<ne_, scalar_ne>();
        register_graph_overload<ne_, comparison_impl_detail::ne_tsl>();

        register_ordered_same_scalar_comparisons<lt_, scalar_lt>();
        register_ordered_same_scalar_comparisons<le_, scalar_le>();
        register_ordered_same_scalar_comparisons<gt_, scalar_gt>();
        register_ordered_same_scalar_comparisons<ge_, scalar_ge>();
        register_mixed_numeric_comparisons<lt_, scalar_lt>();
        register_mixed_numeric_comparisons<le_, scalar_le>();
        register_mixed_numeric_comparisons<gt_, scalar_gt>();
        register_mixed_numeric_comparisons<ge_, scalar_ge>();

        register_ordered_same_scalar_comparisons<cmp_, scalar_cmp>();
        register_overload<cmp_, lift<scalar_cmp<Bool>>>();
        register_overload<eq_, comparison_impl_detail::eq_any_impl>();
        register_overload<ne_, comparison_impl_detail::ne_any_impl>();
        register_overload<cmp_, comparison_impl_detail::cmp_any_impl>();
        register_overload<min_, comparison_impl_detail::enum_extremum_impl<true>>();
        register_overload<max_, comparison_impl_detail::enum_extremum_impl<false>>();
        register_overload<lt_, comparison_impl_detail::enum_ordering_impl<0>>();
        register_overload<le_, comparison_impl_detail::enum_ordering_impl<1>>();
        register_overload<gt_, comparison_impl_detail::enum_ordering_impl<2>>();
        register_overload<ge_, comparison_impl_detail::enum_ordering_impl<3>>();
        register_overload<min_, comparison_impl_detail::running_extremum_impl<true>>();
        register_overload<max_, comparison_impl_detail::running_extremum_impl<false>>();
        register_overload<min_, comparison_impl_detail::nonstrict_extremum_impl<true>>();
        register_overload<max_, comparison_impl_detail::nonstrict_extremum_impl<false>>();
        register_graph_overload<min_, comparison_impl_detail::multi_extremum_impl<true>>();
        register_graph_overload<max_, comparison_impl_detail::multi_extremum_impl<false>>();
        register_mixed_numeric_comparisons<cmp_, scalar_cmp>();

        register_overload<min_, lift<scalar_min<Int>, std::numeric_limits<Int>::max()>>();
        register_overload<min_, lift<scalar_min<Float>, std::numeric_limits<Float>::infinity()>>();
        register_overload<min_, lift<scalar_min<Str>>>();
        register_overload<min_, lift<scalar_min<Date>>>();
        register_overload<min_, lift<scalar_min<DateTime>>>();
        register_overload<min_, lift<scalar_min<TimeDelta>>>();
        register_graph_overload<min_, tsl_binary_map<min_>>();
        register_graph_overload<min_, tsl_rhs_broadcast_map<min_>>();
        register_graph_overload<min_, tsl_lhs_broadcast_map<min_>>();
        register_graph_overload<min_, tsb_binary_map<min_>>();

        register_overload<max_, lift<scalar_max<Int>, std::numeric_limits<Int>::lowest()>>();
        register_overload<max_, lift<scalar_max<Float>, -std::numeric_limits<Float>::infinity()>>();
        register_overload<max_, lift<scalar_max<Str>>>();
        register_overload<max_, lift<scalar_max<Date>>>();
        register_overload<max_, lift<scalar_max<DateTime>>>();
        register_overload<max_, lift<scalar_max<TimeDelta>>>();
        register_mixed_numeric_comparisons<min_, scalar_min>();
        register_mixed_numeric_comparisons<max_, scalar_max>();
        register_graph_overload<max_, tsl_binary_map<max_>>();
        register_graph_overload<max_, tsl_rhs_broadcast_map<max_>>();
        register_graph_overload<max_, tsl_lhs_broadcast_map<max_>>();
        register_graph_overload<max_, tsb_binary_map<max_>>();
    }
}  // namespace hgraph::stdlib
