#include <hgraph/lib/std/operators/impl/comparison_impl.h>

namespace hgraph::stdlib
{
    // min_ / max_: binary scalar forms with their identity defaults, enum,
    // running, non-strict and multi-input variants, and the TSL / TSB maps.
    void register_comparison_extremum_overloads()
    {
        using tsl_itemwise_impl_detail::tsl_binary_map;
        using tsl_itemwise_impl_detail::tsl_lhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_rhs_broadcast_map;
        using tsb_itemwise_impl_detail::tsb_binary_map;

        register_overload<min_, comparison_impl_detail::enum_extremum_impl<true>>();
        register_overload<max_, comparison_impl_detail::enum_extremum_impl<false>>();
        register_overload<min_, comparison_impl_detail::running_extremum_impl<true>>();
        register_overload<max_, comparison_impl_detail::running_extremum_impl<false>>();
        register_overload<min_, comparison_impl_detail::nonstrict_extremum_impl<true>>();
        register_overload<max_, comparison_impl_detail::nonstrict_extremum_impl<false>>();
        register_graph_overload<min_, comparison_impl_detail::multi_extremum_impl<true>>();
        register_graph_overload<max_, comparison_impl_detail::multi_extremum_impl<false>>();

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
