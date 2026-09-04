#include <hgraph/lib/std/operators/impl/comparison_impl.h>

namespace hgraph::stdlib
{
    // eq_ / ne_ / cmp_ over scalars, the epsilon and mixed-numeric forms,
    // the TSL item-wise graphs and the Any fallbacks.
    void register_comparison_equality_overloads()
    {
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

        register_ordered_same_scalar_comparisons<cmp_, scalar_cmp>();
        register_overload<cmp_, lift<scalar_cmp<Bool>>>();
        register_overload<eq_, comparison_impl_detail::eq_any_impl>();
        register_overload<ne_, comparison_impl_detail::ne_any_impl>();
        register_overload<cmp_, comparison_impl_detail::cmp_any_impl>();
        register_mixed_numeric_comparisons<cmp_, scalar_cmp>();
    }
}  // namespace hgraph::stdlib
