#include <hgraph/lib/std/operators/impl/comparison_impl.h>

namespace hgraph::stdlib
{
    // lt_ / le_ / gt_ / ge_ over the ordered scalars, mixed numerics and
    // enums.
    void register_comparison_ordering_overloads()
    {
        register_ordered_same_scalar_comparisons<lt_, scalar_lt>();
        register_ordered_same_scalar_comparisons<le_, scalar_le>();
        register_ordered_same_scalar_comparisons<gt_, scalar_gt>();
        register_ordered_same_scalar_comparisons<ge_, scalar_ge>();
        register_mixed_numeric_comparisons<lt_, scalar_lt>();
        register_mixed_numeric_comparisons<le_, scalar_le>();
        register_mixed_numeric_comparisons<gt_, scalar_gt>();
        register_mixed_numeric_comparisons<ge_, scalar_ge>();

        register_overload<lt_, comparison_impl_detail::enum_ordering_impl<0>>();
        register_overload<le_, comparison_impl_detail::enum_ordering_impl<1>>();
        register_overload<gt_, comparison_impl_detail::enum_ordering_impl<2>>();
        register_overload<ge_, comparison_impl_detail::enum_ordering_impl<3>>();
    }
}  // namespace hgraph::stdlib
