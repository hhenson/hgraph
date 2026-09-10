#include <hgraph/lib/std/operators/impl/comparison_impl.h>

namespace hgraph::stdlib
{
    // lt_ / le_ / gt_ / ge_ over the ordered scalars and enums.
    //
    // Deliberately NO mixed Int/Float kernels: comparing TS<Float> with an Int
    // is an implicit numeric cast, and this type system does not do those
    // (ruling 2026-09-10). Released hgraph rejects the same spellings, so the
    // strict reading is also the parity-matching one. eq_ and cmp_ keep theirs
    // because upstream genuinely has them -- see comparison_impl_equality.cpp.
    void register_comparison_ordering_overloads()
    {
        register_ordered_same_scalar_comparisons<lt_, scalar_lt>();
        register_ordered_same_scalar_comparisons<le_, scalar_le>();
        register_ordered_same_scalar_comparisons<gt_, scalar_gt>();
        register_ordered_same_scalar_comparisons<ge_, scalar_ge>();

        register_overload<lt_, comparison_impl_detail::enum_ordering_impl<0>>();
        register_overload<le_, comparison_impl_detail::enum_ordering_impl<1>>();
        register_overload<gt_, comparison_impl_detail::enum_ordering_impl<2>>();
        register_overload<ge_, comparison_impl_detail::enum_ordering_impl<3>>();
    }
}  // namespace hgraph::stdlib
