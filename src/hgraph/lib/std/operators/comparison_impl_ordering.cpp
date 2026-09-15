#include <hgraph/lib/std/operators/impl/comparison_impl.h>

namespace hgraph::stdlib
{
    // lt_ / le_ / gt_ / ge_ over the ordered scalars and enums.
    //
    // There is deliberately NO mixed int/float overload. Released hgraph
    // declares these as ``(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE)``
    // and resolves both sides to one type, so every mixed ordering fails at
    // wiring there -- with a raw scalar (``gt_(TS[float], 1)``) and with two
    // time series (``gt_(TS[float], TS[int])``) alike. Accepting them here
    // meant a comparison released hgraph refuses silently answered
    // (parity #818 item 5.7).
    //
    // ``eq_`` is NOT the same case and keeps its mixed form: upstream gives
    // it a float-epsilon overload, so ``eq_(TS[float], 1)`` wires and answers
    // there too.
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
