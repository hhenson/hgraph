#include <hgraph/lib/std/operators/impl/arithmetic_impl.h>

namespace hgraph::stdlib
{
    // Operators over TS[list] / TS[set] / TS[map] values and the running
    // sum_ / mean accumulators. The min_ / max_ / sum_ / mean reductions
    // over a single container are the aggregate group.
    void register_arithmetic_container_overloads()
    {
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
        register_overload<getitem_, arithmetic_impl_detail::getitem_map_scalar_impl>();
        register_overload<and_, arithmetic_impl_detail::container_truthy_impl<true>>();
        register_overload<or_, arithmetic_impl_detail::container_truthy_impl<false>>();
        register_overload<len_, arithmetic_impl_detail::len_container_impl>();

        register_overload<sum_, arithmetic_impl_detail::running_sum_impl<Int>>();
        register_overload<sum_, arithmetic_impl_detail::running_sum_impl<Float>>();
        register_overload<sum_, arithmetic_impl_detail::running_sum_reset_impl<Int>>();
        register_overload<sum_, arithmetic_impl_detail::running_sum_reset_impl<Float>>();
        register_graph_overload<sum_, arithmetic_impl_detail::multi_sum_impl<false>>();
        register_overload<mean, arithmetic_impl_detail::running_mean_impl<Int>>();
        register_overload<mean, arithmetic_impl_detail::running_mean_impl<Float>>();
        register_graph_overload<mean, arithmetic_impl_detail::multi_sum_impl<true>>();
    }
}  // namespace hgraph::stdlib
