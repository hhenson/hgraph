#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_LOGICAL_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_LOGICAL_IMPL_H

#include <hgraph/lib/std/lifted_kernels.h>
#include <hgraph/lib/std/operators/impl/tsb_itemwise_impl.h>
#include <hgraph/lib/std/operators/impl/tsl_itemwise_impl.h>
#include <hgraph/lib/std/operators/logical.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/lift.h>

namespace hgraph::stdlib
{
    template <typename Operator, template <typename...> class Kernel>
    inline void register_truthy_binary_overloads()
    {
        register_overload<Operator, lift<Kernel<Bool, Bool>>>();
        register_overload<Operator, lift<Kernel<Int, Int>>>();
        register_overload<Operator, lift<Kernel<Float, Float>>>();
        register_overload<Operator, lift<Kernel<Str, Str>>>();
        register_overload<Operator, lift<Kernel<Int, Float>>>();
        register_overload<Operator, lift<Kernel<Float, Int>>>();
    }

    void register_logical_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_LOGICAL_IMPL_H
