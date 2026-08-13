#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/lib/std/operators/io.h>

namespace hgraph
{
    // Explicit instantiation gives public stdlib scalars one exported plan and
    // ops address across the core runtime and independently built extensions.
#define HGRAPH_DEFINE_STDLIB_SCALAR_BINDING(Type)                                                 \
    template HGRAPH_EXPORT const MemoryUtils::StoragePlan &MemoryUtils::plan_for<Type>() noexcept; \
    template HGRAPH_EXPORT const ValueOps &ops_for<Type>() noexcept

    HGRAPH_DEFINE_STDLIB_SCALAR_BINDING(stdlib::DivideByZero);
    HGRAPH_DEFINE_STDLIB_SCALAR_BINDING(stdlib::CmpResult);
    HGRAPH_DEFINE_STDLIB_SCALAR_BINDING(stdlib::SwitchCases);
    HGRAPH_DEFINE_STDLIB_SCALAR_BINDING(stdlib::DispatchCases);
    HGRAPH_DEFINE_STDLIB_SCALAR_BINDING(stdlib::MapCallConfig);
    HGRAPH_DEFINE_STDLIB_SCALAR_BINDING(stdlib::TryExceptCallConfig);
    HGRAPH_DEFINE_STDLIB_SCALAR_BINDING(stdlib::RecordAsOf);
    HGRAPH_DEFINE_STDLIB_SCALAR_BINDING(stdlib::RecordRemoves);

#undef HGRAPH_DEFINE_STDLIB_SCALAR_BINDING
}  // namespace hgraph
