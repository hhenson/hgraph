#include "fixture_api.h"

#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/lib/std/standard_types.h>

#define HGRAPH_DEFINE_STANDARD_BINDING_ACCESSORS(Name, Type)                                      \
    const hgraph::MemoryUtils::StoragePlan *hgraph_standard_##Name##_plan() noexcept              \
    {                                                                                             \
        return &hgraph::MemoryUtils::plan_for<Type>();                                            \
    }                                                                                             \
    const hgraph::ValueOps *hgraph_standard_##Name##_ops() noexcept                               \
    {                                                                                             \
        return &hgraph::ops_for<Type>();                                                          \
    }

HGRAPH_DEFINE_STANDARD_BINDING_ACCESSORS(canonical_error, hgraph::CanonicalError)
HGRAPH_DEFINE_STANDARD_BINDING_ACCESSORS(error_context, hgraph::ErrorContext)
HGRAPH_DEFINE_STANDARD_BINDING_ACCESSORS(bytes, hgraph::Bytes)
HGRAPH_DEFINE_STANDARD_BINDING_ACCESSORS(str, hgraph::Str)
HGRAPH_DEFINE_STANDARD_BINDING_ACCESSORS(divide_by_zero, hgraph::stdlib::DivideByZero)
HGRAPH_DEFINE_STANDARD_BINDING_ACCESSORS(cmp_result, hgraph::stdlib::CmpResult)
HGRAPH_DEFINE_STANDARD_BINDING_ACCESSORS(switch_cases, hgraph::stdlib::SwitchCases)
HGRAPH_DEFINE_STANDARD_BINDING_ACCESSORS(dispatch_cases, hgraph::stdlib::DispatchCases)
HGRAPH_DEFINE_STANDARD_BINDING_ACCESSORS(map_call_config, hgraph::stdlib::MapCallConfig)
HGRAPH_DEFINE_STANDARD_BINDING_ACCESSORS(try_except_call_config, hgraph::stdlib::TryExceptCallConfig)

#undef HGRAPH_DEFINE_STANDARD_BINDING_ACCESSORS

hgraph::Value hgraph_standard_canonical_error()
{
    static_cast<void>(hgraph::scalar_descriptor<hgraph::CanonicalError>::value_meta());
    return hgraph::Value{hgraph::CanonicalError{
        "value.missing_key", "missing price",
        hgraph::ErrorContext{"child@owned", "evaluate", hgraph::MIN_ST}}};
}

hgraph::NodeResult<void> hgraph_standard_node_result()
{
    return std::unexpected{hgraph::CanonicalError{
        "value.missing_key", "missing price",
        hgraph::ErrorContext{"child@owned", "evaluate", hgraph::MIN_ST}}};
}
