#include "fixture_api.h"

#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/lib/std/standard_types.h>

#include <iostream>

namespace
{
    template <typename T>
    [[nodiscard]] bool canonical_binding(
        const hgraph::MemoryUtils::StoragePlan *(*plan)() noexcept,
        const hgraph::ValueOps *(*ops)() noexcept)
    {
        return plan() == &hgraph::MemoryUtils::plan_for<T>() && ops() == &hgraph::ops_for<T>();
    }
}

int main()
{
    const bool canonical =
        canonical_binding<hgraph::Bytes>(hgraph_standard_bytes_plan, hgraph_standard_bytes_ops) &&
        canonical_binding<hgraph::Str>(hgraph_standard_str_plan, hgraph_standard_str_ops) &&
        canonical_binding<hgraph::stdlib::DivideByZero>(
            hgraph_standard_divide_by_zero_plan, hgraph_standard_divide_by_zero_ops) &&
        canonical_binding<hgraph::stdlib::CmpResult>(
            hgraph_standard_cmp_result_plan, hgraph_standard_cmp_result_ops) &&
        canonical_binding<hgraph::stdlib::SwitchCases>(
            hgraph_standard_switch_cases_plan, hgraph_standard_switch_cases_ops) &&
        canonical_binding<hgraph::stdlib::DispatchCases>(
            hgraph_standard_dispatch_cases_plan, hgraph_standard_dispatch_cases_ops) &&
        canonical_binding<hgraph::stdlib::MapCallConfig>(
            hgraph_standard_map_call_config_plan, hgraph_standard_map_call_config_ops) &&
        canonical_binding<hgraph::stdlib::TryExceptCallConfig>(
            hgraph_standard_try_except_call_config_plan,
            hgraph_standard_try_except_call_config_ops);

    if (!canonical)
    {
        std::cerr << "standard scalar bindings are not canonical across the shared-library boundary\n";
        return 1;
    }

    return 0;
}
