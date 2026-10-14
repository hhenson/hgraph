#include "fixture_api.h"

#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/lib/std/standard_types.h>

#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace
{
    void require(bool condition, std::string_view message)
    {
        if (!condition) { throw std::runtime_error{std::string{message}}; }
    }

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
    try
    {
        auto retained_error = hgraph_standard_canonical_error();
        require(retained_error.schema() == hgraph::scalar_descriptor<hgraph::CanonicalError>::value_meta(),
                "canonical error schema differs across DSO");
        const auto observation = retained_error.view();
        const auto &error = observation.checked_as<hgraph::CanonicalError>();
        require(error.code == "value.missing_key" && error.context && error.context->node == "child@owned",
                "owned error provenance was lost across DSO");
        auto outcome = hgraph_standard_node_result();
        require(!outcome && outcome.error() == error, "typed operational outcome was lost across DSO");
        require(outcome.error().context && outcome.error().context->evaluation_time == hgraph::MIN_ST,
                "error origin time was lost across DSO");
    }
    catch (const std::exception &error)
    {
        std::cerr << "canonical error boundary test failed: " << error.what() << '\n';
        return 1;
    }

    const bool canonical =
        canonical_binding<hgraph::CanonicalError>(
            hgraph_standard_canonical_error_plan, hgraph_standard_canonical_error_ops) &&
        canonical_binding<hgraph::ErrorContext>(
            hgraph_standard_error_context_plan, hgraph_standard_error_context_ops) &&
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
