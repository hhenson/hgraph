#ifndef HGRAPH_TESTS_CPP_STANDARD_BINDING_BOUNDARY_FIXTURE_API_H
#define HGRAPH_TESTS_CPP_STANDARD_BINDING_BOUNDARY_FIXTURE_API_H

#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value_ops.h>

#if defined(_WIN32)
#if defined(HGRAPH_STANDARD_BINDING_FIXTURE_BUILD)
#define HGRAPH_STANDARD_BINDING_FIXTURE_API __declspec(dllexport)
#else
#define HGRAPH_STANDARD_BINDING_FIXTURE_API __declspec(dllimport)
#endif
#else
#define HGRAPH_STANDARD_BINDING_FIXTURE_API __attribute__((visibility("default")))
#endif

#define HGRAPH_DECLARE_STANDARD_BINDING_ACCESSORS(Name)                                           \
    HGRAPH_STANDARD_BINDING_FIXTURE_API const hgraph::MemoryUtils::StoragePlan *                  \
    hgraph_standard_##Name##_plan() noexcept;                                                     \
    HGRAPH_STANDARD_BINDING_FIXTURE_API const hgraph::ValueOps *                                  \
    hgraph_standard_##Name##_ops() noexcept

HGRAPH_DECLARE_STANDARD_BINDING_ACCESSORS(bytes);
HGRAPH_DECLARE_STANDARD_BINDING_ACCESSORS(str);
HGRAPH_DECLARE_STANDARD_BINDING_ACCESSORS(divide_by_zero);
HGRAPH_DECLARE_STANDARD_BINDING_ACCESSORS(cmp_result);
HGRAPH_DECLARE_STANDARD_BINDING_ACCESSORS(switch_cases);
HGRAPH_DECLARE_STANDARD_BINDING_ACCESSORS(dispatch_cases);
HGRAPH_DECLARE_STANDARD_BINDING_ACCESSORS(map_call_config);
HGRAPH_DECLARE_STANDARD_BINDING_ACCESSORS(try_except_call_config);

#undef HGRAPH_DECLARE_STANDARD_BINDING_ACCESSORS

#endif  // HGRAPH_TESTS_CPP_STANDARD_BINDING_BOUNDARY_FIXTURE_API_H
