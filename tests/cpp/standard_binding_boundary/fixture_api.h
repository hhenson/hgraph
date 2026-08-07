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

HGRAPH_STANDARD_BINDING_FIXTURE_API const hgraph::MemoryUtils::StoragePlan *
hgraph_standard_bytes_plan() noexcept;
HGRAPH_STANDARD_BINDING_FIXTURE_API const hgraph::ValueOps *hgraph_standard_bytes_ops() noexcept;
HGRAPH_STANDARD_BINDING_FIXTURE_API const hgraph::MemoryUtils::StoragePlan *
hgraph_standard_str_plan() noexcept;
HGRAPH_STANDARD_BINDING_FIXTURE_API const hgraph::ValueOps *hgraph_standard_str_ops() noexcept;

#endif  // HGRAPH_TESTS_CPP_STANDARD_BINDING_BOUNDARY_FIXTURE_API_H
