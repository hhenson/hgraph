#include "fixture_api.h"

#include <hgraph/lib/std/standard_types.h>

const hgraph::MemoryUtils::StoragePlan *hgraph_standard_bytes_plan() noexcept
{
    return &hgraph::MemoryUtils::plan_for<hgraph::Bytes>();
}

const hgraph::ValueOps *hgraph_standard_bytes_ops() noexcept
{
    return &hgraph::ops_for<hgraph::Bytes>();
}

const hgraph::MemoryUtils::StoragePlan *hgraph_standard_str_plan() noexcept
{
    return &hgraph::MemoryUtils::plan_for<hgraph::Str>();
}

const hgraph::ValueOps *hgraph_standard_str_ops() noexcept
{
    return &hgraph::ops_for<hgraph::Str>();
}
