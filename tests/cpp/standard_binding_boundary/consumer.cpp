#include "fixture_api.h"

#include <hgraph/lib/std/standard_types.h>

#include <iostream>

int main()
{
    const bool bytes_plan_is_canonical =
        hgraph_standard_bytes_plan() == &hgraph::MemoryUtils::plan_for<hgraph::Bytes>();
    const bool bytes_ops_are_canonical = hgraph_standard_bytes_ops() == &hgraph::ops_for<hgraph::Bytes>();
    const bool str_plan_is_canonical =
        hgraph_standard_str_plan() == &hgraph::MemoryUtils::plan_for<hgraph::Str>();
    const bool str_ops_are_canonical = hgraph_standard_str_ops() == &hgraph::ops_for<hgraph::Str>();

    if (!bytes_plan_is_canonical || !bytes_ops_are_canonical || !str_plan_is_canonical ||
        !str_ops_are_canonical)
    {
        std::cerr << "standard scalar bindings are not canonical across the shared-library boundary\n";
        return 1;
    }

    return 0;
}
