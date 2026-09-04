#include <hgraph/lib/std/operators/impl/arithmetic_impl.h>

namespace hgraph::stdlib
{
    // The family registers through one group per translation unit; see
    // "Registration translation units" in the operators developer guide.
    void register_arithmetic_operators()
    {
        register_arithmetic_numeric_overloads();
        register_arithmetic_division_overloads();
        register_arithmetic_temporal_overloads();
        register_arithmetic_container_overloads();
        register_arithmetic_aggregate_overloads();
    }
}  // namespace hgraph::stdlib
