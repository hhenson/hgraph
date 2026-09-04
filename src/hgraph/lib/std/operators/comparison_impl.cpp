#include <hgraph/lib/std/operators/impl/comparison_impl.h>

namespace hgraph::stdlib
{
    // The family registers through one group per translation unit; see
    // "Registration translation units" in the operators developer guide.
    void register_comparison_operators()
    {
        register_comparison_equality_overloads();
        register_comparison_ordering_overloads();
        register_comparison_extremum_overloads();
    }
}  // namespace hgraph::stdlib
