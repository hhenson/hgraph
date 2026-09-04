#include <hgraph/lib/std/operators/impl/temporal_impl.h>

namespace hgraph::stdlib
{
    // The family registers through one group per translation unit; see
    // "Registration translation units" in the operators developer guide.
    void register_temporal_operators()
    {
        register_temporal_components_overloads();
        register_temporal_instants_overloads();
    }
}  // namespace hgraph::stdlib
