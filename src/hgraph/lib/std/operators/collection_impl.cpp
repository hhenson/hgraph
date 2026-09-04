#include <hgraph/lib/std/operators/impl/collection_impl.h>

namespace hgraph::stdlib
{
    // The family registers through one group per translation unit; see
    // "Registration translation units" in the operators developer guide.
    void register_collection_operators()
    {
        register_collection_mapping_overloads();
        register_collection_sequence_overloads();
        register_collection_aggregate_overloads();
        register_collection_set_overloads();
    }
}  // namespace hgraph::stdlib
