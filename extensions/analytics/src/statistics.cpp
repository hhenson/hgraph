#include <hgraph/analytics/operators.h>

#include "operator_registration.h"

namespace hgraph::analytics::detail
{
    // The statistics operators register through one group per translation
    // unit; see "Registration translation units" in the operators developer
    // guide.
    void register_statistics_operators()
    {
        register_statistics_container_overloads();
        register_statistics_collection_overloads();
        register_statistics_window_overloads();
    }
}  // namespace hgraph::analytics::detail
