#include <hgraph/analytics/operators.h>

#include "operator_registration.h"

namespace hgraph::analytics
{
    void register_analytics_operators()
    {
        detail::register_numerical_operators();
        detail::register_array_operators();
        detail::register_shaped_array_operators();
    }
}  // namespace hgraph::analytics
