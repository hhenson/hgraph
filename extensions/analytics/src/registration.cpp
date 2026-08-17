#include <hgraph/analytics/operators.h>
#include <hgraph/types/operator_dispatch.h>

#include "operator_registration.h"

namespace hgraph::analytics
{
    namespace
    {
        void install_analytics_operators()
        {
            detail::register_numerical_operators();
            detail::register_array_operators();
            detail::register_shaped_array_operators();
            detail::register_statistics_operators();
        }
    }  // namespace

    void register_analytics_operators()
    {
        // Keyed installer (RFC 0025 checkpoint 3): a registry reset clears
        // the overload table but keeps registration intent, so the next
        // rebuild replays this extension exactly as it replays core.
        auto &registry = OperatorRegistry::instance();
        registry.register_installer("hgraph.analytics", &install_analytics_operators);
        registry.run_installers();
    }
}  // namespace hgraph::analytics
