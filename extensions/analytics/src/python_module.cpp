#include <hgraph/analytics/operators.h>

#include <nanobind/nanobind.h>

namespace nb = nanobind;

NB_MODULE(_hgraph_analytics, module)
{
    hgraph::analytics::register_analytics_operators();
    module.doc() = "Native hgraph-analytics operator registration";
}
