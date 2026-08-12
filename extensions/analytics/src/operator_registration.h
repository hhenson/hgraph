#ifndef HGRAPH_ANALYTICS_OPERATOR_REGISTRATION_H
#define HGRAPH_ANALYTICS_OPERATOR_REGISTRATION_H

namespace hgraph::analytics::detail
{
    void register_numerical_operators();
    void register_array_operators();
    void register_shaped_array_operators();
}  // namespace hgraph::analytics::detail

#endif  // HGRAPH_ANALYTICS_OPERATOR_REGISTRATION_H
