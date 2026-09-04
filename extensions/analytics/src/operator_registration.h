#ifndef HGRAPH_ANALYTICS_OPERATOR_REGISTRATION_H
#define HGRAPH_ANALYTICS_OPERATOR_REGISTRATION_H

namespace hgraph::analytics::detail
{
    void register_numerical_operators();
    void register_array_operators();
    void register_shaped_array_operators();
    // One translation unit per group (statistics_<group>.cpp); see
    // "Registration translation units" in the operators developer guide.
    void register_statistics_container_overloads();
    void register_statistics_collection_overloads();
    void register_statistics_window_overloads();
    void register_statistics_operators();
}  // namespace hgraph::analytics::detail

#endif  // HGRAPH_ANALYTICS_OPERATOR_REGISTRATION_H
