//
// TSContext - Navigation Context Implementation
//

#include <hgraph/types/time_series/v2/ts_context.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/time_series_type.h>

namespace hgraph {

node_ptr TSContext::owning_node() const {
    if (std::holds_alternative<node_ptr>(owner)) {
        return std::get<node_ptr>(owner);
    }
    // Navigate up through parent time-series
    auto* parent = std::get<TimeSeriesType*>(owner);
    return parent ? parent->owning_node() : nullptr;
}

graph_ptr TSContext::owning_graph() const {
    auto node = owning_node();
    return node ? node->graph() : nullptr;
}

engine_time_t TSContext::current_time() const {
    auto node = owning_node();
    if (!node) return MIN_DT;

    // Use cached evaluation time from node if available
    auto* t = node->cached_evaluation_time_ptr();
    return t ? *t : MIN_DT;
}

} // namespace hgraph
