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

std::shared_ptr<TimeSeriesInput> TSContext::parent_input() const {
    if (!std::holds_alternative<TimeSeriesType*>(owner)) {
        return nullptr;  // Owner is a node, not a parent time-series
    }
    auto* parent = std::get<TimeSeriesType*>(owner);
    if (auto* input = dynamic_cast<TimeSeriesInput*>(parent)) {
        // Use shared_from_this() to get the shared_ptr
        return input->shared_from_this();
    }
    return nullptr;
}

std::shared_ptr<TimeSeriesOutput> TSContext::parent_output() const {
    if (!std::holds_alternative<TimeSeriesType*>(owner)) {
        return nullptr;  // Owner is a node, not a parent time-series
    }
    auto* parent = std::get<TimeSeriesType*>(owner);
    if (auto* output = dynamic_cast<TimeSeriesOutput*>(parent)) {
        // Use shared_from_this() to get the shared_ptr
        return output->shared_from_this();
    }
    return nullptr;
}

} // namespace hgraph
