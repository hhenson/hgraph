#include <hgraph/nodes/non_associative_reduce_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/traits.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {
    TsdNonAssociativeReduceNode::TsdNonAssociativeReduceNode(
        int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
        nb::dict scalars, graph_builder_s_ptr nested_graph_builder,
        const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id,
        const TimeSeriesTypeMeta* input_meta, const TimeSeriesTypeMeta* output_meta,
        const TimeSeriesTypeMeta* error_output_meta, const TimeSeriesTypeMeta* recordable_state_meta)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          nested_graph_builder_(std::move(nested_graph_builder)),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id) {
    }

    std::unordered_map<int, graph_s_ptr> TsdNonAssociativeReduceNode::nested_graphs() {
        return {{0, nested_graph_}};
    }

    void TsdNonAssociativeReduceNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const {
        if (nested_graph_) {
            callback(nested_graph_);
        }
    }

    int64_t TsdNonAssociativeReduceNode::node_size() const {
        // Cache the node size
        if (cached_node_size_ < 0) {
            const_cast<TsdNonAssociativeReduceNode *>(this)->cached_node_size_ =
                    nested_graph_builder_->node_builders.size();
        }
        return cached_node_size_;
    }

    int64_t TsdNonAssociativeReduceNode::node_count() const {
        return nested_graph_ ? nested_graph_->nodes().size() / node_size() : 0;
    }

    // TODO: Implement TsdNonAssociativeReduceNode

    void TsdNonAssociativeReduceNode::initialise() {
        throw std::runtime_error("TsdNonAssociativeReduceNode::initialise not yet implemented");
    }

    void TsdNonAssociativeReduceNode::do_start() {
        throw std::runtime_error("TsdNonAssociativeReduceNode::do_start not yet implemented");
    }

    void TsdNonAssociativeReduceNode::do_stop() {
        if (nested_graph_) { stop_component(*nested_graph_); }
    }

    void TsdNonAssociativeReduceNode::dispose() {
        if (nested_graph_) {
            dispose_component(*nested_graph_);
            nested_graph_ = nullptr;
        }
    }

    void TsdNonAssociativeReduceNode::eval() {
        throw std::runtime_error("TsdNonAssociativeReduceNode::eval not yet implemented");
    }

    void TsdNonAssociativeReduceNode::update_changes() {
        throw std::runtime_error("TsdNonAssociativeReduceNode::update_changes not yet implemented");
    }

    void TsdNonAssociativeReduceNode::extend_nodes_to(int64_t) {
        throw std::runtime_error("TsdNonAssociativeReduceNode::extend_nodes_to not yet implemented");
    }

    void TsdNonAssociativeReduceNode::erase_nodes_from(int64_t ndx) {
        if (nested_graph_) { nested_graph_->reduce_graph(ndx * node_size()); }
    }

    void TsdNonAssociativeReduceNode::bind_output() {
        throw std::runtime_error("TsdNonAssociativeReduceNode::bind_output not yet implemented");
    }

    nb::object TsdNonAssociativeReduceNode::last_output_value() {
        throw std::runtime_error("TsdNonAssociativeReduceNode::last_output_value not yet implemented");
    }

    std::vector<node_s_ptr> TsdNonAssociativeReduceNode::get_node(int64_t) {
        throw std::runtime_error("TsdNonAssociativeReduceNode::get_node not yet implemented");
    }

    void register_non_associative_reduce_node_with_nanobind(nb::module_ &m) {
        nb::class_ < TsdNonAssociativeReduceNode, NestedNode > (m, "TsdNonAssociativeReduceNode")
                .def_prop_ro("nested_graphs", &TsdNonAssociativeReduceNode::nested_graphs);
    }
} // namespace hgraph