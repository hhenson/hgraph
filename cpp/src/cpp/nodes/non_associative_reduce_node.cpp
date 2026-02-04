#include <hgraph/nodes/non_associative_reduce_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/traits.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {
    TsdNonAssociativeReduceNode::TsdNonAssociativeReduceNode(
        int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
        nb::dict scalars,
        const TSMeta* input_meta, const TSMeta* output_meta,
        const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
        graph_builder_s_ptr nested_graph_builder,
        const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          nested_graph_builder_(std::move(nested_graph_builder)),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id) {
    }

    void TsdNonAssociativeReduceNode::initialise() {
        // Create nested graph and set up evaluation engine (matches Python lines 319, 329-331)
        //TODO: Should this be constructed by the GraphBuilder, if it does not escape into Python then
        //      this is fine, otherwise not.
        nested_graph_ = arena_make_shared<Graph>(std::vector<int64_t>{node_ndx()}, std::vector<node_s_ptr>{}, this, "", &graph()->traits());
        nested_graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(), std::make_shared<NestedEngineEvaluationClock>(graph()->evaluation_engine_clock().get(), this)));
        initialise_component(*nested_graph_);
    }

    void TsdNonAssociativeReduceNode::do_start() {
        // TODO: Convert to TSInput-based approach
        // Need to access input TSB's "ts" field (which is a TSD) to check validity
        // For now, just start the nested graph
        // auto tsd = (*input())["ts"];
        // if (tsd->valid()) {
        //     update_changes();
        //     notify();
        // }

        // Start the nested graph (matches Python line 340)
        start_component(*nested_graph_);
    }

    void TsdNonAssociativeReduceNode::do_stop() {
        // Stop the nested graph (matches Python lines 343-345)
        stop_component(*nested_graph_);
    }

    void TsdNonAssociativeReduceNode::dispose() {
        // Clean up nested graph
        if (nested_graph_) {
            dispose_component(*nested_graph_);
            nested_graph_ = nullptr;
        }
    }

    void TsdNonAssociativeReduceNode::eval() {
        // Mark as evaluated (matches Python line 348)
        mark_evaluated();

        // TODO: Convert to TSInput-based approach
        // Need to access input TSB's "ts" field (TSD) to check if modified
        // auto tsd = (*input())["ts"];
        // if (tsd->modified()) {
        //     update_changes();
        // }

        // Evaluate nested graph (matches Python lines 352-354)
        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(nested_graph_->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        nested_graph_->evaluate_graph();

        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(nested_graph_->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        // Bind output (matches Python line 356)
        bind_output();
    }

    void TsdNonAssociativeReduceNode::update_changes() {
        // TODO: Convert to TSInput-based approach
        // Need to access input TSB's "ts" field (TSD) to get size and iterate elements
        // For now, this is a no-op since it requires complex TSD iteration
        throw std::runtime_error("TsdNonAssociativeReduceNode::update_changes needs TSInput conversion - TODO");
    }

    void TsdNonAssociativeReduceNode::extend_nodes_to(int64_t sz) {
        // TODO: Convert to TSInput-based approach
        // This method extends the chain of reduce nodes
        // It requires:
        // - Accessing input TSB's "zero" and "ts" fields
        // - Iterating TSD elements
        // - Binding inner node inputs to TSD elements
        // - Binding inner node outputs to next node's inputs
        throw std::runtime_error("TsdNonAssociativeReduceNode::extend_nodes_to needs TSInput/TSOutput conversion - TODO");
    }

    void TsdNonAssociativeReduceNode::erase_nodes_from(int64_t ndx) {
        // Remove nodes from index onwards (matches Python lines 416-421)
        nested_graph_->reduce_graph(ndx * node_size());
    }

    void TsdNonAssociativeReduceNode::bind_output() {
        // TODO: Convert to TSOutput-based approach
        // This method binds the output to either zero input or last node's output
        // It requires:
        // - Accessing output as REF type
        // - Accessing input TSB's "zero" field if no nodes
        // - Accessing last node's output if there are nodes
        // For now, this is a no-op
    }

    nb::object TsdNonAssociativeReduceNode::last_output_value() {
        // TODO: Convert to TSInput/TSOutput-based approach
        // For testing/debugging purposes
        throw std::runtime_error("TsdNonAssociativeReduceNode::last_output_value needs conversion - TODO");
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
        return nested_graph_->nodes().size() / node_size();
    }

    std::vector<node_s_ptr> TsdNonAssociativeReduceNode::get_node(int64_t ndx) {
        auto &all_nodes = nested_graph_->nodes();
        int64_t ns = node_size();
        int64_t start = ndx * ns;
        int64_t end = start + ns;
        return {all_nodes.begin() + start, all_nodes.begin() + end};
    }

    std::unordered_map<int, graph_s_ptr> TsdNonAssociativeReduceNode::nested_graphs() {
        return {{0, nested_graph_}};
    }

    void TsdNonAssociativeReduceNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const {
        if (nested_graph_) {
            callback(nested_graph_);
        }
    }

    void register_non_associative_reduce_node_with_nanobind(nb::module_ &m) {
        nb::class_ < TsdNonAssociativeReduceNode, NestedNode > (m, "TsdNonAssociativeReduceNode")
                .def_prop_ro("nested_graphs", &TsdNonAssociativeReduceNode::nested_graphs);
    }
} // namespace hgraph