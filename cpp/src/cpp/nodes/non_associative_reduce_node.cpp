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
#include <hgraph/util/lifecycle.h>

namespace hgraph {
    TsdNonAssociativeReduceNode::TsdNonAssociativeReduceNode(
        int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
        nb::dict scalars, graph_builder_ptr nested_graph_builder,
        const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)),
          nested_graph_builder_(std::move(nested_graph_builder)),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id) {
    }

    void TsdNonAssociativeReduceNode::initialise() {
        // Create nested graph and set up evaluation engine (matches Python lines 319, 329-331)
        nested_graph_ = new Graph(std::vector<int64_t>{node_ndx()}, std::vector<node_ptr>{}, this, "", new Traits());
        nested_graph_->set_evaluation_engine(new NestedEvaluationEngine(
            graph()->evaluation_engine(), new NestedEngineEvaluationClock(graph()->evaluation_engine_clock(), this)));
        initialise_component(*nested_graph_);
    }

    void TsdNonAssociativeReduceNode::do_start() {
        // Process initial TSD if valid (matches Python lines 335-338)
        auto tsd = (*input())["ts"];
        if (tsd->valid()) {
            update_changes();
            notify();
        }

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

        // Update if TSD modified (matches Python lines 349-350)
        auto tsd = (*input())["ts"];
        if (tsd->modified()) {
            update_changes();
        }

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
        // Check if size changed (matches Python lines 362-370)
        int64_t sz = node_count();
        auto tsd = (*input())["ts"];
        auto tsd_input = dynamic_cast<TimeSeriesDictInput_T<int64_t> *>(tsd.get());
        int64_t new_size = tsd_input->size();

        if (sz == new_size) {
            return;
        } else if (sz > new_size) {
            erase_nodes_from(new_size);
        } else {
            extend_nodes_to(new_size);
        }
    }

    void TsdNonAssociativeReduceNode::extend_nodes_to(int64_t sz) {
        // Add nodes to the chain (matches Python lines 389-409)
        int64_t curr_size = node_count();
        auto zero = (*input())["zero"];
        auto zero_ref = dynamic_cast<TimeSeriesReferenceInput *>(zero.get());
        auto tsd = (*input())["ts"];
        auto tsd_input = dynamic_cast<TimeSeriesDictInput_T<int64_t> *>(tsd.get());

        for (int64_t ndx = curr_size; ndx < sz; ndx++) {
            // Extend the nested graph
            nested_graph_->extend_graph(*nested_graph_builder_, true);

            auto new_graph = get_node(ndx);

            // Bind LHS input
            auto lhs_node = new_graph[std::get < 0 > (input_node_ids_)];
            auto lhs_input = dynamic_cast<TimeSeriesReferenceInput *>((*lhs_node->input())[0].get());
            if (ndx == 0) {
                // First node: LHS = zero
                lhs_input->clone_binding(zero_ref);
            } else {
                // Subsequent nodes: LHS = previous node's output
                auto prev_graph = get_node(ndx - 1);
                auto lhs_out = prev_graph[output_node_id_]->output();
                lhs_input->bind_output(lhs_out);
            }

            // Bind RHS input to TSD[ndx]
            auto rhs_node = new_graph[std::get < 1 > (input_node_ids_)];
            auto rhs = (*tsd_input)[ndx];
            auto rhs_ref = dynamic_cast<TimeSeriesReferenceInput *>(rhs.get());
            auto rhs_input = dynamic_cast<TimeSeriesReferenceInput *>((*rhs_node->input())[0].get());
            rhs_input->clone_binding(rhs_ref);

            // Notify the nodes
            lhs_node->notify();
            rhs_node->notify();
        }

        // Start the newly added nodes if graph is already started
        if (nested_graph_->is_started() || nested_graph_->is_starting()) {
            int64_t start_idx = curr_size * node_size();
            int64_t end_idx = nested_graph_->nodes().size();
            nested_graph_->start_subgraph(start_idx, end_idx);
        }
    }

    void TsdNonAssociativeReduceNode::erase_nodes_from(int64_t ndx) {
        // Remove nodes from index onwards (matches Python lines 416-421)
        nested_graph_->reduce_graph(ndx * node_size());
    }

    void TsdNonAssociativeReduceNode::bind_output() {
        // Bind the output to the last node's output value (matches Python lines 411-414)
        auto out = dynamic_cast<TimeSeriesReferenceOutput *>(output().get());

        int64_t nc = node_count();
        if (nc == 0) {
            // No nodes: output should reference the zero input
            auto zero = (*input())["zero"];
            auto zero_ref = dynamic_cast<TimeSeriesReferenceInput *>(zero.get());
            if (!out->valid() || !out->has_value() || !(out->value() == zero_ref->value())) {
                out->set_value(zero_ref->value());
            }
        } else {
            // Has nodes: output should reference the last node's output
            auto sub_graph = get_node(nc - 1);
            auto last_out_node = sub_graph[output_node_id_];
            auto last_out = last_out_node->output();
            auto last_ref_out = dynamic_cast<TimeSeriesReferenceOutput *>(last_out.get());

            if (!out->valid() || !out->has_value() || !(out->value() == last_ref_out->value())) {
                out->set_value(last_ref_out->value());
            }
        }
    }

    nb::object TsdNonAssociativeReduceNode::last_output_value() {
        // This method matches Python's _last_output_value property
        // For testing/debugging purposes
        int64_t nc = node_count();
        if (nc == 0) {
            auto zero = (*input())["zero"];
            return nb::cast(zero.get());
        }

        auto sub_graph = get_node(nc - 1);
        auto out_node = sub_graph[output_node_id_];
        auto ref_out = dynamic_cast<TimeSeriesReferenceOutput *>(out_node->output().get());
        return nb::cast(ref_out->value());
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

    std::vector<node_ptr> TsdNonAssociativeReduceNode::get_node(int64_t ndx) {
        auto &all_nodes = nested_graph_->nodes();
        int64_t ns = node_size();
        int64_t start = ndx * ns;
        int64_t end = start + ns;
        return {all_nodes.begin() + start, all_nodes.begin() + end};
    }

    std::unordered_map<int, graph_ptr> &TsdNonAssociativeReduceNode::nested_graphs() {
        static std::unordered_map<int, graph_ptr> graphs;
        graphs[0] = nested_graph_;
        return graphs;
    }

    void TsdNonAssociativeReduceNode::enumerate_nested_graphs(const std::function<void(graph_ptr)>& callback) const {
        if (nested_graph_) {
            callback(nested_graph_);
        }
    }

    void register_non_associative_reduce_node_with_nanobind(nb::module_ &m) {
        nb::class_ < TsdNonAssociativeReduceNode, NestedNode > (m, "TsdNonAssociativeReduceNode")
                .def_prop_ro("nested_graphs", &TsdNonAssociativeReduceNode::nested_graphs);
    }
} // namespace hgraph