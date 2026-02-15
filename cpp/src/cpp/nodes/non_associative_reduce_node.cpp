#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/non_associative_reduce_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {
    TsdNonAssociativeReduceNode::TsdNonAssociativeReduceNode(
        int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
        nb::dict scalars, const TSMeta* input_meta, const TSMeta* output_meta,
        const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
        graph_builder_s_ptr nested_graph_builder,
        const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          nested_graph_builder_(std::move(nested_graph_builder)),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id) {}

    void TsdNonAssociativeReduceNode::initialise() {
        nested_graph_ = arena_make_shared<Graph>(std::vector<int64_t>{node_ndx()}, std::vector<node_s_ptr>{}, this, "", &graph()->traits());
        nested_graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(), std::make_shared<NestedEngineEvaluationClock>(graph()->evaluation_engine_clock().get(), this)));
        initialise_component(*nested_graph_);
    }

    void TsdNonAssociativeReduceNode::do_start() {
        if (nested_graph_) {
            start_component(*nested_graph_);
        }
    }

    void TsdNonAssociativeReduceNode::do_stop() {
        if (nested_graph_) {
            stop_component(*nested_graph_);
        }
    }

    void TsdNonAssociativeReduceNode::dispose() {
        if (nested_graph_) {
            dispose_component(*nested_graph_);
            nested_graph_ = nullptr;
        }
    }

    void TsdNonAssociativeReduceNode::eval() {
        mark_evaluated();
        throw std::runtime_error("TsdNonAssociativeReduceNode TS migration pending: legacy TimeSeriesInput/Output path removed");
    }

    void TsdNonAssociativeReduceNode::update_changes() {}

    void TsdNonAssociativeReduceNode::extend_nodes_to(int64_t) {}

    void TsdNonAssociativeReduceNode::erase_nodes_from(int64_t) {}

    void TsdNonAssociativeReduceNode::bind_output() {}

    nb::object TsdNonAssociativeReduceNode::last_output_value() { return nb::none(); }

    int64_t TsdNonAssociativeReduceNode::node_size() const {
        return nested_graph_builder_ ? static_cast<int64_t>(nested_graph_builder_->node_builders.size()) : 0;
    }

    int64_t TsdNonAssociativeReduceNode::node_count() const {
        auto ns = node_size();
        if (nested_graph_ == nullptr || ns <= 0) {
            return 0;
        }
        return static_cast<int64_t>(nested_graph_->nodes().size()) / ns;
    }

    std::vector<node_s_ptr> TsdNonAssociativeReduceNode::get_node(int64_t) { return {}; }

    std::unordered_map<int, graph_s_ptr> TsdNonAssociativeReduceNode::nested_graphs() {
        if (nested_graph_) {
            return {{0, nested_graph_}};
        }
        return {};
    }

    void TsdNonAssociativeReduceNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)> &callback) const {
        if (nested_graph_) {
            callback(nested_graph_);
        }
    }

    void register_non_associative_reduce_node_with_nanobind(nb::module_ &m) {
        nb::class_<TsdNonAssociativeReduceNode, NestedNode>(m, "TsdNonAssociativeReduceNode")
            .def_prop_ro("nested_graphs", &TsdNonAssociativeReduceNode::nested_graphs);
    }
}  // namespace hgraph
