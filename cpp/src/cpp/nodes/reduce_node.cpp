#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/reduce_node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ref.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {
    ReduceNode::ReduceNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                           nb::dict scalars, const TSMeta* input_meta, const TSMeta* output_meta,
                           const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                           graph_builder_s_ptr nested_graph_builder,
                           const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          nested_graph_builder_(std::move(nested_graph_builder)), input_node_ids_(input_node_ids),
          output_node_id_(output_node_id) {}

    std::unordered_map<int, graph_s_ptr> &ReduceNode::nested_graphs() {
        static std::unordered_map<int, graph_s_ptr> graphs;
        graphs.clear();
        if (nested_graph_) {
            graphs.emplace(0, nested_graph_);
        }
        return graphs;
    }

    void ReduceNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)> &callback) const {
        if (nested_graph_) {
            callback(nested_graph_);
        }
    }

    TimeSeriesDictInputImpl::ptr ReduceNode::ts() { return nullptr; }

    time_series_reference_input_ptr ReduceNode::zero() { return nullptr; }

    void ReduceNode::initialise() {
        nested_graph_ = arena_make_shared<Graph>(std::vector<int64_t>{node_ndx()}, std::vector<node_s_ptr>{}, this, "", &graph()->traits());
        nested_graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(), std::make_shared<NestedEngineEvaluationClock>(graph()->evaluation_engine_clock().get(), this)));
        initialise_component(*nested_graph_);
    }

    void ReduceNode::do_start() {
        if (nested_graph_) {
            start_component(*nested_graph_);
        }
    }

    void ReduceNode::do_stop() {
        if (nested_graph_) {
            stop_component(*nested_graph_);
        }
    }

    void ReduceNode::dispose() {
        if (nested_graph_ == nullptr) {
            return;
        }
        dispose_component(*nested_graph_);
        nested_graph_ = nullptr;
    }

    void ReduceNode::eval() {
        mark_evaluated();
        throw std::runtime_error("ReduceNode TS migration pending: legacy TimeSeriesInput/Output path removed");
    }

    TimeSeriesOutput::s_ptr ReduceNode::last_output() { return nullptr; }

    void ReduceNode::add_nodes_from_views(const std::vector<value::View> &) {}

    void ReduceNode::remove_nodes_from_views(const std::vector<value::View> &) {}

    void ReduceNode::re_balance_nodes() {}

    void ReduceNode::grow_tree() {}

    void ReduceNode::shrink_tree() {}

    void ReduceNode::bind_key_to_node(const value::View &, const std::tuple<int64_t, int64_t> &) {}

    void ReduceNode::zero_node(const std::tuple<int64_t, int64_t> &) {}

    void ReduceNode::swap_node(const std::tuple<int64_t, int64_t> &, const std::tuple<int64_t, int64_t> &) {}

    int64_t ReduceNode::node_size() const {
        return nested_graph_builder_ ? static_cast<int64_t>(nested_graph_builder_->node_builders.size()) : 0;
    }

    int64_t ReduceNode::node_count() const {
        auto ns = node_size();
        if (nested_graph_ == nullptr || ns <= 0) {
            return 0;
        }
        return static_cast<int64_t>(nested_graph_->nodes().size()) / ns;
    }

    std::vector<node_s_ptr> ReduceNode::get_node(int64_t) { return {}; }

    const graph_s_ptr &ReduceNode::nested_graph() const { return nested_graph_; }

    const std::tuple<int64_t, int64_t> &ReduceNode::input_node_ids() const { return input_node_ids_; }

    int64_t ReduceNode::output_node_id() const { return output_node_id_; }

    nb::dict ReduceNode::py_bound_node_indexes() const { return {}; }

    const std::vector<std::tuple<int64_t, int64_t>> &ReduceNode::free_node_indexes() const {
        return free_node_indexes_;
    }

    void register_reduce_node_with_nanobind(nb::module_ &m) {
        nb::class_<ReduceNode, NestedNode>(m, "ReduceNode")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::s_ptr, nb::dict,
                          const TSMeta*, const TSMeta*, const TSMeta*, const TSMeta*,
                          graph_builder_s_ptr, const std::tuple<int64_t, int64_t> &, int64_t>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a,
                 "input_meta"_a, "output_meta"_a, "error_output_meta"_a, "recordable_state_meta"_a,
                 "nested_graph_builder"_a, "input_node_ids"_a, "output_node_id"_a)
            .def_prop_ro("nested_graph", &ReduceNode::nested_graph)
            .def_prop_ro("nested_graphs", &ReduceNode::nested_graphs)
            .def_prop_ro("ts", &ReduceNode::ts)
            .def_prop_ro("zero", &ReduceNode::zero)
            .def_prop_ro("input_node_ids", &ReduceNode::input_node_ids)
            .def_prop_ro("output_node_id", &ReduceNode::output_node_id)
            .def_prop_ro("bound_node_indexes", &ReduceNode::py_bound_node_indexes)
            .def_prop_ro("free_node_indexes", &ReduceNode::free_node_indexes);
    }
}  // namespace hgraph
