#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/reduce_node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <hgraph/util/lifecycle.h>
#include <hgraph/util/string_utils.h>

#include <algorithm>
#include <deque>
#include <utility>

namespace hgraph {

    template<typename K>
    ReduceNode<K>::ReduceNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                              nb::dict scalars, graph_builder_s_ptr nested_graph_builder,
                              const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id,
                              const TSMeta* input_meta, const TSMeta* output_meta,
                              const TSMeta* error_output_meta, const TSMeta* recordable_state_meta)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          nested_graph_builder_(std::move(nested_graph_builder)), input_node_ids_(input_node_ids),
          output_node_id_(output_node_id) {
    }

    template<typename K>
    std::unordered_map<int, graph_s_ptr> &ReduceNode<K>::nested_graphs() {
        static std::unordered_map<int, graph_s_ptr> graphs;
        graphs[0] = nested_graph_;
        return graphs;
    }

    template<typename K>
    void ReduceNode<K>::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const {
        if (nested_graph_) {
            callback(nested_graph_);
        }
    }

    template<typename K>
    const graph_s_ptr &ReduceNode<K>::nested_graph() const { return nested_graph_; }

    template<typename K>
    const std::tuple<int64_t, int64_t> &ReduceNode<K>::input_node_ids() const { return input_node_ids_; }

    template<typename K>
    int64_t ReduceNode<K>::output_node_id() const { return output_node_id_; }

    template<typename K>
    const std::unordered_map<K, std::tuple<int64_t, int64_t> > &ReduceNode<K>::bound_node_indexes() const {
        return bound_node_indexes_;
    }

    template<typename K>
    const std::vector<std::tuple<int64_t, int64_t> > &ReduceNode<K>::free_node_indexes() const {
        return free_node_indexes_;
    }

    // TODO: Implement ReduceNode

    template<typename K>
    ts::TSInput* ReduceNode<K>::ts() {
        throw std::runtime_error("ReduceNode::ts not yet implemented");
    }

    template<typename K>
    ts::TSInput* ReduceNode<K>::zero() {
        throw std::runtime_error("ReduceNode::zero not yet implemented");
    }

    template<typename K>
    void ReduceNode<K>::initialise() {
        throw std::runtime_error("ReduceNode::initialise not yet implemented");
    }

    template<typename K>
    void ReduceNode<K>::do_start() {
        throw std::runtime_error("ReduceNode::do_start not yet implemented");
    }

    template<typename K>
    void ReduceNode<K>::do_stop() {
        if (nested_graph_) { stop_component(*nested_graph_); }
    }

    template<typename K>
    void ReduceNode<K>::dispose() {
        if (nested_graph_) {
            dispose_component(*nested_graph_);
            nested_graph_ = nullptr;
        }
    }

    template<typename K>
    void ReduceNode<K>::eval() {
        throw std::runtime_error("ReduceNode::eval not yet implemented");
    }

    template<typename K>
    time_series_output_s_ptr ReduceNode<K>::last_output() {
        throw std::runtime_error("ReduceNode::last_output not yet implemented");
    }

    template<typename K>
    void ReduceNode<K>::add_nodes(const std::unordered_set<K> &) {
        throw std::runtime_error("ReduceNode::add_nodes not yet implemented");
    }

    template<typename K>
    void ReduceNode<K>::remove_nodes(const std::unordered_set<K> &) {
        throw std::runtime_error("ReduceNode::remove_nodes not yet implemented");
    }

    template<typename K>
    void ReduceNode<K>::swap_node(const std::tuple<int64_t, int64_t> &, const std::tuple<int64_t, int64_t> &) {
        throw std::runtime_error("ReduceNode::swap_node not yet implemented");
    }

    template<typename K>
    void ReduceNode<K>::re_balance_nodes() {}

    template<typename K>
    void ReduceNode<K>::grow_tree() {
        throw std::runtime_error("ReduceNode::grow_tree not yet implemented");
    }

    template<typename K>
    void ReduceNode<K>::shrink_tree() {}

    template<typename K>
    void ReduceNode<K>::bind_key_to_node(const K &, const std::tuple<int64_t, int64_t> &) {
        throw std::runtime_error("ReduceNode::bind_key_to_node not yet implemented");
    }

    template<typename K>
    void ReduceNode<K>::zero_node(const std::tuple<int64_t, int64_t> &) {
        throw std::runtime_error("ReduceNode::zero_node not yet implemented");
    }

    template<typename K>
    int64_t ReduceNode<K>::node_size() const { return nested_graph_builder_->node_builders.size(); }

    template<typename K>
    int64_t ReduceNode<K>::node_count() const {
        return nested_graph_ ? nested_graph_->nodes().size() / node_size() : 0;
    }

    template<typename K>
    std::vector<node_s_ptr> ReduceNode<K>::get_node(int64_t) {
        throw std::runtime_error("ReduceNode::get_node not yet implemented");
    }

    // Explicit template instantiations for supported key types
    template struct ReduceNode<bool>;
    template struct ReduceNode<int64_t>;
    template struct ReduceNode<double>;
    template struct ReduceNode<engine_date_t>;
    template struct ReduceNode<engine_time_t>;
    template struct ReduceNode<engine_time_delta_t>;
    template struct ReduceNode<nb::object>;

    // Template function to register ReduceNode<K> with nanobind
    template<typename K>
    void register_reduce_node_type(nb::module_ &m, const char *class_name) {
        nb::class_<ReduceNode<K>, NestedNode>(m, class_name)
                .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::s_ptr, nb::dict, graph_builder_s_ptr,
                         const std::tuple<int64_t, int64_t> &, int64_t>(),
                     "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a,
                     "input_node_ids"_a,
                     "output_node_id"_a)
                .def_prop_ro("nested_graph", &ReduceNode<K>::nested_graph)
                .def_prop_ro("nested_graphs", &ReduceNode<K>::nested_graphs)
                .def_prop_ro("ts", &ReduceNode<K>::ts)
                .def_prop_ro("zero", &ReduceNode<K>::zero)
                .def_prop_ro("input_node_ids", &ReduceNode<K>::input_node_ids)
                .def_prop_ro("output_node_id", &ReduceNode<K>::output_node_id)
                .def_prop_ro("bound_node_indexes", &ReduceNode<K>::bound_node_indexes)
                .def_prop_ro("free_node_indexes", &ReduceNode<K>::free_node_indexes);
    }

    void register_reduce_node_with_nanobind(nb::module_ &m) {
        register_reduce_node_type<bool>(m, "ReduceNode_bool");
        register_reduce_node_type<int64_t>(m, "ReduceNode_int");
        register_reduce_node_type<double>(m, "ReduceNode_float");
        register_reduce_node_type<engine_date_t>(m, "ReduceNode_date");
        register_reduce_node_type<engine_time_t>(m, "ReduceNode_datetime");
        register_reduce_node_type<engine_time_delta_t>(m, "ReduceNode_timedelta");
        register_reduce_node_type<nb::object>(m, "ReduceNode_object");
    }
} // namespace hgraph