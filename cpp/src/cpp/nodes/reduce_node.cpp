#include <hgraph/nodes/reduce_node.h>
#include <stdexcept>

namespace hgraph {
    ReduceNode::ReduceNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                           nb::dict scalars,
                           const TSMeta* input_meta, const TSMeta* output_meta,
                           const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                           graph_builder_s_ptr nested_graph_builder,
                           const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          nested_graph_builder_(std::move(nested_graph_builder)), input_node_ids_(input_node_ids),
          output_node_id_(output_node_id) {
    }

    std::unordered_map<int, graph_s_ptr> &ReduceNode::nested_graphs() {
        throw std::runtime_error("not implemented: ReduceNode::nested_graphs");
    }

    void ReduceNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const {
        throw std::runtime_error("not implemented: ReduceNode::enumerate_nested_graphs");
    }

    void* ReduceNode::ts() {
        throw std::runtime_error("not implemented: ReduceNode::ts");
    }

    void* ReduceNode::zero() {
        throw std::runtime_error("not implemented: ReduceNode::zero");
    }

    void ReduceNode::initialise() {
        throw std::runtime_error("not implemented: ReduceNode::initialise");
    }

    void ReduceNode::do_start() {
        throw std::runtime_error("not implemented: ReduceNode::do_start");
    }

    void ReduceNode::do_stop() {
        throw std::runtime_error("not implemented: ReduceNode::do_stop");
    }

    void ReduceNode::dispose() {
        throw std::runtime_error("not implemented: ReduceNode::dispose");
    }

    void ReduceNode::eval() {
        throw std::runtime_error("not implemented: ReduceNode::eval");
    }

    void* ReduceNode::last_output() {
        throw std::runtime_error("not implemented: ReduceNode::last_output");
    }

    void ReduceNode::add_nodes_from_views(const std::vector<value::View> &keys) {
        throw std::runtime_error("not implemented: ReduceNode::add_nodes_from_views");
    }

    void ReduceNode::remove_nodes_from_views(const std::vector<value::View> &keys) {
        throw std::runtime_error("not implemented: ReduceNode::remove_nodes_from_views");
    }

    void ReduceNode::re_balance_nodes() {
        throw std::runtime_error("not implemented: ReduceNode::re_balance_nodes");
    }

    void ReduceNode::grow_tree() {
        throw std::runtime_error("not implemented: ReduceNode::grow_tree");
    }

    void ReduceNode::shrink_tree() {
        throw std::runtime_error("not implemented: ReduceNode::shrink_tree");
    }

    void ReduceNode::bind_key_to_node(const value::View &key, const std::tuple<int64_t, int64_t> &ndx) {
        throw std::runtime_error("not implemented: ReduceNode::bind_key_to_node");
    }

    void ReduceNode::zero_node(const std::tuple<int64_t, int64_t> &ndx) {
        throw std::runtime_error("not implemented: ReduceNode::zero_node");
    }

    void ReduceNode::swap_node(const std::tuple<int64_t, int64_t> &src_ndx,
                               const std::tuple<int64_t, int64_t> &dst_ndx) {
        throw std::runtime_error("not implemented: ReduceNode::swap_node");
    }

    int64_t ReduceNode::node_size() const {
        throw std::runtime_error("not implemented: ReduceNode::node_size");
    }

    int64_t ReduceNode::node_count() const {
        throw std::runtime_error("not implemented: ReduceNode::node_count");
    }

    std::vector<node_s_ptr> ReduceNode::get_node(int64_t ndx) {
        throw std::runtime_error("not implemented: ReduceNode::get_node");
    }

    const graph_s_ptr &ReduceNode::nested_graph() const {
        throw std::runtime_error("not implemented: ReduceNode::nested_graph");
    }

    const std::tuple<int64_t, int64_t> &ReduceNode::input_node_ids() const {
        throw std::runtime_error("not implemented: ReduceNode::input_node_ids");
    }

    int64_t ReduceNode::output_node_id() const {
        throw std::runtime_error("not implemented: ReduceNode::output_node_id");
    }

    nb::dict ReduceNode::py_bound_node_indexes() const {
        throw std::runtime_error("not implemented: ReduceNode::py_bound_node_indexes");
    }

    const std::vector<std::tuple<int64_t, int64_t> > &ReduceNode::free_node_indexes() const {
        throw std::runtime_error("not implemented: ReduceNode::free_node_indexes");
    }

    void register_reduce_node_with_nanobind(nb::module_ &m) {
        nb::class_<ReduceNode, NestedNode>(m, "ReduceNode")
                .def_prop_ro("nested_graph", &ReduceNode::nested_graph)
                .def_prop_ro("nested_graphs", &ReduceNode::nested_graphs)
                .def_prop_ro("input_node_ids", &ReduceNode::input_node_ids)
                .def_prop_ro("output_node_id", &ReduceNode::output_node_id)
                .def_prop_ro("bound_node_indexes", &ReduceNode::py_bound_node_indexes)
                .def_prop_ro("free_node_indexes", &ReduceNode::free_node_indexes);
    }
} // namespace hgraph
