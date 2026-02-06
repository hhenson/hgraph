#include <hgraph/nodes/non_associative_reduce_node.h>
#include <stdexcept>

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
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::initialise");
    }

    void TsdNonAssociativeReduceNode::do_start() {
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::do_start");
    }

    void TsdNonAssociativeReduceNode::do_stop() {
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::do_stop");
    }

    void TsdNonAssociativeReduceNode::dispose() {
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::dispose");
    }

    void TsdNonAssociativeReduceNode::eval() {
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::eval");
    }

    void TsdNonAssociativeReduceNode::update_changes() {
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::update_changes");
    }

    void TsdNonAssociativeReduceNode::extend_nodes_to(int64_t sz) {
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::extend_nodes_to");
    }

    void TsdNonAssociativeReduceNode::erase_nodes_from(int64_t ndx) {
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::erase_nodes_from");
    }

    void TsdNonAssociativeReduceNode::bind_output() {
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::bind_output");
    }

    nb::object TsdNonAssociativeReduceNode::last_output_value() {
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::last_output_value");
    }

    int64_t TsdNonAssociativeReduceNode::node_size() const {
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::node_size");
    }

    int64_t TsdNonAssociativeReduceNode::node_count() const {
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::node_count");
    }

    std::vector<node_s_ptr> TsdNonAssociativeReduceNode::get_node(int64_t ndx) {
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::get_node");
    }

    std::unordered_map<int, graph_s_ptr> TsdNonAssociativeReduceNode::nested_graphs() {
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::nested_graphs");
    }

    void TsdNonAssociativeReduceNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const {
        throw std::runtime_error("not implemented: TsdNonAssociativeReduceNode::enumerate_nested_graphs");
    }

    void register_non_associative_reduce_node_with_nanobind(nb::module_ &m) {
        nb::class_ < TsdNonAssociativeReduceNode, NestedNode > (m, "TsdNonAssociativeReduceNode")
                .def_prop_ro("nested_graphs", &TsdNonAssociativeReduceNode::nested_graphs);
    }
} // namespace hgraph
