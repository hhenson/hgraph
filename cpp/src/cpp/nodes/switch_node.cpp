#include <hgraph/nodes/switch_node.h>
#include <stdexcept>

namespace hgraph {

    SwitchNode::SwitchNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                           nb::dict scalars,
                           const TSMeta* input_meta, const TSMeta* output_meta,
                           const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                           const value::TypeMeta* key_type,
                           graph_builders_map_ptr nested_graph_builders,
                           input_node_ids_map_ptr input_node_ids,
                           output_node_ids_map_ptr output_node_ids,
                           bool reload_on_ticked,
                           graph_builder_s_ptr default_graph_builder,
                           std::unordered_map<std::string, int> default_input_node_ids,
                           int default_output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          _key_type(key_type),
          _nested_graph_builders(std::move(nested_graph_builders)),
          _input_node_ids(std::move(input_node_ids)),
          _output_node_ids(std::move(output_node_ids)),
          _reload_on_ticked(reload_on_ticked),
          _default_graph_builder(std::move(default_graph_builder)),
          _default_input_node_ids(std::move(default_input_node_ids)),
          _default_output_node_id(default_output_node_id) {
    }

    void SwitchNode::initialise() {
        throw std::runtime_error("not implemented: SwitchNode::initialise");
    }

    void SwitchNode::do_start() {
        throw std::runtime_error("not implemented: SwitchNode::do_start");
    }

    void SwitchNode::do_stop() {
        throw std::runtime_error("not implemented: SwitchNode::do_stop");
    }

    void SwitchNode::dispose() {
        throw std::runtime_error("not implemented: SwitchNode::dispose");
    }

    bool SwitchNode::keys_equal(const value::View& a, const value::View& b) const {
        throw std::runtime_error("not implemented: SwitchNode::keys_equal");
    }

    void SwitchNode::eval() {
        throw std::runtime_error("not implemented: SwitchNode::eval");
    }

    void SwitchNode::wire_graph(graph_s_ptr &graph) {
        throw std::runtime_error("not implemented: SwitchNode::wire_graph");
    }

    void SwitchNode::unwire_graph(graph_s_ptr &graph) {
        throw std::runtime_error("not implemented: SwitchNode::unwire_graph");
    }

    std::unordered_map<int, graph_s_ptr> SwitchNode::nested_graphs() const {
        throw std::runtime_error("not implemented: SwitchNode::nested_graphs");
    }

    void SwitchNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const {
        throw std::runtime_error("not implemented: SwitchNode::enumerate_nested_graphs");
    }

    void register_switch_node_with_nanobind(nb::module_ &m) {
        nb::class_<SwitchNode, NestedNode>(m, "SwitchNode")
            .def_prop_ro("nested_graphs", &SwitchNode::nested_graphs);
    }
} // namespace hgraph
