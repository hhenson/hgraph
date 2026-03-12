#include <hgraph/builders/nodes/base_nested_graph_node_builder.h>

namespace hgraph {
    BaseNestedGraphNodeBuilder::BaseNestedGraphNodeBuilder(
        node_signature_s_ptr signature_, nb::dict scalars_, graph_builder_s_ptr nested_graph_builder,
        const std::unordered_map<std::string, int> &input_node_ids, int output_node_id)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_)),
          nested_graph_builder(std::move(nested_graph_builder)), input_node_ids(input_node_ids),
          output_node_id(output_node_id) {
    }

    void base_nested_graph_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_ < BaseNestedGraphNodeBuilder, BaseNodeBuilder > (m, "BaseNestedGraphNodeBuilder")
                .def_ro("nested_graph_builder", &BaseNestedGraphNodeBuilder::nested_graph_builder)
                .def_ro("input_node_ids", &BaseNestedGraphNodeBuilder::input_node_ids)
                .def_ro("output_node_id", &BaseNestedGraphNodeBuilder::output_node_id);
    }
} // namespace hgraph
