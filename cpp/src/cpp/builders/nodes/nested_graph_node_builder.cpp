#include <hgraph/builders/nodes/nested_graph_node_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/nodes/nest_graph_node.h>

namespace hgraph {
    node_ptr NestedGraphNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id,
                                                   int64_t node_ndx, std::shared_ptr<void> buffer, size_t* offset) const {
        return _make_instance<NestedGraphNode>(buffer, offset, "NestedGraphNode", owning_graph_id, node_ndx,
                                                     nested_graph_builder, input_node_ids, output_node_id);
    }

    void nested_graph_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<NestedGraphNodeBuilder, BaseNestedGraphNodeBuilder>(m, "NestedGraphNodeBuilder")
                .def("__init__", [](NestedGraphNodeBuilder *self, const nb::args &args) {
                    create_nested_graph_node_builder(self, args);
                });
    }
} // namespace hgraph