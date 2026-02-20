#include <hgraph/builders/nodes/nested_graph_node_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/nodes/nest_graph_node.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {
    size_t NestedGraphNodeBuilder::node_type_size() const {
        return sizeof(NestedGraphNode);
    }

    node_s_ptr NestedGraphNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id,
                                                   int64_t node_ndx) const {
        auto node = arena_make_shared_as<NestedGraphNode, Node>(
            node_ndx, owning_graph_id, signature, scalars,
            input_meta(), output_meta(), error_output_meta(), recordable_state_meta(),
            nested_graph_builder, input_node_ids, output_node_id);
        configure_node_instance(node);
        return node;
    }

    void nested_graph_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<NestedGraphNodeBuilder, BaseNestedGraphNodeBuilder>(m, "NestedGraphNodeBuilder")
                .def("__init__", [](NestedGraphNodeBuilder *self, const nb::args &args) {
                    create_nested_graph_node_builder(self, args);
                });
    }
} // namespace hgraph
