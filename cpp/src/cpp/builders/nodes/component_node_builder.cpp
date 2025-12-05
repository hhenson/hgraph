#include <hgraph/builders/nodes/component_node_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/nodes/component_node.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {
    node_s_ptr ComponentNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        auto node = arena_make_shared_as<ComponentNode, Node>(node_ndx, owning_graph_id, signature, scalars, nested_graph_builder, input_node_ids,
                              output_node_id);
        _build_inputs_and_outputs(node.get());
        return node;
    }

    void component_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<ComponentNodeBuilder, BaseNestedGraphNodeBuilder>(m, "ComponentNodeBuilder")
                .def("__init__", [](ComponentNodeBuilder *self, const nb::args &args) {
                    create_nested_graph_node_builder(self, args);
                });
    }
} // namespace hgraph