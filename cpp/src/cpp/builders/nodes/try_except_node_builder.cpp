#include <hgraph/builders/nodes/try_except_node_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/nodes/try_except_node.h>

namespace hgraph {
    node_ptr TryExceptNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        nb::ref<Node> node{
            new TryExceptNode(node_ndx, owning_graph_id, signature, scalars, nested_graph_builder, input_node_ids,
                              output_node_id)
        };
        _build_inputs_and_outputs(node);
        return node;
    }

    void try_except_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<TryExceptNodeBuilder, BaseNestedGraphNodeBuilder>(m, "TryExceptNodeBuilder")
                .def("__init__", [](TryExceptNodeBuilder *self, const nb::args &args) {
                    create_nested_graph_node_builder(self, args);
                });
    }
} // namespace hgraph