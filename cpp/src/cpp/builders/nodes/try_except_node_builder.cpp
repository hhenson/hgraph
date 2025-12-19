#include <hgraph/builders/nodes/try_except_node_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/nodes/try_except_node.h>

namespace hgraph {
    node_s_ptr TryExceptNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        auto node = std::make_shared<TryExceptNode>(
            node_ndx, owning_graph_id, signature, scalars, nested_graph_builder, input_node_ids, output_node_id,
            input_meta(), output_meta(), error_output_meta(), recordable_state_meta());
        return node;
    }

    void try_except_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<TryExceptNodeBuilder, BaseNestedGraphNodeBuilder>(m, "TryExceptNodeBuilder")
                .def("__init__", [](TryExceptNodeBuilder *self, const nb::args &args) {
                    create_nested_graph_node_builder(self, args);
                });
    }
} // namespace hgraph