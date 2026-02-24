#include <hgraph/builders/nodes/context_node_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/nodes/context_node.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {

    size_t ContextNodeBuilder::node_type_size() const {
        return sizeof(ContextStubSourceNode);
    }

    node_s_ptr ContextNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        auto node = arena_make_shared_as<ContextStubSourceNode, Node>(
            node_ndx, owning_graph_id, signature, scalars,
            input_meta(), output_meta(), error_output_meta(), recordable_state_meta());
        configure_node_instance(node);
        return node;
    }

    void context_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<ContextNodeBuilder, BaseNodeBuilder>(m, "ContextNodeBuilder")
                .def("__init__",
                     [](ContextNodeBuilder *self, const nb::kwargs &kwargs) {
                         auto signature_ = nb::cast<node_signature_s_ptr>(kwargs["signature"]);
                         auto scalars_ = nb::cast<nb::dict>(kwargs["scalars"]);

                         new(self) ContextNodeBuilder(std::move(signature_), std::move(scalars_));
                     });
    }
} // namespace hgraph
