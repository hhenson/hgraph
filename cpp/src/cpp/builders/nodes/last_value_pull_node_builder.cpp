#include <hgraph/builders/nodes/last_value_pull_node_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {

    size_t LastValuePullNodeBuilder::node_type_size() const {
        return sizeof(LastValuePullNode);
    }

    node_s_ptr LastValuePullNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id,
                                                     int64_t node_ndx) const {
        auto node = arena_make_shared_as<LastValuePullNode, Node>(
            node_ndx, owning_graph_id, signature, scalars,
            input_meta(), output_meta(), error_output_meta(), recordable_state_meta());
        configure_node_instance(node);
        return node;
    }

    void last_value_pull_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<LastValuePullNodeBuilder, BaseNodeBuilder>(m, "LastValuePullNodeBuilder")
                .def("__init__",
                     [](LastValuePullNodeBuilder *self, const nb::kwargs &kwargs) {
                         auto signature_ = nb::cast<node_signature_s_ptr>(kwargs["signature"]);
                         auto scalars_ = nb::cast<nb::dict>(kwargs["scalars"]);

                         auto require_none = [&](const char *name) {
                             if (kwargs.contains(name) && !kwargs[name].is_none()) {
                                 throw nb::type_error(
                                     "Legacy input/output/error/recordable builders are not supported in C++ runtime node builders");
                             }
                         };
                         require_none("input_builder");
                         require_none("output_builder");
                         require_none("error_builder");
                         require_none("recordable_state_builder");

                         new(self) LastValuePullNodeBuilder(std::move(signature_), std::move(scalars_));
                     });
    }
} // namespace hgraph
