#include <hgraph/builders/nodes/last_value_pull_node_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/nodes/last_value_pull_node.h>

namespace hgraph {
    node_ptr LastValuePullNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id,
                                                     int64_t node_ndx) const {
        nb::ref<Node> node{new LastValuePullNode{node_ndx, owning_graph_id, signature, scalars}};
        _build_inputs_and_outputs(node);
        return node;
    }

    void last_value_pull_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<LastValuePullNodeBuilder, BaseNodeBuilder>(m, "LastValuePullNodeBuilder")
                .def("__init__",
                     [](LastValuePullNodeBuilder *self, const nb::kwargs &kwargs) {
                         auto signature_ = nb::cast<node_signature_ptr>(kwargs["signature"]);
                         auto scalars_ = nb::cast<nb::dict>(kwargs["scalars"]);

                         std::optional<input_builder_ptr> input_builder_ =
                                 kwargs.contains("input_builder")
                                     ? nb::cast<std::optional<input_builder_ptr> >(kwargs["input_builder"])
                                     : std::nullopt;
                         std::optional<output_builder_ptr> output_builder_ =
                                 kwargs.contains("output_builder")
                                     ? nb::cast<std::optional<output_builder_ptr> >(kwargs["output_builder"])
                                     : std::nullopt;
                         std::optional<output_builder_ptr> error_builder_ =
                                 kwargs.contains("error_builder")
                                     ? nb::cast<std::optional<output_builder_ptr> >(kwargs["error_builder"])
                                     : std::nullopt;
                         std::optional<output_builder_ptr> recordable_state_builder_ =
                                 kwargs.contains("recordable_state_builder")
                                     ? nb::cast<std::optional<output_builder_ptr> >(kwargs["recordable_state_builder"])
                                     : std::nullopt;

                         new(self) LastValuePullNodeBuilder(std::move(signature_), std::move(scalars_),
                                                            std::move(input_builder_),
                                                            std::move(output_builder_), std::move(error_builder_),
                                                            std::move(recordable_state_builder_));
                     });
    }
} // namespace hgraph