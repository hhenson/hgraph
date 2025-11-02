#include <hgraph/builders/nodes/python_generator_node_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/nodes/python_generator_node.h>

namespace hgraph {
    PythonGeneratorNodeBuilder::PythonGeneratorNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                                                           std::optional<input_builder_ptr> input_builder_,
                                                           std::optional<output_builder_ptr> output_builder_,
                                                           std::optional<output_builder_ptr> error_builder_,
                                                           nb::callable eval_fn)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                          std::move(output_builder_),
                          std::move(error_builder_), std::nullopt),
          eval_fn{std::move(eval_fn)} {
    }

    node_ptr PythonGeneratorNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id,
                                                       int64_t node_ndx) const {
        nb::ref<Node> node{new PythonGeneratorNode{node_ndx, owning_graph_id, signature, scalars, eval_fn, {}, {}}};
        _build_inputs_and_outputs(node);
        return node;
    }

    void python_generator_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_ < PythonGeneratorNodeBuilder, BaseNodeBuilder > (m, "PythonGeneratorNodeBuilder")
                .def("__init__",
                     [](PythonGeneratorNodeBuilder *self, const nb::kwargs &kwargs) {
                         auto signature_obj = kwargs["signature"];
                         auto signature_ = nb::cast<node_signature_ptr>(signature_obj);
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
                         auto eval_fn = nb::cast<nb::callable>(kwargs["eval_fn"]);
                         new(self)
                                 PythonGeneratorNodeBuilder(std::move(signature_), std::move(scalars_),
                                                            std::move(input_builder_),
                                                            std::move(output_builder_), std::move(error_builder_),
                                                            std::move(eval_fn));
                     })
                .def_ro("eval_fn", &PythonGeneratorNodeBuilder::eval_fn);
    }
} // namespace hgraph