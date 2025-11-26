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
                                                       int64_t node_ndx, void* buffer, size_t* offset) const {
        node_ptr node;
        if (buffer != nullptr && offset != nullptr) {
            // Arena allocation: construct in-place
            char* buf = static_cast<char*>(buffer);
            // Convert std::shared_ptr<NodeSignature> to nb::ref<NodeSignature>
            NodeSignature::ptr sig_ref = nb::ref<NodeSignature>(this->signature.get());
            size_t node_size = sizeof(PythonGeneratorNode);
            size_t aligned_node_size = align_size(node_size, alignof(size_t));
            // Set canary BEFORE construction
            if (arena_debug_mode) {
                size_t* canary_ptr = reinterpret_cast<size_t*>(buf + *offset + aligned_node_size);
                *canary_ptr = ARENA_CANARY_PATTERN;
            }
            // Now construct the object
            // Use default-constructed callables for start_fn and stop_fn (generator nodes don't use them)
            nb::callable empty_start;
            nb::callable empty_stop;
            Node* node_ptr_raw = new (buf + *offset) PythonGeneratorNode(node_ndx, owning_graph_id, sig_ref, this->scalars, eval_fn, empty_start, empty_stop);
            // Immediately check canary after construction
            verify_canary(node_ptr_raw, sizeof(PythonGeneratorNode), "PythonGeneratorNode");
            *offset += add_canary_size(sizeof(PythonGeneratorNode));
            // Create shared_ptr with no-op deleter (arena manages lifetime)
            node = std::shared_ptr<Node>(node_ptr_raw, [](Node*){ /* no-op, arena manages lifetime */ });
            _build_inputs_and_outputs(node, buffer, offset);
        } else {
            // Heap allocation (legacy path) - use make_shared for proper memory management
            // PythonGeneratorNode uses BasePythonNode constructor which takes eval_fn, start_fn, stop_fn
            NodeSignature::ptr sig_ref = nb::ref<NodeSignature>(this->signature.get());
            // Use default-constructed callables for start_fn and stop_fn (generator nodes don't use them)
            nb::callable empty_start;
            nb::callable empty_stop;
            node = std::make_shared<PythonGeneratorNode>(node_ndx, owning_graph_id, sig_ref, this->scalars, eval_fn, empty_start, empty_stop);
            _build_inputs_and_outputs(node, nullptr, nullptr);
        }
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