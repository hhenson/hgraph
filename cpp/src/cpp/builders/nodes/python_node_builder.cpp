#include <hgraph/builders/nodes/python_node_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/nodes/python_node.h>
#include <hgraph/nodes/push_queue_node.h>

namespace hgraph {
    PythonNodeBuilder::PythonNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                                         std::optional<input_builder_ptr> input_builder_,
                                         std::optional<output_builder_ptr> output_builder_,
                                         std::optional<output_builder_ptr> error_builder_,
                                         std::optional<output_builder_ptr> recordable_state_builder_,
                                         nb::callable eval_fn,
                                         nb::callable start_fn, nb::callable stop_fn)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                          std::move(output_builder_),
                          std::move(error_builder_), std::move(recordable_state_builder_)),
          eval_fn{std::move(eval_fn)}, start_fn{std::move(start_fn)}, stop_fn{std::move(stop_fn)} {
    }

    node_ptr PythonNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx,
                                               void* buffer, size_t* offset) const {
        // Copy eval_fn if it's not a plain function (e.g., KeyStubEvalFn instance)
        // This matches Python: eval_fn=self.eval_fn if isfunction(self.eval_fn) else copy(self.eval_fn)
        nb::callable eval_fn_to_use = eval_fn;

        // Check if it's a plain Python function using inspect.isfunction
        auto inspect_module = nb::module_::import_("inspect");
        bool is_function = nb::cast<bool>(inspect_module.attr("isfunction")(eval_fn));

        if (!is_function) {
            // Use Python's copy module to create a shallow copy for non-function callables
            auto copy_module = nb::module_::import_("copy");
            eval_fn_to_use = nb::cast<nb::callable>(copy_module.attr("copy")(eval_fn));
        }

        node_ptr node;
        
        if (buffer != nullptr && offset != nullptr) {
            // Arena allocation: construct in-place
            char* buf = static_cast<char*>(buffer);
            
            // Construct Node in-place
            Node* node_ptr_raw = nullptr;
            if (signature->is_push_source_node()) {
                size_t node_size = sizeof(PushQueueNode);
                size_t aligned_node_size = align_size(node_size, alignof(size_t));
                // Set canary BEFORE construction
                if (arena_debug_mode) {
                    size_t* canary_ptr = reinterpret_cast<size_t*>(buf + *offset + aligned_node_size);
                    *canary_ptr = ARENA_CANARY_PATTERN;
                }
                // Now construct the object
                node_ptr_raw = new (buf + *offset) PushQueueNode{node_ndx, owning_graph_id, signature, scalars};
                // Immediately check canary after construction
                verify_canary(node_ptr_raw, sizeof(PushQueueNode), "PushQueueNode");
                *offset += add_canary_size(sizeof(PushQueueNode));
                // Provide the eval function so the node can expose a sender in start()
                dynamic_cast<PushQueueNode &>(*node_ptr_raw).set_eval_fn(eval_fn_to_use);
            } else {
                size_t node_size = sizeof(PythonNode);
                size_t aligned_node_size = align_size(node_size, alignof(size_t));
                // Set canary BEFORE construction
                if (arena_debug_mode) {
                    size_t* canary_ptr = reinterpret_cast<size_t*>(buf + *offset + aligned_node_size);
                    *canary_ptr = ARENA_CANARY_PATTERN;
                }
                // Now construct the object
                node_ptr_raw = new (buf + *offset) PythonNode{node_ndx, owning_graph_id, signature, scalars, eval_fn_to_use, start_fn, stop_fn};
                // Immediately check canary after construction
                verify_canary(node_ptr_raw, sizeof(PythonNode), "PythonNode");
                *offset += add_canary_size(sizeof(PythonNode));
            }
            
            // Get shared_ptr using shared_from_this (Node needs graph to be set first for this to work)
            // Actually, we need to set the graph first, then we can use shared_from_this
            // But we don't have the graph yet at this point. We'll need to return a raw pointer
            // and create the shared_ptr later, or we need to pass the graph control block.
            // For now, let's create an aliasing shared_ptr using the graph's control block
            // But we don't have the graph yet. Let's use a different approach:
            // We'll need to set the graph on the node first, then use shared_from_this
            // Actually, the graph will be set later in GraphBuilder, so we can't use shared_from_this yet.
            // We need to pass the graph's control block or create the shared_ptr later.
            // For now, let's create a temporary shared_ptr that will be replaced when graph is set
            // Actually, let's just return a shared_ptr created with a no-op deleter for now
            // The actual lifetime is managed by the arena
            node = std::shared_ptr<Node>(node_ptr_raw, [](Node*){ /* no-op, arena manages lifetime */ });
            
            // Build inputs and outputs in-place
            _build_inputs_and_outputs(node, buffer, offset);
        } else {
            // Heap allocation (legacy path)
            if (signature->is_push_source_node()) {
                nb::ref<Node> node_ref{new PushQueueNode{node_ndx, owning_graph_id, signature, scalars}};
                _build_inputs_and_outputs(node_ref);
                // Provide the eval function so the node can expose a sender in start()
                dynamic_cast<PushQueueNode &>(*node_ref).set_eval_fn(eval_fn_to_use);
                node = node_ref;
            } else {
                nb::ref<Node> node_ref{
                    new PythonNode{node_ndx, owning_graph_id, signature, scalars, eval_fn_to_use, start_fn, stop_fn}
                };
                _build_inputs_and_outputs(node_ref);
                node = node_ref;
            }
        }
        
        return node;
    }

    void python_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_ < PythonNodeBuilder, BaseNodeBuilder > (m, "PythonNodeBuilder")
                .def("__init__",
                     [](PythonNodeBuilder *self, const nb::kwargs &kwargs) {
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
                         nb::handle eval_fn_ = kwargs.contains("eval_fn")
                                                   ? nb::cast<nb::handle>(kwargs["eval_fn"])
                                                   : nb::handle{};
                         nb::handle start_fn_ = kwargs.contains("start_fn")
                                                    ? nb::cast<nb::handle>(kwargs["start_fn"])
                                                    : nb::handle{};
                         nb::handle stop_fn_ = kwargs.contains("stop_fn")
                                                   ? nb::cast<nb::handle>(kwargs["stop_fn"])
                                                   : nb::handle{};

                         nb::callable eval_fn =
                                 eval_fn_.is_valid() && !eval_fn_.is_none()
                                     ? nb::cast<nb::callable>(eval_fn_)
                                     : nb::callable{};
                         nb::callable start_fn =
                                 start_fn_.is_valid() && !start_fn_.is_none()
                                     ? nb::cast<nb::callable>(start_fn_)
                                     : nb::callable{};
                         nb::callable stop_fn =
                                 stop_fn_.is_valid() && !stop_fn_.is_none()
                                     ? nb::cast<nb::callable>(stop_fn_)
                                     : nb::callable{};

                         new(self) PythonNodeBuilder(std::move(signature_), std::move(scalars_),
                                                     std::move(input_builder_),
                                                     std::move(output_builder_), std::move(error_builder_),
                                                     std::move(recordable_state_builder_), std::move(eval_fn),
                                                     std::move(start_fn),
                                                     std::move(stop_fn));
                     })
                .def_ro("eval_fn", &PythonNodeBuilder::eval_fn)
                .def_ro("start_fn", &PythonNodeBuilder::start_fn)
                .def_ro("stop_fn", &PythonNodeBuilder::stop_fn);
    }
} // namespace hgraph