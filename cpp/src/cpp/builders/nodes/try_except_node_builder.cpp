#include <hgraph/builders/nodes/try_except_node_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/nodes/try_except_node.h>

namespace hgraph {
    node_ptr TryExceptNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx, void* buffer, size_t* offset) const {
        node_ptr node;
        if (buffer != nullptr && offset != nullptr) {
            // Arena allocation: construct in-place
            char* buf = static_cast<char*>(buffer);
            // Convert std::shared_ptr<NodeSignature> to nb::ref<NodeSignature>
            NodeSignature::ptr sig_ref = nb::ref<NodeSignature>(this->signature.get());
            size_t node_size = sizeof(TryExceptNode);
            size_t aligned_node_size = align_size(node_size, alignof(size_t));
            // Set canary BEFORE construction
            if (arena_debug_mode) {
                size_t* canary_ptr = reinterpret_cast<size_t*>(buf + *offset + aligned_node_size);
                *canary_ptr = ARENA_CANARY_PATTERN;
            }
            // Now construct the object
            Node* node_ptr_raw = new (buf + *offset) TryExceptNode(node_ndx, owning_graph_id, sig_ref, this->scalars, nested_graph_builder, input_node_ids,
                              output_node_id);
            // Immediately check canary after construction
            verify_canary(node_ptr_raw, sizeof(TryExceptNode), "TryExceptNode");
            *offset += add_canary_size(sizeof(TryExceptNode));
            // Create shared_ptr with no-op deleter (arena manages lifetime)
            node = std::shared_ptr<Node>(node_ptr_raw, [](Node*){ /* no-op, arena manages lifetime */ });
            _build_inputs_and_outputs(node, buffer, offset);
        } else {
            // Heap allocation (legacy path) - use make_shared for proper memory management
            NodeSignature::ptr sig_ref = nb::ref<NodeSignature>(this->signature.get());
            node = std::make_shared<TryExceptNode>(node_ndx, owning_graph_id, sig_ref, this->scalars, nested_graph_builder, input_node_ids,
                              output_node_id);
            _build_inputs_and_outputs(node, nullptr, nullptr);
        }
        return node;
    }

    void try_except_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<TryExceptNodeBuilder, BaseNestedGraphNodeBuilder>(m, "TryExceptNodeBuilder")
                .def("__init__", [](TryExceptNodeBuilder *self, const nb::args &args) {
                    create_nested_graph_node_builder(self, args);
                });
    }
} // namespace hgraph