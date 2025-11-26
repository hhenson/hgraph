#include <hgraph/builders/nodes/tsd_non_associative_reduce_node_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/nodes/non_associative_reduce_node.h>

namespace hgraph {
    template<typename T>
    auto create_reduce_node_builder(T *self, const nb::args &args) {
        // Expected Python signature (positional):
        // (signature, scalars, input_builder, output_builder, error_builder, recordable_state_builder,
        //  nested_graph, input_node_ids, output_node_id)
        if (args.size() != 9) {
            throw nb::type_error("ReduceNodeBuilder expects 9 positional arguments: "
                "(signature, scalars, input_builder, output_builder, error_builder, "
                "recordable_state_builder, nested_graph, input_node_ids, output_node_id)");
        }

        auto signature_ = nb::cast<node_signature_ptr>(args[0]);
        auto scalars_ = nb::cast<nb::dict>(args[1]);
        std::optional<input_builder_ptr> input_builder_ =
                args[2].is_none()
                    ? std::nullopt
                    : std::optional<input_builder_ptr>(nb::cast<input_builder_ptr>(args[2]));
        std::optional<output_builder_ptr> output_builder_ =
                args[3].is_none()
                    ? std::nullopt
                    : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[3]));
        std::optional<output_builder_ptr> error_builder_ =
                args[4].is_none()
                    ? std::nullopt
                    : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[4]));
        std::optional<output_builder_ptr> recordable_state_builder_ =
                args[5].is_none()
                    ? std::nullopt
                    : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[5]));
        auto nested_graph_builder = nb::cast<graph_builder_ptr>(args[6]);
        auto input_node_ids_tuple = nb::cast<std::tuple<int64_t, int64_t> >(args[7]);
        auto output_node_id = nb::cast<int64_t>(args[8]);

        return new(self) T(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                           std::move(output_builder_),
                           std::move(error_builder_), std::move(recordable_state_builder_),
                           std::move(nested_graph_builder),
                           std::move(input_node_ids_tuple), std::move(output_node_id));
    }

    BaseTsdNonAssociativeReduceNodeBuilder::BaseTsdNonAssociativeReduceNodeBuilder(
        node_signature_ptr signature_, nb::dict scalars_, std::optional<input_builder_ptr> input_builder_,
        std::optional<output_builder_ptr> output_builder_, std::optional<output_builder_ptr> error_builder_,
        std::optional<output_builder_ptr> recordable_state_builder_, graph_builder_ptr nested_graph_builder,
        const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                          std::move(output_builder_),
                          std::move(error_builder_), std::move(recordable_state_builder_)),
          nested_graph_builder(std::move(nested_graph_builder)), input_node_ids(input_node_ids),
          output_node_id(output_node_id) {
    }

    node_ptr TsdNonAssociativeReduceNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id,
                                                               int64_t node_ndx, void* buffer, size_t* offset) const {
        node_ptr node;
        if (buffer != nullptr && offset != nullptr) {
            // Arena allocation: construct in-place
            char* buf = static_cast<char*>(buffer);
            // Convert std::shared_ptr<NodeSignature> to nb::ref<NodeSignature>
            NodeSignature::ptr sig_ref = nb::ref<NodeSignature>(this->signature.get());
            size_t node_size = sizeof(TsdNonAssociativeReduceNode);
            size_t aligned_node_size = align_size(node_size, alignof(size_t));
            // Set canary BEFORE construction
            if (arena_debug_mode) {
                size_t* canary_ptr = reinterpret_cast<size_t*>(buf + *offset + aligned_node_size);
                *canary_ptr = ARENA_CANARY_PATTERN;
            }
            // Now construct the object
            Node* node_ptr_raw = new (buf + *offset) TsdNonAssociativeReduceNode(node_ndx, owning_graph_id, sig_ref, this->scalars, nested_graph_builder,
                                            input_node_ids, output_node_id);
            // Immediately check canary after construction
            verify_canary(node_ptr_raw, sizeof(TsdNonAssociativeReduceNode), "TsdNonAssociativeReduceNode");
            *offset += add_canary_size(sizeof(TsdNonAssociativeReduceNode));
            // Create shared_ptr with no-op deleter (arena manages lifetime)
            node = std::shared_ptr<Node>(node_ptr_raw, [](Node*){ /* no-op, arena manages lifetime */ });
            _build_inputs_and_outputs(node, buffer, offset);
        } else {
            // Heap allocation (legacy path) - use make_shared for proper memory management
            NodeSignature::ptr sig_ref = nb::ref<NodeSignature>(this->signature.get());
            node = std::make_shared<TsdNonAssociativeReduceNode>(node_ndx, owning_graph_id, sig_ref, this->scalars, nested_graph_builder,
                                            input_node_ids, output_node_id);
            _build_inputs_and_outputs(node, nullptr, nullptr);
        }
        return node;
    }

    void tsd_non_associative_reduce_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_ < BaseTsdNonAssociativeReduceNodeBuilder, BaseNodeBuilder > (
                    m, "BaseTsdNonAssociativeReduceNodeBuilder")
                .def_ro("nested_graph_builder", &BaseTsdNonAssociativeReduceNodeBuilder::nested_graph_builder)
                .def_ro("input_node_ids", &BaseTsdNonAssociativeReduceNodeBuilder::input_node_ids)
                .def_ro("output_node_id", &BaseTsdNonAssociativeReduceNodeBuilder::output_node_id);

        nb::class_<TsdNonAssociativeReduceNodeBuilder, BaseTsdNonAssociativeReduceNodeBuilder>(
                    m, "TsdNonAssociativeReduceNodeBuilder")
                .def("__init__", [](TsdNonAssociativeReduceNodeBuilder *self, const nb::args &args) {
                    create_reduce_node_builder(self, args);
                });
    }
} // namespace hgraph