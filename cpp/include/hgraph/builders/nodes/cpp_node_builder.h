#pragma once

#include <hgraph/builders/node_builder.h>
#include <hgraph/nodes/cpp_node_spec.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {
    /**
     * Generic builder for Spec-backed compute nodes.
     */
    template<typename Spec>
    struct CppNodeBuilder final : BaseNodeBuilder {
        using BaseNodeBuilder::BaseNodeBuilder;

        node_s_ptr make_instance(const std::vector<int64_t>& owning_graph_id, int64_t node_ndx) const override {
            auto node = arena_make_shared_as<NodeImpl<Spec>, Node>(
                node_ndx,
                owning_graph_id,
                signature,
                scalars,
                input_meta(),
                output_meta(),
                error_output_meta(),
                recordable_state_meta());
            configure_node_instance(node);
            return node;
        }

        [[nodiscard]] size_t node_type_size() const override {
            return sizeof(NodeImpl<Spec>);
        }
    };
}  // namespace hgraph

