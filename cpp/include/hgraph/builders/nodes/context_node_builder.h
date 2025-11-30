//
// Created by Howard Henson on 26/12/2024.
//

#ifndef CONTEXT_NODE_BUILDER_H
#define CONTEXT_NODE_BUILDER_H

#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/nodes/context_node.h>

namespace hgraph {
    struct ContextNodeBuilder : BaseNodeBuilder {
        using BaseNodeBuilder::BaseNodeBuilder;

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx, std::shared_ptr<void> buffer = nullptr, size_t* offset = nullptr) const override;
        
        size_t memory_size() const override {
            return _calculate_memory_size(sizeof(ContextStubSourceNode));
        }
    };

    void context_node_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // CONTEXT_NODE_BUILDER_H