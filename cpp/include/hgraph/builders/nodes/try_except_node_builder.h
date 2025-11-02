//
// Created by Howard Henson on 26/12/2024.
//

#ifndef TRY_EXCEPT_NODE_BUILDER_H
#define TRY_EXCEPT_NODE_BUILDER_H

#include <hgraph/builders/nodes/base_nested_graph_node_builder.h>

namespace hgraph {
    struct TryExceptNodeBuilder : BaseNestedGraphNodeBuilder {
        using BaseNestedGraphNodeBuilder::BaseNestedGraphNodeBuilder;

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;
    };

    void try_except_node_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TRY_EXCEPT_NODE_BUILDER_H