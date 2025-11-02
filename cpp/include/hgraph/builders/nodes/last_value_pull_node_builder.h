//
// Created by Howard Henson on 26/12/2024.
//

#ifndef LAST_VALUE_PULL_NODE_BUILDER_H
#define LAST_VALUE_PULL_NODE_BUILDER_H

#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>

namespace hgraph {
    struct LastValuePullNodeBuilder : BaseNodeBuilder {
        using BaseNodeBuilder::BaseNodeBuilder;

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;
    };

    void last_value_pull_node_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // LAST_VALUE_PULL_NODE_BUILDER_H