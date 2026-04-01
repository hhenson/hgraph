#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/v2/graph.h>
#include <hgraph/types/v2/node_builder.h>

#include <vector>

namespace hgraph::v2
{
    struct HGRAPH_EXPORT Edge
    {
        int64_t src_node{-1};
        Path output_path;
        int64_t dst_node{-1};
        Path input_path;
    };

    struct HGRAPH_EXPORT GraphBuilder
    {
        GraphBuilder() = default;

        GraphBuilder &add_node(NodeBuilder node_builder);
        GraphBuilder &add_edge(Edge edge);

        [[nodiscard]] size_t size() const;
        [[nodiscard]] size_t alignment() const;
        [[nodiscard]] Graph make_graph() const;

      private:
        std::vector<NodeBuilder> m_node_builders;
        std::vector<Edge> m_edges;
    };
}  // namespace hgraph::v2
