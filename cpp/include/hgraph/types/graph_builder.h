#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node_builder.h>

#include <vector>

namespace hgraph
{
    /** A directed binding from one node output path to one node input path. */
    struct HGRAPH_EXPORT Edge
    {
        int64_t src_node{-1};
        Path output_path;
        int64_t dst_node{-1};
        Path input_path;

        bool operator==(const Edge &other) const = default;
        [[nodiscard]] bool operator<(const Edge &other) const noexcept;
    };

    /**
     * Fluent builder for a graph-owned node slab.
     *
     * GraphBuilder groups inbound edges per destination node, asks each
     * concrete builder for the size and alignment of its runtime chunk,
     * allocates a single contiguous block, constructs all nodes into that
     * block, and then binds edges through TSOutputView / TSInputView
     * traversal.
     */
    struct HGRAPH_EXPORT GraphBuilder
    {
        GraphBuilder() = default;
        GraphBuilder(std::vector<NodeBuilder> node_builders, std::vector<Edge> edges);

        GraphBuilder &add_node(NodeBuilder node_builder);
        GraphBuilder &add_edge(Edge edge);

        [[nodiscard]] size_t size() const;
        [[nodiscard]] size_t alignment() const;
        [[nodiscard]] size_t memory_size() const;
        [[nodiscard]] Graph make_graph(GraphEvaluationEngine evaluation_engine) const;

        [[nodiscard]] size_t node_builder_count() const noexcept { return m_node_builders.size(); }
        [[nodiscard]] const NodeBuilder &node_builder_at(size_t index) const;
        [[nodiscard]] const std::vector<Edge> &edges() const noexcept { return m_edges; }

      private:
        std::vector<NodeBuilder> m_node_builders;
        std::vector<Edge> m_edges;
    };
}  // namespace hgraph
