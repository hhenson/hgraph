//
// Created by Howard Henson on 26/12/2024.
//

#ifndef GRAPH_BUILDER_H
#define GRAPH_BUILDER_H

#include <hgraph/builders/builder.h>
#include <hgraph/types/graph.h>

namespace hgraph {
    struct Edge {
        int64_t src_node;
        std::vector<int64_t> output_path;
        int64_t dst_node;
        std::vector<int64_t> input_path;

        Edge(int64_t src, std::vector<int64_t> out_path, int64_t dst, std::vector<int64_t> in_path);

        bool operator==(const Edge &other) const;

        bool operator<(const Edge &other) const;
    };
} // namespace hgraph

namespace std {
    template<>
    struct hash<hgraph::Edge> {
        size_t operator()(const hgraph::Edge &edge) const noexcept;
    };
} // namespace std

namespace hgraph {

    struct GraphBuilder : Builder {
        std::vector<node_builder_s_ptr> node_builders;
        std::vector<Edge> edges;
        size_t _memory_size;  // Cached memory size, calculated in constructor

        GraphBuilder(std::vector<node_builder_s_ptr> node_builders, std::vector<Edge> edges);

        /**
         * Construct an instance of a graph. The id provided is the id for the graph instance to be constructed.
         */
        graph_s_ptr make_instance(const std::vector<int64_t> &graph_id, node_ptr parent_node = nullptr,
                                  const std::string &label = "") const;

        /**
         * Make the nodes described in the node builders and connect the edges as described in the edges.
         * Return the iterable of newly constructed and wired nodes.
         * This can be used to feed into a new graph instance or to extend (or re-initialise) an existing graph.
         */
        Graph::node_list make_and_connect_nodes(const std::vector<int64_t> &graph_id,
                                                int64_t first_node_ndx) const;

        /**
         * Release resources constructed during the build process, plus the graph.
         */
        void release_instance(graph_s_ptr item) const;

        [[nodiscard]] size_t memory_size() const override;

        static void register_with_nanobind(nb::module_ &m);
    };
}; // namespace hgraph
#endif  // GRAPH_BUILDER_H
