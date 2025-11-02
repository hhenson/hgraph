#include "hgraph/types/tsd.h"
#include <hgraph/builders/graph_builder.h>
#include <hgraph/builders/node_builder.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/ts_signal.h>
#include <hgraph/types/tsb.h>

namespace hgraph {
    constexpr int64_t ERROR_PATH = -1; // The path in the wiring edges representing the error output of the node
    constexpr int64_t STATE_PATH = -2;
    // The path in the wiring edges representing the recordable state output of the node
    constexpr int64_t KEY_SET = -3; // The path in the wiring edges representing the recordable state output of the node

    time_series_output_ptr _extract_output(node_ptr node, const std::vector<int64_t> &path) {
        if (path.empty()) { throw std::runtime_error("No path to find an output for"); }

        TimeSeriesOutput *output = node->output();
        for (auto index: path) {
            if (index == KEY_SET) {
                auto tsd_output = dynamic_cast<TimeSeriesDictOutput *>(output);
                if (!tsd_output) { throw std::runtime_error("Output is not a TSD for KEY_SET access"); }
                output = &tsd_output->key_set();
            } else {
                auto indexed_output = dynamic_cast<IndexedTimeSeriesOutput *>(output);
                if (!indexed_output) { throw std::runtime_error("Output is not an indexed time series"); }
                output = (*indexed_output)[index].get();
            }
        }
        return output;
    }

    time_series_input_ptr _extract_input(node_ptr node, const std::vector<int64_t> &path) {
        if (path.empty()) { throw std::runtime_error("No path to find an input for"); }

        auto input = dynamic_cast<TimeSeriesInput *>(node->input().get());

        for (const auto &ndx: path) { input = input->get_input(ndx); }
        return input;
    }

    GraphBuilder::GraphBuilder(std::vector<node_builder_ptr> node_builders_, std::vector<Edge> edges_)
        : node_builders{std::move(node_builders_)}, edges{std::move(edges_)} {
    }

    graph_ptr GraphBuilder::make_instance(const std::vector<int64_t> &graph_id, node_ptr parent_node,
                                          const std::string &label) const {
        auto nodes = make_and_connect_nodes(graph_id, 0);
        return nb::ref<Graph>{new Graph{graph_id, nodes, parent_node, label, new Traits()}};
    }

    std::vector<node_ptr> GraphBuilder::make_and_connect_nodes(const std::vector<int64_t> &graph_id,
                                                               int64_t first_node_ndx) const {
        std::vector<node_ptr> nodes;
        nodes.reserve(node_builders.size());

        for (size_t i = 0; i < node_builders.size(); ++i) {
            // Pass graph_id as the node's owning_graph_id (the graph that owns this node)
            nodes.push_back(node_builders[i]->make_instance(graph_id, i + first_node_ndx));
        }

        for (const auto &edge: edges) {
            auto src_node = nodes[edge.src_node];
            auto dst_node = nodes[edge.dst_node];

            time_series_output_ptr output;
            if (edge.output_path.size() == 1 && edge.output_path[0] == ERROR_PATH) {
                output = src_node->error_output();
            } else if (edge.output_path.size() == 1 && edge.output_path[0] == STATE_PATH) {
                output = dynamic_cast_ref<TimeSeriesOutput>(src_node->recordable_state());
            } else {
                output = edge.output_path.empty() ? src_node->output() : _extract_output(src_node, edge.output_path);
            }

            auto input = _extract_input(dst_node, edge.input_path);
            input->bind_output(output);
        }

        return nodes;
    }

    void GraphBuilder::release_instance(graph_ptr item) const {
        auto nodes = item->nodes();
        for (size_t i = 0, l = nodes.size(); i < l; ++i) { node_builders[i]->release_instance(nodes[i]); }
        dispose_component(*item);
    }

    void GraphBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_ < GraphBuilder, Builder > (m, "GraphBuilder")
                .def(nb::init<std::vector<node_builder_ptr>, std::vector<Edge> >(), "node_builders"_a, "edges"_a)
                .def("make_instance", &GraphBuilder::make_instance, "graph_id"_a, "parent_node"_a = nullptr,
                     "label"_a = "")
                .def("make_and_connect_nodes", &GraphBuilder::make_and_connect_nodes, "graph_id"_a, "first_node_ndx"_a)
                .def("release_instance", &GraphBuilder::release_instance, "item"_a)
                .def_prop_ro("node_builders", [](const GraphBuilder &self) {
                    return nb::tuple(nb::cast(self.node_builders));
                })
                .def_prop_ro("edges", [](const GraphBuilder &self) { return nb::tuple(nb::cast(self.edges)); })
                .def("__str__",
                     [](const GraphBuilder &self) {
                         return fmt::format("GraphBuilder@{:p}[nodes={}, edges={}]", static_cast<const void *>(&self),
                                            self.node_builders.size(), self.edges.size());
                     })
                .def("__repr__", [](const GraphBuilder &self) {
                    return fmt::format("GraphBuilder@{:p}[nodes={}, edges={}]", static_cast<const void *>(&self),
                                       self.node_builders.size(), self.edges.size());
                });

        nb::class_<Edge>(m, "Edge")
                .def(nb::init<int64_t, std::vector<int64_t>, int64_t, std::vector<int64_t> >(), "src_node"_a,
                     "output_path"_a,
                     "dst_node"_a, "input_path"_a)
                .def_ro("src_node", &Edge::src_node)
                .def_ro("output_path", &Edge::output_path)
                .def_ro("dst_node", &Edge::dst_node)
                .def_ro("input_path", &Edge::input_path)
                .def("__eq__", &Edge::operator==)
                .def("__lt__", &Edge::operator<)
                .def("__hash__", [](const Edge &self) {
                    return std::hash<Edge>{}(self);
                })
                .def("__str__",
                     [](const Edge &self) {
                         return fmt::format("Edge[{}->{} to {}->[{}]]", self.src_node, fmt::join(self.output_path, ","),
                                            self.dst_node,
                                            fmt::join(self.input_path, ","));
                     })
                .def("__repr__", [](const Edge &self) {
                    return fmt::format("Edge[{}->{} to {}->[{}]]", self.src_node, fmt::join(self.output_path, ","),
                                       self.dst_node,
                                       fmt::join(self.input_path, ","));
                });
    }

    Edge::Edge(int64_t src, std::vector<int64_t> out_path, int64_t dst, std::vector<int64_t> in_path)
        : src_node(src), output_path(std::move(out_path)), dst_node(dst), input_path(std::move(in_path)) {
    }

    bool Edge::operator==(const Edge &other) const {
        return src_node == other.src_node && output_path == other.output_path && dst_node == other.dst_node &&
               input_path == other.input_path;
    }

    bool Edge::operator<(const Edge &other) const {
        if (src_node != other.src_node) return src_node < other.src_node;
        if (output_path != other.output_path) return output_path < other.output_path;
        if (dst_node != other.dst_node) return dst_node < other.dst_node;
        return input_path < other.input_path;
    }
} // namespace hgraph

size_t std::hash<hgraph::Edge>::operator()(const hgraph::Edge &edge) const noexcept {
    size_t h1 = std::hash<int64_t>{}(edge.src_node);
    size_t h2 = std::hash<int64_t>{}(edge.dst_node);
    size_t h3 = 0;
    for (const auto &v: edge.output_path) { h3 ^= std::hash<int64_t>{}(v) + 0x9e3779b9 + (h3 << 6) + (h3 >> 2); }
    size_t h4 = 0;
    for (const auto &v: edge.input_path) { h4 ^= std::hash<int64_t>{}(v) + 0x9e3779b9 + (h4 << 6) + (h4 >> 2); }
    return h1 ^ (h2 << 1) ^ (h3 << 2) ^ (h4 << 3);
}