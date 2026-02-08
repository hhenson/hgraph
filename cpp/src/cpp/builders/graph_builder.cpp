#include <hgraph/builders/builder.h>
#include <hgraph/builders/graph_builder.h>
#include <fmt/format.h>
#include <hgraph/builders/node_builder.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_dict_view.h>
#include <hgraph/types/traits.h>

#include <string_view>

namespace hgraph
{
    constexpr int64_t ERROR_PATH = -1;  // The path in the wiring edges representing the error output of the node
    constexpr int64_t STATE_PATH = -2;
    // The path in the wiring edges representing the recordable state output of the node
    constexpr int64_t KEY_SET = -3;  // The path in the wiring edges representing the key_set output of the node

    GraphBuilder::GraphBuilder(std::vector<node_builder_s_ptr> node_builders_, std::vector<Edge> edges_)
        : node_builders{std::move(node_builders_)}, edges{std::move(edges_)} {
        // Calculate and cache memory size in the same order as allocation (nodes first, then graph)
        size_t total = 0;
        for (const auto &node_builder : node_builders) {
            // Use the builder's actual type alignment for correct padding calculation
            total = align_size(total, node_builder->type_alignment());
            total += node_builder->memory_size();
        }
        total = align_size(total, alignof(Graph));
        total += add_canary_size(sizeof(Graph));
        _memory_size = total;
    }

    graph_s_ptr GraphBuilder::make_instance(const std::vector<int64_t> &graph_id, node_ptr parent_node,
                                            const std::string &label, bool use_arena) const {
        auto build_graph = [&](const std::shared_ptr<void> &buffer, size_t *offset) {
            auto nodes = make_and_connect_nodes(graph_id, 0);
            return make_instance_impl<Graph, Graph>(
                buffer, offset, "Graph", graph_id, std::move(nodes), parent_node, label,
                parent_node == nullptr ? nullptr : &parent_node->graph()->traits());
        };

        if (!use_arena) {
            return build_graph(nullptr, nullptr);
        }

        size_t buffer_size = _memory_size;
        for (int attempt = 0; attempt < 3; ++attempt) {
            try {
                auto buffer = std::shared_ptr<void>(::operator new(buffer_size), [](void* p) { ::operator delete(p); });
                ArenaAllocationContext ctx{buffer, 0, buffer_size};
                ArenaAllocationGuard guard(ctx);

                return build_graph(buffer, &ctx.offset);
            } catch (const std::runtime_error &e) {
                std::string_view msg{e.what()};
                if (attempt < 2 && msg.find("Arena buffer overflow") != std::string_view::npos) {
                    buffer_size = buffer_size * 2;
                    continue;
                }
                throw;
            }
        }

        return nullptr;
    }

    Graph::node_list GraphBuilder::make_and_connect_nodes(const std::vector<int64_t> &graph_id,
                                                          int64_t                     first_node_ndx) const {
        Graph::node_list nodes;
        nodes.reserve(node_builders.size());

        for (size_t i = 0; i < node_builders.size(); ++i) {
            // Pass graph_id as the node's owning_graph_id (the graph that owns this node)
            nodes.push_back(node_builders[i]->make_instance(graph_id, i + first_node_ndx));
        }

        // Use a placeholder time for view creation during binding
        // This is fine because binding doesn't depend on current_time value
        engine_time_t bind_time = MIN_ST;
        for (const auto &edge : edges) {
            auto src_node = nodes[edge.src_node].get();
            auto dst_node = nodes[edge.dst_node].get();

            // Determine the source output
            TSOutput* src_output = nullptr;
            if (edge.output_path.size() == 1 && edge.output_path[0] == ERROR_PATH) {
                // Error output - need to handle this specially
                // For now, skip - error outputs may need different handling
                continue;  // TODO: Handle error output binding
            } else if (edge.output_path.size() == 1 && edge.output_path[0] == STATE_PATH) {
                // Recordable state output - need to handle this specially
                continue;  // TODO: Handle recordable state binding
            } else {
                src_output = src_node->ts_output();
            }

            if (!src_output) {
                throw std::runtime_error("Source node does not have TSOutput");
            }

            // The input path must have at least one element (the field index in the input bundle)
            if (edge.input_path.empty()) {
                throw std::runtime_error("Cannot bind input with empty path");
            }

            if (!dst_node->has_input()) {
                throw std::runtime_error("Node does not have TSInput for binding");
            }

            // View-based binding approach:
            // 1. Get input view and navigate to the target field
            TSInputView input_view = dst_node->ts_input()->view(bind_time);

            for (auto idx : edge.input_path) {
                if (idx >= 0) {
                    input_view = input_view[static_cast<size_t>(idx)];
                }
                // Skip negative indices (like KEY_SET) for now
            }

            // 2. Get output view and navigate to the source field
            TSOutputView output_view = src_output->view(bind_time);

            for (auto idx : edge.output_path) {
                if (idx >= 0) {
                    output_view = output_view[static_cast<size_t>(idx)];
                } else if (idx == KEY_SET) {
                    // Navigate from TSD output to its embedded key_set TSS
                    auto dict_view = output_view.ts_view().as_dict();
                    TSSView tss_view = dict_view.key_set();
                    output_view = TSOutputView(
                        TSView(tss_view.view_data(), bind_time),
                        output_view.output()
                    );
                }
                // Skip other negative indices (ERROR_PATH, STATE_PATH) for now
            }

            // 3. Bind the input view to the output view
            input_view.bind(output_view);
        }

        return nodes;
    }

    void GraphBuilder::release_instance(graph_s_ptr item) const {
        auto &nodes = item->nodes();
        for (size_t i = 0, l = nodes.size(); i < l; ++i) {
            try { node_builders[i]->release_instance(nodes[i]); } catch (...) {}
        }
        try { dispose_component(*item); } catch (...) {}
    }

    size_t GraphBuilder::memory_size() const {
        // Return cached memory size calculated in constructor
        return _memory_size;
    }

    size_t GraphBuilder::type_alignment() const {
        return alignof(Graph);
    }

    void GraphBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<GraphBuilder, Builder>(m, "GraphBuilder")
            .def(nb::init<std::vector<node_builder_s_ptr>, std::vector<Edge>>(), "node_builders"_a, "edges"_a)
            .def("make_instance", &GraphBuilder::make_instance, "graph_id"_a, "parent_node"_a = nullptr, "label"_a = "", "use_arena"_a = true)
            .def("make_and_connect_nodes", &GraphBuilder::make_and_connect_nodes, "graph_id"_a, "first_node_ndx"_a)
            .def("release_instance", &GraphBuilder::release_instance, "item"_a)
            .def("memory_size", &GraphBuilder::memory_size)
            .def_prop_ro("node_builders",
                         [](const GraphBuilder &self) { return nb::tuple(nb::cast(self.node_builders)); })
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
            .def(nb::init<int64_t, std::vector<int64_t>, int64_t, std::vector<int64_t>>(), "src_node"_a, "output_path"_a,
                 "dst_node"_a, "input_path"_a)
            .def_ro("src_node", &Edge::src_node)
            .def_ro("output_path", &Edge::output_path)
            .def_ro("dst_node", &Edge::dst_node)
            .def_ro("input_path", &Edge::input_path)
            .def("__eq__", &Edge::operator==)
            .def("__lt__", &Edge::operator<)
            .def("__hash__", [](const Edge &self) { return std::hash<Edge>{}(self); })
            .def("__str__",
                 [](const Edge &self) {
                     return fmt::format("Edge[{}->{} to {}->[{}]]", self.src_node, fmt::join(self.output_path, ","),
                                        self.dst_node, fmt::join(self.input_path, ","));
                 })
            .def("__repr__", [](const Edge &self) {
                return fmt::format("Edge[{}->{} to {}->[{}]]", self.src_node, fmt::join(self.output_path, ","),
                                   self.dst_node, fmt::join(self.input_path, ","));
            });
    }

    Edge::Edge(int64_t src, std::vector<int64_t> out_path, int64_t dst, std::vector<int64_t> in_path)
        : src_node(src), output_path(std::move(out_path)), dst_node(dst), input_path(std::move(in_path)) {}

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
}  // namespace hgraph

size_t std::hash<hgraph::Edge>::operator()(const hgraph::Edge &edge) const noexcept {
    size_t h1 = std::hash<int64_t>{}(edge.src_node);
    size_t h2 = std::hash<int64_t>{}(edge.dst_node);
    size_t h3 = 0;
    for (const auto &v : edge.output_path) { h3 ^= std::hash<int64_t>{}(v) + 0x9e3779b9 + (h3 << 6) + (h3 >> 2); }
    size_t h4 = 0;
    for (const auto &v : edge.input_path) { h4 ^= std::hash<int64_t>{}(v) + 0x9e3779b9 + (h4 << 6) + (h4 >> 2); }
    return h1 ^ (h2 << 1) ^ (h3 << 2) ^ (h4 << 3);
}
