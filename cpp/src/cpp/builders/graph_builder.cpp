#include <hgraph/builders/builder.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/builders/node_builder.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/traits.h>

#include <string_view>

namespace hgraph
{
    namespace {
        constexpr int64_t ERROR_PATH = -1;
        constexpr int64_t STATE_PATH = -2;
        constexpr int64_t KEY_SET_PATH_ID = -3;

        void bind_static_container_recursive(TSInputView input_view, TSOutputView output_view) {
            if (!input_view || !output_view) {
                return;
            }

            const TSMeta* input_meta = input_view.ts_meta();
            const bool bind_parent =
                !(input_meta != nullptr &&
                  input_meta->kind == TSKind::TSL &&
                  input_meta->fixed_size() > 0 &&
                  input_meta->element_ts() != nullptr &&
                  input_meta->element_ts()->kind == TSKind::REF);

            if (bind_parent) {
                input_view.bind(output_view);
            }

            if (input_meta == nullptr) {
                return;
            }

            if (input_meta->kind == TSKind::TSB) {
                const size_t n = input_meta->field_count();
                for (size_t i = 0; i < n; ++i) {
                    bind_static_container_recursive(input_view.child_at(i), output_view.child_at(i));
                }
                return;
            }

            if (input_meta->kind == TSKind::TSL && input_meta->fixed_size() > 0) {
                const size_t n = input_meta->fixed_size();
                for (size_t i = 0; i < n; ++i) {
                    bind_static_container_recursive(input_view.child_at(i), output_view.child_at(i));
                }
            }
        }
    }  // namespace

    bool _bind_ts_endpoint(node_ptr src_node, const std::vector<int64_t> &output_path,
                           node_ptr dst_node, const std::vector<int64_t> &input_path) {
        TSOutputView output_view;
        std::vector<int64_t> normalized_output_path = output_path;
        if (output_path.size() == 1 && output_path.front() == ERROR_PATH) {
            output_view = src_node->error_output(MIN_DT);
            normalized_output_path.clear();
        } else if (output_path.size() == 1 && output_path.front() == STATE_PATH) {
            output_view = src_node->recordable_state(MIN_DT);
            normalized_output_path.clear();
        } else {
            output_view = src_node->output(MIN_DT);
        }

        TSInputView input_view = dst_node->input(MIN_DT);
        if (!output_view || !input_view) {
            return false;
        }

        for (int64_t index : normalized_output_path) {
            if (index == KEY_SET_PATH_ID) {
                if (output_view.as_ts_view().kind() != TSKind::TSD) {
                    return false;
                }
                output_view.as_ts_view().view_data().projection = ViewProjection::TSD_KEY_SET;
                continue;
            }
            if (index < 0) {
                return false;
            }
            output_view = output_view.child_at(static_cast<size_t>(index));
            if (!output_view) {
                return false;
            }
        }

        for (int64_t index : input_path) {
            if (index == KEY_SET_PATH_ID) {
                if (input_view.as_ts_view().kind() != TSKind::TSD) {
                    return false;
                }
                input_view.as_ts_view().view_data().projection = ViewProjection::TSD_KEY_SET;
                continue;
            }
            if (index < 0) {
                return false;
            }
            input_view = input_view.child_at(static_cast<size_t>(index));
            if (!input_view) {
                return false;
            }
        }

        const TSMeta* input_meta = input_view.ts_meta();
        if (input_meta != nullptr &&
            (input_meta->kind == TSKind::TSB || (input_meta->kind == TSKind::TSL && input_meta->fixed_size() > 0))) {
            const TSMeta* output_meta = output_view.ts_meta();
            if (output_meta != nullptr && output_meta->kind == TSKind::REF) {
                input_view.bind(output_view);
            } else {
                bind_static_container_recursive(input_view, output_view);
            }
        } else {
            input_view.bind(output_view);
        }
        return true;
    }

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

        for (const auto &edge : edges) {
            auto src_node = nodes[edge.src_node].get();
            auto dst_node = nodes[edge.dst_node].get();

            if (!_bind_ts_endpoint(src_node, edge.output_path, dst_node, edge.input_path)) {
                throw std::runtime_error(
                    fmt::format("TS endpoint bind failed for edge {} -> {}", edge.src_node, edge.dst_node));
            }
        }

        return nodes;
    }

    void GraphBuilder::release_instance(graph_s_ptr item) const {
        auto &nodes = item->nodes();
        for (size_t i = 0, l = nodes.size(); i < l; ++i) { node_builders[i]->release_instance(nodes[i]); }
        dispose_component(*item);
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
