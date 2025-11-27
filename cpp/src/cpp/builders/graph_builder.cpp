#include "hgraph/types/tsd.h"
#include <hgraph/builders/builder.h>
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

        time_series_output_ptr output = node->output();
        for (auto index: path) {
            if (index == KEY_SET) {
                auto tsd_output = std::dynamic_pointer_cast<TimeSeriesDictOutput>(output);
                if (!tsd_output) { throw std::runtime_error("Output is not a TSD for KEY_SET access"); }
                // key_set() returns a reference, we need to create an aliasing shared_ptr
                output = time_series_output_ptr(output, &tsd_output->key_set());
            } else {
                auto indexed_output = std::dynamic_pointer_cast<IndexedTimeSeriesOutput>(output);
                if (!indexed_output) { throw std::runtime_error("Output is not an indexed time series"); }
                output = (*indexed_output)[index];
            }
        }
        return output;
    }

    time_series_input_ptr _extract_input(node_ptr node, const std::vector<int64_t> &path) {
        if (path.empty()) { throw std::runtime_error("No path to find an input for"); }

        auto input_bundle = node->input();
        auto input = dynamic_cast<TimeSeriesInput *>(input_bundle.get());

        for (const auto &ndx: path) { 
            input = input->get_input(ndx);
        }
        // Convert raw pointer to shared_ptr using aliasing constructor
        // The input is owned by the input_bundle, so we use it as the donor
        return time_series_input_ptr(input_bundle, input);
    }

    GraphBuilder::GraphBuilder(std::vector<node_builder_ptr> node_builders_, std::vector<Edge> edges_)
        : node_builders{std::move(node_builders_)}, edges{std::move(edges_)} {
        // Calculate and cache memory size
        // Start with Graph (with canary) - Traits is now a value member, so it's included in Graph's size
        size_t total = add_canary_size(sizeof(Graph));
        // For each node builder, align to Node alignment and add its size (Node already includes canary in memory_size)
        for (const auto &node_builder : node_builders) {
            total = align_size(total, alignof(Node));
            total += node_builder->memory_size();
        }
        _memory_size = total;
    }

    graph_ptr GraphBuilder::make_instance(const std::vector<int64_t> &graph_id, node_ptr parent_node,
                                          const std::string &label) const {
        // Allocate arena buffer
        size_t buffer_size = memory_size();
        void* arena_buffer = std::malloc(buffer_size);
        if (!arena_buffer) {
            throw std::bad_alloc();
        }
        
        // Track current offset in buffer
        size_t offset = 0;
        char* buffer = static_cast<char*>(arena_buffer);
        
        // Construct Traits first (we need it for Graph constructor)
        Traits* parent_traits_ptr = nullptr;
        if (parent_node && parent_node->graph()) {
            auto parent_graph = parent_node->graph();
            parent_traits_ptr = &parent_graph->_traits;
        }
        Traits traits(parent_traits_ptr);
        
        // Construct Graph in-place at the start of the buffer
        offset = align_size(offset, alignof(Graph));
        // Set canary BEFORE construction to detect if construction overwrites beyond object bounds
        size_t graph_size = sizeof(Graph);
        size_t aligned_graph_size = align_size(graph_size, alignof(size_t));
        if (arena_debug_mode) {
            size_t* canary_ptr = reinterpret_cast<size_t*>(buffer + offset + aligned_graph_size);
            *canary_ptr = ARENA_CANARY_PATTERN;
        }
        // Now construct the object
        Graph* graph_ptr_raw = new (buffer + offset) Graph{graph_id, {}, parent_node, label, std::move(traits)};
        // Immediately check canary after construction
        verify_canary(graph_ptr_raw, sizeof(Graph), "Graph");
        offset += add_canary_size(sizeof(Graph));
        
        // Create shared_ptr IMMEDIATELY after construction to initialize enable_shared_from_this
        // This is critical: enable_shared_from_this must be initialized before any code tries to call shared_from_this()
        // Store the GraphBuilder as nb::ref to ensure we retain a reference
        const GraphBuilder* builder_ptr = this;
        nb::ref<const GraphBuilder> builder_ref = nb::ref<const GraphBuilder>(builder_ptr);
        graph_ptr result = std::shared_ptr<Graph>(
            graph_ptr_raw,
            [builder_ref, arena_buffer](Graph* g) {
                // Call release_instance to clean up resources and validate canaries
                if (g) {
                    // Create a temporary shared_ptr for release_instance
                    graph_ptr temp_ptr(g, [](Graph*){ /* no-op, already managing lifetime */ });
                    builder_ref->release_instance(temp_ptr);
                }
                // Free the arena buffer
                std::free(arena_buffer);
            }
        );
        
        // Traits is now stored as a value member in Graph, so we don't need separate construction
        // The Graph constructor already initialized _traits with the passed Traits object
        
        // Construct nodes in-place
        std::vector<node_ptr> nodes;
        nodes.reserve(node_builders.size());
        
        for (size_t i = 0; i < node_builders.size(); ++i) {
            // Align for Node
            offset = align_size(offset, alignof(Node));
            // NodeBuilder will construct the Node and its TimeSeries objects in-place
            // We need to pass the buffer pointer and current offset
            size_t node_offset = offset;  // Save starting offset
            node_ptr node = node_builders[i]->make_instance(graph_id, i, buffer, &offset);
            
            // Set graph on the node immediately so it can use graph's control block if needed
            node->set_graph(result);
            
            nodes.push_back(node);
            // Offset is updated by make_instance, verify it matches expected size
            size_t expected_offset = node_offset + node_builders[i]->memory_size();
            if (offset != expected_offset) {
                throw std::runtime_error(fmt::format("Node {} memory size mismatch: expected offset {}, got {} (signature: {})", 
                    i, expected_offset, offset, node->signature().name));
            }
        }
        
        // Update Graph's nodes vector and resize schedule
        result->_nodes = std::move(nodes);
        
        // Resize _schedule to match nodes size (was 0 because Graph was constructed with empty nodes)
        result->_schedule.resize(result->_nodes.size(), MIN_DT);
        
        // Recalculate _push_source_nodes_end
        auto it = std::find_if(result->_nodes.begin(), result->_nodes.end(),
                               [](const node_ptr &v) { return v->signature().node_type != NodeTypeEnum::PUSH_SOURCE_NODE; });
        result->_push_source_nodes_end = std::distance(result->_nodes.begin(), it);
        
        // Connect edges
        for (const auto &edge: edges) {
            auto src_node = result->_nodes[edge.src_node];
            auto dst_node = result->_nodes[edge.dst_node];

            time_series_output_ptr output;
            if (edge.output_path.size() == 1 && edge.output_path[0] == ERROR_PATH) {
                output = src_node->error_output();
            } else if (edge.output_path.size() == 1 && edge.output_path[0] == STATE_PATH) {
                auto recordable_state = src_node->recordable_state();
                output = std::dynamic_pointer_cast<TimeSeriesOutput>(recordable_state);
                if (!output) {
                    throw std::runtime_error("recordable_state is not a TimeSeriesOutput");
                }
            } else {
                output = edge.output_path.empty() ? src_node->output() : _extract_output(src_node, edge.output_path);
            }

            auto input = _extract_input(dst_node, edge.input_path);
            input->bind_output(output);
        }
        
        return result;
    }

    std::vector<node_ptr> GraphBuilder::make_and_connect_nodes(const std::vector<int64_t> &graph_id,
                                                               int64_t first_node_ndx) const {
        std::vector<node_ptr> nodes;
        nodes.reserve(node_builders.size());

        for (size_t i = 0; i < node_builders.size(); ++i) {
            // Pass graph_id as the node's owning_graph_id (the graph that owns this node)
            size_t dummy_offset = 0;
            nodes.push_back(node_builders[i]->make_instance(graph_id, i + first_node_ndx, nullptr, &dummy_offset));
        }

        for (const auto &edge: edges) {
            auto src_node = nodes[edge.src_node];
            auto dst_node = nodes[edge.dst_node];

            time_series_output_ptr output;
            if (edge.output_path.size() == 1 && edge.output_path[0] == ERROR_PATH) {
                output = src_node->error_output();
            } else if (edge.output_path.size() == 1 && edge.output_path[0] == STATE_PATH) {
                auto recordable_state = src_node->recordable_state();
                output = std::dynamic_pointer_cast<TimeSeriesOutput>(recordable_state);
                if (!output) {
                    throw std::runtime_error("recordable_state is not a TimeSeriesOutput");
                }
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

    size_t GraphBuilder::memory_size() const {
        // Return cached memory size calculated in constructor
        return _memory_size;
    }

    void GraphBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_ < GraphBuilder, Builder > (m, "GraphBuilder")
                .def(nb::init<std::vector<node_builder_ptr>, std::vector<Edge> >(), "node_builders"_a, "edges"_a)
                .def("make_instance", &GraphBuilder::make_instance, "graph_id"_a, "parent_node"_a = nullptr,
                     "label"_a = "")
                .def("make_and_connect_nodes", &GraphBuilder::make_and_connect_nodes, "graph_id"_a, "first_node_ndx"_a)
                .def("release_instance", &GraphBuilder::release_instance, "item"_a)
                .def("memory_size", &GraphBuilder::memory_size)
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