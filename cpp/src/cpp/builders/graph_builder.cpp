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
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/short_path.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/value/value_view.h>

#include <string_view>
#include <unordered_map>

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

        // Deferred REF-into-composite edges: when an input path navigates into a REF type
        // (e.g., REF[TSB[AB]] with sub-field indices), we can't navigate children since REF
        // is scalar. Instead, we collect these edges and construct NON_PEERED TSReferences.
        struct DeferredRefEdge {
            size_t remaining_index;          // Field index within the REF's element_ts
            int64_t src_node_idx;            // Source node index
            int64_t dst_node_idx;            // Destination node index (for subscription)
            std::vector<int64_t> output_path; // Source output path
        };
        // Group deferred edges by the REF input's value_data pointer (uniquely identifies the REF)
        std::unordered_map<void*, std::vector<DeferredRefEdge>> deferred_ref_edges;
        std::unordered_map<void*, ViewData> ref_input_viewdatas;

        for (const auto &edge : edges) {
            auto src_node = nodes[edge.src_node].get();
            auto dst_node = nodes[edge.dst_node].get();

            // Determine the source output
            TSOutput* src_output = nullptr;
            if (edge.output_path.size() == 1 && edge.output_path[0] == ERROR_PATH) {
                src_output = src_node->ts_error_output();
                if (!src_output) continue;  // No error output on this node
            } else if (edge.output_path.size() == 1 && edge.output_path[0] == STATE_PATH) {
                src_output = src_node->ts_recordable_state();
                if (!src_output) continue;  // No recordable state on this node
            } else {
                src_output = src_node->ts_output();
            }

            if (!src_output) {
                throw std::runtime_error("Source node does not have TSOutput");
            }

            if (edge.input_path.empty()) {
                throw std::runtime_error("Cannot bind input with empty path");
            }

            if (!dst_node->has_input()) {
                throw std::runtime_error("Node does not have TSInput for binding");
            }

            // 1. Navigate input path, detecting REF-into-composite and SIGNAL-into-composite edges
            TSInputView input_view = dst_node->ts_input()->view(bind_time);
            bool deferred = false;
            bool signal_multi_bind = false;

            for (size_t pi = 0; pi < edge.input_path.size() && !deferred && !signal_multi_bind; ++pi) {
                auto idx = edge.input_path[pi];
                if (idx >= 0) {
                    input_view = input_view[static_cast<size_t>(idx)];

                    // After navigation, check if we landed on a REF with more path elements
                    auto& curr_vd = input_view.ts_view().view_data();
                    if (curr_vd.meta && curr_vd.meta->kind == TSKind::REF &&
                        pi + 1 < edge.input_path.size()) {
                        // The next index would navigate INTO the REF, which isn't supported
                        // for scalar REF types. Defer this edge for NON_PEERED construction.
                        int64_t next_idx = edge.input_path[pi + 1];
                        if (next_idx >= 0) {
                            deferred_ref_edges[curr_vd.value_data].push_back({
                                static_cast<size_t>(next_idx),
                                edge.src_node,
                                edge.dst_node,
                                edge.output_path
                            });
                            ref_input_viewdatas.try_emplace(curr_vd.value_data, curr_vd);
                        }
                        deferred = true;
                    }

                    // Check if we landed on a SIGNAL with more path elements.
                    // SIGNAL is scalar (no children), but non-peered composites (TSB, TSL)
                    // bound to SIGNAL create edges that navigate into SIGNAL children.
                    // Instead, register a SignalSubscription on the TSInput that updates
                    // the SIGNAL's time_data when the source output ticks.
                    if (curr_vd.meta && curr_vd.meta->kind == TSKind::SIGNAL &&
                        pi + 1 < edge.input_path.size()) {
                        signal_multi_bind = true;
                    }
                }
            }
            if (deferred) {
                continue;
            }

            // 2. Navigate output path
            TSOutputView output_view = src_output->view(bind_time);

            for (auto idx : edge.output_path) {
                if (idx >= 0) {
                    output_view = output_view[static_cast<size_t>(idx)];
                } else if (idx == KEY_SET) {
                    auto dict_view = output_view.ts_view().as_dict();
                    TSSView tss_view = dict_view.key_set();
                    output_view = TSOutputView(
                        TSView(tss_view.view_data(), bind_time),
                        output_view.output()
                    );
                }
            }

            if (signal_multi_bind) {
                // SIGNAL non-peered binding: register a SignalSubscription that
                // updates the SIGNAL's time_data and schedules the node.
                // input_view already navigated to the SIGNAL child.
                auto& signal_vd = input_view.ts_view().view_data();
                auto* signal_time = static_cast<engine_time_t*>(signal_vd.time_data);

                // Extract observer list from the source output
                auto& out_vd = output_view.ts_view().view_data();
                ObserverList* obs_list = nullptr;
                if (out_vd.observer_data) {
                    if (out_vd.meta && (out_vd.meta->kind == TSKind::TSB ||
                                         out_vd.meta->kind == TSKind::TSL ||
                                         out_vd.meta->kind == TSKind::TSD)) {
                        auto obs_view = output_view.ts_view().observer();
                        obs_list = static_cast<ObserverList*>(obs_view.as_tuple().at(0).data());
                    } else {
                        obs_list = static_cast<ObserverList*>(out_vd.observer_data);
                    }
                }

                if (signal_time && obs_list) {
                    dst_node->ts_input()->add_signal_subscription(signal_time, obs_list);
                }
            } else {
                // 3. Standard bind: create LinkTarget and subscribe
                input_view.bind(output_view);
            }
        }

        // Phase 2: Process deferred REF-into-composite edges.
        // For each REF input with deferred sub-edges, construct a NON_PEERED TSReference
        // with PEERED items pointing to the source outputs.
        for (auto& [vd_ptr, deferred_edges] : deferred_ref_edges) {
            auto& ref_vd = ref_input_viewdatas.at(vd_ptr);

            // Determine field count from REF's element_ts
            const TSMeta* element_ts = ref_vd.meta ? ref_vd.meta->element_ts : nullptr;
            size_t field_count = 0;
            if (element_ts) {
                if (element_ts->kind == TSKind::TSB) {
                    field_count = element_ts->field_count;
                } else if (element_ts->kind == TSKind::TSL) {
                    field_count = element_ts->fixed_size;
                }
            }
            if (field_count == 0) {
                // Can't determine structure - use max index + 1
                for (auto& de : deferred_edges) {
                    field_count = std::max(field_count, de.remaining_index + 1);
                }
            }

            // Create PEERED TSReference items from source outputs
            std::vector<TSReference> items(field_count);
            for (auto& de : deferred_edges) {
                // Build ShortPath indices from the output_path
                std::vector<size_t> indices;
                for (auto idx : de.output_path) {
                    if (idx >= 0) indices.push_back(static_cast<size_t>(idx));
                }

                ShortPath sp(nodes[de.src_node_idx].get(), PortType::OUTPUT, std::move(indices));

                if (de.remaining_index < field_count) {
                    items[de.remaining_index] = TSReference::peered(std::move(sp));
                }
            }

            // Create and store NON_PEERED reference in the REF input's value storage
            auto ref = TSReference::non_peered(std::move(items));
            if (ref_vd.value_data && ref_vd.meta && ref_vd.meta->value_type) {
                value::View v(ref_vd.value_data, ref_vd.meta->value_type);
                auto* ref_ptr = static_cast<TSReference*>(v.data());
                *ref_ptr = std::move(ref);
            }

            // Set modification time so the REF input appears valid
            if (ref_vd.time_data) {
                *static_cast<engine_time_t*>(ref_vd.time_data) = bind_time;
            }

            // Set up the LinkTarget so set_active() can detect the TS→REF binding
            // and fire the initial notification (notify(MIN_ST)).
            // The deferred path skips scalar_ops::bind(), so the LinkTarget would
            // otherwise remain uninitialized (is_linked=false, meta=nullptr).
            // Python equivalent: PythonTimeSeriesReferenceInput.do_bind_output()
            // appends to start_inputs for initial notification at node start.
            if (ref_vd.uses_link_target && ref_vd.link_data && element_ts) {
                auto* lt = static_cast<LinkTarget*>(ref_vd.link_data);
                lt->is_linked = true;
                lt->meta = element_ts;  // Inner type (TSB/TSL), not REF — triggers TS→REF detection
            }

            // Subscribe the dst node's TSInput to each unique source output's observer list,
            // but ONLY for REF→REF (peered) bindings where the source is a REF output.
            // For TS→REF (non-peered), the reference is fixed at bind time and the
            // downstream should NOT be notified when the source value changes.
            for (size_t dei = 0; dei < deferred_edges.size(); ++dei) {
                auto& de = deferred_edges[dei];
                // Check if we already subscribed to this source
                bool already = false;
                for (size_t j = 0; j < dei; ++j) {
                    if (deferred_edges[j].src_node_idx == de.src_node_idx) { already = true; break; }
                }
                if (already) continue;

                auto* dst_node = nodes[de.dst_node_idx].get();
                auto* src_output = nodes[de.src_node_idx]->ts_output();
                if (dst_node && dst_node->ts_input() && src_output) {
                    ViewData src_vd = src_output->native_value().make_view_data();
                    // Only subscribe if source is a REF output (REF→REF peered binding).
                    // Non-REF sources (TS→REF) are fixed references that should not
                    // trigger the downstream node on value changes.
                    if (src_vd.meta && src_vd.meta->kind == TSKind::REF &&
                        src_vd.observer_data) {
                        auto* obs = static_cast<ObserverList*>(src_vd.observer_data);
                        obs->add_observer(dst_node->ts_input());
                    }
                }
            }
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
