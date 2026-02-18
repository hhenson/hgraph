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
            std::vector<size_t> remaining_path; // Full path within the REF's element_ts (can be multi-level)
            int64_t src_node_idx;            // Source node index
            int64_t dst_node_idx;            // Destination node index (for subscription)
            std::vector<int64_t> output_path; // Source output path
        };
        // Group deferred edges by the REF input's value_data pointer (uniquely identifies the REF)
        std::unordered_map<void*, std::vector<DeferredRefEdge>> deferred_ref_edges;
        std::unordered_map<void*, ViewData> ref_input_viewdatas;
        // Track ancestor ViewDatas that need their time set when deferred edges are processed.
        // Key: REF value_data ptr, Value: list of ancestor time_data pointers leading to the REF.
        std::unordered_map<void*, std::vector<engine_time_t*>> deferred_ancestor_times;

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

            // Track ancestor time_data pointers during navigation for deferred edges
            std::vector<engine_time_t*> ancestor_times;

            for (size_t pi = 0; pi < edge.input_path.size() && !deferred && !signal_multi_bind; ++pi) {
                auto idx = edge.input_path[pi];
                if (idx >= 0) {
                    // Record the current ViewData's time_data before navigating deeper
                    auto& pre_nav_vd = input_view.ts_view().view_data();
                    if (pre_nav_vd.time_data) {
                        ancestor_times.push_back(static_cast<engine_time_t*>(pre_nav_vd.time_data));
                    }

                    input_view = input_view[static_cast<size_t>(idx)];

                    // After navigation, check if we landed on a REF with more path elements
                    auto& curr_vd = input_view.ts_view().view_data();
                    if (curr_vd.meta && curr_vd.meta->kind == TSKind::REF &&
                        pi + 1 < edge.input_path.size()) {
                        // The next indices would navigate INTO the REF, which isn't supported
                        // for scalar REF types. Defer this edge for NON_PEERED construction.
                        // Capture ALL remaining indices (not just the first) to support
                        // nested structures like TSL[TSL[TS[int]]].
                        std::vector<size_t> remaining;
                        for (size_t ri = pi + 1; ri < edge.input_path.size(); ++ri) {
                            if (edge.input_path[ri] >= 0) {
                                remaining.push_back(static_cast<size_t>(edge.input_path[ri]));
                            }
                        }
                        if (!remaining.empty()) {
                            deferred_ref_edges[curr_vd.value_data].push_back({
                                std::move(remaining),
                                edge.src_node,
                                edge.dst_node,
                                edge.output_path
                            });
                            ref_input_viewdatas.try_emplace(curr_vd.value_data, curr_vd);
                            // Store ancestor times for this REF group (only once per group)
                            deferred_ancestor_times.try_emplace(curr_vd.value_data, ancestor_times);
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
                // 3. Check for REF output → non-REF input binding
                auto& in_vd = input_view.ts_view().view_data();
                auto& out_vd2 = output_view.ts_view().view_data();
                bool ref_to_non_ref = (out_vd2.meta && out_vd2.meta->kind == TSKind::REF &&
                                       in_vd.meta && in_vd.meta->kind != TSKind::REF);

                // Always do standard bind first
                input_view.bind(output_view);

                if (ref_to_non_ref) {
                    // Also create a RefBindingProxy for non-scalar types (TSL, TSS, TSD, TSB)
                    // that can't resolve REF data through their own delegation path.
                    ObserverList* ref_obs = nullptr;
                    if (out_vd2.observer_data) {
                        ref_obs = static_cast<ObserverList*>(out_vd2.observer_data);
                    }
                    if (ref_obs) {
                        dst_node->ts_input()->add_ref_binding_proxy(out_vd2, in_vd, ref_obs);
                    }
                }
            }
        }

        // Phase 2: Process deferred REF-into-composite edges.
        // For each REF input with deferred sub-edges, construct a NON_PEERED TSReference
        // with PEERED items pointing to the source outputs.
        for (auto& [vd_ptr, deferred_edges] : deferred_ref_edges) {
            auto& ref_vd = ref_input_viewdatas.at(vd_ptr);

            // Build nested NON_PEERED reference tree from deferred edges.
            // Edges may have multi-level remaining_paths (e.g., [0,0], [0,1], [1,0], [1,1])
            // which need to be organized into a nested structure:
            // NON_PEERED[NON_PEERED[PEERED(src1), PEERED(src2)], NON_PEERED[PEERED(src3), PEERED(src4)]]
            const TSMeta* element_ts = ref_vd.meta ? ref_vd.meta->element_ts : nullptr;

            // Recursive lambda to build nested TSReference tree
            std::function<TSReference(
                const std::vector<const DeferredRefEdge*>&,
                size_t,           // depth: which level of remaining_path we're examining
                const TSMeta*     // structure at this level
            )> build_ref_tree = [&](
                const std::vector<const DeferredRefEdge*>& edges_at_level,
                size_t depth,
                const TSMeta* meta_at_level
            ) -> TSReference {
                // Determine field count at this level
                size_t field_count = 0;
                if (meta_at_level) {
                    if (meta_at_level->kind == TSKind::TSB) field_count = meta_at_level->field_count;
                    else if (meta_at_level->kind == TSKind::TSL) field_count = meta_at_level->fixed_size;
                }
                if (field_count == 0) {
                    for (auto* de : edges_at_level) {
                        if (depth < de->remaining_path.size()) {
                            field_count = std::max(field_count, de->remaining_path[depth] + 1);
                        }
                    }
                }
                if (field_count == 0) return TSReference::empty();

                // Group edges by their index at this depth
                std::vector<std::vector<const DeferredRefEdge*>> groups(field_count);
                for (auto* de : edges_at_level) {
                    if (depth < de->remaining_path.size()) {
                        size_t idx = de->remaining_path[depth];
                        if (idx < field_count) {
                            groups[idx].push_back(de);
                        }
                    }
                }

                std::vector<TSReference> items(field_count);
                for (size_t i = 0; i < field_count; ++i) {
                    if (groups[i].empty()) continue;

                    // Check if edges in this group are at leaf level (no more path)
                    bool all_leaf = true;
                    for (auto* de : groups[i]) {
                        if (depth + 1 < de->remaining_path.size()) {
                            all_leaf = false;
                            break;
                        }
                    }

                    if (all_leaf) {
                        // Leaf: create PEERED reference from last edge
                        auto* de = groups[i].back();
                        std::vector<size_t> indices;
                        for (auto idx : de->output_path) {
                            if (idx >= 0) indices.push_back(static_cast<size_t>(idx));
                        }
                        ShortPath sp(nodes[de->src_node_idx].get(), PortType::OUTPUT, std::move(indices));
                        items[i] = TSReference::peered(std::move(sp));
                    } else {
                        // Non-leaf: recurse to build nested NON_PEERED
                        const TSMeta* child_meta = meta_at_level ? meta_at_level->element_ts : nullptr;
                        // For TSB, use field meta instead of element_ts
                        if (meta_at_level && meta_at_level->kind == TSKind::TSB &&
                            i < meta_at_level->field_count && meta_at_level->fields) {
                            child_meta = meta_at_level->fields[i].ts_type;
                        }
                        items[i] = build_ref_tree(groups[i], depth + 1, child_meta);
                    }
                }
                return TSReference::non_peered(std::move(items));
            };

            // Build the edge pointer list
            std::vector<const DeferredRefEdge*> edge_ptrs;
            edge_ptrs.reserve(deferred_edges.size());
            for (auto& de : deferred_edges) {
                edge_ptrs.push_back(&de);
            }

            auto ref = build_ref_tree(edge_ptrs, 0, element_ts);
            if (ref_vd.value_data && ref_vd.meta && ref_vd.meta->value_type) {
                value::View v(ref_vd.value_data, ref_vd.meta->value_type);
                auto* ref_ptr = static_cast<TSReference*>(v.data());
                *ref_ptr = std::move(ref);
            }

            // Set modification time so the REF input appears valid
            if (ref_vd.time_data) {
                *static_cast<engine_time_t*>(ref_vd.time_data) = bind_time;
            }

            // Also set modification time on ancestor containers (TSL, TSB, etc.)
            // that were navigated through to reach this REF. Without this, parent
            // containers would have last_modified_time == MIN_DT and valid() would
            // return false, preventing the node from evaluating.
            auto anc_it = deferred_ancestor_times.find(vd_ptr);
            if (anc_it != deferred_ancestor_times.end()) {
                for (auto* anc_time : anc_it->second) {
                    if (*anc_time == MIN_DT) {
                        *anc_time = bind_time;
                    }
                }
            }

            // NOTE: We intentionally do NOT set up LinkTarget here for deferred edges.
            // Setting lt->is_linked=true without valid data pointers (value_data etc.)
            // would cause ref_value() to enter the TS→REF case (Case 1) with null data,
            // returning garbage instead of falling through to Case 3 (direct value_data read).
            // The initial notification still fires because the REF's modification time
            // is set to bind_time above, and the node will be scheduled at start time.

            // Subscribe the dst node's TSInput to each unique source output's observer list,
            // but ONLY for REF→REF (peered) bindings where the source is a REF output.
            // For TS→REF (non-peered), the reference is fixed at bind time and the
            // downstream should NOT be notified when the source value changes.
            // Instead, the RefBindingProxy's NON_PEERED handler subscribes the
            // dereferenced input's LinkTargets to the resolved source observer lists,
            // so the final consumer (e.g., record_to_memory) gets notified directly.
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
                    if (src_vd.meta && src_vd.meta->kind == TSKind::REF &&
                        src_vd.observer_data) {
                        auto* obs = static_cast<ObserverList*>(src_vd.observer_data);
                        obs->add_observer(dst_node->ts_input());
                    }
                }
            }

            // Schedule the destination node at start time so it evaluates and
            // processes its initial REF input value. Without this, nodes with
            // deferred REF edges (like ref_signal in test_free_bundle_ref) would
            // never be scheduled because they're not subscribed to source observers
            // for TS→REF bindings. In Python, the bind sequence triggers an initial
            // notify through the child PythonTimeSeriesReferenceInput chain.
            if (!deferred_edges.empty()) {
                auto* dst_node = nodes[deferred_edges[0].dst_node_idx].get();
                if (dst_node && dst_node->ts_input()) {
                    dst_node->ts_input()->notify(bind_time);
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
