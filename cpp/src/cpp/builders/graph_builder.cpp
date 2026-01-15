#include "hgraph/types/tsd.h"
#include <hgraph/builders/builder.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/builders/node_builder.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/time_series/ts_input_root.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_ref_target_link.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/ts_signal.h>
#include <hgraph/types/tsb.h>
#include <iostream>
#include <variant>

namespace hgraph
{
    // Note: ERROR_PATH and STATE_PATH are defined in ts_value.h (included via node.h)
    constexpr int64_t KEY_SET = -3;  // The path in the wiring edges representing the key_set output of the node

    // ========== TSValue-based navigation and binding ==========

    /**
     * @brief Navigate to a TSValue at a specific path within an output.
     *
     * For empty paths, returns the root output directly.
     * For non-empty paths, navigates using child_value() for bundle types.
     *
     * Note: This is a Phase 6.5 implementation that handles common cases.
     * Full nested navigation may require additional work.
     *
     * @param output The root output TSValue
     * @param path Path indices to navigate (e.g., [0, 1] means field 0, then field 1)
     * @return Pointer to the TSValue at the path, or nullptr if not found
     */
    const TSValue* _extract_ts_output(const TSValue* output, const std::vector<int64_t> &path) {
        if (!output) { return nullptr; }
        if (path.empty()) { return output; }

        // For non-empty paths, we need to navigate through child TSValues.
        // This requires the output to have link support enabled (which creates child_values).
        // For most cases, bundles with separate output fields have this.

        const TSValue* current = output;
        for (auto index : path) {
            if (index == KEY_SET) {
                // KEY_SET navigation: Don't change current - the TSD is the output
                // The binding will set element_index=KEY_SET_INDEX on the link
                // so TSLink::view() returns a TSSView of the TSD's keys
                continue;
            }

            // Try to get child TSValue at this index
            const TSValue* child = current->child_value(static_cast<size_t>(index));
            if (!child) {
                // For outputs without link_support, child_value returns nullptr.
                // In this case, the binding model needs the whole parent TSValue
                // and the input needs to know which field to read.
                // For Phase 6.5, we return the root and handle this in binding.
                // This works when the output is a simple scalar wrapped in a bundle.
                return output;
            }
            current = child;
        }
        return current;
    }

    /**
     * @brief Bind an input field to an output TSValue using the new TSInputRoot interface.
     *
     * This is the TSValue-based binding that replaces the old TimeSeriesInput::bind_output().
     *
     * For nested paths like [0, 1], we navigate through child_value() to reach the
     * nested TSValue with link support, then call create_link() on the final level.
     *
     * @param dst_node The destination node whose input will be bound
     * @param input_path Path to the input field (e.g., [0] means field 0, [0, 1] means field 1 of field 0)
     * @param root_output The root output TSValue from the source node
     * @param output_path Path to navigate within the output
     */
    void _bind_ts_input_to_output(Node* dst_node, const std::vector<int64_t> &input_path,
                                   const TSValue* root_output, const std::vector<int64_t> &output_path) {
        if (input_path.empty()) {
            throw std::runtime_error("Cannot bind input with empty path");
        }

        if (!dst_node->has_ts_input()) {
            throw std::runtime_error("Node does not have TSInputRoot for binding");
        }

        // Navigate output to the default (deepest) level
        const TSValue* output = output_path.empty()
            ? root_output
            : _extract_ts_output(root_output, output_path);

        TSInputRoot& input_root = dst_node->ts_input();

        if (input_path.size() == 1) {
            // Single-level path - bind directly on the root
            input_root.bind_field(static_cast<size_t>(input_path[0]), output);

            // Check for TSL->TS element binding:
            // If output is TSL but input expects TS, the output_path indicates which element to bind to.
            // We need to store this element index in the link so view() can navigate to the correct element.
            if (output && output->ts_meta() && output->ts_meta()->kind() == TSTypeKind::TSL && !output_path.empty()) {
                // Get the expected field type from the input bundle
                size_t field_idx = static_cast<size_t>(input_path[0]);
                TSBView bundle = input_root.bundle_view();
                const TSValue* link_source = bundle.link_source();

                if (link_source) {
                    const TSBTypeMeta* input_bundle_meta = static_cast<const TSBTypeMeta*>(link_source->ts_meta());
                    if (input_bundle_meta && field_idx < input_bundle_meta->field_count()) {
                        const TSMeta* expected_field_type = input_bundle_meta->field(field_idx).type;
                        // If input expects TS (not TSL), this is an element binding
                        if (expected_field_type && expected_field_type->kind() == TSTypeKind::TS) {
                            // Set the element index on the link
                            LinkStorage* storage = const_cast<TSValue*>(link_source)->link_storage_at(field_idx);
                            if (storage) {
                                std::visit([&output_path](auto& link) {
                                    using T = std::decay_t<decltype(link)>;
                                    if constexpr (std::is_same_v<T, std::unique_ptr<TSLink>>) {
                                        if (link) {
                                            // Use the first element of output_path as the element index
                                            link->set_element_index(static_cast<int>(output_path[0]));
                                        }
                                    } else if constexpr (std::is_same_v<T, std::unique_ptr<TSRefTargetLink>>) {
                                        if (link) {
                                            // For TSRefTargetLink, set element_index on target_link
                                            // When the REF resolves to a TSL, target_link.view() will navigate to the element
                                            link->target_link().set_element_index(static_cast<int>(output_path[0]));
                                        }
                                    }
                                }, *storage);
                            }
                        }
                    }
                }
            }

            // Check for TSB->field binding:
            // If output is TSB but input expects non-TSB, the output_path indicates which field to bind to.
            // This happens when we couldn't navigate to the specific output field (no child_values).
            // We store the field index in the link so view() can navigate to the correct field.
            if (output && output->ts_meta() && output->ts_meta()->kind() == TSTypeKind::TSB && !output_path.empty()) {
                size_t field_idx = static_cast<size_t>(input_path[0]);
                TSBView bundle = input_root.bundle_view();
                const TSValue* link_source = bundle.link_source();

                if (link_source) {
                    const TSBTypeMeta* input_bundle_meta = static_cast<const TSBTypeMeta*>(link_source->ts_meta());
                    if (input_bundle_meta && field_idx < input_bundle_meta->field_count()) {
                        const TSMeta* expected_field_type = input_bundle_meta->field(field_idx).type;
                        // If input expects non-TSB, this is a field binding
                        if (expected_field_type && expected_field_type->kind() != TSTypeKind::TSB) {
                            // Set the field index on the link
                            LinkStorage* storage = const_cast<TSValue*>(link_source)->link_storage_at(field_idx);
                            if (storage) {
                                std::visit([&output_path](auto& link) {
                                    using T = std::decay_t<decltype(link)>;
                                    if constexpr (std::is_same_v<T, std::unique_ptr<TSLink>>) {
                                        if (link) {
                                            // Use the first element of output_path as the field index
                                            link->set_field_index(static_cast<int>(output_path[0]));
                                        }
                                    } else if constexpr (std::is_same_v<T, std::unique_ptr<TSRefTargetLink>>) {
                                        if (link) {
                                            // For TSRefTargetLink, set field_index on target_link
                                            link->target_link().set_field_index(static_cast<int>(output_path[0]));
                                        }
                                    }
                                }, *storage);
                            }
                        }
                    }
                }
            }

            // Check for TSD->TSS key_set binding:
            // If output_path contains KEY_SET, this is a key_set binding (TSD viewed as TSS).
            // Set element_index to KEY_SET_INDEX so TSLink::view() returns a TSSView of TSD keys.
            for (auto idx : output_path) {
                if (idx == KEY_SET) {
                    size_t field_idx = static_cast<size_t>(input_path[0]);
                    TSBView bundle = input_root.bundle_view();
                    const TSValue* link_source = bundle.link_source();

                    if (link_source) {
                        LinkStorage* storage = const_cast<TSValue*>(link_source)->link_storage_at(field_idx);
                        if (storage) {
                            std::visit([](auto& link) {
                                using T = std::decay_t<decltype(link)>;
                                if constexpr (std::is_same_v<T, std::unique_ptr<TSLink>>) {
                                    if (link) {
                                        // Set KEY_SET_INDEX so view() creates TSSView of TSD keys
                                        link->set_element_index(TSLink::KEY_SET_INDEX);
                                    }
                                } else if constexpr (std::is_same_v<T, std::unique_ptr<TSRefTargetLink>>) {
                                    if (link) {
                                        link->target_link().set_element_index(TSLink::KEY_SET_INDEX);
                                    }
                                }
                            }, *storage);
                        }
                    }
                    break;  // Only need to handle one KEY_SET
                }
            }
        } else {
            // Nested path - navigate through child_value() to reach the target level
            // Access the underlying TSValue via bundle_view's link source
            TSBView bundle = input_root.bundle_view();
            const TSValue* link_source = bundle.link_source();

            if (!link_source || !link_source->has_link_support()) {
                throw std::runtime_error("TSValue binding: root does not have link support");
            }

            // Navigate through child_values for all but the last index
            // We need mutable access, so we const_cast (safe because we own this structure)
            TSValue* parent = const_cast<TSValue*>(link_source);

            // Check if the first path element is a REF field - if so, links should notify once
            // This matches Python's behavior where REF inputs don't subscribe to output overlays
            bool is_ref_binding = false;
            if (!input_path.empty() && parent->ts_meta()) {
                auto* bundle_meta = dynamic_cast<const TSBTypeMeta*>(parent->ts_meta());
                if (bundle_meta) {
                    size_t first_idx = static_cast<size_t>(input_path[0]);
                    if (first_idx < bundle_meta->field_count()) {
                        const auto& field_info = bundle_meta->field(first_idx);
                        is_ref_binding = (field_info.type && field_info.type->kind() == TSTypeKind::REF);
                    }
                }
            }

            for (size_t i = 0; i < input_path.size() - 1; ++i) {
                size_t idx = static_cast<size_t>(input_path[i]);
                TSValue* child = parent->child_value(idx);

                if (!child) {
                    // Special case: If there's no child_value, check if this is a REF type
                    // REF types don't have nested children - instead, we should bind
                    // the REF directly to the output. The remaining path indices describe
                    // the inner structure of what the REF points to, not navigation targets.
                    // For example, REF[TSB[AB]] has path [0, 0] where:
                    //   - First 0 = the REF field in the input bundle
                    //   - Second 0 = first field of the TSB the REF points to
                    // In this case, bind the REF (at idx) directly to the output.
                    if (i == 0) {
                        // We're at the first level - bind directly at this index
                        parent->create_link(idx, output);
                        TSLink* link = parent->link_at(idx);
                        if (link) {
                            // Set notify_once for REF bindings BEFORE make_active
                            // This must be done regardless of active state
                            if (is_ref_binding) {
                                link->set_notify_once(true);
                            }
                            if (input_root.active()) {
                                link->make_active();
                            }
                        }
                        return;  // Early exit - binding complete
                    }

                    throw std::runtime_error(
                        "TSValue binding: no nested TSValue at path index " + std::to_string(i) +
                        " - the intermediate field may not be a composite type");
                }
                parent = child;
            }

            // Now bind at the final index
            size_t final_idx = static_cast<size_t>(input_path.back());
            parent->create_link(final_idx, output);

            // Check for TSL->TS element binding (same logic as single-level path)
            if (output && output->ts_meta() && output->ts_meta()->kind() == TSTypeKind::TSL && !output_path.empty()) {
                // Get the expected field type from parent's bundle
                const TSBTypeMeta* parent_bundle_meta = dynamic_cast<const TSBTypeMeta*>(parent->ts_meta());
                if (parent_bundle_meta && final_idx < parent_bundle_meta->field_count()) {
                    const TSMeta* expected_field_type = parent_bundle_meta->field(final_idx).type;
                    // If input expects TS (not TSL), this is an element binding
                    if (expected_field_type && expected_field_type->kind() == TSTypeKind::TS) {
                        // Set the element index on the link
                        LinkStorage* storage = parent->link_storage_at(final_idx);
                        if (storage) {
                            std::visit([&output_path](auto& link) {
                                using T = std::decay_t<decltype(link)>;
                                if constexpr (std::is_same_v<T, std::unique_ptr<TSLink>>) {
                                    if (link) {
                                        link->set_element_index(static_cast<int>(output_path[0]));
                                    }
                                } else if constexpr (std::is_same_v<T, std::unique_ptr<TSRefTargetLink>>) {
                                    if (link) {
                                        link->target_link().set_element_index(static_cast<int>(output_path[0]));
                                    }
                                }
                            }, *storage);
                        }
                    }
                }
            }

            // Set notify_once for REF bindings and activate if needed
            TSLink* link = parent->link_at(final_idx);
            if (link) {
                // Set notify_once for REF bindings BEFORE make_active
                // This must be done regardless of active state
                if (is_ref_binding) {
                    link->set_notify_once(true);
                }
                if (input_root.active()) {
                    link->make_active();
                }
            }
        }
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

            // Use new TSValue-based binding
            // For TS→REF conversion, we need access to intermediate output levels,
            // so we pass root output and output_path separately.
            const TSValue* root_output = nullptr;
            if (edge.output_path.size() == 1 && edge.output_path[0] == ERROR_PATH) {
                root_output = src_node->ts_error_output();
            } else if (edge.output_path.size() == 1 && edge.output_path[0] == STATE_PATH) {
                root_output = src_node->ts_recordable_state();
            } else {
                root_output = src_node->ts_output();
            }

            _bind_ts_input_to_output(dst_node, edge.input_path, root_output, edge.output_path);
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
