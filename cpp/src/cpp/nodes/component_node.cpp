#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/component_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/python/global_keys.h>
#include <hgraph/python/global_state.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/util/lifecycle.h>
#include <format>

namespace hgraph {
    // Helper functions for checking time-series validity and extracting values
    // These need to handle TSReference specially

    static bool _get_ts_valid(TSInputView view) {
        if (!view.valid()) {
            return false;
        }

        auto value = view.to_python();

        // Check if it's a TSReference (new value-stack reference type)
        try {
            auto ref = nb::cast<TSReference>(value);
            // A TSReference is valid for component purposes if it's not empty
            return !ref.is_empty();
        } catch (const nb::cast_error &) {
            // Not a TSReference, that's fine
            return true;
        }
    }

    static nb::object _get_ts_value(TSInputView view) {
        auto value = view.to_python();

        // Check if it's a TSReference
        try {
            auto ref = nb::cast<TSReference>(value);
            // TSReference is a value-stack path descriptor, not a live reference.
            // Return the Python value as-is; the caller handles resolution.
            return value;
        } catch (const nb::cast_error &) {
            // Not a TSReference, return value as-is
            return value;
        }
    }

    ComponentNode::ComponentNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                                 nb::dict scalars,
                                 const TSMeta* input_meta, const TSMeta* output_meta,
                                 const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                                 graph_builder_s_ptr nested_graph_builder,
                                 const std::unordered_map<std::string, int> &input_node_ids, int output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          m_nested_graph_builder_(std::move(nested_graph_builder)), m_input_node_ids_(input_node_ids),
          m_output_node_id_(output_node_id), m_active_graph_(nullptr), m_last_evaluation_time_(std::nullopt) {
    }

    std::pair<std::string, bool> ComponentNode::recordable_id() {
        // Get outer recordable_id from graph traits
        auto outer_id_obj = graph()->traits().get_trait_or(RECORDABLE_ID_TRAIT, nb::str(""));
        auto outer_id = nb::cast<std::string>(outer_id_obj);

        // Build the full id: outer_id + "-" + record_replay_id
        std::string id_;
        auto record_id = signature().record_replay_id;
        if (!outer_id.empty() && record_id.has_value()) {
            id_ = outer_id + "-" + record_id.value();
        } else if (record_id.has_value()) {
            id_ = record_id.value();
        } else {
            id_ = "";
        }

        // Parse format string to find dependencies using std::format syntax
        // We need to extract all {key} placeholders
        std::vector<std::string> dependencies;
        size_t pos = 0;
        while ((pos = id_.find('{', pos)) != std::string::npos) {
            size_t end = id_.find('}', pos);
            if (end == std::string::npos) break;

            std::string key = id_.substr(pos + 1, end - pos - 1);
            // Check for empty format descriptor
            if (key.empty()) {
                throw std::runtime_error(
                    std::string("recordable_id: ") + id_ + " in signature: " + signature().signature() +
                    " has non-labeled format descriptors");
            }
            dependencies.push_back(key);
            pos = end + 1;
        }

        if (!dependencies.empty()) {
            // Separate scalar and time-series dependencies
            std::vector<std::string> ts_values;
            for (const auto &k: dependencies) {
                if (scalars().contains(k)) {
                    continue; // Scalar value
                }
                ts_values.push_back(k);
            }

            // Check if time-series values are ready
            if (!ts_values.empty()) {
                if (!is_started() && !is_starting()) {
                    return {id_, false}; // Not started yet, can't read ts values
                }

                // Check all ts values are valid using TSInput view
                if (!ts_input()) {
                    return {id_, false};
                }
                auto input_view = ts_input()->view(graph()->evaluation_time());
                for (const auto &k: ts_values) {
                    auto field_view = input_view.field(k);
                    if (!_get_ts_valid(field_view)) {
                        return {id_, false}; // Not all inputs valid yet
                    }
                }
            }

            // Build args map for formatting
            nb::dict args;
            auto input_view = ts_input() ? ts_input()->view(graph()->evaluation_time()) : TSInputView{};
            for (const auto &k: dependencies) {
                if (scalars().contains(k)) {
                    args[k.c_str()] = scalars()[k.c_str()];
                } else {
                    args[k.c_str()] = _get_ts_value(input_view.field(k));
                }
            }

            // Format the string - use Python's format since we're in nanobind context
            try {
                auto builtins = nb::module_::import_("builtins");
                auto str_obj = nb::str(id_.c_str());
                auto formatted = str_obj.attr("format")(**args);
                return {nb::cast<std::string>(formatted), true};
            } catch (const std::exception &e) {
                throw std::runtime_error(std::string("Error formatting recordable_id: ") + e.what());
            }
        } else {
            return {id_, true};
        }
    }

    void ComponentNode::wire_graph() {
        // Check if already wired
        if (m_active_graph_) {
            return;
        }

        // Check if recordable_id is ready
        auto [id_, ready] = recordable_id();
        if (!ready) {
            return; // Not ready yet, will try again later
        }

        // Check for duplicate component in GlobalState
        auto key = keys::component_key(id_);
        if (GlobalState::contains(key)) {
            throw std::runtime_error(
                std::string("Component[") + id_ + "] " + signature().signature() + " already exists in graph");
        }
        GlobalState::set(key, nb::bool_(true)); // Write marker

        // Create the nested graph instance
        m_active_graph_ = m_nested_graph_builder_->make_instance(node_id(), this, id_);
        m_active_graph_->traits().set_trait(RECORDABLE_ID_TRAIT, nb::cast(id_));
        m_active_graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(), std::make_shared<NestedEngineEvaluationClock>(graph()->evaluation_engine_clock().get(), this)));

        // Initialize the graph
        initialise_component(*m_active_graph_);

        // Wire inputs: For each input stub, resolve the outer field's upstream data and
        // re-bind downstream nodes to read from it directly. This emulates Python's
        // copy_with + BoundTimeSeriesReference.bind_input: downstream nodes are rebound
        // to the upstream output, bypassing the stub's REF output entirely.
        //
        // The inner graph was already wired by make_instance(), so downstream nodes'
        // LinkTargets currently point to the stub's native REF output storage. We use
        // the edge list to find downstream connections and re-bind them.
        if (ts_input()) {
            auto input_view = ts_input()->view(graph()->evaluation_time());
            for (const auto &[arg, node_ndx]: m_input_node_ids_) {
                auto inner_node = m_active_graph_->nodes()[node_ndx];
                inner_node->notify();
                auto field_view = input_view.field(arg);

                // Resolve outer field's ViewData through its LinkTarget to get upstream output data.
                ViewData resolved = resolve_through_link(field_view.ts_view().view_data());
                TSView resolved_target(resolved, graph()->evaluation_time());

                // Bind the stub's input field to the resolved upstream data.
                if (inner_node->ts_input()) {
                    auto inner_input_view = inner_node->ts_input()->view(graph()->evaluation_time());
                    const TSMeta* inner_meta = inner_node->ts_input()->meta();
                    if (inner_meta && inner_meta->kind == TSKind::TSB) {
                        for (size_t fi = 0; fi < inner_meta->field_count; ++fi) {
                            auto inner_field_view = inner_input_view[fi];
                            inner_field_view.ts_view().bind(resolved_target);
                        }
                    } else {
                        inner_input_view.ts_view().bind(resolved_target);
                    }
                }

                // Re-bind downstream nodes: find edges from this stub and re-bind
                // destination inputs to the upstream data directly.
                for (const auto& edge : m_nested_graph_builder_->edges) {
                    if (edge.src_node != node_ndx) continue;

                    auto dst_node = m_active_graph_->nodes()[edge.dst_node];
                    if (!dst_node->has_input()) continue;

                    // Navigate to the destination input field using the edge's input_path
                    TSInputView dst_input_view = dst_node->ts_input()->view(graph()->evaluation_time());
                    for (auto idx : edge.input_path) {
                        if (idx >= 0) {
                            dst_input_view = dst_input_view[static_cast<size_t>(idx)];
                        }
                    }

                    // Navigate to the source field within the resolved data using the edge's output_path
                    TSView src_view(resolved, graph()->evaluation_time());
                    for (auto idx : edge.output_path) {
                        if (idx >= 0) {
                            src_view = src_view[static_cast<size_t>(idx)];
                        }
                    }

                    // Unbind old binding first (cleans up REFBindingHelper if
                    // the original wiring bound through a REF output)
                    if (dst_input_view.ts_view().is_bound()) {
                        dst_input_view.ts_view().unbind();
                    }
                    // Re-bind the destination input to the upstream data
                    dst_input_view.ts_view().bind(src_view);
                }
            }
        }

        // Wire outputs: forward inner sink node's TSOutput to outer's storage
        if (m_output_node_id_ >= 0 && ts_output()) {
            auto inner_node = m_active_graph_->nodes()[m_output_node_id_];
            if (inner_node->ts_output()) {
                ViewData outer_data = ts_output()->native_value().make_view_data();
                LinkTarget& ft = inner_node->ts_output()->forwarded_target();
                ft.is_linked = true;
                ft.value_data = outer_data.value_data;
                ft.time_data = outer_data.time_data;
                ft.observer_data = outer_data.observer_data;
                ft.delta_data = outer_data.delta_data;
                ft.link_data = outer_data.link_data;
                ft.ops = outer_data.ops;
                ft.meta = outer_data.meta;
            }
        }

        // Start if already started
        if (is_started() || is_starting()) {
            start_component(*m_active_graph_);
        }
    }

    void ComponentNode::write_inputs() {
        // Not used in Python implementation - inputs are wired in wire_graph
    }

    void ComponentNode::wire_outputs() {
        // Not used in Python implementation - outputs are wired in wire_graph
    }

    void ComponentNode::initialise() {
        wire_graph();
    }

    void ComponentNode::do_start() {
        if (m_active_graph_) {
            start_component(*m_active_graph_);
        } else {
            wire_graph();
            if (!m_active_graph_) {
                // Still pending - reschedule
                graph()->schedule_node(node_ndx(), graph()->evaluation_time());
            }
        }
    }

    void ComponentNode::do_stop() {
        if (m_active_graph_) {
            stop_component(*m_active_graph_);
        }
    }

    void ComponentNode::dispose() {
        if (m_active_graph_) {
            auto id_ = nb::cast<std::string>(m_active_graph_->traits().get_trait(RECORDABLE_ID_TRAIT));
            GlobalState::remove(keys::component_key(id_));

            dispose_component(*m_active_graph_);
            m_active_graph_ = nullptr;
        }
    }

    void ComponentNode::do_eval() {
        if (!m_active_graph_) {
            wire_graph();
            if (!m_active_graph_) {
                // Still pending
                return;
            }
        }

        mark_evaluated();

        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(m_active_graph_->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        m_active_graph_->evaluate_graph();

        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(m_active_graph_->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }
    }

    std::unordered_map<int, graph_s_ptr> ComponentNode::nested_graphs() const {
        if (m_active_graph_) {
            return {{0, m_active_graph_}};
        } else {
            return {};
        }
    }

    void ComponentNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const {
        if (m_active_graph_) {
            callback(m_active_graph_);
        }
    }

} // namespace hgraph
