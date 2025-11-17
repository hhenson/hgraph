#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/component_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/python/global_keys.h>
#include <hgraph/python/global_state.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/tsb.h>
#include <hgraph/util/lifecycle.h>
#include <format>

namespace hgraph {
    // Helper functions for checking time-series validity and extracting values
    // These need to handle TimeSeriesReference specially

    static bool _get_ts_valid(const time_series_input_ptr &ts) {
        if (!ts->valid()) {
            return false;
        }

        auto value = ts->py_value();

        // Check if it's a TimeSeriesReference using nanobind's isinstance
        // In Python: TimeSeriesReference.is_instance(value)
        try {
            auto ref = nb::cast<TimeSeriesReference>(value);
            return ref.is_bound() && ref.output()->valid();
        } catch (const nb::cast_error &) {
            // Not a TimeSeriesReference, that's fine
            return true;
        }
    }

    static nb::object _get_ts_value(const time_series_input_ptr &ts) {
        auto value = ts->py_value();

        // Check if it's a TimeSeriesReference
        try {
            auto ref = nb::cast<TimeSeriesReference>(value);
            // Must have output and it must be valid
            if (ref.is_bound()) {
                return ref.output()->py_value();
            }
            return value;
        } catch (const nb::cast_error &) {
            // Not a TimeSeriesReference, return value as-is
            return value;
        }
    }

    ComponentNode::ComponentNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                                 nb::dict scalars, graph_builder_ptr nested_graph_builder,
                                 const std::unordered_map<std::string, int> &input_node_ids, int output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)),
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

                // Check all ts values are valid
                // Use input() to get the bundle, then access individual inputs
                auto input_bundle = input();
                for (const auto &k: ts_values) {
                    auto ts = (*input_bundle)[k];
                    if (!_get_ts_valid(ts)) {
                        return {id_, false}; // Not all inputs valid yet
                    }
                }
            }

            // Build args map for formatting
            nb::dict args;
            auto input_bundle = input();
            for (const auto &k: dependencies) {
                if (scalars().contains(k)) {
                    args[k.c_str()] = scalars()[k.c_str()];
                } else {
                    args[k.c_str()] = _get_ts_value((*input_bundle)[k]);
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
        m_active_graph_->set_evaluation_engine(new NestedEvaluationEngine(
            graph()->evaluation_engine(), new NestedEngineEvaluationClock(graph()->evaluation_engine_clock(), this)));

        // Initialize the graph
        initialise_component(*m_active_graph_);

        // Wire inputs
        auto input_bundle = input();
        for (const auto &[arg, node_ndx]: m_input_node_ids_) {
            auto node = m_active_graph_->nodes()[node_ndx];
            node->notify();

            auto ts = (*input_bundle)[arg];
            // Copy input with new parent
            node->reset_input(node->input()->copy_with(node.get(), {ts.get()}));

            // Re-parent the ts input
            ts->re_parent(node->input().get());
        }

        // Wire outputs
        if (m_output_node_id_ >= 0) {
            auto node = m_active_graph_->nodes()[m_output_node_id_];
            node->set_output(output());
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
                graph()->schedule_node(node_ndx(), graph()->evaluation_clock()->evaluation_time());
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

    std::unordered_map<int, graph_ptr> ComponentNode::nested_graphs() const {
        if (m_active_graph_) {
            return {{0, m_active_graph_}};
        } else {
            return {};
        }
    }

    void ComponentNode::enumerate_nested_graphs(const std::function<void(graph_ptr)>& callback) const {
        if (m_active_graph_) {
            callback(m_active_graph_);
        }
    }

    void ComponentNode::register_with_nanobind(nb::module_ &m) {
        nb::class_ < ComponentNode, NestedNode > (m, "ComponentNode")
                .def_prop_ro("active_graph", [](ComponentNode &self) { return self.m_active_graph_; })
                .def_prop_ro("nested_graphs", &ComponentNode::nested_graphs);
    }
} // namespace hgraph