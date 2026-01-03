#include "hgraph/util/string_utils.h"

#include <fmt/format.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/python_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/switch_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tsb.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {

    SwitchNode::SwitchNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                           nb::dict scalars,
                           const value::TypeMeta* key_type,
                           graph_builders_map_ptr nested_graph_builders,
                           input_node_ids_map_ptr input_node_ids,
                           output_node_ids_map_ptr output_node_ids,
                           bool reload_on_ticked,
                           graph_builder_s_ptr default_graph_builder,
                           std::unordered_map<std::string, int> default_input_node_ids,
                           int default_output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)),
          _key_type(key_type),
          _nested_graph_builders(std::move(nested_graph_builders)),
          _input_node_ids(std::move(input_node_ids)),
          _output_node_ids(std::move(output_node_ids)),
          _reload_on_ticked(reload_on_ticked),
          _default_graph_builder(std::move(default_graph_builder)),
          _default_input_node_ids(std::move(default_input_node_ids)),
          _default_output_node_id(default_output_node_id) {
        // Note: The builder extracts DEFAULT entries separately and passes them as
        // default_graph_builder, default_input_node_ids, and default_output_node_id.
        // The DEFAULT marker cannot be converted to most key types (int, str, etc.),
        // so we no longer try to find it in the map here.
    }

    void SwitchNode::initialise() {
        // Switch node doesn't create graphs upfront
        // Graphs are created dynamically in do_eval when key changes
    }

    void SwitchNode::do_start() {
        auto ts{(*input())["key"].get()};
        _key_ts = dynamic_cast<TimeSeriesValueInput*>(ts);
        if (!_key_ts) {
            throw std::runtime_error("SwitchNode requires a TimeSeriesValueInput for key input, but none found");
        }
        // Check if graph has recordable ID trait
        if (has_recordable_id_trait(graph()->traits())) {
            // NodeSignature::record_replay_id is std::optional<std::string>
            auto &record_replay_id = signature().record_replay_id;
            if (!record_replay_id.has_value() || record_replay_id.value().empty()) {
                _recordable_id = get_fq_recordable_id(graph()->traits(), "switch_");
            } else {
                _recordable_id = get_fq_recordable_id(graph()->traits(), record_replay_id.value());
            }
        }
        _initialise_inputs();
    }

    void SwitchNode::do_stop() {
        if (_active_graph != nullptr) { stop_component(*_active_graph); }
    }

    void SwitchNode::dispose() {
        if (_active_graph != nullptr) {
            _active_graph_builder->release_instance(_active_graph);
            _active_graph_builder = nullptr;
            _active_graph = nullptr;
        }
    }

    bool SwitchNode::keys_equal(const value::ConstValueView& a, const value::ConstValueView& b) const {
        if (!a.valid() || !b.valid()) return false;
        return _key_type->ops->equals(a.data(), b.data(), _key_type);
    }

    void SwitchNode::eval() {
        mark_evaluated();

        if (!_key_ts->valid()) {
            return; // No key input or invalid
        }

        // Track if we're switching graphs
        _graph_reset = false;

        // Check if key has been modified
        if (_key_ts->modified()) {
            // Extract the key value from the input time series (using Value system)
            auto current_key_view = _key_ts->value();

            // Check if key changed
            bool key_changed = !_active_key.has_value() ||
                               !keys_equal(current_key_view, _active_key->const_view());

            if (_reload_on_ticked || key_changed) {
                if (_active_key.has_value()) {
                    _graph_reset = true;
                    // Invalidate current output so stale fields (e.g., TSB members) are cleared on branch switch
                    if (output() != nullptr) { output()->invalidate(); }
                    stop_component(*_active_graph);
                    unwire_graph(_active_graph);
                    // Schedule deferred disposal via lambda capture
                    graph_s_ptr graph_to_dispose = _active_graph;
                    // Capture the builder by value for the lambda
                    auto builder = _active_graph_builder;
                    graph()->evaluation_engine()->add_before_evaluation_notification(
                        [graph_to_dispose, builder]() mutable {
                            // release_instance will call dispose_component
                            builder->release_instance(graph_to_dispose);
                        });
                    _active_graph = nullptr;
                    _active_graph_builder = nullptr;
                }

                // Clone the current key for storage
                _active_key = current_key_view.clone();

                // Find the graph builder for this key
                auto it = _nested_graph_builders->find(current_key_view);
                if (it != _nested_graph_builders->end()) {
                    _active_graph_builder = it->second;
                } else {
                    _active_graph_builder = _default_graph_builder;
                }

                if (_active_graph_builder == nullptr) {
                    throw std::runtime_error("No graph defined for key and no default available");
                }

                // Create new graph
                ++_count;
                std::vector<int64_t> new_node_id = node_id();
                new_node_id.push_back(-_count);

                // Get key string for graph label
                std::string key_str = _key_type->ops->to_string(_active_key->data(), _key_type);
                _active_graph = _active_graph_builder->make_instance(new_node_id, this, key_str);

                // Set up evaluation engine
                _active_graph->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
                    graph()->evaluation_engine(),
                    std::make_shared<NestedEngineEvaluationClock>(graph()->evaluation_engine_clock().get(), this)));

                // Initialize and wire the new graph
                initialise_component(*_active_graph);
                wire_graph(_active_graph);
                start_component(*_active_graph);
            }
        }

        // Evaluate the active graph if it exists
        if (_active_graph != nullptr) {
            if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(_active_graph->evaluation_engine_clock().get())) {
                nec->reset_next_scheduled_evaluation_time();
            }
            _active_graph->evaluate_graph();
            // Reset output to None if graph was switched and output wasn't modified
            if (_graph_reset && output() != nullptr && !output()->modified()) {
                output()->invalidate();
            }
            if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(_active_graph->evaluation_engine_clock().get())) {
                nec->reset_next_scheduled_evaluation_time();
            }
        }
    }

    void SwitchNode::wire_graph(graph_s_ptr &graph) {
        if (!_active_key.has_value()) return;

        auto active_key_view = _active_key->const_view();

        // For lookups, we use active_key_view if specific, otherwise fall back to defaults

        // Set recordable ID if needed
        if (!_recordable_id.empty()) {
            std::string key_str = _key_type->ops->to_string(_active_key->data(), _key_type);
            std::string full_id = fmt::format("{}[{}]", _recordable_id, key_str);
            set_parent_recordable_id(*graph, full_id);
        }

        // Try to find input_node_ids for the key, or fallback to defaults
        const std::unordered_map<std::string, int> *input_ids_to_use = nullptr;
        auto input_ids_it = _input_node_ids->find(active_key_view);
        if (input_ids_it != _input_node_ids->end()) {
            input_ids_to_use = &input_ids_it->second;
        } else if (!_default_input_node_ids.empty()) {
            input_ids_to_use = &_default_input_node_ids;
        }

        // Wire inputs (exactly as Python: notify each node; set key; clone REF binding for others)
        if (input_ids_to_use) {
            for (const auto &[arg, node_ndx]: *input_ids_to_use) {
                auto node = graph->nodes()[node_ndx];
                node->notify();

                if (arg == "key") {
                    // The key node is a Python stub whose eval function exposes a 'key' attribute.
                    auto &key_node = dynamic_cast<PythonNode &>(*node);
                    nb::object py_key = _key_type->ops->to_python(_active_key->data(), _key_type);
                    nb::setattr(key_node.eval_fn(), "key", py_key);
                } else {
                    // Python expects REF wiring: clone binding from outer REF input to inner REF input 'ts'
                    auto outer_any = (*input())[arg];
                    auto inner_any = (*node->input())["ts"];
                    auto inner_ref = dynamic_cast<TimeSeriesReferenceInput *>(inner_any.get());
                    auto outer_ref = dynamic_cast<TimeSeriesReferenceInput *>(outer_any.get());
                    if (!inner_ref || !outer_ref) {
                        throw std::runtime_error(
                            fmt::format("SwitchNode wire_graph expects REF inputs for arg '{}'", arg));
                    }
                    inner_ref->clone_binding(outer_ref);
                }
            }
        }

        // Wire output using the key (or default fallback)
        int output_node_id = -1;
        auto output_id_it = _output_node_ids->find(active_key_view);
        if (output_id_it != _output_node_ids->end()) {
            output_node_id = output_id_it->second;
        } else if (_default_output_node_id >= 0) {
            output_node_id = _default_output_node_id;
        }

        if (output_node_id >= 0) {
            auto node = graph->nodes()[output_node_id];
            _old_output = node->output();
            node->set_output(output());
        }
    }

    void SwitchNode::unwire_graph(graph_s_ptr &graph) {
        if (_old_output != nullptr && _active_key.has_value()) {
            auto active_key_view = _active_key->const_view();

            int output_node_id = -1;
            auto output_id_it = _output_node_ids->find(active_key_view);
            if (output_id_it != _output_node_ids->end()) {
                output_node_id = output_id_it->second;
            } else if (_default_output_node_id >= 0) {
                output_node_id = _default_output_node_id;
            }

            if (output_node_id >= 0) {
                auto node = graph->nodes()[output_node_id];
                node->set_output(_old_output);
                _old_output = nullptr;
            }
        }
    }

    std::unordered_map<int, graph_s_ptr> SwitchNode::nested_graphs() const {
        if (_active_graph != nullptr) { return {{static_cast<int>(_count), _active_graph}}; }
        return {};
    }

    void SwitchNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const {
        if (_active_graph) {
            callback(_active_graph);
        }
    }

    void register_switch_node_with_nanobind(nb::module_ &m) {
        nb::class_<SwitchNode, NestedNode>(m, "SwitchNode")
            .def_prop_ro("nested_graphs", &SwitchNode::nested_graphs);
    }
} // namespace hgraph
