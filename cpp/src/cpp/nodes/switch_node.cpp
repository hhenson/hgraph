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
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {
    namespace {
        engine_time_t node_time(const Node &node) {
            if (auto *et = node.cached_evaluation_time_ptr(); et != nullptr) {
                return *et;
            }
            auto g = node.graph();
            return g != nullptr ? g->evaluation_time() : MIN_DT;
        }

        TSInputView node_input_field(Node &node, std::string_view name) {
            auto root = node.input(node_time(node));
            if (!root) {
                return {};
            }
            auto bundle_opt = root.try_as_bundle();
            if (!bundle_opt.has_value()) {
                return {};
            }
            return bundle_opt->field(name);
        }
    }  // namespace

    SwitchNode::SwitchNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                           nb::dict scalars, const TSMeta* input_meta, const TSMeta* output_meta,
                           const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                           const value::TypeMeta* key_type,
                           graph_builders_map_ptr nested_graph_builders,
                           input_node_ids_map_ptr input_node_ids,
                           output_node_ids_map_ptr output_node_ids,
                           bool reload_on_ticked,
                           graph_builder_s_ptr default_graph_builder,
                           std::unordered_map<std::string, int> default_input_node_ids,
                           int default_output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
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
        auto key_view = node_input_field(*this, "key");
        if (!key_view) {
            throw std::runtime_error("SwitchNode requires a 'key' TS input");
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
            unwire_graph(_active_graph);
        }
        if (_active_graph != nullptr) {
            _active_graph_builder->release_instance(_active_graph);
            _active_graph_builder = nullptr;
            _active_graph = nullptr;
        }
    }

    bool SwitchNode::keys_equal(const value::View& a, const value::View& b) const {
        if (!a.valid() || !b.valid()) return false;
        return _key_type->ops().equals(a.data(), b.data(), _key_type);
    }

    void SwitchNode::eval() {
        mark_evaluated();

        auto key_view = node_input_field(*this, "key");
        if (!key_view || !key_view.valid()) {
            return; // No key input or invalid
        }

        bool graph_reset = false;

        // Check if key has been modified
        if (key_view.modified()) {
            // Extract the key value from the input time series (using Value system)
            auto current_key_view = key_view.value();

            // Check if key changed
            bool key_changed = !_active_key.has_value() ||
                               !keys_equal(current_key_view, _active_key->view());

            if (_reload_on_ticked || key_changed) {
                if (_active_key.has_value()) {
                    graph_reset = true;
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
                std::string key_str = _key_type->ops().to_string(_active_key->data(), _key_type);
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

            // Mirror Python switch behavior: on graph reset, if the nested graph did
            // not produce a tick for this cycle then invalidate outer output.
            if (graph_reset) {
                auto out_view = output(node_time(*this));
                if (out_view && !out_view.modified()) {
                    out_view.invalidate();
                }
            }
            if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(_active_graph->evaluation_engine_clock().get())) {
                nec->reset_next_scheduled_evaluation_time();
            }
        }
    }

    void SwitchNode::wire_graph(graph_s_ptr &graph) {
        if (!_active_key.has_value()) return;

        auto active_key_view = _active_key->view();

        // For lookups, we use active_key_view if specific, otherwise fall back to defaults

        // Set recordable ID if needed
        if (!_recordable_id.empty()) {
            std::string key_str = _key_type->ops().to_string(_active_key->data(), _key_type);
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
                    nb::object py_key = _key_type->ops().to_python(_active_key->data(), _key_type);
                    nb::setattr(key_node.eval_fn(), "key", py_key);
                } else {
                    auto outer_any = node_input_field(*this, arg);
                    if (!outer_any) {
                        continue;
                    }
                    auto inner_any = node_input_field(*node, "ts");
                    if (!inner_any) {
                        continue;
                    }

                    const TSMeta* outer_meta = outer_any.ts_meta();
                    if (outer_meta != nullptr && outer_meta->kind == TSKind::REF) {
                        TimeSeriesReference ref = TimeSeriesReference::make();
                        value::View ref_view = outer_any.value();
                        if (ref_view.valid()) {
                            ref = nb::cast<TimeSeriesReference>(ref_view.to_python());
                        }
                        ref.bind_input(inner_any);
                        continue;
                    }

                    ViewData bound_target{};
                    if (resolve_bound_target_view_data(outer_any.as_ts_view().view_data(), bound_target)) {
                        inner_any.as_ts_view().bind(TSView(bound_target, inner_any.current_time()));
                    } else {
                        inner_any.as_ts_view().unbind();
                    }
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
            if (node != nullptr) {
                _wired_output_node = node.get();
                _wired_output_node->set_output_override(this);
            }
        }
    }

    void SwitchNode::unwire_graph(graph_s_ptr &graph) {
        (void)graph;
        if (_wired_output_node != nullptr) {
            _wired_output_node->clear_output_override();
        }
        _wired_output_node = nullptr;
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
