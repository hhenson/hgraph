#include <hgraph/nodes/switch_node.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/python_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/util/lifecycle.h>
#include <stdexcept>
#include <cstdio>

// Debug flag - set to true to enable tracing
static constexpr bool SWITCH_DEBUG = false;

namespace hgraph {

    SwitchNode::SwitchNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                           nb::dict scalars,
                           const TSMeta* input_meta, const TSMeta* output_meta,
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
    }

    void SwitchNode::initialise() {
        // SwitchNode creates graphs lazily in eval() when the key arrives.
        // No-op: nothing to do at initialise time.
    }

    void SwitchNode::do_start() {
        // Set up recordable_id if the graph has the trait
        if (has_recordable_id_trait(graph()->traits())) {
            auto record_id = signature().record_replay_id;
            _recordable_id = get_fq_recordable_id(
                graph()->traits(), record_id.has_value() ? record_id.value() : "switch_");
        }
    }

    void SwitchNode::eval() {
        mark_evaluated();

        if constexpr (SWITCH_DEBUG) {
            fprintf(stderr, "[SWITCH] eval()\n");
        }

        // 1. Read the key input
        auto input_view = ts_input()->view(graph()->evaluation_time());
        auto key_field = input_view.field("key");

        if (key_field.modified()) {
            // Read key value as Python object, then convert to PlainValue
            nb::object py_key = key_field.to_python();
            value::PlainValue new_key(_key_type);
            _key_type->ops->from_python(new_key.view().data(), py_key, _key_type);

            if (_reload_on_ticked || !_active_key.has_value() ||
                !keys_equal(_active_key->const_view(), new_key.const_view())) {

                if (_active_graph) {
                    _graph_reset = true;
                    stop_component(*_active_graph);
                    unwire_graph(_active_graph);

                    // Schedule deferred release of the old graph
                    auto old_graph = _active_graph;
                    auto old_builder = _active_graph_builder;
                    graph()->evaluation_engine()->add_before_evaluation_notification(
                        [old_graph, old_builder]() {
                            old_builder->release_instance(old_graph);
                        });
                }

                _active_key = std::move(new_key);

                // Look up builder for this key
                graph_builder_s_ptr builder;
                auto it = _nested_graph_builders->find(_active_key->const_view());
                if (it != _nested_graph_builders->end()) {
                    builder = it->second;
                } else {
                    builder = _default_graph_builder;
                }

                if (builder) {
                    _count++;
                    _active_graph_builder = builder;

                    // Build graph ID: node_id + (-_count)
                    auto graph_id = node_id();
                    graph_id.push_back(-_count);

                    // Convert key to string for label
                    std::string key_label = _key_type->ops->to_string(
                        _active_key->const_view().data(), _key_type);

                    _active_graph = builder->make_instance(graph_id, this, key_label);
                    _active_graph->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
                        graph()->evaluation_engine(),
                        std::make_shared<NestedEngineEvaluationClock>(
                            graph()->evaluation_engine_clock().get(), this)));

                    initialise_component(*_active_graph);
                    wire_graph(_active_graph);
                    start_component(*_active_graph);
                } else {
                    throw std::runtime_error(
                        "No graph defined for key " +
                        _key_type->ops->to_string(_active_key->const_view().data(), _key_type));
                }
            }
        }

        if (_active_graph) {
            if (auto nec = dynamic_cast<NestedEngineEvaluationClock*>(
                    _active_graph->evaluation_engine_clock().get())) {
                nec->reset_next_scheduled_evaluation_time();
            }

            if constexpr (SWITCH_DEBUG) {
                auto& sched = _active_graph->schedule();
                auto inner_now = *_active_graph->cached_evaluation_time_ptr();
                fprintf(stderr, "[SWITCH] about to evaluate_graph, nodes=%zu\n", _active_graph->nodes().size());
                for (size_t ni = 0; ni < _active_graph->nodes().size(); ++ni) {
                    auto n = _active_graph->nodes()[ni];
                    bool scheduled = (sched[ni] == inner_now);
                    fprintf(stderr, "[SWITCH]   inner node %zu: has_output=%d, output_forwarded=%d, has_input=%d, scheduled=%d\n",
                            ni,
                            n->ts_output() != nullptr ? 1 : 0,
                            (n->ts_output() && n->ts_output()->is_forwarded()) ? 1 : 0,
                            n->has_input() ? 1 : 0,
                            scheduled ? 1 : 0);
                }
                if (ts_output()) {
                    fprintf(stderr, "[SWITCH] outer output before eval: forwarded=%d, valid=%d\n",
                            ts_output()->is_forwarded() ? 1 : 0,
                            ts_output()->native_value().meta() != nullptr ? 1 : 0);
                }
            }

            _active_graph->evaluate_graph();

            if constexpr (SWITCH_DEBUG) {
                if (ts_output()) {
                    auto out_view = ts_output()->view(graph()->evaluation_time());
                    fprintf(stderr, "[SWITCH] outer output after eval: modified=%d, valid=%d\n",
                            out_view.modified() ? 1 : 0, out_view.valid() ? 1 : 0);
                    if (out_view.valid()) {
                        try {
                            nb::object py_val = out_view.to_python();
                            nb::str py_str(py_val);
                            fprintf(stderr, "[SWITCH] outer output value: %s\n", py_str.c_str());
                        } catch (...) {
                            fprintf(stderr, "[SWITCH] outer output value: <error reading>\n");
                        }
                    }
                }
            }

            // If graph was just reset and output exists but wasn't modified, invalidate it
            if (_graph_reset && ts_output()) {
                auto out_view = ts_output()->view(graph()->evaluation_time());
                if (!out_view.modified()) {
                    out_view.invalidate();
                }
            }

            if (auto nec = dynamic_cast<NestedEngineEvaluationClock*>(
                    _active_graph->evaluation_engine_clock().get())) {
                nec->reset_next_scheduled_evaluation_time();
            }

            _graph_reset = false;
        }
    }

    void SwitchNode::wire_graph(graph_s_ptr &graph_) {
        // Determine which key to use for map lookups (actual key or default)
        bool use_keyed = _nested_graph_builders &&
                         _nested_graph_builders->find(_active_key->const_view()) != _nested_graph_builders->end();

        // Set recordable_id on graph traits if available
        if (!_recordable_id.empty()) {
            std::string key_str = _key_type->ops->to_string(
                _active_key->const_view().data(), _key_type);
            std::string recordable_id = _recordable_id + "[" + key_str + "]";
            set_parent_recordable_id(*graph_, recordable_id);
        }

        // Get the appropriate input_node_ids and output_node_id
        const std::unordered_map<std::string, int>* input_ids = nullptr;
        int output_id = -1;

        if (use_keyed) {
            auto it = _input_node_ids->find(_active_key->const_view());
            if (it != _input_node_ids->end()) {
                input_ids = &it->second;
            }
            if (_output_node_ids) {
                auto oit = _output_node_ids->find(_active_key->const_view());
                if (oit != _output_node_ids->end()) {
                    output_id = oit->second;
                }
            }
        } else {
            input_ids = &_default_input_node_ids;
            output_id = _default_output_node_id;
        }

        // Get the appropriate graph builder for edge lookup
        const graph_builder_s_ptr& builder = use_keyed
            ? _nested_graph_builders->find(_active_key->const_view())->second
            : _default_graph_builder;

        // Wire inputs
        if (input_ids && ts_input()) {
            auto outer_input_view = ts_input()->view(this->graph()->evaluation_time());
            for (const auto &[arg, node_ndx] : *input_ids) {
                auto inner_node = graph_->nodes()[node_ndx];
                inner_node->notify();

                if (arg == "key") {
                    // The key stub is a PythonNode with KeyStubEvalFn as eval_fn.
                    // Set eval_fn.key = active_key value as Python object.
                    nb::object py_key = _key_type->ops->to_python(
                        _active_key->const_view().data(), _key_type);
                    auto* py_node = dynamic_cast<PythonNode*>(inner_node.get());
                    if (py_node) {
                        nb::object eval_fn_obj = nb::borrow(py_node->eval_fn());
                        eval_fn_obj.attr("key") = py_key;
                    }
                } else {
                    // Regular input: bind the stub's REF input to the outer input's upstream source.
                    // Python equivalent: inner_input.clone_binding(ts) where inner_input is REF-typed
                    // and ts is the outer (non-REF) input.
                    // We resolve the outer field through its link to get the upstream output data,
                    // then bind the stub's input to it. The downstream edges (stub→computation)
                    // are already set up by the graph builder and should NOT be re-bound.
                    auto field_view = outer_input_view.field(arg);

                    // Resolve outer field's ViewData through its LinkTarget to get upstream output data.
                    // IMPORTANT: Use the LinkTarget's target_path as the resolved path, not the
                    // original input's path. This ensures TSReference::make() captures a ShortPath
                    // pointing to the upstream output (not back to the switch node's input).
                    ViewData resolved = resolve_through_link(field_view.ts_view().view_data());
                    {
                        auto& vd = field_view.ts_view().view_data();
                        if (vd.uses_link_target && vd.link_data) {
                            auto* lt = static_cast<LinkTarget*>(vd.link_data);
                            if (lt->is_linked && lt->target_path.valid()) {
                                resolved.path = lt->target_path;
                            }
                        }
                    }
                    TSView resolved_target(resolved, this->graph()->evaluation_time());

                    // Bind the stub's input (which is REF-typed) to the resolved outer data
                    if (inner_node->ts_input()) {
                        auto inner_input_view = inner_node->ts_input()->view(this->graph()->evaluation_time());
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
                }
            }
        }

        // Wire output: forward inner sink node's TSOutput to outer's storage
        if (output_id >= 0 && ts_output()) {
            auto inner_node = graph_->nodes()[output_id];
            if constexpr (SWITCH_DEBUG) {
                fprintf(stderr, "[SWITCH] wire_graph: output_id=%d, inner_node has output=%d\n",
                        output_id, inner_node->ts_output() != nullptr ? 1 : 0);
            }
            if (inner_node->ts_output()) {
                // Mark that we have output forwarding set up (using shared_ptr with no-op deleter as flag)
                _old_output = std::shared_ptr<TSOutput>(inner_node->ts_output(), [](TSOutput*){});

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

                if constexpr (SWITCH_DEBUG) {
                    fprintf(stderr, "[SWITCH] wire_graph: forwarded target set up. outer_data: value=%p, time=%p, obs=%p\n",
                            outer_data.value_data, outer_data.time_data, outer_data.observer_data);
                }
            }
        }
    }

    void SwitchNode::unwire_graph(graph_s_ptr &graph_) {
        // Restore the inner sink node's output by clearing the forwarded target
        if (_old_output) {
            bool use_keyed = _nested_graph_builders &&
                             _nested_graph_builders->find(_active_key->const_view()) != _nested_graph_builders->end();
            int output_id = use_keyed
                ? (_output_node_ids ? _output_node_ids->find(_active_key->const_view())->second : -1)
                : _default_output_node_id;

            if (output_id >= 0) {
                auto inner_node = graph_->nodes()[output_id];
                if (inner_node->ts_output()) {
                    LinkTarget& ft = inner_node->ts_output()->forwarded_target();
                    ft.is_linked = false;
                }
            }
            _old_output = nullptr;
        }
    }

    void SwitchNode::do_stop() {
        if (_active_graph) {
            stop_component(*_active_graph);
        }
    }

    void SwitchNode::dispose() {
        if (_active_graph) {
            dispose_component(*_active_graph);
            if (_active_graph_builder) {
                _active_graph_builder->release_instance(_active_graph);
            }
            _active_graph = nullptr;
        }
    }

    bool SwitchNode::keys_equal(const value::View& a, const value::View& b) const {
        return _key_type->ops->equals(a.data(), b.data(), _key_type);
    }

    std::unordered_map<int, graph_s_ptr> SwitchNode::nested_graphs() const {
        if (_active_graph) {
            return {{static_cast<int>(_count), _active_graph}};
        }
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
