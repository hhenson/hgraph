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
        // TODO: Convert to TSInput-based approach
        // This method needs to access the "key" field from TSInputView
        // to get the TimeSeriesValueInput for the key
        // For now, stub with TODO until TSInputView field access is available

        // Check if graph has recordable ID trait
        if (has_recordable_id_trait(graph()->traits())) {
            auto &record_replay_id = signature().record_replay_id;
            if (!record_replay_id.has_value() || record_replay_id.value().empty()) {
                _recordable_id = get_fq_recordable_id(graph()->traits(), "switch_");
            } else {
                _recordable_id = get_fq_recordable_id(graph()->traits(), record_replay_id.value());
            }
        }
        _initialise_inputs();

        throw std::runtime_error("SwitchNode::do_start needs TSInput conversion for key access - TODO");
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

    bool SwitchNode::keys_equal(const value::View& a, const value::View& b) const {
        if (!a.valid() || !b.valid()) return false;
        return _key_type->ops->equals(a.data(), b.data(), _key_type);
    }

    void SwitchNode::eval() {
        mark_evaluated();

        // TODO: Convert to TSInput/TSOutput-based approach
        // This method needs:
        // 1. Key input access via TSInputView to check valid/modified
        // 2. Output invalidation via TSOutputView
        // 3. All the graph switching logic using the new approach

        // For now, just evaluate the active graph if it exists
        if (_active_graph != nullptr) {
            if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(_active_graph->evaluation_engine_clock().get())) {
                nec->reset_next_scheduled_evaluation_time();
            }
            _active_graph->evaluate_graph();
            if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(_active_graph->evaluation_engine_clock().get())) {
                nec->reset_next_scheduled_evaluation_time();
            }
        }
    }

    void SwitchNode::wire_graph(graph_s_ptr &graph) {
        // TODO: Convert to TSInput/TSOutput-based approach for cross-graph wiring
        // This method needs to:
        // 1. Access outer node inputs via TSInputView
        // 2. Wire inner node inputs from outer inputs (REF binding)
        // 3. Wire inner node output to outer node output
        // Requires inner node TSInput/TSOutput access
        throw std::runtime_error("SwitchNode::wire_graph needs TSInput/TSOutput conversion - TODO");
    }

    void SwitchNode::unwire_graph(graph_s_ptr &graph) {
        // TODO: Convert to TSOutput-based approach
        // This method restores the inner node's original output after unwiring
        // Requires inner node TSOutput access and set_output semantics
        // For now, just clear _old_output
        _old_output = nullptr;
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
