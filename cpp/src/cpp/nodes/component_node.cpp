#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/component_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/python/global_keys.h>
#include <hgraph/python/global_state.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/util/lifecycle.h>
#include <format>

namespace hgraph {

    ComponentNode::ComponentNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                                 nb::dict scalars, graph_builder_s_ptr nested_graph_builder,
                                 const std::unordered_map<std::string, int> &input_node_ids, int output_node_id,
                                 const TimeSeriesTypeMeta* input_meta, const TimeSeriesTypeMeta* output_meta,
                                 const TimeSeriesTypeMeta* error_output_meta, const TimeSeriesTypeMeta* recordable_state_meta)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          m_nested_graph_builder_(std::move(nested_graph_builder)), m_input_node_ids_(input_node_ids),
          m_output_node_id_(output_node_id), m_active_graph_(nullptr), m_last_evaluation_time_(std::nullopt) {
    }


    // TODO: Implement ComponentNode

    std::pair<std::string, bool> ComponentNode::recordable_id() {
        throw std::runtime_error("ComponentNode::recordable_id not yet implemented");
    }

    void ComponentNode::wire_graph() {
        throw std::runtime_error("ComponentNode::wire_graph not yet implemented");
    }

    void ComponentNode::write_inputs() {}

    void ComponentNode::wire_outputs() {}

    void ComponentNode::initialise() {
        // Stubbed - nested graph wiring not yet implemented
    }

    void ComponentNode::do_start() {
        throw std::runtime_error("ComponentNode::do_start not yet implemented");
    }

    void ComponentNode::do_stop() {
        if (m_active_graph_) {
            stop_component(*m_active_graph_);
        }
    }

    void ComponentNode::dispose() {
        if (m_active_graph_) {
            dispose_component(*m_active_graph_);
            m_active_graph_ = nullptr;
        }
    }

    void ComponentNode::do_eval() {
        throw std::runtime_error("ComponentNode::do_eval not yet implemented");
    }

    std::unordered_map<int, graph_s_ptr> ComponentNode::nested_graphs() const {
        if (m_active_graph_) {
            return {{0, m_active_graph_}};
        }
        return {};
    }

    void ComponentNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const {
        if (m_active_graph_) {
            callback(m_active_graph_);
        }
    }

} // namespace hgraph