#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>

#include <hgraph/nodes/nest_graph_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/ref.h>
#include <utility>

namespace hgraph {
    NestedGraphNode::NestedGraphNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id,
                                     NodeSignature::s_ptr signature,
                                     nb::dict scalars, graph_builder_s_ptr nested_graph_builder,
                                     const std::unordered_map<std::string, int> &input_node_ids, int output_node_id,
                                     const TimeSeriesTypeMeta* input_meta, const TimeSeriesTypeMeta* output_meta,
                                     const TimeSeriesTypeMeta* error_output_meta, const TimeSeriesTypeMeta* recordable_state_meta)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          m_nested_graph_builder_(std::move(nested_graph_builder)), m_input_node_ids_(input_node_ids),
          m_output_node_id_(output_node_id), m_active_graph_(nullptr) {
    }

    void NestedGraphNode::wire_graph() {
        write_inputs();
        wire_outputs();
    }

    void NestedGraphNode::write_inputs() {
        // TODO: Requires TSInput field access and node input management
        if (!m_input_node_ids_.empty()) {
            throw std::runtime_error("NestedGraphNode::write_inputs not yet implemented");
        }
    }

    void NestedGraphNode::wire_outputs() {
        // TODO: Requires node output management
        if (m_output_node_id_ >= 0) {
            throw std::runtime_error("NestedGraphNode::wire_outputs not yet implemented");
        }
    }

    void NestedGraphNode::initialise() {
        m_active_graph_ = m_nested_graph_builder_->make_instance(node_id(), this, signature().name);
        m_active_graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(), std::make_shared<NestedEngineEvaluationClock>(graph()->evaluation_engine_clock().get(), this)));
        initialise_component(*m_active_graph_);
        wire_graph();
    }

    void NestedGraphNode::do_start() { start_component(*m_active_graph_); }

    void NestedGraphNode::do_stop() { stop_component(*m_active_graph_); }

    void NestedGraphNode::dispose() {
        if (m_active_graph_ == nullptr) { return; }
        // Release the graph back to the builder pool (which will call the dispose life-cycle)
        m_nested_graph_builder_->release_instance(m_active_graph_);
        m_active_graph_ = nullptr;
    }

    void NestedGraphNode::do_eval() {
        mark_evaluated();
        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(m_active_graph_->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }
        m_active_graph_->evaluate_graph();
        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(m_active_graph_->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }
    }

    std::unordered_map<int, graph_s_ptr> NestedGraphNode::nested_graphs() const {
        return m_active_graph_
                   ? std::unordered_map<int, graph_s_ptr>{{0, m_active_graph_}}
                   : std::unordered_map<int, graph_s_ptr>();
    }

    void NestedGraphNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const {
        if (m_active_graph_) {
            callback(m_active_graph_);
        }
    }

} // namespace hgraph