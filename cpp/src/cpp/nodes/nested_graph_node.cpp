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
                                     NodeSignature::ptr signature,
                                     nb::dict scalars, graph_builder_ptr nested_graph_builder,
                                     const std::unordered_map<std::string, int> &input_node_ids, int output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)),
          m_nested_graph_builder_(std::move(nested_graph_builder)), m_input_node_ids_(input_node_ids),
          m_output_node_id_(output_node_id), m_active_graph_(nullptr) {
    }

    void NestedGraphNode::wire_graph() {
        write_inputs();
        wire_outputs();
    }

    void NestedGraphNode::write_inputs() {
        // Align with Python's PythonNestedGraphNodeImpl._write_inputs:
        // For each mapped inner node, notify it, set its input via copy_with(owning_node=node, ts=outer_ts),
        // then re-parent the outer ts to the inner node's input bundle.
        if (!m_input_node_ids_.empty()) {
            for (const auto &[arg, node_ndx]: m_input_node_ids_) {
                auto node = m_active_graph_->nodes()[node_ndx];
                node->notify();

                // Fetch the outer time-series input to be passed into the inner node as its 'ts'
                auto ts = (*input())[arg];

                // Replace the inner node's input with a copy that uses the outer ts and is owned by the inner node
                node->reset_input(node->input()->copy_with(node.get(), {ts.get()}));

                // Re-parent the provided ts so its parent container becomes the inner node's input bundle
                ts->re_parent(node->input().get());
            }
        }
    }

    void NestedGraphNode::wire_outputs() {
        if (m_output_node_id_ >= 0) {
            auto node = m_active_graph_->nodes()[m_output_node_id_];
            // Align with Python: simply replace the inner node's output with the parent node's output
            node->set_output(output());
        }
    }

    void NestedGraphNode::initialise() {
        m_active_graph_ = m_nested_graph_builder_->make_instance(node_id(), this, signature().name);
        m_active_graph_->set_evaluation_engine(new NestedEvaluationEngine(
            graph()->evaluation_engine(), new NestedEngineEvaluationClock(graph()->evaluation_engine_clock(), this)));
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

    std::unordered_map<int, graph_ptr> NestedGraphNode::nested_graphs() const {
        return m_active_graph_
                   ? std::unordered_map<int, graph_ptr>{{0, m_active_graph_}}
                   : std::unordered_map<int, graph_ptr>();
    }

    void NestedGraphNode::enumerate_nested_graphs(const std::function<void(graph_ptr)>& callback) const {
        if (m_active_graph_) {
            callback(m_active_graph_);
        }
    }

    void NestedGraphNode::register_with_nanobind(nb::module_ &m) {
        nb::class_ < NestedGraphNode, NestedNode > (m, "NestedGraphNode")
                .def_prop_ro("active_graph", [](NestedGraphNode &self) { return self.m_active_graph_; })
                .def_prop_ro("nested_graphs", &NestedGraphNode::nested_graphs);
    }
} // namespace hgraph