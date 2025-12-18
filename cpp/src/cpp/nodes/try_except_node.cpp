#include <hgraph/nodes/try_except_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/ref.h>
#include <hgraph/util/lifecycle.h>
#include <hgraph/types/time_series/ts_python_helpers.h>

namespace hgraph {
    void TryExceptNode::wire_outputs() {
        if (m_output_node_id_ >= 0) {
            auto node = m_active_graph_->nodes()[m_output_node_id_];
            // Python parity: set the outer REF 'out' to reference the inner node's existing output.
            // Do NOT replace the inner node's output pointer.
            // TODO: V2 implementation needs proper bundle field navigation
            auto* output_ptr = output();
            if (output_ptr && output_ptr->meta() && output_ptr->meta()->ts_kind == TimeSeriesKind::TSB) {
                // Bundle case - need to navigate to 'out' field
                // TODO: Implement bundle field access via view navigation
            } else {
                // Non-bundle case (sink): nothing to wire here
            }
        }
    }

    void TryExceptNode::do_eval() {
        mark_evaluated();

        try {
            if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(m_active_graph_->evaluation_engine_clock().
                get())) {
                nec->reset_next_scheduled_evaluation_time();
            }
            m_active_graph_->evaluate_graph();
            if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(m_active_graph_->evaluation_engine_clock().
                get())) {
                nec->reset_next_scheduled_evaluation_time();
            }
        } catch (const std::exception &e) {
            // Capture the exception and publish it to the error output, mirroring Python behavior
            auto err = NodeError::capture_error(e, *this, "");
            // Create a heap-allocated copy managed by nanobind
            auto error_ptr = nb::ref<NodeError>(new NodeError(err));
            auto eval_time = graph()->evaluation_time();

            auto* output_ptr = output();
            if (output_ptr && output_ptr->meta() && output_ptr->meta()->ts_kind == TimeSeriesKind::TSB) {
                // Bundle case - write to 'exception' field
                // TODO: Implement bundle field access via view navigation
                // For now, try setting on the whole output
                try {
                    ts::set_python_value(output_ptr, nb::cast(error_ptr), eval_time);
                } catch (const std::exception &set_err) {
                    ts::set_python_value(output_ptr, nb::str(err.to_string().c_str()), eval_time);
                }
            } else {
                // Sink case: direct TS[NodeError]
                ts::set_python_value(output_ptr, nb::cast(error_ptr), eval_time);
            }

            // Stop the nested component to mirror Python try/except behavior
            stop_component(*m_active_graph_);
        }
    }

} // namespace hgraph