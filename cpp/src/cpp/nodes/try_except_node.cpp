#include <hgraph/nodes/try_except_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/ref.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {
    void TryExceptNode::wire_outputs() {
        // TODO: Convert to TSOutput-based approach
        // For TryExceptNode, the output is a bundle with "out" and "exception" fields
        // The inner node's output should be bound to the "out" field
        if (m_output_node_id_ >= 0 && ts_output()) {
            auto node = m_active_graph_->nodes()[m_output_node_id_];
            // TODO: Need to implement cross-graph output binding
            // The inner node's output should write to our TSOutput's "out" field
            (void)node; // Suppress unused warning
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

            // TODO: Convert to TSOutput-based approach
            // Output is either a bundle with "exception" field, or direct TS[NodeError]
            if (ts_output()) {
                auto output_view = ts_output()->view(graph()->evaluation_time());
                const TSMeta* meta = ts_output()->ts_meta();
                if (meta && meta->kind == TSKind::TSB && meta->field_count > 0) {
                    // Bundle case: write to "exception" field
                    auto exception_view = output_view.field("exception");
                    try {
                        exception_view.from_python(nb::cast(error_ptr));
                    } catch (const std::exception &set_err) {
                        exception_view.from_python(nb::str(err.to_string().c_str()));
                    }
                } else {
                    // Sink case: direct TS[NodeError]
                    output_view.from_python(nb::cast(error_ptr));
                }
            }

            // Stop the nested component to mirror Python try/except behavior
            stop_component(*m_active_graph_);
        }
    }

} // namespace hgraph