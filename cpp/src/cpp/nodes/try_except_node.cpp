#include <hgraph/nodes/try_except_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/ref.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {
    void TryExceptNode::wire_outputs() {
        if (m_output_node_id_ >= 0) {
            auto node = m_active_graph_->nodes()[m_output_node_id_];
            // Python parity: set the outer REF 'out' to reference the inner node's existing output.
            // Do NOT replace the inner node's output pointer.
            // AB: why is this ^ ? I wrote a test taht fails because of this and made it set output to fix - see test_tsb_out_of_try_except
            if (auto bundle = dynamic_cast<TimeSeriesBundleOutput *>(output().get())) {
                auto out_ts = (*bundle)["out"]; // TimeSeriesOutput::ptr (expected TimeSeriesReferenceOutput)
                node->set_output(out_ts);
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

            if (auto bundle = dynamic_cast<TimeSeriesBundleOutput *>(output().get())) {
                // Write to the 'exception' field of the bundle
                auto exception_ts = (*bundle)["exception"];
                try {
                    exception_ts->py_set_value(nb::cast(error_ptr));
                } catch (const std::exception &set_err) {
                    exception_ts->py_set_value(nb::str(err.to_string().c_str()));
                }
            } else {
                // Sink case: direct TS[NodeError]
                output()->py_set_value(nb::cast(error_ptr));
            }

            // Stop the nested component to mirror Python try/except behavior
            stop_component(*m_active_graph_);
        }
    }

    void TryExceptNode::register_with_nanobind(nb::module_ &m) {
        nb::class_<TryExceptNode, NestedGraphNode>(m, "TryExceptNode");
    }
} // namespace hgraph