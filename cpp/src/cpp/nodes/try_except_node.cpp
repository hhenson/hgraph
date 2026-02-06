#include <hgraph/nodes/try_except_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {
    void TryExceptNode::wire_outputs() {
        // TryExceptNode: inner output maps to the "out" sub-field of outer bundle
        if (m_output_node_id_ >= 0 && ts_output()) {
            auto inner_node = m_active_graph_->nodes()[m_output_node_id_];
            if (inner_node->ts_output()) {
                // Navigate to outer output's "out" field
                auto outer_view = ts_output()->view(MIN_DT);
                auto out_field_view = outer_view.field("out");
                ViewData out_field_data = out_field_view.ts_view().view_data();

                LinkTarget& ft = inner_node->ts_output()->forwarded_target();
                ft.is_linked = true;
                ft.value_data = out_field_data.value_data;
                ft.time_data = out_field_data.time_data;
                ft.observer_data = out_field_data.observer_data;
                ft.delta_data = out_field_data.delta_data;
                ft.link_data = out_field_data.link_data;
                ft.ops = out_field_data.ops;
                ft.meta = out_field_data.meta;
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