#include <hgraph/nodes/try_except_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {
    void TryExceptNode::wire_outputs() {
        // TryExceptNode: inner output maps to the "out" sub-field of outer bundle.
        //
        // In Python, _wire_outputs does: node.output = self.output.out
        // The inner stub's output is replaced with the outer REF field.
        //
        // In C++, we forward the inner stub's output to the outer "out" field's storage,
        // but redirect time_data to a scratch variable and suppress observer_data.
        // This prevents the outer "out" field from appearing modified or firing
        // notifications automatically during inner graph evaluation.
        //
        // In do_eval(), after inner evaluation, we check:
        // 1. Did the inner stub write? (scratch_time_ >= eval_time)
        // 2. Is the resolved reference target valid? (lmt != MIN_DT)
        // Only then do we set the "out" field's time and fire its observers.
        //
        // This mirrors Python's reference_observer semantics where downstream
        // consumers only get notified when the REFERENCED TARGET has been written,
        // not just because the reference itself changed.
        if (m_output_node_id_ >= 0 && ts_output()) {
            auto inner_node = m_active_graph_->nodes()[m_output_node_id_];
            if (inner_node->ts_output()) {
                auto outer_view = ts_output()->view(MIN_DT);
                auto out_field_view = outer_view.field("out");
                ViewData out_field_data = out_field_view.ts_view().view_data();

                // Save pointer to the outer "out" field's real time for manual propagation
                out_field_time_ptr_ = static_cast<engine_time_t*>(out_field_data.time_data);

                LinkTarget& ft = inner_node->ts_output()->forwarded_target();
                ft.is_linked = true;
                ft.value_data = out_field_data.value_data;
                ft.time_data = &scratch_time_;        // Redirect time to scratch
                ft.observer_data = nullptr;            // Suppress auto-notification
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

            // Reset scratch time before inner evaluation
            scratch_time_ = MIN_DT;

            m_active_graph_->evaluate_graph();

            // After inner graph evaluation, check whether to propagate the "out" field.
            //
            // In Python, downstream consumers bind through reference_observers to the
            // REFERENCED TARGET. They only get notified when rebinding to a target that
            // has been written (valid: lmt != MIN_DT). At tick 1 the reference points to
            // nothing's output (never written, lmt=MIN_DT) → no notification. At tick 4
            // it points to add_scalars' output (written at tick 3, lmt=3) → notification.
            if (out_field_time_ptr_ && ts_output() && m_output_node_id_ >= 0) {
                auto eval_time = graph()->evaluation_time();

                if (scratch_time_ >= eval_time) {
                    // Inner output stub wrote during this tick.
                    // Read the TSReference from the "out" field's value storage.
                    auto outer_view = ts_output()->view(eval_time);
                    auto out_field = outer_view.field("out");
                    auto vd = out_field.ts_view().view_data();

                    bool should_propagate = false;

                    if (vd.meta && vd.meta->kind == TSKind::REF && vd.value_data) {
                        auto* ref = static_cast<const TSReference*>(vd.value_data);
                        if (ref->is_peered()) {
                            // Resolve the reference to the actual target
                            try {
                                TSView resolved = ref->resolve(eval_time);
                                // Check if the resolved target has ever been written.
                                // This matches Python's reference_observer semantics:
                                // In bind_output, notification only fires if output.valid
                                // (i.e., last_modified_time != MIN_DT).
                                if (resolved && resolved.last_modified_time() != MIN_DT) {
                                    should_propagate = true;
                                }
                            } catch (...) {
                                // Resolution failed - don't propagate
                            }
                        }
                        // EMPTY or NON_PEERED references: don't propagate
                    }

                    if (should_propagate) {
                        // Set the outer "out" field's actual time
                        *out_field_time_ptr_ = eval_time;
                        // Fire the outer "out" field's observers to propagate through
                        // the binding chain (REFLink → downstream inputs → parent → record node)
                        if (vd.observer_data) {
                            auto* observers = static_cast<ObserverList*>(vd.observer_data);
                            observers->notify_modified(eval_time);
                        }
                    }
                }
            }

            if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(m_active_graph_->evaluation_engine_clock().
                get())) {
                nec->reset_next_scheduled_evaluation_time();
            }
        } catch (const std::exception &e) {
            // Capture the exception and publish it to the error output, mirroring Python behavior
            auto err = NodeError::capture_error(e, *this, "");
            // Create a heap-allocated copy managed by nanobind
            auto error_ptr = nb::ref<NodeError>(new NodeError(err));

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
            // Unbind inner graph inputs to prevent dangling observer pointers
            for (size_t ni = 0; ni < m_active_graph_->nodes().size(); ni++) {
                auto& node = m_active_graph_->nodes()[ni];
                if (node->ts_input()) {
                    ViewData vd = node->ts_input()->value().make_view_data();
                    vd.uses_link_target = true;
                    if (vd.ops && vd.ops->unbind) {
                        vd.ops->unbind(vd);
                    }
                }
            }
        }
    }

} // namespace hgraph
