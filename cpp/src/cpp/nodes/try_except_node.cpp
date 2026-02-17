#include <hgraph/nodes/try_except_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/short_path.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {

    void TryExceptNode::wire_outputs() {
        // Do NOT use forwarded_target here.
        // In Python, _wire_outputs does: node.output = self.output.out
        // This replaces the inner output stub's output object with the outer "out" field.
        // In C++, we can't replace objects, so we manually copy results after evaluation
        // in do_eval(), only when the inner graph actually produced meaningful output.
    }

    // Check if a REF's ultimate target is valid at the given time.
    // Resolves through nested REF chains.
    // Returns true if the REF points to a valid (ever-set) target, false if empty/invalid.
    static bool ref_target_is_valid(const ViewData& vd, engine_time_t et) {
        if (!vd.meta || vd.meta->kind != TSKind::REF || !vd.value_data) {
            // Not a REF — always valid if it has data
            return true;
        }

        auto& ref = *static_cast<const TSReference*>(vd.value_data);
        if (ref.is_empty()) {
            return false;
        }
        if (!ref.is_peered()) {
            return true;
        }

        // Resolve the peered REF to its target
        TSView target = ref.path().resolve(et);

        // Resolve through nested REFs
        while (target.view_data().meta && target.view_data().meta->kind == TSKind::REF) {
            const ViewData& target_vd = target.view_data();
            if (target_vd.value_data) {
                auto& inner_ref = *static_cast<const TSReference*>(target_vd.value_data);
                if (inner_ref.is_peered()) {
                    target = inner_ref.path().resolve(et);
                } else if (inner_ref.is_empty()) {
                    return false;
                } else {
                    break;
                }
            } else {
                return false;
            }
        }

        return target.valid();
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

            // After inner graph evaluates, check if the inner output stub produced
            // meaningful output. The inner output stub's output is a REF that always
            // gets written (because valid_inputs is empty). But we only want to
            // propagate to the outer "out" field when the REF's ultimate target
            // is actually valid and modified.
            if (m_output_node_id_ >= 0 && ts_output()) {
                auto inner_node = m_active_graph_->nodes()[m_output_node_id_];
                if (inner_node->ts_output()) {
                    auto et = graph()->evaluation_time();
                    auto inner_view = inner_node->ts_output()->view(et);

                    if (inner_view.modified()) {
                        // The inner output stub was modified. Check if its REF target
                        // is valid (i.e., points to a real value). We don't check
                        // modified because the target may have been set in a prior tick
                        // but the REF switched to point to it just now.
                        const ViewData& inner_vd = inner_view.view_data();

                        if (ref_target_is_valid(inner_vd, et)) {
                            // Copy the inner output stub's value to the outer "out" field
                            auto output_view = ts_output()->view(et);
                            auto out_field = output_view.field("out");
                            nb::object inner_py = inner_view.to_python();
                            out_field.from_python(inner_py);
                        }
                    }
                }
            }
        } catch (const std::exception &e) {
            // Capture the exception and publish it to the error output
            auto err = NodeError::capture_error(e, *this, "");
            auto error_ptr = nb::ref<NodeError>(new NodeError(err));

            if (ts_output()) {
                auto output_view = ts_output()->view(graph()->evaluation_time());
                const TSMeta* meta = ts_output()->ts_meta();
                if (meta && meta->kind == TSKind::TSB && meta->field_count > 0) {
                    auto exception_view = output_view.field("exception");
                    try {
                        exception_view.from_python(nb::cast(error_ptr));
                    } catch (const std::exception &set_err) {
                        exception_view.from_python(nb::str(err.to_string().c_str()));
                    }
                } else {
                    output_view.from_python(nb::cast(error_ptr));
                }
            }

            stop_component(*m_active_graph_);
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
