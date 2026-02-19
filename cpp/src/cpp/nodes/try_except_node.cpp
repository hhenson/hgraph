#include <hgraph/nodes/try_except_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {
    namespace {
        engine_time_t node_time(const Node &node) {
            if (auto *et = node.cached_evaluation_time_ptr(); et != nullptr) {
                return *et;
            }
            auto g = node.graph();
            return g != nullptr ? g->evaluation_time() : MIN_DT;
        }
    }  // namespace

    void TryExceptNode::wire_outputs() {
        if (m_output_node_id_ >= 0) {
            auto node = m_active_graph_->nodes()[m_output_node_id_];
            auto outer_view = output(MIN_DT);
            auto inner_view = node->output(MIN_DT);
            if (!outer_view || !inner_view) {
                return;
            }

            auto outer_bundle = outer_view.try_as_bundle();
            if (outer_bundle.has_value()) {
                auto out_ts = outer_bundle->field("out");
                if (out_ts) {
                    out_ts.as_ts_view().bind(inner_view.as_ts_view());
                }
            } else {
                outer_view.as_ts_view().bind(inner_view.as_ts_view());
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

            // Keep outer `out` endpoint synchronized with nested output for cycles
            // where the nested output produced a concrete value.
            if (m_output_node_id_ >= 0) {
                auto node = m_active_graph_->nodes()[m_output_node_id_];
                auto inner_view = node->output(node_time(*node));
                auto outer_view = output(node_time(*this));
                if (inner_view && outer_view && inner_view.modified() && inner_view.valid()) {
                    auto outer_bundle = outer_view.try_as_bundle();
                    if (outer_bundle.has_value()) {
                        auto out_ts = outer_bundle->field("out");
                        if (out_ts) {
                            out_ts.copy_from_output(inner_view);
                        }
                    } else {
                        outer_view.copy_from_output(inner_view);
                    }
                }
            }

        } catch (const std::exception &e) {
            // Capture the exception and publish it to the error output, mirroring Python behavior
            auto err = NodeError::capture_error(e, *this, "");
            // Create a heap-allocated copy managed by nanobind
            auto error_ptr = nb::ref<NodeError>(new NodeError(err));

            auto out_view = output(node_time(*this));
            if (!out_view) {
                stop_component(*m_active_graph_);
                return;
            }
            auto bundle = out_view.try_as_bundle();
            if (bundle.has_value()) {
                // Write to the 'exception' field of the bundle
                auto exception_ts = bundle->field("exception");
                try {
                    exception_ts.from_python(nb::cast(error_ptr));
                } catch (const std::exception &set_err) {
                    exception_ts.from_python(nb::str(err.to_string().c_str()));
                }
            } else {
                // Sink case: direct TS[NodeError]
                out_view.from_python(nb::cast(error_ptr));
            }

            // Stop the nested component to mirror Python try/except behavior
            stop_component(*m_active_graph_);
        }
    }

} // namespace hgraph
