#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>

#include <hgraph/nodes/nest_graph_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/view_data.h>
#include <utility>

namespace hgraph {
    NestedGraphNode::NestedGraphNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id,
                                     NodeSignature::s_ptr signature,
                                     nb::dict scalars,
                                     const TSMeta* input_meta, const TSMeta* output_meta,
                                     const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                                     graph_builder_s_ptr nested_graph_builder,
                                     const std::unordered_map<std::string, int> &input_node_ids, int output_node_id)
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
        // Cross-graph input wiring: bind inner stub node's TSInput to
        // the same upstream source as our input field (resolved ViewData),
        // and re-bind downstream nodes to read from upstream directly.
        if (!m_input_node_ids_.empty() && ts_input()) {
            auto input_view = ts_input()->view(graph()->evaluation_time());
            for (const auto &[arg, node_ndx]: m_input_node_ids_) {
                auto inner_node = m_active_graph_->nodes()[node_ndx];
                inner_node->notify();
                auto field_view = input_view.field(arg);

                // Resolve outer field's ViewData through its LinkTarget to get upstream output data.
                ViewData resolved = resolve_through_link(field_view.ts_view().view_data());
                TSView resolved_target(resolved, graph()->evaluation_time());

                if (inner_node->ts_input()) {
                    auto inner_input_view = inner_node->ts_input()->view(graph()->evaluation_time());
                    const TSMeta* inner_meta = inner_node->ts_input()->meta();
                    if (inner_meta && inner_meta->kind == TSKind::TSB) {
                        for (size_t fi = 0; fi < inner_meta->field_count; ++fi) {
                            auto inner_field_view = inner_input_view[fi];
                            inner_field_view.ts_view().bind(resolved_target);
                        }
                    } else {
                        inner_input_view.ts_view().bind(resolved_target);
                    }
                }

                // Re-bind downstream nodes: find edges from this stub and re-bind
                // destination inputs to the upstream data directly.
                for (const auto& edge : m_nested_graph_builder_->edges) {
                    if (edge.src_node != node_ndx) continue;

                    auto dst_node = m_active_graph_->nodes()[edge.dst_node];
                    if (!dst_node->has_input()) continue;

                    TSInputView dst_input_view = dst_node->ts_input()->view(graph()->evaluation_time());
                    for (auto idx : edge.input_path) {
                        if (idx >= 0) {
                            dst_input_view = dst_input_view[static_cast<size_t>(idx)];
                        }
                    }

                    TSView src_view(resolved, graph()->evaluation_time());
                    for (auto idx : edge.output_path) {
                        if (idx >= 0) {
                            src_view = src_view[static_cast<size_t>(idx)];
                        }
                    }

                    dst_input_view.ts_view().bind(src_view);
                }
            }
        }
    }

    void NestedGraphNode::wire_outputs() {
        // Cross-graph output wiring: forward inner sink node's TSOutput
        // to write into this node's TSOutput storage
        if (m_output_node_id_ >= 0 && ts_output()) {
            auto inner_node = m_active_graph_->nodes()[m_output_node_id_];
            if (inner_node->ts_output()) {
                ViewData outer_data = ts_output()->native_value().make_view_data();
                LinkTarget& ft = inner_node->ts_output()->forwarded_target();
                ft.is_linked = true;
                ft.value_data = outer_data.value_data;
                ft.time_data = outer_data.time_data;
                ft.observer_data = outer_data.observer_data;
                ft.delta_data = outer_data.delta_data;
                ft.link_data = outer_data.link_data;
                ft.ops = outer_data.ops;
                ft.meta = outer_data.meta;
            }
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