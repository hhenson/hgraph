#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>

#include <hgraph/nodes/nest_graph_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <utility>

namespace hgraph {
    namespace {
        engine_time_t node_time(const Node &node) {
            if (auto *et = node.cached_evaluation_time_ptr(); et != nullptr) {
                return *et;
            }
            auto g = node.graph();
            return g != nullptr ? g->evaluation_time() : MIN_DT;
        }
    }

    NestedGraphNode::NestedGraphNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id,
                                     NodeSignature::s_ptr signature,
                                     nb::dict scalars, const TSMeta* input_meta, const TSMeta* output_meta,
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
        // Bind inner "ts" inputs to outer mapped inputs.
        if (!m_input_node_ids_.empty()) {
            auto outer_root = input(node_time(*this));
            if (!outer_root) {
                return;
            }

            auto outer_bundle_opt = outer_root.try_as_bundle();
            if (!outer_bundle_opt.has_value()) {
                return;
            }

            for (const auto &[arg, node_ndx]: m_input_node_ids_) {
                auto node = m_active_graph_->nodes()[node_ndx];
                node->notify();

                auto outer_view = outer_bundle_opt->field(arg);
                if (!outer_view) {
                    continue;
                }

                auto inner_root = node->input(node_time(*node));
                if (!inner_root) {
                    continue;
                }
                auto inner_bundle_opt = inner_root.try_as_bundle();
                if (!inner_bundle_opt.has_value()) {
                    continue;
                }

                auto inner_ts = inner_bundle_opt->field("ts");
                if (!inner_ts && inner_bundle_opt->count() > 0) {
                    inner_ts = inner_bundle_opt->at(0);
                }
                if (!inner_ts) {
                    continue;
                }

                inner_ts.as_ts_view().bind(outer_view.as_ts_view());
            }
        }
    }

    void NestedGraphNode::wire_outputs() {
        if (m_output_node_id_ >= 0) {
            auto node = m_active_graph_->nodes()[m_output_node_id_];
            if (node != nullptr) {
                m_wired_output_node_ = node.get();
                m_wired_output_node_->set_output_override(this);
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

        if (m_wired_output_node_ != nullptr) {
            m_wired_output_node_->clear_output_override();
            m_wired_output_node_ = nullptr;
        }

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
