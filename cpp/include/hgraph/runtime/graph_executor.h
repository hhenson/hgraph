#ifndef GRAPH_EXECUTOR_H
#define GRAPH_EXECUTOR_H

#include <hgraph/hgraph_base.h>
#include <memory>

namespace hgraph {
    enum class EvaluationMode { REAL_TIME = 0, SIMULATION = 1 };

    struct EvaluationEngine;  // Forward declaration

    // EvaluationLifeCycleObserver - externally managed observer, keeps nb::intrusive_base
    struct EvaluationLifeCycleObserver : nb::intrusive_base {
        using ptr = EvaluationLifeCycleObserver*;
        using s_ptr = nb::ref<EvaluationLifeCycleObserver>;

        virtual void on_before_start_graph(graph_ptr) {
        };

        virtual void on_after_start_graph(graph_ptr) {
        };

        virtual void on_before_start_node(node_ptr) {
        };

        virtual void on_after_start_node(node_ptr) {
        };

        virtual void on_before_graph_evaluation(graph_ptr) {
        };

        virtual void on_after_graph_evaluation(graph_ptr) {
        };

        virtual void on_after_graph_push_nodes_evaluation(graph_ptr) {
        };

        virtual void on_before_node_evaluation(node_ptr) {
        };

        virtual void on_after_node_evaluation(node_ptr) {
        };

        virtual void on_before_stop_node(node_ptr) {
        };

        virtual void on_after_stop_node(node_ptr) {
        };

        virtual void on_before_stop_graph(graph_ptr) {
        };

        virtual void on_after_stop_graph(graph_ptr) {
        };
    };

    struct HGRAPH_EXPORT GraphExecutor {
        GraphExecutor(graph_builder_s_ptr graph_builder, EvaluationMode run_mode,
                          std::vector<EvaluationLifeCycleObserver::s_ptr> observers = {}, bool cleanup_on_error = true);

        EvaluationMode run_mode() const;

        void run(const engine_time_t &start_time, const engine_time_t &end_time);

        void static register_with_nanobind(nb::module_ &m);

    protected:
        void _evaluate(EvaluationEngine &evaluationEngine, Graph& graph);

    private:
        graph_builder_s_ptr _graph_builder;
        EvaluationMode _run_mode;
        std::vector<EvaluationLifeCycleObserver::s_ptr> _observers;
        bool _cleanup_on_error;
    };
} // namespace hgraph
#endif  // GRAPH_EXECUTOR_H