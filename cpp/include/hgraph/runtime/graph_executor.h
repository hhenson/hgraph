#ifndef GRAPH_EXECUTOR_H
#define GRAPH_EXECUTOR_H

#include <hgraph/hgraph_base.h>

namespace hgraph {
    enum class EvaluationMode { REAL_TIME = 0, SIMULATION = 1 };

    struct Graph;
    struct Node;
    struct EvaluationEngine;
    using graph_ptr = nb::ref<Graph>;
    using node_ptr = nb::ref<Node>;

    struct EvaluationLifeCycleObserver : nb::intrusive_base {
        using ptr = nb::ref<EvaluationLifeCycleObserver>;

        virtual void on_before_start_graph(Graph* graph) {
        };

        virtual void on_after_start_graph(Graph* graph) {
        };

        virtual void on_before_start_node(Node* node) {
        };

        virtual void on_after_start_node(Node* node) {
        };

        virtual void on_before_graph_evaluation(Graph* graph) {
        };

        virtual void on_after_graph_evaluation(Graph* graph) {
        };

        virtual void on_after_graph_push_nodes_evaluation(Graph* graph) {
        };

        virtual void on_before_node_evaluation(Node* node) {
        };

        virtual void on_after_node_evaluation(Node* node) {
        };

        virtual void on_before_stop_node(Node* node) {
        };

        virtual void on_after_stop_node(Node* node) {
        };

        virtual void on_before_stop_graph(Graph* graph) {
        };

        virtual void on_after_stop_graph(Graph* graph) {
        };
    };

    // struct HGRAPH_EXPORT GraphExecutor {
    //     // Abstract methods.
    //     virtual EvaluationMode run_mode() const = 0;
    //
    //     virtual void run(const engine_time_t &start_time, const engine_time_t &end_time) = 0;
    //
    //     void static register_with_nanobind(nb::module_ &m);
    // };

    struct HGRAPH_EXPORT GraphExecutor {
        GraphExecutor(graph_builder_ptr graph_builder, EvaluationMode run_mode,
                          std::vector<EvaluationLifeCycleObserver::ptr> observers = {});

        EvaluationMode run_mode() const;

        void run(const engine_time_t &start_time, const engine_time_t &end_time);

        void static register_with_nanobind(nb::module_ &m);

    protected:
        void _evaluate(EvaluationEngine &evaluationEngine, Graph& graph);

    private:
        graph_builder_ptr _graph_builder;
        EvaluationMode _run_mode;
        std::vector<EvaluationLifeCycleObserver::ptr> _observers;
    };
} // namespace hgraph
#endif  // GRAPH_EXECUTOR_H