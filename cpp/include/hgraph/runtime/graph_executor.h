#ifndef GRAPH_EXECUTOR_H
#define GRAPH_EXECUTOR_H

#include <hgraph/hgraph_base.h>
#include <memory>

namespace hgraph {
    enum class HGRAPH_EXPORT EvaluationMode{REAL_TIME = 0, SIMULATION = 1};

    struct Graph;
    struct Node;
    struct EvaluationEngine;
    using graph_ptr = std::shared_ptr<Graph>;
    using node_ptr = std::shared_ptr<Node>;

    struct EvaluationLifeCycleObserver : nb::intrusive_base {
        using ptr = nb::ref<EvaluationLifeCycleObserver>;  // Not directly owned by Graph

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