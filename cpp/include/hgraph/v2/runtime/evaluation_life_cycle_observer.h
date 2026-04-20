//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_EVALUATION_LIFE_CYCLE_OBSERVER_H
#define HGRAPH_CPP_ROOT_EVALUATION_LIFE_CYCLE_OBSERVER_H

namespace hgraph::v2
{
    struct Graph;
    struct Node;

    struct EvaluationLifeCycleObserver
    {
        virtual void on_before_start_graph(const Graph &graph) {};

        virtual void on_after_start_graph(const Graph &graph) {};

        virtual void on_before_start_node(const Node &node) {};

        virtual void on_after_start_node(const Node &node) {};

        virtual void on_before_graph_evaluation(const Graph &graph) {};

        virtual void on_after_graph_evaluation(const Graph &graph) {};

        virtual void on_after_graph_push_nodes_evaluation(const Graph &graph) {};

        virtual void on_before_node_evaluation(const Node &node) {};

        virtual void on_after_node_evaluation(const Node &node) {};

        virtual void on_before_stop_node(const Node &node) {};

        virtual void on_after_stop_node(const Node &node) {};

        virtual void on_before_stop_graph(const Graph &graph) {};

        virtual void on_after_stop_graph(const Graph &graph) {};
    };

}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_EVALUATION_LIFE_CYCLE_OBSERVER_H
