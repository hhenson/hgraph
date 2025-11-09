#pragma once

#include <hgraph/runtime/graph_executor.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/util/date_time.h>
#include <string>
#include <optional>

namespace hgraph {

    /**
     * @brief Logs out the different steps as the engine evaluates the graph.
     *
     * This is voluminous but can be helpful tracing down unexpected behaviour.
     * Provides detailed logging of graph execution steps including node inputs,
     * outputs, and state changes.
     */
    class EvaluationTrace : public EvaluationLifeCycleObserver {
    public:
        /**
         * @brief Construct a new Evaluation Trace object
         *
         * @param filter Used to restrict which node and graph events to report (substring match)
         * @param start Log start related events
         * @param eval Log eval related events
         * @param stop Log stop related events
         * @param node Log node related events
         * @param graph Log graph related events
         */
        explicit EvaluationTrace(const std::optional<std::string>& filter = std::nullopt,
                                bool start = true, bool eval = true, bool stop = true,
                                bool node = true, bool graph = true);

        void on_before_start_graph(graph_ptr graph) override;
        void on_after_start_graph(graph_ptr graph) override;
        void on_before_start_node(node_ptr node) override;
        void on_after_start_node(node_ptr node) override;
        void on_before_graph_evaluation(graph_ptr graph) override;
        void on_before_node_evaluation(node_ptr node) override;
        void on_after_node_evaluation(node_ptr node) override;
        void on_after_graph_push_nodes_evaluation(graph_ptr graph) override;
        void on_after_graph_evaluation(graph_ptr graph) override;
        void on_before_stop_node(node_ptr node) override;
        void on_after_stop_node(node_ptr node) override;
        void on_before_stop_graph(graph_ptr graph) override;
        void on_after_stop_graph(graph_ptr graph) override;

        // Static configuration
        static void set_print_all_values(bool value);
        static void set_use_logger(bool value);

    private:
        std::optional<std::string> _filter;
        bool _start;
        bool _eval;
        bool _stop;
        bool _node;
        bool _graph;

        static bool _print_all_values;
        static bool _use_logger;

        void _print(engine_time_t eval_time, const std::string& msg) const;
        std::string _graph_name(graph_ptr graph) const;
        void _print_graph(graph_ptr graph, const std::string& msg) const;
        void _print_signature(node_ptr node) const;
        std::string _node_name(node_ptr node) const;
        void _print_node(node_ptr node, const std::string& msg,
                        bool add_input = false, bool add_output = false,
                        bool add_scheduled_time = false) const;
        bool _should_log_graph(graph_ptr graph) const;
        bool _should_log_node(node_ptr node) const;
    };

} // namespace hgraph

