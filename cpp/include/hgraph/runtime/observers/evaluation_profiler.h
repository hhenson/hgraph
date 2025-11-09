#pragma once

#include <hgraph/runtime/graph_executor.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/util/date_time.h>
#include <string>
#include <chrono>
#include <memory>

namespace hgraph {

    /**
     * @brief Prints out some useful metrics of the running graph, can help trace down memory leaks.
     *
     * This observer tracks memory usage and prints profiling metrics during graph evaluation.
     * Takes configuration parameters to control which events are logged.
     */
    class EvaluationProfiler : public EvaluationLifeCycleObserver {
    public:
        /**
         * @brief Construct a new Evaluation Profiler object
         *
         * @param start Log start related events
         * @param eval Log eval related events
         * @param stop Log stop related events
         * @param node Log node related events
         * @param graph Log graph related events
         */
        explicit EvaluationProfiler(bool start = true, bool eval = true, bool stop = true,
                                   bool node = true, bool graph = true);

        void on_before_start_graph(graph_ptr graph) override;
        void on_after_start_graph(graph_ptr graph) override;
        void on_before_start_node(node_ptr node) override;
        void on_after_start_node(node_ptr node) override;
        void on_before_graph_evaluation(graph_ptr graph) override;
        void on_before_node_evaluation(node_ptr node) override;
        void on_after_node_evaluation(node_ptr node) override;
        void on_after_graph_evaluation(graph_ptr graph) override;
        void on_before_stop_node(node_ptr node) override;
        void on_after_stop_node(node_ptr node) override;
        void on_before_stop_graph(graph_ptr graph) override;
        void on_after_stop_graph(graph_ptr graph) override;

    private:
        bool _start;
        bool _eval;
        bool _stop;
        bool _node;
        bool _graph;
        
        // Memory tracking
        size_t _mem;
        bool _has_process_info;

        void _print(engine_time_t eval_time, const std::string& msg) const;
        std::string _graph_name(graph_ptr graph) const;
        void _print_graph(graph_ptr graph, const std::string& msg) const;
        void _print_signature(node_ptr node) const;
        void _print_node(node_ptr node, const std::string& msg) const;
        size_t _get_memory_usage() const;
    };

} // namespace hgraph

