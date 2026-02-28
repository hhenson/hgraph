#pragma once

#include <hgraph/runtime/graph_executor.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/util/date_time.h>
#include <chrono>
#include <deque>
#include <functional>
#include <map>
#include <set>
#include <vector>

namespace hgraph {

    /**
     * @brief Hash function for std::vector<int64_t> to enable its use as key in unordered containers
     *
     * This hash implementation combines individual element hashes using a standard technique
     * that provides good distribution and low collision rates. It leverages STL's std::hash
     * for the underlying integer type.
     */
    struct VectorIntHash {
        std::size_t operator()(const std::vector<int64_t>& vec) const noexcept {
            std::size_t seed = vec.size();
            std::hash<int> hasher;
            for (const auto& element : vec) {
                // Use hash combining technique from boost::hash_combine
                seed ^= hasher(element) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            }
            return seed;
        }
    };

    /**
     * @brief Performance metrics for nodes and graphs
     */
    struct PerformanceMetrics {
        size_t eval_count = 0;
        int64_t eval_time = 0;

        PerformanceMetrics() = default;
        PerformanceMetrics(size_t count, int64_t time) : eval_count(count), eval_time(time) {}
    };

    /**
     * @brief Information collected about a graph during observation
     */
    struct GraphInfo {
        graph_ptr graph;
        std::vector<int64_t> id;
        std::string label;
        Graph* parent_graph;
        bool stopped = false;

        size_t node_count;
        size_t total_subgraph_count;
        size_t total_node_count;
        std::vector<size_t> node_total_subgraph_counts;
        std::vector<size_t> node_total_node_counts;

        size_t eval_count;
        std::chrono::time_point<std::chrono::high_resolution_clock> eval_begin_time;
        int64_t os_eval_begin_thread_time;
        int64_t cycle_time;
        int64_t os_cycle_time;
        int64_t observation_time;
        int64_t eval_time;
        int64_t os_eval_time;
        std::vector<size_t> node_eval_counts;
        std::vector<int64_t> node_eval_begin_times;
        std::vector<int64_t> node_eval_times;

        std::vector<size_t> node_value_sizes;
        std::vector<size_t> node_sizes;
        std::vector<size_t> node_total_value_sizes_begin;
        std::vector<size_t> node_total_value_sizes;
        std::vector<size_t> node_total_sizes_begin;
        std::vector<size_t> node_total_sizes;
        size_t total_value_size_begin = 0;
        size_t total_value_size = 0;
        size_t total_size_begin = 0;
        size_t total_size = 0;
        size_t size = 0;

        GraphInfo();
    };

    using GraphInfoPtr = std::shared_ptr<GraphInfo>;

    /**
     * @brief Collects comprehensive statistics about graph execution
     *
     * This observer tracks evaluation counts, timing, and optionally memory sizes
     * for all nodes and graphs. It supports callbacks for node/graph events and
     * maintains subscription system for selective monitoring.
     */
    class InspectionObserver : public EvaluationLifeCycleObserver {
    public:
        using NodeCallback = std::function<void(node_ptr)>;
        using GraphCallback = std::function<void(graph_ptr)>;
        using ProgressCallback = std::function<void()>;

        /**
         * @brief Construct a new Inspection Observer
         *
         * @param graph Optional graph to walk and initialize
         * @param callback_node Callback for node events
         * @param callback_graph Callback for graph events
         * @param callback_progress Progress callback invoked periodically
         * @param progress_interval Interval between progress callbacks (seconds)
         * @param compute_sizes Whether to compute memory sizes (expensive)
         * @param track_recent_performance Whether to track recent performance batches
         */
        explicit InspectionObserver(graph_ptr graph = nullptr,
                                   NodeCallback callback_node = nullptr,
                                   GraphCallback callback_graph = nullptr,
                                   ProgressCallback callback_progress = nullptr,
                                   double progress_interval = 0.1,
                                   bool compute_sizes = false,
                                   bool track_recent_performance = false);

        void on_before_start_graph(graph_ptr graph) override;
        void on_after_start_graph(graph_ptr graph) override;
        void on_before_graph_evaluation(graph_ptr graph) override;
        void on_before_node_evaluation(node_ptr node) override;
        void on_after_node_evaluation(node_ptr node) override;
        void on_after_graph_push_nodes_evaluation(graph_ptr graph) override;
        void on_after_graph_evaluation(graph_ptr graph) override;
        void on_after_stop_graph(graph_ptr graph) override;

        // Subscription management
        void subscribe_graph(const std::vector<int64_t>& graph_id);
        void unsubscribe_graph(const std::vector<int64_t>& graph_id);
        void subscribe_node(const std::vector<int64_t>& node_id);
        void unsubscribe_node(const std::vector<int64_t>& node_id);

        // Query methods
        GraphInfoPtr get_graph_info(const std::vector<int64_t>& graph_id) const;
        void walk(graph_ptr graph);

        // Recent performance tracking methods
        void get_recent_node_performance(const std::vector<int64_t>& node_id,
                                        std::vector<std::pair<std::chrono::system_clock::time_point,
                                                    PerformanceMetrics>>& result,
                                        const std::optional<std::chrono::system_clock::time_point>& after = std::nullopt) const;

        void get_recent_graph_performance(const std::vector<int64_t>& graph_id,
                                         std::vector<std::pair<std::chrono::system_clock::time_point,
                                                     PerformanceMetrics>>& result,
                                         const std::optional<std::chrono::system_clock::time_point>& after = std::nullopt) const;

        // Getter for recent_performance_batch
        std::chrono::system_clock::time_point recent_performance_batch() const;

    private:
        std::unordered_map<Graph*, GraphInfoPtr> _graphs;
        std::unordered_map<std::vector<int64_t>, GraphInfoPtr, VectorIntHash> _graphs_by_id;
        GraphInfoPtr _current_graph;

        NodeCallback _callback_node;
        GraphCallback _callback_graph;
        ProgressCallback _callback_progress;
        double _progress_interval;
        std::chrono::time_point<std::chrono::high_resolution_clock> _progress_last_time;
        bool _compute_sizes;

        std::unordered_map<std::vector<int64_t>, Graph*, VectorIntHash> _graph_subscriptions;
        std::unordered_set<Graph*> _subscribed_graphs;
        std::unordered_map<std::vector<int64_t>, Node*, VectorIntHash> _node_subscriptions;
        std::unordered_set<Node*> _subscribed_nodes;

        // Recent performance tracking
        typedef std::unordered_map<std::vector<int64_t>, PerformanceMetrics, VectorIntHash> perf_map;
        bool _track_recent_performance;
        std::chrono::system_clock::time_point _recent_performance_batch;
        std::deque<std::pair<std::chrono::system_clock::time_point, perf_map>> _recent_node_performance;
        std::deque<std::pair<std::chrono::system_clock::time_point, perf_map>> _recent_graph_performance;
        size_t _recent_performance_horizon;

        void _check_progress();
        void _process_node_after_eval(node_ptr node);
        size_t _estimate_size(node_ptr node) const;
        size_t _estimate_value_size(node_ptr node) const;
        int64_t _to_nanoseconds(std::chrono::nanoseconds ns) const;
    };

} // namespace hgraph

