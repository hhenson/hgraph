#include <hgraph/runtime/observers/inspection_observer.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/nodes/nested_node.h>
#include <hgraph/runtime/evaluation_context.h>
#include <algorithm>
#include <iostream>

namespace hgraph {

    GraphInfo::GraphInfo()
        : graph(nullptr), parent_graph(nullptr), stopped(false),
          node_count(0), total_subgraph_count(0), total_node_count(0),
          eval_count(0), cycle_time(0.0), os_cycle_time(0.0),
          observation_time(0.0), eval_time(0.0), os_eval_time(0.0),
          total_value_size_begin(0), total_value_size(0),
          total_size_begin(0), total_size(0), size(0) {
    }

    InspectionObserver::InspectionObserver(graph_ptr graph,
                                          NodeCallback callback_node,
                                          GraphCallback callback_graph,
                                          ProgressCallback callback_progress,
                                          double progress_interval,
                                          bool compute_sizes,
                                          bool track_recent_performance)
        : _current_graph(nullptr),
          _callback_node(std::move(callback_node)),
          _callback_graph(std::move(callback_graph)),
          _callback_progress(std::move(callback_progress)),
          _progress_interval(progress_interval),
          _progress_last_time(std::chrono::high_resolution_clock::now()),
          _compute_sizes(compute_sizes),
          _track_recent_performance(track_recent_performance),
          _recent_performance_horizon(15) {

        // Initialize recent performance tracking with rounded time
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        std::tm tm_now = *std::gmtime(&time_t_now);
        tm_now.tm_sec = 0;
        _recent_performance_batch = std::chrono::system_clock::from_time_t(std::mktime(&tm_now));

        // Initialize with first batch
        _recent_node_performance.push_back({_recent_performance_batch, {}});
        _recent_graph_performance.push_back({_recent_performance_batch, {}});

        if (graph) {
            walk(graph);
            on_before_graph_evaluation(graph);
        }
    }

    void InspectionObserver::walk(const graph_ptr& graph) {
        on_before_start_graph(graph);
        for (auto& node : graph->nodes()) {
            if (node->signature().has_nested_graphs) {
                const auto nested = static_cast<const NestedNode*>(node.get());
                nested->enumerate_nested_graphs([this](const graph_ptr& g) {
                    walk(g);
                });
            }
        }
        on_after_start_graph(graph);
    }

    void InspectionObserver::subscribe_graph(const std::vector<int>& graph_id) {
        _graph_subscriptions.insert(graph_id);
    }

    void InspectionObserver::unsubscribe_graph(const std::vector<int>& graph_id) {
        _graph_subscriptions.erase(graph_id);
    }

    void InspectionObserver::subscribe_node(const std::vector<int>& node_id) {
        _node_subscriptions.insert(node_id);
    }

    void InspectionObserver::unsubscribe_node(const std::vector<int>& node_id) {
        _node_subscriptions.erase(node_id);
    }

    GraphInfoPtr InspectionObserver::get_graph_info(const std::vector<int>& graph_id) const {
        auto it = _graphs_by_id.find(graph_id);
        if (it != _graphs_by_id.end()) {
            return it->second;
        }
        return nullptr;
    }

    void InspectionObserver::_check_progress() {
        if (_callback_progress) {
            auto now = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration<double>(now - _progress_last_time).count();
            if (elapsed > _progress_interval) {
                _progress_last_time = now;
                try {
                    _callback_progress();
                } catch (const std::exception& e) {
                    std::cerr << "Error in callback_progress: " << e.what() << std::endl;
                }
            }
        }
    }

    size_t InspectionObserver::_estimate_size(node_ptr node) const {
        if (!_compute_sizes) {
            return 0;
        }
        // TODO: Implement actual size estimation
        // This would need to traverse the node's internal structures
        return sizeof(*node);
    }

    size_t InspectionObserver::_estimate_value_size(node_ptr node) const {
        if (!_compute_sizes) {
            return 0;
        }
        // TODO: Implement actual value size estimation
        // This would need to access the output time series value
        return 0;
    }

    double InspectionObserver::_to_seconds(std::chrono::nanoseconds ns) const {
        return std::chrono::duration<double>(ns).count();
    }

    void InspectionObserver::on_before_start_graph(graph_ptr graph) {
        auto gi = std::make_shared<GraphInfo>();
        gi->graph = graph;
        auto gid = graph->graph_id();
        gi->id = std::vector<int>(gid.begin(), gid.end());
        gi->label = graph->label().has_value() ? graph->label().value() : "";
        gi->parent_graph = graph->parent_node() ? graph->parent_node()->graph().get() : nullptr;
        
        size_t node_count = graph->nodes().size();
        gi->node_count = node_count;
        gi->total_node_count = node_count;
        gi->total_subgraph_count = 0;
        
        // Initialize vectors
        gi->node_total_subgraph_counts.resize(node_count, 0);
        gi->node_total_node_counts.resize(node_count, 0);
        gi->node_eval_counts.resize(node_count, 0);
        gi->node_eval_begin_times.resize(node_count, 0.0);
        gi->node_eval_times.resize(node_count, 0.0);
        
        size_t default_size = _compute_sizes ? 0 : 0;
        gi->node_value_sizes.resize(node_count, default_size);
        gi->node_total_value_sizes_begin.resize(node_count, default_size);
        gi->node_total_value_sizes.resize(node_count, default_size);
        gi->node_total_sizes_begin.resize(node_count, default_size);
        gi->node_total_sizes.resize(node_count, default_size);
        
        if (_compute_sizes) {
            gi->size = _estimate_size(nullptr); // TODO: Estimate graph size
            gi->node_sizes.resize(node_count);
            for (size_t i = 0; i < node_count; ++i) {
                gi->node_sizes[i] = _estimate_size(graph->nodes()[i]);
            }
            gi->total_size = gi->size;
            for (auto s : gi->node_sizes) {
                gi->total_size += s;
            }
        } else {
            gi->node_sizes.resize(node_count, 0);
        }
        
        gi->eval_count = 0;
        gi->eval_begin_time = std::chrono::high_resolution_clock::now();
        
        if (_current_graph) {
            // Verify parent relationship
            // TODO: Add assertion when safe
        }

        _graphs[graph.get()] = gi;
        _graphs_by_id[gi->id] = gi;
        _current_graph = gi;

        // Update parent graph counters
        auto parent_gi = gi;
        while (parent_gi->parent_graph) {
            auto parent_graph = _graphs[parent_gi->parent_graph];
            size_t parent_node_ndx = parent_gi->graph->parent_node()->node_ndx();
            parent_graph->node_total_subgraph_counts[parent_node_ndx] += 1;
            parent_graph->node_total_node_counts[parent_node_ndx] += node_count;
            parent_graph->total_subgraph_count += 1;
            parent_graph->total_node_count += node_count;
            if (_compute_sizes) {
                parent_graph->node_total_sizes[parent_node_ndx] += gi->size;
            }
            parent_gi = parent_graph;
        }
    }

    void InspectionObserver::on_after_start_graph(graph_ptr graph) {
        if (_current_graph->parent_graph) {
            _current_graph = _graphs[_current_graph->parent_graph];
        } else {
            _current_graph = nullptr;
        }
    }

    void InspectionObserver::on_before_graph_evaluation(graph_ptr graph) {
        auto observation_begin = std::chrono::high_resolution_clock::now();

        _current_graph = _graphs[graph.get()];
        if (!_current_graph) {
            on_before_start_graph(graph);
        }
        _current_graph->eval_begin_time = std::chrono::high_resolution_clock::now();

        if (_compute_sizes) {
            _current_graph->total_value_size_begin = _current_graph->total_value_size;
            _current_graph->total_size_begin = _current_graph->total_size;
        }

        // Handle performance batch rollover for root graph
        if (_current_graph->id.empty() && _track_recent_performance) {
            auto now = std::chrono::system_clock::now();
            auto time_t_now = std::chrono::system_clock::to_time_t(now);
            std::tm tm_now = *std::gmtime(&time_t_now);
            tm_now.tm_sec = 0;
            auto batch_time = std::chrono::system_clock::from_time_t(std::mktime(&tm_now));

            if (batch_time > _recent_performance_batch) {
                _recent_performance_batch = batch_time;
                _recent_node_performance.push_back({batch_time, {}});
                while (_recent_node_performance.size() > _recent_performance_horizon) {
                    _recent_node_performance.pop_front();
                }
                _recent_graph_performance.push_back({batch_time, {}});
                while (_recent_graph_performance.size() > _recent_performance_horizon) {
                    _recent_graph_performance.pop_front();
                }
            }
        }

        // Handle dynamic node count changes
        size_t new_node_count = graph->nodes().size();
        if (new_node_count != _current_graph->node_eval_counts.size()) {
            size_t prev_node_count = _current_graph->node_eval_counts.size();
            _current_graph->node_count = new_node_count;
            
            // Resize all vectors
            _current_graph->node_eval_counts.resize(new_node_count, 0);
            _current_graph->node_eval_begin_times.resize(new_node_count, 0.0);
            _current_graph->node_eval_times.resize(new_node_count, 0.0);
            _current_graph->node_value_sizes.resize(new_node_count, 0);
            _current_graph->node_total_value_sizes_begin.resize(new_node_count, 0);
            _current_graph->node_total_value_sizes.resize(new_node_count, 0);
            _current_graph->node_sizes.resize(new_node_count, 0);
            _current_graph->node_total_sizes_begin.resize(new_node_count, 0);
            _current_graph->node_total_sizes.resize(new_node_count, 0);
            _current_graph->node_total_node_counts.resize(new_node_count, 0);
            _current_graph->node_total_subgraph_counts.resize(new_node_count, 0);
            
            // Update parent counters
            auto gi = _current_graph;
            while (gi->parent_graph) {
                auto parent_graph = _graphs[gi->parent_graph];
                size_t parent_node_ndx = gi->graph->parent_node()->node_ndx();
                parent_graph->node_total_node_counts[parent_node_ndx] += new_node_count - prev_node_count;
                parent_graph->total_node_count += new_node_count - prev_node_count;
                gi = parent_graph;
            }
        }
        
        auto observation_end = std::chrono::high_resolution_clock::now();
        _current_graph->observation_time = _to_seconds(
            std::chrono::duration_cast<std::chrono::nanoseconds>(observation_end - observation_begin));
    }

    void InspectionObserver::on_before_node_evaluation(node_ptr node) {
        auto now = std::chrono::high_resolution_clock::now();
        _current_graph->node_eval_begin_times[node->node_ndx()] = _to_seconds(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()));
        
        if (_compute_sizes) {
            _current_graph->node_total_value_sizes_begin[node->node_ndx()] = 
                _current_graph->node_total_value_sizes[node->node_ndx()];
            _current_graph->node_total_sizes_begin[node->node_ndx()] = 
                _current_graph->node_total_sizes[node->node_ndx()];
        }
    }

    void InspectionObserver::on_after_node_evaluation(node_ptr node) {
        auto observation_begin = std::chrono::high_resolution_clock::now();

        auto now = std::chrono::high_resolution_clock::now();
        double eval_time = _to_seconds(std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch())) -
                          _current_graph->node_eval_begin_times[node->node_ndx()];

        size_t ndx = node->node_ndx();
        _current_graph->node_eval_counts[ndx] += 1;
        _current_graph->node_eval_times[ndx] += eval_time;

        // Track recent node performance
        if (_track_recent_performance && !_recent_node_performance.empty()) {
            auto nid = node->node_id();
            std::vector<int> node_vec_id(nid.begin(), nid.end());
            auto& current_batch = _recent_node_performance.back().second;
            auto& recent = current_batch[node_vec_id];
            recent["eval_count"] = recent["eval_count"] + 1.0;
            recent["eval_time"] = recent["eval_time"] + eval_time;
        }

        if (!node->signature().is_source_node()) {
            _process_node_after_eval(node);
        }

        auto observation_end = std::chrono::high_resolution_clock::now();
        _current_graph->observation_time += _to_seconds(
            std::chrono::duration_cast<std::chrono::nanoseconds>(observation_end - observation_begin));
    }

    void InspectionObserver::on_after_graph_push_nodes_evaluation(graph_ptr graph) {
        auto observation_begin = std::chrono::high_resolution_clock::now();
        
        for (int64_t i = 0; i < graph->push_source_nodes_end(); ++i) {
            auto node = graph->nodes()[i];
            if (node->output()->modified()) {
                _process_node_after_eval(node);
            }
        }
        
        auto observation_end = std::chrono::high_resolution_clock::now();
        _current_graph->observation_time += _to_seconds(
            std::chrono::duration_cast<std::chrono::nanoseconds>(observation_end - observation_begin));
    }

    void InspectionObserver::_process_node_after_eval(node_ptr node) {
        size_t ndx = node->node_ndx();
        
        if (_compute_sizes) {
            size_t value_size = _estimate_value_size(node);
            size_t node_size = _estimate_size(node);
            
            _current_graph->total_value_size += 
                value_size - _current_graph->node_value_sizes[ndx] +
                _current_graph->node_total_value_sizes[ndx] -
                _current_graph->node_total_value_sizes_begin[ndx];
            
            _current_graph->total_size += 
                node_size - _current_graph->node_sizes[ndx] +
                _current_graph->node_total_sizes[ndx] -
                _current_graph->node_total_sizes_begin[ndx];
            
            _current_graph->node_value_sizes[ndx] = value_size;
            _current_graph->node_sizes[ndx] = node_size;
        }
        
        auto nid = node->node_id();
        std::vector<int> node_vec_id(nid.begin(), nid.end());
        if (_callback_node && _node_subscriptions.count(node_vec_id)) {
            try {
                _callback_node(node);
            } catch (const std::exception& e) {
                std::cerr << "Error in callback_node: " << e.what() << std::endl;
            }
        }
    }

    void InspectionObserver::on_after_graph_evaluation(graph_ptr graph) {
        auto observation_begin = std::chrono::high_resolution_clock::now();

        _current_graph->eval_count += 1;
        auto now = std::chrono::high_resolution_clock::now();
        _current_graph->cycle_time = _to_seconds(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now - _current_graph->eval_begin_time));
        _current_graph->eval_time += _current_graph->cycle_time;

        // Track recent graph performance
        if (_track_recent_performance && !_recent_graph_performance.empty()) {
            auto gid = graph->graph_id();
            std::vector<int> graph_vec_id(gid.begin(), gid.end());
            auto& current_batch = _recent_graph_performance.back().second;
            auto& recent = current_batch[graph_vec_id];
            recent["eval_count"] = recent["eval_count"] + 1.0;
            recent["eval_time"] = recent["eval_time"] + _current_graph->cycle_time;
        }

        // Update parent graph sizes
        if (!graph->graph_id().empty()) {
            auto parent_graph_id_vec = graph->parent_node()->owning_graph_id();
            std::vector<int> parent_graph_id(parent_graph_id_vec.begin(), parent_graph_id_vec.end());
            auto it = _graphs_by_id.find(parent_graph_id);
            if (it == _graphs_by_id.end()) {
                return;  // Parent graph not found
            }
            auto parent_graph = it->second;
            size_t parent_node_ndx = graph->parent_node()->node_ndx();
            
            if (_compute_sizes) {
                parent_graph->node_total_value_sizes[parent_node_ndx] +=
                    _current_graph->total_value_size - _current_graph->total_value_size_begin;
                parent_graph->node_total_sizes[parent_node_ndx] +=
                    _current_graph->total_size - _current_graph->total_size_begin;
            }
        }
        
        auto observation_end = std::chrono::high_resolution_clock::now();
        _current_graph->observation_time += _to_seconds(
            std::chrono::duration_cast<std::chrono::nanoseconds>(observation_end - observation_begin));

        double prev_observation_time = _current_graph->observation_time;

        if (_current_graph->parent_graph) {
            _current_graph = _graphs[_current_graph->parent_graph];
        } else {
            _current_graph = nullptr;
        }
        
        auto gid = graph->graph_id();
        std::vector<int> graph_vec_id(gid.begin(), gid.end());
        if (_callback_graph && _graph_subscriptions.count(graph_vec_id)) {
            try {
                _callback_graph(graph);
            } catch (const std::exception& e) {
                std::cerr << "Error in callback_graph: " << e.what() << std::endl;
            }
        }
        
        _check_progress();
        
        if (_current_graph) {
            _current_graph->observation_time += prev_observation_time;
        }
    }

    void InspectionObserver::on_after_stop_graph(graph_ptr graph) {
        auto it = _graphs.find(graph.get());
        if (it != _graphs.end()) {
            auto gi = it->second;
            gi->stopped = true;

            // Remove from both maps when graph is stopped
            _graphs_by_id.erase(gi->id);
            _graphs.erase(it);
        }
    }

    void InspectionObserver::get_recent_node_performance(const std::vector<int>& node_id,
                                                        std::vector<std::pair<std::chrono::system_clock::time_point,
                                                                    std::map<std::string, double>>>& result,
                                                        const std::optional<std::chrono::system_clock::time_point>& after) const {
        result.clear();
        result.reserve(_recent_node_performance.size());

        // Iterate in reverse order (most recent first), skipping the first batch
        for (auto it = _recent_node_performance.rbegin(); it != _recent_node_performance.rend(); ++it) {
            // Skip the first (oldest) batch which is at rend() - 1
            if (std::next(it) == _recent_node_performance.rend()) {
                break;
            }

            const auto& batch_time = it->first;
            const auto& batch_data = it->second;

            if (!after.has_value() || batch_time > after.value()) {
                auto node_it = batch_data.find(node_id);
                if (node_it != batch_data.end()) {
                    result.push_back({batch_time, node_it->second});
                }
            } else {
                break;
            }
        }
    }

    void InspectionObserver::get_recent_graph_performance(const std::vector<int>& graph_id,
                                                         std::vector<std::pair<std::chrono::system_clock::time_point,
                                                                     std::map<std::string, double>>>& result,
                                                         const std::optional<std::chrono::system_clock::time_point>& after) const {
        result.clear();
        result.reserve(_recent_graph_performance.size());

        // Iterate in reverse order (most recent first), skipping the first batch
        for (auto it = _recent_graph_performance.rbegin(); it != _recent_graph_performance.rend(); ++it) {
            // Skip the first (oldest) batch which is at rend() - 1
            if (std::next(it) == _recent_graph_performance.rend()) {
                break;
            }

            const auto& batch_time = it->first;
            const auto& batch_data = it->second;

            if (!after.has_value() || batch_time > after.value()) {
                auto graph_it = batch_data.find(graph_id);
                if (graph_it != batch_data.end()) {
                    result.push_back({batch_time, graph_it->second});
                }
            } else {
                break;
            }
        }
    }

    std::chrono::system_clock::time_point InspectionObserver::recent_performance_batch() const {
        return _recent_performance_batch;
    }

} // namespace hgraph

