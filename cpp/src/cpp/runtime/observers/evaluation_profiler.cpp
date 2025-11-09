#include <hgraph/runtime/observers/evaluation_profiler.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/runtime/evaluation_context.h>
#include <fmt/format.h>
#include <iostream>
#include <chrono>
#include <ctime>

#if defined(__linux__)
#include <sys/resource.h>
#include <unistd.h>
#elif defined(__APPLE__)
#include <mach/mach.h>
#elif defined(_WIN32)
#include <windows.h>
#include <psapi.h>
#endif

namespace hgraph {

    EvaluationProfiler::EvaluationProfiler(bool start, bool eval, bool stop, bool node, bool graph)
        : _start(start), _eval(eval), _stop(stop), _node(node), _graph(graph), _mem(0), _has_process_info(true) {
#if !defined(__linux__) && !defined(__APPLE__) && !defined(_WIN32)
        _has_process_info = false;
#endif
    }

    void EvaluationProfiler::_print(engine_time_t eval_time, const std::string& msg) const {
        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()) % 1000;
        
        std::tm tm;
#ifdef _WIN32
        gmtime_s(&tm, &time);
#else
        gmtime_r(&time, &tm);
#endif
        
        auto time_us = std::chrono::duration_cast<std::chrono::microseconds>(eval_time.time_since_epoch()).count();
        std::cout << fmt::format("[{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:03d}][{}] {}",
                                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                                tm.tm_hour, tm.tm_min, tm.tm_sec, ms.count(),
                                time_us, msg)
                  << std::endl;
    }

    std::string EvaluationProfiler::_graph_name(graph_ptr graph) const {
        std::vector<std::string> graph_str;
        while (graph) {
            if (graph->parent_node()) {
                auto parent = graph->parent_node();
                std::string label = parent->signature().label.has_value() 
                    ? parent->signature().label.value() + ":" 
                    : "";
                std::string ids = fmt::format("{}", fmt::join(graph->graph_id(), ", "));
                graph_str.push_back(fmt::format("{}{}<{}>", label, parent->signature().name, ids));
                graph = parent->graph();
            } else {
                graph = nullptr;
            }
        }
        std::reverse(graph_str.begin(), graph_str.end());
        return fmt::format("[{}]", fmt::join(graph_str, "::"));
    }

    void EvaluationProfiler::_print_graph(graph_ptr graph, const std::string& msg) const {
        std::string parent_details = _graph_name(graph);
        _print(graph->evaluation_clock()->evaluation_time(), fmt::format("{} {}", parent_details, msg));
    }

    void EvaluationProfiler::_print_signature(node_ptr node) const {
        std::string node_signature = node->signature().signature();
        _print(node->graph()->evaluation_clock()->evaluation_time(),
               fmt::format("{} Starting: {}", _graph_name(node->graph()), node_signature));
    }

    void EvaluationProfiler::_print_node(node_ptr node, const std::string& msg) const {
        std::string label = node->signature().label.has_value() 
            ? node->signature().label.value() + ":" 
            : "";
        std::string ids = fmt::format("{}", fmt::join(node->node_id(), ", "));
        std::string node_signature = fmt::format("[{}.{}{}<{}>(",
                                                 node->signature().wiring_path_name,
                                                 label,
                                                 node->signature().name,
                                                 ids);
        _print(node->graph()->evaluation_clock()->evaluation_time(),
               fmt::format("{} {} {}", _graph_name(node->graph()), node_signature, msg));
    }

    size_t EvaluationProfiler::_get_memory_usage() const {
        if (!_has_process_info) {
            return 0;
        }

#if defined(__linux__)
        struct rusage usage;
        getrusage(RUSAGE_SELF, &usage);
        return usage.ru_maxrss * 1024; // kilobytes to bytes
#elif defined(__APPLE__)
        struct mach_task_basic_info info;
        mach_msg_type_number_t size = MACH_TASK_BASIC_INFO_COUNT;
        kern_return_t kr = task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                                     (task_info_t)&info, &size);
        if (kr == KERN_SUCCESS) {
            return info.resident_size;
        }
        return 0;
#elif defined(_WIN32)
        PROCESS_MEMORY_COUNTERS pmc;
        if (GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc))) {
            return pmc.WorkingSetSize;
        }
        return 0;
#else
        return 0;
#endif
    }

    void EvaluationProfiler::on_before_start_graph(graph_ptr graph) {
        if (_start && _graph) {
            std::string label = graph->label().has_value() ? graph->label().value() : "";
            _print_graph(graph, fmt::format(">> {} Starting Graph {} {}",
                                          std::string(15, '.'),
                                          label,
                                          std::string(15, '.')));
        }
    }

    void EvaluationProfiler::on_after_start_graph(graph_ptr graph) {
        if (_start && _graph) {
            _print_graph(graph, fmt::format("<< {} Started Graph {}",
                                          std::string(15, '.'),
                                          std::string(15, '.')));
        }
    }

    void EvaluationProfiler::on_before_start_node(node_ptr node) {
        if (_start && _node) {
            _print_signature(node);
        }
    }

    void EvaluationProfiler::on_after_start_node(node_ptr node) {
        if (_start && _node) {
            _print_node(node, "Started node");
        }
    }

    void EvaluationProfiler::on_before_graph_evaluation(graph_ptr graph) {
        if (_eval && _graph) {
            std::string label = graph->label().has_value() ? graph->label().value() : "";
            _print_graph(graph, fmt::format("{} Eval Start {} {}",
                                          std::string(20, '>'),
                                          label,
                                          std::string(20, '>')));
        }
    }

    void EvaluationProfiler::on_before_node_evaluation(node_ptr node) {
        if (_eval && _node && _has_process_info) {
            _mem = _get_memory_usage() / (1024 * 1024); // Convert to MB
        }
    }

    void EvaluationProfiler::on_after_node_evaluation(node_ptr node) {
        if (_eval && _node && _has_process_info) {
            size_t new_mem = _get_memory_usage() / (1024 * 1024); // MB
            if (new_mem - _mem > 0) {
                _print_node(node, fmt::format("[{}, {}]", new_mem - _mem, new_mem));
            }
        }
    }

    void EvaluationProfiler::on_after_graph_evaluation(graph_ptr graph) {
        if (_eval && _graph) {
            std::string next_scheduled = "";
            // TODO: Add next scheduled time logic when available
            _print_graph(graph, fmt::format("{} Eval Done {}{}",
                                          std::string(20, '<'),
                                          std::string(20, '<'),
                                          next_scheduled));
        }
    }

    void EvaluationProfiler::on_before_stop_node(node_ptr node) {
        // No-op as in Python implementation
    }

    void EvaluationProfiler::on_after_stop_node(node_ptr node) {
        if (_stop && _node) {
            _print_node(node, "Stopped node");
        }
    }

    void EvaluationProfiler::on_before_stop_graph(graph_ptr graph) {
        if (_stop && _graph) {
            _print_graph(graph, "vvvvvvv Graph stopping -------");
        }
    }

    void EvaluationProfiler::on_after_stop_graph(graph_ptr graph) {
        if (_stop && _graph) {
            _print_graph(graph, "------- Graph stopped  vvvvvvv");
        }
    }

} // namespace hgraph

