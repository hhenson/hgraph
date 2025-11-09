#include <hgraph/runtime/observers/evaluation_trace.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/runtime/evaluation_context.h>
#include <fmt/format.h>
#include <iostream>

namespace hgraph {

    // Static member initialization
    bool EvaluationTrace::_print_all_values = false;
    bool EvaluationTrace::_use_logger = true;

    EvaluationTrace::EvaluationTrace(const std::optional<std::string>& filter,
                                     bool start, bool eval, bool stop, bool node, bool graph)
        : _filter(filter), _start(start), _eval(eval), _stop(stop), _node(node), _graph(graph) {
    }

    void EvaluationTrace::set_print_all_values(bool value) {
        _print_all_values = value;
    }

    void EvaluationTrace::set_use_logger(bool value) {
        _use_logger = value;
    }

    void EvaluationTrace::_print(engine_time_t eval_time, const std::string& msg) const {
        auto time_us = std::chrono::duration_cast<std::chrono::microseconds>(eval_time.time_since_epoch()).count();
        std::string formatted = fmt::format("[{}] {}", time_us, msg);
        if (_use_logger) {
            // For now, just use cout. In future could integrate with a logging framework
            std::cerr << formatted << std::endl;
        } else {
            std::cout << formatted << std::endl;
        }
    }

    std::string EvaluationTrace::_graph_name(graph_ptr graph) const {
        std::vector<std::string> graph_str;
        while (graph) {
            if (graph->parent_node()) {
                auto parent = graph->parent_node();
                std::string label = parent->signature().label.has_value() 
                    ? ":" + parent->signature().label.value() 
                    : "";
                std::string ids = fmt::format("{}", fmt::join(graph->graph_id(), ", "));
                graph_str.push_back(fmt::format("{}<{}>{}", parent->signature().name, ids, label));
                graph = parent->graph();
            } else {
                graph = nullptr;
            }
        }
        std::reverse(graph_str.begin(), graph_str.end());
        return fmt::format("[{}]", fmt::join(graph_str, "::"));
    }

    void EvaluationTrace::_print_graph(graph_ptr graph, const std::string& msg) const {
        std::string parent_details = _graph_name(graph);
        _print(graph->evaluation_clock()->evaluation_time(), fmt::format("{} {}", parent_details, msg));
    }

    void EvaluationTrace::_print_signature(node_ptr node) const {
        std::string node_signature = node->signature().signature();
        _print(node->graph()->evaluation_clock()->evaluation_time(),
               fmt::format("{} Starting: {}", _graph_name(node->graph()), node_signature));
    }

    std::string EvaluationTrace::_node_name(node_ptr node) const {
        std::string label = node->signature().label.has_value() 
            ? node->signature().label.value() + ":" 
            : "";
        std::string ids = fmt::format("{}", fmt::join(node->node_id(), ", "));
        return fmt::format("[{}.{}{}<{}>(",
                          node->signature().wiring_path_name,
                          label,
                          node->signature().name,
                          ids);
    }

    void EvaluationTrace::_print_node(node_ptr node, const std::string& msg,
                                     bool add_input, bool add_output,
                                     bool add_scheduled_time) const {
        std::string node_signature = _node_name(node);
        
        // TODO: Add input/output value printing when time series access is available
        if (add_input) {
            node_signature += "...";
        } else if (node->signature().time_series_inputs.has_value() && 
                   node->signature().time_series_inputs.value().size() > 0) {
            node_signature += "...";
        }
        
        node_signature += ")";
        
        // TODO: Add output value when available
        if (add_output) {
            // node_signature += " -> <value>";
        }
        
        std::string scheduled_msg = "";
        if (add_scheduled_time) {
            // TODO: Add scheduler info when available
            // scheduled_msg = fmt::format(" SCHED[{}]", ...);
        }
        
        _print(node->graph()->evaluation_clock()->evaluation_time(),
               fmt::format("{} {} {}{}",
                          _graph_name(node->graph()),
                          node_signature,
                          msg,
                          scheduled_msg));
    }

    bool EvaluationTrace::_should_log_graph(graph_ptr graph) const {
        if (!_filter.has_value()) {
            return true;
        }
        std::string name = _graph_name(graph);
        return name.find(_filter.value()) != std::string::npos;
    }

    bool EvaluationTrace::_should_log_node(node_ptr node) const {
        if (!_filter.has_value()) {
            return true;
        }
        std::string name = _node_name(node);
        return name.find(_filter.value()) != std::string::npos;
    }

    void EvaluationTrace::on_before_start_graph(graph_ptr graph) {
        if (_start && _graph && _should_log_graph(graph)) {
            std::string label = graph->label().has_value() ? graph->label().value() : "";
            _print_graph(graph, fmt::format(">> {} Starting Graph {} {}",
                                          std::string(15, '.'),
                                          label,
                                          std::string(15, '.')));
        }
    }

    void EvaluationTrace::on_after_start_graph(graph_ptr graph) {
        if (_start && _graph && _should_log_graph(graph)) {
            _print_graph(graph, fmt::format("<< {} Started Graph {}",
                                          std::string(15, '.'),
                                          std::string(15, '.')));
        }
    }

    void EvaluationTrace::on_before_start_node(node_ptr node) {
        if (_start && _node && _should_log_node(node)) {
            _print_signature(node);
        }
    }

    void EvaluationTrace::on_after_start_node(node_ptr node) {
        if (_start && _node && _should_log_node(node)) {
            // TODO: Add input/scalar info when available
            _print_node(node, "Started node with ...", false, true);
        }
    }

    void EvaluationTrace::on_before_graph_evaluation(graph_ptr graph) {
        if (_eval && _graph && _should_log_graph(graph)) {
            std::string label = graph->label().has_value() ? graph->label().value() : "";
            _print_graph(graph, fmt::format("{} Eval Start {} {}",
                                          std::string(20, '>'),
                                          label,
                                          std::string(20, '>')));
        }
    }

    void EvaluationTrace::on_before_node_evaluation(node_ptr node) {
        if (node->signature().is_source_node()) {
            return;
        }
        if (_eval && _node && _should_log_node(node)) {
            _print_node(node, "[IN]", true);
        }
    }

    void EvaluationTrace::on_after_node_evaluation(node_ptr node) {
        if (node->signature().is_sink_node()) {
            return;
        }
        if (_eval && _node && _should_log_node(node)) {
            bool add_sched = false; // TODO: Check scheduler when available
            _print_node(node, "[OUT]", false, true, add_sched);
        }
    }

    void EvaluationTrace::on_after_graph_push_nodes_evaluation(graph_ptr graph) {
        // No-op for now - this would log after push nodes complete
    }

    void EvaluationTrace::on_after_graph_evaluation(graph_ptr graph) {
        if (_eval && _graph && _should_log_graph(graph)) {
            std::string next_scheduled = "";
            // TODO: Add next scheduled time logic when available
            _print_graph(graph, fmt::format("{} Eval Done {}{}",
                                          std::string(20, '<'),
                                          std::string(20, '<'),
                                          next_scheduled));
        }
    }

    void EvaluationTrace::on_before_stop_node(node_ptr node) {
        // No-op as in Python implementation
    }

    void EvaluationTrace::on_after_stop_node(node_ptr node) {
        if (_stop && _node && _should_log_node(node)) {
            _print_node(node, "Stopped node");
        }
    }

    void EvaluationTrace::on_before_stop_graph(graph_ptr graph) {
        if (_stop && _graph && _should_log_graph(graph)) {
            _print_graph(graph, "vvvvvvv Graph stopping -------");
        }
    }

    void EvaluationTrace::on_after_stop_graph(graph_ptr graph) {
        if (_stop && _graph && _should_log_graph(graph)) {
            _print_graph(graph, "------- Graph stopped  vvvvvvv");
        }
    }

} // namespace hgraph

