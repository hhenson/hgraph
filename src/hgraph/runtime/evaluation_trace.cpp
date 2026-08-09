#include <hgraph/runtime/evaluation_trace.h>

#include <hgraph/runtime/diagnostic_path.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/logger.h>
#include <hgraph/runtime/node.h>

#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <utility>

namespace hgraph
{
    std::atomic_bool EvaluationTrace::print_all_values_{false};
    std::atomic_bool EvaluationTrace::use_logger_{true};

    namespace
    {
        [[nodiscard]] std::string format_time(DateTime when)
        {
            if (when == MIN_DT) { return "MIN_DT"; }
            if (when == MAX_DT) { return "MAX_DT"; }

            using namespace std::chrono;
            const auto day = floor<days>(when);
            const year_month_day date{day};
            const hh_mm_ss<TimeDelta> time{when - day};

            std::ostringstream out;
            out << std::setfill('0') << std::setw(4) << static_cast<int>(date.year()) << '-'
                << std::setw(2) << static_cast<unsigned>(date.month()) << '-'
                << std::setw(2) << static_cast<unsigned>(date.day()) << ' '
                << std::setw(2) << time.hours().count() << ':'
                << std::setw(2) << time.minutes().count() << ':'
                << std::setw(2) << time.seconds().count();
            const auto micros = duration_cast<microseconds>(time.subseconds()).count();
            if (micros != 0) { out << '.' << std::setw(6) << micros; }
            return out.str();
        }

        [[nodiscard]] std::string schema_name(const TSValueTypeMetaData *schema)
        {
            return schema != nullptr && !schema->name().empty() ? std::string{schema->name()} : std::string{"?"};
        }

        [[nodiscard]] std::string input_value(const NodeView &node, bool &modified, bool print_all_values)
        {
            try
            {
                auto input = node.input(node.graph().evaluation_time());
                if (!input.bound() || !input.valid()) { return "<UnSet>"; }
                modified = input.modified();
                if (!modified && !print_all_values) { return {}; }
                return input.delta_value().to_string();
            }
            catch (const std::exception &error)
            {
                return std::string{"<Unavailable: "} + error.what() + '>';
            }
        }

        [[nodiscard]] std::string output_value(const NodeView &node, bool &modified)
        {
            try
            {
                auto output = node.output(node.graph().evaluation_time());
                if (!output.bound() || !output.valid()) { return "<UnSet>"; }
                modified = output.modified();
                return (modified ? output.delta_value() : output.value()).to_string();
            }
            catch (const std::exception &error)
            {
                return std::string{"<Unavailable: "} + error.what() + '>';
            }
        }
    }  // namespace

    EvaluationTrace::EvaluationTrace(EvaluationTraceOptions options, OutputSink output)
        : options_(std::move(options)), output_(std::move(output))
    {
    }

    EvaluationTrace::EvaluationTrace(std::optional<std::string> filter, bool start, bool eval,
                                     bool stop, bool node, bool graph)
        : EvaluationTrace(EvaluationTraceOptions{
              .filter = std::move(filter),
              .start = start,
              .eval = eval,
              .stop = stop,
              .node = node,
              .graph = graph,
          })
    {
    }

    void EvaluationTrace::set_print_all_values(bool value) noexcept
    {
        print_all_values_.store(value, std::memory_order_relaxed);
    }

    void EvaluationTrace::set_use_logger(bool value) noexcept
    {
        use_logger_.store(value, std::memory_order_relaxed);
    }

    std::string EvaluationTrace::graph_name(const GraphView &graph) const
    {
        return diagnostic::graph_path(graph);
    }

    std::string EvaluationTrace::node_name(const NodeView &node) const
    {
        return diagnostic::node_path(node);
    }

    bool EvaluationTrace::should_log_graph(const GraphView &graph) const
    {
        if (!options_.filter.has_value()) { return true; }
        return graph_name(graph).contains(*options_.filter) ||
               diagnostic::graph_label(graph).contains(*options_.filter);
    }

    bool EvaluationTrace::should_log_node(const NodeView &node) const
    {
        return !options_.filter.has_value() || node_name(node).contains(*options_.filter);
    }

    void EvaluationTrace::emit(DateTime evaluation_time, spdlog::logger *logger,
                               std::string message) const
    {
        std::string line = '[' + format_time(evaluation_time) + "] " + std::move(message);
        if (output_)
        {
            output_(line);
            return;
        }
        if (use_logger_.load(std::memory_order_relaxed))
        {
            LoggerView{logger}.info("{}", line);
            return;
        }
        std::cout << line << '\n' << std::flush;
    }

    void EvaluationTrace::print_graph(const GraphView &graph, std::string message) const
    {
        emit(graph.evaluation_time(), graph.logger(),
             graph_name(graph) + ' ' + std::move(message));
    }

    void EvaluationTrace::print_node(const NodeView &node, std::string_view message,
                                     bool add_input, bool add_output,
                                     bool add_scheduled_time) const
    {
        std::string signature = node_name(node) + '(';
        if (node.has_input())
        {
            if (add_input)
            {
                bool modified = false;
                const std::string value = input_value(
                    node, modified, print_all_values_.load(std::memory_order_relaxed));
                signature += modified ? "*inputs*=" : "inputs=";
                signature += value;
            }
            else { signature += "..."; }
        }
        signature += ')';

        if (add_output && node.has_output())
        {
            bool modified = false;
            const std::string value = output_value(node, modified);
            signature += modified ? " *->* " : " -> ";
            signature += value;
        }

        signature += ' ';
        signature += message;
        if (add_scheduled_time && node.has_scheduler())
        {
            GraphView graph = node.graph();
            const DateTime when = graph.node_scheduled_time(node.node_index());
            if (when > graph.evaluation_time() && when < MAX_DT)
            {
                signature += " SCHED[" + format_time(when) + ']';
            }
        }
        GraphView graph = node.graph();
        emit(graph.evaluation_time(), graph.logger(), std::move(signature));
    }

    void EvaluationTrace::on_before_start_graph(const GraphView &graph)
    {
        if (options_.start && options_.graph && should_log_graph(graph))
        {
            print_graph(graph, ">> ............... Starting Graph " +
                                   diagnostic::graph_label(graph) +
                                   " ...............");
        }
    }

    void EvaluationTrace::on_after_start_graph(const GraphView &graph)
    {
        if (options_.start && options_.graph && should_log_graph(graph))
        {
            print_graph(graph, "<< ............... Started Graph ...............");
        }
    }

    void EvaluationTrace::on_before_start_node(const NodeView &node)
    {
        if (!options_.start || !options_.node || !should_log_node(node)) { return; }
        const auto *schema = node.schema();
        std::string signature = diagnostic::node_label(node) + '(';
        if (schema != nullptr && schema->input_schema != nullptr)
        {
            signature += schema_name(schema->input_schema);
        }
        signature += ')';
        if (schema != nullptr && schema->output_schema != nullptr)
        {
            signature += " -> " + schema_name(schema->output_schema);
        }
        GraphView graph = node.graph();
        emit(graph.evaluation_time(), graph.logger(),
             graph_name(graph) + " Starting: " + signature);
    }

    void EvaluationTrace::on_after_start_node(const NodeView &node)
    {
        if (!options_.start || !options_.node || !should_log_node(node)) { return; }
        std::string message{"Started node"};
        if (node.has_scalars())
        {
            const ValueView scalars = node.scalars();
            if (scalars.valid()) { message += " with scalars=" + scalars.to_string(); }
        }
        print_node(node, message, false, true);
    }

    void EvaluationTrace::on_before_graph_evaluation(const GraphView &graph)
    {
        if (options_.eval && options_.graph && should_log_graph(graph))
        {
            print_graph(graph, ">>>>>>>>>>>>>>>>>>>> Eval Start " +
                                   diagnostic::graph_label(graph) +
                                   " >>>>>>>>>>>>>>>>>>>>");
        }
    }

    void EvaluationTrace::on_after_graph_evaluation(const GraphView &graph)
    {
        if (!options_.eval || !options_.graph || !should_log_graph(graph)) { return; }
        std::string message{"<<<<<<<<<<<<<<<<<<<< Eval Done <<<<<<<<<<<<<<<<<<<<"};
        const DateTime next = graph.next_scheduled_time();
        if (next > graph.evaluation_time() && next < MAX_DT)
        {
            message += " NEXT[" + format_time(next) + ']';
        }
        print_graph(graph, std::move(message));
    }

    void EvaluationTrace::on_before_node_evaluation(const NodeView &node)
    {
        if (node.node_kind() == NodeKind::PullSource || node.node_kind() == NodeKind::PushSource) { return; }
        if (options_.eval && options_.node && should_log_node(node))
        {
            print_node(node, "[IN]", true, false);
        }
    }

    void EvaluationTrace::on_after_node_evaluation(const NodeView &node)
    {
        if (node.node_kind() == NodeKind::Sink) { return; }
        if (options_.eval && options_.node && should_log_node(node))
        {
            print_node(node, "[OUT]", false, true, true);
        }
    }

    void EvaluationTrace::on_before_stop_node(const NodeView &) {}

    void EvaluationTrace::on_after_stop_node(const NodeView &node)
    {
        if (options_.stop && options_.node && should_log_node(node))
        {
            print_node(node, "Stopped node");
        }
    }

    void EvaluationTrace::on_before_stop_graph(const GraphView &graph)
    {
        if (options_.stop && options_.graph && should_log_graph(graph))
        {
            print_graph(graph, "vvvvvvv Graph stopping -------");
        }
    }

    void EvaluationTrace::on_after_stop_graph(const GraphView &graph)
    {
        if (options_.stop && options_.graph && should_log_graph(graph))
        {
            print_graph(graph, "------- Graph stopped  vvvvvvv");
        }
    }
}  // namespace hgraph
