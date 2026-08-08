#ifndef HGRAPH_RUNTIME_EVALUATION_TRACE_H
#define HGRAPH_RUNTIME_EVALUATION_TRACE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/lifecycle_observer.h>
#include <hgraph/util/date_time.h>

#include <atomic>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

namespace spdlog
{
    class logger;
}

namespace hgraph
{
    /** Select which lifecycle events an :cpp:class:`EvaluationTrace` emits. */
    struct HGRAPH_EXPORT EvaluationTraceOptions
    {
        /** Optional substring matched against graph and node paths. */
        std::optional<std::string> filter{};
        bool                       start{true};
        bool                       eval{true};
        bool                       stop{true};
        bool                       node{true};
        bool                       graph{true};
    };

    /**
     * Native graph-evaluation trace observer.
     *
     * The output follows the Python hgraph trace vocabulary (graph start/eval/
     * stop markers and node ``[IN]``/``[OUT]`` records), but reads values and
     * schedule state directly from the C++ runtime. The optional sink is useful
     * for tests and embedding; it is called synchronously and must copy the
     * supplied view if it retains the line. Without a sink, output goes through
     * the executor-owned run logger, or stdout when ``set_use_logger(false)``
     * is set.
     */
    class HGRAPH_EXPORT EvaluationTrace final : public LifecycleObserver
    {
      public:
        using OutputSink = std::function<void(std::string_view)>;

        explicit EvaluationTrace(EvaluationTraceOptions options, OutputSink output = {});
        explicit EvaluationTrace(std::optional<std::string> filter = std::nullopt,
                                 bool start = true, bool eval = true, bool stop = true,
                                 bool node = true, bool graph = true);

        void on_before_start_graph(const GraphView &graph) override;
        void on_after_start_graph(const GraphView &graph) override;
        void on_before_start_node(const NodeView &node) override;
        void on_after_start_node(const NodeView &node) override;
        void on_before_graph_evaluation(const GraphView &graph) override;
        void on_after_graph_evaluation(const GraphView &graph) override;
        void on_before_node_evaluation(const NodeView &node) override;
        void on_after_node_evaluation(const NodeView &node) override;
        void on_before_stop_node(const NodeView &node) override;
        void on_after_stop_node(const NodeView &node) override;
        void on_before_stop_graph(const GraphView &graph) override;
        void on_after_stop_graph(const GraphView &graph) override;

        /** Include values for valid, unticked inputs as well as modified inputs. */
        static void set_print_all_values(bool value) noexcept;
        /** Use the executor-owned run logger when true; stdout when false. */
        static void set_use_logger(bool value) noexcept;

      private:
        [[nodiscard]] bool should_log_graph(const GraphView &graph) const;
        [[nodiscard]] bool should_log_node(const NodeView &node) const;
        [[nodiscard]] std::string graph_name(const GraphView &graph) const;
        [[nodiscard]] std::string node_name(const NodeView &node) const;

        void emit(DateTime evaluation_time, spdlog::logger *logger,
                  std::string message) const;
        void print_graph(const GraphView &graph, std::string message) const;
        void print_node(const NodeView &node, std::string_view message,
                        bool add_input = false, bool add_output = false,
                        bool add_scheduled_time = false) const;

        EvaluationTraceOptions options_{};
        OutputSink             output_{};

        static std::atomic_bool print_all_values_;
        static std::atomic_bool use_logger_;
    };
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_EVALUATION_TRACE_H
