#include <hgraph/runtime/observers/evaluation_profiler.h>
#include <hgraph/runtime/observers/evaluation_trace.h>
#include <hgraph/runtime/observers/inspection_observer.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/api/python/py_graph.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/function.h>
#include <nanobind/stl/shared_ptr.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph {

    void register_observers_with_nanobind(nb::module_ &m) {
        // EvaluationProfiler
        nb::class_<EvaluationProfiler, EvaluationLifeCycleObserver>(m, "EvaluationProfiler",
            "Prints out some useful metrics of the running graph, can help trace down memory leaks.\n"
            "\n"
            "This observer tracks memory usage and prints profiling metrics during graph evaluation.")
            .def(nb::init<bool, bool, bool, bool, bool>(),
                 "start"_a = true,
                 "eval"_a = true,
                 "stop"_a = true,
                 "node"_a = true,
                 "graph"_a = true,
                 "Construct a new EvaluationProfiler.\n"
                 "\n"
                 "Args:\n"
                 "    start: Log start related events\n"
                 "    eval: Log eval related events\n"
                 "    stop: Log stop related events\n"
                 "    node: Log node related events\n"
                 "    graph: Log graph related events")
            .def("on_before_start_graph", &EvaluationProfiler::on_before_start_graph)
            .def("on_after_start_graph", &EvaluationProfiler::on_after_start_graph)
            .def("on_before_start_node", &EvaluationProfiler::on_before_start_node)
            .def("on_after_start_node", &EvaluationProfiler::on_after_start_node)
            .def("on_before_graph_evaluation", &EvaluationProfiler::on_before_graph_evaluation)
            .def("on_before_node_evaluation", &EvaluationProfiler::on_before_node_evaluation)
            .def("on_after_node_evaluation", &EvaluationProfiler::on_after_node_evaluation)
            .def("on_after_graph_evaluation", &EvaluationProfiler::on_after_graph_evaluation)
            .def("on_before_stop_node", &EvaluationProfiler::on_before_stop_node)
            .def("on_after_stop_node", &EvaluationProfiler::on_after_stop_node)
            .def("on_before_stop_graph", &EvaluationProfiler::on_before_stop_graph)
            .def("on_after_stop_graph", &EvaluationProfiler::on_after_stop_graph);

        // EvaluationTrace
        nb::class_<EvaluationTrace, EvaluationLifeCycleObserver>(m, "EvaluationTrace",
            "Logs out the different steps as the engine evaluates the graph.\n"
            "\n"
            "This is voluminous but can be helpful tracing down unexpected behaviour.\n"
            "Provides detailed logging of graph execution steps including node inputs,\n"
            "outputs, and state changes.")
            .def(nb::init<const std::optional<std::string>&, bool, bool, bool, bool, bool>(),
                 "filter"_a = std::nullopt,
                 "start"_a = true,
                 "eval"_a = true,
                 "stop"_a = true,
                 "node"_a = true,
                 "graph"_a = true,
                 "Construct a new EvaluationTrace.\n"
                 "\n"
                 "Args:\n"
                 "    filter: Used to restrict which node and graph events to report (substring match)\n"
                 "    start: Log start related events\n"
                 "    eval: Log eval related events\n"
                 "    stop: Log stop related events\n"
                 "    node: Log node related events\n"
                 "    graph: Log graph related events")
            .def("on_before_start_graph", &EvaluationTrace::on_before_start_graph)
            .def("on_after_start_graph", &EvaluationTrace::on_after_start_graph)
            .def("on_before_start_node", &EvaluationTrace::on_before_start_node)
            .def("on_after_start_node", &EvaluationTrace::on_after_start_node)
            .def("on_before_graph_evaluation", &EvaluationTrace::on_before_graph_evaluation)
            .def("on_before_node_evaluation", &EvaluationTrace::on_before_node_evaluation)
            .def("on_after_node_evaluation", &EvaluationTrace::on_after_node_evaluation)
            .def("on_after_graph_evaluation", &EvaluationTrace::on_after_graph_evaluation)
            .def("on_before_stop_node", &EvaluationTrace::on_before_stop_node)
            .def("on_after_stop_node", &EvaluationTrace::on_after_stop_node)
            .def("on_before_stop_graph", &EvaluationTrace::on_before_stop_graph)
            .def("on_after_stop_graph", &EvaluationTrace::on_after_stop_graph)
            .def_static("set_print_all_values", &EvaluationTrace::set_print_all_values,
                       "value"_a,
                       "To see all values (not only ticked ones) in the trace set this to True.")
            .def_static("set_use_logger", &EvaluationTrace::set_use_logger,
                       "value"_a,
                       "To use print instead of the logger set this to False.");

        // PerformanceMetrics
        nb::class_<PerformanceMetrics>(m, "PerformanceMetrics",
            "Performance metrics for nodes and graphs")
            .def(nb::init<>())
            .def(nb::init<size_t, int64_t>(),
                 "eval_count"_a, "eval_time"_a)
            .def_rw("eval_count", &PerformanceMetrics::eval_count,
                   "Number of evaluations")
            .def_rw("eval_time", &PerformanceMetrics::eval_time,
                   "Total evaluation time in nanoseconds");

        // GraphInfo
        nb::class_<GraphInfo>(m, "GraphInfo",
            "Information collected about a graph during observation")
            .def(nb::init<>())
            .def_prop_ro("graph", [](const GraphInfo& self){ return self.graph ? wrap_graph(self.graph->shared_from_this()) : nb::none(); })
            .def_ro("id", &GraphInfo::id)
            .def_ro("label", &GraphInfo::label)
            .def_prop_ro("parent_graph", [](const GraphInfo& self){ return self.parent_graph ? wrap_graph(self.parent_graph->shared_from_this()) : nb::none(); })
            .def_ro("stopped", &GraphInfo::stopped)
            .def_ro("node_count", &GraphInfo::node_count)
            .def_ro("total_subgraph_count", &GraphInfo::total_subgraph_count)
            .def_ro("total_node_count", &GraphInfo::total_node_count)
            .def_ro("node_total_subgraph_counts", &GraphInfo::node_total_subgraph_counts)
            .def_ro("node_total_node_counts", &GraphInfo::node_total_node_counts)
            .def_ro("eval_count", &GraphInfo::eval_count)
            .def_ro("cycle_time", &GraphInfo::cycle_time)
            .def_ro("os_cycle_time", &GraphInfo::os_cycle_time)
            .def_ro("observation_time", &GraphInfo::observation_time)
            .def_ro("eval_time", &GraphInfo::eval_time)
            .def_ro("os_eval_time", &GraphInfo::os_eval_time)
            .def_ro("node_eval_counts", &GraphInfo::node_eval_counts)
            .def_ro("node_eval_times", &GraphInfo::node_eval_times)
            .def_ro("node_value_sizes", &GraphInfo::node_value_sizes)
            .def_ro("node_sizes", &GraphInfo::node_sizes)
            .def_ro("node_total_value_sizes", &GraphInfo::node_total_value_sizes)
            .def_ro("node_total_sizes", &GraphInfo::node_total_sizes)
            .def_ro("total_value_size", &GraphInfo::total_value_size)
            .def_ro("total_size", &GraphInfo::total_size)
            .def_ro("size", &GraphInfo::size);

        // InspectionObserver
        nb::class_<InspectionObserver, EvaluationLifeCycleObserver>(
            m, "InspectionObserver",
            "Collects comprehensive statistics about graph execution.\n"
            "\n"
            "This observer tracks evaluation counts, timing, and optionally memory sizes\n"
            "for all nodes and graphs. It supports callbacks for node/graph events and\n"
            "maintains subscription system for selective monitoring.")
            .def(
                "__init__",
                [](InspectionObserver* self, nb::object graph, std::function<void(nb::object)> callback_node,
                   std::function<void(nb::object)> callback_graph, InspectionObserver::ProgressCallback callback_progress,
                   double progress_interval, bool compute_sizes, bool track_recent_performance) {
                    auto g = graph.is_none() ? nullptr : unwrap_graph(graph).get();
                    return new(self) InspectionObserver(
                         g,
                         [callback_node](Node* node){ callback_node(wrap_node(node->shared_from_this()));},
                         [callback_graph](Graph* graph){ callback_graph(wrap_graph(graph->shared_from_this()));},
                         callback_progress,
                         progress_interval,
                         compute_sizes,
                         track_recent_performance
                    );
                },
                "graph"_a = nullptr, "callback_node"_a = nullptr, "callback_graph"_a = nullptr, "callback_progress"_a = nullptr,
                "progress_interval"_a = 0.1, "compute_sizes"_a = false, "track_recent_performance"_a = false,
                "Construct a new InspectionObserver.\n"
                "\n"
                "Args:\n"
                "    graph: Optional graph to walk and initialize\n"
                "    callback_node: Callback for node events\n"
                "    callback_graph: Callback for graph events\n"
                "    callback_progress: Progress callback invoked periodically\n"
                "    progress_interval: Interval between progress callbacks (seconds)\n"
                "    compute_sizes: Whether to compute memory sizes (expensive)\n"
                "    track_recent_performance: Whether to track recent performance batches"
          )
            .def("on_before_start_graph", [](InspectionObserver* self, const PyGraph& graph){ self->on_before_start_graph(unwrap_graph(graph).get()); }, "graph"_a)
            .def("on_after_start_graph", [](InspectionObserver* self, const PyGraph& graph){ self->on_after_start_graph(unwrap_graph(graph).get()); }, "graph"_a)
            .def("on_before_graph_evaluation", [](InspectionObserver* self, const PyGraph& graph){ self->on_before_graph_evaluation(unwrap_graph(graph).get()); }, "graph"_a)
            .def("on_before_node_evaluation", [](InspectionObserver* self, const PyNode& node){ self->on_before_node_evaluation(unwrap_node(node).get()); }, "node"_a)
            .def("on_after_node_evaluation", [](InspectionObserver* self, const PyNode& node){ self->on_after_node_evaluation(unwrap_node(node).get()); }, "node"_a)
            .def("on_after_graph_push_nodes_evaluation", [](InspectionObserver* self, const PyGraph& graph){ self->on_after_graph_push_nodes_evaluation(unwrap_graph(graph).get()); }, "graph"_a)
            .def("on_after_graph_evaluation", [](InspectionObserver* self, const PyGraph& graph){ self->on_after_graph_evaluation(unwrap_graph(graph).get()); }, "graph"_a)
            .def("on_after_stop_graph", [](InspectionObserver* self, const PyGraph& graph){ self->on_after_stop_graph(unwrap_graph(graph).get()); }, "graph"_a)
            .def("subscribe_graph", &InspectionObserver::subscribe_graph, "graph_id"_a, "Subscribe to events for a specific graph")
            .def("unsubscribe_graph", &InspectionObserver::unsubscribe_graph, "graph_id"_a,
                 "Unsubscribe from events for a specific graph")
            .def("subscribe_node", &InspectionObserver::subscribe_node, "node_id"_a, "Subscribe to events for a specific node")
            .def("unsubscribe_node", &InspectionObserver::unsubscribe_node, "node_id"_a,
                 "Unsubscribe from events for a specific node")
            .def("get_graph_info", &InspectionObserver::get_graph_info, "graph_id"_a, "Get information about a specific graph")
            .def("walk", &InspectionObserver::walk, "graph"_a, "Walk a graph and initialize observation state")
            .def(
                "get_recent_node_performance",
                [](const InspectionObserver &self, const std::vector<int64_t> &node_id,
                   const std::optional<std::chrono::system_clock::time_point> &after) {
                    std::vector<std::pair<std::chrono::system_clock::time_point, PerformanceMetrics>> result;
                    self.get_recent_node_performance(node_id, result, after);
                    return result;
                },
                "node_id"_a, "after"_a = std::nullopt, "Get recent performance data for a specific node")
            .def(
                "get_recent_graph_performance",
                [](const InspectionObserver &self, const std::vector<int64_t> &graph_id,
                   const std::optional<std::chrono::system_clock::time_point> &after) {
                    std::vector<std::pair<std::chrono::system_clock::time_point, PerformanceMetrics>> result;
                    self.get_recent_graph_performance(graph_id, result, after);
                    return result;
                },
                "graph_id"_a, "after"_a = std::nullopt, "Get recent performance data for a specific graph")
            .def_prop_ro("recent_performance_batch", &InspectionObserver::recent_performance_batch,
                         "Current performance batch timestamp");
    }

} // namespace hgraph

