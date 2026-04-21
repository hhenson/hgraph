#include <hgraph/v2/runtime/evaluation_context.h>
#include <hgraph/v2/runtime/graph_executor.h>

#include <hgraph/util/scope.h>
#include <hgraph/v2/types/graph/graph.h>

namespace hgraph::v2
{
    GraphExecutor::GraphExecutor(GraphExecutorOps *ops, void *data) : _ops{ops}, _data{data}{}

    void GraphExecutor::run() {
        // auto now = std::chrono::system_clock::now();
        // auto graph{_ops->build_graph(_data)};
        // auto release_graph = scope_exit([this, graph = graph] { _graph_builder->release_instance(graph); });
        // fmt::print("{} [CPP] Running graph [{}] start time: {} end time: {}\n", fmt::format("{:%Y-%m-%d %H:%M:%S}", now),
        //            (graph ? *graph->label() : std::string{"unknown"}), start_time, end_time);
        //
        // if (end_time <= start_time) {
        //     if (end_time < start_time) {
        //         throw std::invalid_argument("End time cannot be before the start time");
        //     } else {
        //         throw std::invalid_argument("End time cannot be equal to the start time");
        //     }
        // }
        //
        // EngineEvaluationClock::s_ptr clock;
        // switch (_run_mode) {
        //     case EvaluationMode::REAL_TIME: clock = std::make_shared<RealTimeEvaluationClock>(start_time); break;
        //     case EvaluationMode::SIMULATION: clock = std::make_shared<SimulationEvaluationClock>(start_time); break;
        //     default: throw std::runtime_error("Unknown run mode");
        // }
        //
        // auto evaluationEngine = std::make_shared<EvaluationEngineImpl>(clock, start_time, end_time, _run_mode);
        // graph->set_evaluation_engine(evaluationEngine);
        //
        // for (const auto &observer : _observers) { evaluationEngine->add_life_cycle_observer(observer); }
        //
        // try {
        //     // Initialise the graph but do not dispose here; disposal is handled by GraphBuilder.release_instance in Python
        //     initialise_component(*graph);
        //     // Use RAII; StartStopContext destructor will stop and set Python error if exception occurs
        //     {
        //         auto startStopContext = StartStopContext(graph.get());
        //         try {
        //             while (clock->evaluation_time() < end_time) { _evaluate(*evaluationEngine, *graph); }
        //         } catch (...) {
        //             if (!_cleanup_on_error) startStopContext.cancel();
        //             throw;
        //         }
        //     }
        //     // After StartStopContext destruction, check if a Python error was set during stop
        //     if (PyErr_Occurred()) { throw nb::python_error(); }
        // } catch (const NodeException &e) {
        //     // Raise Python hgraph.NodeException constructed from C++ NodeException details
        //     try {
        //         nb::object hgraph_mod      = nb::module_::import_("hgraph");
        //         nb::object py_node_exc_cls = hgraph_mod.attr("NodeException");
        //         nb::tuple  args =
        //             nb::make_tuple(nb::cast(e.signature_name), nb::cast(e.label), nb::cast(e.wiring_path), nb::cast(e.error_msg),
        //                            nb::cast(e.stack_trace), nb::cast(e.activation_back_trace), nb::cast(e.additional_context));
        //         PyErr_SetObject(py_node_exc_cls.ptr(), args.ptr());
        //     } catch (...) { PyErr_SetString(PyExc_RuntimeError, e.what()); }
        //     throw nb::python_error();
        // } catch (const nb::python_error &e) {
        //     throw;  // Preserve Python exception raised above
        // } catch (const std::exception &e) {
        //     // Preserve any active Python exception (e.g., hgraph.NodeException)
        //     if (PyErr_Occurred()) { throw nb::python_error(); }
        //     // Provide a clear message for unexpected exceptions
        //     std::string msg = std::string("Graph execution failed: ") + e.what();
        //     throw nb::builtin_exception(nb::exception_type::runtime_error, msg.c_str());
        // }
    }
}  // namespace hgraph::v2
