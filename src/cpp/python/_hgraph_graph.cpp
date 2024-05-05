/*
 * Expose the graph specific elements to python
 */
#include <hgraph/python/pyb_wiring.h>
#include <memory>



void export_graph_elements(py::module_& m) {
    // using namespace hgraph;
    //
    // py::class_<ExecutionContext, ExecutionContext::s_ptr>(m, "ExecutionContext")
    //     .def_property_readonly("current_engine_time", &ExecutionContext::current_engine_time)
    //     .def_property_readonly("next_cycle_engine_time", &ExecutionContext::next_cycle_engine_time)
    //     .def_property_readonly("proposed_next_engine_time", &ExecutionContext::proposed_next_engine_time)
    //     //This needs to be a method instead of a property as it's value is determined outside the current thread
    //     .def("push_has_pending_values", &ExecutionContext::push_has_pending_values)
    //     .def("wall_clock_time", &ExecutionContext::wall_clock_time)
    //     .def("engine_lag", &ExecutionContext::engine_lag)
    //     .doc() = "Holds context information describing the current state of the graph executor";
    //
    // py::class_<BackTestExecutionContext, ExecutionContext, BackTestExecutionContext::s_ptr>(m, "BackTestExecutionContext")
    //     .doc() = "The back test exucution context";
    //
    // py::class_<RealTimeExecutionContext, ExecutionContext, RealTimeExecutionContext::s_ptr>(m, "RealTimeExecutionContext")
    //     .doc() = "The real time execution context";
    //
    //
    // py::class_<Node, Node::s_ptr>(m, "Node")
    //     .def_property_readonly("id", &Node::id )
    //     .def_property_readonly("owning_graph", static_cast< const Graph& (Node::*)() const>(&Node::owning_graph))
    //     .def_property_readonly("execution_context", static_cast< const ExecutionContext& (Node::*)() const>(&Node::execution_context));
    //
    //
    // //Only export attributes we care to see in Python, this is not to expose all the functionality.
    // py::class_<Graph, Graph::s_ptr>(m, "Graph")
    //     .def_property_readonly("nodes", [](Graph::s_ptr self){
    //         py::list l{};
    //         auto ec{self->executor().shared_from_this()};
    //         for(const auto& sn : self->nodes()){
    //             py::tuple entry(2);
    //             entry[0] = sn.scheduled_time;
    //             entry[1] = Node::s_ptr(ec, sn.node);
    //             l.append(entry);
    //         }
    //         return l;
    //     })  //This is expensive, but when debugging it is useful to see direct in debugger
    //     .def_property_readonly("executor", static_cast< const GraphExecutor& (Graph::*)() const>(&Graph::executor))
    //     .def_property_readonly("execution_context", static_cast< const ExecutionContext& (Graph::*)() const>(&Graph::execution_context));
    //
    // py::class_<GraphExecutor, GraphExecutor::s_ptr>(m, "GraphExecutor")
    //     .def_property_readonly("execution_context",
    //                            [](GraphExecutor::s_ptr self){
    //                                return std::shared_ptr<ExecutionContext>(self, &self->execution_context());})
    //     ;
    //
    // py::class_<BackTestGraphExecutor, GraphExecutor, BackTestGraphExecutor::s_ptr>(m, "BackTestGraphExecutor")
    //     .def(py::init([](GraphGenerator::s_ptr node_generator, engine_time_t start_time, engine_time_t end_time){
    //              return BackTestGraphExecutor::make_graph_executor(node_generator , start_time, end_time);
    //          }), "node_generator"_a, "start_time"_a = MIN_ST, "end_time"_a = MAX_ET);
    //
    // py::class_<RealTimeGraphExecutor, GraphExecutor, RealTimeGraphExecutor::s_ptr>(m, "RealTimeGraphExecutor")
    //     .def(py::init([](GraphGenerator::s_ptr node_generator, engine_time_t start_time, engine_time_t end_time){
    //              return RealTimeGraphExecutor::make_graph_executor(node_generator, start_time, end_time);
    //          }), "node_generator"_a, "start_time"_a = MIN_ST, "end_time"_a = MAX_ET);
}