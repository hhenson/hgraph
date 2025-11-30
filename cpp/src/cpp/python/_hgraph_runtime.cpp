/*
 * Expose the graph-specific elements to python
 */

#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/api/python/py_evaluation_engine.h>
#include <hgraph/runtime/evaluation_engine.h>

namespace hgraph {
    void register_observers_with_nanobind(nb::module_ &m);
}

void export_runtime(nb::module_ &m) {
    using namespace hgraph;

    GraphExecutor::register_with_nanobind(m);

    // Register wrapper classes for Python API (these wrap the base classes)
    PyEvaluationClock::register_with_nanobind(m);
    PyEvaluationEngineApi::register_with_nanobind(m);

    
    // Register observer classes
    register_observers_with_nanobind(m);
}