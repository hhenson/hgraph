/*
 * Expose the graph-specific elements to python
 */

#include <hgraph/runtime/evaluation_engine.h>

namespace hgraph {
    void register_observers_with_nanobind(nb::module_ &m);
}

void export_runtime(nb::module_ &m) {
    using namespace hgraph;

    GraphExecutor::register_with_nanobind(m);
    GraphExecutorImpl::register_with_nanobind(m);

    EvaluationClock::register_with_nanobind(m);
    EngineEvaluationClock::register_with_nanobind(m);
    EngineEvaluationClockDelegate::register_with_nanobind(m);

    BaseEvaluationClock::register_with_nanobind(m);
    SimulationEvaluationClock::register_with_nanobind(m);
    RealTimeEvaluationClock::register_with_nanobind(m);

    EvaluationEngineApi::register_with_nanobind(m);
    EvaluationEngine::register_with_nanobind(m);
    EvaluationEngineImpl::register_with_nanobind(m);
    EvaluationEngineDelegate::register_with_nanobind(m);
    
    // Register observer classes
    register_observers_with_nanobind(m);
}