//
// Python API Registration - Registers all Python wrapper classes with nanobind
//

#include <hgraph/api/python/py_node.h>
#include <hgraph/api/python/py_graph.h>
#include <hgraph/api/python/py_node_scheduler.h>
#include <hgraph/api/python/py_evaluation_engine_api.h>
#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/api/python/py_traits.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/api/python/py_ts_types.h>
#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph::api {
    
    /**
     * Register all Python API wrapper classes with nanobind
     * 
     * This should be called from the main nanobind module initialization
     * to expose the Python API layer.
     */
    void register_python_api(nb::module_& m) {
        // Core types
        PyNode::register_with_nanobind(m);
        PyGraph::register_with_nanobind(m);
        PyNodeScheduler::register_with_nanobind(m);
        PyEvaluationEngineApi::register_with_nanobind(m);
        PyEvaluationClock::register_with_nanobind(m);
        PyTraits::register_with_nanobind(m);
        
        // Base time series types
        PyTimeSeriesInput::register_with_nanobind(m);
        PyTimeSeriesOutput::register_with_nanobind(m);
        
        // TS (Value) types
        PyTimeSeriesValueInput::register_with_nanobind(m);
        PyTimeSeriesValueOutput::register_with_nanobind(m);
        
        // Signal types (input-only)
        PyTimeSeriesSignalInput::register_with_nanobind(m);
        
        // TSL (List) types
        PyTimeSeriesListInput::register_with_nanobind(m);
        PyTimeSeriesListOutput::register_with_nanobind(m);
        
        // TSB (Bundle) types
        PyTimeSeriesBundleInput::register_with_nanobind(m);
        PyTimeSeriesBundleOutput::register_with_nanobind(m);
        
        // TSD (Dict) types
        PyTimeSeriesDictInput::register_with_nanobind(m);
        PyTimeSeriesDictOutput::register_with_nanobind(m);
        
        // TSS (Set) types
        PyTimeSeriesSetInput::register_with_nanobind(m);
        PyTimeSeriesSetOutput::register_with_nanobind(m);
        
        // TSW (Window) types
        PyTimeSeriesWindowInput::register_with_nanobind(m);
        PyTimeSeriesWindowOutput::register_with_nanobind(m);
        
        // REF (Reference) types
        PyTimeSeriesReferenceInput::register_with_nanobind(m);
        PyTimeSeriesReferenceOutput::register_with_nanobind(m);
    }
    
} // namespace hgraph::api

