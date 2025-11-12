//
// Python API - Master header for all Python wrapper classes
//

#ifndef HGRAPH_PYTHON_API_H
#define HGRAPH_PYTHON_API_H

// Smart pointer infrastructure
#include <hgraph/api/python/api_ptr.h>

// Core types
#include <hgraph/api/python/py_node.h>
#include <hgraph/api/python/py_graph.h>
#include <hgraph/api/python/py_node_scheduler.h>

// Base time series types
#include <hgraph/api/python/py_time_series.h>

// Specialized time series types
#include <hgraph/api/python/py_ts_types.h>

// Factory functions
#include <hgraph/api/python/wrapper_factory.h>

#include <nanobind/nanobind.h>

namespace hgraph::api {
    
    /**
     * Register all Python API wrapper classes with nanobind
     * Call this from your module initialization
     */
    void register_python_api(nanobind::module_& m);
    
} // namespace hgraph::api

#endif // HGRAPH_PYTHON_API_H

