#include "hgraph/api/python/py_ref.h"
#include "hgraph/types/tsd.h"

#include <hgraph/types/error_type.h>
#include <hgraph/api/python/py_graph.h>
#include <hgraph/api/python/py_node.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/schema_type.h>
#include <hgraph/types/scalar_types.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/base_time_series.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsl.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/tsw.h>

void export_types(nb::module_ &m) {
    using namespace hgraph;

    // Schema and scalar types (must come before time series types that use them)
    AbstractSchema::register_with_nanobind(m);
    CompoundScalar::register_with_nanobind(m);
    PythonCompoundScalar::register_with_nanobind(m);

    // Error types (derive from CompoundScalar)
    BacktraceSignature::register_with_nanobind(m);
    NodeError::register_with_nanobind(m);

    PyTimeSeriesType::register_with_nanobind(m);
    PyTimeSeriesOutput::register_with_nanobind(m);
    PyTimeSeriesInput::register_with_nanobind(m);

    // All reference types registered inside this function
    register_time_series_reference_with_nanobind(m);
    // IndexedTimeSeriesOutput::register_with_nanobind(m);
    // IndexedTimeSeriesInput::register_with_nanobind(m);
    //
    // TimeSeriesListOutput::register_with_nanobind(m);
    // TimeSeriesListInput::register_with_nanobind(m);
    //
    // TimeSeriesBundleInput::register_with_nanobind(m);
    // TimeSeriesBundleOutput::register_with_nanobind(m);

    SetDelta::register_with_nanobind(m);
    tss_register_with_nanobind(m);

    tsd_register_with_nanobind(m);
    tsw_register_with_nanobind(m);

    PyTraits::register_with_nanobind(m);

    node_type_enum_py_register(m);
    injectable_type_enum(m);
    NodeSignature::register_with_nanobind(m);
    PyNodeScheduler::register_with_nanobind(m);
    PyNode::register_with_nanobind(m);


    PyGraph::register_with_nanobind(m);

    register_ts_with_nanobind(m);

    TimeSeriesSchema::register_with_nanobind(m);
}