#include <hgraph/api/python/py_signal.h>
#include <hgraph/api/python/py_ts.h>
#include <hgraph/api/python/py_tsl.h>

#include <hgraph/api/python/py_ref.h>

#include <hgraph/api/python/py_graph.h>
#include <hgraph/api/python/py_node.h>
#include <hgraph/api/python/py_special_nodes.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/api/python/py_tsb.h>
#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/py_tsw.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tss.h>

#include <hgraph/types/scalar_types.h>
#include <hgraph/types/schema_type.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/tsb.h>

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
    register_set_delta_with_nanobind(m);

    ref_register_with_nanobind(m);
    signal_register_with_nanobind(m);
    ts_register_with_nanobind(m);
    tsb_register_with_nanobind(m);
    tsd_register_with_nanobind(m);
    tsl_register_with_nanobind(m);
    tss_register_with_nanobind(m);
    tsw_register_with_nanobind(m);

    PyTraits::register_with_nanobind(m);

    node_type_enum_py_register(m);
    injectable_type_enum(m);
    NodeSignature::register_with_nanobind(m);
    PyNodeScheduler::register_with_nanobind(m);
    PyNode::register_with_nanobind(m);
    PyPushQueueNode::register_with_nanobind(m);
    PyNestedNode::register_with_nanobind(m);
    PyMeshNestedNode::register_with_nanobind(m);
    PyGraph::register_with_nanobind(m);

    TimeSeriesSchema::register_with_nanobind(m);

    register_special_nodes_with_nanobind(m);
}