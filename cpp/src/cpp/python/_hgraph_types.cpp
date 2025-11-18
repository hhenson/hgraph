#include "hgraph/types/tsd.h"

#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
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

    TimeSeriesType::register_with_nanobind(m);
    TimeSeriesOutput::register_with_nanobind(m);
    TimeSeriesInput::register_with_nanobind(m);
    
    // Register concrete base classes
    BaseTimeSeriesOutput::register_with_nanobind(m);
    BaseTimeSeriesInput::register_with_nanobind(m);

    TimeSeriesReference::register_with_nanobind(m);
    TimeSeriesReferenceOutput::register_with_nanobind(m);
    TimeSeriesReferenceInput::register_with_nanobind(m);

    // Specialized reference input types
    TimeSeriesValueReferenceInput::register_with_nanobind(m);
    TimeSeriesListReferenceInput::register_with_nanobind(m);
    TimeSeriesBundleReferenceInput::register_with_nanobind(m);
    TimeSeriesDictReferenceInput::register_with_nanobind(m);
    TimeSeriesSetReferenceInput::register_with_nanobind(m);
    TimeSeriesWindowReferenceInput::register_with_nanobind(m);

    // Specialized reference output types
    TimeSeriesValueReferenceOutput::register_with_nanobind(m);
    TimeSeriesListReferenceOutput::register_with_nanobind(m);
    TimeSeriesBundleReferenceOutput::register_with_nanobind(m);
    TimeSeriesDictReferenceOutput::register_with_nanobind(m);
    TimeSeriesSetReferenceOutput::register_with_nanobind(m);
    TimeSeriesWindowReferenceOutput::register_with_nanobind(m);

    IndexedTimeSeriesOutput::register_with_nanobind(m);
    IndexedTimeSeriesInput::register_with_nanobind(m);

    TimeSeriesListOutput::register_with_nanobind(m);
    TimeSeriesListInput::register_with_nanobind(m);

    TimeSeriesBundleInput::register_with_nanobind(m);
    TimeSeriesBundleOutput::register_with_nanobind(m);

    register_set_delta_with_nanobind(m);
    tss_register_with_nanobind(m);

    tsd_register_with_nanobind(m);
    tsw_register_with_nanobind(m);

    Traits::register_with_nanobind(m);

    node_type_enum_py_register(m);
    injectable_type_enum(m);
    NodeSignature::register_with_nanobind(m);
    NodeScheduler::register_with_nanobind(m);
    Node::register_with_nanobind(m);


    Graph::register_with_nanobind(m);

    register_ts_with_nanobind(m);

    TimeSeriesSchema::register_with_nanobind(m);
}