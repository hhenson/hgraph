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

// Python API wrappers
#include <hgraph/api/python/python_api.h>

void export_types(nb::module_ &m) {
    using namespace hgraph;

    // Schema and scalar types (must come before time series types that use them)
    AbstractSchema::register_with_nanobind(m);
    CompoundScalar::register_with_nanobind(m);
    PythonCompoundScalar::register_with_nanobind(m);

    // Error types (derive from CompoundScalar)
    BacktraceSignature::register_with_nanobind(m);
    NodeError::register_with_nanobind(m);

    // OLD DIRECT BINDINGS - Must keep for nanobind type hierarchy
    // Note: TimeSeriesOutput/Input and Base* are needed for specialized types to inherit from
    // Our Python API wrappers will shadow these by registering with the same names later
    TimeSeriesType::register_with_nanobind(m);
    TimeSeriesOutput::register_with_nanobind(m);
    TimeSeriesInput::register_with_nanobind(m);
    BaseTimeSeriesOutput::register_with_nanobind(m);
    BaseTimeSeriesInput::register_with_nanobind(m);

    // TimeSeriesReference types - Still needed by Python wiring code for now
    TimeSeriesReference::register_with_nanobind(m);
    TimeSeriesReferenceOutput::register_with_nanobind(m);
    TimeSeriesReferenceInput::register_with_nanobind(m);

    // Specialized reference types - Still needed by Python wiring code
    TimeSeriesValueReferenceInput::register_with_nanobind(m);
    TimeSeriesListReferenceInput::register_with_nanobind(m);
    TimeSeriesBundleReferenceInput::register_with_nanobind(m);
    TimeSeriesDictReferenceInput::register_with_nanobind(m);
    TimeSeriesSetReferenceInput::register_with_nanobind(m);
    TimeSeriesWindowReferenceInput::register_with_nanobind(m);
    TimeSeriesValueReferenceOutput::register_with_nanobind(m);
    TimeSeriesListReferenceOutput::register_with_nanobind(m);
    TimeSeriesBundleReferenceOutput::register_with_nanobind(m);
    TimeSeriesDictReferenceOutput::register_with_nanobind(m);
    TimeSeriesSetReferenceOutput::register_with_nanobind(m);
    TimeSeriesWindowReferenceOutput::register_with_nanobind(m);

    // OLD DIRECT BINDINGS - Commented out, replaced by Python API wrappers
    // IndexedTimeSeriesOutput::register_with_nanobind(m);
    // IndexedTimeSeriesInput::register_with_nanobind(m);
    // TimeSeriesListOutput::register_with_nanobind(m);
    // TimeSeriesListInput::register_with_nanobind(m);
    // TimeSeriesBundleInput::register_with_nanobind(m);
    // TimeSeriesBundleOutput::register_with_nanobind(m);
    // SetDelta::register_with_nanobind(m);
    // tss_register_with_nanobind(m);  // TimeSeriesSetInput/Output
    // tsd_register_with_nanobind(m);  // TimeSeriesDictInput/Output
    // tsw_register_with_nanobind(m);  // TimeSeriesWindowInput/Output

    Traits::register_with_nanobind(m);

    node_type_enum_py_register(m);
    injectable_type_enum(m);
    NodeSignature::register_with_nanobind(m);

    // OLD DIRECT BINDINGS - Renamed with _ prefix for internal use only
    // These are needed for nanobind type hierarchy but Python API wrappers use public names
    NodeScheduler::register_with_nanobind(m);  // Registers as "_NodeScheduler"
    Node::register_with_nanobind(m);           // Registers as "_Node"
    // Graph has no direct registration - only exposed via PyGraph wrapper

    // OLD DIRECT BINDINGS - Commented out, replaced by Python API wrappers
    // register_ts_with_nanobind(m);  // TS_Bool, TS_Int, TS_Out_Bool, etc.

    TimeSeriesSchema::register_with_nanobind(m);
    
    // ========================================================================
    // Register Python API Wrappers (replaces old direct bindings above)
    // ========================================================================
    hgraph::api::register_python_api(m);
}