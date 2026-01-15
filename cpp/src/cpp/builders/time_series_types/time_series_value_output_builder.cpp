#include <hgraph/builders/time_series_types/time_series_value_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {

    TimeSeriesValueOutputBuilder::TimeSeriesValueOutputBuilder(const value::TypeMeta* schema)
        : OutputBuilder(), _schema(schema) {}

    size_t TimeSeriesValueOutputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesValueOutput));
    }

    size_t TimeSeriesValueOutputBuilder::type_alignment() const {
        return alignof(TimeSeriesValueOutput);
    }

    void time_series_value_output_builder_register_with_nanobind(nb::module_& m) {
        nb::class_<TimeSeriesValueOutputBuilder, OutputBuilder>(m, "OutputBuilder_TS_Value")
            .def(nb::init<const value::TypeMeta*>(), "schema"_a);
    }

} // namespace hgraph
