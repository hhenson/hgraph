#include <hgraph/builders/time_series_types/time_series_ref_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>

namespace hgraph {
    time_series_output_ptr TimeSeriesRefOutputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesReferenceOutput(owning_node)};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    time_series_output_ptr TimeSeriesRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesReferenceOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    void TimeSeriesRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TS_Ref").def(nb::init<>());
    }
} // namespace hgraph