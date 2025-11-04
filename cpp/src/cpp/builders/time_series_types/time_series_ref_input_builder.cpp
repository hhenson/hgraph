#include <hgraph/builders/time_series_types/time_series_ref_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>

namespace hgraph
{
    time_series_input_ptr TimeSeriesRefInputBuilder::make_instance(node_ptr owning_node) const {
        return {new TimeSeriesReferenceInput(owning_node)};
    }

    time_series_input_ptr TimeSeriesRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return {new TimeSeriesReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
    }

    void TimeSeriesRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesRefInputBuilder, InputBuilder>(m, "InputBuilder_TS_Ref").def(nb::init<>());
    }
}  // namespace hgraph