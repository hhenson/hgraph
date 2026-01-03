#include <hgraph/builders/time_series_types/time_series_value_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {

    time_series_input_s_ptr TimeSeriesValueInputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesValueInput, TimeSeriesInput>(owning_node);
    }

    time_series_input_s_ptr TimeSeriesValueInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return arena_make_shared_as<TimeSeriesValueInput, TimeSeriesInput>(owning_input);
    }

    size_t TimeSeriesValueInputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesValueInput));
    }

    size_t TimeSeriesValueInputBuilder::type_alignment() const {
        return alignof(TimeSeriesValueInput);
    }

    void time_series_value_input_builder_register_with_nanobind(nb::module_& m) {
        nb::class_<TimeSeriesValueInputBuilder, InputBuilder>(m, "InputBuilder_TS_Value")
            .def(nb::init<>());
    }

} // namespace hgraph
