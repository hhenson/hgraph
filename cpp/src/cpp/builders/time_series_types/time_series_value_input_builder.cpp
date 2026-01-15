#include <hgraph/builders/time_series_types/time_series_value_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {

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
