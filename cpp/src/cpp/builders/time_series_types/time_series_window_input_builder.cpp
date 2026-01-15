#include <hgraph/builders/time_series_types/time_series_window_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsw.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {

    TimeSeriesWindowInputBuilder::TimeSeriesWindowInputBuilder(const value::TypeMeta* element_type)
        : InputBuilder(), _element_type(element_type) {}

    size_t TimeSeriesWindowInputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesWindowInput));
    }

    size_t TimeSeriesWindowInputBuilder::type_alignment() const {
        return alignof(TimeSeriesWindowInput);
    }

    void time_series_window_input_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesWindowInputBuilder, InputBuilder>(m, "InputBuilder_TSW")
            .def(nb::init<const value::TypeMeta*>(), "element_type"_a = nullptr);
    }

} // namespace hgraph
