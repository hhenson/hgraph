#include <hgraph/builders/time_series_types/time_series_window_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsw.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {

    // ========== TimeSeriesWindowOutputBuilder (Fixed-size) ==========

    TimeSeriesWindowOutputBuilder::TimeSeriesWindowOutputBuilder(size_t size, size_t min_size,
                                                                 const value::TypeMeta* element_type)
        : OutputBuilder(), _size(size), _min_size(min_size), _element_type(element_type) {}

    size_t TimeSeriesWindowOutputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesFixedWindowOutput));
    }

    size_t TimeSeriesWindowOutputBuilder::type_alignment() const {
        return alignof(TimeSeriesFixedWindowOutput);
    }

    // ========== TimeSeriesTimeWindowOutputBuilder (Time-based) ==========

    TimeSeriesTimeWindowOutputBuilder::TimeSeriesTimeWindowOutputBuilder(engine_time_delta_t size,
                                                                         engine_time_delta_t min_size,
                                                                         const value::TypeMeta* element_type)
        : OutputBuilder(), _size(size), _min_size(min_size), _element_type(element_type) {}

    size_t TimeSeriesTimeWindowOutputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesTimeWindowOutput));
    }

    size_t TimeSeriesTimeWindowOutputBuilder::type_alignment() const {
        return alignof(TimeSeriesTimeWindowOutput);
    }

    // ========== Nanobind Registration ==========

    void time_series_window_output_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesWindowOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSW")
            .def(nb::init<size_t, size_t, const value::TypeMeta*>(),
                 "size"_a, "min_size"_a, "element_type"_a);

        nb::class_<TimeSeriesTimeWindowOutputBuilder, OutputBuilder>(m, "OutputBuilder_TTSW")
            .def(nb::init<engine_time_delta_t, engine_time_delta_t, const value::TypeMeta*>(),
                 "size"_a, "min_size"_a, "element_type"_a);
    }

} // namespace hgraph
