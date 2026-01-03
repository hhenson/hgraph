#include <hgraph/builders/time_series_types/time_series_window_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsw.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {

    // ========== TimeSeriesWindowOutputBuilder (Fixed-size) ==========

    TimeSeriesWindowOutputBuilder::TimeSeriesWindowOutputBuilder(size_t size, size_t min_size,
                                                                 const value::TypeMeta* element_type)
        : OutputBuilder(), _size(size), _min_size(min_size), _element_type(element_type) {}

    time_series_output_s_ptr TimeSeriesWindowOutputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesFixedWindowOutput, TimeSeriesOutput>(
            owning_node, _size, _min_size, _element_type);
    }

    time_series_output_s_ptr TimeSeriesWindowOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return arena_make_shared_as<TimeSeriesFixedWindowOutput, TimeSeriesOutput>(
            owning_output, _size, _min_size, _element_type);
    }

    void TimeSeriesWindowOutputBuilder::release_instance(time_series_output_ptr item) const {
        OutputBuilder::release_instance(item);
        auto* ts = dynamic_cast<TimeSeriesFixedWindowOutput*>(item);
        if (ts == nullptr) {
            throw std::runtime_error(
                "TimeSeriesWindowOutputBuilder::release_instance: expected TimeSeriesFixedWindowOutput but got different type");
        }
        ts->reset_value();
    }

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

    time_series_output_s_ptr TimeSeriesTimeWindowOutputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesTimeWindowOutput, TimeSeriesOutput>(
            owning_node, _size, _min_size, _element_type);
    }

    time_series_output_s_ptr TimeSeriesTimeWindowOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return arena_make_shared_as<TimeSeriesTimeWindowOutput, TimeSeriesOutput>(
            owning_output, _size, _min_size, _element_type);
    }

    void TimeSeriesTimeWindowOutputBuilder::release_instance(time_series_output_ptr item) const {
        OutputBuilder::release_instance(item);
        // No reset_value for time windows - they auto-clean via _roll()
    }

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
