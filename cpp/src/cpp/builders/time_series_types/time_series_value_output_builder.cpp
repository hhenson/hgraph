#include <hgraph/builders/time_series_types/time_series_value_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {

    TimeSeriesValueOutputBuilder::TimeSeriesValueOutputBuilder(const value::TypeMeta* schema)
        : OutputBuilder(), _schema(schema) {}

    time_series_output_s_ptr TimeSeriesValueOutputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesValueOutput, TimeSeriesOutput>(owning_node, _schema);
    }

    time_series_output_s_ptr TimeSeriesValueOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return arena_make_shared_as<TimeSeriesValueOutput, TimeSeriesOutput>(owning_output, _schema);
    }

    void TimeSeriesValueOutputBuilder::release_instance(time_series_output_ptr item) const {
        OutputBuilder::release_instance(item);
        auto* ts = dynamic_cast<TimeSeriesValueOutput*>(item);
        if (ts == nullptr) {
            throw std::runtime_error(
                "TimeSeriesValueOutputBuilder::release_instance: expected TimeSeriesValueOutput but got different type");
        }
        ts->reset_value();
    }

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
