#include <hgraph/builders/time_series_types/time_series_list_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsl.h>

#include <utility>

namespace hgraph {
    TimeSeriesListOutputBuilder::TimeSeriesListOutputBuilder(OutputBuilder::ptr output_builder, size_t size)
        : output_builder{std::move(output_builder)}, size{size} {
    }

    time_series_output_ptr TimeSeriesListOutputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesListOutput(owning_node)};
        return make_and_set_outputs(v);
    }

    time_series_output_ptr TimeSeriesListOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesListOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
        return make_and_set_outputs(v);
    }

    bool TimeSeriesListOutputBuilder::is_same_type(const Builder &other) const {
        if (auto other_b = dynamic_cast<const TimeSeriesListOutputBuilder *>(&other)) {
            return output_builder->is_same_type(*other_b->output_builder);
        }
        return false;
    }

    void TimeSeriesListOutputBuilder::release_instance(time_series_output_ptr item) const {
        OutputBuilder::release_instance(item);
        auto list = dynamic_cast<TimeSeriesListOutput *>(item.get());
        if (list) {
            for (auto &value: list->ts_values()) { output_builder->release_instance(value); }
        }
    }

    time_series_output_ptr TimeSeriesListOutputBuilder::make_and_set_outputs(TimeSeriesListOutput *output) const {
        std::vector<time_series_output_ptr> outputs;
        outputs.reserve(size);
        for (size_t i = 0; i < size; ++i) { outputs.push_back(output_builder->make_instance(output)); }
        output->set_ts_values(outputs);
        return output;
    }

    void TimeSeriesListOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_ < TimeSeriesListOutputBuilder, OutputBuilder > (m, "OutputBuilder_TSL")
                .def(nb::init<OutputBuilder::ptr, size_t>(), "output_builder"_a, "size"_a);
    }
} // namespace hgraph