#include <hgraph/builders/time_series_types/time_series_list_output_builder.h>
#include <hgraph/builders/builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsl.h>

#include <utility>

namespace hgraph {
    TimeSeriesListOutputBuilder::TimeSeriesListOutputBuilder(OutputBuilder::ptr output_builder, size_t size)
        : output_builder{std::move(output_builder)}, size{size} {
    }

    time_series_output_ptr TimeSeriesListOutputBuilder::make_instance(node_ptr owning_node, void* buffer, size_t* offset) const {
        auto result = make_instance_impl<TimeSeriesListOutput, TimeSeriesOutput>(
            buffer, offset, "TimeSeriesListOutput", owning_node);
        auto list_output = std::static_pointer_cast<TimeSeriesListOutput>(result);
        return make_and_set_outputs(list_output, buffer, offset);
    }

    time_series_output_ptr TimeSeriesListOutputBuilder::make_instance(time_series_output_ptr owning_output, void* buffer, size_t* offset) const {
        // Convert owning_output to TimeSeriesType shared_ptr
        auto owning_ts = std::dynamic_pointer_cast<TimeSeriesType>(owning_output);
        if (!owning_ts) {
            throw std::runtime_error("TimeSeriesListOutputBuilder: owning_output must be a TimeSeriesType");
        }
        auto result = make_instance_impl<TimeSeriesListOutput, TimeSeriesOutput>(
            buffer, offset, "TimeSeriesListOutput", owning_ts);
        auto list_output = std::static_pointer_cast<TimeSeriesListOutput>(result);
        return make_and_set_outputs(list_output, buffer, offset);
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

    time_series_output_ptr TimeSeriesListOutputBuilder::make_and_set_outputs(std::shared_ptr<TimeSeriesListOutput> output, void* buffer, size_t* offset) const {
        std::vector<time_series_output_ptr> outputs;
        outputs.reserve(size);
        for (size_t i = 0; i < size; ++i) { 
            outputs.push_back(output_builder->make_instance(output, buffer, offset)); 
        }
        output->set_ts_values(outputs);
        return output;
    }

    size_t TimeSeriesListOutputBuilder::memory_size() const {
        // Add canary size to the base list object
        size_t total = add_canary_size(sizeof(TimeSeriesListOutput));
        // For each element, align and add its size
        for (size_t i = 0; i < size; ++i) {
            total = align_size(total, alignof(TimeSeriesType));
            total += output_builder->memory_size();
        }
        return total;
    }

    void TimeSeriesListOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_ < TimeSeriesListOutputBuilder, OutputBuilder > (m, "OutputBuilder_TSL")
                .def(nb::init<OutputBuilder::ptr, size_t>(), "output_builder"_a, "size"_a);
    }
} // namespace hgraph