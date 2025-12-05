#include <hgraph/builders/time_series_types/time_series_list_output_builder.h>
#include <hgraph/builders/builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsl.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

#include <utility>

namespace hgraph {
    TimeSeriesListOutputBuilder::TimeSeriesListOutputBuilder(OutputBuilder::ptr output_builder, size_t size)
        : output_builder{std::move(output_builder)}, size{size} {
    }

    time_series_output_s_ptr TimeSeriesListOutputBuilder::make_instance(node_ptr owning_node) const {
        auto v = arena_make_shared_as<TimeSeriesListOutput, TimeSeriesOutput>(owning_node);
        return make_and_set_outputs(v);
    }

    time_series_output_s_ptr TimeSeriesListOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        auto v = arena_make_shared_as<TimeSeriesListOutput, TimeSeriesOutput>(owning_output);
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
        auto list = dynamic_cast<TimeSeriesListOutput *>(item);
        if (list == nullptr) {
            throw std::runtime_error("TimeSeriesListOutputBuilder::release_instance: expected TimeSeriesListOutput but got different type");
        }
        for (auto &value: list->ts_values()) { output_builder->release_instance(value.get()); }
    }

    time_series_output_s_ptr TimeSeriesListOutputBuilder::make_and_set_outputs(time_series_list_output_s_ptr output) const {
        std::vector<time_series_output_s_ptr> outputs;
        outputs.reserve(size);
        for (size_t i = 0; i < size; ++i) { outputs.push_back(output_builder->make_instance(output.get())); }
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