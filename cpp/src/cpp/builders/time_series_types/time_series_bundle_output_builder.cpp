#include <hgraph/builders/time_series_types/time_series_bundle_output_builder.h>
#include <hgraph/builders/builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>

#include <ranges>
#include <utility>

namespace hgraph {
    TimeSeriesBundleOutputBuilder::TimeSeriesBundleOutputBuilder(TimeSeriesSchema::ptr schema,
                                                                 std::vector<OutputBuilder::ptr> output_builders)
        : OutputBuilder(), schema{std::move(schema)}, output_builders{std::move(output_builders)} {
    }

    time_series_output_ptr TimeSeriesBundleOutputBuilder::make_instance(node_ptr owning_node, std::shared_ptr<void> buffer, size_t* offset) const {
        auto result = make_instance_impl<TimeSeriesBundleOutput, TimeSeriesOutput>(
            buffer, offset, "TimeSeriesBundleOutput", owning_node, schema);
        auto bundle_output = std::static_pointer_cast<TimeSeriesBundleOutput>(result);
        return make_and_set_outputs(bundle_output, buffer, offset);
    }

    time_series_output_ptr TimeSeriesBundleOutputBuilder::make_instance(time_series_output_ptr owning_output, std::shared_ptr<void> buffer, size_t* offset) const {
        // Convert owning_output to TimeSeriesType shared_ptr
        auto owning_ts = std::dynamic_pointer_cast<TimeSeriesType>(owning_output);
        if (!owning_ts) {
            throw std::runtime_error("TimeSeriesBundleOutputBuilder: owning_output must be a TimeSeriesType");
        }
        auto result = make_instance_impl<TimeSeriesBundleOutput, TimeSeriesOutput>(
            buffer, offset, "TimeSeriesBundleOutput", owning_ts, schema);
        auto bundle_output = std::static_pointer_cast<TimeSeriesBundleOutput>(result);
        return make_and_set_outputs(bundle_output, buffer, offset);
    }

    bool TimeSeriesBundleOutputBuilder::has_reference() const {
        return std::ranges::any_of(output_builders, [](const auto &builder) { return builder->has_reference(); });
    }

    bool TimeSeriesBundleOutputBuilder::is_same_type(const Builder &other) const {
        if (auto other_b = dynamic_cast<const TimeSeriesBundleOutputBuilder *>(&other)) {
            if (output_builders.size() != other_b->output_builders.size()) { return false; }
            for (size_t i = 0; i < output_builders.size(); ++i) {
                if (!output_builders[i]->is_same_type(*other_b->output_builders[i])) { return false; }
            }
            return true;
        }
        return false;
    }

    void TimeSeriesBundleOutputBuilder::release_instance(time_series_output_ptr item) const {
        OutputBuilder::release_instance(item);
        auto bundle = dynamic_cast<TimeSeriesBundleOutput *>(item.get());
        if (bundle) {
            auto &outputs = bundle->ts_values();
            for (size_t i = 0; i < output_builders.size(); ++i) { output_builders[i]->release_instance(outputs[i]); }
        }
    }

    time_series_output_ptr TimeSeriesBundleOutputBuilder::make_and_set_outputs(std::shared_ptr<TimeSeriesBundleOutput> output, std::shared_ptr<void> buffer, size_t* offset) const {
        std::vector<time_series_output_ptr> outputs;
        outputs.reserve(output_builders.size());
        std::ranges::copy(output_builders | std::views::transform([&](auto &builder) {
                              return builder->make_instance(output, buffer, offset);
                          }),
                          std::back_inserter(outputs));
        output->set_ts_values(outputs);
        return output;
    }

    size_t TimeSeriesBundleOutputBuilder::memory_size() const {
        // Add canary size to the base bundle object
        size_t total = add_canary_size(sizeof(TimeSeriesBundleOutput));
        // Align before each nested time-series output
        for (const auto &builder : output_builders) {
            total = align_size(total, alignof(TimeSeriesType));
            total += builder->memory_size();
        }
        return total;
    }

    void TimeSeriesBundleOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_ < TimeSeriesBundleOutputBuilder, OutputBuilder > (m, "OutputBuilder_TSB")
                .def(nb::init<TimeSeriesSchema::ptr, std::vector<OutputBuilder::ptr> >(), "schema"_a,
                     "output_builders"_a);
    }
} // namespace hgraph