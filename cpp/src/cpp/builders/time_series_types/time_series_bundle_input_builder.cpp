#include <hgraph/builders/time_series_types/time_series_bundle_input_builder.h>
#include <hgraph/builders/builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>

#include <ranges>
#include <utility>

namespace hgraph {
    TimeSeriesBundleInputBuilder::TimeSeriesBundleInputBuilder(TimeSeriesSchema::ptr schema,
                                                               std::vector<InputBuilder::ptr> input_builders)
        : InputBuilder(), schema{std::move(schema)}, input_builders{std::move(input_builders)} {
    }

    time_series_input_ptr TimeSeriesBundleInputBuilder::make_instance(node_ptr owning_node, std::shared_ptr<void> buffer, size_t* offset) const {
        auto result = make_instance_impl<TimeSeriesBundleInput, TimeSeriesInput>(
            buffer, offset, "TimeSeriesBundleInput", owning_node, schema);
        auto bundle_input = std::static_pointer_cast<TimeSeriesBundleInput>(result);
        return make_and_set_inputs(bundle_input, buffer, offset);
    }

    time_series_input_ptr TimeSeriesBundleInputBuilder::make_instance(time_series_input_ptr owning_input, std::shared_ptr<void> buffer, size_t* offset) const {
        // Convert owning_input to TimeSeriesType shared_ptr
        auto owning_ts = std::dynamic_pointer_cast<TimeSeriesType>(owning_input);
        if (!owning_ts) {
            throw std::runtime_error("TimeSeriesBundleInputBuilder: owning_input must be a TimeSeriesType");
        }
        auto result = make_instance_impl<TimeSeriesBundleInput, TimeSeriesInput>(
            buffer, offset, "TimeSeriesBundleInput", owning_ts, schema);
        auto bundle_input = std::static_pointer_cast<TimeSeriesBundleInput>(result);
        return make_and_set_inputs(bundle_input, buffer, offset);
    }

    bool TimeSeriesBundleInputBuilder::has_reference() const {
        return std::ranges::any_of(input_builders, [](const auto &builder) { return builder->has_reference(); });
    }

    bool TimeSeriesBundleInputBuilder::is_same_type(const Builder &other) const {
        if (auto other_b = dynamic_cast<const TimeSeriesBundleInputBuilder *>(&other)) {
            if (input_builders.size() != other_b->input_builders.size()) { return false; }
            for (size_t i = 0; i < input_builders.size(); ++i) {
                if (!input_builders[i]->is_same_type(*other_b->input_builders[i])) { return false; }
            }
            return true;
        }
        return false;
    }

    void TimeSeriesBundleInputBuilder::release_instance(time_series_input_ptr item) const {
        InputBuilder::release_instance(item);
        auto bundle = dynamic_cast<TimeSeriesBundleInput *>(item.get());
        if (bundle == nullptr) { return; }
        for (size_t i = 0; i < input_builders.size(); ++i) {
            input_builders[i]->release_instance(bundle->_ts_values[i]);
        }
    }

    time_series_input_ptr TimeSeriesBundleInputBuilder::make_and_set_inputs(std::shared_ptr<TimeSeriesBundleInput> input, std::shared_ptr<void> buffer, size_t* offset) const {
        std::vector<time_series_input_ptr> inputs;
        inputs.reserve(input_builders.size());
        std::ranges::copy(input_builders | std::views::transform([&](auto &builder) {
                              return builder->make_instance(input, buffer, offset);
                          }),
                          std::back_inserter(inputs));
        input->set_ts_values(inputs);
        return input;
    }

    size_t TimeSeriesBundleInputBuilder::memory_size() const {
        // Add canary size to the base bundle object
        size_t total = add_canary_size(sizeof(TimeSeriesBundleInput));
        // Align before each nested time-series input
        for (const auto &builder : input_builders) {
            total = align_size(total, alignof(TimeSeriesType));
            total += builder->memory_size();
        }
        return total;
    }

    void TimeSeriesBundleInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_ < TimeSeriesBundleInputBuilder, InputBuilder > (m, "InputBuilder_TSB")
                .def(nb::init<TimeSeriesSchema::ptr, std::vector<InputBuilder::ptr> >(), "schema"_a,
                     "input_builders"_a);
    }
} // namespace hgraph