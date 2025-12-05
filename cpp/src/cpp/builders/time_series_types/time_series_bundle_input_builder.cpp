#include <hgraph/builders/time_series_types/time_series_bundle_input_builder.h>
#include <hgraph/builders/builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

#include <ranges>
#include <utility>

namespace hgraph {
    TimeSeriesBundleInputBuilder::TimeSeriesBundleInputBuilder(time_series_schema_s_ptr schema,
                                                               std::vector<InputBuilder::ptr> input_builders)
        : InputBuilder(), schema{std::move(schema)}, input_builders{std::move(input_builders)} {
    }

    time_series_input_s_ptr TimeSeriesBundleInputBuilder::make_instance(node_ptr owning_node) const {
        auto v = arena_make_shared_as<TimeSeriesBundleInput, TimeSeriesInput>(owning_node, schema);
        return make_and_set_inputs(v);
    }

    time_series_input_s_ptr TimeSeriesBundleInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto v = arena_make_shared_as<TimeSeriesBundleInput, TimeSeriesInput>(owning_input, schema);
        return make_and_set_inputs(v);
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
        auto bundle = dynamic_cast<TimeSeriesBundleInput *>(item);
        if (bundle == nullptr) {
            throw std::runtime_error("TimeSeriesBundleInputBuilder::release_instance: expected TimeSeriesBundleInput but got different type");
        }
        for (size_t i = 0; i < input_builders.size(); ++i) {
            input_builders[i]->release_instance(bundle->_ts_values[i].get());
        }
    }

    time_series_input_s_ptr TimeSeriesBundleInputBuilder::make_and_set_inputs(time_series_bundle_input_s_ptr input) const {
        TimeSeriesBundleInput::collection_type inputs;
        inputs.reserve(input_builders.size());
        for (const auto &builder : input_builders) {
            inputs.push_back(builder->make_instance(input.get()));
        }
        input->set_ts_values(std::move(inputs));
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
