#include <hgraph/builders/time_series_types/time_series_bundle_output_builder.h>
#include <hgraph/builders/builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

#include <ranges>
#include <utility>

namespace hgraph {
    TimeSeriesBundleOutputBuilder::TimeSeriesBundleOutputBuilder(time_series_schema_s_ptr schema,
                                                                 std::vector<OutputBuilder::ptr> output_builders)
        : OutputBuilder(), schema{std::move(schema)}, output_builders{std::move(output_builders)} {
    }

    time_series_output_s_ptr TimeSeriesBundleOutputBuilder::make_instance(node_ptr owning_node) const {
        auto v = arena_make_shared_as<TimeSeriesBundleOutput, TimeSeriesOutput>(owning_node, schema);
        return make_and_set_outputs(v);
    }

    time_series_output_s_ptr TimeSeriesBundleOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        auto v = arena_make_shared_as<TimeSeriesBundleOutput, TimeSeriesOutput>(owning_output, schema);
        return make_and_set_outputs(v);
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
        auto bundle = dynamic_cast<TimeSeriesBundleOutput *>(item);
        if (bundle == nullptr) {
            throw std::runtime_error("TimeSeriesBundleOutputBuilder::release_instance: expected TimeSeriesBundleOutput but got different type");
        }
        auto &outputs = bundle->ts_values();
        for (size_t i = 0; i < output_builders.size(); ++i) { output_builders[i]->release_instance(outputs[i].get()); }
    }

    time_series_output_s_ptr TimeSeriesBundleOutputBuilder::make_and_set_outputs(time_series_bundle_output_s_ptr output) const {
        std::vector<time_series_output_s_ptr> outputs;
        outputs.reserve(output_builders.size());
        std::ranges::copy(output_builders | std::views::transform([&](auto &builder) {
                              return builder->make_instance(output.get());
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
