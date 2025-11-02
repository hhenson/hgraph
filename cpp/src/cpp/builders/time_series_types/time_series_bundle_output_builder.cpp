#include <hgraph/builders/time_series_types/time_series_bundle_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>

#include <ranges>
#include <utility>

namespace hgraph {
    TimeSeriesBundleOutputBuilder::TimeSeriesBundleOutputBuilder(TimeSeriesSchema::ptr schema,
                                                                 std::vector<OutputBuilder::ptr> output_builders)
        : OutputBuilder(), schema{std::move(schema)}, output_builders{std::move(output_builders)} {
    }

    time_series_output_ptr TimeSeriesBundleOutputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesBundleOutput{owning_node, schema}};
        return make_and_set_outputs(v);
    }

    time_series_output_ptr TimeSeriesBundleOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesBundleOutput(dynamic_cast_ref<TimeSeriesType>(owning_output), schema)};
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
        auto bundle = dynamic_cast<TimeSeriesBundleOutput *>(item.get());
        if (bundle) {
            auto &outputs = bundle->ts_values();
            for (size_t i = 0; i < output_builders.size(); ++i) { output_builders[i]->release_instance(outputs[i]); }
        }
    }

    time_series_output_ptr TimeSeriesBundleOutputBuilder::make_and_set_outputs(TimeSeriesBundleOutput *output) const {
        std::vector<time_series_output_ptr> outputs;
        time_series_output_ptr output_{output};
        outputs.reserve(output_builders.size());
        std::ranges::copy(output_builders | std::views::transform([&](auto &builder) {
                              return builder->make_instance(output_);
                          }),
                          std::back_inserter(outputs));
        output->set_ts_values(outputs);
        return output_;
    }

    void TimeSeriesBundleOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_ < TimeSeriesBundleOutputBuilder, OutputBuilder > (m, "OutputBuilder_TSB")
                .def(nb::init<TimeSeriesSchema::ptr, std::vector<OutputBuilder::ptr> >(), "schema"_a,
                     "output_builders"_a);
    }
} // namespace hgraph