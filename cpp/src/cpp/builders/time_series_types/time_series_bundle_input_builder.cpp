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

    size_t TimeSeriesBundleInputBuilder::memory_size() const {
        // Add canary size to the base bundle object
        size_t total = add_canary_size(sizeof(TimeSeriesBundleInput));
        // Align before each nested time-series input using builder's actual type alignment
        for (const auto &builder : input_builders) {
            total = align_size(total, builder->type_alignment());
            total += builder->memory_size();
        }
        return total;
    }

    size_t TimeSeriesBundleInputBuilder::type_alignment() const {
        return alignof(TimeSeriesBundleInput);
    }

    void TimeSeriesBundleInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_ < TimeSeriesBundleInputBuilder, InputBuilder > (m, "InputBuilder_TSB")
                .def(nb::init<TimeSeriesSchema::ptr, std::vector<InputBuilder::ptr> >(), "schema"_a,
                     "input_builders"_a);
    }
} // namespace hgraph
