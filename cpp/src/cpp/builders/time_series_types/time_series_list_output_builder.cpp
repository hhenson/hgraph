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

    bool TimeSeriesListOutputBuilder::is_same_type(const Builder &other) const {
        if (auto other_b = dynamic_cast<const TimeSeriesListOutputBuilder *>(&other)) {
            return output_builder->is_same_type(*other_b->output_builder);
        }
        return false;
    }

    size_t TimeSeriesListOutputBuilder::memory_size() const {
        // Add canary size to the base list object
        size_t total = add_canary_size(sizeof(TimeSeriesListOutput));
        // For each element, align and add its size using builder's actual type alignment
        for (size_t i = 0; i < size; ++i) {
            total = align_size(total, output_builder->type_alignment());
            total += output_builder->memory_size();
        }
        return total;
    }

    size_t TimeSeriesListOutputBuilder::type_alignment() const {
        return alignof(TimeSeriesListOutput);
    }

    void TimeSeriesListOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_ < TimeSeriesListOutputBuilder, OutputBuilder > (m, "OutputBuilder_TSL")
                .def(nb::init<OutputBuilder::ptr, size_t>(), "output_builder"_a, "size"_a);
    }
} // namespace hgraph