#include <hgraph/builders/time_series_types/time_series_list_input_builder.h>
#include <hgraph/builders/builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsl.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

#include <utility>

namespace hgraph {
    TimeSeriesListInputBuilder::TimeSeriesListInputBuilder(InputBuilder::ptr input_builder, size_t size)
        : input_builder{std::move(input_builder)}, size{size} {
    }

    bool TimeSeriesListInputBuilder::has_reference() const { return input_builder->has_reference(); }

    bool TimeSeriesListInputBuilder::is_same_type(const Builder &other) const {
        if (auto other_b = dynamic_cast<const TimeSeriesListInputBuilder *>(&other)) {
            if (size != other_b->size) { return false; }
            return input_builder->is_same_type(*other_b->input_builder);
        }
        return false;
    }

    size_t TimeSeriesListInputBuilder::memory_size() const {
        // Add canary size to the base list object
        size_t total = add_canary_size(sizeof(TimeSeriesListInput));
        // For each element, align and add its size using builder's actual type alignment
        for (size_t i = 0; i < size; ++i) {
            total = align_size(total, input_builder->type_alignment());
            total += input_builder->memory_size();
        }
        return total;
    }

    size_t TimeSeriesListInputBuilder::type_alignment() const {
        return alignof(TimeSeriesListInput);
    }

    void TimeSeriesListInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_ < TimeSeriesListInputBuilder, InputBuilder > (m, "InputBuilder_TSL")
                .def(nb::init<InputBuilder::ptr, size_t>(), "input_builder"_a, "size"_a);
    }
} // namespace hgraph
