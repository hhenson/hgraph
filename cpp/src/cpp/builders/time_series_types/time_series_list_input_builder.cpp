#include <hgraph/builders/time_series_types/time_series_list_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsl.h>

#include <utility>

namespace hgraph {
    TimeSeriesListInputBuilder::TimeSeriesListInputBuilder(InputBuilder::ptr input_builder, size_t size)
        : input_builder{std::move(input_builder)}, size{size} {
    }

    time_series_input_ptr TimeSeriesListInputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesListInput{owning_node}};
        return make_and_set_inputs(v);
    }

    time_series_input_ptr TimeSeriesListInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesListInput{dynamic_cast_ref<TimeSeriesType>(owning_input)}};
        return make_and_set_inputs(v);
    }

    bool TimeSeriesListInputBuilder::has_reference() const { return input_builder->has_reference(); }

    bool TimeSeriesListInputBuilder::is_same_type(const Builder &other) const {
        if (auto other_b = dynamic_cast<const TimeSeriesListInputBuilder *>(&other)) {
            if (size != other_b->size) { return false; }
            return input_builder->is_same_type(*other_b->input_builder);
        }
        return false;
    }

    void TimeSeriesListInputBuilder::release_instance(time_series_input_ptr item) const {
        InputBuilder::release_instance(item);
        auto list = dynamic_cast<TimeSeriesListInput *>(item.get());
        if (list == nullptr) { return; }
        for (auto &value: list->_ts_values) { input_builder->release_instance(value); }
    }

    time_series_input_ptr TimeSeriesListInputBuilder::make_and_set_inputs(TimeSeriesListInput *input) const {
        std::vector<time_series_input_ptr> inputs;
        inputs.reserve(size);
        for (size_t i = 0; i < size; ++i) { inputs.push_back(input_builder->make_instance(input)); }
        input->set_ts_values(inputs);
        return input;
    }

    void TimeSeriesListInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_ < TimeSeriesListInputBuilder, InputBuilder > (m, "InputBuilder_TSL")
                .def(nb::init<InputBuilder::ptr, size_t>(), "input_builder"_a, "size"_a);
    }
} // namespace hgraph