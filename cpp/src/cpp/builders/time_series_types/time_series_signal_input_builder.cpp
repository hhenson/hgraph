#include <hgraph/builders/time_series_types/time_series_signal_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts_signal.h>

namespace hgraph {
    time_series_input_ptr TimeSeriesSignalInputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesSignalInput(owning_node)};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    time_series_input_ptr TimeSeriesSignalInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesSignalInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    void TimeSeriesSignalInputBuilder::release_instance(time_series_input_ptr item) const {
        release_instance(dynamic_cast<TimeSeriesSignalInput *>(item.get()));
    }

    void TimeSeriesSignalInputBuilder::release_instance(TimeSeriesSignalInput *signal_input) const {
        if (signal_input == nullptr) { return; }
        InputBuilder::release_instance(signal_input);
        if (signal_input->_ts_values.empty()) { return; }
        for (auto &ts_value: signal_input->_ts_values) { release_instance(ts_value.get()); }
    }

    void TimeSeriesSignalInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSignalInputBuilder, InputBuilder>(m, "InputBuilder_TS_Signal").def(nb::init<>());
    }
} // namespace hgraph