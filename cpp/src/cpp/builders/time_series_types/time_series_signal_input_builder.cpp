#include <hgraph/builders/time_series_types/time_series_signal_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts_signal.h>

namespace hgraph {
    time_series_input_ptr TimeSeriesSignalInputBuilder::make_instance(node_ptr owning_node, std::shared_ptr<void> buffer, size_t* offset) const {
        return make_instance_impl<TimeSeriesSignalInput, TimeSeriesInput>(
            buffer, offset, "TimeSeriesSignalInput", owning_node);
    }

    time_series_input_ptr TimeSeriesSignalInputBuilder::make_instance(time_series_input_ptr owning_input, std::shared_ptr<void> buffer, size_t* offset) const {
        // Convert owning_input to TimeSeriesType shared_ptr
        auto owning_ts = std::dynamic_pointer_cast<TimeSeriesType>(owning_input);
        if (!owning_ts) {
            throw std::runtime_error("TimeSeriesSignalInputBuilder: owning_input must be a TimeSeriesType");
        }
        return make_instance_impl<TimeSeriesSignalInput, TimeSeriesInput>(
            buffer, offset, "TimeSeriesSignalInput", owning_ts);
    }

    void TimeSeriesSignalInputBuilder::release_instance(time_series_input_ptr item) const {
        release_instance(dynamic_cast<TimeSeriesSignalInput *>(item.get()));
    }

    void TimeSeriesSignalInputBuilder::release_instance(TimeSeriesSignalInput *signal_input) const {
        if (signal_input == nullptr) { return; }
        // Convert raw pointer to shared_ptr for InputBuilder::release_instance
        time_series_input_ptr input_ptr = std::static_pointer_cast<TimeSeriesInput>(
            std::shared_ptr<TimeSeriesSignalInput>(signal_input, [](TimeSeriesSignalInput*){}));
        InputBuilder::release_instance(input_ptr);
        if (signal_input->_ts_values.empty()) { return; }
        for (auto &ts_value: signal_input->_ts_values) { release_instance(ts_value); }
    }

    size_t TimeSeriesSignalInputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesSignalInput));
    }

    void TimeSeriesSignalInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSignalInputBuilder, InputBuilder>(m, "InputBuilder_TS_Signal").def(nb::init<>());
    }
} // namespace hgraph