#include <hgraph/builders/time_series_types/time_series_signal_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts_signal.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {
    time_series_input_s_ptr TimeSeriesSignalInputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesSignalInput, TimeSeriesInput>(owning_node);
    }

    time_series_input_s_ptr TimeSeriesSignalInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return arena_make_shared_as<TimeSeriesSignalInput, TimeSeriesInput>(owning_input);
    }

    void TimeSeriesSignalInputBuilder::release_instance(time_series_input_ptr item) const {
        release_instance(dynamic_cast<TimeSeriesSignalInput *>(item));
    }

    void TimeSeriesSignalInputBuilder::release_instance(TimeSeriesSignalInput *signal_input) const {
        if (signal_input == nullptr) {
            throw std::runtime_error("TimeSeriesSignalInputBuilder::release_instance: expected TimeSeriesSignalInput but got different type");
        }
        InputBuilder::release_instance(signal_input);
        if (signal_input->_ts_values.empty()) { return; }
        for (auto &ts_value: signal_input->_ts_values) { release_instance(ts_value.get()); }
    }

    size_t TimeSeriesSignalInputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesSignalInput));
    }

    void TimeSeriesSignalInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSignalInputBuilder, InputBuilder>(m, "InputBuilder_TS_Signal").def(nb::init<>());
    }
} // namespace hgraph
