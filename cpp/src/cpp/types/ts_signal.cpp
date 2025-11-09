#include <hgraph/types/ts_signal.h>

namespace hgraph {
    nb::object TimeSeriesSignalInput::py_value() const { return nb::cast(modified()); }

    nb::object TimeSeriesSignalInput::py_delta_value() const { return py_value(); }

    TimeSeriesInput *TimeSeriesSignalInput::get_input(size_t index) {
        // This signal has been bound to a free bundle or a TSL so will be bound item-wise
        // Create child signals on demand, similar to Python implementation
        while (index >= _ts_values.size()) {
            // Create child with this as parent - child will notify parent, parent notifies node
            auto new_item = new TimeSeriesSignalInput(TimeSeriesType::ptr{this});
            if (active()) { new_item->make_active(); }
            _ts_values.push_back(new_item);
        }
        return _ts_values[index].get();
    }

    bool TimeSeriesSignalInput::valid() const {
        if (!_ts_values.empty()) {
            return std::ranges::any_of(_ts_values, [](const auto &item) { return item->valid(); });
        }
        return BaseTimeSeriesInput::valid();
    }

    bool TimeSeriesSignalInput::modified() const {
        if (!_ts_values.empty()) {
            return std::ranges::any_of(_ts_values, [](const auto &item) { return item->modified(); });
        }
        return BaseTimeSeriesInput::modified();
    }

    engine_time_t TimeSeriesSignalInput::last_modified_time() const {
        if (!_ts_values.empty()) {
            engine_time_t max_time = MIN_DT;
            for (const auto &item: _ts_values) { max_time = std::max(max_time, item->last_modified_time()); }
            return max_time;
        }
        return BaseTimeSeriesInput::last_modified_time();
    }

    void TimeSeriesSignalInput::make_active() {
        if (active()) { return; }
        BaseTimeSeriesInput::make_active();
        if (!_ts_values.empty()) {
            for (auto &item: _ts_values) { item->make_active(); }
        }
    }

    void TimeSeriesSignalInput::make_passive() {
        if (!active()) { return; }
        BaseTimeSeriesInput::make_passive();
        if (!_ts_values.empty()) {
            for (auto &item: _ts_values) { item->make_passive(); }
        }
    }

    void TimeSeriesSignalInput::do_un_bind_output(bool unbind_refs) {
        if (!_ts_values.empty()) {
            for (auto &item: _ts_values) { item->un_bind_output(unbind_refs); }
        }
    }

    void TimeSeriesSignalInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSignalInput, BaseTimeSeriesInput>(m, "TS_Signal")
                .def("__getitem__", [](TimeSeriesSignalInput &self, size_t index) { return self.get_input(index); });
    }
} // namespace hgraph