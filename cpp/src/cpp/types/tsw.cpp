#include <hgraph/types/tsw.h>
#include <hgraph/types/graph.h>
#include <type_traits>

namespace hgraph {
    // Template method definitions
    template<typename T>
    nb::object TimeSeriesFixedWindowOutput<T>::py_value() const {
        if (!valid() || _length < _min_size) return nb::none();

        // Build a materialized Python sequence in correct chronological order.
        // This mirrors the Python reference behavior and avoids zero-copy view
        // issues that can expose uninitialized/rotated storage.
        std::vector<T> out;
        if (_length < _size) {
            // No rotation has occurred; use the first _length elements
            out.assign(_buffer.begin(), _buffer.begin() + _length);
        } else {
            // Full window; materialize from the logical start
            out.resize(_size);
            for (size_t i = 0; i < _size; ++i) out[i] = _buffer[(i + _start) % _size];
        }
        return nb::cast(out);
    }

    template<typename T>
    nb::object TimeSeriesFixedWindowOutput<T>::py_delta_value() const {
        if (_length == 0) return nb::none();
        size_t pos = (_length < _size) ? (_length - 1) : ((_start + _length - 1) % _size);
        if (_times[pos] == owning_graph()->evaluation_clock()->evaluation_time()) {
            if constexpr (std::is_same_v<T, bool>) {
                bool v = static_cast<bool>(_buffer[pos]);
                return nb::cast(v);
            } else {
                return nb::cast(_buffer[pos]);
            }
        } else {
            return nb::none();
        }
    }

    template<typename T>
    void TimeSeriesFixedWindowOutput<T>::py_set_value(nb::object value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        try {
            T v = nb::cast<T>(value);
            size_t capacity = _size;
            size_t start = _start;
            size_t length = _length + 1;
            if (length > capacity) {
                _removed_value.reset();
                _removed_value = _buffer[start];
                owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([this]() {
                    _removed_value.reset();
                });
                start = (start + 1) % capacity;
                _start = start;
                length = capacity;
            }
            _length = length;
            size_t pos = (start + length - 1) % capacity;
            _buffer[pos] = v;
            _times[pos] = owning_graph()->evaluation_clock()->evaluation_time();
            mark_modified();
        } catch (const std::exception &e) {
            throw std::runtime_error(std::string("Cannot apply node output: ") + e.what());
        }
    }

    template<typename T>
    void TimeSeriesFixedWindowOutput<T>::apply_result(nb::object value) {
        if (!value.is_valid() || value.is_none()) return;
        py_set_value(value);
    }

    template<typename T>
    void TimeSeriesFixedWindowOutput<T>::mark_invalid() {
        _start = 0;
        _length = 0;
        BaseTimeSeriesOutput::mark_invalid();
    }

    template<typename T>
    nb::object TimeSeriesFixedWindowOutput<T>::py_value_times() const {
        if (!valid() || _length < _min_size) return nb::none();

        // Mirror value() semantics: if contiguous from start, return the active portion.
        if (_start == 0) {
            size_t len = (_length < _size) ? _length : _size;
            if (len == 0) return nb::none();
            std::vector<engine_time_t> out(_times.begin(), _times.begin() + len);
            return nb::cast(out);
        }

        // General path: build ordered times (copy semantics for rotation/partial)
        std::vector<engine_time_t> out;
        if (_length < _size) {
            out.assign(_times.begin(), _times.begin() + _length);
        } else {
            out.resize(_size);
            for (size_t i = 0; i < _size; ++i) out[i] = _times[(i + _start) % _size];
        }
        return nb::cast(out);
    }

    template<typename T>
    engine_time_t TimeSeriesFixedWindowOutput<T>::first_modified_time() const {
        return _times.empty() ? engine_time_t{} : _times[_start];
    }

    template<typename T>
    static void bind_tsw_for_type(nb::module_ &m, const char *suffix) {
        using Out = TimeSeriesFixedWindowOutput<T>;
        using In = TimeSeriesWindowInput<T>;

        auto out_cls = nb::class_<Out, TimeSeriesOutput>(
                    m, (std::string("TimeSeriesFixedWindowOutput_") + suffix).c_str())
                .def_prop_ro("value_times", &Out::py_value_times)
                .def_prop_ro("first_modified_time", &Out::first_modified_time)
                .def_prop_ro("size", &Out::size)
                .def_prop_ro("min_size", &Out::min_size)
                .def_prop_ro("has_removed_value", &Out::has_removed_value)
                .def_prop_ro("removed_value", [](const Out &o) {
                    return o.has_removed_value() ? nb::cast(o.removed_value()) : nb::none();
                })
                .def("__len__", &Out::len);

        auto in_cls = nb::class_<In, TimeSeriesInput>(m, (std::string("TimeSeriesWindowInput_") + suffix).c_str())
                .def_prop_ro("value_times", &In::py_value_times)
                .def_prop_ro("first_modified_time", &In::first_modified_time)
                .def_prop_ro("has_removed_value", &In::has_removed_value)
                .def_prop_ro("removed_value", &In::removed_value)
                .def("__len__", [](const In &self) {
                    if (auto *f = self.as_fixed_output()) return f->len();
                    if (auto *t = self.as_time_output()) return t->len();
                    throw std::runtime_error("TimeSeriesWindowInput: output is not a window output");
                });

        (void) out_cls;
        (void) in_cls;
    }

    // Unified TimeSeriesWindowInput implementation
    template<typename T>
    bool TimeSeriesWindowInput<T>::all_valid() const {
        if (!valid()) return false;
        if (auto *f = as_fixed_output()) return f->len() >= f->min_size();
        if (auto *t = as_time_output()) {
            // For time windows, check if enough time has passed
            auto elapsed = owning_graph()->evaluation_clock()->evaluation_time() -
                           owning_graph()->evaluation_engine_api()->start_time();
            return elapsed >= t->min_size();
        }
        return false;
    }

    // TimeSeriesTimeWindowOutput implementation
    template<typename T>
    void TimeSeriesTimeWindowOutput<T>::_roll() const {
        auto tm = owning_graph()->evaluation_clock()->evaluation_time() - _size;
        if (!_times.empty() && _times.front() < tm) {
            std::vector<T> removed;
            while (!_times.empty() && _times.front() < tm) {
                _times.pop_front();
                removed.push_back(_buffer.front());
                _buffer.pop_front();
            }
            _removed_values = std::move(removed);
            auto *self = const_cast<TimeSeriesTimeWindowOutput<T> *>(this);
            owning_graph()->evaluation_engine_api()->add_after_evaluation_notification(
                [self]() { self->_reset_removed_values(); });
        }
    }

    template<typename T>
    void TimeSeriesTimeWindowOutput<T>::_reset_removed_values() {
        _removed_values.clear();
    }

    template<typename T>
    bool TimeSeriesTimeWindowOutput<T>::has_removed_value() const {
        _roll();
        return !_removed_values.empty();
    }

    template<typename T>
    nb::object TimeSeriesTimeWindowOutput<T>::removed_value() const {
        _roll();
        if (_removed_values.empty()) return nb::none();
        // Return the removed values as a tuple/list
        return nb::cast(_removed_values);
    }

    template<typename T>
    size_t TimeSeriesTimeWindowOutput<T>::len() const {
        _roll();
        return _buffer.size();
    }

    template<typename T>
    nb::object TimeSeriesTimeWindowOutput<T>::py_value() const {
        if (!_ready) {
            // Check if enough time has passed
            auto elapsed = owning_graph()->evaluation_clock()->evaluation_time() -
                           owning_graph()->evaluation_engine_api()->start_time();
            if (elapsed >= _min_size) {
                _ready = true;
            } else {
                return nb::none();
            }
        }

        _roll();
        if (_buffer.empty()) return nb::none();

        // Convert deque to Python list/array
        std::vector<T> out(_buffer.begin(), _buffer.end());
        return nb::cast(out);
    }

    template<typename T>
    nb::object TimeSeriesTimeWindowOutput<T>::py_delta_value() const {
        // Check if enough time has passed to make the window ready
        if (!_ready) {
            auto elapsed = owning_graph()->evaluation_clock()->evaluation_time() -
                           owning_graph()->evaluation_engine_api()->start_time();
            if (elapsed >= _min_size) {
                _ready = true;
            }
        }

        if (_ready && !_times.empty()) {
            auto current_time = owning_graph()->evaluation_clock()->evaluation_time();
            if (_times.back() == current_time) {
                if constexpr (std::is_same_v<T, bool>) {
                    bool v = static_cast<bool>(_buffer.back());
                    return nb::cast(v);
                } else {
                    return nb::cast(_buffer.back());
                }
            }
        }
        return nb::none();
    }

    template<typename T>
    void TimeSeriesTimeWindowOutput<T>::py_set_value(nb::object value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        // This should not be called for time windows - they only append
        throw std::runtime_error("py_set_value should not be called on TimeSeriesTimeWindowOutput");
    }

    template<typename T>
    void TimeSeriesTimeWindowOutput<T>::apply_result(nb::object value) {
        if (!value.is_valid() || value.is_none()) return;
        try {
            T v = nb::cast<T>(value);
            _buffer.push_back(v);
            _times.push_back(owning_graph()->evaluation_clock()->evaluation_time());
            mark_modified();
        } catch (const std::exception &e) {
            throw std::runtime_error(std::string("Cannot apply node output: ") + e.what());
        }
    }

    template<typename T>
    void TimeSeriesTimeWindowOutput<T>::mark_invalid() {
        _buffer.clear();
        _times.clear();
        _ready = false;
        BaseTimeSeriesOutput::mark_invalid();
    }

    template<typename T>
    nb::object TimeSeriesTimeWindowOutput<T>::py_value_times() const {
        _roll();
        if (_times.empty()) return nb::none();
        std::vector<engine_time_t> out(_times.begin(), _times.end());
        return nb::cast(out);
    }

    template<typename T>
    engine_time_t TimeSeriesTimeWindowOutput<T>::first_modified_time() const {
        _roll();
        return _times.empty() ? engine_time_t{} : _times.front();
    }

    template<typename T>
    void TimeSeriesTimeWindowOutput<T>::copy_from_input(const TimeSeriesInput &input) {
        auto &i = dynamic_cast<const TimeSeriesWindowInput<T> &>(input);
        if (auto *src = i.as_time_output()) {
            _buffer = src->_buffer;
            _times = src->_times;
            _size = src->_size;
            _min_size = src->_min_size;
            _ready = src->_ready;
            mark_modified();
        } else {
            throw std::runtime_error("TimeSeriesTimeWindowOutput::copy_from_input: input output is not time window");
        }
    }

    // Binding functions for time-based windows
    template<typename T>
    static void bind_time_tsw_for_type(nb::module_ &m, const char *suffix) {
        using Out = TimeSeriesTimeWindowOutput<T>;

        auto out_cls = nb::class_<Out, TimeSeriesOutput>(
                    m, (std::string("TimeSeriesTimeWindowOutput_") + suffix).c_str())
                .def_prop_ro("value_times", &Out::py_value_times)
                .def_prop_ro("first_modified_time", &Out::first_modified_time)
                .def_prop_ro("size", &Out::size)
                .def_prop_ro("min_size", &Out::min_size)
                .def_prop_ro("has_removed_value", &Out::has_removed_value)
                .def_prop_ro("removed_value", &Out::removed_value)
                .def("__len__", &Out::len);

        (void) out_cls;
    }

    void tsw_register_with_nanobind(nb::module_ &m) {
        // Fixed-size (tick-based) windows
        bind_tsw_for_type<bool>(m, "bool");
        bind_tsw_for_type<int64_t>(m, "int");
        bind_tsw_for_type<double>(m, "float");
        bind_tsw_for_type<engine_date_t>(m, "date");
        bind_tsw_for_type<engine_time_t>(m, "date_time");
        bind_tsw_for_type<engine_time_delta_t>(m, "time_delta");
        bind_tsw_for_type<nb::object>(m, "object");

        // Time-based (timedelta) windows
        bind_time_tsw_for_type<bool>(m, "bool");
        bind_time_tsw_for_type<int64_t>(m, "int");
        bind_time_tsw_for_type<double>(m, "float");
        bind_time_tsw_for_type<engine_date_t>(m, "date");
        bind_time_tsw_for_type<engine_time_t>(m, "date_time");
        bind_time_tsw_for_type<engine_time_delta_t>(m, "time_delta");
        bind_time_tsw_for_type<nb::object>(m, "object");
    }

    // Template instantiations for unified TimeSeriesWindowInput
    template struct TimeSeriesWindowInput<bool>;
    template struct TimeSeriesWindowInput<int64_t>;
    template struct TimeSeriesWindowInput<double>;
    template struct TimeSeriesWindowInput<engine_date_t>;
    template struct TimeSeriesWindowInput<engine_time_t>;
    template struct TimeSeriesWindowInput<engine_time_delta_t>;
    template struct TimeSeriesWindowInput<nb::object>;
} // namespace hgraph