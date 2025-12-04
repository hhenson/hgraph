#include <hgraph/types/graph.h>
#include <hgraph/types/tsw.h>
#include <type_traits>

namespace hgraph
{
    // Template method definitions
    template <typename T> nb::object TimeSeriesFixedWindowOutput<T>::py_value() const {
        if (!all_valid()) return nb::none();
        return nb::cast(value());
    }

    template <typename T> std::vector<T> TimeSeriesFixedWindowOutput<T>::value() const {
        std::vector<T> out;
        if (_length < _size) {
            // No rotation has occurred; use the first _length elements
            out.assign(_buffer.begin(), _buffer.begin() + _length);
        } else {
            out.reserve(_size); // Only allocate, don't construct

            size_t first_chunk = _size - _start;
            // Copy from start to end of buffer
            out.insert(out.end(), _buffer.begin() + _start, _buffer.end());
            // Copy from beginning of buffer to the rest
            out.insert(out.end(), _buffer.begin(), _buffer.begin() + (_size - first_chunk));
        }
        return out;
    }

    template <typename T> nb::object TimeSeriesFixedWindowOutput<T>::py_delta_value() const {
        auto dv{delta_value()};
        if (dv.has_value()){return nb::cast(*dv);}
        return nb::none();
    }

    template <typename T> std::optional<T> TimeSeriesFixedWindowOutput<T>::delta_value() const {
        if (_length == 0) return {};
        size_t pos = (_length < _size) ? (_length - 1) : ((_start + _length - 1) % _size);
        if (_times[pos] == owning_graph()->evaluation_time()) {
            if constexpr (std::is_same_v<T, bool>) {
                bool v = static_cast<bool>(_buffer[pos]);
                return {v};
            } else {
                return {_buffer[pos]};
            }
        }
        return {};
    }

    template <typename T> void TimeSeriesFixedWindowOutput<T>::py_set_value(const nb::object& value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        try {
            set_value(nb::cast<T>(value));
        } catch (const std::exception &e) { throw std::runtime_error(std::string("Cannot apply node output: ") + e.what()); }
    }

    template <typename T> void TimeSeriesFixedWindowOutput<T>::set_value(const T &value) {
        _length += 1;
        if (_length > _size) {
            _removed_value.reset();
            _removed_value = _buffer[_start];
            auto weak_self = weak_from_this();
            owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self]() {
                if (auto self = weak_self.lock()) {
                    static_cast<TimeSeriesFixedWindowOutput *>(self.get())->_removed_value.reset();
                }
            });
            _start  = (_start + 1) % _size;
            _length = _size;
        }
        size_t pos   = (_start + _length - 1) % _size;
        _buffer[pos] = value;
        _times[pos]  = owning_graph()->evaluation_time();
        mark_modified();
    }

    template <typename T> void TimeSeriesFixedWindowOutput<T>::apply_result(const nb::object& value) {
        if (!value.is_valid() || value.is_none()) return;
        py_set_value(value);
    }

    template <typename T> void TimeSeriesFixedWindowOutput<T>::mark_invalid() {
        _start  = 0;
        _length = 0;
        _buffer.clear();  // May have values that require destruction. (i.e. nb::object)
        BaseTimeSeriesOutput::mark_invalid();
    }
    template <typename T> bool TimeSeriesFixedWindowOutput<T>::all_valid() const { return valid() && _length >= _min_size; }

    template <typename T> nb::object TimeSeriesFixedWindowOutput<T>::py_value_times() const {
        return nb::cast(value_times());
    }

    template <typename T> std::vector<engine_time_t> TimeSeriesFixedWindowOutput<T>::value_times() const {
        // Simplify to work with _length and not _size
        std::vector<engine_time_t> out;
        if (_length < _size) {
            out.assign(_times.begin(), _times.begin() + _length);
        } else {
            out.reserve(_size);
            auto split = _times.begin() + _start;
            out.insert(out.end(), split, _times.end());
            out.insert(out.end(), _times.begin(), split);
        }
        return out;
    }

    template <typename T> engine_time_t TimeSeriesFixedWindowOutput<T>::first_modified_time() const {
        return _times.empty() ? engine_time_t{} : _times[_start];
    }


    // Unified TimeSeriesWindowInput implementation
    template <typename T> bool TimeSeriesWindowInput<T>::all_valid() const {
        if (!valid()) return false;
        if (auto *f = as_fixed_output()) return f->len() >= f->min_size();
        if (auto *t = as_time_output()) {
            // For time windows, check if enough time has passed
            auto elapsed =
                owning_graph()->evaluation_time() - owning_graph()->evaluation_engine_api()->start_time();
            return elapsed >= t->min_size();
        }
        return false;
    }

    // TimeSeriesTimeWindowOutput implementation
    template <typename T> void TimeSeriesTimeWindowOutput<T>::_roll() const {
        auto tm = owning_graph()->evaluation_time() - _size;
        if (!_times.empty() && _times.front() < tm) {
            std::vector<T> removed;
            while (!_times.empty() && _times.front() < tm) {
                _times.pop_front();
                removed.push_back(_buffer.front());
                _buffer.pop_front();
            }
            _removed_values = std::move(removed);
            auto weak_self  = std::const_pointer_cast<TimeSeriesOutput>(shared_from_this());
            owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self = std::weak_ptr(weak_self)]() {
                if (auto self = weak_self.lock()) {
                    static_cast<TimeSeriesTimeWindowOutput<T> *>(self.get())->_reset_removed_values();
                }
            });
        }
    }

    template <typename T> void TimeSeriesTimeWindowOutput<T>::_reset_removed_values() { _removed_values.clear(); }

    template <typename T> bool TimeSeriesTimeWindowOutput<T>::has_removed_value() const {
        _roll();
        return !_removed_values.empty();
    }

    template <typename T> nb::object TimeSeriesTimeWindowOutput<T>::removed_value() const {
        _roll();
        if (_removed_values.empty()) return nb::none();
        // Return the removed values as a tuple/list
        return nb::cast(_removed_values);
    }

    template <typename T> size_t TimeSeriesTimeWindowOutput<T>::len() const {
        _roll();
        return _buffer.size();
    }

    template <typename T> nb::object TimeSeriesTimeWindowOutput<T>::py_value() const {
        if (!_ready) {
            // Check if enough time has passed
            auto elapsed =
                owning_graph()->evaluation_time() - owning_graph()->evaluation_engine_api()->start_time();
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

    template <typename T> nb::object TimeSeriesTimeWindowOutput<T>::py_delta_value() const {
        // Check if enough time has passed to make the window ready
        if (!_ready) {
            auto elapsed =
                owning_graph()->evaluation_time() - owning_graph()->evaluation_engine_api()->start_time();
            if (elapsed >= _min_size) { _ready = true; }
        }

        if (_ready && !_times.empty()) {
            auto current_time = owning_graph()->evaluation_time();
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

    template <typename T> void TimeSeriesTimeWindowOutput<T>::py_set_value(const nb::object& value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        // This should not be called for time windows - they only append
        throw std::runtime_error("py_set_value should not be called on TimeSeriesTimeWindowOutput");
    }

    template <typename T> void TimeSeriesTimeWindowOutput<T>::apply_result(const nb::object& value) {
        if (!value.is_valid() || value.is_none()) return;
        try {
            T v = nb::cast<T>(value);
            _buffer.push_back(v);
            _times.push_back(owning_graph()->evaluation_time());
            mark_modified();
        } catch (const std::exception &e) { throw std::runtime_error(std::string("Cannot apply node output: ") + e.what()); }
    }

    template <typename T> void TimeSeriesTimeWindowOutput<T>::mark_invalid() {
        _buffer.clear();
        _times.clear();
        _ready = false;
        BaseTimeSeriesOutput::mark_invalid();
    }

    template <typename T> nb::object TimeSeriesTimeWindowOutput<T>::py_value_times() const {
        _roll();
        if (_times.empty()) return nb::none();
        std::vector<engine_time_t> out(_times.begin(), _times.end());
        return nb::cast(out);
    }

    template <typename T> engine_time_t TimeSeriesTimeWindowOutput<T>::first_modified_time() const {
        _roll();
        return _times.empty() ? engine_time_t{} : _times.front();
    }

    template <typename T> void TimeSeriesTimeWindowOutput<T>::copy_from_input(const TimeSeriesInput &input) {
        auto &i = dynamic_cast<const TimeSeriesWindowInput<T> &>(input);
        if (auto *src = i.as_time_output()) {
            _buffer   = src->_buffer;
            _times    = src->_times;
            _size     = src->_size;
            _min_size = src->_min_size;
            _ready    = src->_ready;
            mark_modified();
        } else {
            throw std::runtime_error("TimeSeriesTimeWindowOutput::copy_from_input: input output is not time window");
        }
    }



    // Template instantiations for unified TimeSeriesWindowInput
    template struct TimeSeriesWindowInput<bool>;
    template struct TimeSeriesWindowInput<int64_t>;
    template struct TimeSeriesWindowInput<double>;
    template struct TimeSeriesWindowInput<engine_date_t>;
    template struct TimeSeriesWindowInput<engine_time_t>;
    template struct TimeSeriesWindowInput<engine_time_delta_t>;
    template struct TimeSeriesWindowInput<nb::object>;

    // Template instantiations for TimeSeriesFixedWindowOutput
    template struct TimeSeriesFixedWindowOutput<bool>;
    template struct TimeSeriesFixedWindowOutput<int64_t>;
    template struct TimeSeriesFixedWindowOutput<double>;
    template struct TimeSeriesFixedWindowOutput<engine_date_t>;
    template struct TimeSeriesFixedWindowOutput<engine_time_t>;
    template struct TimeSeriesFixedWindowOutput<engine_time_delta_t>;
    template struct TimeSeriesFixedWindowOutput<nb::object>;

    // Template instantiations for TimeSeriesTimeWindowOutput
    template struct TimeSeriesTimeWindowOutput<bool>;
    template struct TimeSeriesTimeWindowOutput<int64_t>;
    template struct TimeSeriesTimeWindowOutput<double>;
    template struct TimeSeriesTimeWindowOutput<engine_date_t>;
    template struct TimeSeriesTimeWindowOutput<engine_time_t>;
    template struct TimeSeriesTimeWindowOutput<engine_time_delta_t>;
    template struct TimeSeriesTimeWindowOutput<nb::object>;
}  // namespace hgraph