#include <hgraph/types/graph.h>
#include <hgraph/types/tsw.h>

namespace hgraph
{
    // ========== TimeSeriesFixedWindowOutput Implementation ==========

    TimeSeriesFixedWindowOutput::TimeSeriesFixedWindowOutput(node_ptr parent, size_t size, size_t min_size,
                                                             const value::TypeMeta* element_type)
        : BaseTimeSeriesOutput(parent), _size(size), _min_size(min_size), _element_type(element_type) {
        _buffer.reserve(_size);
        for (size_t i = 0; i < _size; ++i) {
            _buffer.emplace_back(_element_type);
        }
        _times.resize(_size, engine_time_t{});
    }

    TimeSeriesFixedWindowOutput::TimeSeriesFixedWindowOutput(time_series_output_ptr parent, size_t size, size_t min_size,
                                                             const value::TypeMeta* element_type)
        : BaseTimeSeriesOutput(parent), _size(size), _min_size(min_size), _element_type(element_type) {
        _buffer.reserve(_size);
        for (size_t i = 0; i < _size; ++i) {
            _buffer.emplace_back(_element_type);
        }
        _times.resize(_size, engine_time_t{});
    }

    nb::object TimeSeriesFixedWindowOutput::py_value() const {
        if (!all_valid()) return nb::none();

        nb::list result;
        if (_length < _size) {
            // No rotation has occurred; use the first _length elements
            for (size_t i = 0; i < _length; ++i) {
                result.append(_element_type->ops->to_python(_buffer[i].data(), _element_type));
            }
        } else {
            // Buffer is full with rotation
            for (size_t i = 0; i < _size; ++i) {
                size_t idx = (_start + i) % _size;
                result.append(_element_type->ops->to_python(_buffer[idx].data(), _element_type));
            }
        }
        return result;
    }

    nb::object TimeSeriesFixedWindowOutput::py_delta_value() const {
        if (_length == 0) return nb::none();
        size_t pos = (_length < _size) ? (_length - 1) : ((_start + _length - 1) % _size);
        if (_times[pos] == owning_graph()->evaluation_time()) {
            return _element_type->ops->to_python(_buffer[pos].data(), _element_type);
        }
        return nb::none();
    }

    void TimeSeriesFixedWindowOutput::py_set_value(const nb::object& value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        try {
            _length += 1;
            if (_length > _size) {
                _has_removed_value = true;
                _removed_value = value::PlainValue::copy(_buffer[_start]);
                auto weak_self = weak_from_this();
                owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self]() {
                    if (auto self = weak_self.lock()) {
                        auto* tsw = static_cast<TimeSeriesFixedWindowOutput*>(self.get());
                        tsw->_has_removed_value = false;
                    }
                });
                _start  = (_start + 1) % _size;
                _length = _size;
            }
            size_t pos = (_start + _length - 1) % _size;
            _element_type->ops->from_python(_buffer[pos].data(), value, _element_type);
            _times[pos] = owning_graph()->evaluation_time();
            mark_modified();
        } catch (const std::exception &e) {
            throw std::runtime_error(std::string("Cannot apply node output: ") + e.what());
        }
    }

    void TimeSeriesFixedWindowOutput::apply_result(const nb::object& value) {
        if (!value.is_valid() || value.is_none()) return;
        py_set_value(value);
    }

    void TimeSeriesFixedWindowOutput::mark_invalid() {
        _start  = 0;
        _length = 0;
        _buffer.clear();
        _has_removed_value = false;
        BaseTimeSeriesOutput::mark_invalid();
    }

    bool TimeSeriesFixedWindowOutput::all_valid() const {
        return valid() && _length >= _min_size;
    }

    nb::object TimeSeriesFixedWindowOutput::py_value_times() const {
        return nb::cast(value_times());
    }

    std::vector<engine_time_t> TimeSeriesFixedWindowOutput::value_times() const {
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

    engine_time_t TimeSeriesFixedWindowOutput::first_modified_time() const {
        return _times.empty() ? engine_time_t{} : _times[_start];
    }

    void TimeSeriesFixedWindowOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto &o = dynamic_cast<const TimeSeriesFixedWindowOutput &>(output);
        _buffer.clear();
        _buffer.reserve(o._buffer.size());
        for (const auto& v : o._buffer) {
            _buffer.push_back(value::PlainValue::copy(v));
        }
        _times = o._times;
        _start = o._start;
        _length = o._length;
        _size = o._size;
        _min_size = o._min_size;
        _element_type = o._element_type;
        mark_modified();
    }

    void TimeSeriesFixedWindowOutput::copy_from_input(const TimeSeriesInput &input) {
        auto &i = dynamic_cast<const TimeSeriesWindowInput &>(input);
        if (auto *src = i.as_fixed_output()) {
            copy_from_output(*src);
        } else {
            throw std::runtime_error("TimeSeriesFixedWindowOutput::copy_from_input: input output is not fixed window");
        }
    }

    nb::object TimeSeriesFixedWindowOutput::py_removed_value() const {
        if (!_has_removed_value) return nb::none();
        return _element_type->ops->to_python(_removed_value.data(), _element_type);
    }

    void TimeSeriesFixedWindowOutput::reset_value() {
        _buffer.clear();
        _times.clear();
        _has_removed_value = false;
        _start = 0;
        _length = 0;
    }

    // ========== TimeSeriesWindowInput Implementation ==========

    TimeSeriesWindowInput::TimeSeriesWindowInput(const node_ptr &parent, const value::TypeMeta* element_type)
        : BaseTimeSeriesInput(parent), _element_type(element_type) {}

    TimeSeriesWindowInput::TimeSeriesWindowInput(time_series_input_ptr parent, const value::TypeMeta* element_type)
        : BaseTimeSeriesInput(parent), _element_type(element_type) {}

    const value::TypeMeta* TimeSeriesWindowInput::element_type() const {
        // First check local element type (set during construction)
        if (_element_type) return _element_type;
        // Fall back to output's element type if available
        if (auto *f = as_fixed_output()) return f->element_type();
        if (auto *t = as_time_output()) return t->element_type();
        return nullptr;
    }

    nb::object TimeSeriesWindowInput::py_value() const {
        if (auto *f = as_fixed_output()) return f->py_value();
        if (auto *t = as_time_output()) return t->py_value();
        throw std::runtime_error("TimeSeriesWindowInput: output is not a window output");
    }

    nb::object TimeSeriesWindowInput::py_delta_value() const {
        if (auto *f = as_fixed_output()) return f->py_delta_value();
        if (auto *t = as_time_output()) return t->py_delta_value();
        throw std::runtime_error("TimeSeriesWindowInput: output is not a window output");
    }

    bool TimeSeriesWindowInput::all_valid() const {
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

    nb::object TimeSeriesWindowInput::py_value_times() const {
        if (auto *f = as_fixed_output()) return f->py_value_times();
        if (auto *t = as_time_output()) return t->py_value_times();
        throw std::runtime_error("TimeSeriesWindowInput: output is not a window output");
    }

    engine_time_t TimeSeriesWindowInput::first_modified_time() const {
        if (auto *f = as_fixed_output()) return f->first_modified_time();
        if (auto *t = as_time_output()) return t->first_modified_time();
        throw std::runtime_error("TimeSeriesWindowInput: output is not a window output");
    }

    bool TimeSeriesWindowInput::has_removed_value() const {
        if (auto *f = as_fixed_output()) return f->has_removed_value();
        if (auto *t = as_time_output()) return t->has_removed_value();
        return false;
    }

    nb::object TimeSeriesWindowInput::py_removed_value() const {
        if (auto *f = as_fixed_output()) return f->py_removed_value();
        if (auto *t = as_time_output()) return t->py_removed_value();
        return nb::none();
    }

    size_t TimeSeriesWindowInput::len() const {
        if (auto *f = as_fixed_output()) return f->len();
        if (auto *t = as_time_output()) return t->len();
        return 0;
    }

    // ========== TimeSeriesTimeWindowOutput Implementation ==========

    TimeSeriesTimeWindowOutput::TimeSeriesTimeWindowOutput(node_ptr parent, engine_time_delta_t size,
                                                           engine_time_delta_t min_size, const value::TypeMeta* element_type)
        : BaseTimeSeriesOutput(parent), _size(size), _min_size(min_size), _ready(false), _element_type(element_type) {
    }

    TimeSeriesTimeWindowOutput::TimeSeriesTimeWindowOutput(time_series_output_ptr parent, engine_time_delta_t size,
                                                           engine_time_delta_t min_size, const value::TypeMeta* element_type)
        : BaseTimeSeriesOutput(parent), _size(size), _min_size(min_size), _ready(false), _element_type(element_type) {
    }

    void TimeSeriesTimeWindowOutput::_roll() const {
        auto tm = owning_graph()->evaluation_time() - _size;
        if (!_times.empty() && _times.front() < tm) {
            std::vector<value::PlainValue> removed;
            while (!_times.empty() && _times.front() < tm) {
                _times.pop_front();
                removed.push_back(std::move(_buffer.front()));
                _buffer.pop_front();
            }
            _removed_values = std::move(removed);
            auto weak_self = std::const_pointer_cast<TimeSeriesOutput>(shared_from_this());
            owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self = std::weak_ptr(weak_self)]() {
                if (auto self = weak_self.lock()) {
                    static_cast<TimeSeriesTimeWindowOutput*>(self.get())->_reset_removed_values();
                }
            });
        }
    }

    void TimeSeriesTimeWindowOutput::_reset_removed_values() {
        _removed_values.clear();
    }

    bool TimeSeriesTimeWindowOutput::has_removed_value() const {
        _roll();
        return !_removed_values.empty();
    }

    nb::object TimeSeriesTimeWindowOutput::py_removed_value() const {
        _roll();
        if (_removed_values.empty()) return nb::none();
        // Return the removed values as a list
        nb::list result;
        for (const auto& v : _removed_values) {
            result.append(_element_type->ops->to_python(v.data(), _element_type));
        }
        return result;
    }

    size_t TimeSeriesTimeWindowOutput::len() const {
        _roll();
        return _buffer.size();
    }

    nb::object TimeSeriesTimeWindowOutput::py_value() const {
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

        // Convert deque to Python list
        nb::list result;
        for (const auto& v : _buffer) {
            result.append(_element_type->ops->to_python(v.data(), _element_type));
        }
        return result;
    }

    nb::object TimeSeriesTimeWindowOutput::py_delta_value() const {
        // Check if enough time has passed to make the window ready
        if (!_ready) {
            auto elapsed =
                owning_graph()->evaluation_time() - owning_graph()->evaluation_engine_api()->start_time();
            if (elapsed >= _min_size) { _ready = true; }
        }

        if (_ready && !_times.empty()) {
            auto current_time = owning_graph()->evaluation_time();
            if (_times.back() == current_time) {
                return _element_type->ops->to_python(_buffer.back().data(), _element_type);
            }
        }
        return nb::none();
    }

    void TimeSeriesTimeWindowOutput::py_set_value(const nb::object& value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        // This should not be called for time windows - they only append
        throw std::runtime_error("py_set_value should not be called on TimeSeriesTimeWindowOutput");
    }

    void TimeSeriesTimeWindowOutput::apply_result(const nb::object& value) {
        if (!value.is_valid() || value.is_none()) return;
        try {
            value::PlainValue v(_element_type);
            _element_type->ops->from_python(v.data(), value, _element_type);
            _buffer.push_back(std::move(v));
            _times.push_back(owning_graph()->evaluation_time());
            mark_modified();
        } catch (const std::exception &e) {
            throw std::runtime_error(std::string("Cannot apply node output: ") + e.what());
        }
    }

    void TimeSeriesTimeWindowOutput::mark_invalid() {
        _buffer.clear();
        _times.clear();
        _ready = false;
        _removed_values.clear();
        BaseTimeSeriesOutput::mark_invalid();
    }

    nb::object TimeSeriesTimeWindowOutput::py_value_times() const {
        _roll();
        if (_times.empty()) return nb::none();
        std::vector<engine_time_t> out(_times.begin(), _times.end());
        return nb::cast(out);
    }

    engine_time_t TimeSeriesTimeWindowOutput::first_modified_time() const {
        _roll();
        return _times.empty() ? engine_time_t{} : _times.front();
    }

    void TimeSeriesTimeWindowOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto &o = dynamic_cast<const TimeSeriesTimeWindowOutput &>(output);
        _buffer.clear();
        for (const auto& v : o._buffer) {
            _buffer.push_back(value::PlainValue::copy(v));
        }
        _times = o._times;
        _size = o._size;
        _min_size = o._min_size;
        _ready = o._ready;
        _element_type = o._element_type;
        mark_modified();
    }

    void TimeSeriesTimeWindowOutput::copy_from_input(const TimeSeriesInput &input) {
        auto &i = dynamic_cast<const TimeSeriesWindowInput &>(input);
        if (auto *src = i.as_time_output()) {
            copy_from_output(*src);
        } else {
            throw std::runtime_error("TimeSeriesTimeWindowOutput::copy_from_input: input output is not time window");
        }
    }

}  // namespace hgraph
