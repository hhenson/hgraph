#include <hgraph/types/graph.h>
#include <hgraph/types/tsw.h>
#include <hgraph/types/value/cyclic_buffer_ops.h>

#include <cstring>

namespace hgraph
{
    // ========== TimeSeriesFixedWindowOutput Implementation ==========

    TimeSeriesFixedWindowOutput::TimeSeriesFixedWindowOutput(node_ptr parent, size_t size, size_t min_size,
                                                             const value::TypeMeta* element_type)
        : BaseTimeSeriesOutput(parent), _capacity(size + 1), _min_size(min_size), _user_size(size), _element_type(element_type) {
        // Allocate capacity + 1 so removed value stays in buffer until next update
        allocate_buffers();
    }

    TimeSeriesFixedWindowOutput::TimeSeriesFixedWindowOutput(time_series_output_ptr parent, size_t size, size_t min_size,
                                                             const value::TypeMeta* element_type)
        : BaseTimeSeriesOutput(parent), _capacity(size + 1), _min_size(min_size), _user_size(size), _element_type(element_type) {
        // Allocate capacity + 1 so removed value stays in buffer until next update
        allocate_buffers();
    }

    TimeSeriesFixedWindowOutput::~TimeSeriesFixedWindowOutput() {
        deallocate_buffers();
    }

    void TimeSeriesFixedWindowOutput::allocate_buffers() {
        if (_capacity == 0 || !_element_type) return;

        // Allocate contiguous buffer for values (capacity = user_size + 1)
        // The extra slot holds the removed value until next update
        size_t elem_size = _element_type->size;
        _value_storage.capacity = _capacity;
        _value_storage.size = 0;
        _value_storage.head = 0;
        _value_storage.data = std::malloc(_capacity * elem_size);

        if (!_value_storage.data) {
            throw std::bad_alloc();
        }

        // Construct all elements in the buffer (they may be overwritten later)
        for (size_t i = 0; i < _capacity; ++i) {
            void* elem_ptr = static_cast<char*>(_value_storage.data) + i * elem_size;
            if (_element_type->ops && _element_type->ops->construct) {
                _element_type->ops->construct(elem_ptr, _element_type);
            }
        }

        // Allocate timestamps array
        _times.resize(_capacity, engine_time_t{});
    }

    void TimeSeriesFixedWindowOutput::deallocate_buffers() {
        // Destruct and free value buffer
        if (_value_storage.data && _element_type) {
            size_t elem_size = _element_type->size;
            for (size_t i = 0; i < _capacity; ++i) {
                void* elem_ptr = static_cast<char*>(_value_storage.data) + i * elem_size;
                if (_element_type->ops && _element_type->ops->destruct) {
                    _element_type->ops->destruct(elem_ptr, _element_type);
                }
            }
            std::free(_value_storage.data);
            _value_storage.data = nullptr;
        }
        // Note: removed value lives in the buffer, no separate deallocation needed
    }

    void* TimeSeriesFixedWindowOutput::get_value_ptr(size_t logical_index) {
        size_t physical = (_value_storage.head + logical_index) % _capacity;
        return static_cast<char*>(_value_storage.data) + physical * _element_type->size;
    }

    const void* TimeSeriesFixedWindowOutput::get_value_ptr(size_t logical_index) const {
        size_t physical = (_value_storage.head + logical_index) % _capacity;
        return static_cast<const char*>(_value_storage.data) + physical * _element_type->size;
    }

    nb::object TimeSeriesFixedWindowOutput::py_value() const {
        if (!all_valid()) return nb::none();

        nb::list result;
        // Return only active elements in logical order (oldest first)
        // Active count = min(_value_storage.size, _user_size)
        size_t active_count = len();
        for (size_t i = 0; i < active_count; ++i) {
            const void* elem_ptr = get_value_ptr(i);
            result.append(_element_type->ops->to_python(elem_ptr, _element_type));
        }
        return result;
    }

    nb::object TimeSeriesFixedWindowOutput::py_delta_value() const {
        size_t active_count = len();
        if (active_count == 0) return nb::none();

        // Get the newest active element (last logical index)
        size_t last_idx = active_count - 1;
        size_t physical = (_value_storage.head + last_idx) % _capacity;

        if (_times[physical] == owning_graph()->evaluation_time()) {
            const void* elem_ptr = get_value_ptr(last_idx);
            return _element_type->ops->to_python(elem_ptr, _element_type);
        }
        return nb::none();
    }

    void TimeSeriesFixedWindowOutput::py_set_value(const nb::object& value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        try {
            size_t& size = _value_storage.size;
            size_t& head = _value_storage.head;

            // If we previously had a removed value, clear that flag first
            // (the old removed slot is about to be overwritten with the new value)
            _has_removed_value = false;

            if (size < _user_size) {
                // Buffer not yet at user-specified size: add at end
                size_t physical = (head + size) % _capacity;
                void* elem_ptr = static_cast<char*>(_value_storage.data) + physical * _element_type->size;
                _element_type->ops->from_python(elem_ptr, value, _element_type);
                _times[physical] = owning_graph()->evaluation_time();
                size++;
            } else {
                // At user-specified size: the oldest becomes the removed value
                // Write new value at (head + _user_size) % _capacity
                // This is the slot just past the active window (either fresh or the previous removed slot)
                size_t write_pos = (head + _user_size) % _capacity;
                void* elem_ptr = static_cast<char*>(_value_storage.data) + write_pos * _element_type->size;
                _element_type->ops->from_python(elem_ptr, value, _element_type);
                _times[write_pos] = owning_graph()->evaluation_time();

                // Mark that we have a removed value (at current head before we advance)
                _has_removed_value = true;

                // Advance head - old head is now the "removed" slot
                head = (head + 1) % _capacity;

                // Size reaches capacity once and stays there
                if (size < _capacity) {
                    size++;
                }

                // Schedule cleanup of removed value flag after evaluation
                auto weak_self = weak_from_this();
                owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self]() {
                    if (auto self = weak_self.lock()) {
                        auto* tsw = static_cast<TimeSeriesFixedWindowOutput*>(self.get());
                        tsw->_has_removed_value = false;
                    }
                });
            }

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
        _value_storage.size = 0;
        _value_storage.head = 0;
        _has_removed_value = false;
        BaseTimeSeriesOutput::mark_invalid();
    }

    bool TimeSeriesFixedWindowOutput::all_valid() const {
        return valid() && len() >= _min_size;
    }

    nb::object TimeSeriesFixedWindowOutput::py_value_times() const {
        return nb::cast(value_times());
    }

    std::vector<engine_time_t> TimeSeriesFixedWindowOutput::value_times() const {
        std::vector<engine_time_t> out;
        size_t active_count = len();
        out.reserve(active_count);

        // Return timestamps for active elements only, in logical order
        for (size_t i = 0; i < active_count; ++i) {
            size_t physical = (_value_storage.head + i) % _capacity;
            out.push_back(_times[physical]);
        }
        return out;
    }

    engine_time_t TimeSeriesFixedWindowOutput::first_modified_time() const {
        if (_value_storage.size == 0) return engine_time_t{};
        return _times[_value_storage.head];
    }

    void TimeSeriesFixedWindowOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto &o = dynamic_cast<const TimeSeriesFixedWindowOutput &>(output);

        // Copy only active elements (not the removed value)
        size_t src_active_count = o.len();
        size_t copy_count = std::min(src_active_count, _user_size);

        for (size_t i = 0; i < copy_count; ++i) {
            size_t src_physical = (o._value_storage.head + i) % o._capacity;
            size_t dst_physical = i;

            const void* src = static_cast<const char*>(o._value_storage.data) + src_physical * _element_type->size;
            void* dst = static_cast<char*>(_value_storage.data) + dst_physical * _element_type->size;

            if (_element_type->ops->copy_assign) {
                _element_type->ops->copy_assign(dst, src, _element_type);
            }
            _times[dst_physical] = o._times[src_physical];
        }

        _value_storage.size = copy_count;
        _value_storage.head = 0;  // Reset head since we copied in order
        _has_removed_value = false;

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
        // Removed value is at (head - 1 + capacity) % capacity
        // This is the slot just before the current head
        size_t removed_pos = (_value_storage.head + _capacity - 1) % _capacity;
        const void* elem_ptr = static_cast<const char*>(_value_storage.data) + removed_pos * _element_type->size;
        return _element_type->ops->to_python(elem_ptr, _element_type);
    }

    void TimeSeriesFixedWindowOutput::reset_value() {
        _value_storage.size = 0;
        _value_storage.head = 0;
        _has_removed_value = false;
    }

    // ========== TimeSeriesWindowInput Implementation ==========

    TimeSeriesWindowInput::TimeSeriesWindowInput(const node_ptr &parent, const value::TypeMeta* element_type)
        : BaseTimeSeriesInput(parent), _element_type(element_type) {}

    TimeSeriesWindowInput::TimeSeriesWindowInput(time_series_input_ptr parent, const value::TypeMeta* element_type)
        : BaseTimeSeriesInput(parent), _element_type(element_type) {}

    const value::TypeMeta* TimeSeriesWindowInput::element_type() const {
        if (_element_type) return _element_type;
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
        : BaseTimeSeriesOutput(parent), _window_duration(size), _min_duration(min_size),
          _ready(false), _element_type(element_type), _element_size(element_type ? element_type->size : 0) {
    }

    TimeSeriesTimeWindowOutput::TimeSeriesTimeWindowOutput(time_series_output_ptr parent, engine_time_delta_t size,
                                                           engine_time_delta_t min_size, const value::TypeMeta* element_type)
        : BaseTimeSeriesOutput(parent), _window_duration(size), _min_duration(min_size),
          _ready(false), _element_type(element_type), _element_size(element_type ? element_type->size : 0) {
    }

    TimeSeriesTimeWindowOutput::~TimeSeriesTimeWindowOutput() {
        // Free all allocated elements
        for (void* ptr : _buffer_ptrs) {
            deallocate_element(ptr);
        }
        _buffer_ptrs.clear();

        for (void* ptr : _removed_value_ptrs) {
            deallocate_element(ptr);
        }
        _removed_value_ptrs.clear();
    }

    void* TimeSeriesTimeWindowOutput::allocate_element() {
        if (!_element_type || _element_size == 0) return nullptr;

        void* ptr = std::malloc(_element_size);
        if (!ptr) throw std::bad_alloc();

        if (_element_type->ops && _element_type->ops->construct) {
            _element_type->ops->construct(ptr, _element_type);
        }
        return ptr;
    }

    void TimeSeriesTimeWindowOutput::deallocate_element(void* ptr) {
        if (!ptr) return;

        if (_element_type && _element_type->ops && _element_type->ops->destruct) {
            _element_type->ops->destruct(ptr, _element_type);
        }
        std::free(ptr);
    }

    void TimeSeriesTimeWindowOutput::_roll() const {
        // Check if we've already rolled this tick
        auto current = owning_graph()->evaluation_time();
        if (!_needs_roll && _last_roll_time == current) {
            return;
        }

        auto cutoff = current - _window_duration;
        while (!_times.empty() && _times.front() < cutoff) {
            _times.pop_front();

            // Move element to removed list
            void* removed_ptr = _buffer_ptrs.front();
            _buffer_ptrs.pop_front();
            _removed_value_ptrs.push_back(removed_ptr);
        }

        if (!_removed_value_ptrs.empty()) {
            auto weak_self = std::const_pointer_cast<TimeSeriesOutput>(shared_from_this());
            owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self = std::weak_ptr(weak_self)]() {
                if (auto self = weak_self.lock()) {
                    static_cast<TimeSeriesTimeWindowOutput*>(self.get())->_reset_removed_values();
                }
            });
        }

        _last_roll_time = current;
        _needs_roll = false;
    }

    void TimeSeriesTimeWindowOutput::_reset_removed_values() {
        for (void* ptr : _removed_value_ptrs) {
            deallocate_element(ptr);
        }
        _removed_value_ptrs.clear();
    }

    bool TimeSeriesTimeWindowOutput::has_removed_value() const {
        _roll();
        return !_removed_value_ptrs.empty();
    }

    nb::object TimeSeriesTimeWindowOutput::py_removed_value() const {
        _roll();
        if (_removed_value_ptrs.empty()) return nb::none();

        nb::list result;
        for (void* ptr : _removed_value_ptrs) {
            result.append(_element_type->ops->to_python(ptr, _element_type));
        }
        return result;
    }

    size_t TimeSeriesTimeWindowOutput::len() const {
        _roll();
        return _buffer_ptrs.size();
    }

    nb::object TimeSeriesTimeWindowOutput::py_value() const {
        if (!_ready) {
            auto elapsed =
                owning_graph()->evaluation_time() - owning_graph()->evaluation_engine_api()->start_time();
            if (elapsed >= _min_duration) {
                _ready = true;
            } else {
                return nb::none();
            }
        }

        _roll();
        if (_buffer_ptrs.empty()) return nb::none();

        nb::list result;
        for (void* ptr : _buffer_ptrs) {
            result.append(_element_type->ops->to_python(ptr, _element_type));
        }
        return result;
    }

    nb::object TimeSeriesTimeWindowOutput::py_delta_value() const {
        if (!_ready) {
            auto elapsed =
                owning_graph()->evaluation_time() - owning_graph()->evaluation_engine_api()->start_time();
            if (elapsed >= _min_duration) { _ready = true; }
        }

        if (_ready && !_times.empty()) {
            auto current_time = owning_graph()->evaluation_time();
            if (_times.back() == current_time) {
                return _element_type->ops->to_python(_buffer_ptrs.back(), _element_type);
            }
        }
        return nb::none();
    }

    void TimeSeriesTimeWindowOutput::py_set_value(const nb::object& value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        throw std::runtime_error("py_set_value should not be called on TimeSeriesTimeWindowOutput");
    }

    void TimeSeriesTimeWindowOutput::apply_result(const nb::object& value) {
        if (!value.is_valid() || value.is_none()) return;
        try {
            void* elem_ptr = allocate_element();
            _element_type->ops->from_python(elem_ptr, value, _element_type);
            _buffer_ptrs.push_back(elem_ptr);
            _times.push_back(owning_graph()->evaluation_time());
            _needs_roll = true;  // Next access should check for expired items
            mark_modified();
        } catch (const std::exception &e) {
            throw std::runtime_error(std::string("Cannot apply node output: ") + e.what());
        }
    }

    void TimeSeriesTimeWindowOutput::mark_invalid() {
        for (void* ptr : _buffer_ptrs) {
            deallocate_element(ptr);
        }
        _buffer_ptrs.clear();
        _times.clear();
        _ready = false;
        _needs_roll = true;

        for (void* ptr : _removed_value_ptrs) {
            deallocate_element(ptr);
        }
        _removed_value_ptrs.clear();

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

        // Clear current state
        for (void* ptr : _buffer_ptrs) {
            deallocate_element(ptr);
        }
        _buffer_ptrs.clear();
        _times.clear();

        // Copy elements
        for (size_t i = 0; i < o._buffer_ptrs.size(); ++i) {
            void* new_elem = allocate_element();
            if (_element_type->ops->copy_assign) {
                _element_type->ops->copy_assign(new_elem, o._buffer_ptrs[i], _element_type);
            }
            _buffer_ptrs.push_back(new_elem);
        }
        _times = o._times;

        _window_duration = o._window_duration;
        _min_duration = o._min_duration;
        _ready = o._ready;
        _needs_roll = true;

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
