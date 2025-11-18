//
// TimeSeriesWindow (TSW) implementation
// Includes both fixed-size (tick-count) and time-window (timedelta) variants.
//

#ifndef TSW_H
#define TSW_H

#include <hgraph/types/base_time_series.h>
#include <hgraph/types/time_series_visitor.h>
#include <deque>

namespace hgraph {
    // Forward declarations
    template<typename T>
    struct TimeSeriesTimeWindowOutput;

    template<typename T>
    struct TimeSeriesFixedWindowOutput : BaseTimeSeriesOutput {
        using value_type = T;

        using BaseTimeSeriesOutput::BaseTimeSeriesOutput;

        // Construct with capacity and min size
        TimeSeriesFixedWindowOutput(const node_ptr &parent, size_t size, size_t min_size)
            : BaseTimeSeriesOutput(parent), _size(size), _min_size(min_size) {
            _buffer.resize(_size);
            _times.resize(_size, engine_time_t{});
        }

        TimeSeriesFixedWindowOutput(const TimeSeriesType::ptr &parent, size_t size, size_t min_size)
            : BaseTimeSeriesOutput(parent), _size(size), _min_size(min_size) {
            _buffer.resize(_size);
            _times.resize(_size, engine_time_t{});
        }

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        void py_set_value(nb::object value) override;

        bool can_apply_result(nb::object) override { return !modified(); }

        void apply_result(nb::object value) override;

        void invalidate() override { mark_invalid(); }

        void mark_invalid() override;

        [[nodiscard]] nb::object py_value_times() const;

        [[nodiscard]] engine_time_t first_modified_time() const;

        void copy_from_output(const TimeSeriesOutput &output) override {
            auto &o = dynamic_cast<const TimeSeriesFixedWindowOutput<T> &>(output);
            _buffer = o._buffer;
            _times = o._times;
            _start = o._start;
            _length = o._length;
            _size = o._size;
            _min_size = o._min_size;
            mark_modified();
        }

        void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override {
            return dynamic_cast<const TimeSeriesFixedWindowOutput<T> *>(other) != nullptr;
        }

        // Extra API to mirror Python TSW
        [[nodiscard]] size_t size() const { return _size; }
        [[nodiscard]] size_t min_size() const { return _min_size; }
        [[nodiscard]] bool has_removed_value() const { return _removed_value.has_value(); }
        [[nodiscard]] T removed_value() const { return _removed_value.value_or(T{}); }

        [[nodiscard]] size_t len() const { return _length; }

        void reset_value() {
            auto b{std::vector<T>()};
            std::swap(_buffer, b);
            auto t{std::vector<engine_time_t>()};
            std::swap(_times, t);
            _removed_value.reset();
        }

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<TimeSeriesFixedWindowOutput<T>>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<TimeSeriesFixedWindowOutput<T>>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }

    private:
        std::vector<T> _buffer{};
        std::vector<engine_time_t> _times{};
        size_t _size{0};
        size_t _min_size{0};
        size_t _start{0};
        size_t _length{0};
        std::optional<T> _removed_value{};
    };

    // Unified window input that works with both fixed-size and timedelta outputs
    template<typename T>
    struct TimeSeriesWindowInput : BaseTimeSeriesInput {
        using BaseTimeSeriesInput::BaseTimeSeriesInput;

        // Helpers to dynamically get the output as the correct type
        [[nodiscard]] TimeSeriesFixedWindowOutput<T> *as_fixed_output() const {
            return dynamic_cast<TimeSeriesFixedWindowOutput<T> *>(output().get());
        }

        [[nodiscard]] TimeSeriesTimeWindowOutput<T> *as_time_output() const {
            return dynamic_cast<TimeSeriesTimeWindowOutput<T> *>(output().get());
        }

        [[nodiscard]] nb::object py_value() const override {
            if (auto *f = as_fixed_output()) return f->py_value();
            if (auto *t = as_time_output()) return t->py_value();
            throw std::runtime_error("TimeSeriesWindowInput: output is not a window output");
        }

        [[nodiscard]] nb::object py_delta_value() const override {
            if (auto *f = as_fixed_output()) return f->py_delta_value();
            if (auto *t = as_time_output()) return t->py_delta_value();
            throw std::runtime_error("TimeSeriesWindowInput: output is not a window output");
        }

        [[nodiscard]] bool modified() const override { return output()->modified(); }
        [[nodiscard]] bool valid() const override { return output()->valid(); }

        [[nodiscard]] bool all_valid() const override;

        [[nodiscard]] engine_time_t last_modified_time() const override { return output()->last_modified_time(); }

        [[nodiscard]] nb::object py_value_times() const {
            if (auto *f = as_fixed_output()) return f->py_value_times();
            if (auto *t = as_time_output()) return t->py_value_times();
            throw std::runtime_error("TimeSeriesWindowInput: output is not a window output");
        }

        [[nodiscard]] engine_time_t first_modified_time() const {
            if (auto *f = as_fixed_output()) return f->first_modified_time();
            if (auto *t = as_time_output()) return t->first_modified_time();
            throw std::runtime_error("TimeSeriesWindowInput: output is not a window output");
        }

        [[nodiscard]] bool has_removed_value() const {
            if (auto *f = as_fixed_output()) return f->has_removed_value();
            if (auto *t = as_time_output()) return t->has_removed_value();
            return false;
        }

        [[nodiscard]] nb::object removed_value() const {
            if (auto *f = as_fixed_output()) return f->has_removed_value() ? nb::cast(f->removed_value()) : nb::none();
            if (auto *t = as_time_output()) return t->removed_value();
            return nb::none();
        }

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override {
            return dynamic_cast<const TimeSeriesWindowInput<T> *>(other) != nullptr;
        }

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesInputVisitor<TimeSeriesWindowInput<T>>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesInputVisitor<TimeSeriesWindowInput<T>>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }
    };

    template<typename T>
    void TimeSeriesFixedWindowOutput<T>::copy_from_input(const TimeSeriesInput &input) {
        auto &i = dynamic_cast<const TimeSeriesWindowInput<T> &>(input);
        if (auto *src = i.as_fixed_output()) {
            _buffer = src->_buffer;
            _times = src->_times;
            _start = src->_start;
            _length = src->_length;
            _size = src->_size;
            _min_size = src->_min_size;
            mark_modified();
        } else {
            throw std::runtime_error("TimeSeriesFixedWindowOutput::copy_from_input: input output is not fixed window");
        }
    }

    // TimeSeriesTimeWindowOutput - timedelta-based window
    template<typename T>
    struct TimeSeriesTimeWindowOutput : BaseTimeSeriesOutput {
        using value_type = T;

        using BaseTimeSeriesOutput::BaseTimeSeriesOutput;

        // Construct with time window and min time window
        TimeSeriesTimeWindowOutput(const node_ptr &parent, engine_time_delta_t size, engine_time_delta_t min_size)
            : BaseTimeSeriesOutput(parent), _size(size), _min_size(min_size), _ready(false) {
        }

        TimeSeriesTimeWindowOutput(const TimeSeriesType::ptr &parent, engine_time_delta_t size,
                                   engine_time_delta_t min_size)
            : BaseTimeSeriesOutput(parent), _size(size), _min_size(min_size), _ready(false) {
        }

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        void py_set_value(nb::object value) override;

        bool can_apply_result(nb::object) override { return !modified(); }

        void apply_result(nb::object value) override;

        void invalidate() override { mark_invalid(); }

        void mark_invalid() override;

        [[nodiscard]] nb::object py_value_times() const;

        [[nodiscard]] engine_time_t first_modified_time() const;

        void copy_from_output(const TimeSeriesOutput &output) override {
            auto &o = dynamic_cast<const TimeSeriesTimeWindowOutput<T> &>(output);
            _buffer = o._buffer;
            _times = o._times;
            _size = o._size;
            _min_size = o._min_size;
            _ready = o._ready;
            mark_modified();
        }

        void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override {
            return dynamic_cast<const TimeSeriesTimeWindowOutput<T> *>(other) != nullptr;
        }

        // Extra API to mirror Python TSW
        [[nodiscard]] engine_time_delta_t size() const { return _size; }
        [[nodiscard]] engine_time_delta_t min_size() const { return _min_size; }

        [[nodiscard]] bool has_removed_value() const;

        [[nodiscard]] nb::object removed_value() const;

        [[nodiscard]] size_t len() const;

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<TimeSeriesTimeWindowOutput<T>>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<TimeSeriesTimeWindowOutput<T>>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }

    private:
        void _roll() const; // mutable operation to clean up old items
        void _reset_removed_values();

        mutable std::deque<T> _buffer;
        mutable std::deque<engine_time_t> _times;
        engine_time_delta_t _size{};
        engine_time_delta_t _min_size{};
        mutable bool _ready{false};
        mutable std::vector<T> _removed_values;
    };

    // Registration
    void tsw_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TSW_H