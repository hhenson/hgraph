//
// TimeSeriesWindow (TSW) implementation
// Includes both fixed-size (tick-count) and time-window (timedelta) variants.
// Leverages CyclicBufferStorage from the Value system for efficient ring buffer storage.
//

#ifndef TSW_H
#define TSW_H

#include <hgraph/types/base_time_series.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/cyclic_buffer_ops.h>
#include <deque>

namespace hgraph {

    /**
     * @brief Non-templated TimeSeriesFixedWindowOutput using CyclicBufferStorage.
     *
     * Storage layout leverages value::CyclicBufferStorage for the element ring buffer:
     * - Values stored in contiguous byte buffer (TSW[int] = contiguous int64_t buffer)
     * - Timestamps stored in parallel CyclicBufferStorage
     * - Ring buffer semantics: head points to oldest, wraps on overflow
     *
     * This ensures efficient cache access patterns and proper memory layout.
     */
    struct TimeSeriesFixedWindowOutput final : BaseTimeSeriesOutput {
        using s_ptr = std::shared_ptr<TimeSeriesFixedWindowOutput>;

        using BaseTimeSeriesOutput::BaseTimeSeriesOutput;

        // Construct with capacity, min size, and element type
        TimeSeriesFixedWindowOutput(node_ptr parent, size_t size, size_t min_size,
                                    const value::TypeMeta* element_type);
        TimeSeriesFixedWindowOutput(time_series_output_ptr parent, size_t size, size_t min_size,
                                    const value::TypeMeta* element_type);

        ~TimeSeriesFixedWindowOutput() override;

        // Non-copyable, non-movable (complex resource management)
        TimeSeriesFixedWindowOutput(const TimeSeriesFixedWindowOutput&) = delete;
        TimeSeriesFixedWindowOutput& operator=(const TimeSeriesFixedWindowOutput&) = delete;
        TimeSeriesFixedWindowOutput(TimeSeriesFixedWindowOutput&&) = delete;
        TimeSeriesFixedWindowOutput& operator=(TimeSeriesFixedWindowOutput&&) = delete;

        [[nodiscard]] const value::TypeMeta* element_type() const { return _element_type; }

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        void py_set_value(const nb::object& value) override;

        bool can_apply_result(const nb::object&) override { return !modified(); }

        void apply_result(const nb::object& value) override;

        void invalidate() override { mark_invalid(); }

        void mark_invalid() override;

        [[nodiscard]] bool all_valid() const override;

        [[nodiscard]] nb::object py_value_times() const;

        [[nodiscard]] std::vector<engine_time_t> value_times() const;

        [[nodiscard]] engine_time_t first_modified_time() const;

        void copy_from_output(const TimeSeriesOutput &output) override;

        void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override {
            if (other->kind() != kind()) { return false; }
            auto* other_tsw = static_cast<const TimeSeriesFixedWindowOutput*>(other);
            return _element_type == other_tsw->_element_type;
        }

        [[nodiscard]] TimeSeriesKind kind() const override { return TimeSeriesKind::Window | TimeSeriesKind::FixedWindow | TimeSeriesKind::Output; }

        // Extra API to mirror Python TSW
        [[nodiscard]] size_t size() const { return _user_size; }
        [[nodiscard]] size_t min_size() const { return _min_size; }
        [[nodiscard]] bool has_removed_value() const { return _has_removed_value; }
        [[nodiscard]] nb::object py_removed_value() const;

        // len() returns active element count (max is _user_size)
        [[nodiscard]] size_t len() const { return std::min(_value_storage.size, _user_size); }

        void reset_value();

        VISITOR_SUPPORT()

    private:
        // Allocate and construct the cyclic buffers
        void allocate_buffers();
        // Destruct and deallocate buffers
        void deallocate_buffers();

        // Helper to get element pointer at logical index
        [[nodiscard]] void* get_value_ptr(size_t logical_index);
        [[nodiscard]] const void* get_value_ptr(size_t logical_index) const;

        // CyclicBufferStorage for values (uses CyclicBufferOps pattern)
        value::CyclicBufferStorage _value_storage{};

        // Parallel array for timestamps (simple vector, indexed same as values)
        std::vector<engine_time_t> _times{};

        size_t _capacity{0};      // Actual buffer capacity (user_size + 1)
        size_t _min_size{0};
        size_t _user_size{0};     // User-specified window size

        // Removed value tracking - the removed value stays in the buffer
        // at position (head - 1 + capacity) % capacity until next update
        bool _has_removed_value{false};

        const value::TypeMeta* _element_type{nullptr};
    };

    /**
     * @brief Non-templated TimeSeriesTimeWindowOutput using deque-based storage.
     *
     * Time-based windows use deques for unbounded growth with efficient front removal.
     * Elements stored as contiguous bytes for each entry.
     */
    struct TimeSeriesTimeWindowOutput final : BaseTimeSeriesOutput {
        using s_ptr = std::shared_ptr<TimeSeriesTimeWindowOutput>;

        using BaseTimeSeriesOutput::BaseTimeSeriesOutput;

        // Construct with time window, min time window, and element type
        TimeSeriesTimeWindowOutput(node_ptr parent, engine_time_delta_t size,
                                   engine_time_delta_t min_size, const value::TypeMeta* element_type);
        TimeSeriesTimeWindowOutput(time_series_output_ptr parent, engine_time_delta_t size,
                                   engine_time_delta_t min_size, const value::TypeMeta* element_type);

        ~TimeSeriesTimeWindowOutput() override;

        // Non-copyable, non-movable
        TimeSeriesTimeWindowOutput(const TimeSeriesTimeWindowOutput&) = delete;
        TimeSeriesTimeWindowOutput& operator=(const TimeSeriesTimeWindowOutput&) = delete;
        TimeSeriesTimeWindowOutput(TimeSeriesTimeWindowOutput&&) = delete;
        TimeSeriesTimeWindowOutput& operator=(TimeSeriesTimeWindowOutput&&) = delete;

        [[nodiscard]] const value::TypeMeta* element_type() const { return _element_type; }

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        void py_set_value(const nb::object& value) override;

        bool can_apply_result(const nb::object&) override { return !modified(); }

        void apply_result(const nb::object& value) override;

        void invalidate() override { mark_invalid(); }

        void mark_invalid() override;

        [[nodiscard]] nb::object py_value_times() const;

        [[nodiscard]] engine_time_t first_modified_time() const;

        void copy_from_output(const TimeSeriesOutput &output) override;

        void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override {
            if (other->kind() != kind()) { return false; }
            auto* other_tsw = static_cast<const TimeSeriesTimeWindowOutput*>(other);
            return _element_type == other_tsw->_element_type;
        }

        [[nodiscard]] TimeSeriesKind kind() const override { return TimeSeriesKind::Window | TimeSeriesKind::TimeWindow | TimeSeriesKind::Output; }

        // Extra API to mirror Python TSW
        [[nodiscard]] engine_time_delta_t size() const { return _window_duration; }
        [[nodiscard]] engine_time_delta_t min_size() const { return _min_duration; }

        [[nodiscard]] bool has_removed_value() const;

        [[nodiscard]] nb::object py_removed_value() const;

        [[nodiscard]] size_t len() const;

        VISITOR_SUPPORT()

    private:
        void _roll() const; // mutable operation to clean up old items
        void _reset_removed_values();

        // Allocate storage for a new element
        void* allocate_element();
        // Deallocate element storage
        void deallocate_element(void* ptr);

        // Storage: Each entry is a separate allocation
        // Could be optimized with chunked storage for large windows
        mutable std::deque<void*> _buffer_ptrs;     // Pointers to allocated element storage
        mutable std::deque<engine_time_t> _times;

        engine_time_delta_t _window_duration{};
        engine_time_delta_t _min_duration{};
        mutable bool _ready{false};
        mutable std::vector<void*> _removed_value_ptrs;  // Pointers to removed elements
        const value::TypeMeta* _element_type{nullptr};
        size_t _element_size{0};

        // Cache for roll state to avoid redundant rolling within same tick
        mutable engine_time_t _last_roll_time{};
        mutable bool _needs_roll{true};
    };

    /**
     * @brief Non-templated TimeSeriesWindowInput.
     *
     * Unified input that works with both fixed-size and timedelta outputs.
     */
    struct TimeSeriesWindowInput final : BaseTimeSeriesInput {
        using BaseTimeSeriesInput::BaseTimeSeriesInput;

        explicit TimeSeriesWindowInput(const node_ptr &parent, const value::TypeMeta* element_type = nullptr);
        explicit TimeSeriesWindowInput(time_series_input_ptr parent, const value::TypeMeta* element_type = nullptr);

        [[nodiscard]] const value::TypeMeta* element_type() const;

        // Helpers to dynamically get the output as the correct type
        [[nodiscard]] TimeSeriesFixedWindowOutput *as_fixed_output() const {
            return dynamic_cast<TimeSeriesFixedWindowOutput *>(output().get());
        }

        [[nodiscard]] TimeSeriesTimeWindowOutput *as_time_output() const {
            return dynamic_cast<TimeSeriesTimeWindowOutput *>(output().get());
        }

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] bool modified() const override { return output() != nullptr && output()->modified(); }
        [[nodiscard]] bool valid() const override { return output() != nullptr && output()->valid(); }

        [[nodiscard]] bool all_valid() const override;

        [[nodiscard]] engine_time_t last_modified_time() const override {
            return output() != nullptr ? output()->last_modified_time() : MIN_DT;
        }

        [[nodiscard]] nb::object py_value_times() const;

        [[nodiscard]] engine_time_t first_modified_time() const;

        [[nodiscard]] bool has_removed_value() const;

        [[nodiscard]] nb::object py_removed_value() const;

        [[nodiscard]] size_t len() const;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override {
            if (other->kind() != kind()) { return false; }
            auto* other_input = static_cast<const TimeSeriesWindowInput*>(other);
            return element_type() == other_input->element_type();
        }

        [[nodiscard]] TimeSeriesKind kind() const override { return TimeSeriesKind::Window | TimeSeriesKind::Input; }

        VISITOR_SUPPORT()

    private:
        const value::TypeMeta* _element_type{nullptr};
    };

} // namespace hgraph

#endif  // TSW_H
