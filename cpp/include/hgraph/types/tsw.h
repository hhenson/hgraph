//
// TimeSeriesWindow (TSW) implementation
// Includes both fixed-size (tick-count) and time-window (timedelta) variants.
// Non-templated implementation using Value/TypeMeta for type erasure.
//

#ifndef TSW_H
#define TSW_H

#include <hgraph/types/base_time_series.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/type_meta.h>
#include <deque>

namespace hgraph {

    /**
     * @brief Non-templated TimeSeriesFixedWindowOutput using Value-based storage.
     *
     * This class stores window elements using the Value system.
     * Element type is determined at runtime via TypeMeta*.
     */
    struct TimeSeriesFixedWindowOutput final : BaseTimeSeriesOutput {
        using s_ptr = std::shared_ptr<TimeSeriesFixedWindowOutput>;

        using BaseTimeSeriesOutput::BaseTimeSeriesOutput;

        // Construct with capacity, min size, and element type
        TimeSeriesFixedWindowOutput(node_ptr parent, size_t size, size_t min_size,
                                    const value::TypeMeta* element_type);
        TimeSeriesFixedWindowOutput(time_series_output_ptr parent, size_t size, size_t min_size,
                                    const value::TypeMeta* element_type);

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
        [[nodiscard]] size_t size() const { return _size; }
        [[nodiscard]] size_t min_size() const { return _min_size; }
        [[nodiscard]] bool has_removed_value() const { return _has_removed_value; }
        [[nodiscard]] nb::object py_removed_value() const;

        [[nodiscard]] size_t len() const { return _length; }

        void reset_value();

        VISITOR_SUPPORT()

    private:
        std::vector<value::Value> _buffer{};
        std::vector<engine_time_t> _times{};
        size_t _size{0};
        size_t _min_size{0};
        size_t _start{0};
        size_t _length{0};
        value::Value _removed_value{};
        bool _has_removed_value{false};
        const value::TypeMeta* _element_type{nullptr};
    };

    /**
     * @brief Non-templated TimeSeriesTimeWindowOutput using Value-based storage.
     *
     * Timedelta-based window that stores elements using the Value system.
     */
    struct TimeSeriesTimeWindowOutput final : BaseTimeSeriesOutput {
        using s_ptr = std::shared_ptr<TimeSeriesTimeWindowOutput>;

        using BaseTimeSeriesOutput::BaseTimeSeriesOutput;

        // Construct with time window, min time window, and element type
        TimeSeriesTimeWindowOutput(node_ptr parent, engine_time_delta_t size,
                                   engine_time_delta_t min_size, const value::TypeMeta* element_type);
        TimeSeriesTimeWindowOutput(time_series_output_ptr parent, engine_time_delta_t size,
                                   engine_time_delta_t min_size, const value::TypeMeta* element_type);

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
        [[nodiscard]] engine_time_delta_t size() const { return _size; }
        [[nodiscard]] engine_time_delta_t min_size() const { return _min_size; }

        [[nodiscard]] bool has_removed_value() const;

        [[nodiscard]] nb::object py_removed_value() const;

        [[nodiscard]] size_t len() const;

        VISITOR_SUPPORT()

    private:
        void _roll() const; // mutable operation to clean up old items
        void _reset_removed_values();

        mutable std::deque<value::Value> _buffer;
        mutable std::deque<engine_time_t> _times;
        engine_time_delta_t _size{};
        engine_time_delta_t _min_size{};
        mutable bool _ready{false};
        mutable std::vector<value::Value> _removed_values;
        const value::TypeMeta* _element_type{nullptr};
    };

    /**
     * @brief Non-templated TimeSeriesWindowInput using Value-based storage.
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
            return dynamic_cast<TimeSeriesFixedWindowOutput *>(output());
        }

        [[nodiscard]] TimeSeriesTimeWindowOutput *as_time_output() const {
            return dynamic_cast<TimeSeriesTimeWindowOutput *>(output());
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
