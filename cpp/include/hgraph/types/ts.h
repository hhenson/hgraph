//
// Created by Howard Henson on 03/01/2025.
//

#ifndef TS_H
#define TS_H

#include <hgraph/types/base_time_series.h>
#include <hgraph/types/value/value.h>

namespace hgraph {

    /**
     * @brief Base class for TimeSeriesValueOutput - used for visitor pattern.
     */
    struct TimeSeriesValueOutputBase : BaseTimeSeriesOutput {
        using BaseTimeSeriesOutput::BaseTimeSeriesOutput;

        VISITOR_SUPPORT()
    };

    /**
     * @brief Non-templated time series value output using CachedValue storage.
     *
     * This class stores values using the Value type system with Python caching.
     * The TypeMeta* schema defines the value type at runtime instead of compile time.
     */
    struct TimeSeriesValueOutput final : TimeSeriesValueOutputBase {
        using s_ptr = std::shared_ptr<TimeSeriesValueOutput>;

        /**
         * @brief Construct with owning node and schema.
         */
        TimeSeriesValueOutput(Node* owning_node, const value::TypeMeta* schema);

        /**
         * @brief Construct with parent output and schema.
         */
        TimeSeriesValueOutput(TimeSeriesOutput* parent, const value::TypeMeta* schema);

        // Python interop
        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;
        void py_set_value(const nb::object& value) override;
        void apply_result(const nb::object& value) override;

        // Value access via views
        [[nodiscard]] value::ConstValueView value() const { return _value.const_view(); }
        [[nodiscard]] value::ValueView value_mut() { return _value.view(); }

        // Schema access
        [[nodiscard]] const value::TypeMeta* schema() const { return _value.schema(); }

        // Lifecycle
        void mark_invalid() override;
        void copy_from_output(const TimeSeriesOutput& output) override;
        void copy_from_input(const TimeSeriesInput& input) override;
        [[nodiscard]] bool is_same_type(const TimeSeriesType* other) const override;
        void reset_value();

        VISITOR_SUPPORT()

    private:
        value::CachedValue _value;  // Type-erased storage with Python caching
    };

    /**
     * @brief Base class for TimeSeriesValueInput - used for visitor pattern.
     */
    struct TimeSeriesValueInputBase : BaseTimeSeriesInput {
        using BaseTimeSeriesInput::BaseTimeSeriesInput;

        VISITOR_SUPPORT()
    };

    /**
     * @brief Non-templated time series value input.
     *
     * Delegates to the bound TimeSeriesValueOutput for value access.
     */
    struct TimeSeriesValueInput final : TimeSeriesValueInputBase {
        using ptr = TimeSeriesValueInput*;

        using TimeSeriesValueInputBase::TimeSeriesValueInputBase;

        [[nodiscard]] TimeSeriesValueOutput& value_output();
        [[nodiscard]] const TimeSeriesValueOutput& value_output() const;

        [[nodiscard]] value::ConstValueView value() const;
        [[nodiscard]] const value::TypeMeta* schema() const;

        [[nodiscard]] bool is_same_type(const TimeSeriesType* other) const override;

        VISITOR_SUPPORT()
    };

    void register_ts_with_nanobind(nb::module_& m);

} // namespace hgraph

#endif  // TS_H