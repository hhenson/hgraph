//
// Created by Howard Henson on 03/01/2025.
//

#ifndef TS_H
#define TS_H

#include <hgraph/types/base_time_series.h>
#include <hgraph/types/v2/any_value.h>

namespace hgraph {

    /**
     * Non-templated TimeSeriesValueOutput using type-erased AnyValue storage.
     * This replaces the templated TimeSeriesValueOutput<T> to reduce template instantiations.
     */
    struct HGRAPH_EXPORT TimeSeriesValueOutput final : BaseTimeSeriesOutput {
        using s_ptr = std::shared_ptr<TimeSeriesValueOutput>;

        // Constructor takes type_info for type checking
        TimeSeriesValueOutput(node_ptr parent, const std::type_info &tp);
        TimeSeriesValueOutput(time_series_output_ptr parent, const std::type_info &tp);

        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;
        void py_set_value(const nb::object& value) override;
        bool can_apply_result(const nb::object &value) override;
        void apply_result(const nb::object& value) override;

        // Type-safe value access via templates
        template <typename T> const T& value() const {
            const T* pv = _value.template get_if<T>();
            if (!pv) throw std::bad_cast();
            return *pv;
        }

        template <typename T> void set_value(const T& v) {
            _value.template emplace<T>(v);
            mark_modified();
        }

        template <typename T> void set_value(T&& v) {
            _value.template emplace<std::decay_t<T>>(std::forward<T>(v));
            mark_modified();
        }

        void mark_invalid() override;
        void copy_from_output(const TimeSeriesOutput &output) override;
        void copy_from_input(const TimeSeriesInput &input) override;
        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        // Access to type info
        [[nodiscard]] const std::type_info& value_type() const { return *_value_type; }

        VISITOR_SUPPORT()

    private:
        AnyValue<> _value;
        const std::type_info* _value_type;
    };

    /**
     * Non-templated TimeSeriesValueInput.
     * Gets values from bound output via the existing output() mechanism.
     */
    struct HGRAPH_EXPORT TimeSeriesValueInput final : BaseTimeSeriesInput {
        using ptr = TimeSeriesValueInput*;
        using s_ptr = std::shared_ptr<TimeSeriesValueInput>;

        // Constructor takes type_info for type checking
        TimeSeriesValueInput(node_ptr parent, const std::type_info &tp);
        TimeSeriesValueInput(time_series_input_ptr parent, const std::type_info &tp);

        // Get bound output as TimeSeriesValueOutput
        [[nodiscard]] TimeSeriesValueOutput& value_output();
        [[nodiscard]] const TimeSeriesValueOutput& value_output() const;

        // Type-safe value access via templates
        template <typename T> const T& value() const {
            return value_output().template value<T>();
        }

        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        // Access to type info
        [[nodiscard]] const std::type_info& value_type() const { return *_value_type; }

        VISITOR_SUPPORT()

    private:
        const std::type_info* _value_type;
    };

    void register_ts_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TS_H