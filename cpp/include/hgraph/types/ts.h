//
// Created by Howard Henson on 03/01/2025.
//

#ifndef TS_H
#define TS_H

#include <hgraph/types/base_time_series.h>
#include <hgraph/types/v2/ts_value.h>

namespace hgraph {

    /**
     * Non-templated TimeSeriesValueOutput using v2 TSOutput for value storage.
     * This replaces the templated TimeSeriesValueOutput<T> to reduce template instantiations.
     *
     * Inherits from BaseTimeSeriesOutput for compatibility with the existing parent/notification
     * infrastructure, but delegates value storage and modification tracking to TSOutput.
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
            const T* pv = _ts_output.value().template get_if<T>();
            if (!pv) throw std::bad_cast();
            return *pv;
        }

        template <typename T> void set_value(const T& v) {
            AnyValue<> av;
            av.template emplace<T>(v);
            _ts_output.set_value(std::move(av));
            // Note: TSOutput::set_value will notify parent, but we also need to trigger
            // the BaseTimeSeriesOutput notification mechanism
            mark_modified();
        }

        template <typename T> void set_value(T&& v) {
            AnyValue<> av;
            av.template emplace<std::decay_t<T>>(std::forward<T>(v));
            _ts_output.set_value(std::move(av));
            mark_modified();
        }

        void mark_invalid() override;
        void invalidate() override;
        void copy_from_output(const TimeSeriesOutput &output) override;
        void copy_from_input(const TimeSeriesInput &input) override;
        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        // NOTE: modified/valid/last_modified_time are inherited from BaseTimeSeriesOutput
        // We use the base class's tracking mechanism for these to maintain compatibility
        // with the existing notification infrastructure. _ts_output is used for value storage only.

        // Access to type info
        [[nodiscard]] const std::type_info& value_type() const { return _ts_output.value_type(); }

        // Access to underlying TSOutput for direct binding
        [[nodiscard]] TSOutput& ts_output() { return _ts_output; }
        [[nodiscard]] const TSOutput& ts_output() const { return _ts_output; }

        VISITOR_SUPPORT()

    private:
        TSOutput _ts_output;
    };

    /**
     * Non-templated TimeSeriesValueInput using v2 TSInput for binding/value access.
     *
     * Inherits from BaseTimeSeriesInput for compatibility with the existing parent/notification
     * infrastructure, but delegates binding and value access to TSInput.
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
            const T* pv = _ts_input.value().template get_if<T>();
            if (!pv) throw std::bad_cast();
            return *pv;
        }

        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        // Override binding to use TSInput for value sharing
        bool bind_output(time_series_output_s_ptr output_) override;
        void un_bind_output(bool unbind_refs) override;

        // NOTE: modified/valid/last_modified_time/active/make_active/make_passive
        // are inherited from BaseTimeSeriesInput. We use the base class's tracking
        // mechanism to maintain compatibility with existing infrastructure.

        // Access to type info
        [[nodiscard]] const std::type_info& value_type() const { return _ts_input.value_type(); }

        // Access to underlying TSInput for direct operations
        [[nodiscard]] TSInput& ts_input() { return _ts_input; }
        [[nodiscard]] const TSInput& ts_input() const { return _ts_input; }

        VISITOR_SUPPORT()

    private:
        friend struct TimeSeriesValueOutput;  // For copy_from_input
        TSInput _ts_input;
    };

    void register_ts_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TS_H
