//
// Created by Howard Henson on 03/01/2025.
//

#ifndef TS_H
#define TS_H

#include <hgraph/types/base_time_series.h>
#include <hgraph/types/time_series_visitor.h>

namespace hgraph {
    template<typename T>
    struct TimeSeriesValueOutput : BaseTimeSeriesOutput {
        using value_type = T;
        using ptr = nb::ref<TimeSeriesValueOutput<T> >;

        using BaseTimeSeriesOutput::BaseTimeSeriesOutput;

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        void py_set_value(nb::object value) override;

        void apply_result(nb::object value) override;

        const T &value() const { return _value; }

        void set_value(const T &value);

        void set_value(T &&value);

        void mark_invalid() override;

        void copy_from_output(const TimeSeriesOutput &output) override;

        void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        void reset_value();

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<TimeSeriesValueOutput<T>>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<TimeSeriesValueOutput<T>>*>(&visitor)) {
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
        T _value{};
    };

    template<typename T>
    struct TimeSeriesValueInput : BaseTimeSeriesInput {
        using value_type = T;
        using ptr = nb::ref<TimeSeriesValueInput<T> >;

        using BaseTimeSeriesInput::BaseTimeSeriesInput;

        [[nodiscard]] TimeSeriesValueOutput<T> &value_output();

        [[nodiscard]] const TimeSeriesValueOutput<T> &value_output() const;

        [[nodiscard]] const T &value() const;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesInputVisitor<TimeSeriesValueInput<T>>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesInputVisitor<TimeSeriesValueInput<T>>*>(&visitor)) {
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

    void register_ts_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TS_H