//
// Created by Howard Henson on 06/06/2025.
//

#ifndef TS_SIGNAL_H
#define TS_SIGNAL_H

#include <hgraph/types/base_time_series.h>
#include <hgraph/types/time_series_visitor.h>

namespace hgraph {
    struct TimeSeriesSignalInputBuilder;

    struct TimeSeriesSignalInput : BaseTimeSeriesInput {
        using ptr = nb::ref<TimeSeriesSignalInput>;
        using BaseTimeSeriesInput::BaseTimeSeriesInput;

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override { return true; }

        [[nodiscard]] TimeSeriesInput *get_input(size_t index) override;

        // Override to aggregate from children like Python implementation
        [[nodiscard]] bool valid() const override;

        [[nodiscard]] bool modified() const override;

        [[nodiscard]] engine_time_t last_modified_time() const override;

        // Override to propagate to children
        void make_active() override;

        void make_passive() override;

        void do_un_bind_output(bool unbind_refs) override;

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesInputVisitor<TimeSeriesSignalInput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesInputVisitor<TimeSeriesSignalInput>*>(&visitor)) {
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

        static void register_with_nanobind(nb::module_ &m);

    private:
        friend TimeSeriesSignalInputBuilder;
        mutable std::vector<ptr> _ts_values; // Lazily created child signals
    };
}

#endif //TS_SIGNAL_H