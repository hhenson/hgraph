//
// Created by Howard Henson on 06/06/2025.
//

#ifndef TS_SIGNAL_H
#define TS_SIGNAL_H

#include <hgraph/types/time_series_type.h>

namespace hgraph {
    struct TimeSeriesSignalInputBuilder;

    struct TimeSeriesSignalInput : TimeSeriesInput {
        using ptr = nb::ref<TimeSeriesSignalInput>;
        using TimeSeriesInput::TimeSeriesInput;

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

        static void register_with_nanobind(nb::module_ &m);

    private:
        friend TimeSeriesSignalInputBuilder;
        mutable std::vector<ptr> _ts_values; // Lazily created child signals
    };
}

#endif //TS_SIGNAL_H