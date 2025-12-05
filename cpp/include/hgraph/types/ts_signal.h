//
// Created by Howard Henson on 06/06/2025.
//

#ifndef TS_SIGNAL_H
#define TS_SIGNAL_H

#include <hgraph/types/base_time_series.h>

namespace hgraph {
    struct TimeSeriesSignalInputBuilder;

    struct TimeSeriesSignalInput final : BaseTimeSeriesInput {
        using ptr = TimeSeriesSignalInput*;
        using BaseTimeSeriesInput::BaseTimeSeriesInput;

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override { return true; }

        [[nodiscard]] TimeSeriesInput::s_ptr get_input(size_t index) override;

        // Override to aggregate from children like Python implementation
        [[nodiscard]] bool valid() const override;

        [[nodiscard]] bool modified() const override;

        [[nodiscard]] engine_time_t last_modified_time() const override;

        // Override to propagate to children
        void make_active() override;

        void make_passive() override;

        void do_un_bind_output(bool unbind_refs) override;

        VISITOR_SUPPORT()

    private:
        friend TimeSeriesSignalInputBuilder;
        mutable std::vector<TimeSeriesInput::s_ptr> _ts_values; // Lazily created child signals
    };
}

#endif //TS_SIGNAL_H