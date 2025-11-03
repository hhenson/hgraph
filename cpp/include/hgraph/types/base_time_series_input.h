#ifndef BASE_TIME_SERIES_INPUT_H
#define BASE_TIME_SERIES_INPUT_H

#include <hgraph/types/time_series_type.h>

namespace hgraph {
    /*
     * Concrete reusable base that implements the common state/behaviour for TimeSeriesInput.
     * All concrete input types should inherit from this instead of directly from TimeSeriesInput.
     */
    struct HGRAPH_EXPORT BaseTimeSeriesInput : TimeSeriesInput {
        using ptr = nb::ref<BaseTimeSeriesInput>;
        using TimeSeriesInput::TimeSeriesInput;

        static void register_with_nanobind(nb::module_ &m);

        // Relationship helpers
        [[nodiscard]] TimeSeriesInput::ptr parent_input() const override;
        [[nodiscard]] bool has_parent_input() const override;

        // Binding / peer state
        [[nodiscard]] bool bound() const override;
        [[nodiscard]] bool has_peer() const override;
        [[nodiscard]] time_series_output_ptr output() const override;
        bool bind_output(time_series_output_ptr output_) override;
        void un_bind_output(bool unbind_refs) override;

        // Activity state
        [[nodiscard]] bool active() const override;
        void make_active() override;
        void make_passive() override;
        [[nodiscard]] bool has_output() const override;

        // Minimal teardown for builders
        void builder_release_cleanup() override;

        // Type/graph-facing methods implemented using output
        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;
        [[nodiscard]] bool modified() const override;
        [[nodiscard]] bool valid() const override;
        [[nodiscard]] bool all_valid() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;

        [[nodiscard]] time_series_reference_output_ptr reference_output() const override;

        [[nodiscard]] const TimeSeriesInput *get_input(size_t index) const override;
        [[nodiscard]] TimeSeriesInput *get_input(size_t index) override;

    protected:
        bool do_bind_output(time_series_output_ptr &output_) override;
        void do_un_bind_output(bool unbind_refs) override;
        void notify(engine_time_t modified_time) override;
        void notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override;

        void set_sample_time(engine_time_t sample_time);
        [[nodiscard]] engine_time_t sample_time() const;
        [[nodiscard]] bool sampled() const;

        void reset_output();
        void set_output(time_series_output_ptr output);
        void set_active(bool active);

    private:
        time_series_output_ptr _output;
        time_series_reference_output_ptr _reference_output;
        bool _active{false};
        engine_time_t _sample_time{MIN_DT};
        engine_time_t _notify_time{MIN_DT};
    };
}

#endif // BASE_TIME_SERIES_INPUT_H
