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

        // The input that this input is bound to. This will be nullptr if this is the root input.
        [[nodiscard]] TimeSeriesInput::ptr parent_input() const override;

        // True if this input is a child of another input, False otherwise
        [[nodiscard]] bool has_parent_input() const override;

        // Is this time-series input bound to an output?
        [[nodiscard]] bool bound() const override;

        // True if this input is peered.
        [[nodiscard]] bool has_peer() const override;

        // The output bound to this input. If the input is not bound then this will be nullptr.
        [[nodiscard]] time_series_output_ptr output() const override;

        // FOR LIBRARY USE ONLY. Binds the output provided to this input.
        bool bind_output(time_series_output_ptr output_) override;

        // FOR LIBRARY USE ONLY. Unbinds the output from this input.
        void un_bind_output(bool unbind_refs) override;

        // An active input will cause the node it is associated with to be scheduled when the value
        // the input represents is modified. Returns True if this input is active.
        [[nodiscard]] bool active() const override;

        // Marks the input as being active, causing its node to be scheduled for evaluation when the value changes.
        void make_active() override;

        // Marks the input as passive, preventing the associated node from being scheduled for evaluation
        // when the value changes.
        void make_passive() override;

        [[nodiscard]] bool has_output() const override;

        // Minimal-teardown helper used by builders during release; must not access owning_node/graph
        void builder_release_cleanup() override;

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
        // Derived classes override this to implement specific behaviours
        virtual bool do_bind_output(time_series_output_ptr &output_);

        // Derived classes override this to implement specific behaviours
        virtual void do_un_bind_output(bool unbind_refs);

        void notify(engine_time_t modified_time) override;

        virtual void notify_parent(TimeSeriesInput *child, engine_time_t modified_time);

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
} // namespace hgraph

#endif // BASE_TIME_SERIES_INPUT_H
