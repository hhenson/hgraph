#ifndef BASE_TIME_SERIES_INPUT_H
#define BASE_TIME_SERIES_INPUT_H

#include <hgraph/types/time_series_type.h>
#include <optional>

namespace hgraph {
    /*
     * Concrete reusable base that implements the common state/behaviour for TimeSeriesInput.
     * All concrete input types should inherit from this instead of directly from TimeSeriesInput.
     */
    struct HGRAPH_EXPORT BaseTimeSeriesInput : TimeSeriesInput {
        using ptr = nb::ref<BaseTimeSeriesInput>;

        BaseTimeSeriesInput() = default;
        explicit BaseTimeSeriesInput(const node_ptr &parent) { re_parent(parent); }
        explicit BaseTimeSeriesInput(const TimeSeriesType::ptr &parent) { re_parent(parent); }

        static void register_with_nanobind(nb::module_ &m);

        // Implement TimeSeriesType abstract interface
        [[nodiscard]] engine_time_t current_engine_time() const override;
        [[nodiscard]] node_ptr owning_node() override;
        [[nodiscard]] node_ptr owning_node() const override;
        [[nodiscard]] graph_ptr owning_graph() override;
        [[nodiscard]] graph_ptr owning_graph() const override;
        void re_parent(const node_ptr &parent) override;
        void re_parent(const TimeSeriesType::ptr &parent) override;
        [[nodiscard]] bool is_reference() const override;
        [[nodiscard]] bool has_reference() const override;
        void reset_parent_or_node() override;

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
        void virtual notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override;

    protected:
        bool virtual do_bind_output(time_series_output_ptr &output_);
        void virtual do_un_bind_output(bool unbind_refs);
        void notify(engine_time_t modified_time) override;

        void set_sample_time(engine_time_t sample_time);
        [[nodiscard]] engine_time_t sample_time() const;
        [[nodiscard]] bool sampled() const;

        void reset_output();
        void set_output(time_series_output_ptr output);
        void set_active(bool active);

        // TimeSeriesType storage hooks
        [[nodiscard]] TimeSeriesType::ptr &_parent_time_series();
        [[nodiscard]] TimeSeriesType::ptr &_parent_time_series() const;
        [[nodiscard]] bool _has_parent_time_series() const;
        void _set_parent_time_series(TimeSeriesType *ts);
        void _set_parent(const node_ptr &parent);
        void _set_parent(const TimeSeriesType::ptr &parent);
        void _reset_parent_or_node();
        [[nodiscard]] bool has_parent_or_node() const;
        [[nodiscard]] bool has_owning_node() const override;
        [[nodiscard]] node_ptr _owning_node() const;

    private:
        using TsOrNode = std::variant<time_series_type_ptr, node_ptr>;
        std::optional<TsOrNode> _parent_ts_or_node{};
        time_series_output_ptr _output;
        time_series_reference_output_ptr _reference_output;
        bool _active{false};
        engine_time_t _sample_time{MIN_DT};
        engine_time_t _notify_time{MIN_DT};
    };
}

#endif // BASE_TIME_SERIES_INPUT_H
