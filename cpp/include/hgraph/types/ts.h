//
// Created by Howard Henson on 03/01/2025.
//

#ifndef TS_H
#define TS_H

#include <hgraph/types/time_series_type.h>
#include <hgraph/types/v2/ts_value.h>
#include <hgraph/util/string_utils.h>


namespace hgraph
{
    template <typename T>
    struct HGRAPH_EXPORT TimeSeriesValueOutput : TimeSeriesOutput
    {
        using value_type = T;
        using ptr        = nb::ref<TimeSeriesValueOutput<T>>;

        // Constructors
        explicit TimeSeriesValueOutput(const node_ptr &parent);
        explicit TimeSeriesValueOutput(const TimeSeriesType::ptr &parent);

        // TimeSeriesType interface (best-effort)
        [[nodiscard]] node_ptr  owning_node() override;
        [[nodiscard]] node_ptr  owning_node() const override;
        [[nodiscard]] graph_ptr owning_graph() override;
        [[nodiscard]] graph_ptr owning_graph() const override;
        void                    re_parent(const node_ptr &) override;
        void                    re_parent(const TimeSeriesType::ptr &) override;
        [[nodiscard]] bool      has_owning_node() const override;
        [[nodiscard]] bool      is_reference() const override;
        [[nodiscard]] bool      has_reference() const override;

        //This is a candidate for removal later
        void                    reset_parent_or_node() override;

        // Python interop
        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;

        void py_set_value(nb::object value) override;
        bool can_apply_result(nb::object value) override;
        void apply_result(nb::object value) override;

        // Value API
        [[nodiscard]] const T &value() const;

        void set_value(const T &v);

        void set_value(T &&v);

        // Output state and operations
        void                                mark_invalid() override;
        void                                mark_modified() override;
        void                                mark_child_modified(TimeSeriesOutput &, engine_time_t modified_time) override;
        [[nodiscard]] bool                  modified() const override;
        [[nodiscard]] bool                  valid() const override;
        [[nodiscard]] bool                  all_valid() const override;
        [[nodiscard]] engine_time_t         last_modified_time() const override;
        [[nodiscard]] TimeSeriesOutput::ptr parent_output() const override;
        [[nodiscard]] TimeSeriesOutput::ptr parent_output() override;
        [[nodiscard]] bool                  has_parent_output() const override;
        void                                subscribe(Notifiable *node) override;
        void                                un_subscribe(Notifiable *node) override;
        void                                builder_release_cleanup() override;
        void                                clear() override;
        void                                invalidate() override;
        void                                mark_modified(engine_time_t modified_time) override;
        void                                notify(engine_time_t) override;

        void               copy_from_output(const TimeSeriesOutput &output) override;
        void               copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        [[nodiscard]] engine_time_t current_engine_time() const override;

        TSOutput &ts();
        const TSOutput &ts() const;

      private:
        TSOutput _ts;
    };

    template <typename T>
    struct HGRAPH_EXPORT TimeSeriesValueInput : TimeSeriesInput
    {
        using value_type = T;
        using ptr        = nb::ref<TimeSeriesValueInput<T>>;

        // Constructors
        TimeSeriesValueInput() = default;
        explicit TimeSeriesValueInput(const node_ptr &parent);

        explicit TimeSeriesValueInput(const TimeSeriesType::ptr &parent);

        // TimeSeriesType interface (best-effort)
        [[nodiscard]] node_ptr  owning_node() override;
        [[nodiscard]] node_ptr  owning_node() const override;
        [[nodiscard]] graph_ptr owning_graph() override;
        [[nodiscard]] graph_ptr owning_graph() const override;

        void re_parent(const node_ptr &) override;

        void re_parent(const TimeSeriesType::ptr &) override;

        [[nodiscard]] bool has_owning_node() const override;
        [[nodiscard]] bool is_reference() const override;
        [[nodiscard]] bool has_reference() const override;

        void reset_parent_or_node() override;

        // Relationship helpers
        [[nodiscard]] TimeSeriesInput::ptr parent_input() const override;
        [[nodiscard]] bool                 has_parent_input() const override;

        // Binding / peer state
        [[nodiscard]] bool                   bound() const override;
        [[nodiscard]] bool                   has_peer() const override;
        [[nodiscard]] time_series_output_ptr output() const override;

        bool bind_output(time_series_output_ptr output_) override;

        void un_bind_output(bool) override;

        // Activity state
        [[nodiscard]] bool active() const override;
        void               make_active() override;
        void               make_passive() override;
        [[nodiscard]] bool has_output() const override;

        // Minimal teardown for builders
        void builder_release_cleanup() override;

        // Type/graph-facing methods implemented using TSInput
        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object    py_delta_value() const override;
        [[nodiscard]] bool          modified() const override;
        [[nodiscard]] bool          valid() const override;
        [[nodiscard]] bool          all_valid() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;

        [[nodiscard]] time_series_reference_output_ptr reference_output() const override;

        [[nodiscard]] const TimeSeriesInput *get_input(size_t) const override;
        [[nodiscard]] TimeSeriesInput *      get_input(size_t) override;

        // Extra helpers
        [[nodiscard]] const T &value() const;

        [[nodiscard]] TimeSeriesValueOutput<T> &value_output();

        [[nodiscard]] const TimeSeriesValueOutput<T> &value_output() const;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        void                        notify(engine_time_t et) override;
        [[nodiscard]] engine_time_t current_engine_time() const override;

        void notify_parent(TimeSeriesInput *child, engine_time_t et) override;

      private:
        TSInput _ts{static_cast<Notifiable *>(nullptr), typeid(T)};
    };

    void register_ts_with_nanobind(nb::module_ &m);
} // namespace hgraph

#endif  // TS_H