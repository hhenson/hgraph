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
    struct HGRAPH_EXPORT TimeSeriesValueOutput : TimeSeriesOutput
    {
        using ptr = nb::ref<TimeSeriesValueOutput>;

        // Constructors
        explicit TimeSeriesValueOutput(const node_ptr &parent, const std::type_info &tp);
        explicit TimeSeriesValueOutput(const TimeSeriesType::ptr &parent, const std::type_info &tp);

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

        // This is a candidate for removal later
        void reset_parent_or_node() override;

        // Python interop
        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;

        void py_set_value(nb::object value) override;
        bool can_apply_result(nb::object value) override;
        void apply_result(nb::object value) override;

        template <typename T> const T value() const {
            const auto &av = _ts.value();
            const T    *pv = av.template get_if<T>();
            if (!pv) throw std::bad_cast();
            return *pv;
        }

        template <typename T> void set_value(const T &v) {
            AnyValue<> any;
            any.emplace<T>(v);
            _ts.set_value(any);
        }

        template <typename T> void set_value(T &&v) {
            AnyValue<> any;
            any.emplace<T>(std::move(v));
            _ts.set_value(std::move(any));
        }

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

        void copy_from_output(const TimeSeriesOutput &output) override;
        void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        TSOutput       &ts();
        const TSOutput &ts() const;

      private:
        TSOutput _ts;
    };

    struct HGRAPH_EXPORT TimeSeriesValueInput : TimeSeriesInput
    {
        using ptr = nb::ref<TimeSeriesValueInput>;

        // Constructors
        explicit TimeSeriesValueInput(const node_ptr &parent, const std::type_info &tp);

        explicit TimeSeriesValueInput(const TimeSeriesType::ptr &parent, const std::type_info &tp);

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
        [[nodiscard]] TimeSeriesInput       *get_input(size_t) override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        void notify(engine_time_t et) override;

        void notify_parent(TimeSeriesInput *child, engine_time_t et) override;

        template <typename T> const T &value() const {
            const auto &av = _ts.value();
            const T    *pv = av.template get_if<T>();
            if (!pv) throw std::bad_cast();
            return *pv;
        }

        TSInput       &ts();
        const TSInput &ts() const;

      private:
        TSInput _ts;
    };

    void register_ts_with_nanobind(nb::module_ &m);
}  // namespace hgraph

#endif  // TS_H