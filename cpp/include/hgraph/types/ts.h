//
// Created by Howard Henson on 03/01/2025.
//

#ifndef TS_H
#define TS_H

#include <hgraph/types/base_time_series.h>
#include <hgraph/types/v2/ts_value.h>
#include <hgraph/types/v2_adaptor.h>

namespace hgraph {

    /**
     * Non-templated TimeSeriesValueOutput using v2 TSOutput for value storage.
     * This replaces the templated TimeSeriesValueOutput<T> to reduce template instantiations.
     *
     * Inherits from TimeSeriesOutput and implements NotifiableContext for the TSOutput.
     * Delegates value storage and modification tracking to TSOutput.
     */
    struct HGRAPH_EXPORT TimeSeriesValueOutput final : TimeSeriesOutput, NotifiableContext {
        using s_ptr = std::shared_ptr<TimeSeriesValueOutput>;

        // Constructor takes type_info for type checking
        explicit TimeSeriesValueOutput(node_ptr parent, const std::type_info &tp);
        explicit TimeSeriesValueOutput(time_series_output_ptr parent, const std::type_info &tp);

        // NotifiableContext implementation
        void notify(engine_time_t et) override;
        [[nodiscard]] engine_time_t current_engine_time() const override;
        void add_before_evaluation_notification(std::function<void()> &&fn) override;
        void add_after_evaluation_notification(std::function<void()> &&fn) override;

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
        }

        template <typename T> void set_value(T&& v) {
            AnyValue<> av;
            av.template emplace<std::decay_t<T>>(std::forward<T>(v));
            _ts_output.set_value(std::move(av));
        }

        void mark_invalid() override;
        void invalidate() override;
        void copy_from_output(const TimeSeriesOutput &output) override;
        void copy_from_input(const TimeSeriesInput &input) override;
        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        // Access to type info
        [[nodiscard]] const std::type_info& value_type() const { return _ts_output.value_type(); }

        // Access to underlying TSOutput for direct binding
        [[nodiscard]] TSOutput& ts_output() { return _ts_output; }
        [[nodiscard]] const TSOutput& ts_output() const { return _ts_output; }

        [[nodiscard]] node_ptr                owning_node() override;
        [[nodiscard]] node_ptr                owning_node() const override;
        [[nodiscard]] graph_ptr               owning_graph() override;
        [[nodiscard]] graph_ptr               owning_graph() const override;
        [[nodiscard]] bool                    has_parent_or_node() const override;
        [[nodiscard]] bool                    has_owning_node() const override;
        [[nodiscard]] engine_time_t           last_modified_time() const override;
        [[nodiscard]] bool                    modified() const override;
        [[nodiscard]] bool                    valid() const override;
        [[nodiscard]] bool                    all_valid() const override;
        void                                  re_parent(node_ptr parent) override;
        void                                  re_parent(const time_series_type_ptr parent) override;
        void                                  reset_parent_or_node() override;
        void                                  builder_release_cleanup() override;
        [[nodiscard]] bool                    is_reference() const override;
        [[nodiscard]] bool                    has_reference() const override;
        [[nodiscard]] TimeSeriesOutput::s_ptr parent_output() const override;
        [[nodiscard]] TimeSeriesOutput::s_ptr parent_output() override;
        [[nodiscard]] bool                    has_parent_output() const override;
        void                                  subscribe(Notifiable *node) override;
        void                                  un_subscribe(Notifiable *node) override;
        void                                  clear() override;
        void                                  mark_modified() override;
        void                                  mark_modified(engine_time_t modified_time) override;
        void                                  mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) override;

        VISITOR_SUPPORT()

      private:
        ParentAdapter<TimeSeriesOutput> _parent_adapter;
        TSOutput                        _ts_output;
    };

    /**
     * Non-templated TimeSeriesValueInput using v2 TSInput for binding/value access.
     *
     * Inherits from TimeSeriesInput and implements NotifiableContext for the TSInput.
     * Delegates binding and value access to TSInput.
     */
    struct HGRAPH_EXPORT TimeSeriesValueInput final : TimeSeriesInput, NotifiableContext {
        using ptr = TimeSeriesValueInput*;
        using s_ptr = std::shared_ptr<TimeSeriesValueInput>;

        // Constructor takes type_info for type checking
        explicit TimeSeriesValueInput(node_ptr parent, const std::type_info &tp);
        explicit TimeSeriesValueInput(time_series_input_ptr parent, const std::type_info &tp);

        // NotifiableContext implementation
        void notify(engine_time_t et) override;
        [[nodiscard]] engine_time_t current_engine_time() const override;
        void add_before_evaluation_notification(std::function<void()> &&fn) override;
        void add_after_evaluation_notification(std::function<void()> &&fn) override;

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

        // Binding operations
        bool bind_output(const time_series_output_s_ptr & output_) override;
        void un_bind_output(bool unbind_refs) override;

        // Access to type info
        [[nodiscard]] const std::type_info& value_type() const { return _ts_input.value_type(); }

        // Access to underlying TSInput for direct operations
        [[nodiscard]] TSInput& ts_input() { return _ts_input; }
        [[nodiscard]] const TSInput& ts_input() const { return _ts_input; }

        // TimeSeriesInput interface - delegate to _parent_adapter and _ts_input
        [[nodiscard]] node_ptr                owning_node() override;
        [[nodiscard]] node_ptr                owning_node() const override;
        [[nodiscard]] graph_ptr               owning_graph() override;
        [[nodiscard]] graph_ptr               owning_graph() const override;
        [[nodiscard]] bool                    has_parent_or_node() const override;
        [[nodiscard]] bool                    has_owning_node() const override;
        [[nodiscard]] engine_time_t           last_modified_time() const override;
        [[nodiscard]] bool                    modified() const override;
        [[nodiscard]] bool                    valid() const override;
        [[nodiscard]] bool                    all_valid() const override;
        void                                  re_parent(node_ptr parent) override;
        void                                  re_parent(const time_series_type_ptr parent) override;
        void                                  reset_parent_or_node() override;
        void                                  builder_release_cleanup() override;
        [[nodiscard]] bool                    is_reference() const override;
        [[nodiscard]] bool                    has_reference() const override;
        [[nodiscard]] TimeSeriesInput::s_ptr  parent_input() const override;
        [[nodiscard]] bool                    has_parent_input() const override;
        [[nodiscard]] bool                    active() const override;
        void                                  make_active() override;
        void                                  make_passive() override;
        [[nodiscard]] bool                    bound() const override;
        [[nodiscard]] bool                    has_peer() const override;
        [[nodiscard]] time_series_output_s_ptr output() const override;
        [[nodiscard]] bool                    has_output() const override;
        [[nodiscard]] time_series_reference_output_s_ptr reference_output() const override;
        [[nodiscard]] TimeSeriesInput::s_ptr  get_input(size_t index) override;
        void                                  notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override;

        VISITOR_SUPPORT()

    private:
        friend struct TimeSeriesValueOutput;  // For copy_from_input
        ParentAdapter<TimeSeriesInput> _parent_adapter;
        TSInput                        _ts_input;
        time_series_output_s_ptr       _bound_output;  // Track bound output
    };

    void register_ts_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TS_H
