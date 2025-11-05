//
// Created by Howard Henson on 03/01/2025.
//

#ifndef REF_H
#define REF_H

#include <hgraph/types/time_series_type.h>
#include <hgraph/types/v2/ts_value.h>

namespace hgraph
{
    struct HGRAPH_EXPORT TimeSeriesReference : nb::intrusive_base
    {
        using ptr = nb::ref<TimeSeriesReference>;

        virtual void bind_input(TimeSeriesInput &ts_input) const = 0;

        virtual bool has_output() const = 0;

        virtual bool is_empty() const = 0;

        virtual bool is_valid() const = 0;

        virtual bool operator==(const TimeSeriesReferenceOutput &other) const = 0;

        virtual std::string to_string() const = 0;

        static ptr make();

        static ptr make(time_series_output_ptr output);

        static ptr make(std::vector<ptr> items);

        static ptr make(const std::vector<nb::ref<TimeSeriesReferenceInput>>& items);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct EmptyTimeSeriesReference final : TimeSeriesReference
    {
        void bind_input(TimeSeriesInput &ts_input) const override;

        bool has_output() const override;

        bool is_empty() const override;

        bool is_valid() const override;

        bool operator==(const TimeSeriesReferenceOutput &other) const override;

        std::string to_string() const override;
    };

    struct BoundTimeSeriesReference final : TimeSeriesReference
    {
        explicit BoundTimeSeriesReference(const time_series_output_ptr &output);

        const TimeSeriesOutput::ptr &output() const;

        void bind_input(TimeSeriesInput &input_) const override;

        bool has_output() const override;

        bool is_empty() const override;

        bool is_valid() const override;

        bool operator==(const TimeSeriesReferenceOutput &other) const override;

        std::string to_string() const override;

      private:
        TimeSeriesOutput::ptr _output;
    };

    struct UnBoundTimeSeriesReference final : TimeSeriesReference
    {
        explicit UnBoundTimeSeriesReference(std::vector<ptr> items);

        const std::vector<ptr> &items() const;

        void bind_input(TimeSeriesInput &input_) const override;

        bool has_output() const override;

        bool is_empty() const override;

        bool is_valid() const override;

        bool operator==(const TimeSeriesReferenceOutput &other) const override;

        const ptr &operator[](size_t ndx);

        std::string to_string() const override;

      private:
        std::vector<ptr> _items;
    };

    // Use this definition of the ref-value-type to make it easier to switch
    // out later when we turn the ref into a proper value type.
    using ref_value_tp = TimeSeriesReference::ptr;

    // ORIGINAL: Inherited from BaseTimeSeriesOutput with _value member
    // NEW: Direct TimeSeriesOutput inheritance, uses TSOutput _ts for storage
    // REF-SPECIFIC: Maintains _reference_observers for bind_input notification
    struct TimeSeriesReferenceOutput : TimeSeriesOutput
    {
        // Constructors
        explicit TimeSeriesReferenceOutput(const node_ptr &parent);
        explicit TimeSeriesReferenceOutput(const TimeSeriesType::ptr &parent);

        // TimeSeriesType interface (delegated to _ts or implemented)
        [[nodiscard]] node_ptr  owning_node() override;
        [[nodiscard]] node_ptr  owning_node() const override;
        [[nodiscard]] graph_ptr owning_graph() override;
        [[nodiscard]] graph_ptr owning_graph() const override;
        void                    re_parent(const node_ptr &) override;
        void                    re_parent(const TimeSeriesType::ptr &) override;
        [[nodiscard]] bool      has_owning_node() const override;
        [[nodiscard]] bool      is_reference() const override;
        [[nodiscard]] bool      has_reference() const override;
        void                    reset_parent_or_node() override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        // The value is either a reference ptr or a value object, in either
        // case will return a non-reference
        ref_value_tp value() const;
        ref_value_tp      value();

        // Python interop
        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;
        void                     py_set_value(nb::object value) override;
        bool                     can_apply_result(nb::object value) override;
        void                     apply_result(nb::object value) override;

        // ORIGINAL: set_value() assigned to _value then called mark_modified()
        // NEW: Stores to _ts.set_value() which handles modification internally
        // REF-SPECIFIC: Still needs to notify reference observers after setting
        void set_value(ref_value_tp value);

        // Output state and operations - delegated to _ts
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

        // REF-SPECIFIC: Reference observer management
        // This now standardised by using the fact that the TSI is an instance of Notifiable
        // So we can just follow the standard subscription model.
        void observe_reference(TimeSeriesInput::ptr input_);
        void stop_observing_reference(TimeSeriesInput::ptr input_);

        // Accessors for internal TSOutput
        TSOutput       &ts();
        const TSOutput &ts() const;

        static void register_with_nanobind(nb::module_ &m);

      private:
        friend struct TimeSeriesReferenceInput;
        friend struct TimeSeriesReference;

        TSOutput _ts;
    };

    /**
     * Reference inputs have 2 main modes of operations, namely peered or non-peered.
     * In peered mode, this works just like any other reference time-series. But in non-peered mode
     * the input becomes the same as a TSL/TSB in that it holds an indexed collection of inputs.
     *
     * The key modes of binding these values are:
     * 1. During the wiring phase, the builder may have non-peered edges it needs to bind.
     * 2. When an UnBoundTimeSeriesReference is received and the bind_input method is called.
     *
     * This also has a special start method, this seems to be a side effect of the model not being
     * normal, i.e. if we bind a reference output to a non-reference input, we don't have the normal
     * notification path. This may be something we can correct with the new modeling concept, as
     * we should be able to better model this issue with the new V2 model.
     */
    struct TimeSeriesReferenceInput : TimeSeriesInput
    {
        using ptr = nb::ref<TimeSeriesReferenceInput>;

        // Constructors
        explicit TimeSeriesReferenceInput(const node_ptr &parent);
        explicit TimeSeriesReferenceInput(const TimeSeriesType::ptr &parent);

        // TimeSeriesType interface
        [[nodiscard]] node_ptr  owning_node() override;
        [[nodiscard]] node_ptr  owning_node() const override;
        [[nodiscard]] graph_ptr owning_graph() override;
        [[nodiscard]] graph_ptr owning_graph() const override;
        void                    re_parent(const node_ptr &) override;
        void                    re_parent(const TimeSeriesType::ptr &) override;
        [[nodiscard]] bool      has_owning_node() const override;
        [[nodiscard]] bool      is_reference() const override;
        [[nodiscard]] bool      has_reference() const override;
        void                    reset_parent_or_node() override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        // REF-SPECIFIC: Start handling for scheduled inputs
        void start();

        // Python interop
        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] ref_value_tp value() const;

        // REF-SPECIFIC: Clone binding from another input
        void clone_binding(const TimeSeriesReferenceInput::ptr &other);

        // Binding / peer state
        [[nodiscard]] bool                   bound() const override;
        [[nodiscard]] bool                   has_peer() const override;
        [[nodiscard]] time_series_output_ptr output() const override;
        bool                                 bind_output(time_series_output_ptr value) override;
        void                                 un_bind_output(bool unbind_refs) override;

        // Activity state
        [[nodiscard]] bool active() const override;
        void               make_active() override;
        void               make_passive() override;
        [[nodiscard]] bool has_output() const override;

        // State - delegates to _ts when bound, otherwise uses local state
        [[nodiscard]] bool          modified() const override;
        [[nodiscard]] bool          valid() const override;
        [[nodiscard]] bool          all_valid() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;

        // Input access
        [[nodiscard]] TimeSeriesInput          *get_input(size_t index) override;
        [[nodiscard]] const TimeSeriesInput    *get_input(size_t index) const override;
        [[nodiscard]] TimeSeriesReferenceInput *get_ref_input(size_t index);

        // Relationship helpers
        [[nodiscard]] TimeSeriesInput::ptr             parent_input() const override;
        [[nodiscard]] bool                             has_parent_input() const override;
        [[nodiscard]] time_series_reference_output_ptr reference_output() const override;

        void                        builder_release_cleanup() override;
        void                        notify(engine_time_t et) override;

        // Accessors for internal TSInput
        TSInput       &ts();
        const TSInput &ts() const;

        void notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override;

        static void register_with_nanobind(nb::module_ &m);

      private:
        friend struct TimeSeriesReferenceOutput;
        friend struct TimeSeriesReference;

        TSInput _ts;  // NEW: v2 storage using AnyValue<ref_value_tp> when bound

        // We will need to track _items for now as this drifts into collections
        // This is effectively a non-peered collection of values (for example a TSL)
        // Once we have a solution for that scenario, we can re-look into how we
        // deal with it here.
        std::optional<std::vector<TimeSeriesReferenceInput::ptr>> _items;
        static inline std::vector<TimeSeriesReferenceInput::ptr>  empty_items{};
    };
}  // namespace hgraph

#endif  // REF_H