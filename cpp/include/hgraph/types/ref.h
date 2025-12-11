//
// Created by Howard Henson on 03/01/2025.
//

#ifndef REF_H
#define REF_H

#include <hgraph/types/v2/ts_value.h>
#include <hgraph/types/v2_adaptor.h>

#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/base_time_series.h>

namespace hgraph
{
    struct HGRAPH_EXPORT TimeSeriesReference
    {
        enum class Kind : uint8_t { EMPTY = 0, BOUND = 1, UNBOUND = 2 };

        // Copy/Move semantics
        TimeSeriesReference(const TimeSeriesReference &other);
        TimeSeriesReference(TimeSeriesReference &&other) noexcept;
        TimeSeriesReference &operator=(const TimeSeriesReference &other);
        TimeSeriesReference &operator=(TimeSeriesReference &&other) noexcept;
        ~TimeSeriesReference();

        // Query methods
        [[nodiscard]] Kind kind() const noexcept { return _kind; }
        [[nodiscard]] bool is_empty() const noexcept { return _kind == Kind::EMPTY; }
        [[nodiscard]] bool is_bound() const noexcept { return _kind == Kind::BOUND; }
        [[nodiscard]] bool is_unbound() const noexcept { return _kind == Kind::UNBOUND; }
        [[nodiscard]] bool has_output() const;
        [[nodiscard]] bool is_valid() const;

        // Accessors (throw if wrong kind)
        [[nodiscard]] const time_series_output_s_ptr         &output() const;
        [[nodiscard]] const std::vector<TimeSeriesReference> &items() const;
        [[nodiscard]] const TimeSeriesReference              &operator[](size_t ndx) const;

        // Operations
        void                      bind_input(TimeSeriesInput &ts_input) const;
        bool                      operator==(const TimeSeriesReference &other) const;
        [[nodiscard]] std::string to_string() const;

        // Factory methods - use these to construct instances
        static TimeSeriesReference make();
        static TimeSeriesReference make(time_series_output_s_ptr output);
        static TimeSeriesReference make(std::vector<TimeSeriesReference> items);
        static TimeSeriesReference make(const std::vector<time_series_input_s_ptr> &items);

      private:
        // Private constructors - must use make() factory methods
        TimeSeriesReference() noexcept;                                        // Empty
        explicit TimeSeriesReference(time_series_output_s_ptr output);         // Bound
        explicit TimeSeriesReference(std::vector<TimeSeriesReference> items);  // Unbound

        Kind _kind;

        // Union for the three variants - only one is active at a time
        union Storage {
            // Empty uses no storage
            char empty;
            // Bound stores a shared_ptr to keep output alive (mirrors original nb::ref behavior)
            time_series_output_s_ptr bound;
            // Unbound stores a vector of references
            std::vector<TimeSeriesReference> unbound;

            Storage() noexcept : empty{} {}
            ~Storage() {}  // Manual destruction based on kind
        } _storage;

        // Helper methods for variant management
        void destroy() noexcept;
        void copy_from(const TimeSeriesReference &other);
        void move_from(TimeSeriesReference &&other) noexcept;
    };

    struct TimeSeriesReferenceOutput : BaseTimeSeriesOutput
    {
        using BaseTimeSeriesOutput::BaseTimeSeriesOutput;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        const TimeSeriesReference &value() const;  // Throws if no value

        TimeSeriesReference &value();  // Throws if no value

        // Python-safe value access that returns the reference value or makes an empty one
        TimeSeriesReference py_value_or_empty() const;

        void py_set_value(const nb::object &value) override;

        void set_value(TimeSeriesReference value);

        void apply_result(const nb::object &value) override;

        bool can_apply_result(const nb::object &value) override;

        // Registers an input as observing the reference value
        void observe_reference(TimeSeriesInput::ptr input_);

        // Unregisters an input as observing the reference value
        void stop_observing_reference(TimeSeriesInput::ptr input_);

        // Clears the reference by setting it to an empty reference
        void clear() override;

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        void invalidate() override;

        void copy_from_output(const TimeSeriesOutput &output) override;

        void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] bool is_reference() const override;

        [[nodiscard]] bool has_reference() const override;

        VISITOR_SUPPORT()

        [[nodiscard]] bool has_value() const;

      protected:
        void reset_value();

      private:
        friend struct TimeSeriesReferenceInput;
        friend struct TimeSeriesReference;
        std::optional<TimeSeriesReference> _value;
        // Use a raw pointer as we don't have hash implemented on ptr at the moment,
        // So this is a work arround the code managing this also ensures the pointers are incremented
        // and decremented.
        std::unordered_set<TimeSeriesInput::ptr> _reference_observers;
    };

    // Marker interface for all reference inputs - allows dynamic_cast to identify reference types.
    // This is NOT a class with implementation - it just marks a type as being a reference input.
    // Subclasses must inherit from TimeSeriesInput separately (via BaseTimeSeriesInput or directly).
    struct TimeSeriesReferenceInput : TimeSeriesInput
    {
        using ptr   = TimeSeriesReferenceInput *;
        using s_ptr = std::shared_ptr<TimeSeriesReferenceInput>;

        virtual ~TimeSeriesReferenceInput() = default;

        virtual std::vector<time_series_input_s_ptr>       &items()       = 0;
        virtual const std::vector<time_series_input_s_ptr> &items() const = 0;

        [[nodiscard]] virtual TimeSeriesReference value() const                                      = 0;
        virtual void                              clone_binding(TimeSeriesReferenceInput::ptr other) = 0;

        virtual bool                               has_value() const          = 0;
        virtual std::optional<TimeSeriesReference> raw_value()                = 0;
        virtual time_series_input_s_ptr            clone_blank_ref_instance() = 0;

        virtual void start() = 0;

        VISITOR_SUPPORT()
    };

    // Base implementation for reference inputs that use the old-style output binding
    // Note: Does NOT inherit from TimeSeriesReferenceInput to avoid ambiguous visitor issues.
    // Use is_reference()/has_reference() to check if an input is a reference type.
    struct BaseTimeSeriesReferenceInput : BaseTimeSeriesInput<TimeSeriesReferenceInput>
    {
        using BaseTimeSeriesInput::BaseTimeSeriesInput;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override {
            return dynamic_cast<const TimeSeriesReferenceInput *>(other) != nullptr;
        }

        [[nodiscard]] bool is_reference() const override { return true; }
        [[nodiscard]] bool has_reference() const override { return true; }

        void start() override;

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] TimeSeriesReference value() const override;

        [[nodiscard]] bool bound() const override;

        [[nodiscard]] bool modified() const override;

        [[nodiscard]] bool valid() const override;

        [[nodiscard]] bool all_valid() const override;

        [[nodiscard]] engine_time_t last_modified_time() const override;

        void clone_binding(TimeSeriesReferenceInput::ptr other) override;

        bool bind_output(const time_series_output_s_ptr &value) override;

        void un_bind_output(bool unbind_refs) override;

        void make_active() override;

        void make_passive() override;

        [[nodiscard]] TimeSeriesInput::s_ptr get_input(size_t index) override;

        [[nodiscard]] virtual TimeSeriesReferenceInput *get_ref_input(size_t index);

        std::vector<time_series_input_s_ptr>       &items() override { return empty_items; }
        const std::vector<time_series_input_s_ptr> &items() const override { return empty_items; }

      protected:
        friend struct PyTimeSeriesReferenceInput;
        bool do_bind_output(time_series_output_s_ptr output_) override;

        void do_un_bind_output(bool unbind_refs) override;

        TimeSeriesReferenceOutput *output_t() const;

        TimeSeriesReferenceOutput *output_t();

        void notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override;

        [[nodiscard]] bool has_value() const override;

        void reset_value();

        std::optional<TimeSeriesReference> raw_value() override;

        mutable std::optional<TimeSeriesReference> _value;

        static inline std::vector<time_series_input_s_ptr> empty_items{};

      private:
        friend struct TimeSeriesReferenceOutput;
        friend struct TimeSeriesReference;
    };

    // ============================================================
    // Specialized Reference Input Classes
    // ============================================================

    struct TimeSeriesValueReferenceInput final : TimeSeriesReferenceInput, NotifiableContext
    {
        explicit TimeSeriesValueReferenceInput(node_ptr parent);
        explicit TimeSeriesValueReferenceInput(time_series_input_ptr parent);

        void notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override;

        void notify(engine_time_t et) override;

        [[nodiscard]] engine_time_t current_engine_time() const override;

        void add_before_evaluation_notification(std::function<void()> &&fn) override;

        void add_after_evaluation_notification(std::function<void()> &&fn) override;

        [[nodiscard]] node_ptr owning_node() override;

        [[nodiscard]] node_ptr owning_node() const override;

        [[nodiscard]] graph_ptr owning_graph() override;

        [[nodiscard]] graph_ptr owning_graph() const override;

        [[nodiscard]] bool has_parent_or_node() const override;

        [[nodiscard]] bool has_owning_node() const override;

        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] TimeSeriesReference value() const override;

        [[nodiscard]] engine_time_t last_modified_time() const override;
        [[nodiscard]] bool          modified() const override;
        [[nodiscard]] bool          valid() const override;
        [[nodiscard]] bool          all_valid() const override;

        void re_parent(node_ptr parent) override;

        void re_parent(const time_series_type_ptr parent) override;

        void reset_parent_or_node() override;

        void builder_release_cleanup() override {
            // Think about what may be required to be done here?
        }

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        [[nodiscard]] bool is_reference() const override { return true; }
        [[nodiscard]] bool has_reference() const override { return true; }

        [[nodiscard]] TimeSeriesInput::s_ptr parent_input() const override;

        [[nodiscard]] bool has_parent_input() const override;

        [[nodiscard]] bool active() const override;

        void make_active() override;
        void make_passive() override;

        [[nodiscard]] bool bound() const override;

        [[nodiscard]] bool has_peer() const override;

        [[nodiscard]] time_series_output_s_ptr output() const override {
            return _bound_reference_output;  // Return the reference output we're bound to (if any)
        }

        [[nodiscard]] bool has_output() const override {
            return _bound_reference_output != nullptr;
        }

        bool bind_output(const time_series_output_s_ptr &output_) override;
        void un_bind_output(bool unbind_refs) override;

        [[nodiscard]] time_series_reference_output_s_ptr reference_output() const override {
            throw std::runtime_error("TimeSeriesValueReferenceInput does not support reference_output");
        }

        [[nodiscard]] TimeSeriesInput::s_ptr get_input(size_t index) override {
            throw std::runtime_error("TimeSeriesValueReferenceInput does not support get_input");
        }

        void clone_binding(TimeSeriesReferenceInput::ptr other) override;

        std::vector<time_series_input_s_ptr>       &items() override { return empty_items; }
        const std::vector<time_series_input_s_ptr> &items() const override { return empty_items; }

        bool has_value() const override { return _ts_input.valid(); }

        std::optional<TimeSeriesReference> raw_value() override {
            if (_ts_input.valid()) {
                auto value{_ts_input.value().get_if<TimeSeriesReference>()};
                if (value != nullptr) { return {*value}; }
            }
            return {};
        };

        time_series_input_s_ptr clone_blank_ref_instance() override {
            if (_parent_adapter.has_parent_input()) {
                return std::make_shared<TimeSeriesValueReferenceInput>(_parent_adapter.parent_input());
            }
            return std::make_shared<TimeSeriesValueReferenceInput>(_parent_adapter.owning_node());
        }

        void start() override;

        VISITOR_SUPPORT();

      private:
        friend struct TimeSeriesValueReferenceOutput;
        ParentAdapter<TimeSeriesInput> _parent_adapter;
        TSInput                        _ts_input;
        time_series_output_s_ptr       _bound_reference_output;  // Track bound reference output

        static inline std::vector<time_series_input_s_ptr> empty_items{};
    };

    struct TimeSeriesListReferenceInput final : BaseTimeSeriesReferenceInput
    {
        using BaseTimeSeriesReferenceInput::BaseTimeSeriesReferenceInput;

        // Constructor that accepts size
        TimeSeriesListReferenceInput(Node *owning_node, InputBuilder::ptr value_builder, size_t size);
        TimeSeriesListReferenceInput(TimeSeriesInput *parent_input, InputBuilder::ptr value_builder, size_t size);

        TimeSeriesInput::s_ptr            get_input(size_t index) override;
        size_t                            size() const { return _size; }
        [[nodiscard]] TimeSeriesReference value() const override;

        [[nodiscard]] bool          bound() const override;
        [[nodiscard]] bool          modified() const override;
        [[nodiscard]] bool          valid() const override;
        [[nodiscard]] bool          all_valid() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;
        void                        clone_binding(TimeSeriesReferenceInput::ptr other) override;

        std::vector<time_series_input_s_ptr>       &items() override;
        const std::vector<time_series_input_s_ptr> &items() const override;

        void make_active() override;
        void make_passive() override;

        VISITOR_SUPPORT()

        time_series_input_s_ptr clone_blank_ref_instance() override;

        [[nodiscard]] TimeSeriesReferenceInput *get_ref_input(size_t index) override;

      private:
        InputBuilder::ptr                                   _value_builder;
        size_t                                              _size{0};
        std::optional<std::vector<time_series_input_s_ptr>> _items;
    };

    struct TimeSeriesBundleReferenceInput final : BaseTimeSeriesReferenceInput
    {
        using BaseTimeSeriesReferenceInput::BaseTimeSeriesReferenceInput;

        // Constructor that accepts size
        TimeSeriesBundleReferenceInput(Node *owning_node, std::vector<InputBuilder::ptr> value_builders, size_t size);
        TimeSeriesBundleReferenceInput(TimeSeriesInput *parent_input, std::vector<InputBuilder::ptr> value_builders, size_t size);

        TimeSeriesReference         value() const override;
        size_t                      size() const { return _size; }
        [[nodiscard]] bool          bound() const override;
        [[nodiscard]] bool          modified() const override;
        [[nodiscard]] bool          valid() const override;
        [[nodiscard]] bool          all_valid() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;
        void                        clone_binding(TimeSeriesReferenceInput::ptr other) override;

        std::vector<time_series_input_s_ptr>       &items() override;
        const std::vector<time_series_input_s_ptr> &items() const override;

        void make_active() override;
        void make_passive() override;

        VISITOR_SUPPORT()

        time_series_input_s_ptr clone_blank_ref_instance() override;

        [[nodiscard]] TimeSeriesReferenceInput *get_ref_input(size_t index) override;

      private:
        std::vector<InputBuilder::ptr>                      _value_builders;
        size_t                                              _size{0};
        std::optional<std::vector<time_series_input_s_ptr>> _items;
    };

    struct TimeSeriesDictReferenceInput final : BaseTimeSeriesReferenceInput
    {
        using BaseTimeSeriesReferenceInput::BaseTimeSeriesReferenceInput;

        VISITOR_SUPPORT()

        time_series_input_s_ptr clone_blank_ref_instance() override;
    };

    struct TimeSeriesSetReferenceInput final : BaseTimeSeriesReferenceInput
    {
        using BaseTimeSeriesReferenceInput::BaseTimeSeriesReferenceInput;

        VISITOR_SUPPORT()

        time_series_input_s_ptr clone_blank_ref_instance() override;
    };

    struct TimeSeriesWindowReferenceInput final : BaseTimeSeriesReferenceInput
    {
        using BaseTimeSeriesReferenceInput::BaseTimeSeriesReferenceInput;

        VISITOR_SUPPORT()

        time_series_input_s_ptr clone_blank_ref_instance() override;
    };

    // ============================================================
    // Specialized Reference Output Classes
    // ============================================================

    struct TimeSeriesValueReferenceOutput final : TimeSeriesOutput, NotifiableContext
    {
        explicit TimeSeriesValueReferenceOutput(node_ptr parent);
        explicit TimeSeriesValueReferenceOutput(time_series_output_ptr parent);

        void notify(engine_time_t et) override;

        [[nodiscard]] engine_time_t current_engine_time() const override;

        void add_before_evaluation_notification(std::function<void()> &&fn) override;

        void add_after_evaluation_notification(std::function<void()> &&fn) override;

        [[nodiscard]] node_ptr      owning_node() override;
        [[nodiscard]] node_ptr      owning_node() const override;
        [[nodiscard]] graph_ptr     owning_graph() override;
        [[nodiscard]] graph_ptr     owning_graph() const override;
        [[nodiscard]] bool          has_parent_or_node() const override;
        [[nodiscard]] bool          has_owning_node() const override;
        [[nodiscard]] nb::object    py_value() const override;
        [[nodiscard]] nb::object    py_delta_value() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;
        [[nodiscard]] bool          modified() const override;
        [[nodiscard]] bool          valid() const override;
        [[nodiscard]] bool          all_valid() const override;
        void                        re_parent(node_ptr parent) override;
        void                        re_parent(const time_series_type_ptr parent) override;
        void                        reset_parent_or_node() override;
        void                        builder_release_cleanup() override;
        [[nodiscard]] bool          is_same_type(const TimeSeriesType *other) const override;
        [[nodiscard]] bool          is_reference() const override;
        [[nodiscard]] bool          has_reference() const override;
        [[nodiscard]] s_ptr         parent_output() const override;
        [[nodiscard]] s_ptr         parent_output() override;
        [[nodiscard]] bool          has_parent_output() const override;
        void                        subscribe(Notifiable *node) override;
        void                        un_subscribe(Notifiable *node) override;
        void                        apply_result(const nb::object &value) override;
        void                        py_set_value(const nb::object &value) override;
        void                        copy_from_output(const TimeSeriesOutput &output) override;
        void                        copy_from_input(const TimeSeriesInput &input) override;
        void                        clear() override;
        void                        invalidate() override;
        void                        mark_invalid() override;
        void                        mark_modified() override;
        void                        mark_modified(engine_time_t modified_time) override;
        void                        mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) override;
        bool                        can_apply_result(const nb::object &value) override;

        // Get the underlying TimeSeriesReference value (for binding purposes)
        [[nodiscard]] std::optional<TimeSeriesReference> reference_value() const;

        // Observer mechanism for inputs that bind through this reference output
        void observe_reference(TimeSeriesInput::ptr input_);
        void stop_observing_reference(TimeSeriesInput::ptr input_);

        VISITOR_SUPPORT()

      private:
        void notify_reference_observers();

        friend struct TimeSeriesValueReferenceInput;
        ParentAdapter<TimeSeriesOutput> _parent_adapter;
        TSOutput                        _ts_output;
        std::unordered_set<TimeSeriesInput::ptr> _reference_observers;
    };

    struct TimeSeriesListReferenceOutput final : TimeSeriesReferenceOutput
    {
        using TimeSeriesReferenceOutput::TimeSeriesReferenceOutput;

        // Constructor that accepts size
        TimeSeriesListReferenceOutput(Node *owning_node, OutputBuilder::ptr value_builder, size_t size);
        TimeSeriesListReferenceOutput(TimeSeriesOutput *parent_output, OutputBuilder::ptr value_builder, size_t size);

        size_t size() const { return _size; }

        VISITOR_SUPPORT()

      private:
        OutputBuilder::ptr _value_builder;
        size_t             _size{0};
    };

    struct TimeSeriesBundleReferenceOutput final : TimeSeriesReferenceOutput
    {
        using TimeSeriesReferenceOutput::TimeSeriesReferenceOutput;

        // Constructor that accepts size
        TimeSeriesBundleReferenceOutput(Node *owning_node, std::vector<OutputBuilder::ptr> value_builder, size_t size);
        TimeSeriesBundleReferenceOutput(TimeSeriesOutput *parent_output, std::vector<OutputBuilder::ptr> value_builder,
                                        size_t size);

        size_t size() const { return _size; }

        VISITOR_SUPPORT()

      private:
        // Fix this later, perhaps we can create a schema style object to ensure we don't have all this extra memory wasted.
        std::vector<OutputBuilder::ptr> _value_builder;
        size_t                          _size{0};
    };

    struct TimeSeriesDictReferenceOutput final : TimeSeriesReferenceOutput
    {
        using TimeSeriesReferenceOutput::TimeSeriesReferenceOutput;

        VISITOR_SUPPORT()
    };

    struct TimeSeriesSetReferenceOutput final : TimeSeriesReferenceOutput
    {
        using TimeSeriesReferenceOutput::TimeSeriesReferenceOutput;

        VISITOR_SUPPORT()
    };

    struct TimeSeriesWindowReferenceOutput final : TimeSeriesReferenceOutput
    {
        using TimeSeriesReferenceOutput::TimeSeriesReferenceOutput;

        VISITOR_SUPPORT()
    };

}  // namespace hgraph

// Include template implementations now that all types are fully defined
#include <hgraph/types/base_time_series_impl.h>

#endif  // REF_H