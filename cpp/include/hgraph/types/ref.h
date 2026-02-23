//
// Created by Howard Henson on 03/01/2025.
//

#ifndef REF_H
#define REF_H

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
        static TimeSeriesReference make(const std::vector<TimeSeriesReferenceInput*>& items);
        static TimeSeriesReference make(const std::vector<std::shared_ptr<TimeSeriesReferenceInput>>& items);

        static const TimeSeriesReference& empty();

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
        [[nodiscard]] TimeSeriesKind kind() const override { return TimeSeriesKind::Reference | TimeSeriesKind::Output; }

        const TimeSeriesReference &value() const;  // Throws if no value

        TimeSeriesReference &value();  // Throws if no value

        // Python-safe value access that returns the reference value or makes an empty one
        TimeSeriesReference py_value_or_empty() const;

        void py_set_value(const nb::object& value) override;

        void set_value(TimeSeriesReference value);

        void apply_result(const nb::object& value) override;

        bool can_apply_result(const nb::object& value) override;

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

    struct TimeSeriesReferenceInput : BaseTimeSeriesInput
    {
        using ptr = TimeSeriesReferenceInput*;
        using s_ptr = std::shared_ptr<TimeSeriesReferenceInput>;

        using BaseTimeSeriesInput::BaseTimeSeriesInput;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override {
            // Single comparison checks both type (Reference) and direction (Input)
            return other->kind() == kind();
        }
        [[nodiscard]] TimeSeriesKind kind() const override { return TimeSeriesKind::Reference | TimeSeriesKind::Input; }

        void start();

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] virtual const TimeSeriesReference& value() const;

        // Duplicate binding of another input
        virtual void clone_binding(const TimeSeriesReferenceInput::ptr other);

        [[nodiscard]] bool bound() const override;

        [[nodiscard]] bool modified() const override;

        [[nodiscard]] bool valid() const override;

        [[nodiscard]] bool all_valid() const override;

        [[nodiscard]] engine_time_t last_modified_time() const override;

        bool bind_output(time_series_output_s_ptr value) override;

        void un_bind_output(bool unbind_refs) override;

        void make_active() override;

        void make_passive() override;

        [[nodiscard]] TimeSeriesInput::s_ptr get_input(size_t index) override;

        [[nodiscard]] virtual TimeSeriesReferenceInput *get_ref_input(size_t index);

        VISITOR_SUPPORT()

        [[nodiscard]] bool is_reference() const override;

        [[nodiscard]] bool has_reference() const override;

        virtual time_series_input_s_ptr clone_blank_ref_instance() = 0;

        virtual std::vector<TimeSeriesReferenceInput::s_ptr> &items() { return empty_items; }

        virtual const std::vector<TimeSeriesReferenceInput::s_ptr> &items() const { return empty_items; };

      protected:
        friend struct PyTimeSeriesReferenceInput;
        bool do_bind_output(time_series_output_s_ptr output_) override;

        void do_un_bind_output(bool unbind_refs) override;

        TimeSeriesReferenceOutput *output_t() const;

        TimeSeriesReferenceOutput *output_t();

        void notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override;

        [[nodiscard]] bool has_value() const;

        void reset_value();

        std::optional<TimeSeriesReference> &raw_value();

        mutable std::optional<TimeSeriesReference> _value;

        static inline std::vector<TimeSeriesReferenceInput::s_ptr> empty_items{};

      private:
        friend struct TimeSeriesReferenceOutput;
        friend struct TimeSeriesReference;
    };

    // ============================================================
    // Specialized Reference Input Classes
    // ============================================================

    struct TimeSeriesValueReferenceInput : TimeSeriesReferenceInput
    {
        using TimeSeriesReferenceInput::TimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);

        VISITOR_SUPPORT();

        time_series_input_s_ptr clone_blank_ref_instance() override;
    };

    struct TimeSeriesListReferenceInput final : TimeSeriesReferenceInput
    {
        using TimeSeriesReferenceInput::TimeSeriesReferenceInput;

        // Constructor that accepts size
        TimeSeriesListReferenceInput(Node *owning_node, InputBuilder::ptr value_builder, size_t size);
        TimeSeriesListReferenceInput(TimeSeriesInput *parent_input, InputBuilder::ptr value_builder, size_t size);

        TimeSeriesInput::s_ptr            get_input(size_t index) override;
        size_t                            size() const { return _size; }
        [[nodiscard]] const TimeSeriesReference& value() const override;

        [[nodiscard]] bool          bound() const override;
        [[nodiscard]] bool          modified() const override;
        [[nodiscard]] bool          valid() const override;
        [[nodiscard]] bool          all_valid() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;
        void                        clone_binding(const TimeSeriesReferenceInput::ptr other) override;
        void                        un_bind_output(bool unbind_refs) override;
        
        std::vector<TimeSeriesReferenceInput::s_ptr>       &items() override;
        const std::vector<TimeSeriesReferenceInput::s_ptr> &items() const override;

        void make_active() override;
        void make_passive() override;

        VISITOR_SUPPORT()

        time_series_input_s_ptr clone_blank_ref_instance() override;

        [[nodiscard]] TimeSeriesReferenceInput *get_ref_input(size_t index) override;

      private:
        InputBuilder::ptr                                           _value_builder;
        size_t                                                      _size{0};
        std::optional<std::vector<TimeSeriesReferenceInput::s_ptr>> _items;
    };

    struct TimeSeriesBundleReferenceInput final : TimeSeriesReferenceInput
    {
        using TimeSeriesReferenceInput::TimeSeriesReferenceInput;

        // Constructor that accepts size
        TimeSeriesBundleReferenceInput(Node *owning_node, std::vector<InputBuilder::ptr> value_builders, size_t size);
        TimeSeriesBundleReferenceInput(TimeSeriesInput *parent_input, std::vector<InputBuilder::ptr> value_builders, size_t size);

        const TimeSeriesReference&  value() const override;
        size_t                      size() const { return _size; }
        [[nodiscard]] bool          bound() const override;
        [[nodiscard]] bool          modified() const override;
        [[nodiscard]] bool          valid() const override;
        [[nodiscard]] bool          all_valid() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;
        void                        clone_binding(const TimeSeriesReferenceInput::ptr other) override;
        void                        un_bind_output(bool unbind_refs) override;

        std::vector<TimeSeriesReferenceInput::s_ptr>       &items() override;
        const std::vector<TimeSeriesReferenceInput::s_ptr> &items() const override;

        void make_active() override;
        void make_passive() override;

        VISITOR_SUPPORT()

        time_series_input_s_ptr clone_blank_ref_instance() override;

        [[nodiscard]] TimeSeriesReferenceInput *get_ref_input(size_t index) override;

      private:
        std::vector<InputBuilder::ptr>                              _value_builders;
        size_t                                                      _size{0};
        std::optional<std::vector<TimeSeriesReferenceInput::s_ptr>> _items;
    };

    struct TimeSeriesDictReferenceInput final : TimeSeriesReferenceInput
    {
        using TimeSeriesReferenceInput::TimeSeriesReferenceInput;

        VISITOR_SUPPORT()

        time_series_input_s_ptr clone_blank_ref_instance() override;
    };

    struct TimeSeriesSetReferenceInput final : TimeSeriesReferenceInput
    {
        using TimeSeriesReferenceInput::TimeSeriesReferenceInput;

        VISITOR_SUPPORT()

        time_series_input_s_ptr clone_blank_ref_instance() override;
    };

    struct TimeSeriesWindowReferenceInput final : TimeSeriesReferenceInput
    {
        using TimeSeriesReferenceInput::TimeSeriesReferenceInput;

        VISITOR_SUPPORT()

        time_series_input_s_ptr clone_blank_ref_instance() override;
    };

    // ============================================================
    // Specialized Reference Output Classes
    // ============================================================

    struct TimeSeriesValueReferenceOutput final : TimeSeriesReferenceOutput
    {
        using TimeSeriesReferenceOutput::TimeSeriesReferenceOutput;

        VISITOR_SUPPORT();
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
        TimeSeriesBundleReferenceOutput(TimeSeriesOutput *parent_output, std::vector<OutputBuilder::ptr> value_builder, size_t size);

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

#endif  // REF_H