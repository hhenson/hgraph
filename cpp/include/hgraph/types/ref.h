//
// Created by Howard Henson on 03/01/2025.
//

#ifndef REF_H
#define REF_H

#include <hgraph/types/base_time_series.h>
#include <hgraph/types/time_series_visitor.h>

namespace hgraph {
    struct HGRAPH_EXPORT TimeSeriesReference {
        enum class Kind : uint8_t {
            EMPTY = 0,
            BOUND = 1,
            UNBOUND = 2
        };

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
        [[nodiscard]] const TimeSeriesOutput::ptr &output() const;
        [[nodiscard]] const std::vector<TimeSeriesReference> &items() const;
        [[nodiscard]] const TimeSeriesReference &operator[](size_t ndx) const;

        // Operations
        void bind_input(TimeSeriesInput &ts_input) const;
        bool operator==(const TimeSeriesReference &other) const;
        [[nodiscard]] std::string to_string() const;

        // Factory methods - use these to construct instances
        static TimeSeriesReference make();
        static TimeSeriesReference make(time_series_output_ptr output);
        static TimeSeriesReference make(std::vector<TimeSeriesReference> items);
        static TimeSeriesReference make(std::vector<nb::ref<TimeSeriesReferenceInput>> items);

        static void register_with_nanobind(nb::module_ &m);

    private:
        // Private constructors - must use make() factory methods
        TimeSeriesReference() noexcept;  // Empty
        explicit TimeSeriesReference(time_series_output_ptr output);  // Bound
        explicit TimeSeriesReference(std::vector<TimeSeriesReference> items);  // Unbound

        Kind _kind;

        // Union for the three variants - only one is active at a time
        union Storage {
            // Empty uses no storage
            char empty;
            // Bound stores a single output pointer
            TimeSeriesOutput::ptr bound;
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

    struct TimeSeriesReferenceOutput : BaseTimeSeriesOutput {
        using BaseTimeSeriesOutput::BaseTimeSeriesOutput;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        const TimeSeriesReference &value() const;  // Throws if no value

        TimeSeriesReference &value();  // Throws if no value

        // Python-safe value access that returns the reference value or makes an empty one
        TimeSeriesReference py_value_or_empty() const;

        void py_set_value(nb::object value) override;

        void set_value(TimeSeriesReference value);

        void apply_result(nb::object value) override;

        bool can_apply_result(nb::object value) override;

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

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<TimeSeriesReferenceOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<TimeSeriesReferenceOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        [[nodiscard]] bool has_value() const;

        static void register_with_nanobind(nb::module_ &m);

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

    struct TimeSeriesReferenceInput : BaseTimeSeriesInput {
        using ptr = nb::ref<TimeSeriesReferenceInput>;
        using BaseTimeSeriesInput::BaseTimeSeriesInput;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override {
            return dynamic_cast<const TimeSeriesReferenceInput *>(other) != nullptr;
        }

        void start();

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] TimeSeriesReference value() const;

        // Duplicate binding of another input
        void clone_binding(const TimeSeriesReferenceInput::ptr &other);

        [[nodiscard]] bool bound() const override;

        [[nodiscard]] bool modified() const override;

        [[nodiscard]] bool valid() const override;

        [[nodiscard]] bool all_valid() const override;

        [[nodiscard]] engine_time_t last_modified_time() const override;

        bool bind_output(time_series_output_ptr value) override;

        void un_bind_output(bool unbind_refs) override;

        void make_active() override;

        void make_passive() override;

        [[nodiscard]] TimeSeriesInput *get_input(size_t index) override;

        [[nodiscard]] TimeSeriesReferenceInput *get_ref_input(size_t index);

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesInputVisitor<TimeSeriesReferenceInput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesInputVisitor<TimeSeriesReferenceInput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        static void register_with_nanobind(nb::module_ &m);

        [[nodiscard]] bool is_reference() const override;

        [[nodiscard]] bool has_reference() const override;

    protected:
        bool do_bind_output(time_series_output_ptr &output_) override;

        void do_un_bind_output(bool unbind_refs) override;

        TimeSeriesReferenceOutput *output_t() const;

        TimeSeriesReferenceOutput *output_t();

        void notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override;

        std::vector<TimeSeriesReferenceInput::ptr> &items();

        const std::vector<TimeSeriesReferenceInput::ptr> &items() const;

        [[nodiscard]] bool has_value() const;

        void reset_value();

    private:
        friend struct TimeSeriesReferenceOutput;
        friend struct TimeSeriesReference;
        mutable std::optional<TimeSeriesReference> _value;
        std::optional<std::vector<TimeSeriesReferenceInput::ptr> > _items;
        static inline std::vector<TimeSeriesReferenceInput::ptr> empty_items{};
    };

    // ============================================================
    // Specialized Reference Input Classes
    // ============================================================

    struct TimeSeriesValueReferenceInput : TimeSeriesReferenceInput {
        using TimeSeriesReferenceInput::TimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }
    };

    struct TimeSeriesListReferenceInput : TimeSeriesReferenceInput {
        using TimeSeriesReferenceInput::TimeSeriesReferenceInput;

        // Constructor that accepts size
        TimeSeriesListReferenceInput(Node *owning_node, size_t size);
        TimeSeriesListReferenceInput(TimeSeriesType *parent_input, size_t size);

        TimeSeriesInput *get_input(size_t index) override;
        size_t size() const { return _size; }

        static void register_with_nanobind(nb::module_ &m);

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }

    private:
        size_t _size{0};
    };

    struct TimeSeriesBundleReferenceInput : TimeSeriesReferenceInput {
        using TimeSeriesReferenceInput::TimeSeriesReferenceInput;

        // Constructor that accepts size
        TimeSeriesBundleReferenceInput(Node *owning_node, size_t size);
        TimeSeriesBundleReferenceInput(TimeSeriesType *parent_input, size_t size);

        size_t size() const { return _size; }

        static void register_with_nanobind(nb::module_ &m);

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }

    private:
        size_t _size{0};
    };

    struct TimeSeriesDictReferenceInput : TimeSeriesReferenceInput {
        using TimeSeriesReferenceInput::TimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }
    };

    struct TimeSeriesSetReferenceInput : TimeSeriesReferenceInput {
        using TimeSeriesReferenceInput::TimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }
    };

    struct TimeSeriesWindowReferenceInput : TimeSeriesReferenceInput {
        using TimeSeriesReferenceInput::TimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }
    };

    // ============================================================
    // Specialized Reference Output Classes
    // ============================================================

    struct TimeSeriesValueReferenceOutput final : TimeSeriesReferenceOutput {
        using TimeSeriesReferenceOutput::TimeSeriesReferenceOutput;
        static void register_with_nanobind(nb::module_ &m);

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<TimeSeriesValueReferenceOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<TimeSeriesValueReferenceOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }
    };

    struct TimeSeriesListReferenceOutput final : TimeSeriesReferenceOutput {
        using TimeSeriesReferenceOutput::TimeSeriesReferenceOutput;

        // Constructor that accepts size
        TimeSeriesListReferenceOutput(Node *owning_node, size_t size);
        TimeSeriesListReferenceOutput(TimeSeriesType *parent_output, size_t size);

        size_t size() const { return _size; }

        static void register_with_nanobind(nb::module_ &m);

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<TimeSeriesListReferenceOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<TimeSeriesListReferenceOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }

    private:
        size_t _size{0};
    };

    struct TimeSeriesBundleReferenceOutput final : TimeSeriesReferenceOutput {
        using TimeSeriesReferenceOutput::TimeSeriesReferenceOutput;

        // Constructor that accepts size
        TimeSeriesBundleReferenceOutput(Node *owning_node, size_t size);
        TimeSeriesBundleReferenceOutput(TimeSeriesType *parent_output, size_t size);

        size_t size() const { return _size; }

        static void register_with_nanobind(nb::module_ &m);

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<TimeSeriesBundleReferenceOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<TimeSeriesBundleReferenceOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }

    private:
        size_t _size{0};
    };

    struct TimeSeriesDictReferenceOutput final : TimeSeriesReferenceOutput {
        using TimeSeriesReferenceOutput::TimeSeriesReferenceOutput;
        static void register_with_nanobind(nb::module_ &m);

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<TimeSeriesDictReferenceOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<TimeSeriesDictReferenceOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }
    };

    struct TimeSeriesSetReferenceOutput final : TimeSeriesReferenceOutput {
        using TimeSeriesReferenceOutput::TimeSeriesReferenceOutput;
        static void register_with_nanobind(nb::module_ &m);

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<TimeSeriesSetReferenceOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<TimeSeriesSetReferenceOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }
    };

    struct TimeSeriesWindowReferenceOutput final : TimeSeriesReferenceOutput {
        using TimeSeriesReferenceOutput::TimeSeriesReferenceOutput;
        static void register_with_nanobind(nb::module_ &m);

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<TimeSeriesWindowReferenceOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<TimeSeriesWindowReferenceOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }
    };

} // namespace hgraph

#endif  // REF_H