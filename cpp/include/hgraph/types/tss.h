//
// Created by Howard Henson on 04/05/2025.
//

#ifndef TSS_H
#define TSS_H

#include <hgraph/python/hashable.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/feature_extension.h>
#include <hgraph/types/base_time_series.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/v2/tss_value.h>
#include <hgraph/types/v2_adaptor.h>

namespace hgraph {

    // ============================================================================
    // SetDelta_T - Templated set delta (kept for Python bindings compatibility)
    // ============================================================================

    template<typename T>
    struct SetDelta_T {
        using scalar_type = T;
        using collection_type = std::unordered_set<T>;

        template<typename U = T>
            requires(!std::is_same_v<U, nb::object>)
        SetDelta_T(collection_type added, collection_type removed);

        template<typename U = T>
            requires(std::is_same_v<U, nb::object>)
        SetDelta_T(collection_type added, collection_type removed, nb::object tp);

        [[nodiscard]] const collection_type &added() const;

        [[nodiscard]] const collection_type &removed() const;

        [[nodiscard]] bool operator==(const SetDelta_T<T> &other) const;

        [[nodiscard]] size_t hash() const;

        [[nodiscard]] SetDelta_T<T> operator+(const SetDelta_T<T> &other) const;

        [[nodiscard]] nb::object py_type() const;

    private:
        collection_type _added;
        collection_type _removed;
        std::conditional_t<std::is_same_v<T, nb::object>, nb::object, std::monostate> _tp;
    };

    template<typename T>
    SetDelta_T<T> make_set_delta(std::unordered_set<T> added, std::unordered_set<T> removed) {
        return SetDelta_T<T>(std::move(added), std::move(removed));
    }

    template<>
    inline SetDelta_T<nb::object> make_set_delta(std::unordered_set<nb::object> added,
                                                      std::unordered_set<nb::object> removed) {
        nb::object tp;
        if (!added.empty()) {
            tp = nb::borrow(*added.begin()->type());
        } else if (!removed.empty()) {
            tp = nb::borrow(*removed.begin()->type());
        } else {
            tp = get_object();
        }
        return SetDelta_T<nb::object>(added, removed, tp);
    }

    // ============================================================================
    // TimeSeriesSetOutput - Non-templated using v2 TSSOutput
    // ============================================================================

    /**
     * Non-templated TimeSeriesSetOutput using v2 TSSOutput for value storage.
     * This replaces the templated TimeSeriesSetOutput_T<T> to reduce template instantiations.
     *
     * Inherits from TimeSeriesOutput and implements NotifiableContext for the TSSOutput.
     * Delegates value storage and modification tracking to TSSOutput.
     */
    struct HGRAPH_EXPORT TimeSeriesSetOutput final : TimeSeriesOutput, NotifiableContext {
        using ptr = TimeSeriesSetOutput*;
        using s_ptr = std::shared_ptr<TimeSeriesSetOutput>;

        // Constructor takes type_info for element type checking
        explicit TimeSeriesSetOutput(node_ptr parent, const std::type_info &element_tp);
        explicit TimeSeriesSetOutput(time_series_output_ptr parent, const std::type_info &element_tp);

        // NotifiableContext implementation
        void notify(engine_time_t et) override;
        [[nodiscard]] engine_time_t current_engine_time() const override;
        void add_before_evaluation_notification(std::function<void()> &&fn) override;
        void add_after_evaluation_notification(std::function<void()> &&fn) override;

        // Python interface
        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;
        void py_set_value(const nb::object& value) override;
        bool can_apply_result(const nb::object &value) override;
        void apply_result(const nb::object& value) override;

        // Python-specific methods that dispatch based on element_type
        [[nodiscard]] nb::object py_added() const;
        [[nodiscard]] nb::object py_removed() const;
        [[nodiscard]] bool py_contains(const nb::object& item) const;
        [[nodiscard]] bool py_was_added(const nb::object& item) const;
        [[nodiscard]] bool py_was_removed(const nb::object& item) const;
        void py_add(const nb::object& item);
        void py_remove(const nb::object& item);

        // Type-safe value access via templates
        template <typename T> std::unordered_set<T> value() const {
            std::unordered_set<T> result;
            for (const auto& av : _tss_output.values()) {
                const T* pv = av.template get_if<T>();
                if (pv) result.insert(*pv);
            }
            return result;
        }

        template <typename T> std::unordered_set<T> added() const {
            std::unordered_set<T> result;
            for (const auto& av : _tss_output.added()) {
                const T* pv = av.template get_if<T>();
                if (pv) result.insert(*pv);
            }
            return result;
        }

        template <typename T> std::unordered_set<T> removed() const {
            std::unordered_set<T> result;
            for (const auto& av : _tss_output.removed()) {
                const T* pv = av.template get_if<T>();
                if (pv) result.insert(*pv);
            }
            return result;
        }

        template <typename T> void add(const T& item) {
            AnyValue<> av;
            av.template emplace<T>(item);
            _tss_output.add(av);
            _post_modify();
        }

        template <typename T> void remove(const T& item) {
            AnyValue<> av;
            av.template emplace<T>(item);
            _tss_output.remove(av);
            _post_modify();
        }

        template <typename T> bool contains(const T& item) const {
            AnyValue<> av;
            av.template emplace<T>(item);
            return _tss_output.contains(av);
        }

        template <typename T> bool was_added(const T& item) const {
            AnyValue<> av;
            av.template emplace<T>(item);
            return _tss_output.was_added(av);
        }

        template <typename T> bool was_removed(const T& item) const {
            AnyValue<> av;
            av.template emplace<T>(item);
            return _tss_output.was_removed(av);
        }

        template <typename T> void set_value(std::unordered_set<T> added_items, std::unordered_set<T> removed_items) {
            std::vector<AnyValue<>> added_av, removed_av;
            for (const auto& item : added_items) {
                AnyValue<> av;
                av.template emplace<T>(item);
                added_av.push_back(std::move(av));
            }
            for (const auto& item : removed_items) {
                AnyValue<> av;
                av.template emplace<T>(item);
                removed_av.push_back(std::move(av));
            }
            _tss_output.set_delta(added_av, removed_av);
            _post_modify();
        }

        template <typename T> void set_value(const SetDelta_T<T>& delta) {
            set_value<T>(delta.added(), delta.removed());
        }

        // Non-template operations
        [[nodiscard]] size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] bool has_added() const;
        [[nodiscard]] bool has_removed() const;

        // Reference outputs
        [[nodiscard]] TimeSeriesValueOutput::s_ptr get_contains_output(const nb::object &item, const nb::object &requester);
        void release_contains_output(const nb::object &item, const nb::object &requester);
        [[nodiscard]] TimeSeriesValueOutput::s_ptr& is_empty_output();

        // TimeSeriesOutput interface
        void mark_invalid() override;
        void invalidate() override;
        void clear() override;
        void copy_from_output(const TimeSeriesOutput &output) override;
        void copy_from_input(const TimeSeriesInput &input) override;
        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        // Access to type info
        [[nodiscard]] const std::type_info& element_type() const { return _tss_output.element_type(); }

        // Access to underlying TSSOutput for direct binding
        [[nodiscard]] TSSOutput& tss_output() { return _tss_output; }
        [[nodiscard]] const TSSOutput& tss_output() const { return _tss_output; }

        // Parent/node tracking
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
        void                                  mark_modified() override;
        void                                  mark_modified(engine_time_t modified_time) override;
        void                                  mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) override;

        VISITOR_SUPPORT()

    protected:
        friend struct TimeSeriesSetOutputBuilder;  // Allow builder to call _reset
        void _post_modify();
        void _reset();

    private:
        ParentAdapter<TimeSeriesOutput> _parent_adapter;
        TSSOutput                       _tss_output;
        TimeSeriesValueOutput::s_ptr    _is_empty_ref_output;

        // Contains output management - using nb::object as key for Python compatibility
        struct ContainsRefEntry {
            TimeSeriesValueOutput::s_ptr output;
            std::unordered_set<void*> requesters;
        };
        std::unordered_map<size_t, ContainsRefEntry> _contains_ref_outputs;  // hash -> entry

        // Python cache
        mutable nb::frozenset _py_value{};
        mutable nb::frozenset _py_added{};
        mutable nb::frozenset _py_removed{};
    };

    // ============================================================================
    // TimeSeriesSetInput - Non-templated using v2 TSSInput
    // ============================================================================

    /**
     * Non-templated TimeSeriesSetInput using v2 TSSInput for binding/value access.
     *
     * Inherits from TimeSeriesInput and implements NotifiableContext for the TSSInput.
     * Delegates binding and value access to TSSInput.
     */
    struct HGRAPH_EXPORT TimeSeriesSetInput final : TimeSeriesInput, NotifiableContext {
        using ptr = TimeSeriesSetInput*;
        using s_ptr = std::shared_ptr<TimeSeriesSetInput>;

        // Constructor takes type_info for element type checking
        explicit TimeSeriesSetInput(node_ptr parent, const std::type_info &element_tp);
        explicit TimeSeriesSetInput(time_series_input_ptr parent, const std::type_info &element_tp);

        // NotifiableContext implementation
        void notify(engine_time_t et) override;
        [[nodiscard]] engine_time_t current_engine_time() const override;
        void add_before_evaluation_notification(std::function<void()> &&fn) override;
        void add_after_evaluation_notification(std::function<void()> &&fn) override;

        // Get bound output as TimeSeriesSetOutput
        [[nodiscard]] TimeSeriesSetOutput& set_output();
        [[nodiscard]] const TimeSeriesSetOutput& set_output() const;

        // Type-safe value access via templates
        template <typename T> std::unordered_set<T> value() const {
            std::unordered_set<T> result;
            for (const auto& av : _tss_input.values()) {
                const T* pv = av.template get_if<T>();
                if (pv) result.insert(*pv);
            }
            return result;
        }

        template <typename T> std::unordered_set<T> added() const {
            std::unordered_set<T> result;
            for (const auto& av : _tss_input.added()) {
                const T* pv = av.template get_if<T>();
                if (pv) result.insert(*pv);
            }
            return result;
        }

        template <typename T> std::unordered_set<T> removed() const {
            std::unordered_set<T> result;
            for (const auto& av : _tss_input.removed()) {
                const T* pv = av.template get_if<T>();
                if (pv) result.insert(*pv);
            }
            return result;
        }

        template <typename T> SetDelta_T<T> delta_value() const {
            return make_set_delta<T>(added<T>(), removed<T>());
        }

        template <typename T> bool contains(const T& item) const {
            AnyValue<> av;
            av.template emplace<T>(item);
            return _tss_input.contains(av);
        }

        template <typename T> bool was_added(const T& item) const {
            AnyValue<> av;
            av.template emplace<T>(item);
            return _tss_input.was_added(av);
        }

        template <typename T> bool was_removed(const T& item) const {
            AnyValue<> av;
            av.template emplace<T>(item);
            return _tss_input.was_removed(av);
        }

        // Python interface
        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;

        // Python-specific methods that dispatch based on element_type
        [[nodiscard]] nb::object py_added() const;
        [[nodiscard]] nb::object py_removed() const;
        [[nodiscard]] bool py_contains(const nb::object& item) const;
        [[nodiscard]] bool py_was_added(const nb::object& item) const;
        [[nodiscard]] bool py_was_removed(const nb::object& item) const;

        // Non-template operations
        [[nodiscard]] size_t size() const;
        [[nodiscard]] bool empty() const;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        // Binding operations
        bool bind_output(const time_series_output_s_ptr & output_) override;
        void un_bind_output(bool unbind_refs) override;

        // Access to type info
        [[nodiscard]] const std::type_info& element_type() const { return _tss_input.element_type(); }

        // Access to underlying TSSInput for direct operations
        [[nodiscard]] TSSInput& tss_input() { return _tss_input; }
        [[nodiscard]] const TSSInput& tss_input() const { return _tss_input; }

        // TimeSeriesInput interface - delegate to _parent_adapter and _tss_input
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
        friend struct TimeSeriesSetOutput;  // For copy_from_input
        ParentAdapter<TimeSeriesInput> _parent_adapter;
        TSSInput                       _tss_input;
        time_series_output_s_ptr       _bound_output;  // Track bound output
    };

    void register_set_delta_with_nanobind(nb::module_ & m);

} // namespace hgraph

#endif  // TSS_H
