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
#include <hgraph/types/value/tracked_set_storage.h>
#include <hgraph/types/value/set_delta_value.h>

namespace hgraph {

    template<typename T_TS>
        requires TimeSeriesT<T_TS>
    struct TimeSeriesSet : T_TS {
        // Map concrete Base types back to interface types for collections
        using ts_type = std::conditional_t<
            std::is_base_of_v<TimeSeriesInput, T_TS>,
            TimeSeriesInput,
            TimeSeriesOutput
        >;
        using T_TS::T_TS;

        [[nodiscard]] virtual size_t size() const = 0;

        [[nodiscard]] virtual bool empty() const = 0;
    };

    /**
     * @brief Non-templated TimeSeriesSetOutput using Value-based storage.
     *
     * This class stores set elements using the Value system with TrackedSetStorage.
     * Element type is determined at runtime via TypeMeta*.
     *
     * API:
     * - value_view(), added_view(), removed_view() - Value-based access
     * - contains(ConstValueView), add(ConstValueView), remove(ConstValueView) - Value operations
     * - py_contains(), py_add(), py_remove() - Python interop
     */
    struct TimeSeriesSetOutput final : TimeSeriesSet<BaseTimeSeriesOutput> {
        using ptr = TimeSeriesSetOutput*;
        using s_ptr = std::shared_ptr<TimeSeriesSetOutput>;

        // Constructors with element type
        explicit TimeSeriesSetOutput(const node_ptr &parent, const value::TypeMeta* element_type);
        explicit TimeSeriesSetOutput(time_series_output_ptr parent, const value::TypeMeta* element_type);

        [[nodiscard]] time_series_value_output_s_ptr get_contains_output(const nb::object &item,
            const nb::object &requester);

        void release_contains_output(const nb::object &item, const nb::object &requester);

        [[nodiscard]] time_series_value_output_s_ptr &is_empty_output();

        void invalidate() override;

        // ========== Value-based API ==========

        /**
         * @brief Get the element type schema.
         */
        [[nodiscard]] const value::TypeMeta* element_type() const { return _element_type; }

        /**
         * @brief Get const view of current set value.
         */
        [[nodiscard]] value::ConstSetView value_view() const { return _storage.value(); }

        /**
         * @brief Get const view of added elements.
         */
        [[nodiscard]] value::ConstSetView added_view() const { return _storage.added(); }

        /**
         * @brief Get const view of removed elements.
         */
        [[nodiscard]] value::ConstSetView removed_view() const { return _storage.removed(); }

        /**
         * @brief Check if element is in set using Value.
         */
        [[nodiscard]] bool contains(const value::ConstValueView& elem) const { return _storage.contains(elem); }

        /**
         * @brief Check if element was added this cycle.
         */
        [[nodiscard]] bool was_added(const value::ConstValueView& elem) const { return _storage.was_added(elem); }

        /**
         * @brief Check if element was removed this cycle.
         */
        [[nodiscard]] bool was_removed(const value::ConstValueView& elem) const { return _storage.was_removed(elem); }

        /**
         * @brief Add element using Value.
         */
        void add(const value::ConstValueView& elem);

        /**
         * @brief Remove element using Value.
         */
        void remove(const value::ConstValueView& elem);

        // ========== Python Interop API ==========

        /**
         * @brief Check if Python object is in set.
         */
        [[nodiscard]] bool py_contains(const nb::object& item) const;

        /**
         * @brief Check if Python object was added this cycle.
         */
        [[nodiscard]] bool py_was_added(const nb::object& item) const;

        /**
         * @brief Check if Python object was removed this cycle.
         */
        [[nodiscard]] bool py_was_removed(const nb::object& item) const;

        /**
         * @brief Add Python object to set.
         */
        void py_add(const nb::object& item);

        /**
         * @brief Remove Python object from set.
         */
        void py_remove(const nb::object& item);

        /**
         * @brief Get added elements as Python frozenset.
         */
        [[nodiscard]] nb::object py_added() const;

        /**
         * @brief Get removed elements as Python frozenset.
         */
        [[nodiscard]] nb::object py_removed() const;

        // ========== TimeSeriesOutput Interface ==========

        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;
        void py_set_value(const nb::object& value) override;
        void apply_result(const nb::object& value) override;
        void clear() override;
        void copy_from_output(const TimeSeriesOutput &output) override;
        void copy_from_input(const TimeSeriesInput &input) override;
        // Bring base class mark_modified() into scope so it's not hidden by override
        using TimeSeriesSet<BaseTimeSeriesOutput>::mark_modified;
        void mark_modified(engine_time_t modified_time) override;

        [[nodiscard]] size_t size() const override { return _storage.size(); }
        [[nodiscard]] bool empty() const override { return _storage.empty(); }

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override {
            if (auto* other_tss = dynamic_cast<const TimeSeriesSetOutput*>(other)) {
                return _element_type == other_tss->_element_type;
            }
            return false;
        }

        void _reset_value();

        VISITOR_SUPPORT()

    private:
        value::TrackedSetStorage _storage;
        const value::TypeMeta* _element_type{nullptr};

        time_series_value_output_s_ptr _is_empty_ref_output;
        FeatureOutputExtensionValue _contains_ref_outputs;

        void _post_modify();
        void _update_contains_refs();
    };

    /**
     * @brief Non-templated TimeSeriesSetInput using Value-based storage.
     */
    struct TimeSeriesSetInput final : TimeSeriesSet<BaseTimeSeriesInput> {
        // Constructors with element type
        explicit TimeSeriesSetInput(const node_ptr &parent, const value::TypeMeta* element_type = nullptr);
        explicit TimeSeriesSetInput(time_series_input_ptr parent, const value::TypeMeta* element_type = nullptr);

        TimeSeriesSetOutput &set_output() const;

        bool do_bind_output(time_series_output_s_ptr output) override;

        void do_un_bind_output(bool unbind_refs) override;

        [[nodiscard]] const TimeSeriesSetOutput &prev_output() const;

        [[nodiscard]] bool has_prev_output() const;

        // ========== Value-based API ==========

        /**
         * @brief Get the element type schema (delegates to output).
         */
        [[nodiscard]] const value::TypeMeta* element_type() const;

        /**
         * @brief Get const view of current set value (delegates to output).
         */
        [[nodiscard]] value::ConstSetView value_view() const;

        /**
         * @brief Check if element is in set using Value (delegates to output).
         */
        [[nodiscard]] bool contains(const value::ConstValueView& elem) const;

        /**
         * @brief Check if element was added this cycle.
         */
        [[nodiscard]] bool was_added(const value::ConstValueView& elem) const;

        /**
         * @brief Check if element was removed this cycle.
         */
        [[nodiscard]] bool was_removed(const value::ConstValueView& elem) const;

        /**
         * @brief Collect added elements into a set of PlainValue keys.
         * Handles _prev_output case for when input is rebinding.
         * Returns elements that are in current set but weren't before.
         */
        [[nodiscard]] std::vector<value::PlainValue> collect_added() const;

        /**
         * @brief Collect removed elements into a set of PlainValue keys.
         * Handles _prev_output case for when input is unbound.
         * Returns elements that were in the set before but are no longer present.
         */
        [[nodiscard]] std::vector<value::PlainValue> collect_removed() const;

        // ========== Python Interop API ==========

        /**
         * @brief Check if Python object is in set.
         */
        [[nodiscard]] bool py_contains(const nb::object& item) const;

        /**
         * @brief Check if Python object was added this cycle.
         */
        [[nodiscard]] bool py_was_added(const nb::object& item) const;

        /**
         * @brief Check if Python object was removed this cycle.
         */
        [[nodiscard]] bool py_was_removed(const nb::object& item) const;

        /**
         * @brief Get added elements as Python frozenset.
         */
        [[nodiscard]] nb::object py_added() const;

        /**
         * @brief Get removed elements as Python frozenset.
         */
        [[nodiscard]] nb::object py_removed() const;

        // ========== TimeSeriesInput Interface ==========

        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] size_t size() const override;
        [[nodiscard]] bool empty() const override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override {
            if (auto* other_tss = dynamic_cast<const TimeSeriesSetInput*>(other)) {
                return element_type() == other_tss->element_type();
            }
            return false;
        }

        VISITOR_SUPPORT()

    protected:
        void reset_prev();

        void _add_reset_prev() const;

    private:
        TimeSeriesSetOutput::s_ptr _prev_output;
        mutable bool _pending_reset_prev{false};
        const value::TypeMeta* _element_type{nullptr};
    };

} // namespace hgraph

#endif  // TSS_H
