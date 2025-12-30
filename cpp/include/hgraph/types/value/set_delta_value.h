#pragma once

/**
 * @file set_delta_value.h
 * @brief Value class representing set delta (added/removed elements).
 *
 * SetDeltaValue is returned by TimeSeriesSetInput::delta_value() and
 * represents a snapshot of what was added/removed since last evaluation.
 */

#include <hgraph/types/value/value.h>
#include <hgraph/types/value/indexed_view.h>
#include <hgraph/types/value/type_registry.h>

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph::value {

/**
 * @brief Value class representing set delta changes.
 *
 * Contains snapshots of added and removed elements from a TrackedSetStorage.
 * This is an owning value class (copies the delta sets).
 */
struct SetDeltaValue {
    PlainValue _added;
    PlainValue _removed;
    const TypeMeta* _element_type{nullptr};
    const TypeMeta* _set_schema{nullptr};

    // ========== Construction ==========

    SetDeltaValue() = default;

    /**
     * @brief Construct empty delta with element type.
     */
    explicit SetDeltaValue(const TypeMeta* element_type)
        : _element_type(element_type) {
        if (_element_type) {
            _set_schema = TypeRegistry::instance().set(_element_type).build();
            _added = PlainValue(_set_schema);
            _removed = PlainValue(_set_schema);
        }
    }

    /**
     * @brief Construct from existing set views (copies data).
     */
    SetDeltaValue(ConstSetView added_view, ConstSetView removed_view,
                  const TypeMeta* element_type)
        : _element_type(element_type) {
        if (_element_type) {
            _set_schema = TypeRegistry::instance().set(_element_type).build();
            _added = PlainValue(_set_schema);
            _removed = PlainValue(_set_schema);

            // Copy elements from views
            auto add_set = _added.view().as_set();
            for (auto elem : added_view) {
                add_set.insert(elem);
            }
            auto rem_set = _removed.view().as_set();
            for (auto elem : removed_view) {
                rem_set.insert(elem);
            }
        }
    }

    // Move-only (PlainValue has unique ownership)
    SetDeltaValue(SetDeltaValue&&) noexcept = default;
    SetDeltaValue& operator=(SetDeltaValue&&) noexcept = default;
    SetDeltaValue(const SetDeltaValue&) = delete;
    SetDeltaValue& operator=(const SetDeltaValue&) = delete;

    // ========== View Accessors ==========

    /**
     * @brief Get const view of added elements.
     */
    [[nodiscard]] ConstSetView added() const {
        return _added.const_view().as_set();
    }

    /**
     * @brief Get const view of removed elements.
     */
    [[nodiscard]] ConstSetView removed() const {
        return _removed.const_view().as_set();
    }

    // ========== Size and State ==========

    /**
     * @brief Check if there are any changes.
     */
    [[nodiscard]] bool empty() const {
        return added().empty() && removed().empty();
    }

    /**
     * @brief Get number of added elements.
     */
    [[nodiscard]] size_t added_count() const {
        return added().size();
    }

    /**
     * @brief Get number of removed elements.
     */
    [[nodiscard]] size_t removed_count() const {
        return removed().size();
    }

    // ========== Python Interop ==========

    /**
     * @brief Convert to Python SetDelta object.
     *
     * Returns a Python object with 'added' and 'removed' frozenset attributes.
     */
    [[nodiscard]] nb::object to_python() const {
        if (!_element_type) {
            return nb::none();
        }

        // Convert to Python sets
        nb::set py_added;
        for (auto elem : added()) {
            py_added.add(elem.to_python());
        }

        nb::set py_removed;
        for (auto elem : removed()) {
            py_removed.add(elem.to_python());
        }

        // Create a SetDelta-like object
        // For now, return a dict with 'added' and 'removed' keys
        nb::dict result;
        result["added"] = nb::frozenset(py_added);
        result["removed"] = nb::frozenset(py_removed);
        return result;
    }

    // ========== Element Type ==========

    [[nodiscard]] const TypeMeta* element_type() const {
        return _element_type;
    }
};

} // namespace hgraph::value
