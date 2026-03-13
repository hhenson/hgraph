#pragma once

/**
 * @file set_delta_value.h
 * @brief Value class representing set delta (added/removed elements).
 *
 * SetDeltaValue is returned by TimeSeriesSetInput::delta_value() and
 * represents a snapshot of what was added/removed since last evaluation.
 */

#include <hgraph/types/value/value.h>
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
    Value _added;
    Value _removed;
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
            _added = Value(_set_schema);
            _removed = Value(_set_schema);
            _added.reset();
            _removed.reset();
        }
    }

    /**
     * @brief Construct from existing set views (copies data).
     */
    SetDeltaValue(SetView added_view, SetView removed_view,
                  const TypeMeta* element_type)
        : _element_type(element_type) {
        if (_element_type) {
            _set_schema = TypeRegistry::instance().set(_element_type).build();
            _added = Value(_set_schema);
            _removed = Value(_set_schema);
            _added.reset();
            _removed.reset();

            // Copy elements from views
            auto add_set = _added.view().as_set();
            auto add_mut = add_set.begin_mutation();
            for (size_t i = 0; i < added_view.size(); ++i) {
                static_cast<void>(add_mut.add(added_view.at(i)));
            }
            auto rem_set = _removed.view().as_set();
            auto rem_mut = rem_set.begin_mutation();
            for (size_t i = 0; i < removed_view.size(); ++i) {
                static_cast<void>(rem_mut.add(removed_view.at(i)));
            }
        }
    }

    // Move-only (Value has unique ownership)
    SetDeltaValue(SetDeltaValue&&) noexcept = default;
    SetDeltaValue& operator=(SetDeltaValue&&) noexcept = default;
    SetDeltaValue(const SetDeltaValue&) = delete;
    SetDeltaValue& operator=(const SetDeltaValue&) = delete;

    // ========== View Accessors ==========

    /**
     * @brief Get view of added elements.
     */
    [[nodiscard]] SetView added() const {
        return _added.view().as_set();
    }

    /**
     * @brief Get view of removed elements.
     */
    [[nodiscard]] SetView removed() const {
        return _removed.view().as_set();
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
        auto add_view = added();
        for (size_t i = 0; i < add_view.size(); ++i) {
            py_added.add(add_view.at(i).to_python());
        }

        nb::set py_removed;
        auto rem_view = removed();
        for (size_t i = 0; i < rem_view.size(); ++i) {
            py_removed.add(rem_view.at(i).to_python());
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
