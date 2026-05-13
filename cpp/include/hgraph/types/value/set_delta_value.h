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
#include <hgraph/types/constants.h>

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph::value {

/**
 * @brief Value class representing set delta changes.
 *
 * Contains owning snapshots of added and removed set elements.
 *
 * This is used when code needs stable materialized delta sets rather than the
 * storage-backed `SetDeltaView` ranges exposed by the value layer.
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
            auto add_mut = add_set.begin_mutation(MIN_ST);
            for (auto elem : added_view.values()) {
                static_cast<void>(add_mut.add(elem));
            }
            auto rem_set = _removed.view().as_set();
            auto rem_mut = rem_set.begin_mutation(MIN_ST);
            for (auto elem : removed_view.values()) {
                static_cast<void>(rem_mut.add(elem));
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
        for (auto elem : add_view.values()) {
            py_added.add(elem.to_python());
        }

        nb::set py_removed;
        auto rem_view = removed();
        for (auto elem : rem_view.values()) {
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
