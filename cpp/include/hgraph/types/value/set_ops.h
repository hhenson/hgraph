//
// Created by Claude on 19/12/2025.
//
// Set operations as free functions operating on Value objects
//

#ifndef HGRAPH_VALUE_SET_OPS_H
#define HGRAPH_VALUE_SET_OPS_H

#include <hgraph/types/value/value.h>
#include <hgraph/types/value/set_type.h>
#include <stdexcept>

namespace hgraph::value {

    // =========================================================================
    // Type checking helpers (internal)
    // =========================================================================

    namespace detail {
        inline void check_set_type(const Value& v, const char* op_name) {
            if (!v.valid()) {
                throw std::runtime_error(std::string(op_name) + ": invalid value");
            }
            if (v.schema()->kind != TypeKind::Set) {
                throw std::runtime_error(std::string(op_name) + ": requires set type");
            }
        }

        inline void check_matching_set_types(const Value& a, const Value& b, const char* op_name) {
            check_set_type(a, op_name);
            check_set_type(b, op_name);
            if (a.schema() != b.schema()) {
                throw std::runtime_error(std::string(op_name) + ": requires matching set types");
            }
        }
    }

    // =========================================================================
    // Set algebra operations - return new Value
    // =========================================================================

    /**
     * Union of two sets: a | b
     * Returns a new set containing all elements from both sets.
     */
    inline Value set_union(const Value& a, const Value& b) {
        detail::check_matching_set_types(a, b, "set_union");

        const auto& storage_a = *static_cast<const SetStorage*>(a.data());
        const auto& storage_b = *static_cast<const SetStorage*>(b.data());

        Value result(a.schema());
        auto& result_storage = *static_cast<SetStorage*>(result.data());
        result_storage = storage_a.union_with(storage_b);

        return result;
    }

    /**
     * Intersection of two sets: a & b
     * Returns a new set containing elements present in both sets.
     */
    inline Value set_intersection(const Value& a, const Value& b) {
        detail::check_matching_set_types(a, b, "set_intersection");

        const auto& storage_a = *static_cast<const SetStorage*>(a.data());
        const auto& storage_b = *static_cast<const SetStorage*>(b.data());

        Value result(a.schema());
        auto& result_storage = *static_cast<SetStorage*>(result.data());
        result_storage = storage_a.intersection_with(storage_b);

        return result;
    }

    /**
     * Difference of two sets: a - b
     * Returns a new set containing elements in a but not in b.
     */
    inline Value set_difference(const Value& a, const Value& b) {
        detail::check_matching_set_types(a, b, "set_difference");

        const auto& storage_a = *static_cast<const SetStorage*>(a.data());
        const auto& storage_b = *static_cast<const SetStorage*>(b.data());

        Value result(a.schema());
        auto& result_storage = *static_cast<SetStorage*>(result.data());
        result_storage = storage_a.difference_with(storage_b);

        return result;
    }

    /**
     * Symmetric difference of two sets: a ^ b
     * Returns a new set containing elements in either set but not both.
     */
    inline Value set_symmetric_difference(const Value& a, const Value& b) {
        detail::check_matching_set_types(a, b, "set_symmetric_difference");

        const auto& storage_a = *static_cast<const SetStorage*>(a.data());
        const auto& storage_b = *static_cast<const SetStorage*>(b.data());

        Value result(a.schema());
        auto& result_storage = *static_cast<SetStorage*>(result.data());
        result_storage = storage_a.symmetric_difference_with(storage_b);

        return result;
    }

    // =========================================================================
    // Set predicates
    // =========================================================================

    /**
     * Check if a is a subset of b: a <= b
     * Returns true if all elements of a are in b.
     */
    inline bool is_subset(const Value& a, const Value& b) {
        detail::check_matching_set_types(a, b, "is_subset");

        const auto& storage_a = *static_cast<const SetStorage*>(a.data());
        const auto& storage_b = *static_cast<const SetStorage*>(b.data());

        return storage_a.is_subset_of(storage_b);
    }

    /**
     * Check if a is a proper subset of b: a < b
     * Returns true if a is a subset of b and a != b.
     */
    inline bool is_proper_subset(const Value& a, const Value& b) {
        detail::check_matching_set_types(a, b, "is_proper_subset");

        const auto& storage_a = *static_cast<const SetStorage*>(a.data());
        const auto& storage_b = *static_cast<const SetStorage*>(b.data());

        return storage_a.is_proper_subset_of(storage_b);
    }

    /**
     * Check if a is a superset of b: a >= b
     * Returns true if all elements of b are in a.
     */
    inline bool is_superset(const Value& a, const Value& b) {
        detail::check_matching_set_types(a, b, "is_superset");

        const auto& storage_a = *static_cast<const SetStorage*>(a.data());
        const auto& storage_b = *static_cast<const SetStorage*>(b.data());

        return storage_a.is_superset_of(storage_b);
    }

    /**
     * Check if a is a proper superset of b: a > b
     * Returns true if a is a superset of b and a != b.
     */
    inline bool is_proper_superset(const Value& a, const Value& b) {
        detail::check_matching_set_types(a, b, "is_proper_superset");

        const auto& storage_a = *static_cast<const SetStorage*>(a.data());
        const auto& storage_b = *static_cast<const SetStorage*>(b.data());

        return storage_a.is_proper_superset_of(storage_b);
    }

    /**
     * Check if two sets are disjoint (have no common elements)
     */
    inline bool is_disjoint(const Value& a, const Value& b) {
        detail::check_matching_set_types(a, b, "is_disjoint");

        const auto& storage_a = *static_cast<const SetStorage*>(a.data());
        const auto& storage_b = *static_cast<const SetStorage*>(b.data());

        return storage_a.is_disjoint_with(storage_b);
    }

    // =========================================================================
    // In-place mutations
    // =========================================================================

    /**
     * In-place union: dest |= other
     * Adds all elements from other to dest.
     */
    inline void set_update(Value& dest, const Value& other) {
        detail::check_matching_set_types(dest, other, "set_update");

        auto& dest_storage = *static_cast<SetStorage*>(dest.data());
        const auto& other_storage = *static_cast<const SetStorage*>(other.data());

        dest_storage.update(other_storage);
    }

    /**
     * In-place intersection: dest &= other
     * Removes elements from dest that are not in other.
     */
    inline void set_intersection_update(Value& dest, const Value& other) {
        detail::check_matching_set_types(dest, other, "set_intersection_update");

        auto& dest_storage = *static_cast<SetStorage*>(dest.data());
        const auto& other_storage = *static_cast<const SetStorage*>(other.data());

        dest_storage.intersection_update(other_storage);
    }

    /**
     * In-place difference: dest -= other
     * Removes elements from dest that are in other.
     */
    inline void set_difference_update(Value& dest, const Value& other) {
        detail::check_matching_set_types(dest, other, "set_difference_update");

        auto& dest_storage = *static_cast<SetStorage*>(dest.data());
        const auto& other_storage = *static_cast<const SetStorage*>(other.data());

        dest_storage.difference_update(other_storage);
    }

    /**
     * In-place symmetric difference: dest ^= other
     */
    inline void set_symmetric_difference_update(Value& dest, const Value& other) {
        detail::check_matching_set_types(dest, other, "set_symmetric_difference_update");

        auto& dest_storage = *static_cast<SetStorage*>(dest.data());
        const auto& other_storage = *static_cast<const SetStorage*>(other.data());

        dest_storage.symmetric_difference_update(other_storage);
    }

    /**
     * Discard element from set (silent if not present)
     * Returns true if element was removed, false if not present.
     */
    inline bool set_discard(Value& dest, const Value& elem) {
        detail::check_set_type(dest, "set_discard");
        if (!elem.valid()) {
            throw std::runtime_error("set_discard: invalid element");
        }

        auto* set_meta = static_cast<const SetTypeMeta*>(dest.schema());
        if (elem.schema() != set_meta->element_type) {
            throw std::runtime_error("set_discard: element type mismatch");
        }

        auto& dest_storage = *static_cast<SetStorage*>(dest.data());
        return dest_storage.discard(elem.data());
    }

    /**
     * Add element to set
     * Returns true if element was added (not already present).
     */
    inline bool set_add(Value& dest, const Value& elem) {
        detail::check_set_type(dest, "set_add");
        if (!elem.valid()) {
            throw std::runtime_error("set_add: invalid element");
        }

        auto* set_meta = static_cast<const SetTypeMeta*>(dest.schema());
        if (elem.schema() != set_meta->element_type) {
            throw std::runtime_error("set_add: element type mismatch");
        }

        auto& dest_storage = *static_cast<SetStorage*>(dest.data());
        return dest_storage.add(elem.data());
    }

} // namespace hgraph::value

#endif // HGRAPH_VALUE_SET_OPS_H
