//
// Created by Claude on 19/12/2025.
//
// List operations as free functions operating on Value objects
//

#ifndef HGRAPH_VALUE_LIST_OPS_H
#define HGRAPH_VALUE_LIST_OPS_H

#include <hgraph/types/value/value.h>
#include <hgraph/types/value/dynamic_list_type.h>
#include <stdexcept>
#include <optional>

namespace hgraph::value {

    // =========================================================================
    // Type checking helpers (internal)
    // =========================================================================

    namespace detail {
        inline void check_dynamic_list_type(const Value& v, const char* op_name) {
            if (!v.valid()) {
                throw std::runtime_error(std::string(op_name) + ": invalid value");
            }
            // DynamicList uses TypeKind::List with DynamicListStorage
            if (v.schema()->kind != TypeKind::List) {
                throw std::runtime_error(std::string(op_name) + ": requires list type");
            }
        }

        inline void check_matching_list_types(const Value& a, const Value& b, const char* op_name) {
            check_dynamic_list_type(a, op_name);
            check_dynamic_list_type(b, op_name);
            if (a.schema() != b.schema()) {
                throw std::runtime_error(std::string(op_name) + ": requires matching list types");
            }
        }

        inline bool is_dynamic_list(const Value& v) {
            if (!v.valid() || v.schema()->kind != TypeKind::List) return false;
            // DynamicListTypeMeta has element_type field directly
            // vs ListTypeMeta which has count field
            // Check by storage size
            return v.schema()->size == sizeof(DynamicListStorage);
        }
    }

    // =========================================================================
    // List concatenation operations
    // =========================================================================

    /**
     * Concatenate two lists: a + b
     * Returns a new list with all elements from a followed by all elements from b.
     */
    inline Value list_concat(const Value& a, const Value& b) {
        detail::check_matching_list_types(a, b, "list_concat");

        if (!detail::is_dynamic_list(a)) {
            throw std::runtime_error("list_concat: requires dynamic list type");
        }

        const auto& storage_a = *static_cast<const DynamicListStorage*>(a.data());
        const auto& storage_b = *static_cast<const DynamicListStorage*>(b.data());

        Value result(a.schema());
        auto& result_storage = *static_cast<DynamicListStorage*>(result.data());
        result_storage = storage_a.concat_with(storage_b);

        return result;
    }

    /**
     * In-place extend: dest += other
     * Appends all elements from other to dest.
     */
    inline void list_extend(Value& dest, const Value& other) {
        detail::check_matching_list_types(dest, other, "list_extend");

        if (!detail::is_dynamic_list(dest)) {
            throw std::runtime_error("list_extend: requires dynamic list type");
        }

        auto& dest_storage = *static_cast<DynamicListStorage*>(dest.data());
        const auto& other_storage = *static_cast<const DynamicListStorage*>(other.data());

        dest_storage.extend(other_storage);
    }

    // =========================================================================
    // List slicing
    // =========================================================================

    /**
     * Slice a list: list[start:end]
     * Returns a new list with elements from index start (inclusive) to end (exclusive).
     */
    inline Value list_slice(const Value& list, size_t start, size_t end) {
        detail::check_dynamic_list_type(list, "list_slice");

        if (!detail::is_dynamic_list(list)) {
            throw std::runtime_error("list_slice: requires dynamic list type");
        }

        const auto& storage = *static_cast<const DynamicListStorage*>(list.data());

        Value result(list.schema());
        auto& result_storage = *static_cast<DynamicListStorage*>(result.data());
        result_storage = storage.slice(start, end);

        return result;
    }

    // =========================================================================
    // List query operations
    // =========================================================================

    /**
     * Find index of element in list.
     * Returns nullopt if element not found.
     */
    inline std::optional<size_t> list_index(const Value& list, const Value& elem) {
        detail::check_dynamic_list_type(list, "list_index");
        if (!elem.valid()) {
            throw std::runtime_error("list_index: invalid element");
        }

        if (!detail::is_dynamic_list(list)) {
            throw std::runtime_error("list_index: requires dynamic list type");
        }

        auto* list_meta = static_cast<const DynamicListTypeMeta*>(list.schema());
        if (elem.schema() != list_meta->element_type) {
            throw std::runtime_error("list_index: element type mismatch");
        }

        const auto& storage = *static_cast<const DynamicListStorage*>(list.data());
        return storage.index_of(elem.data());
    }

    /**
     * Count occurrences of element in list.
     */
    inline size_t list_count(const Value& list, const Value& elem) {
        detail::check_dynamic_list_type(list, "list_count");
        if (!elem.valid()) {
            throw std::runtime_error("list_count: invalid element");
        }

        if (!detail::is_dynamic_list(list)) {
            throw std::runtime_error("list_count: requires dynamic list type");
        }

        auto* list_meta = static_cast<const DynamicListTypeMeta*>(list.schema());
        if (elem.schema() != list_meta->element_type) {
            throw std::runtime_error("list_count: element type mismatch");
        }

        const auto& storage = *static_cast<const DynamicListStorage*>(list.data());
        return storage.count(elem.data());
    }

    // =========================================================================
    // List mutation operations
    // =========================================================================

    /**
     * Append element to list.
     */
    inline void list_append(Value& list, const Value& elem) {
        detail::check_dynamic_list_type(list, "list_append");
        if (!elem.valid()) {
            throw std::runtime_error("list_append: invalid element");
        }

        if (!detail::is_dynamic_list(list)) {
            throw std::runtime_error("list_append: requires dynamic list type");
        }

        auto* list_meta = static_cast<const DynamicListTypeMeta*>(list.schema());
        if (elem.schema() != list_meta->element_type) {
            throw std::runtime_error("list_append: element type mismatch");
        }

        auto& storage = *static_cast<DynamicListStorage*>(list.data());
        storage.push_back(elem.data());
    }

    /**
     * Pop element at index from list.
     * Returns a copy of the removed element.
     */
    inline Value list_pop(Value& list, size_t idx) {
        detail::check_dynamic_list_type(list, "list_pop");

        if (!detail::is_dynamic_list(list)) {
            throw std::runtime_error("list_pop: requires dynamic list type");
        }

        auto* list_meta = static_cast<const DynamicListTypeMeta*>(list.schema());
        auto& storage = *static_cast<DynamicListStorage*>(list.data());

        if (idx >= storage.size()) {
            throw std::runtime_error("list_pop: index out of range");
        }

        // Copy the element before removing
        Value result(list_meta->element_type);
        list_meta->element_type->copy_assign_at(result.data(), storage.get(idx));

        storage.pop_at(idx);
        return result;
    }

    /**
     * Pop last element from list.
     * Returns a copy of the removed element.
     */
    inline Value list_pop_back(Value& list) {
        detail::check_dynamic_list_type(list, "list_pop_back");

        if (!detail::is_dynamic_list(list)) {
            throw std::runtime_error("list_pop_back: requires dynamic list type");
        }

        auto& storage = *static_cast<DynamicListStorage*>(list.data());
        if (storage.empty()) {
            throw std::runtime_error("list_pop_back: list is empty");
        }

        return list_pop(list, storage.size() - 1);
    }

    /**
     * Insert element at index.
     */
    inline void list_insert(Value& list, size_t idx, const Value& elem) {
        detail::check_dynamic_list_type(list, "list_insert");
        if (!elem.valid()) {
            throw std::runtime_error("list_insert: invalid element");
        }

        if (!detail::is_dynamic_list(list)) {
            throw std::runtime_error("list_insert: requires dynamic list type");
        }

        auto* list_meta = static_cast<const DynamicListTypeMeta*>(list.schema());
        if (elem.schema() != list_meta->element_type) {
            throw std::runtime_error("list_insert: element type mismatch");
        }

        auto& storage = *static_cast<DynamicListStorage*>(list.data());
        storage.insert_at(idx, elem.data());
    }

    /**
     * Remove first occurrence of element from list.
     * Returns true if element was found and removed.
     */
    inline bool list_remove(Value& list, const Value& elem) {
        detail::check_dynamic_list_type(list, "list_remove");
        if (!elem.valid()) {
            throw std::runtime_error("list_remove: invalid element");
        }

        if (!detail::is_dynamic_list(list)) {
            throw std::runtime_error("list_remove: requires dynamic list type");
        }

        auto* list_meta = static_cast<const DynamicListTypeMeta*>(list.schema());
        if (elem.schema() != list_meta->element_type) {
            throw std::runtime_error("list_remove: element type mismatch");
        }

        auto& storage = *static_cast<DynamicListStorage*>(list.data());
        return storage.remove_first(elem.data());
    }

    /**
     * Reverse list in place.
     */
    inline void list_reverse(Value& list) {
        detail::check_dynamic_list_type(list, "list_reverse");

        if (!detail::is_dynamic_list(list)) {
            throw std::runtime_error("list_reverse: requires dynamic list type");
        }

        auto& storage = *static_cast<DynamicListStorage*>(list.data());
        storage.reverse();
    }

    /**
     * Clear list.
     */
    inline void list_clear(Value& list) {
        detail::check_dynamic_list_type(list, "list_clear");

        if (!detail::is_dynamic_list(list)) {
            throw std::runtime_error("list_clear: requires dynamic list type");
        }

        auto& storage = *static_cast<DynamicListStorage*>(list.data());
        storage.clear();
    }

} // namespace hgraph::value

#endif // HGRAPH_VALUE_LIST_OPS_H
