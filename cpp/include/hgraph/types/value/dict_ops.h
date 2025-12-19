//
// Created by Claude on 19/12/2025.
//
// Dict operations as free functions operating on Value objects
//

#ifndef HGRAPH_VALUE_DICT_OPS_H
#define HGRAPH_VALUE_DICT_OPS_H

#include <hgraph/types/value/value.h>
#include <hgraph/types/value/dict_type.h>
#include <stdexcept>

namespace hgraph::value {

    // =========================================================================
    // Type checking helpers (internal)
    // =========================================================================

    namespace detail {
        inline void check_dict_type(const Value& v, const char* op_name) {
            if (!v.valid()) {
                throw std::runtime_error(std::string(op_name) + ": invalid value");
            }
            if (v.schema()->kind != TypeKind::Dict) {
                throw std::runtime_error(std::string(op_name) + ": requires dict type");
            }
        }

        inline void check_matching_dict_types(const Value& a, const Value& b, const char* op_name) {
            check_dict_type(a, op_name);
            check_dict_type(b, op_name);
            if (a.schema() != b.schema()) {
                throw std::runtime_error(std::string(op_name) + ": requires matching dict types");
            }
        }
    }

    // =========================================================================
    // Dict merge operations
    // =========================================================================

    /**
     * Merge two dicts: a | b
     * Returns a new dict with all entries from both dicts.
     * If a key exists in both, the value from b takes precedence.
     */
    inline Value dict_merge(const Value& a, const Value& b) {
        detail::check_matching_dict_types(a, b, "dict_merge");

        const auto& storage_a = *static_cast<const DictStorage*>(a.data());
        const auto& storage_b = *static_cast<const DictStorage*>(b.data());

        Value result(a.schema());
        auto& result_storage = *static_cast<DictStorage*>(result.data());
        result_storage = storage_a.merge_with(storage_b);

        return result;
    }

    /**
     * In-place merge: dest |= other
     * Adds all entries from other to dest.
     * If a key exists in both, the value from other takes precedence.
     */
    inline void dict_update(Value& dest, const Value& other) {
        detail::check_matching_dict_types(dest, other, "dict_update");

        auto& dest_storage = *static_cast<DictStorage*>(dest.data());
        const auto& other_storage = *static_cast<const DictStorage*>(other.data());

        dest_storage.update(other_storage);
    }

    // =========================================================================
    // Dict access operations
    // =========================================================================

    /**
     * Get value for key, or default if key not found.
     * Note: Returns a copy of the value in a new Value object.
     */
    inline Value dict_get(const Value& dict, const Value& key, const Value& default_val) {
        detail::check_dict_type(dict, "dict_get");
        if (!key.valid()) {
            throw std::runtime_error("dict_get: invalid key");
        }

        auto* dict_meta = static_cast<const DictTypeMeta*>(dict.schema());
        if (key.schema() != dict_meta->key_type()) {
            throw std::runtime_error("dict_get: key type mismatch");
        }
        if (default_val.valid() && default_val.schema() != dict_meta->value_type) {
            throw std::runtime_error("dict_get: default value type mismatch");
        }

        const auto& storage = *static_cast<const DictStorage*>(dict.data());
        const void* val = storage.get_or_default(key.data(), default_val.data());

        if (!val) {
            return Value::copy(default_val);
        }

        // Create a copy of the value
        Value result(dict_meta->value_type);
        dict_meta->value_type->copy_assign_at(result.data(), val);
        return result;
    }

    /**
     * Pop: remove key and return true if existed.
     * Note: This doesn't return the value - call dict_get first if you need it.
     */
    inline bool dict_pop(Value& dict, const Value& key) {
        detail::check_dict_type(dict, "dict_pop");
        if (!key.valid()) {
            throw std::runtime_error("dict_pop: invalid key");
        }

        auto* dict_meta = static_cast<const DictTypeMeta*>(dict.schema());
        if (key.schema() != dict_meta->key_type()) {
            throw std::runtime_error("dict_pop: key type mismatch");
        }

        auto& storage = *static_cast<DictStorage*>(dict.data());
        return storage.pop(key.data());
    }

    /**
     * Setdefault: if key exists, return its value; otherwise insert default and return it.
     * Returns a view of the value (not a copy).
     */
    inline ValueView dict_setdefault(Value& dict, const Value& key, const Value& default_val) {
        detail::check_dict_type(dict, "dict_setdefault");
        if (!key.valid() || !default_val.valid()) {
            throw std::runtime_error("dict_setdefault: invalid key or default");
        }

        auto* dict_meta = static_cast<const DictTypeMeta*>(dict.schema());
        if (key.schema() != dict_meta->key_type()) {
            throw std::runtime_error("dict_setdefault: key type mismatch");
        }
        if (default_val.schema() != dict_meta->value_type) {
            throw std::runtime_error("dict_setdefault: default value type mismatch");
        }

        auto& storage = *static_cast<DictStorage*>(dict.data());
        void* val = storage.setdefault(key.data(), default_val.data());
        return {val, dict_meta->value_type};
    }

    /**
     * Insert key-value pair into dict.
     * If key exists, value is updated.
     */
    inline void dict_insert(Value& dict, const Value& key, const Value& value) {
        detail::check_dict_type(dict, "dict_insert");
        if (!key.valid() || !value.valid()) {
            throw std::runtime_error("dict_insert: invalid key or value");
        }

        auto* dict_meta = static_cast<const DictTypeMeta*>(dict.schema());
        if (key.schema() != dict_meta->key_type()) {
            throw std::runtime_error("dict_insert: key type mismatch");
        }
        if (value.schema() != dict_meta->value_type) {
            throw std::runtime_error("dict_insert: value type mismatch");
        }

        auto& storage = *static_cast<DictStorage*>(dict.data());
        storage.insert(key.data(), value.data());
    }

    /**
     * Remove key from dict.
     * Returns true if key was present.
     */
    inline bool dict_remove(Value& dict, const Value& key) {
        detail::check_dict_type(dict, "dict_remove");
        if (!key.valid()) {
            throw std::runtime_error("dict_remove: invalid key");
        }

        auto* dict_meta = static_cast<const DictTypeMeta*>(dict.schema());
        if (key.schema() != dict_meta->key_type()) {
            throw std::runtime_error("dict_remove: key type mismatch");
        }

        auto& storage = *static_cast<DictStorage*>(dict.data());
        auto [removed, idx] = storage.remove(key.data());
        return removed;
    }

    /**
     * Check if key exists in dict.
     */
    inline bool dict_contains(const Value& dict, const Value& key) {
        detail::check_dict_type(dict, "dict_contains");
        if (!key.valid()) {
            throw std::runtime_error("dict_contains: invalid key");
        }

        auto* dict_meta = static_cast<const DictTypeMeta*>(dict.schema());
        if (key.schema() != dict_meta->key_type()) {
            throw std::runtime_error("dict_contains: key type mismatch");
        }

        const auto& storage = *static_cast<const DictStorage*>(dict.data());
        return storage.contains(key.data());
    }

} // namespace hgraph::value

#endif // HGRAPH_VALUE_DICT_OPS_H
