#pragma once

/**
 * @file delta_storage.h
 * @brief Storage classes for delta values (tracking changes to collections).
 *
 * Provides storage for tracking additions, removals, and updates to:
 * - Sets: added and removed elements
 * - Maps: added, updated, and removed key-value pairs
 * - Lists: updated indices and values
 *
 * Uses Struct of Arrays (SoA) layout for cache efficiency.
 */

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/view_range.h>

#include <cstddef>
#include <cstring>
#include <vector>

namespace hgraph::value {

// ============================================================================
// SetDeltaStorage
// ============================================================================

/**
 * @brief Storage for set delta changes (added/removed elements).
 *
 * Elements are stored contiguously in byte vectors, with separate
 * vectors for added and removed elements.
 */
struct SetDeltaStorage {
    std::vector<std::byte> added;      ///< Element data for additions
    std::vector<std::byte> removed;    ///< Element data for removals
    size_t added_count{0};             ///< Number of added elements
    size_t removed_count{0};           ///< Number of removed elements
    const TypeMeta* element_type{nullptr};

    // ========== Construction ==========

    SetDeltaStorage() = default;

    explicit SetDeltaStorage(const TypeMeta* elem_type)
        : element_type(elem_type) {}

    // Move only (owns element data)
    SetDeltaStorage(SetDeltaStorage&&) noexcept = default;
    SetDeltaStorage& operator=(SetDeltaStorage&&) noexcept = default;
    SetDeltaStorage(const SetDeltaStorage&) = delete;
    SetDeltaStorage& operator=(const SetDeltaStorage&) = delete;

    // ========== State Queries ==========

    [[nodiscard]] bool empty() const noexcept {
        return added_count == 0 && removed_count == 0;
    }

    [[nodiscard]] size_t change_count() const noexcept {
        return added_count + removed_count;
    }

    // ========== Range Access ==========

    [[nodiscard]] ViewRange added_range() const {
        if (!element_type || added_count == 0) {
            return ViewRange();
        }
        return ViewRange(added.data(), element_type, element_type->size, added_count);
    }

    [[nodiscard]] ViewRange removed_range() const {
        if (!element_type || removed_count == 0) {
            return ViewRange();
        }
        return ViewRange(removed.data(), element_type, element_type->size, removed_count);
    }

    // ========== Modification ==========

    /**
     * @brief Record an element as added.
     *
     * @param element Pointer to element data to copy
     */
    void add_element(const void* element) {
        if (!element_type) return;

        size_t elem_size = element_type->size;
        size_t required_size = (added_count + 1) * elem_size;

        // Grow storage if needed
        if (added.size() < required_size) {
            added.resize(required_size);
        }

        // Copy element to storage
        void* dest = added.data() + added_count * elem_size;
        if (element_type->ops && element_type->ops->construct) {
            element_type->ops->construct(dest, element_type);
        }
        if (element_type->ops && element_type->ops->copy_assign) {
            element_type->ops->copy_assign(dest, element, element_type);
        } else {
            std::memcpy(dest, element, elem_size);
        }

        ++added_count;
    }

    /**
     * @brief Record an element as removed.
     *
     * @param element Pointer to element data to copy
     */
    void remove_element(const void* element) {
        if (!element_type) return;

        size_t elem_size = element_type->size;
        size_t required_size = (removed_count + 1) * elem_size;

        // Grow storage if needed
        if (removed.size() < required_size) {
            removed.resize(required_size);
        }

        // Copy element to storage
        void* dest = removed.data() + removed_count * elem_size;
        if (element_type->ops && element_type->ops->construct) {
            element_type->ops->construct(dest, element_type);
        }
        if (element_type->ops && element_type->ops->copy_assign) {
            element_type->ops->copy_assign(dest, element, element_type);
        } else {
            std::memcpy(dest, element, elem_size);
        }

        ++removed_count;
    }

    /**
     * @brief Clear all delta records.
     */
    void clear() {
        // Destruct elements
        if (element_type && element_type->ops && element_type->ops->destruct) {
            for (size_t i = 0; i < added_count; ++i) {
                void* elem = added.data() + i * element_type->size;
                element_type->ops->destruct(elem, element_type);
            }
            for (size_t i = 0; i < removed_count; ++i) {
                void* elem = removed.data() + i * element_type->size;
                element_type->ops->destruct(elem, element_type);
            }
        }
        added_count = 0;
        removed_count = 0;
    }

    ~SetDeltaStorage() {
        clear();
    }
};

// ============================================================================
// MapDeltaStorage
// ============================================================================

/**
 * @brief Storage for map delta changes (added/updated/removed entries).
 *
 * Stores parallel arrays for keys and values, enabling efficient iteration
 * and SoA-style access patterns.
 */
struct MapDeltaStorage {
    // Added entries (new keys)
    std::vector<std::byte> added_keys;
    std::vector<std::byte> added_values;
    size_t added_count{0};

    // Updated entries (existing keys with new values)
    std::vector<std::byte> updated_keys;
    std::vector<std::byte> updated_values;
    size_t updated_count{0};

    // Removed entries (keys only - no values needed)
    std::vector<std::byte> removed_keys;
    size_t removed_count{0};

    const TypeMeta* key_type{nullptr};
    const TypeMeta* value_type{nullptr};

    // ========== Construction ==========

    MapDeltaStorage() = default;

    MapDeltaStorage(const TypeMeta* k_type, const TypeMeta* v_type)
        : key_type(k_type), value_type(v_type) {}

    // Move only
    MapDeltaStorage(MapDeltaStorage&&) noexcept = default;
    MapDeltaStorage& operator=(MapDeltaStorage&&) noexcept = default;
    MapDeltaStorage(const MapDeltaStorage&) = delete;
    MapDeltaStorage& operator=(const MapDeltaStorage&) = delete;

    // ========== State Queries ==========

    [[nodiscard]] bool empty() const noexcept {
        return added_count == 0 && updated_count == 0 && removed_count == 0;
    }

    [[nodiscard]] size_t change_count() const noexcept {
        return added_count + updated_count + removed_count;
    }

    // ========== Range Access ==========

    [[nodiscard]] ViewRange added_keys_range() const {
        if (!key_type || added_count == 0) return ViewRange();
        return ViewRange(added_keys.data(), key_type, key_type->size, added_count);
    }

    [[nodiscard]] ViewPairRange added_items_range() const {
        if (!key_type || !value_type || added_count == 0) return ViewPairRange();
        return ViewPairRange(
            added_keys.data(), added_values.data(),
            key_type, value_type,
            key_type->size, value_type->size,
            added_count
        );
    }

    [[nodiscard]] ViewRange updated_keys_range() const {
        if (!key_type || updated_count == 0) return ViewRange();
        return ViewRange(updated_keys.data(), key_type, key_type->size, updated_count);
    }

    [[nodiscard]] ViewPairRange updated_items_range() const {
        if (!key_type || !value_type || updated_count == 0) return ViewPairRange();
        return ViewPairRange(
            updated_keys.data(), updated_values.data(),
            key_type, value_type,
            key_type->size, value_type->size,
            updated_count
        );
    }

    [[nodiscard]] ViewRange removed_keys_range() const {
        if (!key_type || removed_count == 0) return ViewRange();
        return ViewRange(removed_keys.data(), key_type, key_type->size, removed_count);
    }

    // ========== Modification ==========

    void add_entry(const void* key, const void* value) {
        if (!key_type || !value_type) return;

        // Grow key storage
        size_t key_offset = added_count * key_type->size;
        if (added_keys.size() < key_offset + key_type->size) {
            added_keys.resize(key_offset + key_type->size);
        }

        // Grow value storage
        size_t value_offset = added_count * value_type->size;
        if (added_values.size() < value_offset + value_type->size) {
            added_values.resize(value_offset + value_type->size);
        }

        // Copy key
        void* key_dest = added_keys.data() + key_offset;
        if (key_type->ops && key_type->ops->construct) {
            key_type->ops->construct(key_dest, key_type);
        }
        if (key_type->ops && key_type->ops->copy_assign) {
            key_type->ops->copy_assign(key_dest, key, key_type);
        } else {
            std::memcpy(key_dest, key, key_type->size);
        }

        // Copy value
        void* value_dest = added_values.data() + value_offset;
        if (value_type->ops && value_type->ops->construct) {
            value_type->ops->construct(value_dest, value_type);
        }
        if (value_type->ops && value_type->ops->copy_assign) {
            value_type->ops->copy_assign(value_dest, value, value_type);
        } else {
            std::memcpy(value_dest, value, value_type->size);
        }

        ++added_count;
    }

    void update_entry(const void* key, const void* value) {
        if (!key_type || !value_type) return;

        // Similar to add_entry but for updated arrays
        size_t key_offset = updated_count * key_type->size;
        if (updated_keys.size() < key_offset + key_type->size) {
            updated_keys.resize(key_offset + key_type->size);
        }

        size_t value_offset = updated_count * value_type->size;
        if (updated_values.size() < value_offset + value_type->size) {
            updated_values.resize(value_offset + value_type->size);
        }

        void* key_dest = updated_keys.data() + key_offset;
        if (key_type->ops && key_type->ops->construct) {
            key_type->ops->construct(key_dest, key_type);
        }
        if (key_type->ops && key_type->ops->copy_assign) {
            key_type->ops->copy_assign(key_dest, key, key_type);
        } else {
            std::memcpy(key_dest, key, key_type->size);
        }

        void* value_dest = updated_values.data() + value_offset;
        if (value_type->ops && value_type->ops->construct) {
            value_type->ops->construct(value_dest, value_type);
        }
        if (value_type->ops && value_type->ops->copy_assign) {
            value_type->ops->copy_assign(value_dest, value, value_type);
        } else {
            std::memcpy(value_dest, value, value_type->size);
        }

        ++updated_count;
    }

    void remove_key(const void* key) {
        if (!key_type) return;

        size_t key_offset = removed_count * key_type->size;
        if (removed_keys.size() < key_offset + key_type->size) {
            removed_keys.resize(key_offset + key_type->size);
        }

        void* key_dest = removed_keys.data() + key_offset;
        if (key_type->ops && key_type->ops->construct) {
            key_type->ops->construct(key_dest, key_type);
        }
        if (key_type->ops && key_type->ops->copy_assign) {
            key_type->ops->copy_assign(key_dest, key, key_type);
        } else {
            std::memcpy(key_dest, key, key_type->size);
        }

        ++removed_count;
    }

    void clear();  // Implemented below after destructor helpers

    ~MapDeltaStorage() {
        clear();
    }

private:
    void destruct_keys(std::vector<std::byte>& keys, size_t count) {
        if (key_type && key_type->ops && key_type->ops->destruct) {
            for (size_t i = 0; i < count; ++i) {
                void* elem = keys.data() + i * key_type->size;
                key_type->ops->destruct(elem, key_type);
            }
        }
    }

    void destruct_values(std::vector<std::byte>& values, size_t count) {
        if (value_type && value_type->ops && value_type->ops->destruct) {
            for (size_t i = 0; i < count; ++i) {
                void* elem = values.data() + i * value_type->size;
                value_type->ops->destruct(elem, value_type);
            }
        }
    }
};

inline void MapDeltaStorage::clear() {
    destruct_keys(added_keys, added_count);
    destruct_values(added_values, added_count);
    destruct_keys(updated_keys, updated_count);
    destruct_values(updated_values, updated_count);
    destruct_keys(removed_keys, removed_count);

    added_count = 0;
    updated_count = 0;
    removed_count = 0;
}

// ============================================================================
// ListDeltaStorage
// ============================================================================

/**
 * @brief Storage for list delta changes (updated indices and values).
 *
 * Only stores modified elements, using sparse representation.
 * Indices and values are stored in parallel arrays.
 */
struct ListDeltaStorage {
    std::vector<size_t> updated_indices;     ///< Indices of modified elements
    std::vector<std::byte> updated_values;   ///< New values for modified elements
    size_t updated_count{0};
    const TypeMeta* element_type{nullptr};

    // ========== Construction ==========

    ListDeltaStorage() = default;

    explicit ListDeltaStorage(const TypeMeta* elem_type)
        : element_type(elem_type) {}

    // Move only
    ListDeltaStorage(ListDeltaStorage&&) noexcept = default;
    ListDeltaStorage& operator=(ListDeltaStorage&&) noexcept = default;
    ListDeltaStorage(const ListDeltaStorage&) = delete;
    ListDeltaStorage& operator=(const ListDeltaStorage&) = delete;

    // ========== State Queries ==========

    [[nodiscard]] bool empty() const noexcept {
        return updated_count == 0;
    }

    [[nodiscard]] size_t change_count() const noexcept {
        return updated_count;
    }

    // ========== Range Access ==========

    /**
     * @brief Get range of updated items as (index, value) pairs.
     *
     * Note: Returns ViewPairRange where:
     * - First element is a size_t index (NOT a value view)
     * - Second element is the new value
     */
    [[nodiscard]] ViewPairRange updated_items_range() const {
        if (!element_type || updated_count == 0) return ViewPairRange();

        // For index-value pairs, we use size_t as the "key type"
        return ViewPairRange(
            updated_indices.data(), updated_values.data(),
            scalar_type_meta<size_t>(), element_type,
            sizeof(size_t), element_type->size,
            updated_count
        );
    }

    // ========== Modification ==========

    void update_element(size_t index, const void* value) {
        if (!element_type) return;

        // Check if this index is already recorded
        for (size_t i = 0; i < updated_count; ++i) {
            if (updated_indices[i] == index) {
                // Update existing entry
                void* dest = updated_values.data() + i * element_type->size;
                if (element_type->ops && element_type->ops->copy_assign) {
                    element_type->ops->copy_assign(dest, value, element_type);
                } else {
                    std::memcpy(dest, value, element_type->size);
                }
                return;
            }
        }

        // Add new entry
        updated_indices.push_back(index);

        size_t value_offset = updated_count * element_type->size;
        if (updated_values.size() < value_offset + element_type->size) {
            updated_values.resize(value_offset + element_type->size);
        }

        void* dest = updated_values.data() + value_offset;
        if (element_type->ops && element_type->ops->construct) {
            element_type->ops->construct(dest, element_type);
        }
        if (element_type->ops && element_type->ops->copy_assign) {
            element_type->ops->copy_assign(dest, value, element_type);
        } else {
            std::memcpy(dest, value, element_type->size);
        }

        ++updated_count;
    }

    void clear() {
        if (element_type && element_type->ops && element_type->ops->destruct) {
            for (size_t i = 0; i < updated_count; ++i) {
                void* elem = updated_values.data() + i * element_type->size;
                element_type->ops->destruct(elem, element_type);
            }
        }
        updated_indices.clear();
        updated_count = 0;
    }

    ~ListDeltaStorage() {
        clear();
    }
};

} // namespace hgraph::value
