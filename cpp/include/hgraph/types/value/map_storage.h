#pragma once

/**
 * @file map_storage.h
 * @brief MapStorage - Map implementation composing SetStorage and ValueArray.
 *
 * MapStorage provides the storage layer for Map types by composing:
 * - SetStorage for key storage (wraps KeySet)
 * - ValueArray for parallel value storage (implements SlotObserver)
 *
 * Design notes:
 * - Toll-free casting: as_set() returns const SetStorage& for key iteration
 * - ValueArray is registered as observer on KeySet
 * - Value updates notify observers via on_update() for delta tracking
 */

#include <hgraph/types/value/set_storage.h>
#include <hgraph/types/value/value_array.h>

namespace hgraph::value {

/**
 * @brief Storage structure for maps using SetStorage + ValueArray composition.
 *
 * Keys are stored in SetStorage (which wraps KeySet), and values are stored
 * in a parallel ValueArray that observes the KeySet for synchronization.
 */
class MapStorage {
public:
    // ========== Construction ==========

    MapStorage() = default;

    MapStorage(const TypeMeta* key_type, const TypeMeta* value_type)
        : set_(key_type)
        , values_(value_type)
        , key_type_(key_type)
        , value_type_(value_type) {
        // Register values as observer on the key set
        set_.key_set().add_observer(&values_);
    }

    // Non-copyable due to observer registration
    MapStorage(const MapStorage&) = delete;
    MapStorage& operator=(const MapStorage&) = delete;

    MapStorage(MapStorage&& other) noexcept
        : set_(std::move(other.set_))
        , values_(std::move(other.values_))
        , key_type_(other.key_type_)
        , value_type_(other.value_type_) {
        // Re-register observer with new address
        set_.key_set().add_observer(&values_);
    }

    MapStorage& operator=(MapStorage&& other) noexcept {
        if (this != &other) {
            // Unregister old observer
            set_.key_set().remove_observer(&values_);

            set_ = std::move(other.set_);
            values_ = std::move(other.values_);
            key_type_ = other.key_type_;
            value_type_ = other.value_type_;

            // Re-register observer with new address
            set_.key_set().add_observer(&values_);
        }
        return *this;
    }

    ~MapStorage() {
        // Unregister observer before destruction
        set_.key_set().remove_observer(&values_);
    }

    // ========== Toll-Free Casting ==========

    [[nodiscard]] const SetStorage& as_set() const { return set_; }

    // ========== Size and State ==========

    [[nodiscard]] size_t size() const { return set_.size(); }
    [[nodiscard]] bool empty() const { return set_.empty(); }

    // ========== Key Operations ==========

    [[nodiscard]] bool contains(const void* key) const {
        return set_.contains(key);
    }

    // ========== Value Access ==========

    [[nodiscard]] const void* at(const void* key) const {
        size_t slot = set_.key_set().find(key);
        if (slot == static_cast<size_t>(-1)) {
            throw std::out_of_range("Map key not found");
        }
        return values_.value_at_slot(slot);
    }

    [[nodiscard]] void* at(const void* key) {
        size_t slot = set_.key_set().find(key);
        if (slot == static_cast<size_t>(-1)) {
            throw std::out_of_range("Map key not found");
        }
        return const_cast<void*>(values_.value_at_slot(slot));
    }

    void set_item(const void* key, const void* value) {
        size_t slot = set_.key_set().find(key);

        if (slot != static_cast<size_t>(-1)) {
            // Key exists - update value
            void* val_ptr = const_cast<void*>(values_.value_at_slot(slot));
            if (value_type_ && value_type_->ops().copy) {
                value_type_->ops().copy(val_ptr, value, value_type_);
            }
            // Notify observers of value update
            set_.key_set().observer_dispatcher().notify_update(slot);
        } else {
            // Insert new key (observers notified via on_insert)
            auto [new_slot, inserted] = set_.key_set().insert(key);
            if (inserted) {
                // Copy value to the newly constructed slot
                void* val_ptr = const_cast<void*>(values_.value_at_slot(new_slot));
                if (value_type_ && value_type_->ops().copy) {
                    value_type_->ops().copy(val_ptr, value, value_type_);
                }
            }
        }
    }

    bool remove(const void* key) {
        // KeySet::erase will trigger ValueArray::on_erase
        return set_.key_set().erase(key);
    }

    void clear() {
        // First destruct all values at live slots
        if (value_type_ && value_type_->ops().destroy) {
            for (auto slot : set_.key_set()) {
                void* val_ptr = const_cast<void*>(values_.value_at_slot(slot));
                value_type_->ops().destroy(val_ptr, value_type_);
            }
        }
        // Now clear keys (KeySet::clear will call on_clear)
        set_.key_set().clear();
    }

    // ========== Type Info ==========

    [[nodiscard]] const TypeMeta* key_type() const { return key_type_; }
    [[nodiscard]] const TypeMeta* value_type() const { return value_type_; }

    // ========== Iteration Support ==========

    [[nodiscard]] const void* key_at_slot(size_t slot) const {
        return set_.key_set().key_at_slot(slot);
    }

    [[nodiscard]] const void* value_at_slot(size_t slot) const {
        return values_.value_at_slot(slot);
    }

    [[nodiscard]] void* value_at_slot(size_t slot) {
        return const_cast<void*>(values_.value_at_slot(slot));
    }

    // ========== Internal Access ==========

    [[nodiscard]] KeySet& key_set() { return set_.key_set(); }
    [[nodiscard]] const KeySet& key_set() const { return set_.key_set(); }

private:
    SetStorage set_;           // Key storage (wraps KeySet)
    ValueArray values_;        // Parallel value storage (observes KeySet)
    const TypeMeta* key_type_{nullptr};
    const TypeMeta* value_type_{nullptr};
};

} // namespace hgraph::value
