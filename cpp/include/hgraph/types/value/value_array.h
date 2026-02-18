#pragma once

/**
 * @file value_array.h
 * @brief ValueArray - SlotObserver for parallel value storage in Maps.
 *
 * ValueArray maintains values in parallel with a KeySet's keys. When the
 * KeySet inserts/erases, ValueArray constructs/destructs values at matching
 * slots, keeping the arrays synchronized.
 *
 * Key design:
 * - Implements SlotObserver to receive KeySet notifications
 * - Values at slot i correspond to keys at slot i
 * - Handles capacity changes, insertions, erasures, and clears
 */

#include <hgraph/types/value/slot_observer.h>
#include <hgraph/types/value/type_meta.h>

#include <algorithm>
#include <cstddef>
#include <vector>

namespace hgraph::value {

/**
 * @brief Parallel value storage synchronized with a KeySet.
 *
 * ValueArray implements SlotObserver to maintain values in sync with
 * a KeySet. Each value at slot i corresponds to the key at slot i.
 */
class ValueArray : public SlotObserver {
public:
    // ========== Construction ==========

    ValueArray() = default;

    /**
     * @brief Construct with a specific value type.
     * @param value_type The TypeMeta for values
     */
    explicit ValueArray(const TypeMeta* value_type)
        : value_type_(value_type) {
    }

    // Non-copyable, movable
    ValueArray(const ValueArray&) = delete;
    ValueArray& operator=(const ValueArray&) = delete;

    ValueArray(ValueArray&& other) noexcept
        : values_(std::move(other.values_))
        , value_type_(other.value_type_)
        , capacity_(other.capacity_)
        , initialized_(std::move(other.initialized_)) {
        other.capacity_ = 0;
    }

    ValueArray& operator=(ValueArray&& other) noexcept {
        if (this != &other) {
            destroy_all_initialized();
            values_ = std::move(other.values_);
            value_type_ = other.value_type_;
            capacity_ = other.capacity_;
            initialized_ = std::move(other.initialized_);
            other.capacity_ = 0;
        }
        return *this;
    }

    ~ValueArray() {
        destroy_all_initialized();
    }

    // ========== SlotObserver Implementation ==========

    void on_capacity(size_t /*old_cap*/, size_t new_cap) override {
        if (!value_type_) return;

        size_t new_byte_size = new_cap * value_type_->size;

        // Handle non-trivially-copyable types
        if (!value_type_->is_trivially_copyable() && capacity_ > 0) {
            std::vector<std::byte> new_values(new_byte_size);

            // Move existing values (we don't know which are alive, caller must handle)
            // For now, just copy raw bytes and let caller manage lifecycle
            size_t copy_bytes = std::min(values_.size(), new_byte_size);
            std::memcpy(new_values.data(), values_.data(), copy_bytes);

            values_ = std::move(new_values);
        } else {
            values_.resize(new_byte_size);
        }

        capacity_ = new_cap;
        initialized_.resize(new_cap, false);
    }

    void on_insert(size_t slot) override {
        // Construct a default value at this slot
        if (!value_type_) return;
        if (slot >= capacity_) return;

        // If a previously-erased value is retained at this slot, destruct it
        // before constructing the new value.
        if (slot < initialized_.size() && initialized_[slot]) {
            if (value_type_->ops && value_type_->ops->destruct) {
                void* old_ptr = value_at_slot(slot);
                value_type_->ops->destruct(old_ptr, value_type_);
            }
        }

        void* val_ptr = value_at_slot(slot);
        if (value_type_->ops && value_type_->ops->construct) {
            value_type_->ops->construct(val_ptr, value_type_);
        }
        if (slot < initialized_.size()) {
            initialized_[slot] = true;
        }
    }

    void on_erase(size_t slot) override {
        (void)slot;
        // Preserve erased slot values so removed_items() can still access
        // value payloads during the current tick.
    }

    void on_update(size_t /*slot*/) override {
        // Value updates are handled by MapStorage - no action needed here
    }

    void on_clear() override {
        destroy_all_initialized();
        std::fill(initialized_.begin(), initialized_.end(), false);
    }

    // ========== Value Access ==========

    /**
     * @brief Get value pointer at a slot (unchecked).
     */
    [[nodiscard]] void* value_at_slot(size_t slot) {
        return values_.data() + slot * value_type_->size;
    }

    [[nodiscard]] const void* value_at_slot(size_t slot) const {
        return values_.data() + slot * value_type_->size;
    }

    /**
     * @brief Get the value type.
     */
    [[nodiscard]] const TypeMeta* value_type() const { return value_type_; }

    /**
     * @brief Get current capacity.
     */
    [[nodiscard]] size_t capacity() const { return capacity_; }

    // ========== Data Access ==========

    [[nodiscard]] const std::byte* data() const { return values_.data(); }

private:
    void destroy_all_initialized() {
        if (!value_type_ || !value_type_->ops || !value_type_->ops->destruct) {
            return;
        }
        size_t limit = std::min(initialized_.size(), capacity_);
        for (size_t i = 0; i < limit; ++i) {
            if (initialized_[i]) {
                void* val_ptr = value_at_slot(i);
                value_type_->ops->destruct(val_ptr, value_type_);
                initialized_[i] = false;
            }
        }
    }

    std::vector<std::byte> values_;
    const TypeMeta* value_type_{nullptr};
    size_t capacity_{0};
    std::vector<bool> initialized_;
};

} // namespace hgraph::value
