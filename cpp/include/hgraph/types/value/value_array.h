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

#include <cstddef>
#include <cstring>
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

    explicit ValueArray(const TypeMeta* value_type)
        : value_type_(value_type) {
    }

    // Non-copyable, movable
    ValueArray(const ValueArray&) = delete;
    ValueArray& operator=(const ValueArray&) = delete;

    ValueArray(ValueArray&& other) noexcept
        : values_(std::move(other.values_))
        , value_type_(other.value_type_)
        , capacity_(other.capacity_) {
        other.capacity_ = 0;
    }

    ValueArray& operator=(ValueArray&& other) noexcept {
        if (this != &other) {
            values_ = std::move(other.values_);
            value_type_ = other.value_type_;
            capacity_ = other.capacity_;
            other.capacity_ = 0;
        }
        return *this;
    }

    ~ValueArray() {
        // Note: Destruction of values is handled by on_clear or MapStorage destructor
        // that knows which slots are alive
    }

    // ========== SlotObserver Implementation ==========

    void on_capacity(size_t /*old_cap*/, size_t new_cap) override {
        if (!value_type_) return;

        size_t new_byte_size = new_cap * value_type_->size;

        // Handle non-trivially-copyable types
        if (!value_type_->is_trivially_copyable() && capacity_ > 0) {
            std::vector<std::byte> new_values(new_byte_size);

            // Copy raw bytes (caller manages lifecycle via on_insert/on_erase)
            size_t copy_bytes = std::min(values_.size(), new_byte_size);
            std::memcpy(new_values.data(), values_.data(), copy_bytes);

            values_ = std::move(new_values);
        } else {
            values_.resize(new_byte_size);
        }

        capacity_ = new_cap;
    }

    void on_insert(size_t slot) override {
        // Construct a default value at this slot
        if (!value_type_) return;
        void* val_ptr = value_at_slot(slot);
        if (value_type_->ops().construct) {
            value_type_->ops().construct(val_ptr, value_type_);
        }
    }

    void on_erase(size_t slot) override {
        // Destruct the value at this slot
        if (!value_type_) return;
        void* val_ptr = value_at_slot(slot);
        if (value_type_->ops().destroy) {
            value_type_->ops().destroy(val_ptr, value_type_);
        }
    }

    void on_update(size_t /*slot*/) override {
        // Value updates are handled by MapStorage - no action needed here
    }

    void on_clear() override {
        // All values will be destructed - handled by caller iterating live slots
        // Just reset our state (values storage remains allocated for reuse)
    }

    // ========== Value Access ==========

    [[nodiscard]] void* value_at_slot(size_t slot) {
        return values_.data() + slot * value_type_->size;
    }

    [[nodiscard]] const void* value_at_slot(size_t slot) const {
        return values_.data() + slot * value_type_->size;
    }

    [[nodiscard]] const TypeMeta* value_type() const { return value_type_; }
    [[nodiscard]] size_t capacity() const { return capacity_; }
    [[nodiscard]] const std::byte* data() const { return values_.data(); }

private:
    std::vector<std::byte> values_;
    const TypeMeta* value_type_{nullptr};
    size_t capacity_{0};
};

} // namespace hgraph::value
