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
#include <hgraph/types/value/validity_bitmap.h>

#include <algorithm>
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
    static size_t mask_bytes(size_t slots) {
        return validity_mask_bytes(slots);
    }

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
        , validity_(std::move(other.validity_))
        , constructed_(std::move(other.constructed_))
        , value_type_(other.value_type_)
        , capacity_(other.capacity_) {
        other.capacity_ = 0;
    }

    ValueArray& operator=(ValueArray&& other) noexcept {
        if (this != &other) {
            values_ = std::move(other.values_);
            validity_ = std::move(other.validity_);
            constructed_ = std::move(other.constructed_);
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
        size_t new_mask_size = mask_bytes(new_cap);

        // Relocate non-trivial objects with move-construct to preserve invariants.
        if (!value_type_->is_trivially_copyable() && capacity_ > 0) {
            std::vector<std::byte> new_values(new_byte_size);
            for (size_t slot = 0; slot < capacity_; ++slot) {
                if (!is_constructed_slot(slot)) continue;

                void* old_ptr = values_.data() + slot * value_type_->size;
                void* new_ptr = new_values.data() + slot * value_type_->size;

                if (value_type_->ops().move_construct) {
                    value_type_->ops().move_construct(new_ptr, old_ptr, value_type_);
                } else if (value_type_->ops().construct && value_type_->ops().copy) {
                    value_type_->ops().construct(new_ptr, value_type_);
                    value_type_->ops().copy(new_ptr, old_ptr, value_type_);
                }

                if (value_type_->ops().destroy) {
                    value_type_->ops().destroy(old_ptr, value_type_);
                }
            }

            values_ = std::move(new_values);
        } else {
            values_.resize(new_byte_size);
        }

        validity_.resize(new_mask_size, std::byte{0});
        constructed_.resize(new_mask_size, std::byte{0});
        capacity_ = new_cap;
    }

    void on_insert(size_t slot) override {
        // Construct a default value at this slot
        if (!value_type_) return;
        void* val_ptr = value_at_slot(slot);
        if (value_type_->ops().construct) {
            value_type_->ops().construct(val_ptr, value_type_);
        }
        set_constructed_slot(slot, true);
        set_valid_slot(slot, true);
    }

    void on_erase(size_t slot) override {
        // Destruct the value at this slot
        if (!value_type_) return;
        if (is_constructed_slot(slot)) {
            void* val_ptr = value_at_slot(slot);
            if (value_type_->ops().destroy) {
                value_type_->ops().destroy(val_ptr, value_type_);
            }
        }
        set_constructed_slot(slot, false);
        set_valid_slot(slot, false);
    }

    void on_update(size_t /*slot*/) override {
        // Value updates are handled by MapStorage - no action needed here
    }

    void on_clear() override {
        // All values will be destructed - handled by caller iterating live slots
        // Just reset our state (values storage remains allocated for reuse)
        std::fill(validity_.begin(), validity_.end(), std::byte{0});
        std::fill(constructed_.begin(), constructed_.end(), std::byte{0});
    }

    // ========== Value Access ==========

    [[nodiscard]] void* value_at_slot(size_t slot) {
        return values_.data() + slot * value_type_->size;
    }

    [[nodiscard]] const void* value_at_slot(size_t slot) const {
        return values_.data() + slot * value_type_->size;
    }

    [[nodiscard]] const void* value_or_null_at_slot(size_t slot) const {
        return is_valid_slot(slot) ? value_at_slot(slot) : nullptr;
    }

    [[nodiscard]] bool is_valid_slot(size_t slot) const {
        if (slot >= capacity_ || validity_.empty()) return false;
        return validity_bit_get(validity_.data(), slot);
    }

    void set_valid_slot(size_t slot, bool valid) {
        if (slot >= capacity_ || validity_.empty()) return;
        validity_bit_set(validity_.data(), slot, valid);
    }

    [[nodiscard]] bool is_constructed_slot(size_t slot) const {
        if (slot >= capacity_ || constructed_.empty()) return false;
        return validity_bit_get(constructed_.data(), slot);
    }

    void set_constructed_slot(size_t slot, bool constructed) {
        if (slot >= capacity_ || constructed_.empty()) return;
        validity_bit_set(constructed_.data(), slot, constructed);
    }

    [[nodiscard]] const TypeMeta* value_type() const { return value_type_; }
    [[nodiscard]] size_t capacity() const { return capacity_; }
    [[nodiscard]] const std::byte* data() const { return values_.data(); }

private:
    std::vector<std::byte> values_;
    std::vector<std::byte> validity_;
    std::vector<std::byte> constructed_;
    const TypeMeta* value_type_{nullptr};
    size_t capacity_{0};
};

} // namespace hgraph::value
