#pragma once

/**
 * @file key_set.h
 * @brief KeySet - Core slot-based storage with alive/dead tracking.
 *
 * KeySet provides stable key storage where keys NEVER move after insertion.
 * Slot reuse is managed via a free list, and each slot has an alive bit
 * to track whether it contains a valid key.
 *
 * Key design principles:
 * - Memory stability: Keys stay at their original slot forever (no swap-with-last)
 * - Alive tracking: Uses efficient bitset for slot liveness
 * - Observer pattern: Parallel arrays (values, deltas) stay synchronized
 * - O(1) operations: Insert, find, erase via hash table
 */

#include <hgraph/types/value/slot_observer.h>
#include <hgraph/types/value/type_meta.h>

#include <ankerl/unordered_dense.h>
#include <sul/dynamic_bitset.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace hgraph::value {

// Forward declaration
class KeySet;

/**
 * @brief Handle to a slot for external reference.
 *
 * SlotHandle allows external code to hold references to KeySet elements.
 * Validity can be checked via is_valid() which tests if the slot is still alive.
 * Note: This is a weak reference - the slot may be reused for a different key.
 */
struct SlotHandle {
    size_t slot{0};

    SlotHandle() = default;
    explicit SlotHandle(size_t s) : slot(s) {}

    [[nodiscard]] bool is_valid(const KeySet& ks) const;

    bool operator==(const SlotHandle& other) const {
        return slot == other.slot;
    }

    bool operator!=(const SlotHandle& other) const {
        return !(*this == other);
    }
};

/**
 * @brief Hash functor for KeySet that hashes key data.
 *
 * Uses transparent hashing to support heterogeneous lookup:
 * - Hash by slot index (for existing elements)
 * - Hash by raw pointer (for lookup queries)
 */
struct KeySetHash {
    using is_transparent = void;
    using is_avalanching = void;

    const KeySet* key_set{nullptr};

    KeySetHash() = default;
    explicit KeySetHash(const KeySet* ks) : key_set(ks) {}

    [[nodiscard]] uint64_t operator()(size_t slot) const;
    [[nodiscard]] uint64_t operator()(const void* ptr) const;
};

/**
 * @brief Equality functor for KeySet that compares key data.
 *
 * Uses transparent comparison to support heterogeneous lookup.
 */
struct KeySetEqual {
    using is_transparent = void;

    const KeySet* key_set{nullptr};

    KeySetEqual() = default;
    explicit KeySetEqual(const KeySet* ks) : key_set(ks) {}

    [[nodiscard]] bool operator()(size_t a, size_t b) const;
    [[nodiscard]] bool operator()(size_t slot, const void* ptr) const;
    [[nodiscard]] bool operator()(const void* ptr, size_t slot) const;
};

/**
 * @brief Core slot-based key storage with bitset-based alive tracking.
 *
 * KeySet stores keys in stable slots - once a key is inserted at a slot,
 * it never moves. When a key is erased, its slot is added to a free list
 * for reuse, and its alive bit is cleared.
 *
 * This design enables:
 * - Stable pointers/references to keys
 * - Efficient parallel arrays via SlotObserver
 * - Memory-efficient alive tracking via bitset
 */
class KeySet {
public:
    using IndexSet = ankerl::unordered_dense::set<size_t, KeySetHash, KeySetEqual>;

    // ========== Construction ==========

    KeySet() = default;

    explicit KeySet(const TypeMeta* key_type)
        : key_type_(key_type) {
        index_set_ = std::make_unique<IndexSet>(0, KeySetHash(this), KeySetEqual(this));
    }

    // Non-copyable, movable
    KeySet(const KeySet&) = delete;
    KeySet& operator=(const KeySet&) = delete;

    KeySet(KeySet&& other) noexcept
        : keys_(std::move(other.keys_))
        , alive_(std::move(other.alive_))
        , free_list_(std::move(other.free_list_))
        , key_type_(other.key_type_)
        , size_(other.size_)
        , observers_(std::move(other.observers_)) {
        // Rebuild index set with new 'this' pointer
        if (other.index_set_) {
            index_set_ = std::make_unique<IndexSet>(0, KeySetHash(this), KeySetEqual(this));
            for (size_t i = 0; i < alive_.size(); ++i) {
                if (alive_[i]) {
                    index_set_->insert(i);
                }
            }
        }
        other.size_ = 0;
        other.index_set_.reset();
    }

    KeySet& operator=(KeySet&& other) noexcept {
        if (this != &other) {
            keys_ = std::move(other.keys_);
            alive_ = std::move(other.alive_);
            free_list_ = std::move(other.free_list_);
            key_type_ = other.key_type_;
            size_ = other.size_;
            observers_ = std::move(other.observers_);

            if (other.index_set_) {
                index_set_ = std::make_unique<IndexSet>(0, KeySetHash(this), KeySetEqual(this));
                for (size_t i = 0; i < alive_.size(); ++i) {
                    if (alive_[i]) {
                        index_set_->insert(i);
                    }
                }
            } else {
                index_set_.reset();
            }
            other.size_ = 0;
            other.index_set_.reset();
        }
        return *this;
    }

    ~KeySet() {
        if (key_type_ && key_type_->ops().destroy) {
            for (size_t i = 0; i < alive_.size(); ++i) {
                if (alive_[i]) {
                    key_type_->ops().destroy(key_at_slot(i), key_type_);
                }
            }
        }
    }

    // ========== Observers ==========

    void add_observer(SlotObserver* observer) {
        observers_.add_observer(observer);
    }

    void remove_observer(SlotObserver* observer) {
        observers_.remove_observer(observer);
    }

    [[nodiscard]] ObserverDispatcher& observer_dispatcher() { return observers_; }
    [[nodiscard]] const ObserverDispatcher& observer_dispatcher() const { return observers_; }

    // ========== Size and Capacity ==========

    [[nodiscard]] size_t size() const { return size_; }
    [[nodiscard]] bool empty() const { return size_ == 0; }
    [[nodiscard]] size_t capacity() const {
        return key_type_ ? keys_.size() / key_type_->size : 0;
    }

    // ========== Key Access ==========

    [[nodiscard]] void* key_at_slot(size_t slot) {
        return keys_.data() + slot * key_type_->size;
    }

    [[nodiscard]] const void* key_at_slot(size_t slot) const {
        return keys_.data() + slot * key_type_->size;
    }

    [[nodiscard]] bool is_alive(size_t slot) const {
        return slot < alive_.size() && alive_[slot];
    }

    [[nodiscard]] const TypeMeta* key_type() const { return key_type_; }

    // ========== Operations ==========

    [[nodiscard]] size_t find(const void* key) const {
        if (!index_set_) return static_cast<size_t>(-1);
        auto it = index_set_->find(key);
        if (it == index_set_->end()) return static_cast<size_t>(-1);
        return *it;
    }

    [[nodiscard]] bool contains(const void* key) const {
        return find(key) != static_cast<size_t>(-1);
    }

    std::pair<size_t, bool> insert(const void* key) {
        if (!index_set_) return {static_cast<size_t>(-1), false};

        // Check if already exists
        auto it = index_set_->find(key);
        if (it != index_set_->end()) {
            return {*it, false};
        }

        // Get a slot (from free list or expand)
        if (free_list_.empty()) {
            ensure_capacity(capacity() + 1);
        }
        size_t slot = free_list_.back();
        free_list_.pop_back();

        // Construct and copy key
        void* key_ptr = key_at_slot(slot);
        if (key_type_->ops().construct) {
            key_type_->ops().construct(key_ptr, key_type_);
        }
        if (key_type_->ops().copy) {
            key_type_->ops().copy(key_ptr, key, key_type_);
        }

        // Mark slot as alive
        alive_.set(slot);
        size_++;

        // Add to index set
        index_set_->insert(slot);

        // Notify observers
        observers_.notify_insert(slot);

        return {slot, true};
    }

    bool erase(const void* key) {
        size_t slot = find(key);
        if (slot == static_cast<size_t>(-1)) return false;
        return erase_slot(slot);
    }

    bool erase_slot(size_t slot) {
        if (!is_alive(slot)) return false;

        // Notify observers BEFORE destruction
        observers_.notify_erase(slot);

        // Remove from index set
        index_set_->erase(slot);

        // Destruct the key
        void* key_ptr = key_at_slot(slot);
        if (key_type_ && key_type_->ops().destroy) {
            key_type_->ops().destroy(key_ptr, key_type_);
        }

        // Mark slot as dead
        alive_.reset(slot);
        size_--;

        // Add to free list for reuse
        free_list_.push_back(slot);

        return true;
    }

    void clear() {
        // Notify observers
        observers_.notify_clear();

        // Destruct all live keys
        if (key_type_ && key_type_->ops().destroy) {
            for (size_t i = 0; i < alive_.size(); ++i) {
                if (alive_[i]) {
                    key_type_->ops().destroy(key_at_slot(i), key_type_);
                }
            }
        }

        // Reset state
        index_set_->clear();
        alive_.reset();  // Clear all bits
        free_list_.clear();
        // Populate free list with all slots (in reverse for LIFO reuse)
        for (size_t i = alive_.size(); i > 0; --i) {
            free_list_.push_back(i - 1);
        }
        size_ = 0;
    }

    // ========== Iteration ==========

    class iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = size_t;
        using difference_type = std::ptrdiff_t;
        using pointer = const size_t*;
        using reference = size_t;

        iterator() = default;
        iterator(const KeySet* ks, size_t slot) : key_set_(ks), slot_(slot) {
            advance_to_live();
        }

        reference operator*() const { return slot_; }

        iterator& operator++() {
            ++slot_;
            advance_to_live();
            return *this;
        }

        iterator operator++(int) {
            iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        bool operator==(const iterator& other) const {
            return key_set_ == other.key_set_ && slot_ == other.slot_;
        }

        bool operator!=(const iterator& other) const {
            return !(*this == other);
        }

    private:
        void advance_to_live() {
            if (!key_set_) return;
            size_t cap = key_set_->alive_.size();
            while (slot_ < cap && !key_set_->alive_[slot_]) {
                ++slot_;
            }
        }

        const KeySet* key_set_{nullptr};
        size_t slot_{0};
    };

    [[nodiscard]] iterator begin() const {
        return iterator(this, 0);
    }

    [[nodiscard]] iterator end() const {
        return iterator(this, alive_.size());
    }

    // ========== Index Set Access ==========

    [[nodiscard]] const IndexSet* index_set() const { return index_set_.get(); }

private:
    void ensure_capacity(size_t min_slots) {
        if (!key_type_) return;

        size_t current_cap = capacity();
        if (min_slots <= current_cap) return;

        size_t new_cap = std::max(min_slots, current_cap == 0 ? 8 : current_cap * 2);
        size_t new_byte_size = new_cap * key_type_->size;

        // Notify observers of capacity change
        observers_.notify_capacity(current_cap, new_cap);

        // Handle non-trivially-copyable types
        if (!key_type_->is_trivially_copyable() && current_cap > 0) {
            std::vector<std::byte> new_keys(new_byte_size);

            // Move existing keys
            for (size_t i = 0; i < current_cap; ++i) {
                if (alive_[i]) {
                    void* old_ptr = keys_.data() + i * key_type_->size;
                    void* new_ptr = new_keys.data() + i * key_type_->size;
                    if (key_type_->ops().move_construct) {
                        key_type_->ops().move_construct(new_ptr, old_ptr, key_type_);
                    }
                    if (key_type_->ops().destroy) {
                        key_type_->ops().destroy(old_ptr, key_type_);
                    }
                }
            }
            keys_ = std::move(new_keys);
        } else {
            keys_.resize(new_byte_size);
        }

        // Expand alive bitset
        size_t old_alive_size = alive_.size();
        alive_.resize(new_cap);  // New bits are initialized to 0 (dead)

        // Add new slots to free list (in reverse for LIFO)
        for (size_t i = new_cap; i > old_alive_size; --i) {
            free_list_.push_back(i - 1);
        }
    }

    std::vector<std::byte> keys_;          // Contiguous key storage
    sul::dynamic_bitset<> alive_;          // Bit i = 1 if slot i is alive
    std::vector<size_t> free_list_;        // Available slots for reuse
    std::unique_ptr<IndexSet> index_set_;  // Hash index for O(1) lookup
    const TypeMeta* key_type_{nullptr};
    size_t size_{0};                       // Number of live keys
    ObserverDispatcher observers_;         // Observer notification dispatcher
};

// ========== SlotHandle Implementation ==========

inline bool SlotHandle::is_valid(const KeySet& ks) const {
    return ks.is_alive(slot);
}

// ========== KeySetHash Implementation ==========

inline uint64_t KeySetHash::operator()(size_t slot) const {
    const void* key = key_set->key_at_slot(slot);
    const TypeMeta* kt = key_set->key_type();
    if (kt && kt->ops().hash) {
        return kt->ops().hash(key, kt);
    }
    return 0;
}

inline uint64_t KeySetHash::operator()(const void* ptr) const {
    const TypeMeta* kt = key_set->key_type();
    if (kt && kt->ops().hash) {
        return kt->ops().hash(ptr, kt);
    }
    return 0;
}

// ========== KeySetEqual Implementation ==========

inline bool KeySetEqual::operator()(size_t a, size_t b) const {
    if (a == b) return true;
    const void* key_a = key_set->key_at_slot(a);
    const void* key_b = key_set->key_at_slot(b);
    const TypeMeta* kt = key_set->key_type();
    if (kt && kt->ops().equals) {
        return kt->ops().equals(key_a, key_b, kt);
    }
    return false;
}

inline bool KeySetEqual::operator()(size_t slot, const void* ptr) const {
    const void* key = key_set->key_at_slot(slot);
    const TypeMeta* kt = key_set->key_type();
    if (kt && kt->ops().equals) {
        return kt->ops().equals(key, ptr, kt);
    }
    return false;
}

inline bool KeySetEqual::operator()(const void* ptr, size_t slot) const {
    return (*this)(slot, ptr);
}

} // namespace hgraph::value
