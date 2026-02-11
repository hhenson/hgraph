#pragma once

/**
 * @file set_storage.h
 * @brief SetStorage - Set implementation wrapping KeySet.
 *
 * SetStorage provides the storage layer for Set types, wrapping a KeySet
 * and exposing the API expected by SetOps (add, remove, contains, etc.).
 *
 * Design notes:
 * - Delegates to KeySet for all key operations
 * - Exposes key_set() for observers to register
 * - Methods use user-guide naming: add(), remove(), values()
 */

#include <hgraph/types/value/key_set.h>

namespace hgraph::value {

/**
 * @brief Storage structure for sets using KeySet.
 *
 * This is the inline storage for Set Values. It wraps KeySet and provides
 * the interface expected by SetOps.
 */
class SetStorage {
public:
    // ========== Construction ==========

    SetStorage() = default;

    explicit SetStorage(const TypeMeta* element_type)
        : key_set_(element_type)
        , element_type_(element_type) {
    }

    // Non-copyable, movable
    SetStorage(const SetStorage&) = delete;
    SetStorage& operator=(const SetStorage&) = delete;

    SetStorage(SetStorage&& other) noexcept = default;
    SetStorage& operator=(SetStorage&& other) noexcept = default;

    // ========== KeySet Access ==========

    [[nodiscard]] KeySet& key_set() { return key_set_; }
    [[nodiscard]] const KeySet& key_set() const { return key_set_; }

    // ========== Size and State ==========

    [[nodiscard]] size_t size() const { return key_set_.size(); }
    [[nodiscard]] bool empty() const { return key_set_.empty(); }

    // ========== Element Operations ==========

    bool add(const void* elem) {
        auto [slot, inserted] = key_set_.insert(elem);
        return inserted;
    }

    bool remove(const void* elem) {
        return key_set_.erase(elem);
    }

    [[nodiscard]] bool contains(const void* elem) const {
        return key_set_.contains(elem);
    }

    void clear() {
        key_set_.clear();
    }

    // ========== Type Info ==========

    [[nodiscard]] const TypeMeta* element_type() const { return element_type_; }

    // ========== Iteration ==========

    class iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = const void*;
        using difference_type = std::ptrdiff_t;
        using pointer = const void**;
        using reference = const void*;

        iterator() = default;
        iterator(const SetStorage* storage, KeySet::iterator it)
            : storage_(storage), it_(it) {}

        reference operator*() const {
            return storage_->key_set_.key_at_slot(*it_);
        }

        iterator& operator++() {
            ++it_;
            return *this;
        }

        iterator operator++(int) {
            iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        bool operator==(const iterator& other) const {
            return it_ == other.it_;
        }

        bool operator!=(const iterator& other) const {
            return !(*this == other);
        }

        [[nodiscard]] size_t slot() const { return *it_; }

    private:
        const SetStorage* storage_{nullptr};
        KeySet::iterator it_;
    };

    [[nodiscard]] iterator begin() const {
        return iterator(this, key_set_.begin());
    }

    [[nodiscard]] iterator end() const {
        return iterator(this, key_set_.end());
    }

private:
    KeySet key_set_;
    const TypeMeta* element_type_{nullptr};
};

} // namespace hgraph::value
