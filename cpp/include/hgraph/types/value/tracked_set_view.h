#pragma once

/**
 * @file tracked_set_view.h
 * @brief View class for TrackedSetStorage.
 *
 * TrackedSetView provides non-owning access to TrackedSetStorage, similar to
 * how SetView provides non-owning access to set storage.
 */

#include <hgraph/types/value/tracked_set_storage.h>

namespace hgraph::value {

/**
 * @brief View for TrackedSetStorage (merged const/mutable).
 *
 * Provides access to a tracked set's current value and deltas.
 * Supports both read-only and mutable operations depending on
 * how it was constructed.
 */
class TrackedSetView {
public:
    TrackedSetView() = default;

    /// Construct from const storage (read-only access)
    explicit TrackedSetView(const TrackedSetStorage* storage)
        : _storage(storage), _mutable_storage(nullptr) {}

    /// Construct from mutable storage (read-write access)
    explicit TrackedSetView(TrackedSetStorage* storage)
        : _storage(storage), _mutable_storage(storage) {}

    // ========== Validity ==========

    [[nodiscard]] bool valid() const { return _storage != nullptr; }
    explicit operator bool() const { return valid(); }

    // ========== View Accessors ==========

    /**
     * @brief Get view of current set value.
     */
    [[nodiscard]] SetView value() const {
        return _storage->value();
    }

    /**
     * @brief Get view of added elements.
     */
    [[nodiscard]] SetView added() const {
        return _storage->added();
    }

    /**
     * @brief Get view of removed elements.
     */
    [[nodiscard]] SetView removed() const {
        return _storage->removed();
    }

    // ========== Size and State ==========

    [[nodiscard]] size_t size() const { return _storage->size(); }
    [[nodiscard]] bool empty() const { return _storage->empty(); }
    [[nodiscard]] bool has_delta() const { return _storage->has_delta(); }

    // ========== Element Access ==========

    [[nodiscard]] bool contains(const View& elem) const {
        return _storage->contains(elem);
    }

    [[nodiscard]] bool was_added(const View& elem) const {
        return _storage->was_added(elem);
    }

    [[nodiscard]] bool was_removed(const View& elem) const {
        return _storage->was_removed(elem);
    }

    // ========== Typed Convenience (const) ==========

    template<typename T>
    [[nodiscard]] bool contains(const T& elem) const {
        return _storage->contains(elem);
    }

    // ========== Iteration ==========

    using const_iterator = SetView::const_iterator;

    [[nodiscard]] const_iterator begin() const { return value().begin(); }
    [[nodiscard]] const_iterator end() const { return value().end(); }

    // ========== Element Type ==========

    [[nodiscard]] const TypeMeta* element_type() const {
        return _storage->_element_type;
    }

    // ========== Mutation with Delta Tracking ==========

    /**
     * @brief Add an element with delta tracking.
     * @return true if element was newly added
     */
    bool add(const View& elem) {
        return _mutable_storage->add(elem);
    }

    /**
     * @brief Remove an element with delta tracking.
     * @return true if element was removed
     */
    bool remove(const View& elem) {
        return _mutable_storage->remove(elem);
    }

    /**
     * @brief Clear all delta tracking (call at end of cycle).
     */
    void clear_deltas() {
        _mutable_storage->clear_deltas();
    }

    /**
     * @brief Clear the entire set (tracks removals).
     */
    void clear() {
        _mutable_storage->clear();
    }

    // ========== Typed Convenience (mutable) ==========

    template<typename T>
    bool add(const T& elem) {
        return _mutable_storage->add(elem);
    }

    template<typename T>
    bool remove(const T& elem) {
        return _mutable_storage->remove(elem);
    }

private:
    const TrackedSetStorage* _storage{nullptr};
    TrackedSetStorage* _mutable_storage{nullptr};
};

} // namespace hgraph::value
