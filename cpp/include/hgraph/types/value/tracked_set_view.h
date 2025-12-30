#pragma once

/**
 * @file tracked_set_view.h
 * @brief View classes for TrackedSetStorage.
 *
 * These views provide non-owning access to TrackedSetStorage, similar to
 * how SetView provides non-owning access to set storage.
 */

#include <hgraph/types/value/tracked_set_storage.h>

namespace hgraph::value {

/**
 * @brief Const view for TrackedSetStorage.
 *
 * Provides read-only access to a tracked set's current value and deltas.
 */
class ConstTrackedSetView {
public:
    ConstTrackedSetView() = default;
    explicit ConstTrackedSetView(const TrackedSetStorage* storage)
        : _storage(storage) {}

    // ========== Validity ==========

    [[nodiscard]] bool valid() const { return _storage != nullptr; }
    explicit operator bool() const { return valid(); }

    // ========== View Accessors ==========

    /**
     * @brief Get const view of current set value.
     */
    [[nodiscard]] ConstSetView value() const {
        return _storage->value();
    }

    /**
     * @brief Get const view of added elements.
     */
    [[nodiscard]] ConstSetView added() const {
        return _storage->added();
    }

    /**
     * @brief Get const view of removed elements.
     */
    [[nodiscard]] ConstSetView removed() const {
        return _storage->removed();
    }

    // ========== Size and State ==========

    [[nodiscard]] size_t size() const { return _storage->size(); }
    [[nodiscard]] bool empty() const { return _storage->empty(); }
    [[nodiscard]] bool has_delta() const { return _storage->has_delta(); }

    // ========== Element Access ==========

    [[nodiscard]] bool contains(const ConstValueView& elem) const {
        return _storage->contains(elem);
    }

    [[nodiscard]] bool was_added(const ConstValueView& elem) const {
        return _storage->was_added(elem);
    }

    [[nodiscard]] bool was_removed(const ConstValueView& elem) const {
        return _storage->was_removed(elem);
    }

    // ========== Typed Convenience ==========

    template<typename T>
    [[nodiscard]] bool contains(const T& elem) const {
        return _storage->contains(elem);
    }

    // ========== Iteration ==========

    using const_iterator = ConstSetView::const_iterator;

    [[nodiscard]] const_iterator begin() const { return value().begin(); }
    [[nodiscard]] const_iterator end() const { return value().end(); }

    // ========== Element Type ==========

    [[nodiscard]] const TypeMeta* element_type() const {
        return _storage->_element_type;
    }

protected:
    const TrackedSetStorage* _storage{nullptr};
};

/**
 * @brief Mutable view for TrackedSetStorage.
 *
 * Provides read-write access to a tracked set with delta tracking.
 */
class TrackedSetView : public ConstTrackedSetView {
public:
    TrackedSetView() = default;
    explicit TrackedSetView(TrackedSetStorage* storage)
        : ConstTrackedSetView(storage), _mutable_storage(storage) {}

    // ========== Mutation with Delta Tracking ==========

    /**
     * @brief Add an element with delta tracking.
     * @return true if element was newly added
     */
    bool add(const ConstValueView& elem) {
        return _mutable_storage->add(elem);
    }

    /**
     * @brief Remove an element with delta tracking.
     * @return true if element was removed
     */
    bool remove(const ConstValueView& elem) {
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

    // ========== Typed Convenience ==========

    template<typename T>
    bool add(const T& elem) {
        return _mutable_storage->add(elem);
    }

    template<typename T>
    bool remove(const T& elem) {
        return _mutable_storage->remove(elem);
    }

private:
    TrackedSetStorage* _mutable_storage{nullptr};
};

} // namespace hgraph::value
