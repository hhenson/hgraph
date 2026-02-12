#pragma once

/**
 * @file tracked_set_view.h
 * @brief View classes for TrackedSetStorage.
 *
 * This view provides non-owning access to TrackedSetStorage, similar to
 * how SetView provides non-owning access to set storage.
 */

#include <hgraph/types/value/tracked_set_storage.h>
#include <stdexcept>

namespace hgraph::value {

/**
 * @brief View for TrackedSetStorage.
 *
 * Uses normal C++ constness rules:
 * - const TrackedSetView exposes read-only operations
 * - non-const TrackedSetView additionally exposes mutation operations
 */
class TrackedSetView {
public:
    TrackedSetView() = default;
    explicit TrackedSetView(const TrackedSetStorage* storage)
        : _storage(storage), _mutable_storage(nullptr) {}
    explicit TrackedSetView(TrackedSetStorage* storage)
        : _storage(storage), _mutable_storage(storage) {}

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
        return _storage ? _storage->_element_type : nullptr;
    }

    // ========== Mutation with Delta Tracking ==========

    /**
     * @brief Add an element with delta tracking.
     * @return true if element was newly added
     */
    bool add(const ConstValueView& elem) {
        require_mutable_storage("add");
        return _mutable_storage->add(elem);
    }

    /**
     * @brief Remove an element with delta tracking.
     * @return true if element was removed
     */
    bool remove(const ConstValueView& elem) {
        require_mutable_storage("remove");
        return _mutable_storage->remove(elem);
    }

    /**
     * @brief Clear all delta tracking (call at end of cycle).
     */
    void clear_deltas() {
        require_mutable_storage("clear_deltas");
        _mutable_storage->clear_deltas();
    }

    /**
     * @brief Clear the entire set (tracks removals).
     */
    void clear() {
        require_mutable_storage("clear");
        _mutable_storage->clear();
    }

    // ========== Typed Convenience ==========

    template<typename T>
    bool add(const T& elem) {
        require_mutable_storage("add");
        return _mutable_storage->add(elem);
    }

    template<typename T>
    bool remove(const T& elem) {
        require_mutable_storage("remove");
        return _mutable_storage->remove(elem);
    }

private:
    void require_mutable_storage(const char* method) const {
        if (!_mutable_storage) {
            throw std::runtime_error(std::string("TrackedSetView::") + method +
                                     " requires mutable storage");
        }
    }

    const TrackedSetStorage* _storage{nullptr};
    TrackedSetStorage* _mutable_storage{nullptr};
};

} // namespace hgraph::value
