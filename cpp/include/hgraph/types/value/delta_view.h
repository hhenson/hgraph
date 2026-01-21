#pragma once

/**
 * @file delta_view.h
 * @brief Non-owning view classes for delta values.
 *
 * Provides read-only access to delta data for sets, maps, and lists:
 * - SetDeltaView: Access to added/removed elements
 * - MapDeltaView: Access to added/updated/removed entries
 * - ListDeltaView: Access to updated indices/values
 */

#include <hgraph/types/value/delta_storage.h>
#include <hgraph/types/value/view_range.h>

namespace hgraph::value {

// Forward declarations
class SetDeltaView;
class MapDeltaView;
class ListDeltaView;

// ============================================================================
// DeltaView - Base class for delta views
// ============================================================================

/**
 * @brief Base class for non-owning delta views.
 *
 * Provides common functionality for all delta view types.
 */
class DeltaView {
public:
    DeltaView() noexcept = default;

    /**
     * @brief Check if the delta is empty (no changes).
     */
    [[nodiscard]] virtual bool empty() const noexcept = 0;

    /**
     * @brief Get the total number of changes.
     */
    [[nodiscard]] virtual size_t change_count() const noexcept = 0;

    /**
     * @brief Check if this is a valid view.
     */
    [[nodiscard]] virtual bool valid() const noexcept = 0;

    virtual ~DeltaView() = default;

protected:
    DeltaView(const DeltaView&) = default;
    DeltaView& operator=(const DeltaView&) = default;
};

// ============================================================================
// SetDeltaView
// ============================================================================

/**
 * @brief Non-owning view into set delta data.
 *
 * Provides access to added and removed elements.
 */
class SetDeltaView : public DeltaView {
public:
    SetDeltaView() noexcept = default;

    /**
     * @brief Construct from SetDeltaStorage.
     */
    explicit SetDeltaView(const SetDeltaStorage* storage) noexcept
        : _storage(storage) {}

    // ========== State Queries ==========

    [[nodiscard]] bool empty() const noexcept override {
        return !_storage || _storage->empty();
    }

    [[nodiscard]] size_t change_count() const noexcept override {
        return _storage ? _storage->change_count() : 0;
    }

    [[nodiscard]] bool valid() const noexcept override {
        return _storage != nullptr;
    }

    // ========== Added Elements ==========

    /**
     * @brief Get range of added elements.
     */
    [[nodiscard]] ViewRange added() const {
        return _storage ? _storage->added_range() : ViewRange();
    }

    /**
     * @brief Get number of added elements.
     */
    [[nodiscard]] size_t added_count() const noexcept {
        return _storage ? _storage->added_count : 0;
    }

    // ========== Removed Elements ==========

    /**
     * @brief Get range of removed elements.
     */
    [[nodiscard]] ViewRange removed() const {
        return _storage ? _storage->removed_range() : ViewRange();
    }

    /**
     * @brief Get number of removed elements.
     */
    [[nodiscard]] size_t removed_count() const noexcept {
        return _storage ? _storage->removed_count : 0;
    }

    // ========== Element Type ==========

    /**
     * @brief Get the element type.
     */
    [[nodiscard]] const TypeMeta* element_type() const noexcept {
        return _storage ? _storage->element_type : nullptr;
    }

private:
    const SetDeltaStorage* _storage{nullptr};
};

// ============================================================================
// MapDeltaView
// ============================================================================

/**
 * @brief Non-owning view into map delta data.
 *
 * Provides access to added, updated, and removed entries.
 */
class MapDeltaView : public DeltaView {
public:
    MapDeltaView() noexcept = default;

    /**
     * @brief Construct from MapDeltaStorage.
     */
    explicit MapDeltaView(const MapDeltaStorage* storage) noexcept
        : _storage(storage) {}

    // ========== State Queries ==========

    [[nodiscard]] bool empty() const noexcept override {
        return !_storage || _storage->empty();
    }

    [[nodiscard]] size_t change_count() const noexcept override {
        return _storage ? _storage->change_count() : 0;
    }

    [[nodiscard]] bool valid() const noexcept override {
        return _storage != nullptr;
    }

    // ========== Added Entries ==========

    /**
     * @brief Get range of added keys.
     */
    [[nodiscard]] ViewRange added_keys() const {
        return _storage ? _storage->added_keys_range() : ViewRange();
    }

    /**
     * @brief Get range of added entries as (key, value) pairs.
     */
    [[nodiscard]] ViewPairRange added_items() const {
        return _storage ? _storage->added_items_range() : ViewPairRange();
    }

    /**
     * @brief Get number of added entries.
     */
    [[nodiscard]] size_t added_count() const noexcept {
        return _storage ? _storage->added_count : 0;
    }

    // ========== Updated Entries ==========

    /**
     * @brief Get range of updated keys.
     */
    [[nodiscard]] ViewRange updated_keys() const {
        return _storage ? _storage->updated_keys_range() : ViewRange();
    }

    /**
     * @brief Get range of updated entries as (key, new_value) pairs.
     */
    [[nodiscard]] ViewPairRange updated_items() const {
        return _storage ? _storage->updated_items_range() : ViewPairRange();
    }

    /**
     * @brief Get number of updated entries.
     */
    [[nodiscard]] size_t updated_count() const noexcept {
        return _storage ? _storage->updated_count : 0;
    }

    // ========== Removed Entries ==========

    /**
     * @brief Get range of removed keys.
     */
    [[nodiscard]] ViewRange removed_keys() const {
        return _storage ? _storage->removed_keys_range() : ViewRange();
    }

    /**
     * @brief Get number of removed entries.
     */
    [[nodiscard]] size_t removed_count() const noexcept {
        return _storage ? _storage->removed_count : 0;
    }

    // ========== Type Information ==========

    [[nodiscard]] const TypeMeta* key_type() const noexcept {
        return _storage ? _storage->key_type : nullptr;
    }

    [[nodiscard]] const TypeMeta* value_type() const noexcept {
        return _storage ? _storage->value_type : nullptr;
    }

private:
    const MapDeltaStorage* _storage{nullptr};
};

// ============================================================================
// ListDeltaView
// ============================================================================

/**
 * @brief Non-owning view into list delta data.
 *
 * Provides access to updated indices and values.
 */
class ListDeltaView : public DeltaView {
public:
    ListDeltaView() noexcept = default;

    /**
     * @brief Construct from ListDeltaStorage.
     */
    explicit ListDeltaView(const ListDeltaStorage* storage) noexcept
        : _storage(storage) {}

    // ========== State Queries ==========

    [[nodiscard]] bool empty() const noexcept override {
        return !_storage || _storage->empty();
    }

    [[nodiscard]] size_t change_count() const noexcept override {
        return _storage ? _storage->change_count() : 0;
    }

    [[nodiscard]] bool valid() const noexcept override {
        return _storage != nullptr;
    }

    // ========== Updated Elements ==========

    /**
     * @brief Get range of updated items as (index, value) pairs.
     *
     * The first element of each pair is the list index (as View of size_t).
     * The second element is the new value.
     */
    [[nodiscard]] ViewPairRange updated_items() const {
        return _storage ? _storage->updated_items_range() : ViewPairRange();
    }

    /**
     * @brief Get the updated indices directly.
     */
    [[nodiscard]] const std::vector<size_t>& updated_indices() const {
        static const std::vector<size_t> empty_vec;
        return _storage ? _storage->updated_indices : empty_vec;
    }

    /**
     * @brief Get number of updated elements.
     */
    [[nodiscard]] size_t updated_count() const noexcept {
        return _storage ? _storage->updated_count : 0;
    }

    // ========== Element Type ==========

    [[nodiscard]] const TypeMeta* element_type() const noexcept {
        return _storage ? _storage->element_type : nullptr;
    }

private:
    const ListDeltaStorage* _storage{nullptr};
};

} // namespace hgraph::value
