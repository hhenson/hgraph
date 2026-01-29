#pragma once

/**
 * @file delta_value.h
 * @brief Owning delta value class for tracking collection changes.
 *
 * DeltaValue provides a unified interface for tracking changes to sets, maps,
 * and lists. It owns the delta storage and provides view access.
 */

#include <hgraph/types/value/delta_storage.h>
#include <hgraph/types/value/delta_view.h>
#include <hgraph/types/value/type_meta.h>

#include <memory>
#include <stdexcept>
#include <variant>

namespace hgraph::value {

/**
 * @brief Owning storage for delta changes to collections.
 *
 * DeltaValue tracks changes (additions, removals, updates) to a collection.
 * The type of delta is determined by the schema of the value it tracks:
 * - Set schema -> SetDeltaStorage
 * - Map schema -> MapDeltaStorage
 * - List schema -> ListDeltaStorage
 *
 * Usage:
 * @code
 * // Create delta for a set of integers
 * auto set_schema = TypeRegistry::instance().set(scalar_type_meta<int64_t>()).build();
 * DeltaValue delta(set_schema);
 *
 * // Record changes
 * delta.as_set_delta().add_element(&value);
 * delta.as_set_delta().remove_element(&old_value);
 *
 * // Access changes via view
 * auto view = delta.const_view().as_set_delta();
 * for (auto elem : view.added()) {
 *     // Process added element
 * }
 * @endcode
 */
class DeltaValue {
public:
    // ========== Construction ==========

    /**
     * @brief Default constructor - creates invalid/empty delta.
     */
    DeltaValue() noexcept = default;

    /**
     * @brief Construct delta for a given value schema.
     *
     * @param value_schema Schema of the collection this delta applies to
     * @throws std::invalid_argument if schema is not a collection type
     */
    explicit DeltaValue(const TypeMeta* value_schema)
        : _value_schema(value_schema) {
        if (!value_schema) {
            return;
        }

        switch (value_schema->kind) {
            case TypeKind::Set:
                _storage.emplace<SetDeltaStorage>(value_schema->element_type);
                break;
            case TypeKind::Map:
                _storage.emplace<MapDeltaStorage>(
                    value_schema->key_type,
                    value_schema->element_type
                );
                break;
            case TypeKind::List:
                _storage.emplace<ListDeltaStorage>(value_schema->element_type);
                break;
            default:
                throw std::invalid_argument(
                    "DeltaValue: schema must be Set, Map, or List type");
        }
    }

    // Move-only semantics
    DeltaValue(DeltaValue&&) noexcept = default;
    DeltaValue& operator=(DeltaValue&&) noexcept = default;
    DeltaValue(const DeltaValue&) = delete;
    DeltaValue& operator=(const DeltaValue&) = delete;

    // ========== Type Information ==========

    /**
     * @brief Get the schema of the value this delta applies to.
     */
    [[nodiscard]] const TypeMeta* value_schema() const noexcept {
        return _value_schema;
    }

    /**
     * @brief Get the kind of collection this delta tracks.
     */
    [[nodiscard]] TypeKind kind() const noexcept {
        return _value_schema ? _value_schema->kind : TypeKind::Atomic;
    }

    /**
     * @brief Check if this is a valid delta.
     */
    [[nodiscard]] bool valid() const noexcept {
        return _value_schema != nullptr &&
               !std::holds_alternative<std::monostate>(_storage);
    }

    // ========== State Queries ==========

    /**
     * @brief Check if the delta is empty (no changes).
     */
    [[nodiscard]] bool empty() const noexcept {
        if (auto* s = std::get_if<SetDeltaStorage>(&_storage)) {
            return s->empty();
        }
        if (auto* m = std::get_if<MapDeltaStorage>(&_storage)) {
            return m->empty();
        }
        if (auto* l = std::get_if<ListDeltaStorage>(&_storage)) {
            return l->empty();
        }
        return true;
    }

    /**
     * @brief Get the total number of changes.
     */
    [[nodiscard]] size_t change_count() const noexcept {
        if (auto* s = std::get_if<SetDeltaStorage>(&_storage)) {
            return s->change_count();
        }
        if (auto* m = std::get_if<MapDeltaStorage>(&_storage)) {
            return m->change_count();
        }
        if (auto* l = std::get_if<ListDeltaStorage>(&_storage)) {
            return l->change_count();
        }
        return 0;
    }

    // ========== Clear ==========

    /**
     * @brief Clear all recorded changes.
     */
    void clear() {
        if (auto* s = std::get_if<SetDeltaStorage>(&_storage)) {
            s->clear();
        } else if (auto* m = std::get_if<MapDeltaStorage>(&_storage)) {
            m->clear();
        } else if (auto* l = std::get_if<ListDeltaStorage>(&_storage)) {
            l->clear();
        }
    }

    // ========== Mutable Storage Access ==========

    /**
     * @brief Check if this is a set delta.
     */
    [[nodiscard]] bool is_set_delta() const noexcept {
        return std::holds_alternative<SetDeltaStorage>(_storage);
    }

    /**
     * @brief Check if this is a map delta.
     */
    [[nodiscard]] bool is_map_delta() const noexcept {
        return std::holds_alternative<MapDeltaStorage>(_storage);
    }

    /**
     * @brief Check if this is a list delta.
     */
    [[nodiscard]] bool is_list_delta() const noexcept {
        return std::holds_alternative<ListDeltaStorage>(_storage);
    }

    /**
     * @brief Get mutable set delta storage.
     * @throws std::bad_variant_access if not a set delta
     */
    [[nodiscard]] SetDeltaStorage& as_set_storage() {
        return std::get<SetDeltaStorage>(_storage);
    }

    /**
     * @brief Get mutable map delta storage.
     * @throws std::bad_variant_access if not a map delta
     */
    [[nodiscard]] MapDeltaStorage& as_map_storage() {
        return std::get<MapDeltaStorage>(_storage);
    }

    /**
     * @brief Get mutable list delta storage.
     * @throws std::bad_variant_access if not a list delta
     */
    [[nodiscard]] ListDeltaStorage& as_list_storage() {
        return std::get<ListDeltaStorage>(_storage);
    }

    // ========== Const View Access ==========

    /**
     * @brief Get const view for set delta.
     * @return SetDeltaView, or empty view if not a set delta
     */
    [[nodiscard]] SetDeltaView set_view() const noexcept {
        if (auto* s = std::get_if<SetDeltaStorage>(&_storage)) {
            return SetDeltaView(s);
        }
        return SetDeltaView();
    }

    /**
     * @brief Get const view for map delta.
     * @return MapDeltaView, or empty view if not a map delta
     */
    [[nodiscard]] MapDeltaView map_view() const noexcept {
        if (auto* m = std::get_if<MapDeltaStorage>(&_storage)) {
            return MapDeltaView(m);
        }
        return MapDeltaView();
    }

    /**
     * @brief Get const view for list delta.
     * @return ListDeltaView, or empty view if not a list delta
     */
    [[nodiscard]] ListDeltaView list_view() const noexcept {
        if (auto* l = std::get_if<ListDeltaStorage>(&_storage)) {
            return ListDeltaView(l);
        }
        return ListDeltaView();
    }

    // ========== Python Interop ==========

    /**
     * @brief Convert to Python representation.
     *
     * Returns a dict with:
     * - For sets: {"added": frozenset, "removed": frozenset}
     * - For maps: {"added": dict, "updated": dict, "removed": frozenset of keys}
     * - For lists: {"updated": dict of index->value}
     */
    [[nodiscard]] nb::object to_python() const {
        if (!valid()) {
            return nb::none();
        }

        nb::dict result;

        if (auto* s = std::get_if<SetDeltaStorage>(&_storage)) {
            nb::set py_added;
            for (auto elem : s->added_range()) {
                py_added.add(elem.to_python());
            }

            nb::set py_removed;
            for (auto elem : s->removed_range()) {
                py_removed.add(elem.to_python());
            }

            result["added"] = nb::frozenset(py_added);
            result["removed"] = nb::frozenset(py_removed);
        }
        else if (auto* m = std::get_if<MapDeltaStorage>(&_storage)) {
            // Added entries
            nb::dict py_added;
            for (auto [key, value] : m->added_items_range()) {
                py_added[key.to_python()] = value.to_python();
            }

            // Updated entries
            nb::dict py_updated;
            for (auto [key, value] : m->updated_items_range()) {
                py_updated[key.to_python()] = value.to_python();
            }

            // Removed keys
            nb::set py_removed;
            for (auto key : m->removed_keys_range()) {
                py_removed.add(key.to_python());
            }

            result["added"] = py_added;
            result["updated"] = py_updated;
            result["removed"] = nb::frozenset(py_removed);
        }
        else if (auto* l = std::get_if<ListDeltaStorage>(&_storage)) {
            nb::dict py_updated;
            for (auto [idx_view, value] : l->updated_items_range()) {
                size_t idx = idx_view.as<size_t>();
                py_updated[nb::int_(idx)] = value.to_python();
            }
            result["updated"] = py_updated;
        }

        return result;
    }

private:
    const TypeMeta* _value_schema{nullptr};
    std::variant<std::monostate, SetDeltaStorage, MapDeltaStorage, ListDeltaStorage> _storage;
};

} // namespace hgraph::value
