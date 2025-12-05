#ifndef HGRAPH_CPP_ROOT_TS_EVENT_H
#define HGRAPH_CPP_ROOT_TS_EVENT_H

#include <cstddef>
#include <cstdint>
#include <typeinfo>
#include <utility>
#include <new>
#include <vector>
#include <string>
#include <cstring>
#include <type_traits>
#include <functional>

#include "hgraph/util/date_time.h"
#include "hgraph/hgraph_export.h"
#include <nanobind/nanobind.h>
#include "hgraph/types/v2/any_value.h"

namespace hgraph
{
    namespace nb = nanobind;


    /// Time series event kind enumeration.
    /// Represents the type of change in a time series.
    enum class HGRAPH_EXPORT TsEventKind : std::uint8_t
    {
        None = 0,       ///< No event
        Recover = 1,    ///< Recovery/replay event
        Invalidate = 2, ///< Invalidation event
        Modify = 3      ///< Modification event
    };

    /// Time series event with type-erased value.
    /// Represents a timestamped change to a time series value.
    struct HGRAPH_EXPORT TsEventAny
    {
        engine_time_t time{};                  ///< Event timestamp
        TsEventKind   kind{TsEventKind::None}; ///< Event kind
        AnyValue<>    value;                   ///< Event payload (engaged for Modify, optionally for Recover)

        /// Factory methods for creating events
        static TsEventAny none(engine_time_t t);
        static TsEventAny invalidate(engine_time_t t);
        static TsEventAny recover(engine_time_t t);

        // Overload for AnyValue - just assign directly
        static TsEventAny modify(engine_time_t t, const AnyValue<> &v) {
            TsEventAny e{t, TsEventKind::Modify, v};
            return e;
        }

        static TsEventAny modify(engine_time_t t, AnyValue<> &&v) {
            TsEventAny e{t, TsEventKind::Modify, std::move(v)};
            return e;
        }

        // Template for other types - emplace into AnyValue
        template <class T>
        static TsEventAny modify(engine_time_t t, T &&v) {
            TsEventAny e{t, TsEventKind::Modify, {}};
            e.value.template emplace<std::decay_t<T>>(std::forward<T>(v));
            return e;
        }

        template <class T>
        static TsEventAny recover(engine_time_t t, T &&v) {
            TsEventAny e{t, TsEventKind::Recover, {}};
            e.value.template emplace<std::decay_t<T>>(std::forward<T>(v));
            return e;
        }

        /// Check if the event is well-formed (value presence matches event kind)
        [[nodiscard]] bool is_valid() const {
            switch (kind) {
                case TsEventKind::None:
                case TsEventKind::Invalidate: return !value.has_value(); // Should have no value
                case TsEventKind::Modify: return value.has_value();      // Must have value
                case TsEventKind::Recover: return true;                  // Value is optional for Recover
                default: return false;
            }
        }

        /// Visit the event value with a specific type (if present and type matches)
        /// Returns true if visitor was called, false otherwise
        template <typename T, typename Visitor>
        bool visit_value_as(Visitor &&visitor) const {
            if (kind == TsEventKind::Modify ||
                (kind == TsEventKind::Recover && value.has_value())) { return value.visit_as<T>(std::forward<Visitor>(visitor)); }
            return false;
        }

        /// Visit the event value with a specific type (mutable version)
        template <typename T, typename Visitor>
        bool visit_value_as(Visitor &&visitor) {
            if (kind == TsEventKind::Modify ||
                (kind == TsEventKind::Recover && value.has_value())) { return value.visit_as<T>(std::forward<Visitor>(visitor)); }
            return false;
        }

        /// Equality comparison (useful for testing)
        friend bool operator==(const TsEventAny &a, const TsEventAny &b) {
            if (a.time != b.time || a.kind != b.kind) return false;
            // Only compare value if it should be engaged
            if (a.kind == TsEventKind::Modify ||
                (a.kind == TsEventKind::Recover && a.value.has_value())) { return a.value == b.value; }
            return true;
        }

        friend bool operator!=(const TsEventAny &a, const TsEventAny &b) { return !(a == b); }
    };

    /// Simple value holder with optional semantics.
    /// Wraps an AnyValue with a has_value flag.
    struct HGRAPH_EXPORT TsValueAny
    {
        bool       has_value{false}; ///< Whether the value is present
        AnyValue<> value;            ///< The actual value (engaged when has_value is true)

        /// Create an empty value
        static TsValueAny none();

        /// Create a value from the given argument
        template <class T>
        static TsValueAny of(T &&v) {
            TsValueAny sv;
            sv.has_value = true;
            sv.value.template emplace<std::decay_t<T>>(std::forward<T>(v));
            return sv;
        }
    };

    // Collection event support (type-erased keys and values)
    using AnyKey = AnyValue<>; ///< Type alias for collection keys (improves readability)

    /// Collection item operation kind.
    enum class HGRAPH_EXPORT ColItemKind : std::uint8_t
    {
        Reset = 0,  ///< Reset operation (clear key's value)
        Modify = 1, ///< Modify operation (set key's value)
        Remove = 2  ///< Remove operation (delete key)
    };

    /// Single item in a collection event.
    /// Represents a change to one key-value pair in a collection.
    struct HGRAPH_EXPORT CollectionItem
    {
        AnyKey      key;                       ///< The key being modified
        ColItemKind kind{ColItemKind::Modify}; ///< The operation type
        AnyValue<>  value;                     ///< The new value (optionally engaged only when kind==Modify)

        /// Visit the key with a specific type
        template <typename T, typename Visitor>
        bool visit_key_as(Visitor &&visitor) const { return key.visit_as<T>(std::forward<Visitor>(visitor)); }

        /// Visit the key with a specific type (mutable version)
        template <typename T, typename Visitor>
        bool visit_key_as(Visitor &&visitor) { return key.visit_as<T>(std::forward<Visitor>(visitor)); }

        /// Visit the value with a specific type (only for Modify operations)
        template <typename T, typename Visitor>
        bool visit_value_as(Visitor &&visitor) const {
            if (kind == ColItemKind::Modify && value.has_value()) { return value.visit_as<T>(std::forward<Visitor>(visitor)); }
            return false;
        }

        /// Visit the value with a specific type (mutable version)
        template <typename T, typename Visitor>
        bool visit_value_as(Visitor &&visitor) {
            if (kind == ColItemKind::Modify && value.has_value()) { return value.visit_as<T>(std::forward<Visitor>(visitor)); }
            return false;
        }
    };

    /// Time series collection event (for dict/set/list types).
    /// Represents a batch of changes to a collection.
    struct HGRAPH_EXPORT TsCollectionEventAny
    {
        engine_time_t               time{};                  ///< Event timestamp
        TsEventKind                 kind{TsEventKind::None}; ///< Event kind (None, Invalidate, Modify, Recover)
        std::vector<CollectionItem> items;
        ///< Collection items (engaged when kind==Modify), optionaly engaged when Recover

        /// Factory methods for creating collection events
        static TsCollectionEventAny none(engine_time_t t);
        static TsCollectionEventAny invalidate(engine_time_t t);
        static TsCollectionEventAny modify(engine_time_t t);
        static TsCollectionEventAny recover(engine_time_t t);

        /// Fluent builder methods (valid only when kind==Modify)
        /// Add a modify operation (set key to value)
        TsCollectionEventAny &add_modify(AnyKey key, AnyValue<> value);

        /// Add a reset operation (clear key's value)
        TsCollectionEventAny &add_reset(AnyKey key);

        /// Add a remove operation (delete key)
        TsCollectionEventAny &remove(AnyKey key);

        /// Range-based iteration support for items
        [[nodiscard]] auto begin() const { return items.begin(); }
        [[nodiscard]] auto end() const { return items.end(); }
        [[nodiscard]] auto begin() { return items.begin(); }
        [[nodiscard]] auto end() { return items.end(); }

        /// Visit all items with typed key/value and separate handlers per operation.
        /// This is the most type-safe and ergonomic way to process collection changes.
        ///
        /// @tparam KeyType The expected type of keys in the collection
        /// @tparam ValueType The expected type of values in the collection
        /// @tparam ModifyFn Callable with signature: void(const KeyType&, const ValueType&)
        /// @tparam ResetFn Callable with signature: void(const KeyType&)
        /// @tparam RemoveFn Callable with signature: void(const KeyType&)
        ///
        /// @param on_modify Handler for Modify operations (set key to value)
        /// @param on_reset Handler for Reset operations (clear key's value)
        /// @param on_remove Handler for Remove operations (delete key)
        ///
        /// Example:
        /// @code
        /// std::map<std::string, int> my_map;
        /// event.visit_items_as<std::string, int>(
        ///     [&](const std::string& key, int value) { my_map[key] = value; },
        ///     [&](const std::string& key) { my_map[key] = 0; },
        ///     [&](const std::string& key) { my_map.erase(key); }
        /// );
        /// @endcode
        template <typename KeyType, typename ValueType, typename ModifyFn, typename ResetFn, typename RemoveFn>
        void visit_items_as(ModifyFn &&on_modify, ResetFn &&on_reset, RemoveFn &&on_remove) const {
            for (const auto &item : items) {
                item.key.visit_as<KeyType>([&](const KeyType &k) {
                    switch (item.kind) {
                        case ColItemKind::Modify: item.value.visit_as<ValueType>([&](const ValueType &v) { on_modify(k, v); });
                            break;
                        case ColItemKind::Reset: on_reset(k);
                            break;
                        case ColItemKind::Remove: on_remove(k);
                            break;
                    }
                });
            }
        }

        /// Visit all items with typed key/value and separate handlers per operation (mutable version).
        /// Allows modifying values in-place during visitation.
        template <typename KeyType, typename ValueType, typename ModifyFn, typename ResetFn, typename RemoveFn>
        void visit_items_as(ModifyFn &&on_modify, ResetFn &&on_reset, RemoveFn &&on_remove) {
            for (auto &item : items) {
                item.key.visit_as<KeyType>([&](KeyType &k) {
                    switch (item.kind) {
                        case ColItemKind::Modify: item.value.visit_as<ValueType>([&](ValueType &v) { on_modify(k, v); });
                            break;
                        case ColItemKind::Reset: on_reset(k);
                            break;
                        case ColItemKind::Remove: on_remove(k);
                            break;
                    }
                });
            }
        }
    };

    // String formatting helpers (exported API)
    HGRAPH_EXPORT std::string to_string(const TsEventAny &e);
    HGRAPH_EXPORT std::string to_string(const TsValueAny &v);
    HGRAPH_EXPORT std::string to_string(const TsCollectionEventAny &e);
} // namespace hgraph

#endif // HGRAPH_CPP_ROOT_TS_EVENT_H
