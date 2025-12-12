#pragma once

#include <cstddef>
#include <cstdint>
#include <typeinfo>
#include <utility>
#include <vector>
#include <string>
#include <type_traits>
#include <functional>

#include "hgraph/util/date_time.h"
#include "hgraph/hgraph_export.h"
#include "hgraph/types/v2/any_value.h"
#include "hgraph/types/v2/ts_event.h"

namespace hgraph
{

    /**
     * @brief Type-erased set delta for TSS.
     *
     * Represents the delta (changes) to a time series set in a single evaluation cycle.
     * Contains vectors of added and removed items, both type-erased via AnyValue.
     *
     * This is the v2 equivalent of SetDelta_T<T>, but without template parameters.
     */
    struct HGRAPH_EXPORT TsSetDeltaAny
    {
        std::vector<AnyValue<>> added;   ///< Items added in this cycle
        std::vector<AnyValue<>> removed; ///< Items removed in this cycle

        /// Check if there are any changes
        [[nodiscard]] bool empty() const { return added.empty() && removed.empty(); }

        /// Check if an item was added
        [[nodiscard]] bool was_added(const AnyValue<> &item) const {
            return std::find(added.begin(), added.end(), item) != added.end();
        }

        /// Check if an item was removed
        [[nodiscard]] bool was_removed(const AnyValue<> &item) const {
            return std::find(removed.begin(), removed.end(), item) != removed.end();
        }

        /// Clear the delta
        void clear() {
            added.clear();
            removed.clear();
        }

        /// Visit all added items with a specific type
        template <typename T, typename Visitor>
        void visit_added_as(Visitor &&visitor) const {
            for (const auto &item : added) { item.visit_as<T>(std::forward<Visitor>(visitor)); }
        }

        /// Visit all removed items with a specific type
        template <typename T, typename Visitor>
        void visit_removed_as(Visitor &&visitor) const {
            for (const auto &item : removed) { item.visit_as<T>(std::forward<Visitor>(visitor)); }
        }

        /// Equality comparison
        friend bool operator==(const TsSetDeltaAny &a, const TsSetDeltaAny &b) {
            return a.added == b.added && a.removed == b.removed;
        }

        friend bool operator!=(const TsSetDeltaAny &a, const TsSetDeltaAny &b) { return !(a == b); }
    };

    /**
     * @brief Time series set event with type-erased items.
     *
     * Represents a timestamped change to a time series set. Unlike TsCollectionEventAny
     * which handles key-value pairs, TsSetEventAny handles pure set operations (add/remove items).
     *
     * Event kinds:
     * - None: No event occurred (query result)
     * - Recover: Initialize or replay set state
     * - Invalidate: Set became invalid
     * - Modify: Items were added and/or removed
     */
    struct HGRAPH_EXPORT TsSetEventAny
    {
        engine_time_t time{};                  ///< Event timestamp
        TsEventKind   kind{TsEventKind::None}; ///< Event kind
        TsSetDeltaAny delta;                   ///< The set changes (added/removed items)

        /// Factory: Create None event (no changes)
        static TsSetEventAny none(engine_time_t t) { return TsSetEventAny{t, TsEventKind::None, {}}; }

        /// Factory: Create Invalidate event
        static TsSetEventAny invalidate(engine_time_t t) { return TsSetEventAny{t, TsEventKind::Invalidate, {}}; }

        /// Factory: Create empty Modify event (use fluent API to add items)
        static TsSetEventAny modify(engine_time_t t) { return TsSetEventAny{t, TsEventKind::Modify, {}}; }

        /// Factory: Create Recover event (optionally with initial state)
        static TsSetEventAny recover(engine_time_t t) { return TsSetEventAny{t, TsEventKind::Recover, {}}; }

        /// Factory: Create Modify event with delta
        static TsSetEventAny modify(engine_time_t t, TsSetDeltaAny d) {
            return TsSetEventAny{t, TsEventKind::Modify, std::move(d)};
        }

        /// Fluent builder: Add an item (for Modify events)
        TsSetEventAny &add(const AnyValue<> &item) {
            delta.added.push_back(item);
            return *this;
        }

        /// Fluent builder: Add an item (move)
        TsSetEventAny &add(AnyValue<> &&item) {
            delta.added.push_back(std::move(item));
            return *this;
        }

        /// Fluent builder: Add an item with type deduction
        template <typename T>
        TsSetEventAny &add(T &&item) {
            AnyValue<> v;
            v.template emplace<std::decay_t<T>>(std::forward<T>(item));
            delta.added.push_back(std::move(v));
            return *this;
        }

        /// Fluent builder: Remove an item (for Modify events)
        TsSetEventAny &remove(const AnyValue<> &item) {
            delta.removed.push_back(item);
            return *this;
        }

        /// Fluent builder: Remove an item (move)
        TsSetEventAny &remove(AnyValue<> &&item) {
            delta.removed.push_back(std::move(item));
            return *this;
        }

        /// Fluent builder: Remove an item with type deduction
        template <typename T>
        TsSetEventAny &remove(T &&item) {
            AnyValue<> v;
            v.template emplace<std::decay_t<T>>(std::forward<T>(item));
            delta.removed.push_back(std::move(v));
            return *this;
        }

        /// Check if the event has any changes
        [[nodiscard]] bool has_changes() const { return !delta.empty(); }

        /// Check if event is well-formed
        [[nodiscard]] bool is_valid() const {
            switch (kind) {
                case TsEventKind::None:
                case TsEventKind::Invalidate: return delta.empty(); // Should have no delta
                case TsEventKind::Modify: return true;              // Delta can be empty or non-empty
                case TsEventKind::Recover: return true;             // Delta is optional for Recover
                default: return false;
            }
        }

        /// Visit all added items with a specific type
        template <typename T, typename Visitor>
        void visit_added_as(Visitor &&visitor) const {
            delta.visit_added_as<T>(std::forward<Visitor>(visitor));
        }

        /// Visit all removed items with a specific type
        template <typename T, typename Visitor>
        void visit_removed_as(Visitor &&visitor) const {
            delta.visit_removed_as<T>(std::forward<Visitor>(visitor));
        }

        /// Visit all items with separate handlers for add/remove
        template <typename T, typename AddFn, typename RemoveFn>
        void visit_items_as(AddFn &&on_add, RemoveFn &&on_remove) const {
            delta.visit_added_as<T>(std::forward<AddFn>(on_add));
            delta.visit_removed_as<T>(std::forward<RemoveFn>(on_remove));
        }

        /// Equality comparison
        friend bool operator==(const TsSetEventAny &a, const TsSetEventAny &b) {
            if (a.time != b.time || a.kind != b.kind) return false;
            if (a.kind == TsEventKind::Modify || a.kind == TsEventKind::Recover) { return a.delta == b.delta; }
            return true;
        }

        friend bool operator!=(const TsSetEventAny &a, const TsSetEventAny &b) { return !(a == b); }
    };

    /// String formatting for debugging
    HGRAPH_EXPORT std::string to_string(const TsSetDeltaAny &d);
    HGRAPH_EXPORT std::string to_string(const TsSetEventAny &e);

}  // namespace hgraph
