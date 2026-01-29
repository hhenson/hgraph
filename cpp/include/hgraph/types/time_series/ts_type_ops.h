#pragma once

/**
 * @file ts_type_ops.h
 * @brief TypeOps implementations for time-series infrastructure types.
 *
 * This file provides TypeOps implementations for the time-series types that
 * need to be stored in Value containers:
 * - ObserverList
 * - SetDelta
 * - MapDelta
 * - BundleDeltaNav
 * - ListDeltaNav
 *
 * These are used by the schema generation functions to create TypeMeta
 * for the parallel Value structures (time_, observer_, delta_value_).
 */

#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/set_delta.h>
#include <hgraph/types/time_series/map_delta.h>
#include <hgraph/types/time_series/delta_nav.h>
#include <hgraph/types/value/type_meta.h>

#include <nanobind/nanobind.h>

#include <string>

namespace nb = nanobind;

namespace hgraph {

// ============================================================================
// ObserverList TypeOps
// ============================================================================

/**
 * @brief TypeOps implementation for ObserverList.
 *
 * ObserverList is used in the observer_ parallel Value structure.
 * It doesn't need Python interop or hash - just basic lifecycle management.
 */
struct ObserverListOps {
    static void construct(void* dst, const value::TypeMeta*) {
        new (dst) ObserverList();
    }

    static void destruct(void* obj, const value::TypeMeta*) {
        static_cast<ObserverList*>(obj)->~ObserverList();
    }

    static void copy_assign(void* dst, const void* src, const value::TypeMeta*) {
        *static_cast<ObserverList*>(dst) = *static_cast<const ObserverList*>(src);
    }

    static void move_assign(void* dst, void* src, const value::TypeMeta*) {
        *static_cast<ObserverList*>(dst) = std::move(*static_cast<ObserverList*>(src));
    }

    static void move_construct(void* dst, void* src, const value::TypeMeta*) {
        new (dst) ObserverList(std::move(*static_cast<ObserverList*>(src)));
    }

    static bool equals(const void* a, const void* b, const value::TypeMeta*) {
        // Two observer lists are "equal" if they have the same size
        // (We don't compare the actual observer pointers)
        return static_cast<const ObserverList*>(a)->size() ==
               static_cast<const ObserverList*>(b)->size();
    }

    static std::string to_string(const void* obj, const value::TypeMeta*) {
        return "ObserverList(size=" +
               std::to_string(static_cast<const ObserverList*>(obj)->size()) + ")";
    }

    static nb::object to_python(const void*, const value::TypeMeta*) {
        // ObserverList is internal, return None for Python
        return nb::none();
    }

    static void from_python(void*, const nb::object&, const value::TypeMeta*) {
        // No-op - ObserverList is internal
    }

    /// Get the operations vtable for ObserverList
    static const value::TypeOps* ops() {
        static const value::TypeOps observer_list_ops = {
            &construct,
            &destruct,
            &copy_assign,
            &move_assign,
            &move_construct,
            &equals,
            &to_string,
            &to_python,
            &from_python,
            nullptr,   // hash (not hashable)
            nullptr,   // less_than (not comparable)
            nullptr,   // size (not iterable)
            nullptr,   // get_at (not indexable)
            nullptr,   // set_at (not indexable)
            nullptr,   // get_field (not bundle)
            nullptr,   // set_field (not bundle)
            nullptr,   // contains (not set)
            nullptr,   // insert (not set)
            nullptr,   // erase (not set)
            nullptr,   // map_get (not map)
            nullptr,   // map_set (not map)
            nullptr,   // resize (not resizable)
            nullptr,   // clear (not clearable)
        };
        return &observer_list_ops;
    }
};

// ============================================================================
// SetDelta TypeOps
// ============================================================================

/**
 * @brief TypeOps implementation for SetDelta.
 *
 * SetDelta is used in the delta_value_ parallel Value structure for TSS types.
 */
struct SetDeltaOps {
    static void construct(void* dst, const value::TypeMeta*) {
        new (dst) SetDelta();
    }

    static void destruct(void* obj, const value::TypeMeta*) {
        static_cast<SetDelta*>(obj)->~SetDelta();
    }

    static void copy_assign(void* dst, const void* src, const value::TypeMeta*) {
        // SetDelta is non-copyable by design, but we need copy_assign for TypeOps
        // Just copy the state manually
        auto* d = static_cast<SetDelta*>(dst);
        auto* s = static_cast<const SetDelta*>(src);
        d->clear();
        // Note: We can't copy the internal vectors since SetDelta is designed
        // to be non-copyable. For schema generation, we only need construct/destruct.
        (void)s;
    }

    static void move_assign(void* dst, void* src, const value::TypeMeta*) {
        *static_cast<SetDelta*>(dst) = std::move(*static_cast<SetDelta*>(src));
    }

    static void move_construct(void* dst, void* src, const value::TypeMeta*) {
        new (dst) SetDelta(std::move(*static_cast<SetDelta*>(src)));
    }

    static bool equals(const void* a, const void* b, const value::TypeMeta*) {
        auto* da = static_cast<const SetDelta*>(a);
        auto* db = static_cast<const SetDelta*>(b);
        return da->added() == db->added() &&
               da->removed() == db->removed() &&
               da->was_cleared() == db->was_cleared();
    }

    static std::string to_string(const void* obj, const value::TypeMeta*) {
        auto* d = static_cast<const SetDelta*>(obj);
        return "SetDelta(added=" + std::to_string(d->added().size()) +
               ", removed=" + std::to_string(d->removed().size()) +
               ", cleared=" + (d->was_cleared() ? "true" : "false") + ")";
    }

    static nb::object to_python(const void* obj, const value::TypeMeta*) {
        auto* d = static_cast<const SetDelta*>(obj);
        nb::dict result;

        nb::list added_list;
        for (auto slot : d->added()) {
            added_list.append(nb::int_(slot));
        }
        result["added"] = added_list;

        nb::list removed_list;
        for (auto slot : d->removed()) {
            removed_list.append(nb::int_(slot));
        }
        result["removed"] = removed_list;

        result["cleared"] = nb::bool_(d->was_cleared());
        return result;
    }

    static void from_python(void*, const nb::object&, const value::TypeMeta*) {
        // No-op - SetDelta is populated via SlotObserver
    }

    /// Get the operations vtable for SetDelta
    static const value::TypeOps* ops() {
        static const value::TypeOps set_delta_ops = {
            &construct,
            &destruct,
            &copy_assign,
            &move_assign,
            &move_construct,
            &equals,
            &to_string,
            &to_python,
            &from_python,
            nullptr,   // hash
            nullptr,   // less_than
            nullptr,   // size
            nullptr,   // get_at
            nullptr,   // set_at
            nullptr,   // get_field
            nullptr,   // set_field
            nullptr,   // contains
            nullptr,   // insert
            nullptr,   // erase
            nullptr,   // map_get
            nullptr,   // map_set
            nullptr,   // resize
            nullptr,   // clear
        };
        return &set_delta_ops;
    }
};

// ============================================================================
// MapDelta TypeOps
// ============================================================================

/**
 * @brief TypeOps implementation for MapDelta.
 *
 * MapDelta is used in the delta_value_ parallel Value structure for TSD types.
 */
struct MapDeltaOps {
    static void construct(void* dst, const value::TypeMeta*) {
        new (dst) MapDelta();
    }

    static void destruct(void* obj, const value::TypeMeta*) {
        static_cast<MapDelta*>(obj)->~MapDelta();
    }

    static void copy_assign(void* dst, const void* src, const value::TypeMeta*) {
        // MapDelta is non-copyable by design
        auto* d = static_cast<MapDelta*>(dst);
        d->clear();
        (void)src;
    }

    static void move_assign(void* dst, void* src, const value::TypeMeta*) {
        *static_cast<MapDelta*>(dst) = std::move(*static_cast<MapDelta*>(src));
    }

    static void move_construct(void* dst, void* src, const value::TypeMeta*) {
        new (dst) MapDelta(std::move(*static_cast<MapDelta*>(src)));
    }

    static bool equals(const void* a, const void* b, const value::TypeMeta*) {
        auto* da = static_cast<const MapDelta*>(a);
        auto* db = static_cast<const MapDelta*>(b);
        return da->added() == db->added() &&
               da->removed() == db->removed() &&
               da->updated() == db->updated() &&
               da->was_cleared() == db->was_cleared();
    }

    static std::string to_string(const void* obj, const value::TypeMeta*) {
        auto* d = static_cast<const MapDelta*>(obj);
        return "MapDelta(added=" + std::to_string(d->added().size()) +
               ", removed=" + std::to_string(d->removed().size()) +
               ", updated=" + std::to_string(d->updated().size()) +
               ", cleared=" + (d->was_cleared() ? "true" : "false") + ")";
    }

    static nb::object to_python(const void* obj, const value::TypeMeta*) {
        auto* d = static_cast<const MapDelta*>(obj);
        nb::dict result;

        nb::list added_list;
        for (auto slot : d->added()) {
            added_list.append(nb::int_(slot));
        }
        result["added"] = added_list;

        nb::list removed_list;
        for (auto slot : d->removed()) {
            removed_list.append(nb::int_(slot));
        }
        result["removed"] = removed_list;

        nb::list updated_list;
        for (auto slot : d->updated()) {
            updated_list.append(nb::int_(slot));
        }
        result["updated"] = updated_list;

        result["cleared"] = nb::bool_(d->was_cleared());
        return result;
    }

    static void from_python(void*, const nb::object&, const value::TypeMeta*) {
        // No-op - MapDelta is populated via SlotObserver
    }

    /// Get the operations vtable for MapDelta
    static const value::TypeOps* ops() {
        static const value::TypeOps map_delta_ops = {
            &construct,
            &destruct,
            &copy_assign,
            &move_assign,
            &move_construct,
            &equals,
            &to_string,
            &to_python,
            &from_python,
            nullptr,   // hash
            nullptr,   // less_than
            nullptr,   // size
            nullptr,   // get_at
            nullptr,   // set_at
            nullptr,   // get_field
            nullptr,   // set_field
            nullptr,   // contains
            nullptr,   // insert
            nullptr,   // erase
            nullptr,   // map_get
            nullptr,   // map_set
            nullptr,   // resize
            nullptr,   // clear
        };
        return &map_delta_ops;
    }
};

// ============================================================================
// BundleDeltaNav TypeOps
// ============================================================================

/**
 * @brief TypeOps implementation for BundleDeltaNav.
 *
 * BundleDeltaNav is used in the delta_value_ parallel Value structure for
 * TSB types that contain fields with delta tracking.
 */
struct BundleDeltaNavOps {
    static void construct(void* dst, const value::TypeMeta*) {
        new (dst) BundleDeltaNav();
    }

    static void destruct(void* obj, const value::TypeMeta*) {
        static_cast<BundleDeltaNav*>(obj)->~BundleDeltaNav();
    }

    static void copy_assign(void* dst, const void* src, const value::TypeMeta*) {
        auto* d = static_cast<BundleDeltaNav*>(dst);
        auto* s = static_cast<const BundleDeltaNav*>(src);
        d->last_cleared_time = s->last_cleared_time;
        d->children = s->children;
    }

    static void move_assign(void* dst, void* src, const value::TypeMeta*) {
        auto* d = static_cast<BundleDeltaNav*>(dst);
        auto* s = static_cast<BundleDeltaNav*>(src);
        d->last_cleared_time = s->last_cleared_time;
        d->children = std::move(s->children);
    }

    static void move_construct(void* dst, void* src, const value::TypeMeta*) {
        auto* s = static_cast<BundleDeltaNav*>(src);
        new (dst) BundleDeltaNav();
        auto* d = static_cast<BundleDeltaNav*>(dst);
        d->last_cleared_time = s->last_cleared_time;
        d->children = std::move(s->children);
    }

    static bool equals(const void* a, const void* b, const value::TypeMeta*) {
        auto* da = static_cast<const BundleDeltaNav*>(a);
        auto* db = static_cast<const BundleDeltaNav*>(b);
        return da->last_cleared_time == db->last_cleared_time &&
               da->children.size() == db->children.size();
    }

    static std::string to_string(const void* obj, const value::TypeMeta*) {
        auto* d = static_cast<const BundleDeltaNav*>(obj);
        return "BundleDeltaNav(children=" + std::to_string(d->children.size()) + ")";
    }

    static nb::object to_python(const void* obj, const value::TypeMeta*) {
        auto* d = static_cast<const BundleDeltaNav*>(obj);
        nb::dict result;
        result["type"] = "BundleDeltaNav";
        result["child_count"] = nb::int_(d->children.size());
        return result;
    }

    static void from_python(void*, const nb::object&, const value::TypeMeta*) {
        // No-op
    }

    /// Get the operations vtable for BundleDeltaNav
    static const value::TypeOps* ops() {
        static const value::TypeOps bundle_delta_nav_ops = {
            &construct,
            &destruct,
            &copy_assign,
            &move_assign,
            &move_construct,
            &equals,
            &to_string,
            &to_python,
            &from_python,
            nullptr,   // hash
            nullptr,   // less_than
            nullptr,   // size
            nullptr,   // get_at
            nullptr,   // set_at
            nullptr,   // get_field
            nullptr,   // set_field
            nullptr,   // contains
            nullptr,   // insert
            nullptr,   // erase
            nullptr,   // map_get
            nullptr,   // map_set
            nullptr,   // resize
            nullptr,   // clear
        };
        return &bundle_delta_nav_ops;
    }
};

// ============================================================================
// ListDeltaNav TypeOps
// ============================================================================

/**
 * @brief TypeOps implementation for ListDeltaNav.
 *
 * ListDeltaNav is used in the delta_value_ parallel Value structure for
 * TSL types that contain elements with delta tracking.
 */
struct ListDeltaNavOps {
    static void construct(void* dst, const value::TypeMeta*) {
        new (dst) ListDeltaNav();
    }

    static void destruct(void* obj, const value::TypeMeta*) {
        static_cast<ListDeltaNav*>(obj)->~ListDeltaNav();
    }

    static void copy_assign(void* dst, const void* src, const value::TypeMeta*) {
        auto* d = static_cast<ListDeltaNav*>(dst);
        auto* s = static_cast<const ListDeltaNav*>(src);
        d->last_cleared_time = s->last_cleared_time;
        d->children = s->children;
    }

    static void move_assign(void* dst, void* src, const value::TypeMeta*) {
        auto* d = static_cast<ListDeltaNav*>(dst);
        auto* s = static_cast<ListDeltaNav*>(src);
        d->last_cleared_time = s->last_cleared_time;
        d->children = std::move(s->children);
    }

    static void move_construct(void* dst, void* src, const value::TypeMeta*) {
        auto* s = static_cast<ListDeltaNav*>(src);
        new (dst) ListDeltaNav();
        auto* d = static_cast<ListDeltaNav*>(dst);
        d->last_cleared_time = s->last_cleared_time;
        d->children = std::move(s->children);
    }

    static bool equals(const void* a, const void* b, const value::TypeMeta*) {
        auto* da = static_cast<const ListDeltaNav*>(a);
        auto* db = static_cast<const ListDeltaNav*>(b);
        return da->last_cleared_time == db->last_cleared_time &&
               da->children.size() == db->children.size();
    }

    static std::string to_string(const void* obj, const value::TypeMeta*) {
        auto* d = static_cast<const ListDeltaNav*>(obj);
        return "ListDeltaNav(children=" + std::to_string(d->children.size()) + ")";
    }

    static nb::object to_python(const void* obj, const value::TypeMeta*) {
        auto* d = static_cast<const ListDeltaNav*>(obj);
        nb::dict result;
        result["type"] = "ListDeltaNav";
        result["child_count"] = nb::int_(d->children.size());
        return result;
    }

    static void from_python(void*, const nb::object&, const value::TypeMeta*) {
        // No-op
    }

    /// Get the operations vtable for ListDeltaNav
    static const value::TypeOps* ops() {
        static const value::TypeOps list_delta_nav_ops = {
            &construct,
            &destruct,
            &copy_assign,
            &move_assign,
            &move_construct,
            &equals,
            &to_string,
            &to_python,
            &from_python,
            nullptr,   // hash
            nullptr,   // less_than
            nullptr,   // size
            nullptr,   // get_at
            nullptr,   // set_at
            nullptr,   // get_field
            nullptr,   // set_field
            nullptr,   // contains
            nullptr,   // insert
            nullptr,   // erase
            nullptr,   // map_get
            nullptr,   // map_set
            nullptr,   // resize
            nullptr,   // clear
        };
        return &list_delta_nav_ops;
    }
};

} // namespace hgraph
