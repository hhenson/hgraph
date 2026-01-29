#pragma once

/**
 * @file link_target.h
 * @brief LinkTarget - Storage for link binding targets.
 *
 * LinkTarget stores the information needed to redirect navigation to a
 * target TSView when a position is bound (linked). It contains all the
 * ViewData fields except ShortPath (which is not needed for link following).
 *
 * When a TSL or TSD is bound to a target, the LinkTarget stores enough
 * information to reconstruct the target's ViewData during navigation.
 */

#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/value/type_meta.h>

#include <nanobind/nanobind.h>

#include <new>
#include <string>

namespace nb = nanobind;

namespace hgraph {

// Forward declarations
struct ts_ops;
struct ViewData;

/**
 * @brief Storage for link target information.
 *
 * LinkTarget stores the essential ViewData fields needed to follow a link:
 * - Data pointers (value, time, observer, delta, link)
 * - Operations table and metadata
 *
 * The ShortPath is not stored because it's not needed for link following.
 * Navigation creates a new path when returning a child TSView.
 *
 * Memory management: The pointers in LinkTarget reference the target TSValue's
 * storage. The caller must ensure the target TSValue remains alive while the
 * link is active.
 */
struct LinkTarget {
    /**
     * @brief Whether this LinkTarget is active (bound).
     */
    bool is_linked{false};

    /**
     * @brief Pointer to the target's value data.
     */
    void* value_data{nullptr};

    /**
     * @brief Pointer to the target's time data.
     */
    void* time_data{nullptr};

    /**
     * @brief Pointer to the target's observer data.
     */
    void* observer_data{nullptr};

    /**
     * @brief Pointer to the target's delta data.
     */
    void* delta_data{nullptr};

    /**
     * @brief Pointer to the target's link data.
     */
    void* link_data{nullptr};

    /**
     * @brief Target's operations vtable.
     */
    const ts_ops* ops{nullptr};

    /**
     * @brief Target's time-series metadata.
     */
    const TSMeta* meta{nullptr};

    // ========== Construction ==========

    /**
     * @brief Default constructor - creates unlinked target.
     */
    LinkTarget() noexcept = default;

    /**
     * @brief Check if this link target is active.
     */
    [[nodiscard]] bool valid() const noexcept {
        return is_linked && ops != nullptr && value_data != nullptr;
    }

    /**
     * @brief Clear the link target (unbind).
     */
    void clear() noexcept {
        is_linked = false;
        value_data = nullptr;
        time_data = nullptr;
        observer_data = nullptr;
        delta_data = nullptr;
        link_data = nullptr;
        ops = nullptr;
        meta = nullptr;
    }
};

/**
 * @brief TypeOps implementation for LinkTarget.
 *
 * Provides the TypeOps interface for LinkTarget so it can be stored
 * in Value structures as part of the link schema.
 */
struct LinkTargetOps {
    static void construct(void* dst, const value::TypeMeta*) {
        new (dst) LinkTarget();
    }

    static void destruct(void* obj, const value::TypeMeta*) {
        static_cast<LinkTarget*>(obj)->~LinkTarget();
    }

    static void copy_assign(void* dst, const void* src, const value::TypeMeta*) {
        *static_cast<LinkTarget*>(dst) = *static_cast<const LinkTarget*>(src);
    }

    static void move_assign(void* dst, void* src, const value::TypeMeta*) {
        *static_cast<LinkTarget*>(dst) = std::move(*static_cast<LinkTarget*>(src));
    }

    static void move_construct(void* dst, void* src, const value::TypeMeta*) {
        new (dst) LinkTarget(std::move(*static_cast<LinkTarget*>(src)));
    }

    static bool equals(const void* a, const void* b, const value::TypeMeta*) {
        const auto* lt_a = static_cast<const LinkTarget*>(a);
        const auto* lt_b = static_cast<const LinkTarget*>(b);
        return lt_a->is_linked == lt_b->is_linked &&
               lt_a->value_data == lt_b->value_data &&
               lt_a->ops == lt_b->ops;
    }

    static std::string to_string(const void* obj, const value::TypeMeta*) {
        const auto* lt = static_cast<const LinkTarget*>(obj);
        return "LinkTarget(is_linked=" + std::string(lt->is_linked ? "true" : "false") + ")";
    }

    static nb::object to_python(const void* obj, const value::TypeMeta*) {
        const auto* lt = static_cast<const LinkTarget*>(obj);
        return nb::make_tuple(lt->is_linked);
    }

    static void from_python(void*, const nb::object&, const value::TypeMeta*) {
        // LinkTarget cannot be set from Python - it's managed internally
        throw std::runtime_error("LinkTarget cannot be set from Python");
    }

    /// Get the operations vtable for LinkTarget
    static const value::TypeOps* ops() {
        static const value::TypeOps link_target_ops = {
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
        return &link_target_ops;
    }
};

} // namespace hgraph
