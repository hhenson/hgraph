#pragma once

/**
 * @file link_target.h
 * @brief LinkTarget - Storage for link binding targets with time-accounting notification.
 *
 * LinkTarget stores the information needed to redirect navigation to a
 * target TSView when a position is bound (linked). It contains all the
 * ViewData fields except ShortPath (which is not needed for link following).
 *
 * LinkTarget also implements the time-accounting notification chain:
 * when a bound target changes, LinkTarget stamps modification time at
 * its level and propagates up through parent levels. This is one of
 * two independent notification chains (the other being node-scheduling
 * via ActiveNotifier).
 *
 * When a TSL or TSD is bound to a target, the LinkTarget stores enough
 * information to reconstruct the target's ViewData during navigation.
 */

#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/util/date_time.h>

#include <nanobind/nanobind.h>

#include <new>
#include <string>

namespace nb = nanobind;

namespace hgraph {

// Forward declarations
struct ts_ops;
struct ViewData;
class TSInput;

/**
 * @brief Storage for link target information with time-accounting notification.
 *
 * LinkTarget stores the essential ViewData fields needed to follow a link:
 * - Data pointers (value, time, observer, delta, link)
 * - Operations table and metadata
 *
 * Additionally, LinkTarget implements Notifiable for the time-accounting chain:
 * - owner_time_ptr: pointer to this level's time slot in the INPUT's TSValue
 * - parent_link: pointer to parent level's LinkTarget for upward propagation
 * - ActiveNotifier: embedded wrapper for the node-scheduling chain
 *
 * Two notification chains on the same target observer:
 * 1. Time-accounting (LinkTarget::notify): stamps times up through parent hierarchy
 * 2. Node-scheduling (ActiveNotifier::notify): schedules owning node for evaluation
 *
 * Memory management: The pointers in LinkTarget reference the target TSValue's
 * storage. The caller must ensure the target TSValue remains alive while the
 * link is active. The structural fields (owner_time_ptr, parent_link,
 * active_notifier) reference the INPUT's own storage and are NOT copied
 * by store_to_link_target.
 */
struct LinkTarget : public Notifiable {

    /**
     * @brief Embedded notifier for node-scheduling chain.
     *
     * Each LinkTarget has its own ActiveNotifier. When set_active is called,
     * the ActiveNotifier is subscribed to the target's observer list.
     * The guard `owning_input == nullptr` prevents double-subscription
     * when set_active is called at multiple composite levels.
     */
    struct ActiveNotifier : public Notifiable {
        TSInput* owning_input{nullptr};
        void notify(engine_time_t et) override;
    };

    // ========== Target-data fields (copied by store_to_link_target) ==========

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

    // ========== Structural fields (NOT copied by store_to_link_target) ==========

    /**
     * @brief Pointer to this level's time slot in the INPUT's TSValue::time_.
     * Set at bind time. Used by notify() to stamp modification time.
     */
    engine_time_t* owner_time_ptr{nullptr};

    /**
     * @brief Parent level's LinkTarget for upward time propagation.
     * nullptr at root level.
     */
    LinkTarget* parent_link{nullptr};

    /**
     * @brief Deduplication guard for time-accounting notifications.
     */
    engine_time_t last_notify_time{MIN_DT};

    /**
     * @brief Embedded node-scheduling notifier.
     */
    ActiveNotifier active_notifier;

    // ========== Construction ==========

    /**
     * @brief Default constructor - creates unlinked target.
     */
    LinkTarget() noexcept = default;

    // Virtual destructor from Notifiable base
    ~LinkTarget() override = default;

    /**
     * @brief Check if this link target is active.
     */
    [[nodiscard]] bool valid() const noexcept {
        return is_linked && ops != nullptr && value_data != nullptr;
    }

    /**
     * @brief Clear all fields (unbind).
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
        owner_time_ptr = nullptr;
        parent_link = nullptr;
        last_notify_time = MIN_DT;
        active_notifier.owning_input = nullptr;
    }

    // ========== Notifiable interface (time-accounting chain) ==========

    /**
     * @brief Time-accounting notification: stamp time and propagate up.
     *
     * Called when the bound target changes. Stamps owner_time_ptr with
     * the modification time and propagates to parent_link.
     * Does NOT schedule nodes - that's ActiveNotifier's job.
     */
    void notify(engine_time_t et) override {
        if (last_notify_time == et) return;  // dedup
        last_notify_time = et;
        if (owner_time_ptr) *owner_time_ptr = et;
        if (parent_link) parent_link->notify(et);
    }
};

/**
 * @brief TypeOps implementation for LinkTarget.
 *
 * Provides the TypeOps interface for LinkTarget so it can be stored
 * in Value structures as part of the link schema.
 *
 * IMPORTANT: copy_assign and move_assign only copy target-data fields,
 * NOT structural fields (owner_time_ptr, parent_link, active_notifier).
 * This is because structural fields belong to the INPUT's own hierarchy,
 * while target-data fields reference the bound OUTPUT.
 */
struct LinkTargetOps {
    static void construct(void* dst, const value::TypeMeta*) {
        new (dst) LinkTarget();
    }

    static void destruct(void* obj, const value::TypeMeta*) {
        static_cast<LinkTarget*>(obj)->~LinkTarget();
    }

    static void copy_assign(void* dst, const void* src, const value::TypeMeta*) {
        auto* d = static_cast<LinkTarget*>(dst);
        const auto* s = static_cast<const LinkTarget*>(src);
        // Only copy target-data fields, NOT structural fields
        d->is_linked = s->is_linked;
        d->value_data = s->value_data;
        d->time_data = s->time_data;
        d->observer_data = s->observer_data;
        d->delta_data = s->delta_data;
        d->link_data = s->link_data;
        d->ops = s->ops;
        d->meta = s->meta;
    }

    static void move_assign(void* dst, void* src, const value::TypeMeta*) {
        auto* d = static_cast<LinkTarget*>(dst);
        auto* s = static_cast<LinkTarget*>(src);
        // Only move target-data fields, NOT structural fields
        d->is_linked = s->is_linked;
        d->value_data = s->value_data;
        d->time_data = s->time_data;
        d->observer_data = s->observer_data;
        d->delta_data = s->delta_data;
        d->link_data = s->link_data;
        d->ops = s->ops;
        d->meta = s->meta;
        // Clear source target-data
        s->is_linked = false;
        s->value_data = nullptr;
        s->time_data = nullptr;
        s->observer_data = nullptr;
        s->delta_data = nullptr;
        s->link_data = nullptr;
        s->ops = nullptr;
        s->meta = nullptr;
    }

    static void move_construct(void* dst, void* src, const value::TypeMeta*) {
        new (dst) LinkTarget();
        move_assign(dst, src, nullptr);
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
