#pragma once

/**
 * @file ref_link.h
 * @brief REFLink - Link that dereferences a REF source.
 *
 * REFLink is used when an alternative needs to dereference a REF (REF -> TS conversion).
 * It manages two subscriptions:
 * 1. To the REF source (for rebind notifications)
 * 2. To the current dereferenced target (for value notifications)
 *
 * REFLink also participates in the dual notification chain:
 * - Time-accounting: stamps modification times up through parent hierarchy
 * - Node-scheduling: schedules owning node via ActiveNotifier
 *
 * @see design/04_LINKS_AND_BINDING.md
 */

#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/notifiable.h>

#include <new>
#include <string>

namespace hgraph {

// Forward declaration
struct TSMeta;

/**
 * @brief Link that dereferences a REF source.
 *
 * REFLink manages two subscriptions:
 * 1. To the REF source (for rebind notifications when the TSReference changes)
 * 2. To the current dereferenced target (for value notifications)
 *
 * When the REF source changes (new TSReference value), REFLink:
 * 1. Unbinds from old target (unsubscribes)
 * 2. Reads new TSReference from ref_source
 * 3. Resolves and binds to new target
 *
 * The "sampled" flag is set when the REF changes - even if the new target
 * wasn't modified at current time, the view reports modified=true because
 * the data source changed.
 *
 * REFLink also participates in the dual notification chain:
 * - Time-accounting (REFLink::notify): stamps owner_time_ptr and propagates up
 * - Node-scheduling (active_notifier_): subscribed when set_active is called
 */
class REFLink : public Notifiable {
public:
    // ========== Construction ==========

    /**
     * @brief Default constructor - creates unbound REFLink.
     */
    REFLink() noexcept = default;

    /**
     * @brief Construct and bind to a REF source.
     *
     * @param ref_source View of the REF source to dereference
     * @param current_time Current engine time
     */
    REFLink(TSView ref_source, engine_time_t current_time);

    // Non-copyable due to subscriptions
    REFLink(const REFLink&) = delete;
    REFLink& operator=(const REFLink&) = delete;

    // Movable
    REFLink(REFLink&& other) noexcept;
    REFLink& operator=(REFLink&& other) noexcept;

    ~REFLink() override;

    // ========== Binding ==========

    /**
     * @brief Bind to a REF source.
     *
     * Subscribes to the REF source for change notifications.
     * Reads the current TSReference and binds to the target.
     *
     * @param ref_source View of the REF source to dereference
     * @param current_time Current engine time
     */
    void bind_to_ref(TSView ref_source, engine_time_t current_time);

    /**
     * @brief Unbind from everything.
     *
     * Unsubscribes from REF source and target (both time-accounting
     * and node-scheduling chains).
     */
    void unbind();

    /**
     * @brief Check if bound to a REF source.
     */
    [[nodiscard]] bool is_bound() const noexcept { return ref_source_bound_; }

    /**
     * @brief Get the dereferenced type meta (the type inside the REF).
     *
     * When bound to a REF source, returns element_ts from the REF source's meta.
     * This is useful when the target hasn't resolved yet but we need to know
     * the expected dereferenced type.
     *
     * @return Dereferenced TSMeta, or nullptr if not bound to a REF
     */
    [[nodiscard]] const TSMeta* dereferenced_meta() const noexcept;

    // ========== Target Access ==========

    /**
     * @brief Get view of the current dereferenced target.
     *
     * @param current_time Current engine time
     * @return TSView of the target, or invalid TSView if unbound
     */
    [[nodiscard]] TSView target_view(engine_time_t current_time) const;

    /**
     * @brief Get the LinkTarget for the current target.
     */
    [[nodiscard]] const LinkTarget& target() const noexcept { return target_; }

    // ========== Modification Tracking ==========

    /**
     * @brief Check if modified at current time.
     *
     * Returns true if:
     * - REF source changed (reference changed - sampled semantics)
     * - OR target value changed
     *
     * When REF changes, the result is "sampled" even if the new target
     * wasn't modified at current time.
     *
     * @param current_time Current engine time
     * @return true if modified
     */
    [[nodiscard]] bool modified(engine_time_t current_time) const;

    /**
     * @brief Check if valid (has been set).
     */
    [[nodiscard]] bool valid() const;

    /**
     * @brief Get the last time the REF source was modified.
     *
     * Used for sampled flag calculation. Returns the REF source's
     * last_modified_time() rather than storing a separate timestamp.
     */
    [[nodiscard]] engine_time_t last_rebind_time() const noexcept;

    // ========== Notifiable Interface ==========

    /**
     * @brief Called when REF source changes.
     *
     * Performs time-accounting (stamp + propagate) and rebinds to new target.
     *
     * @param et The time at which the modification occurred
     */
    void notify(engine_time_t et) override;

    // ========== Time-Accounting Chain Access ==========

    /**
     * @brief Set the owner time pointer for time-accounting.
     */
    void set_owner_time_ptr(engine_time_t* ptr) { owner_time_ptr_ = ptr; }

    /**
     * @brief Set the parent link for upward time propagation.
     */
    void set_parent_link(LinkTarget* parent) { parent_link_ = parent; }

    /**
     * @brief Get the embedded ActiveNotifier for node-scheduling subscription.
     */
    LinkTarget::ActiveNotifier& active_notifier() { return active_notifier_; }

private:
    /**
     * @brief Rebind target based on current TSReference.
     *
     * @param current_time Current engine time
     */
    void rebind_target(engine_time_t current_time);

    // ========== Member Variables ==========

    LinkTarget target_;                     ///< Current dereferenced target
    ViewData ref_source_view_data_;         ///< ViewData for the REF source
    bool ref_source_bound_{false};          ///< Whether bound to a REF source

    // Time-accounting chain (structural, owned by this REFLink)
    engine_time_t* owner_time_ptr_{nullptr};   ///< Ptr to this level's time slot in INPUT's TSValue
    LinkTarget* parent_link_{nullptr};          ///< Parent level's LinkTarget (nullptr at root)
    engine_time_t last_notify_time_{MIN_DT};   ///< Dedup guard

    // Node-scheduling wrapper
    LinkTarget::ActiveNotifier active_notifier_;  ///< Embedded notifier for set_active
};

/**
 * @brief TypeOps implementation for REFLink.
 *
 * Provides the TypeOps interface for REFLink so it can be stored
 * in Value structures as part of the link schema.
 *
 * REFLink is stored inline in the link schema and provides:
 * - Simple link functionality (like LinkTarget) when not bound to a REF
 * - Full REF->TS dereferencing when bound to a REF source
 */
struct REFLinkOps {
    static void construct(void* dst, const value::TypeMeta*) {
        new (dst) REFLink();
    }

    static void destruct(void* obj, const value::TypeMeta*) {
        static_cast<REFLink*>(obj)->~REFLink();
    }

    static void copy_assign(void* dst, const void* src, const value::TypeMeta*) {
        // REFLink is non-copyable due to subscriptions
        // For schema operations, just default-construct
        static_cast<REFLink*>(dst)->~REFLink();
        new (dst) REFLink();
        (void)src;
    }

    static void move_assign(void* dst, void* src, const value::TypeMeta*) {
        *static_cast<REFLink*>(dst) = std::move(*static_cast<REFLink*>(src));
    }

    static void move_construct(void* dst, void* src, const value::TypeMeta*) {
        new (dst) REFLink(std::move(*static_cast<REFLink*>(src)));
    }

    static bool equals(const void* a, const void* b, const value::TypeMeta*) {
        const auto* rl_a = static_cast<const REFLink*>(a);
        const auto* rl_b = static_cast<const REFLink*>(b);
        return rl_a->is_bound() == rl_b->is_bound() &&
               rl_a->target().is_linked == rl_b->target().is_linked;
    }

    static std::string to_string(const void* obj, const value::TypeMeta*) {
        const auto* rl = static_cast<const REFLink*>(obj);
        return "REFLink(bound=" + std::string(rl->is_bound() ? "true" : "false") +
               ", linked=" + std::string(rl->target().is_linked ? "true" : "false") + ")";
    }

    static nb::object to_python(const void* obj, const value::TypeMeta*) {
        const auto* rl = static_cast<const REFLink*>(obj);
        return nb::make_tuple(rl->is_bound(), rl->target().is_linked);
    }

    static void from_python(void*, const nb::object&, const value::TypeMeta*) {
        // REFLink cannot be set from Python - it's managed internally
        throw std::runtime_error("REFLink cannot be set from Python");
    }

    /// Get the operations vtable for REFLink
    static const value::TypeOps* ops() {
        static const value::TypeOps ref_link_ops = {
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
        return &ref_link_ops;
    }
};

} // namespace hgraph
