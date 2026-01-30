#pragma once

/**
 * @file ref_link.h
 * @brief REFLink - Link that dereferences a REF source.
 *
 * REFLink is used when an alternative needs to dereference a REF (REF → TS conversion).
 * It manages two subscriptions:
 * 1. To the REF source (for rebind notifications)
 * 2. To the current dereferenced target (for value notifications)
 *
 * @see design/04_LINKS_AND_BINDING.md §REFLink
 */

#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/notifiable.h>

namespace hgraph {

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
 * Usage:
 * @code
 * REFLink ref_link;
 *
 * // Bind to a REF source
 * ref_link.bind_to_ref(ref_source_view);
 *
 * // Access the dereferenced target
 * TSView target_view = ref_link.target_view(current_time);
 *
 * // Check modification (includes sampled flag)
 * if (ref_link.modified(current_time)) {
 *     // Either REF changed OR target changed
 * }
 * @endcode
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
     * Unsubscribes from REF source and target.
     */
    void unbind();

    /**
     * @brief Check if bound to a REF source.
     */
    [[nodiscard]] bool is_bound() const noexcept { return ref_source_bound_; }

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
     * Rebinds to the new target pointed to by the TSReference.
     *
     * @param et The time at which the modification occurred
     */
    void notify(engine_time_t et) override;

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
};

} // namespace hgraph
