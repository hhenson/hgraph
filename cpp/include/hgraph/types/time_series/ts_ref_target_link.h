#pragma once

/**
 * @file ts_ref_target_link.h
 * @brief TSRefTargetLink - Extended link for REF->TS binding with two-channel architecture.
 *
 * Also defines LinkStorage variant for zero-overhead link abstraction.
 *
 * TSRefTargetLink is used when a REF output binds to a non-REF input. It maintains:
 * 1. Control channel (_ref_link): Always-active subscription to REF output
 * 2. Data channel (_target_link): User-controlled subscription to resolved target
 *
 * This enables zero-overhead for non-REF bindings while supporting dynamic
 * rebinding when REF is involved.
 *
 * See: ts_design_docs/Phase6_75_REF_Type_Handling_DESIGN.md Section 14
 */

#include <hgraph/types/time_series/ts_link.h>
#include <hgraph/types/time_series/ts_path.h>
#include <hgraph/types/time_series/ts_view.h>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

namespace hgraph {

// Forward declarations
struct TSValue;
struct TimeSeriesReferenceOutput;

/**
 * @brief Delta storage for collection rebind.
 *
 * Allocated lazily only when rebinding occurs. Contains precomputed
 * delta values to avoid dangling pointer issues with type-erased storage.
 *
 * When a REF->TS binding switches targets, the delta between old and new
 * values is computed eagerly and stored here. This is cleared after the
 * evaluation cycle.
 */
struct RebindDelta {
    // For TSS: sets of added/removed values (type-erased as bytes)
    std::optional<std::vector<uint8_t>> added_values;
    std::optional<std::vector<uint8_t>> removed_values;

    // For TSD: keys of added/removed entries (type-erased as bytes)
    std::optional<std::vector<uint8_t>> added_keys;
    std::optional<std::vector<uint8_t>> removed_keys;

    // For TSL/TSB: changed indices
    std::optional<std::vector<size_t>> changed_indices;

    /**
     * @brief Check if any delta is stored.
     */
    [[nodiscard]] bool has_delta() const noexcept {
        return added_values.has_value() || removed_values.has_value() ||
               added_keys.has_value() || removed_keys.has_value() ||
               changed_indices.has_value();
    }

    /**
     * @brief Clear all delta storage.
     */
    void clear() noexcept {
        added_values.reset();
        removed_values.reset();
        added_keys.reset();
        removed_keys.reset();
        changed_indices.reset();
    }
};

/**
 * @brief Extended link for REF->TS binding with two-channel architecture.
 *
 * TSRefTargetLink maintains two subscriptions:
 * 1. Control channel (_ref_link): Always-active subscription to REF output
 * 2. Data channel (_target_link): User-controlled subscription to resolved target
 *
 * This is ONLY used when binding to a REF output that resolves to a non-REF target.
 * For all other bindings (TS->TS, TS->REF, REF->REF), use TSLink directly.
 *
 * Size: ~112 bytes (two TSLinks + delta storage pointer + ref output pointer)
 *
 * Usage:
 * @code
 * TSRefTargetLink link;
 * link.set_node(owning_node);
 *
 * // Bind to a REF output (sets up control channel)
 * link.bind_ref(ref_output, current_time);
 *
 * // User controls data channel
 * link.make_active();  // Subscribes target_link
 *
 * // Get view into resolved target data
 * TSView view = link.view();
 * float price = view.as<float>();
 *
 * // When REF output ticks, on_ref_modified() is called internally
 * // This may rebind target_link to a new resolved target
 *
 * // Clear delta after cycle
 * link.clear_rebind_delta();
 * @endcode
 */
struct TSRefTargetLink {
    // ========== Construction ==========

    TSRefTargetLink() noexcept = default;

    /**
     * @brief Construct with owning node.
     * @param node The node to notify on modifications
     */
    explicit TSRefTargetLink(Node* node) noexcept;

    // Non-copyable (registered as observer)
    TSRefTargetLink(const TSRefTargetLink&) = delete;
    TSRefTargetLink& operator=(const TSRefTargetLink&) = delete;

    // Movable
    TSRefTargetLink(TSRefTargetLink&& other) noexcept;
    TSRefTargetLink& operator=(TSRefTargetLink&& other) noexcept;

    ~TSRefTargetLink();

    // ========== Node Association ==========

    /**
     * @brief Set the owning node (for notification delegation).
     * @param node The node to notify on modifications
     */
    void set_node(Node* node) noexcept;

    /**
     * @brief Get the owning node.
     */
    [[nodiscard]] Node* node() const noexcept { return _ref_link.node(); }

    // ========== REF Binding ==========

    /**
     * @brief Bind to a REF output.
     *
     * This sets up the control channel (always-active subscription to REF)
     * and resolves the initial target for the data channel.
     *
     * @param ref_output The REF output to observe (its TSValue containing the path)
     * @param ref_output_ptr Pointer to the TimeSeriesReferenceOutput for observer registration
     * @param time Current engine time
     */
    void bind_ref(const TSValue* ref_output, TimeSeriesReferenceOutput* ref_output_ptr, engine_time_t time);

    /**
     * @brief Unbind from REF output.
     *
     * Unsubscribes from both channels and clears state.
     */
    void unbind();

    /**
     * @brief Check if bound to a REF output.
     */
    [[nodiscard]] bool bound() const noexcept { return _ref_link.bound(); }

    /**
     * @brief Get the REF output being observed (control channel).
     */
    [[nodiscard]] const TSValue* ref_output() const noexcept { return _ref_link.output(); }

    /**
     * @brief Get the resolved target output (data channel).
     *
     * For element-based bindings (TSL elements), returns the container.
     */
    [[nodiscard]] const TSValue* target_output() const noexcept {
        if (_target_elem_index >= 0) {
            return _target_container;
        }
        return _target_link.output();
    }

    /**
     * @brief Check if this is an element-based binding (into a container like TSL).
     */
    [[nodiscard]] bool is_element_binding() const noexcept { return _target_elem_index >= 0; }

    /**
     * @brief Get the element index for element-based bindings.
     * @return Element index, or -1 if not element-based
     */
    [[nodiscard]] int target_element_index() const noexcept { return _target_elem_index; }

    // ========== Target Management (Called by REF output) ==========

    /**
     * @brief Rebind data channel to a new target.
     *
     * This is the primary interface for REF→TS rebinding. Called by the
     * observer mechanism when the REF output's target changes.
     *
     * Computes delta eagerly if target changed, then updates binding.
     *
     * @param new_target New target TSValue (nullptr to unbind target)
     * @param time Current engine time
     */
    void rebind_target(const TSValue* new_target, engine_time_t time);

    /**
     * @brief Rebind data channel to an element within a container.
     *
     * Used when the REF points to an element in a container (like TSL) that
     * doesn't have its own TSValue. The view() method will navigate into
     * the container at the specified index.
     *
     * @param container The container TSValue
     * @param elem_index Element index within the container
     * @param time Current engine time
     */
    void rebind_target_element(const TSValue* container, size_t elem_index, engine_time_t time);

    // ========== Subscription Control (User-Facing) ==========

    /**
     * @brief Make data channel active (user-controlled).
     *
     * Note: Control channel (_ref_link) is always active and not
     * affected by this call.
     */
    void make_active();

    /**
     * @brief Make data channel passive (user-controlled).
     *
     * Control channel remains active.
     */
    void make_passive();

    /**
     * @brief Check if data channel is active.
     */
    [[nodiscard]] bool active() const noexcept { return _target_link.active(); }

    // ========== State Queries ==========

    /**
     * @brief Check if target is valid (has been set).
     */
    [[nodiscard]] bool valid() const;

    /**
     * @brief Modified at time - uses max of both channels.
     *
     * This ensures that rebinding shows as modified even if the
     * new target wasn't modified this tick.
     */
    [[nodiscard]] bool modified_at(engine_time_t time) const;

    /**
     * @brief Last modified time - max of ref and target times.
     *
     * This derived value ensures that rebinding naturally shows
     * as modified without explicit _sample_time tracking.
     */
    [[nodiscard]] engine_time_t last_modified_time() const {
        engine_time_t ref_time = _ref_link.last_modified_time();
        engine_time_t target_time = _target_link.last_modified_time();
        return std::max(ref_time, target_time);
    }

    // ========== View Access ==========

    /**
     * @brief Get view into resolved target data.
     *
     * Users see the target data, not the REF path.
     *
     * @return TSView into the target, or invalid view if no target
     */
    [[nodiscard]] TSView view() const;

    // ========== Delta Access ==========

    /**
     * @brief Check if there is a precomputed rebind delta.
     *
     * True when rebind occurred this cycle and delta was computed eagerly.
     */
    [[nodiscard]] bool has_rebind_delta() const noexcept {
        return _rebind_delta != nullptr && _rebind_delta->has_delta();
    }

    /**
     * @brief Get the rebind delta storage (for specialized inputs).
     *
     * Returns nullptr if no delta stored.
     */
    [[nodiscard]] RebindDelta* rebind_delta() noexcept {
        return _rebind_delta.get();
    }

    /**
     * @brief Get the rebind delta storage (const version).
     */
    [[nodiscard]] const RebindDelta* rebind_delta() const noexcept {
        return _rebind_delta.get();
    }

    /**
     * @brief Clear rebind delta after evaluation cycle.
     *
     * Should be called at the end of each cycle where rebinding occurred.
     */
    void clear_rebind_delta() noexcept {
        if (_rebind_delta) {
            _rebind_delta->clear();
        }
    }

    // ========== Notifiable Support ==========

    /**
     * @brief Get the ref link's notify interface for direct notification.
     *
     * Used when the REF output needs to notify this link directly.
     */
    [[nodiscard]] TSLink& ref_link() noexcept { return _ref_link; }

    /**
     * @brief Get the target link for setting properties.
     *
     * Used when the wiring needs to configure element index for TSL->TS binding.
     */
    [[nodiscard]] TSLink& target_link() noexcept { return _target_link; }

    /**
     * @brief Get the target link's sample time (when last rebound).
     *
     * Used to detect if a rebind occurred at a specific time.
     */
    [[nodiscard]] engine_time_t target_sample_time() const noexcept { return _target_link.sample_time(); }

private:
    // Control channel: always-active to REF output
    TSLink _ref_link;

    // Data channel: user-controlled to resolved target
    TSLink _target_link;

    // Lazy-allocated delta storage (only when rebinding)
    std::unique_ptr<RebindDelta> _rebind_delta;

    // Reference to the REF output for observer cleanup
    TimeSeriesReferenceOutput* _ref_output_ptr{nullptr};

    // Element-based binding support (for TSL elements that don't have separate TSValues)
    const TSValue* _target_container{nullptr};
    int _target_elem_index{-1};

    // Previous target tracking for delta computation during rebind
    // Stored during rebind_target() and cleared at end of evaluation cycle
    const TSValue* _prev_target_output{nullptr};

public:
    /**
     * @brief Get the previous target output (from before rebind).
     *
     * Used for delta computation when REF target changes. Returns the
     * target that was bound before the most recent rebind_target() call.
     *
     * @return Previous target TSValue, or nullptr if no rebind occurred
     */
    [[nodiscard]] const TSValue* prev_target_output() const noexcept { return _prev_target_output; }

    /**
     * @brief Clear the previous target reference.
     *
     * Should be called at the end of each evaluation cycle after delta
     * computation is complete.
     */
    void clear_prev_target() noexcept { _prev_target_output = nullptr; }

private:

    // ========== Helpers ==========

    /**
     * @brief Ensure delta storage is allocated.
     */
    void ensure_delta_storage();

    /**
     * @brief Compute delta between old and new targets.
     *
     * Called during rebind_target() to eagerly compute collection deltas.
     * For scalar types, this is a no-op. For TSS/TSD/TSL/TSB, this computes
     * added/removed elements.
     *
     * @param old_target Previous target (may be nullptr)
     * @param new_target New target (may be nullptr)
     */
    void compute_delta(const TSValue* old_target, const TSValue* new_target);
};

// ============================================================================
// LinkStorage - Type-erased link storage with zero-overhead for non-REF
// ============================================================================

/**
 * @brief Type-erased link storage using variant.
 *
 * Uses std::variant for static dispatch - no virtual overhead.
 *
 * States:
 * - std::monostate: No link (unbound)
 * - std::unique_ptr<TSLink>: Standard non-REF binding
 * - std::unique_ptr<TSRefTargetLink>: REF->TS binding with two channels
 *
 * Usage:
 * @code
 * LinkStorage storage;
 *
 * // For non-REF output
 * storage = std::make_unique<TSLink>(node);
 * std::get<std::unique_ptr<TSLink>>(storage)->bind(output);
 *
 * // For REF output
 * storage = std::make_unique<TSRefTargetLink>(node);
 * std::get<std::unique_ptr<TSRefTargetLink>>(storage)->bind_ref(ref_output, ...);
 *
 * // Access via visit
 * std::visit([](auto& link) {
 *     if constexpr (!std::is_same_v<std::decay_t<decltype(link)>, std::monostate>) {
 *         if (link) link->make_active();
 *     }
 * }, storage);
 * @endcode
 */
using LinkStorage = std::variant<
    std::monostate,
    std::unique_ptr<TSLink>,
    std::unique_ptr<TSRefTargetLink>
>;

/**
 * @brief Helper to check if LinkStorage is bound (has a link).
 */
inline bool link_storage_bound(const LinkStorage& storage) {
    return std::visit([](const auto& link) -> bool {
        using T = std::decay_t<decltype(link)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            return false;
        } else {
            return link && link->bound();
        }
    }, storage);
}

/**
 * @brief Helper to get TSView from LinkStorage.
 */
inline TSView link_storage_view(const LinkStorage& storage) {
    return std::visit([](const auto& link) -> TSView {
        using T = std::decay_t<decltype(link)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            return TSView{};
        } else {
            return link ? link->view() : TSView{};
        }
    }, storage);
}

/**
 * @brief Helper to make LinkStorage active.
 */
inline void link_storage_make_active(LinkStorage& storage) {
    std::visit([](auto& link) {
        using T = std::decay_t<decltype(link)>;
        if constexpr (!std::is_same_v<T, std::monostate>) {
            if (link) link->make_active();
        }
    }, storage);
}

/**
 * @brief Helper to make LinkStorage passive.
 */
inline void link_storage_make_passive(LinkStorage& storage) {
    std::visit([](auto& link) {
        using T = std::decay_t<decltype(link)>;
        if constexpr (!std::is_same_v<T, std::monostate>) {
            if (link) link->make_passive();
        }
    }, storage);
}

/**
 * @brief Helper to get last modified time from LinkStorage.
 */
inline engine_time_t link_storage_last_modified_time(const LinkStorage& storage) {
    return std::visit([](const auto& link) -> engine_time_t {
        using T = std::decay_t<decltype(link)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            return MIN_DT;
        } else {
            return link ? link->last_modified_time() : MIN_DT;
        }
    }, storage);
}

/**
 * @brief Helper to check if LinkStorage was modified at time.
 */
inline bool link_storage_modified_at(const LinkStorage& storage, engine_time_t time) {
    return std::visit([time](const auto& link) -> bool {
        using T = std::decay_t<decltype(link)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            return false;
        } else {
            return link && link->modified_at(time);
        }
    }, storage);
}

/**
 * @brief Helper to check if LinkStorage is valid.
 */
inline bool link_storage_valid(const LinkStorage& storage) {
    return std::visit([](const auto& link) -> bool {
        using T = std::decay_t<decltype(link)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            return false;
        } else {
            return link && link->valid();
        }
    }, storage);
}

}  // namespace hgraph
