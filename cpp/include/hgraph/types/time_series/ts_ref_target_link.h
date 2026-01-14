#pragma once

/**
 * @file ts_ref_target_link.h
 * @brief TSRefTargetLink - Extended link for REF->TS binding with two-channel architecture.
 *
 * Also defines LinkStorage variant for zero-overhead link abstraction.
 *
 * TSRefTargetLink is used when a REF output binds to a non-REF input. It maintains:
 * 1. Control channel: Always-active subscription to REF output's overlay
 * 2. Data channel (_target_link): User-controlled subscription to resolved target
 *
 * When the REF output is modified, TSRefTargetLink's notify() is called, which:
 * - Reads the TimeSeriesReference value from the REF output
 * - Resolves it to get the target TSValue
 * - Rebinds _target_link to the new target
 * - Notifies the owning node
 *
 * This enables zero-overhead for non-REF bindings while supporting dynamic
 * rebinding when REF is involved.
 *
 * See: ts_design_docs/Phase6_75_REF_Type_Handling_DESIGN.md Section 14
 */

#include <hgraph/types/time_series/ts_link.h>
#include <hgraph/types/time_series/ts_path.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/notifiable.h>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

namespace hgraph {

// Forward declarations
struct TSValue;
struct TimeSeriesReferenceOutput;
struct TSOverlayStorage;

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
 * TSRefTargetLink implements Notifiable and subscribes directly to the REF
 * output's overlay (control channel). When notified:
 * 1. Reads the TimeSeriesReference from the REF output
 * 2. Resolves it to get the target TSValue
 * 3. Rebinds _target_link (data channel) to the new target
 * 4. Notifies the owning node
 *
 * This is ONLY used when binding to a REF output that resolves to a non-REF target.
 * For all other bindings (TS->TS, TS->REF, REF->REF), use TSLink directly.
 *
 * Size: ~96 bytes (TSLink + subscription state + delta storage pointer)
 */
struct TSRefTargetLink : Notifiable {
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

    ~TSRefTargetLink() override;

    // ========== Node Association ==========

    /**
     * @brief Set the owning node (for notification delegation).
     * @param node The node to notify on modifications
     */
    void set_node(Node* node) noexcept;

    /**
     * @brief Get the owning node.
     */
    [[nodiscard]] Node* node() const noexcept { return _node; }

    // ========== REF Binding (Control Channel) ==========

    /**
     * @brief Bind to a REF output.
     *
     * This sets up the control channel (always-active subscription to REF output's overlay).
     * When the REF output is modified, this TSRefTargetLink's notify() will be called,
     * which resolves the TimeSeriesReference and rebinds the target.
     *
     * @param ref_output The REF output to observe
     * @param field_index Optional field index within a parent bundle (default -1 for direct REF)
     */
    void bind_ref(const TSValue* ref_output, int field_index = -1);

    /**
     * @brief Unbind from REF output.
     *
     * Unsubscribes from both channels and clears state.
     */
    void unbind();

    /**
     * @brief Check if bound to a REF output.
     */
    [[nodiscard]] bool bound() const noexcept { return _ref_output != nullptr; }

    /**
     * @brief Get the REF output being observed (control channel).
     */
    [[nodiscard]] const TSValue* ref_output() const noexcept { return _ref_output; }

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

    // ========== Notifiable Implementation ==========

    /**
     * @brief Called when the REF output is modified.
     *
     * This is the core of the bottom-up notification pattern:
     * 1. Read the TimeSeriesReference from the REF output
     * 2. Resolve it to get the target TSValue
     * 3. Rebind _target_link to the new target
     * 4. If active, ensure _target_link is subscribed to new target
     * 5. Notify the owning node
     *
     * @param time The engine time of the modification
     */
    void notify(engine_time_t time) override;

    // ========== Subscription Control (User-Facing) ==========

    /**
     * @brief Make data channel active (user-controlled).
     *
     * Note: Control channel (REF subscription) is always active and not
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
     */
    [[nodiscard]] engine_time_t last_modified_time() const;

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
     */
    [[nodiscard]] bool has_rebind_delta() const noexcept {
        return _rebind_delta != nullptr && _rebind_delta->has_delta();
    }

    /**
     * @brief Get the rebind delta storage (for specialized inputs).
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
     */
    void clear_rebind_delta() noexcept {
        if (_rebind_delta) {
            _rebind_delta->clear();
        }
    }

    // ========== Target Link Access ==========

    /**
     * @brief Get the target link for configuration.
     */
    [[nodiscard]] TSLink& target_link() noexcept { return _target_link; }

    /**
     * @brief Get the target link's sample time (when last rebound).
     */
    [[nodiscard]] engine_time_t target_sample_time() const noexcept { return _target_link.sample_time(); }

    /**
     * @brief Get the previous target output (from before rebind).
     */
    [[nodiscard]] const TSValue* prev_target_output() const noexcept { return _prev_target_output; }

    /**
     * @brief Clear the previous target reference.
     */
    void clear_prev_target() noexcept { _prev_target_output = nullptr; }

    /**
     * @brief Get the expected element type for auto-dereferenced REF inputs.
     *
     * This is the type that the input expects to receive (e.g., TS[int] when the
     * REF output is REF[TS[int]]). Used by TSBView::field() to create a view
     * with the correct metadata during initialization before the target is bound.
     */
    [[nodiscard]] const TSMeta* expected_element_meta() const noexcept { return _expected_element_meta; }

private:
    // ========== Control Channel (REF subscription) ==========
    const TSValue* _ref_output{nullptr};
    TSOverlayStorage* _ref_overlay{nullptr};
    bool _ref_subscribed{false};
    int _field_index{-1};  // Field index within parent bundle (-1 for direct REF binding)

    // ========== Data Channel (target subscription) ==========
    TSLink _target_link;

    // ========== Node ==========
    Node* _node{nullptr};

    // ========== Lazy-allocated delta storage ==========
    std::unique_ptr<RebindDelta> _rebind_delta;

    // ========== Element-based binding support ==========
    const TSValue* _target_container{nullptr};
    int _target_elem_index{-1};

    // ========== Previous target for delta computation ==========
    const TSValue* _prev_target_output{nullptr};

    // ========== Notification deduplication ==========
    engine_time_t _notify_time{MIN_DT};

    // ========== Expected element type (for view when no target bound) ==========
    const TSMeta* _expected_element_meta{nullptr};

    // ========== Helpers ==========

    /**
     * @brief Subscribe to REF output's overlay.
     */
    void subscribe_ref();

    /**
     * @brief Unsubscribe from REF output's overlay.
     */
    void unsubscribe_ref();

    /**
     * @brief Rebind data channel to a new target.
     */
    void rebind_target(const TSValue* new_target, engine_time_t time);

    /**
     * @brief Rebind data channel to an element within a container.
     */
    void rebind_target_element(const TSValue* container, size_t elem_index, engine_time_t time);

    /**
     * @brief Ensure delta storage is allocated.
     */
    void ensure_delta_storage();

    /**
     * @brief Compute delta between old and new targets.
     */
    void compute_delta(const TSValue* old_target, const TSValue* new_target);

    /**
     * @brief Check if the owning node's graph is stopping.
     */
    [[nodiscard]] bool is_graph_stopping() const;
};

// ============================================================================
// LinkStorage - Type-erased link storage with zero-overhead for non-REF
// ============================================================================

/**
 * @brief Type-erased link storage using variant.
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
