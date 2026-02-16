/**
 * @file ts_ops.cpp
 * @brief Kind-specific ts_ops implementations.
 *
 * This file provides the polymorphic operations vtables for each TSKind.
 * Each kind has its own ts_ops struct with appropriate implementations
 * based on the data layout for that kind.
 */

#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/ts_dict_view.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/set_delta.h>
#include <hgraph/types/time_series/map_delta.h>
#include <hgraph/types/value/map_storage.h>
#include <hgraph/types/value/set_storage.h>
#include <hgraph/types/value/cyclic_buffer_ops.h>
#include <hgraph/types/value/queue_ops.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>

#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <cstdio>

// Forward declarations to avoid including py_time_series.h / py_tss.h / py_ref.h
// (including those headers in this translation unit changes nanobind type resolution
// behavior and causes regressions in error_handling tests)
namespace hgraph {
    std::optional<ViewData> resolve_python_bound_reference(const TSReference* ts_ref, engine_time_t current_time);
}

namespace hgraph {

// ============================================================================
// Helper Functions
// ============================================================================

namespace {

// Create value::View from ViewData
inline value::View make_value_view(const ViewData& vd) {
    if (!vd.value_data || !vd.meta) return value::View{};
    return value::View(vd.value_data, vd.meta->value_type);
}

// Create link::View from ViewData (for checking/setting link flags)
inline value::View make_link_view(const ViewData& vd) {
    if (!vd.link_data || !vd.meta) return value::View{};
    auto* link_schema = TSMetaSchemaCache::instance().get_link_schema(vd.meta);
    if (!link_schema) return value::View{};
    return value::View(vd.link_data, link_schema);
}

inline value::View make_time_view(const ViewData& vd) {
    if (!vd.time_data || !vd.meta) return value::View{};
    auto* time_schema = TSMetaSchemaCache::instance().get_time_schema(vd.meta);
    if (!time_schema) return value::View{};
    return value::View(vd.time_data, time_schema);
}

inline value::View make_observer_view(const ViewData& vd) {
    if (!vd.observer_data || !vd.meta) return value::View{};
    auto* observer_schema = TSMetaSchemaCache::instance().get_observer_schema(vd.meta);
    if (!observer_schema) return value::View{};
    return value::View(vd.observer_data, observer_schema);
}

inline value::View make_delta_view(const ViewData& vd) {
    if (!vd.delta_data || !vd.meta) return value::View{};
    auto* delta_schema = TSMetaSchemaCache::instance().get_delta_value_schema(vd.meta);
    if (!delta_schema) return value::View{};
    return value::View(vd.delta_data, delta_schema);
}

// Get REFLink pointer from link_data (for TSOutput alternatives)
// Only valid when uses_link_target is false
inline REFLink* get_ref_link(void* link_data) {
    return link_data ? static_cast<REFLink*>(link_data) : nullptr;
}

inline const REFLink* get_ref_link(const void* link_data) {
    return link_data ? static_cast<const REFLink*>(link_data) : nullptr;
}

// Get LinkTarget pointer from link_data (for TSInput simple binding)
// Only valid when uses_link_target is true
inline LinkTarget* get_link_target(void* link_data) {
    return link_data ? static_cast<LinkTarget*>(link_data) : nullptr;
}

inline const LinkTarget* get_link_target(const void* link_data) {
    return link_data ? static_cast<const LinkTarget*>(link_data) : nullptr;
}

// Create ViewData from a REFLink's target
// The sampled parameter indicates whether this view was obtained through a modified REF
inline ViewData make_view_data_from_link(const REFLink& rl, const ShortPath& path, bool sampled = false) {
    const LinkTarget& lt = rl.target();
    ViewData vd;
    vd.path = path;
    vd.value_data = lt.value_data;
    vd.time_data = lt.time_data;
    vd.observer_data = lt.observer_data;
    vd.delta_data = lt.delta_data;
    vd.link_data = lt.link_data;
    vd.sampled = sampled;
    vd.ops = lt.ops;
    vd.meta = lt.meta;
    return vd;
}

// Create ViewData from a LinkTarget (for TSInput simple binding)
inline ViewData make_view_data_from_link_target(const LinkTarget& lt, const ShortPath& path) {
    ViewData vd;
    vd.path = path;
    vd.value_data = lt.value_data;
    vd.time_data = lt.time_data;
    vd.observer_data = lt.observer_data;
    vd.delta_data = lt.delta_data;
    vd.link_data = lt.link_data;
    vd.sampled = false;  // LinkTarget doesn't track sampled state
    vd.ops = lt.ops;
    vd.meta = lt.meta;
    return vd;
}

// Store ViewData into a LinkTarget (for TSInput simple binding)
inline void store_to_link_target(LinkTarget& lt, const ViewData& target) {
    lt.is_linked = true;
    lt.target_path = target.path;
    lt.value_data = target.value_data;
    lt.time_data = target.time_data;
    lt.observer_data = target.observer_data;
    lt.delta_data = target.delta_data;
    lt.link_data = target.link_data;
    lt.ops = target.ops;
    lt.meta = target.meta;
}

// Check if a REFLink was rebound at the given time (indicating sampled semantics)
inline bool is_ref_sampled(const REFLink& rl, engine_time_t current_time) {
    return rl.is_bound() && rl.last_rebind_time() >= current_time;
}

// Store ViewData into a REFLink's internal target (for simple link usage)
// This uses the REFLink as a simple link (like LinkTarget) without REF tracking
inline void store_link_target(REFLink& rl, const ViewData& target) {
    // Access the internal target through the public interface
    // Since REFLink doesn't expose direct target modification,
    // we need to use it differently. For simple binding without REF tracking,
    // we need to access the target_ member directly or have a setter.
    // For now, cast to access internal state (this is internal infrastructure code)
    LinkTarget& lt = const_cast<LinkTarget&>(rl.target());
    lt.is_linked = true;
    lt.value_data = target.value_data;
    lt.time_data = target.time_data;
    lt.observer_data = target.observer_data;
    lt.delta_data = target.delta_data;
    lt.link_data = target.link_data;
    lt.ops = target.ops;
    lt.meta = target.meta;
}

// Helper: Check if this view is linked via REFLink and get the REFLink
// Only call when uses_link_target is false.
// Only REF outputs have active REFLinks for delegation (REFLink stores the
// resolved target of a TSReference). For all other kinds, link_data may point
// to collection-level link storage (TSD/TSL/TSB) or a default-constructed
// REFLink (TS/TSS/TSW/SIGNAL), neither of which should be followed.
inline const REFLink* get_active_link(const ViewData& vd) {
    if (vd.uses_link_target) return nullptr;  // TSInput uses LinkTarget, not REFLink
    if (!vd.meta || vd.meta->kind != TSKind::REF) return nullptr;  // Only REF has active REFLinks
    auto* rl = get_ref_link(vd.link_data);
    if (rl && rl->target().valid()) {
        return rl;
    }
    return nullptr;
}

// Helper: Check if this view is linked via LinkTarget and get the LinkTarget
// Only call when uses_link_target is true
inline const LinkTarget* get_active_link_target(const ViewData& vd) {
    if (!vd.uses_link_target) return nullptr;  // Wrong type
    auto* lt = get_link_target(vd.link_data);
    return (lt && lt->valid()) ? lt : nullptr;
}

// ============================================================================
// Link Delegation Helpers
// ============================================================================
// These helpers extract the common "check link, delegate to target" pattern
// that repeats across all namespace implementations.

// Simple delegation: returns the target ViewData if linked, or nullopt if local.
// Used by all namespaces except scalar_ops (which needs REF resolution).
inline std::optional<ViewData> resolve_delegation_target(const ViewData& vd) {
    if (auto* lt = get_active_link_target(vd)) {
        return make_view_data_from_link_target(*lt, vd.path);
    }
    if (auto* rl = get_active_link(vd)) {
        return make_view_data_from_link(*rl, vd.path);
    }
    return std::nullopt;
}

// 3-state result for scalar_ops delegation with REF resolution.
// When a non-REF TSInput is bound to a REF output, we must resolve through the reference.
enum class DelegateResult { DELEGATED, NO_LINK, REF_UNRESOLVED };

struct DelegateTarget {
    DelegateResult result;
    ViewData target;  // valid only when result == DELEGATED
};

// Forward declaration: defined below after resolve_ref_link_target_from_vd
inline std::optional<ViewData> resolve_ref_link_target(const LinkTarget& lt, engine_time_t current_time);

// REF-resolving delegation: handles the case where a non-REF reader is bound to a REF output.
// Returns DELEGATED with resolved target, REF_UNRESOLVED if REF couldn't be resolved, or NO_LINK.
inline DelegateTarget resolve_delegation_target_with_ref(const ViewData& vd, engine_time_t time) {
    if (auto* lt = get_active_link_target(vd)) {
        if (lt->meta && lt->meta->kind == TSKind::REF &&
            (!vd.meta || vd.meta->kind != TSKind::REF)) {
            if (auto resolved = resolve_ref_link_target(*lt, time)) {
                return {DelegateResult::DELEGATED, *resolved};
            }
            return {DelegateResult::REF_UNRESOLVED, {}};
        }
        return {DelegateResult::DELEGATED, make_view_data_from_link_target(*lt, vd.path)};
    }
    if (auto* rl = get_active_link(vd)) {
        return {DelegateResult::DELEGATED, make_view_data_from_link(*rl, vd.path)};
    }
    return {DelegateResult::NO_LINK, {}};
}

// Helper: Resolve ViewData that points to REF data.
// Reads the TSReference value from the REF ViewData, resolves the ShortPath,
// and returns a ViewData pointing to the actual target.
// Returns nullopt if the REF can't be resolved.
inline std::optional<ViewData> resolve_ref_link_target_from_vd(const ViewData& vd, engine_time_t current_time) {
    if (!vd.meta || vd.meta->kind != TSKind::REF) {
        return std::nullopt;
    }
    if (!vd.value_data) {
        return std::nullopt;
    }

    auto value_meta = vd.meta->value_type;
    if (!value_meta) {
        return std::nullopt;
    }

    value::View v(vd.value_data, value_meta);
    if (!v.valid()) {
        return std::nullopt;
    }

    const auto* ts_ref = static_cast<const TSReference*>(v.data());
    if (!ts_ref || ts_ref->is_empty() || !ts_ref->is_peered()) {
        return std::nullopt;
    }

    try {
        TSView resolved = ts_ref->resolve(current_time);
        if (!resolved) {
            return std::nullopt;
        }
        return resolved.view_data();
    } catch (...) {
        return std::nullopt;
    }
}

// Helper: Resolve a LinkTarget that points to REF data.
// When a non-REF TSInput is bound to a REF output, the LinkTarget stores the
// REF output's data. This helper reads the TSReference value from the REF data,
// resolves the ShortPath, and returns a ViewData pointing to the actual target.
// Returns nullopt if the REF can't be resolved.
inline std::optional<ViewData> resolve_ref_link_target(const LinkTarget& lt, engine_time_t current_time) {
    // Build a ViewData from the LinkTarget fields and delegate
    ViewData vd{};
    vd.value_data = lt.value_data;
    vd.time_data = lt.time_data;
    vd.observer_data = lt.observer_data;
    vd.delta_data = lt.delta_data;
    vd.link_data = nullptr;
    vd.ops = lt.ops;
    vd.meta = lt.meta;
    vd.path = lt.target_path;
    return resolve_ref_link_target_from_vd(vd, current_time);
}

// ============================================================================
// REFBindingHelper: Manages dual subscription when TSInput binds to REF output.
//
// When a non-REF TSInput (e.g., TS[float]) binds to a REF output (e.g., REF[TS[float]]),
// we need two subscriptions:
// 1. To the REF source's observer list → for rebind notifications when the reference changes
// 2. To the resolved target's observer list → for value change notifications
//
// REFBindingHelper handles subscription (1) and manages the lifecycle of subscription (2)
// by resolving the TSReference and binding the LinkTarget to the actual underlying target.
//
// This matches the Python PythonBoundTimeSeriesInput.bind_output() pattern:
//   output.value.bind_input(self)  → binds to resolved target
//   output.observe_reference(self) → subscribes for rebind notifications
// ============================================================================

struct REFBindingHelper : public Notifiable {
    LinkTarget* owner;
    ViewData ref_source;          // REF output's ViewData (for reading TSReference)
    void* resolved_obs{nullptr};  // Current resolved target's observer_data
    void* input_observer_data{nullptr}; // TSInput's own observer list (for notifying downstream consumers)
    bool subscribed_to_ref{false};
    bool is_ref_to_ref_{false};   // True when REF input binds to REF output
    bool target_changed_{false};  // True when last rebind changed the target (not same target)
    ViewData prev_resolved_vd_{};  // Previous resolved target's ViewData (for delta computation)
    bool has_prev_resolved_{false}; // Whether prev_resolved_vd_ is valid
    ViewData pending_old_vd_{};    // Old target ViewData cached during rebind, consumed by delta_to_python
    engine_time_t pending_rebind_time_{MIN_DT}; // Time of pending rebind
    bool has_pending_delta_{false}; // True when rebind just happened and delta_to_python hasn't consumed it

    explicit REFBindingHelper(LinkTarget* lt, const ViewData& ref_src, bool ref_to_ref = false,
                              void* input_obs = nullptr)
        : owner(lt), ref_source(ref_src), input_observer_data(input_obs), is_ref_to_ref_(ref_to_ref) {
    }

    ~REFBindingHelper() override {
        // Do NOT call unsubscribe_all() here.
        // During graph teardown, the target ObserverList may already be freed,
        // causing use-after-free (SIGSEGV). Unsubscription is done explicitly
        // before deletion in the normal unbind path (scalar_ops::unbind).
    }

    void subscribe_to_ref_source() {
        if (!subscribed_to_ref && ref_source.observer_data) {
            auto* obs = static_cast<ObserverList*>(ref_source.observer_data);
            obs->add_observer(this);
            subscribed_to_ref = true;
        }
    }

    void unsubscribe_from_ref_source() {
        if (subscribed_to_ref && ref_source.observer_data) {
            auto* obs = static_cast<ObserverList*>(ref_source.observer_data);
            if (obs->is_alive()) {
                obs->remove_observer(this);
            }
            subscribed_to_ref = false;
        }
    }

    void unsubscribe_from_resolved() {
        if (resolved_obs && owner->is_linked) {
            auto* obs = static_cast<ObserverList*>(resolved_obs);
            if (obs->is_alive()) {
                obs->remove_observer(owner);  // time-accounting chain
                if (owner->active_notifier.owning_input != nullptr) {
                    obs->remove_observer(&owner->active_notifier);  // node-scheduling chain
                }
            }
        }
        resolved_obs = nullptr;
    }

    void unsubscribe_all() {
        unsubscribe_from_resolved();
        unsubscribe_from_ref_source();
    }

    // Resolve the current TSReference and rebind the LinkTarget to the resolved target.
    void rebind(engine_time_t current_time) {
        // Read TSReference from REF source FIRST, before any cleanup.
        // This lets us detect same-target rebinds and skip the expensive unsubscribe/resubscribe.
        if (!ref_source.value_data || !ref_source.meta) {
            // No valid REF source — clean up and return
            unsubscribe_from_resolved();
            if (!is_ref_to_ref_) {
                owner->is_linked = false;
                owner->value_data = nullptr;
                owner->time_data = nullptr;
                owner->observer_data = nullptr;
                owner->delta_data = nullptr;
                owner->link_data = nullptr;
                owner->ops = nullptr;
                owner->meta = nullptr;
            }
            target_changed_ = true;
            return;
        }
        auto value_meta = ref_source.meta->value_type;
        if (!value_meta) {
            unsubscribe_from_resolved();
            if (!is_ref_to_ref_) {
                owner->is_linked = false;
                owner->value_data = nullptr;
                owner->time_data = nullptr;
                owner->observer_data = nullptr;
                owner->delta_data = nullptr;
                owner->link_data = nullptr;
                owner->ops = nullptr;
                owner->meta = nullptr;
            }
            target_changed_ = true;
            return;
        }

        value::View v(ref_source.value_data, value_meta);
        if (!v.valid()) {
            unsubscribe_from_resolved();
            if (!is_ref_to_ref_) {
                owner->is_linked = false;
                owner->value_data = nullptr;
                owner->time_data = nullptr;
                owner->observer_data = nullptr;
                owner->delta_data = nullptr;
                owner->link_data = nullptr;
                owner->ops = nullptr;
                owner->meta = nullptr;
            }
            target_changed_ = true;
            return;
        }

        const auto* ts_ref = static_cast<const TSReference*>(v.data());
        if (!ts_ref) {
            unsubscribe_from_resolved();
            if (!is_ref_to_ref_) {
                owner->is_linked = false;
                owner->value_data = nullptr;
                owner->time_data = nullptr;
                owner->observer_data = nullptr;
                owner->delta_data = nullptr;
                owner->link_data = nullptr;
                owner->ops = nullptr;
                owner->meta = nullptr;
            }
            target_changed_ = true;
            return;
        }
        if (ts_ref->is_empty()) {
            unsubscribe_from_resolved();
            if (!is_ref_to_ref_) {
                // Store REF source data so valid()/modified() can check
                owner->is_linked = true;
                owner->target_path = ref_source.path;
                owner->value_data = ref_source.value_data;
                owner->time_data = ref_source.time_data;
                owner->observer_data = nullptr;
                owner->delta_data = ref_source.delta_data;
                owner->link_data = nullptr;
                owner->ops = ref_source.ops;
                owner->meta = ref_source.meta;
            }
            target_changed_ = true;
            return;
        }

        TSView resolved;

        if (ts_ref->is_peered()) {
            try {
                resolved = ts_ref->resolve(current_time);
            } catch (const std::exception& e) {
                return;
            }
        } else if (ts_ref->is_python_bound()) {
            // PYTHON_BOUND: extract ViewData from the stored Python object.
            // Implemented in py_ref.cpp to avoid including py_time_series.h here.
            auto vd = resolve_python_bound_reference(ts_ref, current_time);
            if (vd) {
                resolved = TSView(*vd, current_time);
            }
        } else {
            // NON_PEERED: store REF source data
            unsubscribe_from_resolved();
            if (!is_ref_to_ref_) {
                owner->is_linked = true;
                owner->target_path = ref_source.path;
                owner->value_data = ref_source.value_data;
                owner->time_data = ref_source.time_data;
                owner->observer_data = nullptr;
                owner->delta_data = ref_source.delta_data;
                owner->link_data = nullptr;
                owner->ops = ref_source.ops;
                owner->meta = ref_source.meta;
            }
            target_changed_ = true;
            return;
        }
        if (!resolved) return;

        // If the resolved target is itself a REF, dereference through it to get
        // the actual TS data. This happens in switch/map scenarios where the
        // ShortPath resolves to a stub's REF output rather than the underlying data.
        ViewData final_vd = resolved.view_data();
        if (final_vd.meta && final_vd.meta->kind == TSKind::REF) {
            auto inner = resolve_ref_link_target_from_vd(final_vd, current_time);
            if (inner) {
                final_vd = *inner;
            }
        }

        // Check if the resolved target is the same as the current target.
        // This matches Python's PythonBoundTimeSeriesInput.bind_output():
        //   if output is self._output: return self.has_peer  (no-op)
        // When the REF output fires with the same reference, rebinding to the
        // same target should be a no-op — no time stamp update, no notification.
        if (!is_ref_to_ref_ && owner->is_linked &&
            owner->value_data == final_vd.value_data) {
            // Same target — no need to unsubscribe/resubscribe since we didn't do it yet
            target_changed_ = false;
            return;
        }

        // Target is different — now perform the actual unsubscribe from old target
        // and capture old target values for delta computation.
        unsubscribe_from_resolved();

        // Cache the previous resolved target's ViewData for delta computation.
        // This is the C++ equivalent of Python's PythonTimeSeriesSetInput._prev_output
        // which stores a reference to the old output object, allowing delta_value to
        // reconstruct the pre-tick state at read time.
        // We use prev_resolved_vd_ (saved from the PREVIOUS rebind) which contains
        // the actual target output's ViewData (with correct time_data, delta_data, etc.)
        target_changed_ = true;
        if (!is_ref_to_ref_ && has_prev_resolved_) {
            pending_old_vd_ = prev_resolved_vd_;
            pending_rebind_time_ = current_time;
            has_pending_delta_ = true;
        }

        if (!is_ref_to_ref_) {
            // TS→REF mode: Store resolved target in LinkTarget
            owner->is_linked = true;
            owner->target_path = final_vd.path;
            owner->value_data = final_vd.value_data;
            // TSD has structured time data (container + per-element var_list)
            // that delta_to_python needs. Use target's time_data for TSD only.
            // All other types use owner_time_ptr (set by notify() each tick).
            if (final_vd.meta && final_vd.meta->kind == TSKind::TSD) {
                owner->time_data = final_vd.time_data;
            } else {
                owner->time_data = owner->owner_time_ptr;
            }
            owner->observer_data = final_vd.observer_data;
            owner->delta_data = final_vd.delta_data;
            owner->link_data = final_vd.link_data;
            owner->ops = final_vd.ops;
            owner->meta = final_vd.meta;
        }
        // REF→REF mode: LinkTarget keeps REF source data; only subscribe to resolved target

        // Subscribe to resolved target for notifications (both modes)
        // Use final_vd (which may have been dereferenced through REF layers)
        resolved_obs = final_vd.observer_data;
        if (resolved_obs) {
            auto* obs = static_cast<ObserverList*>(resolved_obs);
            obs->add_observer(owner);
            if (owner->active_notifier.owning_input != nullptr) {
                obs->add_observer(&owner->active_notifier);
            }
        }

        // Save the resolved target's ViewData for future delta computation.
        // On the next rebind, this will be the "old" target whose pre-tick
        // state is needed to compute the full diff delta.
        // Clear link_data so the cached ViewData reads directly from the
        // target's data without link following.
        prev_resolved_vd_ = final_vd;
        prev_resolved_vd_.link_data = nullptr;
        has_prev_resolved_ = true;
    }

    // Note: previous target ViewData is stored per-instance in pending_old_vd_
    // rather than in a thread-local cache to avoid cross-graph stale pointer issues.

    // Called when REF source changes - rebind to new target and schedule node
    void notify(engine_time_t et) override {
        // REF source changed - rebind to new resolved target
        rebind(et);

        // If the target didn't actually change (same output reference applied),
        // skip all notifications. This matches Python's bind_output():
        //   if output is self._output: return self.has_peer  (no-op)
        if (!target_changed_) {
            return;
        }

        // Time-accounting: propagate through LinkTarget chain
        owner->notify(et);

        // Notify the input's own observer list so downstream consumers (e.g., inner stubs
        // of nested graph nodes that bound to this input field) see the REF change.
        // This is needed because the REF change updates the LinkTarget's data (or
        // resolved target) but the TSInput's own observer list is separate and not
        // automatically notified.
        if (input_observer_data) {
            auto* obs = static_cast<ObserverList*>(input_observer_data);
            obs->notify_modified(et);
        }

        // Schedule owning node if active (the data source changed)
        if (owner->active_notifier.owning_input) {
            owner->active_notifier.notify(et);
        }
    }
};

void delete_ref_binding_helper(void* ptr) {
    delete static_cast<REFBindingHelper*>(ptr);
}

} // anonymous namespace

// ============================================================================
// Scalar Operations (TSValue, TSW, SIGNAL, REF)
// ============================================================================

namespace scalar_ops {

// For scalar TS types:
// - time is directly engine_time_t*
// - observer is directly ObserverList*
// - link is REFLink (TSOutput) or LinkTarget (TSInput)

engine_time_t last_modified_time(const ViewData& vd) {
    auto [result, target] = resolve_delegation_target_with_ref(vd, MIN_DT);
    if (result == DelegateResult::DELEGATED) return target.ops->last_modified_time(target);
    if (result == DelegateResult::REF_UNRESOLVED) return MIN_DT;
    if (!vd.time_data) return MIN_DT;
    return *static_cast<engine_time_t*>(vd.time_data);
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    auto [result, target] = resolve_delegation_target_with_ref(vd, current_time);
    if (result == DelegateResult::DELEGATED) return target.ops->modified(target, current_time);
    if (result == DelegateResult::REF_UNRESOLVED) return false;
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    // Peered REF input is always valid (Python: PythonTimeSeriesReferenceInput.valid = True always).
    // Must check BEFORE delegation because delegation would resolve to the output's REF data,
    // which returns False for empty REFs (below). But peered REF inputs need to be valid
    // regardless of emptiness so that consumer nodes are scheduled.
    if (vd.meta && vd.meta->kind == TSKind::REF) {
        if (auto* lt = get_active_link_target(vd)) {
            if (lt->meta && lt->meta->kind == TSKind::REF) {
                return true;  // Peered REF input — always valid
            }
        } else if (auto* rl = get_active_link(vd)) {
            if (rl->target().meta && rl->target().meta->kind == TSKind::REF) {
                return true;  // Peered REF input — always valid
            }
        }
    }
    auto [result, target] = resolve_delegation_target_with_ref(vd, MIN_DT);
    if (result == DelegateResult::DELEGATED) {
        return target.ops->valid(target);
    }
    if (result == DelegateResult::REF_UNRESOLVED) return false;
    // For REF outputs (no links), check that the TSReference is non-empty.
    // Matches Python: REF output valid = has_output (non-empty ref).
    if (vd.meta && vd.meta->kind == TSKind::REF && vd.value_data) {
        auto* ref = static_cast<const TSReference*>(vd.value_data);
        if (ref->is_empty()) return false;
    }
    return last_modified_time(vd) != MIN_DT;
}

bool all_valid(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) {
        return target->ops->all_valid(*target);
    }
    return valid(vd);
}

bool sampled(const ViewData& vd) {
    // Return the sampled flag from ViewData
    // This flag is set when navigating through a REFLink that was rebound
    return vd.sampled;
}

value::View value(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) {
        return target->ops->value(*target);
    }
    return make_value_view(vd);
}

value::View delta_value(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) {
        return target->ops->delta_value(*target);
    }
    return make_value_view(vd);
}

bool has_delta(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) {
        return target->ops->has_delta(*target);
    }
    return valid(vd);
}

void set_value(ViewData& vd, const value::View& src, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("set_value on invalid ViewData");
    }

    // Copy value
    auto dst = make_value_view(vd);
    dst.copy_from(src);

    // Update time
    *static_cast<engine_time_t*>(vd.time_data) = current_time;

    // Notify observers
    if (vd.observer_data) {
        auto* observers = static_cast<ObserverList*>(vd.observer_data);
        observers->notify_modified(current_time);
    }
}

void apply_delta(ViewData& vd, const value::View& delta, engine_time_t current_time) {
    // Scalar types don't support delta application - just set the value
    set_value(vd, delta, current_time);
}

void invalidate(ViewData& vd) {
    if (vd.time_data) {
        *static_cast<engine_time_t*>(vd.time_data) = MIN_DT;
    }
}

nb::object to_python(const ViewData& vd) {
    auto [result, target] = resolve_delegation_target_with_ref(vd, MIN_DT);
    if (result == DelegateResult::DELEGATED) return target.ops->to_python(target);
    if (result == DelegateResult::REF_UNRESOLVED) return nb::none();
    if (!valid(vd)) return nb::none();
    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

nb::object delta_to_python(const ViewData& vd) {
    auto [result, target] = resolve_delegation_target_with_ref(vd, MIN_DT);
    if (result == DelegateResult::DELEGATED) {
        return target.ops->delta_to_python(target);
    }
    if (result == DelegateResult::REF_UNRESOLVED) return nb::none();
    if (!valid(vd)) return nb::none();
    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("from_python on invalid ViewData");
    }

    // Set value from Python
    auto dst = make_value_view(vd);
    dst.from_python(src);

    // Update time
    *static_cast<engine_time_t*>(vd.time_data) = current_time;

    // Notify observers
    if (vd.observer_data) {
        auto* observers = static_cast<ObserverList*>(vd.observer_data);
        observers->notify_modified(current_time);
    }
}

TSView child_at(const ViewData& vd, size_t index, engine_time_t current_time) {
    // Scalar types have no children
    return TSView{};
}

TSView child_by_name(const ViewData& vd, const std::string& name, engine_time_t current_time) {
    // Scalar types have no children
    return TSView{};
}

TSView child_by_key(const ViewData& vd, const value::View& key, engine_time_t current_time) {
    // Scalar types have no children
    return TSView{};
}

size_t child_count(const ViewData& vd) {
    return 0;
}

value::View observer(const ViewData& vd) {
    return make_observer_view(vd);
}

void notify_observers(ViewData& vd, engine_time_t current_time) {
    if (vd.observer_data) {
        auto* observers = static_cast<ObserverList*>(vd.observer_data);
        observers->notify_modified(current_time);
    }
}

void bind(ViewData& vd, const ViewData& target) {
    // For scalar TS types, binding stores the target ViewData in the link storage.
    // This enables the scalar to delegate value/modified/valid checks to the target.
    if (!vd.link_data) {
        throw std::runtime_error("bind on scalar without link data");
    }

    if (vd.uses_link_target) {
        // TSInput: Store directly in LinkTarget
        auto* lt = get_link_target(vd.link_data);
        if (!lt) {
            throw std::runtime_error("bind on TSInput with invalid link data");
        }

        // Set time-accounting chain
        if (vd.time_data) {
            lt->owner_time_ptr = static_cast<engine_time_t*>(vd.time_data);
        }
        // parent_link is set by caller for nested scalars (or nullptr for root)

        // Check if target is a REF type - need special handling
        if (target.meta && target.meta->kind == TSKind::REF) {
            // Clean up existing REF binding if any (prevents leak/double subscription
            // when bind_output is called multiple times without unbind)
            if (lt->ref_binding_) {
                auto* old_helper = static_cast<REFBindingHelper*>(lt->ref_binding_);
                old_helper->unsubscribe_all();
                if (lt->ref_binding_deleter_) {
                    lt->ref_binding_deleter_(lt->ref_binding_);
                }
                lt->ref_binding_ = nullptr;
                lt->ref_binding_deleter_ = nullptr;
                // Clear stale LinkTarget data from old binding
                lt->is_linked = false;
                lt->value_data = nullptr;
                lt->time_data = nullptr;
                lt->observer_data = nullptr;
                lt->delta_data = nullptr;
                lt->link_data = nullptr;
                lt->ops = nullptr;
                lt->meta = nullptr;
            }

            // Detect REF→REF binding (input is also REF type)
            bool is_ref_to_ref = (vd.meta && vd.meta->kind == TSKind::REF);

            // REF-aware binding: resolve through REF, store resolved target in LinkTarget,
            // and subscribe to both REF source (for rebind) and resolved target (for value changes).
            // This matches Python's observe_reference + bind_input pattern.
            auto* helper = new REFBindingHelper(lt, target, is_ref_to_ref, vd.observer_data);
            lt->ref_binding_ = helper;
            lt->ref_binding_deleter_ = &delete_ref_binding_helper;

            if (is_ref_to_ref) {
                // REF→REF: Store REF source data in LinkTarget so that
                // ref_value() can read the TSReference, and modified()/valid()
                // check the REF output's state (not the resolved target's).
                lt->is_linked = true;
                lt->target_path = target.path;
                lt->value_data = target.value_data;
                lt->time_data = target.time_data;
                lt->observer_data = nullptr;  // REFBindingHelper manages subscriptions
                lt->delta_data = target.delta_data;
                lt->link_data = nullptr;  // Prevent chaining through REF source's own links
                lt->ops = target.ops;
                lt->meta = target.meta;  // meta->kind == REF (preserves REF identity)
            }

            // Subscribe helper to REF source observer list (for rebind notifications)
            helper->subscribe_to_ref_source();

            // Initial resolve and bind to current target (may fail if REF not yet set)
            helper->rebind(MIN_DT);

            return;
        }

        // Non-REF target: store directly and subscribe
        store_to_link_target(*lt, target);

        // For TS→REF binding (REF input binding to non-REF output):
        // The reference is fixed at bind time - do NOT subscribe to the target's observer list.
        // Python equivalent: PythonTimeSeriesReferenceInput.do_bind_output sets _output=None
        // (no subscription) and _value=TimeSeriesReference.make(output) (fixed reference).
        if (vd.meta && vd.meta->kind == TSKind::REF) {
            // Mark the REF input's modification time so it ticks at the first evaluation
            if (vd.time_data) {
                *static_cast<engine_time_t*>(vd.time_data) = MIN_ST;
            } else if (lt->owner_time_ptr) {
                *lt->owner_time_ptr = MIN_ST;
            }
            // Point the LinkTarget's time_data to the owner's time, not the target's.
            // This ensures valid()/modified() checks via delegation see the REF input's
            // own modification state (set to MIN_ST above), not the target output's.
            // Without this, the node wouldn't evaluate at the initial tick because
            // delegation would check the target's time (which may not be modified yet).
            lt->time_data = lt->owner_time_ptr ? lt->owner_time_ptr : vd.time_data;
            return;  // Skip subscribing to target's observer list
        }

        // Subscribe for time-accounting (always, regardless of active state)
        if (lt->observer_data) {
            auto* obs = static_cast<ObserverList*>(lt->observer_data);
            obs->add_observer(lt);
        }
    } else {
        // TSOutput: Use REFLink with possible REF dereferencing
        auto* rl = get_ref_link(vd.link_data);
        if (!rl) {
            throw std::runtime_error("bind on scalar with invalid link data");
        }

        // Check if target is a REF type - if so, we need to dereference through it
        if (target.meta && target.meta->kind == TSKind::REF) {
            // Target is a REF - use bind_to_ref for dereferencing
            // This sets up the REFLink to subscribe to the REF source and resolve the target
            TSView target_view(target, MIN_DT);
            rl->bind_to_ref(target_view, MIN_DT);
        } else {
            // Direct binding for non-REF targets
            store_link_target(*rl, target);
        }
    }
}

void unbind(ViewData& vd) {
    // For scalar TS types, unbinding clears the link storage.
    if (!vd.link_data) return;

    if (vd.uses_link_target) {
        // TSInput: Unsubscribe both chains then clear LinkTarget
        auto* lt = get_link_target(vd.link_data);
        if (!lt) return;

        if (lt->ref_binding_) {
            // REF binding: explicitly unsubscribe before deleting the helper,
            // since the destructor intentionally skips unsubscription (to avoid
            // use-after-free during graph teardown).
            auto* helper = static_cast<REFBindingHelper*>(lt->ref_binding_);
            helper->unsubscribe_all();
            lt->cleanup_ref_binding();
            // Reset the input's own time data so valid() returns false after unbind.
            // owner_time_ptr points to the input's time slot in TSValue::time_.
            if (lt->owner_time_ptr) *lt->owner_time_ptr = MIN_DT;
            lt->clear();
        } else if (lt->is_linked) {
            // Non-REF binding: original unsubscription logic
            if (lt->observer_data) {
                auto* obs = static_cast<ObserverList*>(lt->observer_data);
                // Unsubscribe time-accounting
                obs->remove_observer(lt);
                // Unsubscribe node-scheduling if active
                if (lt->active_notifier.owning_input != nullptr) {
                    obs->remove_observer(&lt->active_notifier);
                    lt->active_notifier.owning_input = nullptr;
                }
            }
            // Reset the input's own time data so valid() returns false after unbind.
            // owner_time_ptr points to the input's time slot in TSValue::time_.
            if (lt->owner_time_ptr) *lt->owner_time_ptr = MIN_DT;
            lt->clear();
        }
    } else {
        // TSOutput: Unbind REFLink (handles its own unsubscription)
        auto* rl = get_ref_link(vd.link_data);
        if (rl) {
            rl->unbind();
        }
    }
}

bool is_bound(const ViewData& vd) {
    if (!vd.link_data) return false;

    if (vd.uses_link_target) {
        // TSInput: Check LinkTarget
        auto* lt = get_link_target(vd.link_data);
        return lt && lt->is_linked;
    } else {
        // TSOutput: Check REFLink
        auto* rl = get_ref_link(vd.link_data);
        return rl && rl->target().is_linked;
    }
}

bool is_peered(const ViewData& vd) {
    // Scalar types are always peered when bound (there is no element-level distinction)
    return is_bound(vd);
}

void set_active(ViewData& vd, value::View active_view, bool active, TSInput* input) {
    if (!active_view) return;

    // Scalar active schema is just a bool
    *static_cast<bool*>(active_view.data()) = active;

    // Manage node-scheduling subscription for scalar input if bound
    if (vd.link_data) {
        void* observer_data = nullptr;
        const LinkTarget* bound_lt = nullptr;
        LinkTarget* mutable_lt = nullptr;
        bool is_ts_to_ref = false;

        if (vd.uses_link_target) {
            // TSInput: Get observer from LinkTarget
            auto* lt = get_link_target(vd.link_data);

            // Detect TS→REF binding: REF input bound to non-REF target.
            // In this case, the reference is fixed and we should NOT subscribe
            // to the target's observer list (matches Python behavior).
            is_ts_to_ref = (vd.meta && vd.meta->kind == TSKind::REF &&
                            lt && lt->is_linked && lt->meta && lt->meta->kind != TSKind::REF);

            if (is_ts_to_ref) {
                // TS→REF: Fixed reference to non-REF target.
                // Python's PythonTimeSeriesReferenceInput.make_active() subscribes
                // to the output observer list, so the node IS scheduled when the
                // underlying value changes. We must do the same.
                observer_data = lt->observer_data;
                bound_lt = lt;
                mutable_lt = lt;
            } else if (lt && lt->is_linked) {
                observer_data = lt->observer_data;
                bound_lt = lt;
                mutable_lt = lt;
            } else if (lt && lt->ref_binding_) {
                // REF binding exists but hasn't resolved yet.
                // We still need to set owning_input so that when
                // REFBindingHelper::rebind() resolves the target later,
                // it can subscribe the ActiveNotifier.
                mutable_lt = lt;
            }
        } else {
            // TSOutput: Get observer from REFLink's target
            auto* rl = get_ref_link(vd.link_data);
            if (rl && rl->target().is_linked) {
                observer_data = rl->target().observer_data;
                bound_lt = &rl->target();
            }
        }

        if (active) {
            if (mutable_lt) {
                // Set owning_input even if not yet linked (for REF binding that resolves later)
                if (mutable_lt->active_notifier.owning_input == nullptr) {
                    mutable_lt->active_notifier.owning_input = input;
                }
                // Subscribe to observer list if available (linked to target)
                if (observer_data) {
                    auto* observers = static_cast<ObserverList*>(observer_data);
                    observers->add_observer(&mutable_lt->active_notifier);
                }
            } else if (observer_data) {
                // TSOutput path: subscribe input directly (no LinkTarget)
                auto* observers = static_cast<ObserverList*>(observer_data);
                observers->add_observer(input);
            }

            if (is_ts_to_ref && input) {
                // TS→REF: The reference is valid from bind time.
                // Fire initial notification at MIN_ST so the node evaluates at the first tick.
                // Python equivalent: PythonTimeSeriesReferenceInput.make_active() always
                // notifies at evaluation_time when valid (and TS→REF is always valid).
                input->notify(MIN_ST);
            } else if (input && bound_lt && bound_lt->ops) {
                // Initial notification: match Python make_active() behavior.
                // After subscribing, if the output is already valid AND modified,
                // fire notify to schedule the owning node for evaluation.
                ViewData output_vd = make_view_data_from_link_target(*bound_lt, vd.path);
                if (bound_lt->ops->valid(output_vd)) {
                    auto* node = input->owning_node();
                    if (node && node->cached_evaluation_time_ptr()) {
                        engine_time_t eval_time = *node->cached_evaluation_time_ptr();
                        if (bound_lt->ops->modified(output_vd, eval_time)) {
                            engine_time_t lmt = bound_lt->ops->last_modified_time(output_vd);
                            input->notify(lmt);
                        }
                    }
                }
            }
        } else {
            // Unsubscribe ActiveNotifier (TSInput path)
            if (mutable_lt) {
                if (mutable_lt->active_notifier.owning_input != nullptr) {
                    if (observer_data) {
                        auto* observers = static_cast<ObserverList*>(observer_data);
                        observers->remove_observer(&mutable_lt->active_notifier);
                    }
                    mutable_lt->active_notifier.owning_input = nullptr;
                }
            } else if (observer_data) {
                auto* observers = static_cast<ObserverList*>(observer_data);
                observers->remove_observer(input);
            }
        }
    }
}

} // namespace scalar_ops

// ============================================================================
// Bundle Operations (TSB)
// ============================================================================

namespace bundle_ops {

// Forward declarations for functions used by all_valid
TSView child_at(const ViewData& vd, size_t index, engine_time_t current_time);

// For TSB types:
// - value is bundle type
// - time is tuple[engine_time_t, field_times...]
// - observer is tuple[ObserverList, field_observers...]
// - link is tuple[LinkType, link_schema(field_0), link_schema(field_1), ...]
//   where LinkType is LinkTarget (TSInput) or REFLink (TSOutput)
//   element 0 is the bundle-level link, and elements 1+ are per-field link data

// Helper: Get the appropriate link schema based on uses_link_target flag
inline const value::TypeMeta* get_bundle_link_schema(const ViewData& vd) {
    if (!vd.meta) return nullptr;
    if (vd.uses_link_target) {
        return TSMetaSchemaCache::instance().get_input_link_schema(vd.meta);
    }
    return TSMetaSchemaCache::instance().get_link_schema(vd.meta);
}

// Helper: Get link data for a specific field (returns void* to field's link storage)
inline void* get_field_link_data(const ViewData& vd, size_t field_index) {
    if (!vd.link_data || !vd.meta || field_index >= vd.meta->field_count) {
        return nullptr;
    }
    auto link_schema = get_bundle_link_schema(vd);
    if (!link_schema) return nullptr;

    value::View link_view(vd.link_data, link_schema);
    // Field link is at index field_index + 1 (since element 0 is bundle-level link)
    return link_view.as_tuple().at(field_index + 1).data();
}

// Helper: Get REFLink for a scalar field (field's link is just a REFLink)
// Only valid when uses_link_target is false
inline REFLink* get_scalar_field_ref_link(const ViewData& vd, size_t field_index) {
    if (vd.uses_link_target) return nullptr;  // Wrong type
    void* link_data = get_field_link_data(vd, field_index);
    if (!link_data) return nullptr;
    return static_cast<REFLink*>(link_data);
}

// Helper: Get LinkTarget for a scalar field (field's link is just a LinkTarget)
// Only valid when uses_link_target is true
inline LinkTarget* get_scalar_field_link_target(const ViewData& vd, size_t field_index) {
    if (!vd.uses_link_target) return nullptr;  // Wrong type
    void* link_data = get_field_link_data(vd, field_index);
    if (!link_data) return nullptr;
    return static_cast<LinkTarget*>(link_data);
}

// Helper: Check if any field is linked (only checks scalar fields)
inline bool any_field_linked(const ViewData& vd) {
    if (!vd.link_data || !vd.meta) return false;
    auto link_schema = get_bundle_link_schema(vd);
    if (!link_schema) return false;

    value::View link_view(vd.link_data, link_schema);
    auto link_tuple = link_view.as_tuple();

    // Check each scalar field's link (starting at index 1)
    for (size_t i = 0; i < vd.meta->field_count; ++i) {
        const TSMeta* field_meta = vd.meta->fields[i].ts_type;
        if (field_meta && field_meta->is_scalar_ts()) {
            if (vd.uses_link_target) {
                // TSInput: LinkTarget
                auto* lt = static_cast<const LinkTarget*>(link_tuple.at(i + 1).data());
                if (lt && lt->is_linked) {
                    return true;
                }
            } else {
                // TSOutput: REFLink
                auto* rl = static_cast<const REFLink*>(link_tuple.at(i + 1).data());
                if (rl && rl->target().is_linked) {
                    return true;
                }
            }
        }
    }
    return false;
}

engine_time_t last_modified_time(const ViewData& vd) {
    auto time_view = make_time_view(vd);
    if (!time_view.valid()) return MIN_DT;
    return time_view.as_tuple().at(0).as<engine_time_t>();
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    // Check bundle-level time first (proactive path via LinkTarget::notify)
    if (last_modified_time(vd) >= current_time) {
        return true;
    }

    // Fallback: For input bundles with field-by-field binding (where parent chain
    // isn't set up), check if any linked field was modified directly
    if (vd.link_data && vd.meta) {
        auto link_schema = get_bundle_link_schema(vd);
        if (!link_schema) return false;

        value::View link_view(vd.link_data, link_schema);
        auto link_tuple = link_view.as_tuple();

        for (size_t i = 0; i < vd.meta->field_count; ++i) {
            const TSMeta* field_meta = vd.meta->fields[i].ts_type;
            if (!field_meta) continue;

            if (field_meta->is_scalar_ts()) {
                if (vd.uses_link_target) {
                    auto* lt = static_cast<const LinkTarget*>(link_tuple.at(i + 1).data());
                    if (lt && lt->is_linked && lt->ops) {
                        ViewData field_vd = make_view_data_from_link_target(*lt, vd.path.child(i));
                        if (field_vd.ops->modified(field_vd, current_time)) {
                            return true;
                        }
                    }
                }
            }
        }
    }
    return false;
}

bool valid(const ViewData& vd) {
    // First check if the bundle's own time indicates validity (proactive path)
    if (last_modified_time(vd) != MIN_DT) {
        return true;
    }

    // Fallback: For input bundles with field-by-field binding, check linked fields
    if (!vd.link_data || !vd.meta) return false;
    auto link_schema = get_bundle_link_schema(vd);
    if (!link_schema) return false;

    value::View link_view(vd.link_data, link_schema);
    auto link_tuple = link_view.as_tuple();

    for (size_t i = 0; i < vd.meta->field_count; ++i) {
        const TSMeta* field_meta = vd.meta->fields[i].ts_type;
        if (!field_meta) continue;

        if (field_meta->is_scalar_ts()) {
            if (vd.uses_link_target) {
                auto* lt = static_cast<const LinkTarget*>(link_tuple.at(i + 1).data());
                if (lt && lt->is_linked && lt->ops) {
                    ViewData field_vd = make_view_data_from_link_target(*lt, vd.path);
                    if (lt->ops->valid(field_vd)) {
                        return true;
                    }
                }
            } else {
                auto* rl = static_cast<const REFLink*>(link_tuple.at(i + 1).data());
                if (rl && rl->target().is_linked && rl->target().ops) {
                    ViewData field_vd = make_view_data_from_link(*rl, vd.path);
                    if (rl->target().ops->valid(field_vd)) {
                        return true;
                    }
                }
            }
        } else {
            void* field_link_data = link_tuple.at(i + 1).data();
            if (field_link_data) {
                ViewData field_vd;
                field_vd.link_data = field_link_data;
                field_vd.meta = field_meta;
                field_vd.uses_link_target = vd.uses_link_target;
                field_vd.ops = get_ts_ops(field_meta);
                if (field_vd.ops && field_vd.ops->valid(field_vd)) {
                    return true;
                }
            }
        }
    }

    return false;
}

bool all_valid(const ViewData& vd) {
    if (!vd.meta) return false;

    // Empty bundle: all_valid is vacuously true (matches Python's all([]) == True)
    if (vd.meta->field_count == 0) return true;

    // First check if this bundle itself is valid (any field has ticked)
    if (!valid(vd)) return false;

    // Use MIN_DT (smallest valid time) as the current_time parameter for child_at
    // This ensures we get the proper child view regardless of when we're checking
    engine_time_t query_time = MIN_DT;

    for (size_t i = 0; i < vd.meta->field_count; ++i) {
        // Get the child view - this properly handles linked fields by delegating to target
        TSView child_view = child_at(vd, i, query_time);

        // Check if the child is all_valid
        if (!child_view || !child_view.all_valid()) {
            return false;
        }
    }

    return true;
}

bool sampled(const ViewData& vd) {
    // Return the sampled flag from ViewData
    return vd.sampled;
}

value::View value(const ViewData& vd) {
    return make_value_view(vd);
}

value::View delta_value(const ViewData& vd) {
    return make_delta_view(vd);
}

bool has_delta(const ViewData& vd) {
    if (!vd.delta_data) return false;
    if (!vd.meta) return false;
    for (size_t i = 0; i < vd.meta->field_count; ++i) {
        if (::hgraph::has_delta(vd.meta->fields[i].ts_type)) {
            return true;
        }
    }
    return false;
}

void set_value(ViewData& vd, const value::View& src, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("set_value on invalid ViewData");
    }

    // Copy value
    auto dst = make_value_view(vd);
    dst.copy_from(src);

    // Update container time
    auto time_view = make_time_view(vd);
    time_view.as_tuple().at(0).as<engine_time_t>() = current_time;

    // Also update field-level times (important for valid() checks on linked fields)
    // This matches Python behavior where setting tsb.value sets each field individually
    if (vd.meta) {
        auto time_tuple = time_view.as_tuple();
        for (size_t i = 0; i < vd.meta->field_count; ++i) {
            value::View field_time = time_tuple.at(i + 1);  // +1 for bundle-level time
            if (field_time) {
                const TSMeta* field_meta = vd.meta->fields[i].ts_type;
                if (field_meta && field_meta->kind == TSKind::TSB) {
                    // Nested TSB: set container time AND recursively set sub-field times
                    auto nested_time_tuple = field_time.as_tuple();
                    nested_time_tuple.at(0).as<engine_time_t>() = current_time;
                    for (size_t j = 0; j < field_meta->field_count; ++j) {
                        value::View sub_field_time = nested_time_tuple.at(j + 1);
                        if (sub_field_time) {
                            const TSMeta* sub_meta = field_meta->fields[j].ts_type;
                            if (sub_meta && (sub_meta->is_collection() || sub_meta->kind == TSKind::TSB)) {
                                sub_field_time.as_tuple().at(0).as<engine_time_t>() = current_time;
                            } else {
                                *static_cast<engine_time_t*>(sub_field_time.data()) = current_time;
                            }
                        }
                    }
                } else if (field_meta && field_meta->is_collection()) {
                    // Collection field: time is tuple[engine_time_t, elem_times...]
                    auto coll_time_tuple = field_time.as_tuple();
                    coll_time_tuple.at(0).as<engine_time_t>() = current_time;

                    // For TSL: stamp all fixed-size element times
                    if (field_meta->kind == TSKind::TSL && field_meta->fixed_size > 0) {
                        auto elem_times = coll_time_tuple.at(1).as_list();
                        size_t max_idx = std::min(static_cast<size_t>(field_meta->fixed_size), elem_times.size());
                        for (size_t j = 0; j < max_idx; ++j) {
                            elem_times.at(j).as<engine_time_t>() = current_time;
                        }
                    }
                } else {
                    // Scalar field: time is just engine_time_t
                    *static_cast<engine_time_t*>(field_time.data()) = current_time;
                }
            }
        }
    }

    // Notify container observers
    if (vd.observer_data) {
        auto observer_view = make_observer_view(vd);
        auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
        observers->notify_modified(current_time);
    }
}

void apply_delta(ViewData& vd, const value::View& delta, engine_time_t current_time) {
    // TSB doesn't support delta - just set value
    set_value(vd, delta, current_time);
}

void invalidate(ViewData& vd) {
    if (vd.time_data) {
        auto time_view = make_time_view(vd);
        time_view.as_tuple().at(0).as<engine_time_t>() = MIN_DT;
    }
}

nb::object to_python(const ViewData& vd) {
    // Check time-series validity first (has value been set?)
    if (!valid(vd)) return nb::none();

    // For input bundles with per-field links, we need to build the dict
    // by following the links to get actual values from the bound outputs
    if (vd.link_data && vd.meta) {
        auto* link_schema = get_bundle_link_schema(vd);
        if (link_schema) {
            value::View link_view(vd.link_data, link_schema);
            auto link_tuple = link_view.as_tuple();

            // Check if any scalar field has a valid link (indicating this is a linked input bundle)
            // Note: Collection/composite fields use per-element links, but if any scalar is linked,
            // we're in the linked path and should handle all fields through links/child_at.
            bool has_links = false;
            for (size_t i = 0; i < vd.meta->field_count; ++i) {
                const TSMeta* field_meta = vd.meta->fields[i].ts_type;
                if (field_meta && field_meta->is_scalar_ts()) {
                    value::View field_link = link_tuple.at(i + 1);  // +1 for bundle-level link
                    if (vd.uses_link_target) {
                        auto* lt = static_cast<const LinkTarget*>(field_link.data());
                        if (lt && lt->is_linked) {
                            has_links = true;
                            break;
                        }
                    } else {
                        auto* rl = static_cast<const REFLink*>(field_link.data());
                        if (rl && rl->target().is_linked) {
                            has_links = true;
                            break;
                        }
                    }
                }
            }
            // Also check if any collection/composite field has links (via its link_data)
            if (!has_links) {
                for (size_t i = 0; i < vd.meta->field_count; ++i) {
                    const TSMeta* field_meta = vd.meta->fields[i].ts_type;
                    if (field_meta && (field_meta->is_collection() || field_meta->kind == TSKind::TSB)) {
                        // Collection fields have their own link schemas - check if link data exists
                        void* field_link_data = get_field_link_data(vd, i);
                        if (field_link_data) {
                            has_links = true;
                            break;
                        }
                    }
                }
            }

            if (has_links) {
                // Build dict from linked field values
                // Matching Python: {k: ts.value for k, ts in items() if ts.valid or getattr(s, k, None) is None}
                bool has_cs = vd.meta->python_type && !vd.meta->python_type.is_none();
                nb::dict result;
                for (size_t i = 0; i < vd.meta->field_count; ++i) {
                    const TSBFieldInfo& field_info = vd.meta->fields[i];
                    const TSMeta* field_meta = field_info.ts_type;
                    const char* field_name = field_info.name;

                    bool field_included = false;
                    if (field_meta && field_meta->is_scalar_ts()) {
                        value::View field_link = link_tuple.at(i + 1);
                        if (vd.uses_link_target) {
                            auto* lt = static_cast<const LinkTarget*>(field_link.data());
                            if (lt && lt->is_linked && lt->ops) {
                                ViewData target_vd = make_view_data_from_link_target(*lt, vd.path.child(i));
                                if (target_vd.ops->valid(target_vd)) {
                                    nb::object field_val = target_vd.ops->to_python(target_vd);
                                    if (!field_val.is_none()) {
                                        result[field_name] = field_val;
                                        field_included = true;
                                    }
                                }
                            }
                        } else {
                            auto* rl = static_cast<const REFLink*>(field_link.data());
                            if (rl && rl->target().is_linked && rl->target().ops) {
                                ViewData target_vd = make_view_data_from_link(*rl, vd.path.child(i));
                                if (target_vd.ops->valid(target_vd)) {
                                    nb::object field_val = target_vd.ops->to_python(target_vd);
                                    if (!field_val.is_none()) {
                                        result[field_name] = field_val;
                                        field_included = true;
                                    }
                                }
                            }
                        }
                    } else if (field_meta && (field_meta->is_collection() || field_meta->kind == TSKind::TSB)) {
                        // Collection/composite fields: use child_at to get field view with proper link data
                        TSView field_view = child_at(vd, i, MIN_DT);
                        const auto& fvd = field_view.view_data();
                        if (fvd.valid() && fvd.ops && fvd.ops->valid(fvd)) {
                            nb::object field_val = fvd.ops->to_python(fvd);
                            if (!field_val.is_none()) {
                                result[field_name] = field_val;
                                field_included = true;
                            }
                        }
                    }
                    // For CompoundScalar: include required fields (no default) as None
                    // Matching Python: getattr(s, k, None) is None
                    if (!field_included && has_cs) {
                        nb::object default_val = nb::getattr(vd.meta->python_type, field_name, nb::none());
                        if (default_val.is_none()) {
                            result[field_name] = nb::none();
                        }
                    }
                }
                if (has_cs) {
                    return vd.meta->python_type(**result);
                }
                return result;
            }
        }
    }

    // No links or not an input bundle - use local value storage
    // Build dict with only valid fields (matching Python: {k: ts.value for k, ts in items() if ts.valid})
    if (vd.meta && vd.meta->field_count > 0 && vd.time_data) {
        bool has_cs = vd.meta->python_type && !vd.meta->python_type.is_none();
        auto time_view = make_time_view(vd);
        auto time_tuple = time_view.as_tuple();
        auto value_view = make_value_view(vd);
        auto value_indexed = value_view.as_bundle();

        nb::dict result;
        for (size_t i = 0; i < vd.meta->field_count; ++i) {
            const TSBFieldInfo& field_info = vd.meta->fields[i];
            const TSMeta* field_meta = field_info.ts_type;
            const char* field_name = field_info.name;

            // Check if this field is valid (its time has been set)
            value::View field_time = time_tuple.at(i + 1);  // +1 for bundle-level time
            bool field_valid = false;
            if (field_time) {
                engine_time_t ft;
                if (field_meta && (field_meta->is_collection() || field_meta->kind == TSKind::TSB)) {
                    ft = field_time.as_tuple().at(0).as<engine_time_t>();
                } else {
                    ft = *static_cast<const engine_time_t*>(field_time.data());
                }
                field_valid = (ft != MIN_DT);
            }

            if (field_valid) {
                if (field_meta && (field_meta->is_collection() || field_meta->kind == TSKind::TSB)) {
                    // Composite field: use TS-level to_python for correct formatting
                    // (e.g., TSL returns {index: value} dict, not a list)
                    ViewData field_vd;
                    field_vd.path = vd.path.child(i);
                    field_vd.value_data = value_indexed.at(i).data();
                    field_vd.time_data = time_tuple.at(i + 1).data();
                    if (vd.observer_data) {
                        auto observer_view = make_observer_view(vd);
                        field_vd.observer_data = observer_view.as_tuple().at(i + 1).data();
                    }
                    field_vd.delta_data = nullptr;
                    field_vd.uses_link_target = vd.uses_link_target;
                    field_vd.ops = get_ts_ops(field_meta);
                    field_vd.meta = field_meta;
                    nb::object py_val = field_vd.ops->to_python(field_vd);
                    if (!py_val.is_none()) {
                        result[field_name] = py_val;
                    }
                } else {
                    // Scalar field: use value-level to_python
                    value::View field_value = value_indexed.at(i);
                    if (field_value.valid()) {
                        result[field_name] = field_value.to_python();
                    }
                }
            } else if (has_cs) {
                // For CompoundScalar: include required fields (no default) as None
                nb::object default_val = nb::getattr(vd.meta->python_type, field_name, nb::none());
                if (default_val.is_none()) {
                    result[field_name] = nb::none();
                }
            }
        }
        if (has_cs) {
            return vd.meta->python_type(**result);
        }
        return result;
    }

    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

nb::object delta_to_python(const ViewData& vd) {
    // For TSB, delta_value returns only modified AND valid fields
    // Matching Python: {k: ts.delta_value for k, ts in items() if ts.modified and ts.valid}
    if (!valid(vd)) return nb::none();

    // For input bundles with links, follow links and check per-field modified+valid
    if (vd.link_data && vd.meta) {
        auto* link_schema = get_bundle_link_schema(vd);
        bool has_links = link_schema && any_field_linked(vd);
        if (has_links) {
            value::View link_view(vd.link_data, link_schema);
            auto link_tuple = link_view.as_tuple();

            // First pass: determine current engine time (max last_modified_time across all fields)
            engine_time_t current_time = MIN_DT;
            for (size_t i = 0; i < vd.meta->field_count; ++i) {
                const TSMeta* field_meta = vd.meta->fields[i].ts_type;
                if (!field_meta) continue;

                if (field_meta->is_scalar_ts()) {
                    if (vd.uses_link_target) {
                        auto* lt = static_cast<const LinkTarget*>(link_tuple.at(i + 1).data());
                        if (lt && lt->is_linked && lt->ops) {
                            ViewData target_vd = make_view_data_from_link_target(*lt, vd.path.child(i));
                            engine_time_t ft = target_vd.ops->last_modified_time(target_vd);
                            if (ft > current_time) current_time = ft;
                        }
                    }
                } else if (field_meta->is_collection() || field_meta->kind == TSKind::TSB) {
                    TSView field_view = child_at(vd, i, MIN_DT);
                    const auto& fvd = field_view.view_data();
                    if (fvd.valid() && fvd.ops) {
                        engine_time_t ft = fvd.ops->last_modified_time(fvd);
                        if (ft > current_time) current_time = ft;
                    }
                }
            }

            // Second pass: collect modified+valid fields
            nb::dict result;
            for (size_t i = 0; i < vd.meta->field_count; ++i) {
                const TSBFieldInfo& field_info = vd.meta->fields[i];
                const TSMeta* field_meta = field_info.ts_type;
                const char* field_name = field_info.name;

                if (field_meta && field_meta->is_scalar_ts()) {
                    if (vd.uses_link_target) {
                        auto* lt = static_cast<const LinkTarget*>(link_tuple.at(i + 1).data());
                        if (lt && lt->is_linked && lt->ops) {
                            ViewData target_vd = make_view_data_from_link_target(*lt, vd.path.child(i));
                            if (target_vd.ops->valid(target_vd) &&
                                target_vd.ops->modified(target_vd, current_time)) {
                                // Python TSB delta_value filters by ts.valid which for REF returns
                                // has_output (False for empty refs). Match that here.
                                if (target_vd.meta && target_vd.meta->kind == TSKind::REF &&
                                    target_vd.value_data) {
                                    auto* ref = static_cast<const TSReference*>(target_vd.value_data);
                                    if (ref->is_empty()) continue;
                                }
                                nb::object field_val = target_vd.ops->delta_to_python(target_vd);
                                if (!field_val.is_none()) {
                                    result[field_name] = field_val;
                                }
                            }
                        }
                    } else {
                        auto* rl = static_cast<const REFLink*>(link_tuple.at(i + 1).data());
                        if (rl && rl->target().is_linked && rl->target().ops) {
                            ViewData target_vd = make_view_data_from_link(*rl, vd.path.child(i));
                            if (target_vd.ops->valid(target_vd) &&
                                target_vd.ops->modified(target_vd, current_time)) {
                                // Same REF emptiness check for REFLink path
                                if (target_vd.meta && target_vd.meta->kind == TSKind::REF &&
                                    target_vd.value_data) {
                                    auto* ref = static_cast<const TSReference*>(target_vd.value_data);
                                    if (ref->is_empty()) continue;
                                }
                                nb::object field_val = target_vd.ops->delta_to_python(target_vd);
                                if (!field_val.is_none()) {
                                    result[field_name] = field_val;
                                }
                            }
                        }
                    }
                } else if (field_meta && (field_meta->is_collection() || field_meta->kind == TSKind::TSB)) {
                    // Collection/composite fields: use child_at to get field view with proper link data
                    TSView field_view = child_at(vd, i, current_time);
                    const auto& fvd = field_view.view_data();
                    if (fvd.valid() && fvd.ops &&
                        fvd.ops->valid(fvd) && fvd.ops->modified(fvd, current_time)) {
                        nb::object field_val = fvd.ops->delta_to_python(fvd);
                        if (!field_val.is_none()) {
                            result[field_name] = field_val;
                        }
                    }
                }
            }
            return result;
        }
    }

    // Output bundles (no links) - use local time storage to check per-field modified+valid
    if (vd.meta && vd.meta->field_count > 0 && vd.time_data) {
        auto time_view = make_time_view(vd);
        auto time_tuple = time_view.as_tuple();
        auto value_view = make_value_view(vd);
        auto value_indexed = value_view.as_bundle();

        // Bundle-level time is the current engine time (set during from_python)
        engine_time_t bundle_time = time_tuple.at(0).as<engine_time_t>();

        nb::dict result;
        for (size_t i = 0; i < vd.meta->field_count; ++i) {
            const TSBFieldInfo& field_info = vd.meta->fields[i];
            const TSMeta* field_meta = field_info.ts_type;
            const char* field_name = field_info.name;

            // Check if this field is valid and modified
            value::View field_time = time_tuple.at(i + 1);
            if (!field_time) continue;

            engine_time_t ft;
            if (field_meta && (field_meta->is_collection() || field_meta->kind == TSKind::TSB)) {
                ft = field_time.as_tuple().at(0).as<engine_time_t>();
            } else {
                ft = *static_cast<const engine_time_t*>(field_time.data());
            }

            bool field_valid = (ft != MIN_DT);
            bool field_modified = (ft >= bundle_time);

            if (field_valid && field_modified) {
                // Python TSB delta_value filters by ts.valid which for REF returns
                // has_output (False for empty refs). Match that here.
                if (field_meta && field_meta->kind == TSKind::REF) {
                    value::View field_value = value_indexed.at(i);
                    if (field_value.valid()) {
                        auto* ref = static_cast<const TSReference*>(field_value.data());
                        if (ref->is_empty()) continue;
                    }
                }
                if (field_meta && (field_meta->is_collection() || field_meta->kind == TSKind::TSB)) {
                    // Composite field: use TS-level delta_to_python
                    ViewData field_vd;
                    field_vd.path = vd.path.child(i);
                    field_vd.value_data = value_indexed.at(i).data();
                    field_vd.time_data = time_tuple.at(i + 1).data();
                    if (vd.observer_data) {
                        auto observer_view = make_observer_view(vd);
                        field_vd.observer_data = observer_view.as_tuple().at(i + 1).data();
                    }
                    field_vd.delta_data = nullptr;
                    field_vd.uses_link_target = vd.uses_link_target;
                    field_vd.ops = get_ts_ops(field_meta);
                    field_vd.meta = field_meta;
                    nb::object py_val = field_vd.ops->delta_to_python(field_vd);
                    if (!py_val.is_none()) {
                        result[field_name] = py_val;
                    }
                } else {
                    value::View field_value = value_indexed.at(i);
                    if (field_value.valid()) {
                        result[field_name] = field_value.to_python();
                    }
                }
            }
        }
        return result;
    }

    return to_python(vd);
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("from_python on invalid ViewData");
    }

    // Determine which fields are present in src (dict keys or object attributes).
    // Only these fields should have their time stamped and observers notified,
    // matching Python behavior where only dict keys trigger per-field updates.
    std::vector<bool> field_modified;
    if (vd.meta && vd.meta->field_count > 0) {
        field_modified.resize(vd.meta->field_count, false);

        if (nb::isinstance<nb::dict>(src)) {
            nb::dict d = nb::cast<nb::dict>(src);
            for (size_t i = 0; i < vd.meta->field_count; ++i) {
                const char* name = vd.meta->fields[i].name;
                if (name && d.contains(name)) {
                    nb::object val = d[name];
                    if (!val.is_none()) {
                        field_modified[i] = true;
                    }
                }
            }
        } else {
            // CompoundScalar or other object: check attributes
            for (size_t i = 0; i < vd.meta->field_count; ++i) {
                const char* name = vd.meta->fields[i].name;
                if (name && nb::hasattr(src, name)) {
                    nb::object val = nb::getattr(src, name);
                    if (!val.is_none()) {
                        field_modified[i] = true;
                    }
                }
            }
        }
    }

    auto dst = make_value_view(vd);
    dst.from_python(src);

    bool any_modified = false;
    auto time_view = make_time_view(vd);

    // Only update field-level times for fields actually present in src
    if (vd.meta) {
        auto time_tuple = time_view.as_tuple();
        for (size_t i = 0; i < vd.meta->field_count; ++i) {
            if (!field_modified[i]) continue;
            any_modified = true;

            value::View field_time = time_tuple.at(i + 1);  // +1 for bundle-level time
            if (field_time) {
                const TSMeta* field_meta = vd.meta->fields[i].ts_type;
                if (field_meta && field_meta->kind == TSKind::TSB) {
                    // Nested TSB: set container time AND recursively set sub-field times
                    auto nested_time_tuple = field_time.as_tuple();
                    nested_time_tuple.at(0).as<engine_time_t>() = current_time;
                    // Recursively stamp all sub-fields so to_python considers them valid
                    for (size_t j = 0; j < field_meta->field_count; ++j) {
                        value::View sub_field_time = nested_time_tuple.at(j + 1);
                        if (sub_field_time) {
                            const TSMeta* sub_meta = field_meta->fields[j].ts_type;
                            if (sub_meta && (sub_meta->is_collection() || sub_meta->kind == TSKind::TSB)) {
                                sub_field_time.as_tuple().at(0).as<engine_time_t>() = current_time;
                            } else {
                                *static_cast<engine_time_t*>(sub_field_time.data()) = current_time;
                            }
                        }
                    }
                } else if (field_meta && field_meta->is_collection()) {
                    // Collection field: time is tuple[engine_time_t, elem_times...]
                    auto coll_time_tuple = field_time.as_tuple();
                    coll_time_tuple.at(0).as<engine_time_t>() = current_time;

                    // For fixed-size TSL: stamp all element times since from_python sets all elements
                    if (field_meta->kind == TSKind::TSL && field_meta->fixed_size > 0) {
                        auto elem_times = coll_time_tuple.at(1).as_list();
                        size_t max_idx = std::min(static_cast<size_t>(field_meta->fixed_size), elem_times.size());
                        for (size_t j = 0; j < max_idx; ++j) {
                            elem_times.at(j).as<engine_time_t>() = current_time;
                        }
                    }
                } else {
                    // Scalar field: time is just engine_time_t
                    *static_cast<engine_time_t*>(field_time.data()) = current_time;
                }
            }
        }
    }

    // Only stamp bundle-level time if any field was actually modified
    if (any_modified) {
        time_view.as_tuple().at(0).as<engine_time_t>() = current_time;
    }

    // Notify bundle-level observers if any field was modified
    if (any_modified && vd.observer_data) {
        auto observer_view = make_observer_view(vd);
        auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
        observers->notify_modified(current_time);

        // Only notify field-level observers for fields that were actually modified
        if (vd.meta) {
            auto observer_tuple = observer_view.as_tuple();
            for (size_t i = 0; i < vd.meta->field_count; ++i) {
                if (!field_modified[i]) continue;

                value::View field_obs = observer_tuple.at(i + 1);  // +1 for bundle-level observer
                if (field_obs) {
                    const TSMeta* field_meta = vd.meta->fields[i].ts_type;
                    ObserverList* field_observers = nullptr;

                    if (field_meta && (field_meta->is_collection() || field_meta->kind == TSKind::TSB)) {
                        // Composite field: observer is tuple[ObserverList, ...]
                        field_observers = static_cast<ObserverList*>(field_obs.as_tuple().at(0).data());
                    } else {
                        // Scalar field: observer is just ObserverList
                        field_observers = static_cast<ObserverList*>(field_obs.data());
                    }

                    if (field_observers) {
                        field_observers->notify_modified(current_time);
                    }
                }
            }
        }
    }
}

TSView child_at(const ViewData& vd, size_t index, engine_time_t current_time) {
    if (!vd.meta || index >= vd.meta->field_count) {
        return TSView{};
    }

    const TSMeta* field_meta = vd.meta->fields[index].ts_type;

    // For TSOutput: follow REFLink early (REFLink navigation is different from scalar ops)
    // For TSInput: do NOT follow LinkTarget early - scalar_ops handle it lazily
    //             (preserves link_data so is_bound() works correctly)
    if (field_meta && field_meta->is_scalar_ts() && !vd.uses_link_target) {
        // TSOutput: Check REFLink
        auto* rl = get_scalar_field_ref_link(vd, index);
        if (rl && rl->target().valid()) {
            bool is_sampled = vd.sampled || is_ref_sampled(*rl, current_time);
            ViewData target_vd = make_view_data_from_link(*rl, vd.path.child(index), is_sampled);
            return TSView(target_vd, current_time);
        }
    }

    auto value_view = make_value_view(vd);
    auto time_view = make_time_view(vd);
    auto observer_view = make_observer_view(vd);

    // Create ViewData for the field
    ViewData field_vd;
    field_vd.path = vd.path.child(index);
    field_vd.value_data = value_view.as_bundle().at(index).data();
    field_vd.time_data = time_view.as_tuple().at(index + 1).data();
    field_vd.observer_data = observer_view.as_tuple().at(index + 1).data();
    field_vd.delta_data = nullptr;  // Field deltas not supported yet
    field_vd.sampled = vd.sampled;  // Propagate sampled flag from parent
    field_vd.uses_link_target = vd.uses_link_target;  // Propagate link type flag

    // Set link_data for binding support
    // - For scalar fields: points to the LinkTarget/REFLink (so binding stores target there)
    // - For composite fields: points to the field's nested link storage
    field_vd.link_data = get_field_link_data(vd, index);

    field_vd.ops = get_ts_ops(field_meta);
    field_vd.meta = field_meta;

    return TSView(field_vd, current_time);
}

TSView child_by_name(const ViewData& vd, const std::string& name, engine_time_t current_time) {
    if (!vd.meta) return TSView{};

    // Find field index (child_at will handle per-field link checking)
    for (size_t i = 0; i < vd.meta->field_count; ++i) {
        if (name == vd.meta->fields[i].name) {
            return child_at(vd, i, current_time);
        }
    }

    return TSView{};
}

TSView child_by_key(const ViewData& vd, const value::View& key, engine_time_t current_time) {
    // Bundles don't support key access
    return TSView{};
}

size_t child_count(const ViewData& vd) {
    return vd.meta ? vd.meta->field_count : 0;
}

value::View observer(const ViewData& vd) {
    return make_observer_view(vd);
}

void notify_observers(ViewData& vd, engine_time_t current_time) {
    if (vd.observer_data) {
        auto observer_view = make_observer_view(vd);
        auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
        observers->notify_modified(current_time);
    }
}

// ============================================================================
// BundleREFBindingHelper: Manages binding when a TSB input binds to a REF output.
//
// When a TSB input (e.g., TSB[AB]) binds to a REF output (e.g., REF[TSB[AB]]),
// we can't navigate the REF's children at bind time. Instead, we:
// 1. Subscribe to the REF source's observer list for change notifications
// 2. On notify, resolve the TSReference to get the underlying TSB
// 3. Bind each field's LinkTarget to the resolved target's fields
// ============================================================================

struct BundleREFBindingHelper : public Notifiable {
    LinkTarget* container_lt;
    ViewData input_vd;           // TSB input's ViewData (for reconstructing link/time tuples)
    ViewData ref_source;         // REF output's ViewData
    bool subscribed_to_ref{false};

    // Track per-field observer subscriptions for cleanup
    struct FieldSub {
        LinkTarget* lt;
        void* observer_data;
    };
    std::vector<FieldSub> field_subs;

    explicit BundleREFBindingHelper(LinkTarget* ct, const ViewData& ivd, const ViewData& ref_src)
        : container_lt(ct), input_vd(ivd), ref_source(ref_src) {}

    ~BundleREFBindingHelper() override {
        // Don't unsubscribe here — done explicitly before delete to avoid use-after-free
    }

    void subscribe_to_ref_source() {
        if (!subscribed_to_ref && ref_source.observer_data) {
            auto* obs = static_cast<ObserverList*>(ref_source.observer_data);
            obs->add_observer(this);
            subscribed_to_ref = true;
        }
    }

    void unsubscribe_from_ref_source() {
        if (subscribed_to_ref && ref_source.observer_data) {
            auto* obs = static_cast<ObserverList*>(ref_source.observer_data);
            obs->remove_observer(this);
            subscribed_to_ref = false;
        }
    }

    void unsubscribe_fields() {
        for (auto& fs : field_subs) {
            if (fs.observer_data && fs.lt) {
                auto* obs = static_cast<ObserverList*>(fs.observer_data);
                obs->remove_observer(fs.lt);
                if (fs.lt->active_notifier.owning_input != nullptr) {
                    obs->remove_observer(&fs.lt->active_notifier);
                }
            }
        }
        field_subs.clear();
    }

    void unsubscribe_all() {
        unsubscribe_fields();
        unsubscribe_from_ref_source();
    }

    void clear_field_link_targets() {
        auto link_schema = get_bundle_link_schema(input_vd);
        if (!link_schema) return;

        value::View link_view(input_vd.link_data, link_schema);
        auto link_tuple = link_view.as_tuple();

        for (size_t i = 0; i < input_vd.meta->field_count; ++i) {
            const TSMeta* field_meta = input_vd.meta->fields[i].ts_type;
            if (field_meta && field_meta->is_scalar_ts()) {
                auto* lt = static_cast<LinkTarget*>(link_tuple.at(i + 1).data());
                if (lt) {
                    lt->is_linked = false;
                    lt->value_data = nullptr;
                    lt->time_data = nullptr;
                    lt->observer_data = nullptr;
                    lt->delta_data = nullptr;
                    lt->link_data = nullptr;
                    lt->ops = nullptr;
                    lt->meta = nullptr;
                }
            }
        }
    }

    void rebind(engine_time_t current_time) {
        // 1. Unsubscribe and clear old field bindings
        unsubscribe_fields();
        clear_field_link_targets();

        // 2. Read TSReference from REF source
        if (!ref_source.value_data || !ref_source.meta) return;
        auto value_meta = ref_source.meta->value_type;
        if (!value_meta) return;

        value::View v(ref_source.value_data, value_meta);
        if (!v.valid()) return;

        const auto* ts_ref = static_cast<const TSReference*>(v.data());
        if (!ts_ref || ts_ref->is_empty()) return;

        if (ts_ref->is_peered()) {
            // PEERED reference: resolve to get the underlying TSB target
            TSView resolved;
            try {
                resolved = ts_ref->resolve(current_time);
            } catch (...) {
                return;
            }
            if (!resolved) return;

            // Dereference if resolved target is itself REF
            ViewData resolved_vd = resolved.view_data();
            if (resolved_vd.meta && resolved_vd.meta->kind == TSKind::REF) {
                auto inner = resolve_ref_link_target_from_vd(resolved_vd, current_time);
                if (inner) resolved_vd = *inner;
            }

            // Bind each field to the resolved target's fields
            bind_fields(resolved_vd, current_time);
        } else {
            // NON_PEERED reference: resolve each item individually.
            // Items are PEERED TSReferences pointing to individual fields.
            bind_non_peered_items(*ts_ref, current_time);
        }
    }

    void bind_non_peered_items(const TSReference& ref, engine_time_t current_time) {
        auto link_schema = get_bundle_link_schema(input_vd);
        if (!link_schema) return;

        value::View link_view(input_vd.link_data, link_schema);
        auto link_tuple = link_view.as_tuple();

        auto* time_schema = TSMetaSchemaCache::instance().get_time_schema(input_vd.meta);
        value::TupleView time_tuple;
        if (time_schema && input_vd.time_data) {
            value::View time_view(input_vd.time_data, time_schema);
            time_tuple = time_view.as_tuple();
        }

        TSInput* owning_input = container_lt->active_notifier.owning_input;

        size_t item_count = ref.size();
        for (size_t i = 0; i < input_vd.meta->field_count && i < item_count; ++i) {
            const TSMeta* field_meta = input_vd.meta->fields[i].ts_type;
            const auto& item = ref[i];

            if (item.is_empty() || !item.is_peered()) continue;

            // Resolve this item's ShortPath to get the actual field target
            TSView resolved;
            try {
                resolved = item.resolve(current_time);
            } catch (...) {
                continue;
            }
            if (!resolved) continue;

            // Dereference if resolved target is itself REF
            ViewData field_target_vd = resolved.view_data();
            if (field_target_vd.meta && field_target_vd.meta->kind == TSKind::REF) {
                auto inner = resolve_ref_link_target_from_vd(field_target_vd, current_time);
                if (inner) field_target_vd = *inner;
            }

            if (field_meta && field_meta->is_scalar_ts()) {
                auto* lt = static_cast<LinkTarget*>(link_tuple.at(i + 1).data());
                if (lt) {
                    store_to_link_target(*lt, field_target_vd);
                    if (time_tuple) {
                        lt->owner_time_ptr = static_cast<engine_time_t*>(time_tuple.at(i + 1).data());
                    }
                    lt->parent_link = container_lt;

                    if (lt->observer_data) {
                        auto* obs = static_cast<ObserverList*>(lt->observer_data);
                        obs->add_observer(lt);
                        field_subs.push_back({lt, lt->observer_data});

                        if (owning_input && lt->active_notifier.owning_input == nullptr) {
                            lt->active_notifier.owning_input = owning_input;
                        }
                        if (lt->active_notifier.owning_input != nullptr) {
                            obs->add_observer(&lt->active_notifier);
                        }
                    }
                }
            } else {
                // Composite field item: recursively bind
                ViewData field_vd;
                field_vd.link_data = link_tuple.at(i + 1).data();
                field_vd.meta = field_meta;
                field_vd.uses_link_target = input_vd.uses_link_target;
                field_vd.ops = get_ts_ops(field_meta);
                if (time_tuple) {
                    field_vd.time_data = time_tuple.at(i + 1).data();
                }
                if (field_vd.ops && field_vd.ops->bind) {
                    field_vd.ops->bind(field_vd, field_target_vd);
                }
            }
        }
    }

    void bind_fields(const ViewData& resolved_vd, engine_time_t current_time) {
        auto link_schema = get_bundle_link_schema(input_vd);
        if (!link_schema) return;

        value::View link_view(input_vd.link_data, link_schema);
        auto link_tuple = link_view.as_tuple();

        auto* time_schema = TSMetaSchemaCache::instance().get_time_schema(input_vd.meta);
        value::TupleView time_tuple;
        if (time_schema && input_vd.time_data) {
            value::View time_view(input_vd.time_data, time_schema);
            time_tuple = time_view.as_tuple();
        }

        // Get the owning input for active_notifier setup (stored on container_lt)
        TSInput* owning_input = container_lt->active_notifier.owning_input;

        for (size_t i = 0; i < input_vd.meta->field_count; ++i) {
            const TSMeta* field_meta = input_vd.meta->fields[i].ts_type;

            // Navigate to resolved target's field
            if (!resolved_vd.ops) continue;
            TSView target_field = resolved_vd.ops->child_at(resolved_vd, i, current_time);
            if (!target_field) continue;

            if (field_meta && field_meta->is_scalar_ts()) {
                auto* lt = static_cast<LinkTarget*>(link_tuple.at(i + 1).data());
                if (lt) {
                    store_to_link_target(*lt, target_field.view_data());
                    if (time_tuple) {
                        lt->owner_time_ptr = static_cast<engine_time_t*>(time_tuple.at(i + 1).data());
                    }
                    lt->parent_link = container_lt;

                    // Subscribe for time-accounting
                    if (lt->observer_data) {
                        auto* obs = static_cast<ObserverList*>(lt->observer_data);
                        obs->add_observer(lt);
                        field_subs.push_back({lt, lt->observer_data});

                        // Also subscribe ActiveNotifier if input is active
                        if (owning_input && lt->active_notifier.owning_input == nullptr) {
                            lt->active_notifier.owning_input = owning_input;
                        }
                        if (lt->active_notifier.owning_input != nullptr) {
                            obs->add_observer(&lt->active_notifier);
                        }
                    }
                }
            } else {
                // Composite field: recursively bind using nested link storage
                ViewData field_vd;
                field_vd.link_data = link_tuple.at(i + 1).data();
                field_vd.meta = field_meta;
                field_vd.uses_link_target = input_vd.uses_link_target;
                field_vd.ops = get_ts_ops(field_meta);
                if (time_tuple) {
                    field_vd.time_data = time_tuple.at(i + 1).data();
                }
                if (field_vd.ops && field_vd.ops->bind) {
                    field_vd.ops->bind(field_vd, target_field.view_data());
                }
                // Set nested container's parent_link
                if (container_lt) {
                    auto nested_link_schema = get_bundle_link_schema(field_vd);
                    if (nested_link_schema) {
                        value::View nested_link(field_vd.link_data, nested_link_schema);
                        auto nested_tuple = nested_link.as_tuple();
                        auto* nested_container_lt = static_cast<LinkTarget*>(nested_tuple.at(0).data());
                        if (nested_container_lt) {
                            nested_container_lt->parent_link = container_lt;
                        }
                    }
                }
            }
        }
    }

    // Called when REF source changes - rebind to new target and schedule node
    void notify(engine_time_t et) override {
        // REF source changed - rebind field LinkTargets to new resolved target
        rebind(et);

        // Time-accounting: update container-level time
        container_lt->notify(et);

        // Schedule owning node if active
        if (container_lt->active_notifier.owning_input) {
            container_lt->active_notifier.notify(et);
        }
    }
};

void delete_bundle_ref_binding_helper(void* ptr) {
    delete static_cast<BundleREFBindingHelper*>(ptr);
}

void bind(ViewData& vd, const ViewData& target) {
    // TSB: Bind the entire bundle to target
    // This binds all fields to corresponding fields in the target
    if (!vd.link_data || !vd.meta) {
        throw std::runtime_error("bind on bundle without link data");
    }

    auto link_schema = get_bundle_link_schema(vd);
    if (!link_schema) {
        throw std::runtime_error("bind on bundle without link schema");
    }

    // For TSB, link_data points to tuple[LinkType, link_schema(field_0), ...]
    // where LinkType is LinkTarget (TSInput) or REFLink (TSOutput)
    // Bind each field to the corresponding field in target
    value::View link_view(vd.link_data, link_schema);
    auto link_tuple = link_view.as_tuple();

    // Set up container-level LinkTarget for time-accounting (TSInput path)
    LinkTarget* container_lt = nullptr;
    value::TupleView time_tuple;
    if (vd.uses_link_target) {
        container_lt = static_cast<LinkTarget*>(link_tuple.at(0).data());
        // Mark as peered (binding happened at bundle level)
        if (container_lt) container_lt->peered = true;
        if (vd.time_data) {
            auto time_schema = TSMetaSchemaCache::instance().get_time_schema(vd.meta);
            if (time_schema) {
                value::View time_view(vd.time_data, time_schema);
                time_tuple = time_view.as_tuple();
                // Container LT's owner_time_ptr = container-level time (element 0)
                if (container_lt && time_tuple) {
                    container_lt->owner_time_ptr = static_cast<engine_time_t*>(time_tuple.at(0).data());
                }
            }
        }
        // parent_link set by caller if this bundle is nested
        // Container LT is NOT subscribed to any observer — it receives from children
    }

    // Check if target is a REF type — need deferred binding via BundleREFBindingHelper
    if (target.meta && target.meta->kind == TSKind::REF && vd.uses_link_target && container_lt) {
        // Clean up existing REF binding if any
        if (container_lt->ref_binding_) {
            auto* old_helper = static_cast<BundleREFBindingHelper*>(container_lt->ref_binding_);
            old_helper->unsubscribe_all();
            if (container_lt->ref_binding_deleter_) {
                container_lt->ref_binding_deleter_(container_lt->ref_binding_);
            }
            container_lt->ref_binding_ = nullptr;
            container_lt->ref_binding_deleter_ = nullptr;
        }

        auto* helper = new BundleREFBindingHelper(container_lt, vd, target);
        container_lt->ref_binding_ = helper;
        container_lt->ref_binding_deleter_ = &delete_bundle_ref_binding_helper;

        // Subscribe to REF source observer list for rebind notifications
        helper->subscribe_to_ref_source();

        // Initial resolve and bind (may succeed if REF already has a value)
        helper->rebind(MIN_DT);
        return;
    }

    // Get target field data for each field
    for (size_t i = 0; i < vd.meta->field_count; ++i) {
        const TSMeta* field_meta = vd.meta->fields[i].ts_type;

        // Navigate to target's field
        // Note: We check structural validity (operator bool), not time-series validity (valid())
        // The field may not have a value yet, but the structure should be valid for binding
        TSView target_field = target.ops->child_at(target, i, MIN_DT);
        if (!target_field) continue;  // Skip if structurally invalid (no view data)

        if (field_meta && field_meta->is_scalar_ts()) {
            if (vd.uses_link_target) {
                // TSInput: Scalar field uses LinkTarget
                auto* lt = static_cast<LinkTarget*>(link_tuple.at(i + 1).data());
                if (lt) {
                    const auto& target_vd = target_field.view_data();

                    // Check if target field is REF kind but input field is not REF.
                    // This requires REFBindingHelper for auto-dereference (TS→REF mode).
                    // Without this, the LinkTarget would store the REF's raw data
                    // (TimeSeriesReference) instead of the dereferenced value.
                    if (target_vd.meta && target_vd.meta->kind == TSKind::REF
                        && field_meta->kind != TSKind::REF) {
                        // Set parent_link before delegating to scalar bind
                        lt->parent_link = container_lt;

                        // Construct field ViewData and delegate to scalar bind
                        // which creates REFBindingHelper for REF targets
                        ViewData field_vd;
                        field_vd.link_data = lt;
                        field_vd.meta = field_meta;
                        field_vd.uses_link_target = true;
                        field_vd.ops = get_ts_ops(field_meta);
                        if (time_tuple) {
                            field_vd.time_data = time_tuple.at(i + 1).data();
                        }
                        field_vd.ops->bind(field_vd, target_vd);
                    } else {
                        store_to_link_target(*lt, target_vd);

                        // Set time-accounting chain
                        if (time_tuple) {
                            lt->owner_time_ptr = static_cast<engine_time_t*>(time_tuple.at(i + 1).data());
                        }
                        lt->parent_link = container_lt;

                        // Check for TS→REF binding: REF input field bound to non-REF target.
                        // The reference is fixed at bind time — do NOT subscribe to the target.
                        // Python equivalent: do_bind_output sets _output=None (no subscription).
                        bool is_ts_to_ref = (field_meta->kind == TSKind::REF &&
                                             target_vd.meta && target_vd.meta->kind != TSKind::REF);

                        if (is_ts_to_ref) {
                            // Mark the field as valid (MIN_ST) so validity check passes.
                            if (time_tuple) {
                                auto* field_time = static_cast<engine_time_t*>(time_tuple.at(i + 1).data());
                                *field_time = MIN_ST;
                            }
                            // Point time_data to owner's time for delegation
                            lt->time_data = lt->owner_time_ptr;
                            // Subscribe for time-accounting: Python's make_active subscribes
                            // to the output, so the node is notified on value changes.
                            if (lt->observer_data) {
                                auto* obs = static_cast<ObserverList*>(lt->observer_data);
                                obs->add_observer(lt);
                            }
                        } else {
                            // Subscribe for time-accounting (always, regardless of active state)
                            if (lt->observer_data) {
                                auto* obs = static_cast<ObserverList*>(lt->observer_data);
                                obs->add_observer(lt);
                            }
                        }
                    }
                }
            } else {
                // TSOutput: Scalar field uses REFLink
                auto* rl = static_cast<REFLink*>(link_tuple.at(i + 1).data());
                if (rl) {
                    store_link_target(*rl, target_field.view_data());
                }
            }
        } else {
            // Composite field: recursively bind using the nested link storage
            ViewData field_vd;
            field_vd.link_data = link_tuple.at(i + 1).data();
            field_vd.meta = field_meta;
            field_vd.uses_link_target = vd.uses_link_target;  // Propagate flag
            field_vd.ops = get_ts_ops(field_meta);

            // Pass time data for nested composite's time-accounting setup
            if (time_tuple && vd.uses_link_target) {
                field_vd.time_data = time_tuple.at(i + 1).data();
            }

            if (field_vd.ops && field_vd.ops->bind) {
                field_vd.ops->bind(field_vd, target_field.view_data());
            }

            // Set nested container's parent_link to this container (TSInput path)
            if (vd.uses_link_target && container_lt) {
                // Only TSB and TSD have a container-level LinkTarget at element 0
                // of their link tuple. TSL uses fixed_list[LinkTarget] without a
                // container-level entry.
                if (field_meta && (field_meta->kind == TSKind::TSB || field_meta->kind == TSKind::TSD)) {
                    auto nested_link_schema = get_bundle_link_schema(field_vd);
                    if (nested_link_schema) {
                        value::View nested_link(field_vd.link_data, nested_link_schema);
                        auto nested_tuple = nested_link.as_tuple();
                        auto* nested_container_lt = static_cast<LinkTarget*>(nested_tuple.at(0).data());
                        if (nested_container_lt) {
                            nested_container_lt->parent_link = container_lt;
                        }
                    }
                }
            }
        }
    }
}

void unbind(ViewData& vd) {
    if (!vd.link_data || !vd.meta) {
        return;  // No-op if not linked
    }

    auto link_schema = get_bundle_link_schema(vd);
    if (!link_schema) return;

    value::View link_view(vd.link_data, link_schema);
    auto link_tuple = link_view.as_tuple();

    // Clean up container-level BundleREFBindingHelper if present
    if (vd.uses_link_target) {
        auto* container_lt = static_cast<LinkTarget*>(link_tuple.at(0).data());
        if (container_lt && container_lt->ref_binding_) {
            auto* helper = static_cast<BundleREFBindingHelper*>(container_lt->ref_binding_);
            helper->unsubscribe_all();
            container_lt->cleanup_ref_binding();
        }
    }

    for (size_t i = 0; i < vd.meta->field_count; ++i) {
        const TSMeta* field_meta = vd.meta->fields[i].ts_type;

        if (field_meta && field_meta->is_scalar_ts()) {
            if (vd.uses_link_target) {
                // TSInput: Unsubscribe both chains then clear LinkTarget.
                // Delegate to scalar_ops::unbind to properly handle REFBindingHelper cleanup.
                auto* lt = static_cast<LinkTarget*>(link_tuple.at(i + 1).data());
                if (lt) {
                    if (lt->ref_binding_) {
                        // REF binding: explicitly unsubscribe before deleting
                        auto* helper = static_cast<REFBindingHelper*>(lt->ref_binding_);
                        helper->unsubscribe_all();
                        lt->cleanup_ref_binding();
                        lt->clear();
                    } else if (lt->is_linked) {
                        if (lt->observer_data) {
                            auto* obs = static_cast<ObserverList*>(lt->observer_data);
                            obs->remove_observer(lt);
                            if (lt->active_notifier.owning_input != nullptr) {
                                obs->remove_observer(&lt->active_notifier);
                                lt->active_notifier.owning_input = nullptr;
                            }
                        }
                        lt->clear();
                    }
                }
            } else {
                // TSOutput: Scalar field uses REFLink - unbind it
                auto* rl = static_cast<REFLink*>(link_tuple.at(i + 1).data());
                if (rl) {
                    rl->unbind();
                }
            }
        } else {
            // Composite field: recursively unbind
            ViewData field_vd;
            field_vd.link_data = link_tuple.at(i + 1).data();
            field_vd.meta = field_meta;
            field_vd.uses_link_target = vd.uses_link_target;  // Propagate flag
            field_vd.ops = get_ts_ops(field_meta);
            if (field_vd.ops && field_vd.ops->unbind) {
                field_vd.ops->unbind(field_vd);
            }
        }
    }

    // Clear container-level LinkTarget structural fields (TSInput path)
    if (vd.uses_link_target) {
        auto* container_lt = static_cast<LinkTarget*>(link_tuple.at(0).data());
        if (container_lt) {
            container_lt->owner_time_ptr = nullptr;
            container_lt->parent_link = nullptr;
            container_lt->last_notify_time = MIN_DT;
        }
    }
}

bool is_bound(const ViewData& vd) {
    // TSB is bound if any field is linked
    if (any_field_linked(vd)) return true;

    // Also bound if container has a BundleREFBindingHelper (REF target, fields not yet resolved)
    if (vd.uses_link_target && vd.link_data && vd.meta) {
        auto link_schema = get_bundle_link_schema(vd);
        if (link_schema) {
            value::View link_view(vd.link_data, link_schema);
            auto* container_lt = static_cast<const LinkTarget*>(link_view.as_tuple().at(0).data());
            if (container_lt && container_lt->ref_binding_) return true;
        }
    }
    return false;
}

bool is_peered(const ViewData& vd) {
    // TSB is peered if the container-level LinkTarget has peered=true
    if (!vd.link_data || !vd.uses_link_target || !vd.meta) return false;
    auto link_schema = get_bundle_link_schema(vd);
    if (!link_schema) return false;
    value::View link_view(vd.link_data, link_schema);
    auto link_tuple = link_view.as_tuple();
    auto* container_lt = static_cast<const LinkTarget*>(link_tuple.at(0).data());
    return container_lt && container_lt->peered;
}

void set_active(ViewData& vd, value::View active_view, bool active, TSInput* input) {
    if (!active_view || !vd.meta) return;

    // TSB active schema: tuple[bool, active_schema(field_0), active_schema(field_1), ...]
    value::TupleView tv = active_view.as_tuple();
    value::View root = tv[0];
    if (root) {
        *static_cast<bool*>(root.data()) = active;
    }

    // Get link view for subscription management
    auto* link_schema = get_bundle_link_schema(vd);
    value::View link_view = link_schema ? value::View(vd.link_data, link_schema) : value::View{};
    value::TupleView link_tuple = link_view ? link_view.as_tuple() : value::TupleView{};

    // Process each field
    for (size_t i = 0; i < vd.meta->field_count; ++i) {
        value::View field_active = tv[i + 1]; // +1 because index 0 is root bool
        if (!field_active) continue;

        const TSMeta* field_ts = vd.meta->fields[i].ts_type;

        // Set the field's active state
        if (field_ts->is_collection() || field_ts->kind == TSKind::TSB) {
            // Composite field - recurse
            ViewData field_vd;
            field_vd.meta = field_ts;
            field_vd.uses_link_target = vd.uses_link_target;  // Propagate flag
            field_vd.ops = get_ts_ops(field_ts);
            // Get link data for this field (tuple index i+1 since element 0 is bundle-level link)
            if (link_tuple) {
                field_vd.link_data = link_tuple.at(i + 1).data();
            }
            field_vd.ops->set_active(field_vd, field_active, active, input);
        } else {
            // Scalar field - set directly
            *static_cast<bool*>(field_active.data()) = active;
        }

        // Manage node-scheduling subscription for bound scalar fields
        if (field_ts->is_scalar_ts() && link_tuple) {
            if (vd.uses_link_target) {
                // TSInput: Use ActiveNotifier for node-scheduling
                auto* lt = static_cast<LinkTarget*>(link_tuple.at(i + 1).data());

                // Detect TS→REF binding: REF field bound to non-REF target.
                // The reference is fixed - do NOT subscribe to the target's observer list.
                bool field_is_ts_to_ref = (field_ts->kind == TSKind::REF &&
                                           lt && lt->is_linked && lt->meta &&
                                           lt->meta->kind != TSKind::REF);

                if (field_is_ts_to_ref) {
                    // TS→REF: Fixed reference to non-REF target.
                    // Python's PythonTimeSeriesReferenceInput.make_active() subscribes
                    // to the output observer list, so the node IS scheduled when the
                    // underlying value changes. We must do the same.
                    if (active) {
                        lt->active_notifier.owning_input = input;
                        // Subscribe to the target's observer list so we're notified
                        // when the underlying value changes (matches Python make_active)
                        if (lt->observer_data) {
                            auto* observers = static_cast<ObserverList*>(lt->observer_data);
                            observers->add_observer(&lt->active_notifier);
                        }
                        // Fire initial notification - the REF is valid from bind time
                        if (input) {
                            input->notify(MIN_ST);
                        }
                    } else {
                        if (lt->observer_data) {
                            auto* observers = static_cast<ObserverList*>(lt->observer_data);
                            observers->remove_observer(&lt->active_notifier);
                        }
                        lt->active_notifier.owning_input = nullptr;
                    }
                } else if (lt && lt->is_linked && lt->observer_data) {
                    auto* observers = static_cast<ObserverList*>(lt->observer_data);
                    if (active) {
                        if (lt->active_notifier.owning_input == nullptr) {
                            lt->active_notifier.owning_input = input;
                            observers->add_observer(&lt->active_notifier);
                        }
                        // Initial notification: if the output is already valid AND modified,
                        // fire notify to schedule the owning node (matches Python make_active behavior).
                        if (input && lt->ops) {
                            ViewData output_vd = make_view_data_from_link_target(*lt, vd.path);
                            if (lt->ops->valid(output_vd)) {
                                auto* node = input->owning_node();
                                if (node && node->cached_evaluation_time_ptr()) {
                                    engine_time_t eval_time = *node->cached_evaluation_time_ptr();
                                    if (lt->ops->modified(output_vd, eval_time)) {
                                        engine_time_t lmt = lt->ops->last_modified_time(output_vd);
                                        input->notify(lmt);
                                    }
                                }
                            }
                        }
                    } else {
                        if (lt->active_notifier.owning_input != nullptr) {
                            observers->remove_observer(&lt->active_notifier);
                            lt->active_notifier.owning_input = nullptr;
                        }
                    }
                } else if (lt && lt->ref_binding_) {
                    // REF binding exists but hasn't resolved yet.
                    // Set owning_input so that when REFBindingHelper::rebind()
                    // resolves the target later, it can subscribe the ActiveNotifier.
                    if (active) {
                        if (lt->active_notifier.owning_input == nullptr) {
                            lt->active_notifier.owning_input = input;
                        }
                    } else {
                        lt->active_notifier.owning_input = nullptr;
                    }
                }
            } else {
                // TSOutput: REFLink - subscribe input directly
                auto* rl = static_cast<REFLink*>(link_tuple.at(i + 1).data());
                if (rl && rl->target().is_linked && rl->target().observer_data) {
                    auto* observers = static_cast<ObserverList*>(rl->target().observer_data);
                    if (active) {
                        observers->add_observer(input);
                    } else {
                        observers->remove_observer(input);
                    }
                }
            }
        }
    }

    // Handle container-level active_notifier for BundleREFBindingHelper
    // When a TSB is bound to a REF target, the container_lt needs owning_input
    // so that BundleREFBindingHelper::rebind() can set up per-field active_notifiers
    if (vd.uses_link_target && link_tuple) {
        auto* container_lt = static_cast<LinkTarget*>(link_tuple.at(0).data());
        if (container_lt && container_lt->ref_binding_) {
            if (active) {
                container_lt->active_notifier.owning_input = input;
            } else {
                container_lt->active_notifier.owning_input = nullptr;
            }
        }
    }
}

} // namespace bundle_ops

// ============================================================================
// List Operations (TSL)
// ============================================================================

namespace list_ops {

// Forward declarations for functions used by all_valid
TSView child_at(const ViewData& vd, size_t index, engine_time_t current_time);
size_t child_count(const ViewData& vd);

// For TSL types:
// - value is list type
// - time is tuple[engine_time_t, list[element_times]]
// - observer is tuple[ObserverList, list[element_observers]]
// - link: For dynamic TSL, single LinkType for collection-level binding
//         For fixed-size TSL, fixed_list[LinkType] for per-element binding
//         where LinkType is LinkTarget (TSInput) or REFLink (TSOutput)

// Helper: Get the appropriate link schema based on uses_link_target flag
inline const value::TypeMeta* get_list_link_schema(const ViewData& vd) {
    if (!vd.meta) return nullptr;
    if (vd.uses_link_target) {
        return TSMetaSchemaCache::instance().get_input_link_schema(vd.meta);
    }
    return TSMetaSchemaCache::instance().get_link_schema(vd.meta);
}

// Helper: Check if this TSL has collection-level linking (dynamic TSL only) via REFLink
// Fixed-size TSL uses per-element binding, so doesn't have collection-level link
// Only valid when uses_link_target is false
inline const REFLink* get_active_link(const ViewData& vd) {
    // Only check for collection-level link if this is a dynamic TSL (fixed_size == 0)
    // Fixed-size TSL uses per-element binding
    if (vd.meta && vd.meta->fixed_size > 0) {
        return nullptr;  // Per-element binding, no collection-level link
    }
    if (vd.uses_link_target) return nullptr;  // Wrong type
    auto* rl = get_ref_link(vd.link_data);
    return (rl && rl->target().valid()) ? rl : nullptr;
}

// Helper: Check if this TSL has collection-level linking (dynamic TSL only) via LinkTarget
// Only valid when uses_link_target is true
inline const LinkTarget* get_active_link_target(const ViewData& vd) {
    // Only check for collection-level link if this is a dynamic TSL (fixed_size == 0)
    if (vd.meta && vd.meta->fixed_size > 0) {
        return nullptr;  // Per-element binding, no collection-level link
    }
    if (!vd.uses_link_target) return nullptr;  // Wrong type
    auto* lt = get_link_target(vd.link_data);
    return (lt && lt->valid()) ? lt : nullptr;
}

// List-specific delegation: uses the list_ops link helpers which skip
// collection-level delegation for fixed-size TSL (per-element binding).
inline std::optional<ViewData> resolve_delegation_target(const ViewData& vd) {
    if (auto* lt = get_active_link_target(vd)) {
        return make_view_data_from_link_target(*lt, vd.path);
    }
    if (auto* rl = get_active_link(vd)) {
        return make_view_data_from_link(*rl, vd.path);
    }
    return std::nullopt;
}

engine_time_t last_modified_time(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->last_modified_time(*target);
    auto time_view = make_time_view(vd);
    if (!time_view.valid()) return MIN_DT;
    return time_view.as_tuple().at(0).as<engine_time_t>();
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->modified(make_view_data_from_link(*rl, vd.path), current_time);
    }

    // Check container time first
    if (last_modified_time(vd) >= current_time) {
        return true;
    }

    // For non-linked TSL (outputs), check if any element is modified
    // This matches Python behavior: any(ts.modified for ts in self.values())
    if (vd.meta && vd.meta->element_ts) {
        size_t count = 0;
        if (vd.meta->fixed_size > 0) {
            count = static_cast<size_t>(vd.meta->fixed_size);
        } else if (vd.value_data) {
            auto value_view = make_value_view(vd);
            if (value_view.valid()) {
                count = value_view.as_list().size();
            }
        }

        for (size_t i = 0; i < count; ++i) {
            TSView child = child_at(vd, i, current_time);
            if (child.view_data().valid() && child.view_data().ops) {
                if (child.view_data().ops->modified(child.view_data(), current_time)) {
                    return true;
                }
            }
        }
    }

    return false;
}

bool valid(const ViewData& vd) {
    // If linked (dynamic TSL) via LinkTarget, delegate to target
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->valid(make_view_data_from_link_target(*lt, vd.path));
    }
    // If linked (dynamic TSL) via REFLink, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->valid(make_view_data_from_link(*rl, vd.path));
    }

    // Check if the list's own time indicates validity
    if (last_modified_time(vd) != MIN_DT) {
        return true;
    }

    // For fixed-size TSL inputs with per-element binding, check if any element link is valid
    if (vd.meta && vd.meta->fixed_size > 0 && vd.link_data) {
        auto* link_schema = get_list_link_schema(vd);
        if (link_schema) {
            value::View link_view(vd.link_data, link_schema);
            auto link_list = link_view.as_list();

            for (size_t i = 0; i < link_list.size() && i < static_cast<size_t>(vd.meta->fixed_size); ++i) {
                if (vd.uses_link_target) {
                    // TSInput: LinkTarget
                    auto* lt = static_cast<const LinkTarget*>(link_list.at(i).data());
                    if (lt && lt->is_linked && lt->ops) {
                        ViewData elem_vd = make_view_data_from_link_target(*lt, vd.path.child(i));
                        if (lt->ops->valid(elem_vd)) {
                            return true;
                        }
                    }
                } else {
                    // TSOutput: REFLink
                    auto* rl = static_cast<const REFLink*>(link_list.at(i).data());
                    if (rl && rl->target().is_linked && rl->target().ops) {
                        ViewData elem_vd = make_view_data_from_link(*rl, vd.path.child(i));
                        if (rl->target().ops->valid(elem_vd)) {
                            return true;
                        }
                    }
                }
            }
        }
    }

    return false;
}

bool all_valid(const ViewData& vd) {
    // If linked via LinkTarget, delegate to target
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->all_valid(make_view_data_from_link_target(*lt, vd.path));
    }
    // If linked via REFLink, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->all_valid(make_view_data_from_link(*rl, vd.path));
    }

    // First check if this list itself is valid
    if (!valid(vd)) return false;
    if (!vd.meta) return false;

    // Use MIN_DT (smallest valid time) as the current_time parameter for child_at
    engine_time_t query_time = MIN_DT;

    // Check all elements using child_at which handles links properly
    size_t count = child_count(vd);
    for (size_t i = 0; i < count; ++i) {
        TSView child_view = child_at(vd, i, query_time);

        // Check if the child is all_valid
        if (!child_view || !child_view.all_valid()) {
            return false;
        }
    }

    return true;
}

bool sampled(const ViewData& vd) {
    // Return the sampled flag from ViewData
    // This flag is set when navigating through a REFLink that was rebound
    // If linked, also check target's sampled flag (propagates through chain)
    if (auto* rl = get_active_link(vd)) {
        return vd.sampled || rl->target().ops->sampled(make_view_data_from_link(*rl, vd.path, vd.sampled));
    }
    return vd.sampled;
}

value::View value(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->value(make_view_data_from_link(*rl, vd.path, vd.sampled));
    }
    return make_value_view(vd);
}

value::View delta_value(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->delta_value(make_view_data_from_link(*rl, vd.path, vd.sampled));
    }
    return make_delta_view(vd);
}

bool has_delta(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->has_delta(make_view_data_from_link(*rl, vd.path));
    }
    if (!vd.delta_data) return false;
    if (!vd.meta) return false;
    return ::hgraph::has_delta(vd.meta->element_ts);
}

void set_value(ViewData& vd, const value::View& src, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("set_value on invalid ViewData");
    }

    auto dst = make_value_view(vd);
    dst.copy_from(src);

    auto time_view = make_time_view(vd);
    time_view.as_tuple().at(0).as<engine_time_t>() = current_time;

    if (vd.observer_data) {
        auto observer_view = make_observer_view(vd);
        auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
        observers->notify_modified(current_time);
    }
}

void apply_delta(ViewData& vd, const value::View& delta, engine_time_t current_time) {
    set_value(vd, delta, current_time);
}

void invalidate(ViewData& vd) {
    if (vd.time_data) {
        auto time_view = make_time_view(vd);
        time_view.as_tuple().at(0).as<engine_time_t>() = MIN_DT;
    }
}

nb::object to_python(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->to_python(*target);
    // Check time-series validity first (has value been set?)
    if (!valid(vd)) return nb::none();

    // Build tuple with None for invalid elements, matching Python:
    // tuple(ts.value if ts.valid else None for ts in self._ts_values)
    if (!vd.meta || !vd.meta->element_ts) {
        auto v = make_value_view(vd);
        if (!v.valid()) return nb::none();
        return v.to_python();
    }

    size_t count = 0;
    if (vd.meta->fixed_size > 0) {
        count = static_cast<size_t>(vd.meta->fixed_size);
    } else if (vd.value_data) {
        auto value_view = make_value_view(vd);
        if (value_view.valid()) {
            count = value_view.as_list().size();
        }
    }

    nb::tuple result = nb::steal<nb::tuple>(PyTuple_New(static_cast<Py_ssize_t>(count)));
    for (size_t i = 0; i < count; ++i) {
        TSView child = child_at(vd, i, MIN_DT);
        nb::object elem;
        if (child.view_data().valid() && child.view_data().ops &&
            child.view_data().ops->valid(child.view_data())) {
            elem = child.view_data().ops->to_python(child.view_data());
        } else {
            elem = nb::none();
        }
        PyTuple_SET_ITEM(result.ptr(), static_cast<Py_ssize_t>(i), elem.release().ptr());
    }
    return result;
}

nb::object delta_to_python(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->delta_to_python(*target);

    // For non-linked TSL, return {index: element.delta_value for modified elements}
    // This matches Python: {k: ts.delta_value for k, ts in self.modified_items()}
    if (!vd.meta || !vd.meta->element_ts) {
        return nb::none();
    }

    // Get element count
    size_t count = 0;
    if (vd.meta->fixed_size > 0) {
        count = static_cast<size_t>(vd.meta->fixed_size);
    } else if (vd.value_data) {
        auto value_view = make_value_view(vd);
        if (value_view.valid()) {
            count = value_view.as_list().size();
        }
    }

    if (count == 0) {
        return nb::none();
    }

    // Get container time and find max element time.
    // For inputs with per-element binding, container_time may be stale (from the input's
    // local storage rather than the output's). Use max of container_time and all element
    // times to determine the current tick threshold.
    engine_time_t container_time = last_modified_time(vd);
    engine_time_t max_elem_time = MIN_DT;

    // First pass: collect element views and find the max time
    struct ElemInfo { TSView child; engine_time_t time; };
    std::vector<ElemInfo> elems;
    elems.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        TSView child = child_at(vd, i, MIN_DT);
        engine_time_t elem_time = MIN_DT;
        if (child.view_data().valid() && child.view_data().ops) {
            elem_time = child.view_data().ops->last_modified_time(child.view_data());
            if (elem_time > max_elem_time) {
                max_elem_time = elem_time;
            }
        }
        elems.push_back({std::move(child), elem_time});
    }

    // Use max of container_time and max_elem_time as the current tick threshold.
    // MIN_DT (epoch) means "never modified" — if no element was ever modified, return None.
    engine_time_t threshold = std::max(container_time, max_elem_time);
    if (threshold == MIN_DT) {
        return nb::none();
    }

    // Second pass: build dict of modified elements (time >= threshold)
    nb::dict result;
    for (size_t i = 0; i < count; ++i) {
        auto& info = elems[i];
        if (info.child.view_data().valid() && info.child.view_data().ops && info.time >= threshold) {
            nb::object elem_delta = info.child.view_data().ops->delta_to_python(info.child.view_data());
            if (!elem_delta.is_none()) {
                result[nb::int_(i)] = elem_delta;
            }
        }
    }

    if (result.size() == 0) {
        return nb::none();
    }
    return result;
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    // If linked, delegate to target (write through to target)
    if (auto* rl = get_ref_link(vd.link_data)) {
        if (rl->target().valid()) {
            ViewData target_vd = make_view_data_from_link(*rl, vd.path);
            rl->target().ops->from_python(target_vd, src, current_time);
            return;
        }
    }

    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("from_python on invalid ViewData");
    }

    // Handle dict format for sparse TSL updates (e.g., {0: value, 2: value})
    // This mirrors Python's TSL.value setter behavior
    if (nb::isinstance<nb::dict>(src)) {
        auto dict = nb::cast<nb::dict>(src);
        for (auto item : dict) {
            auto key = nb::cast<size_t>(item.first);
            auto val = nb::borrow<nb::object>(item.second);
            if (!val.is_none()) {
                TSView child = child_at(vd, key, current_time);
                if (child.view_data().valid()) {
                    child.view_data().ops->from_python(child.view_data(), val, current_time);
                }
            }
        }
        // Update container time and notify observers (same as list/tuple path)
        auto time_view = make_time_view(vd);
        time_view.as_tuple().at(0).as<engine_time_t>() = current_time;

        if (vd.observer_data) {
            auto observer_view = make_observer_view(vd);
            auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
            observers->notify_modified(current_time);
        }
        return;
    }

    auto dst = make_value_view(vd);
    dst.from_python(src);

    auto time_view = make_time_view(vd);
    // Set container time
    time_view.as_tuple().at(0).as<engine_time_t>() = current_time;

    // Also set each element's time for values that were set (not None)
    // This is required for element-level valid() checks to work
    // AND notify each element's observers (required for per-element binding)
    if (nb::isinstance<nb::sequence>(src)) {
        auto seq = nb::cast<nb::sequence>(src);
        size_t src_len = nb::len(seq);
        auto elem_times = time_view.as_tuple().at(1).as_list();
        size_t max_idx = std::min(src_len, elem_times.size());

        // Get element observers list for notifications
        value::ListView elem_observers;
        if (vd.observer_data) {
            auto observer_view = make_observer_view(vd);
            elem_observers = observer_view.as_tuple().at(1).as_list();
        }

        for (size_t i = 0; i < max_idx; ++i) {
            nb::object elem = seq[i];
            if (!elem.is_none()) {
                // Set element time
                elem_times.at(i).as<engine_time_t>() = current_time;

                // Notify element observers (for per-element binding)
                if (elem_observers && i < elem_observers.size()) {
                    auto* elem_obs = static_cast<ObserverList*>(elem_observers.at(i).data());
                    if (elem_obs) {
                        elem_obs->notify_modified(current_time);
                    }
                }
            }
        }
    }

    // Notify container observers
    if (vd.observer_data) {
        auto observer_view = make_observer_view(vd);
        auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
        observers->notify_modified(current_time);
    }
}

TSView child_at(const ViewData& vd, size_t index, engine_time_t current_time) {
    // If linked (dynamic TSL), navigate through target
    // Set sampled flag if REF was rebound at current_time OR if parent was already sampled
    if (auto* rl = get_active_link(vd)) {
        bool is_sampled = vd.sampled || is_ref_sampled(*rl, current_time);
        ViewData target_vd = make_view_data_from_link(*rl, vd.path, is_sampled);
        TSView result = rl->target().ops->child_at(target_vd, index, current_time);
        // Ensure sampled flag is propagated to the result
        if (is_sampled && result.view_data().valid()) {
            result.view_data().sampled = true;
        }
        return result;
    }

    // If linked via LinkTarget (dynamic TSL, TSInput)
    if (auto* lt = get_active_link_target(vd)) {
        ViewData target_vd = make_view_data_from_link_target(*lt, vd.path);
        return lt->ops->child_at(target_vd, index, current_time);
    }

    if (!vd.meta || !vd.meta->element_ts) {
        return TSView{};
    }

    const TSMeta* elem_meta = vd.meta->element_ts;

    // For fixed-size TSL with per-element binding, check if this element is linked
    if (vd.link_data && vd.meta->fixed_size > 0 && index < static_cast<size_t>(vd.meta->fixed_size)) {
        auto* link_schema = get_list_link_schema(vd);
        if (link_schema) {
            value::View link_view(vd.link_data, link_schema);
            if (vd.uses_link_target) {
                // TSInput: Check LinkTarget
                auto* lt = static_cast<LinkTarget*>(link_view.as_list().at(index).data());
                if (lt && lt->valid() && elem_meta->kind != TSKind::REF) {
                    // Non-REF element: delegate to target (standard peered binding)
                    ViewData target_vd = make_view_data_from_link_target(*lt, vd.path.child(index));
                    return TSView(target_vd, current_time);
                }
                // REF elements fall through to Phase 4 which preserves REF meta and
                // sets link_data to the element's LinkTarget (needed by ref_value()).
            } else {
                // TSOutput: Check REFLink
                auto* rl = static_cast<REFLink*>(link_view.as_list().at(index).data());
                if (rl && rl->target().is_linked && rl->target().ops) {
                    // Element is linked - return TSView pointing to target
                    bool is_sampled = vd.sampled || is_ref_sampled(*rl, current_time);
                    ViewData target_vd = make_view_data_from_link(*rl, vd.path.child(index), is_sampled);
                    return TSView(target_vd, current_time);
                }
                // If REFLink is bound to a REF source but target not resolved yet,
                // we need to use the dereferenced meta type, not the REF meta
                if (rl && rl->is_bound()) {
                    const TSMeta* deref_meta = rl->dereferenced_meta();
                    if (deref_meta) {
                        // Use dereferenced meta - target will be resolved at runtime
                        elem_meta = deref_meta;
                    }
                }
            }
        }
    }

    // Fall back to local storage (for outputs or unlinked inputs)
    auto value_view = make_value_view(vd);
    auto time_view = make_time_view(vd);
    auto observer_view = make_observer_view(vd);

    if (!value_view.valid()) {
        return TSView{};
    }

    auto value_list = value_view.as_list();
    if (index >= value_list.size()) {
        return TSView{};
    }

    ViewData elem_vd;
    elem_vd.path = vd.path.child(index);
    elem_vd.value_data = value_list.at(index).data();
    elem_vd.time_data = time_view.as_tuple().at(1).as_list().at(index).data();
    elem_vd.observer_data = observer_view.as_tuple().at(1).as_list().at(index).data();
    elem_vd.delta_data = nullptr;
    elem_vd.sampled = vd.sampled;  // Propagate sampled flag from parent
    elem_vd.uses_link_target = vd.uses_link_target;  // Propagate link type flag
    elem_vd.ops = get_ts_ops(elem_meta);
    elem_vd.meta = elem_meta;

    // For fixed-size TSL, also set link_data for potential future binding
    if (vd.link_data && vd.meta->fixed_size > 0) {
        auto* link_schema = get_list_link_schema(vd);
        if (link_schema && index < static_cast<size_t>(vd.meta->fixed_size)) {
            value::View link_view(vd.link_data, link_schema);
            elem_vd.link_data = link_view.as_list().at(index).data();
        }
    }

    return TSView(elem_vd, current_time);
}

TSView child_by_name(const ViewData& vd, const std::string& name, engine_time_t current_time) {
    // If linked via REFLink, navigate through target
    if (auto* rl = get_active_link(vd)) {
        bool is_sampled = vd.sampled || is_ref_sampled(*rl, current_time);
        ViewData target_vd = make_view_data_from_link(*rl, vd.path, is_sampled);
        TSView result = rl->target().ops->child_by_name(target_vd, name, current_time);
        if (is_sampled && result.view_data().valid()) {
            result.view_data().sampled = true;
        }
        return result;
    }
    // If linked via LinkTarget, navigate through target
    if (auto* lt = get_active_link_target(vd)) {
        ViewData target_vd = make_view_data_from_link_target(*lt, vd.path);
        return lt->ops->child_by_name(target_vd, name, current_time);
    }
    // Lists don't have named children
    return TSView{};
}

TSView child_by_key(const ViewData& vd, const value::View& key, engine_time_t current_time) {
    // If linked via REFLink, navigate through target
    if (auto* rl = get_active_link(vd)) {
        bool is_sampled = vd.sampled || is_ref_sampled(*rl, current_time);
        ViewData target_vd = make_view_data_from_link(*rl, vd.path, is_sampled);
        TSView result = rl->target().ops->child_by_key(target_vd, key, current_time);
        if (is_sampled && result.view_data().valid()) {
            result.view_data().sampled = true;
        }
        return result;
    }
    // If linked via LinkTarget, navigate through target
    if (auto* lt = get_active_link_target(vd)) {
        ViewData target_vd = make_view_data_from_link_target(*lt, vd.path);
        return lt->ops->child_by_key(target_vd, key, current_time);
    }
    // Lists don't support key access
    return TSView{};
}

size_t child_count(const ViewData& vd) {
    // If linked (dynamic TSL), delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->child_count(make_view_data_from_link(*rl, vd.path, vd.sampled));
    }
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->child_count(make_view_data_from_link_target(*lt, vd.path));
    }

    // For fixed-size TSL, return the fixed size
    if (vd.meta && vd.meta->fixed_size > 0) {
        return static_cast<size_t>(vd.meta->fixed_size);
    }

    auto value_view = make_value_view(vd);
    if (!value_view.valid()) return 0;
    return value_view.as_list().size();
}

value::View observer(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->observer(make_view_data_from_link(*rl, vd.path));
    }
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->observer(make_view_data_from_link_target(*lt, vd.path));
    }
    return make_observer_view(vd);
}

void notify_observers(ViewData& vd, engine_time_t current_time) {
    // If linked via REFLink, delegate to target
    if (!vd.uses_link_target) {
        if (auto* rl = get_ref_link(vd.link_data)) {
            if (rl->target().valid()) {
                ViewData target_vd = make_view_data_from_link(*rl, vd.path);
                rl->target().ops->notify_observers(target_vd, current_time);
                return;
            }
        }
    }
    // If linked via LinkTarget, delegate to target
    if (auto* lt = get_active_link_target(vd)) {
        ViewData target_vd = make_view_data_from_link_target(*lt, vd.path);
        lt->ops->notify_observers(target_vd, current_time);
        return;
    }

    if (vd.observer_data) {
        auto observer_view = make_observer_view(vd);
        auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
        observers->notify_modified(current_time);
    }
}

void bind(ViewData& vd, const ViewData& target) {
    if (!vd.link_data) {
        throw std::runtime_error("bind on list without link data");
    }

    // Fixed-size TSL uses per-element binding (fixed_list[LinkType])
    if (vd.meta && vd.meta->fixed_size > 0) {
        auto* link_schema = get_list_link_schema(vd);
        if (!link_schema) {
            throw std::runtime_error("bind on fixed-size list without link schema");
        }

        value::View link_view(vd.link_data, link_schema);
        auto link_list = link_view.as_list();

        // Bind each element to the corresponding element in target
        for (size_t i = 0; i < static_cast<size_t>(vd.meta->fixed_size) && i < link_list.size(); ++i) {
            // Get target's element
            TSView target_elem = target.ops->child_at(target, i, MIN_DT);
            if (!target_elem) continue;

            if (vd.uses_link_target) {
                // TSInput: Use LinkTarget
                auto* lt = static_cast<LinkTarget*>(link_list.at(i).data());
                if (lt) {
                    // Set time-accounting chain
                    if (vd.time_data) {
                        lt->owner_time_ptr = static_cast<engine_time_t*>(vd.time_data);
                    }

                    // Check if target element is REF - need REFBindingHelper
                    const auto& elem_vd = target_elem.view_data();
                    if (elem_vd.meta && elem_vd.meta->kind == TSKind::REF) {
                        if (lt->ref_binding_) {
                            auto* old_helper = static_cast<REFBindingHelper*>(lt->ref_binding_);
                            old_helper->unsubscribe_all();
                            if (lt->ref_binding_deleter_) {
                                lt->ref_binding_deleter_(lt->ref_binding_);
                            }
                            lt->ref_binding_ = nullptr;
                            lt->ref_binding_deleter_ = nullptr;
                            lt->is_linked = false;
                            lt->value_data = nullptr;
                            lt->time_data = nullptr;
                            lt->observer_data = nullptr;
                            lt->delta_data = nullptr;
                            lt->link_data = nullptr;
                            lt->ops = nullptr;
                            lt->meta = nullptr;
                        }

                        bool is_ref_to_ref = (vd.meta->element_ts &&
                                              vd.meta->element_ts->kind == TSKind::REF);
                        auto* helper = new REFBindingHelper(lt, elem_vd, is_ref_to_ref);
                        lt->ref_binding_ = helper;
                        lt->ref_binding_deleter_ = &delete_ref_binding_helper;

                        if (is_ref_to_ref) {
                            // REF→REF: Store REF source data in LinkTarget so that
                            // ref_value() can read the TSReference (matches scalar bind lines 738-751).
                            lt->is_linked = true;
                            lt->target_path = elem_vd.path;
                            lt->value_data = elem_vd.value_data;
                            lt->time_data = elem_vd.time_data;
                            lt->observer_data = nullptr;
                            lt->delta_data = elem_vd.delta_data;
                            lt->link_data = nullptr;
                            lt->ops = elem_vd.ops;
                            lt->meta = elem_vd.meta;  // Preserves REF identity
                        }

                        helper->subscribe_to_ref_source();
                        helper->rebind(MIN_DT);
                        continue;
                    }

                    store_to_link_target(*lt, elem_vd);
                    // Mark peered: binding happened at the list level
                    lt->peered = true;

                    // Subscribe for time-accounting
                    if (lt->observer_data) {
                        auto* obs = static_cast<ObserverList*>(lt->observer_data);
                        obs->add_observer(lt);
                    }
                }
            } else {
                // TSOutput: Use REFLink
                auto* rl = static_cast<REFLink*>(link_list.at(i).data());
                if (rl) {
                    const TSMeta* target_meta = target_elem.view_data().meta;
                    if (target_meta && target_meta->kind == TSKind::REF) {
                        rl->bind_to_ref(target_elem, MIN_DT);
                    } else {
                        store_link_target(*rl, target_elem.view_data());
                    }
                }
            }
        }
        return;
    }

    // Dynamic TSL uses collection-level binding (single LinkType)
    if (vd.uses_link_target) {
        // TSInput: Use LinkTarget
        auto* lt = get_link_target(vd.link_data);
        if (!lt) {
            throw std::runtime_error("bind on list with invalid link data");
        }

        // Set time-accounting chain
        if (vd.time_data) {
            lt->owner_time_ptr = static_cast<engine_time_t*>(vd.time_data);
        }

        // Check if target is REF - need REFBindingHelper
        if (target.meta && target.meta->kind == TSKind::REF) {
            if (lt->ref_binding_) {
                auto* old_helper = static_cast<REFBindingHelper*>(lt->ref_binding_);
                old_helper->unsubscribe_all();
                if (lt->ref_binding_deleter_) {
                    lt->ref_binding_deleter_(lt->ref_binding_);
                }
                lt->ref_binding_ = nullptr;
                lt->ref_binding_deleter_ = nullptr;
                lt->is_linked = false;
                lt->value_data = nullptr;
                lt->time_data = nullptr;
                lt->observer_data = nullptr;
                lt->delta_data = nullptr;
                lt->link_data = nullptr;
                lt->ops = nullptr;
                lt->meta = nullptr;
            }

            auto* helper = new REFBindingHelper(lt, target, false);
            lt->ref_binding_ = helper;
            lt->ref_binding_deleter_ = &delete_ref_binding_helper;
            helper->subscribe_to_ref_source();
            helper->rebind(MIN_DT);
            return;
        }

        store_to_link_target(*lt, target);
        // Mark peered: binding happened at the list level
        lt->peered = true;

        // Subscribe for time-accounting
        if (lt->observer_data) {
            auto* obs = static_cast<ObserverList*>(lt->observer_data);
            obs->add_observer(lt);
        }
    } else {
        // TSOutput: Use REFLink
        auto* rl = get_ref_link(vd.link_data);
        if (!rl) {
            throw std::runtime_error("bind on list with invalid link data");
        }
        store_link_target(*rl, target);
    }
}

void unbind(ViewData& vd) {
    if (!vd.link_data) {
        return;  // No-op if not linked
    }

    // Fixed-size TSL uses per-element binding
    if (vd.meta && vd.meta->fixed_size > 0) {
        auto* link_schema = get_list_link_schema(vd);
        if (link_schema) {
            value::View link_view(vd.link_data, link_schema);
            auto link_list = link_view.as_list();

            for (size_t i = 0; i < static_cast<size_t>(vd.meta->fixed_size) && i < link_list.size(); ++i) {
                if (vd.uses_link_target) {
                    // TSInput: Unsubscribe both chains then clear
                    auto* lt = static_cast<LinkTarget*>(link_list.at(i).data());
                    if (!lt) continue;

                    if (lt->ref_binding_) {
                        auto* helper = static_cast<REFBindingHelper*>(lt->ref_binding_);
                        helper->unsubscribe_all();
                        lt->cleanup_ref_binding();
                        lt->clear();
                    } else if (lt->is_linked) {
                        if (lt->observer_data) {
                            auto* obs = static_cast<ObserverList*>(lt->observer_data);
                            obs->remove_observer(lt);
                            if (lt->active_notifier.owning_input != nullptr) {
                                obs->remove_observer(&lt->active_notifier);
                                lt->active_notifier.owning_input = nullptr;
                            }
                        }
                        lt->clear();
                    }
                } else {
                    // TSOutput: Use REFLink
                    auto* rl = static_cast<REFLink*>(link_list.at(i).data());
                    if (rl) {
                        rl->unbind();
                    }
                }
            }
        }
        return;
    }

    // Dynamic TSL uses collection-level binding
    if (vd.uses_link_target) {
        // TSInput: Unsubscribe both chains then clear
        auto* lt = get_link_target(vd.link_data);
        if (!lt) return;

        if (lt->ref_binding_) {
            auto* helper = static_cast<REFBindingHelper*>(lt->ref_binding_);
            helper->unsubscribe_all();
            lt->cleanup_ref_binding();
            lt->clear();
        } else if (lt->is_linked) {
            if (lt->observer_data) {
                auto* obs = static_cast<ObserverList*>(lt->observer_data);
                obs->remove_observer(lt);
                if (lt->active_notifier.owning_input != nullptr) {
                    obs->remove_observer(&lt->active_notifier);
                    lt->active_notifier.owning_input = nullptr;
                }
            }
            lt->clear();
        }
    } else {
        // TSOutput: Use REFLink
        auto* rl = get_ref_link(vd.link_data);
        if (rl) {
            rl->unbind();
        }
    }
}

bool is_bound(const ViewData& vd) {
    // Fixed-size TSL uses per-element binding - check if any element is bound
    if (vd.meta && vd.meta->fixed_size > 0 && vd.link_data) {
        auto* link_schema = get_list_link_schema(vd);
        if (link_schema) {
            value::View link_view(vd.link_data, link_schema);
            auto link_list = link_view.as_list();

            for (size_t i = 0; i < static_cast<size_t>(vd.meta->fixed_size) && i < link_list.size(); ++i) {
                if (vd.uses_link_target) {
                    // TSInput: Use LinkTarget
                    auto* lt = static_cast<const LinkTarget*>(link_list.at(i).data());
                    if (lt && lt->is_linked) {
                        return true;
                    }
                } else {
                    // TSOutput: Use REFLink
                    auto* rl = static_cast<const REFLink*>(link_list.at(i).data());
                    if (rl && rl->target().is_linked) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // Dynamic TSL uses collection-level binding
    if (vd.uses_link_target) {
        // TSInput: Use LinkTarget
        auto* lt = get_link_target(vd.link_data);
        return lt && lt->is_linked;
    } else {
        // TSOutput: Use REFLink
        auto* rl = get_ref_link(vd.link_data);
        return rl && rl->target().is_linked;
    }
}

bool is_peered(const ViewData& vd) {
    if (!vd.link_data || !vd.uses_link_target) return false;

    // Fixed-size TSL: check first element's peered flag
    if (vd.meta && vd.meta->fixed_size > 0) {
        auto* link_schema = get_list_link_schema(vd);
        if (link_schema) {
            value::View link_view(vd.link_data, link_schema);
            auto link_list = link_view.as_list();
            if (link_list.size() > 0) {
                auto* lt = static_cast<const LinkTarget*>(link_list.at(0).data());
                return lt && lt->peered;
            }
        }
        return false;
    }

    // Dynamic TSL: check collection-level LinkTarget's peered flag
    auto* lt = get_link_target(vd.link_data);
    return lt && lt->peered;
}

void set_active(ViewData& vd, value::View active_view, bool active, TSInput* input) {
    if (!active_view || !vd.meta) return;

    // TSL active schema: tuple[bool, list[element_active]]
    value::TupleView tv = active_view.as_tuple();
    value::View root = tv[0];
    if (root) {
        *static_cast<bool*>(root.data()) = active;
    }

    // Set active for each element
    value::View element_list = tv[1];
    if (element_list && element_list.is_list()) {
        value::ListView lv = element_list.as_list();
        const TSMeta* elem_ts = vd.meta->element_ts;

        for (size_t i = 0; i < lv.size(); ++i) {
            value::View elem_active = lv[i];
            if (!elem_active) continue;

            if (elem_ts && (elem_ts->is_collection() || elem_ts->kind == TSKind::TSB)) {
                // Composite element - recurse
                ViewData elem_vd;
                elem_vd.meta = elem_ts;
                elem_vd.ops = get_ts_ops(elem_ts);
                // TODO: Get link data for this element if linked
                elem_vd.ops->set_active(elem_vd, elem_active, active, input);
            } else {
                // Scalar element - set directly
                *static_cast<bool*>(elem_active.data()) = active;
            }
        }
    }

    // Manage node-scheduling subscription based on link type
    if (vd.link_data) {
        if (vd.uses_link_target) {
            // TSInput: Fixed-size TSL uses per-element binding
            if (vd.meta->fixed_size > 0) {
                auto* link_schema = TSMetaSchemaCache::instance().get_input_link_schema(vd.meta);
                if (link_schema) {
                    value::View link_view(vd.link_data, link_schema);
                    auto link_list = link_view.as_list();

                    for (size_t i = 0; i < link_list.size() && i < static_cast<size_t>(vd.meta->fixed_size); ++i) {
                        auto* lt = static_cast<LinkTarget*>(link_list.at(i).data());
                        if (lt && lt->is_linked && lt->observer_data) {
                            auto* observers = static_cast<ObserverList*>(lt->observer_data);
                            if (active) {
                                if (lt->active_notifier.owning_input == nullptr) {
                                    lt->active_notifier.owning_input = input;
                                    observers->add_observer(&lt->active_notifier);
                                }
                            } else {
                                if (lt->active_notifier.owning_input != nullptr) {
                                    observers->remove_observer(&lt->active_notifier);
                                    lt->active_notifier.owning_input = nullptr;
                                }
                            }
                        } else if (lt && lt->ref_binding_) {
                            // REF binding exists but hasn't resolved yet.
                            // Set owning_input so REFBindingHelper::rebind() can subscribe later.
                            if (active) {
                                if (lt->active_notifier.owning_input == nullptr) {
                                    lt->active_notifier.owning_input = input;
                                }
                            } else {
                                lt->active_notifier.owning_input = nullptr;
                            }
                        }
                    }
                }
            } else {
                // Dynamic TSL: single LinkTarget
                auto* lt = get_link_target(vd.link_data);
                if (lt && lt->is_linked && lt->observer_data) {
                    auto* observers = static_cast<ObserverList*>(lt->observer_data);
                    if (active) {
                        if (lt->active_notifier.owning_input == nullptr) {
                            lt->active_notifier.owning_input = input;
                            observers->add_observer(&lt->active_notifier);
                        }
                    } else {
                        if (lt->active_notifier.owning_input != nullptr) {
                            observers->remove_observer(&lt->active_notifier);
                            lt->active_notifier.owning_input = nullptr;
                        }
                    }
                } else if (lt && lt->ref_binding_) {
                    // REF binding exists but hasn't resolved yet.
                    // Set owning_input so REFBindingHelper::rebind() can subscribe later.
                    if (active) {
                        if (lt->active_notifier.owning_input == nullptr) {
                            lt->active_notifier.owning_input = input;
                        }
                    } else {
                        lt->active_notifier.owning_input = nullptr;
                    }
                }
            }
        } else {
            // TSOutput path: subscribe input directly
            void* observer_data = nullptr;
            if (vd.meta->fixed_size > 0) {
                auto* link_schema = TSMetaSchemaCache::instance().get_link_schema(vd.meta);
                if (link_schema) {
                    value::View link_view(vd.link_data, link_schema);
                    auto link_list = link_view.as_list();

                    for (size_t i = 0; i < link_list.size() && i < static_cast<size_t>(vd.meta->fixed_size); ++i) {
                        auto* rl = static_cast<REFLink*>(link_list.at(i).data());
                        if (rl && rl->target().is_linked && rl->target().observer_data) {
                            auto* observers = static_cast<ObserverList*>(rl->target().observer_data);
                            if (active) {
                                observers->add_observer(input);
                            } else {
                                observers->remove_observer(input);
                            }
                        }
                    }
                }
            } else {
                auto* rl = get_ref_link(vd.link_data);
                if (rl && rl->target().is_linked) {
                    observer_data = rl->target().observer_data;
                }
            }

            if (observer_data) {
                auto* observers = static_cast<ObserverList*>(observer_data);
                if (active) {
                    observers->add_observer(input);
                } else {
                    observers->remove_observer(input);
                }
            }
        }
    }
}

} // namespace list_ops

// ============================================================================
// Set Operations (TSS)
// ============================================================================

namespace set_ops {

// For TSS types (with nested is_empty TS[bool]):
// - value is tuple[SetStorage, bool(is_empty)]
// - time is tuple[engine_time_t, engine_time_t]
// - observer is tuple[ObserverList, ObserverList]
// - delta is SetDelta (unchanged, flat)
// Element [0] = set container data, Element [1] = is_empty child data

// ========== Tuple Navigation Helpers ==========
// These helpers support two TSS data formats:
// - Tuple format (TSTypeRegistry::tss()): value = tuple(SetStorage, bool), time/observer = tuples
// - Raw format (TSTypeRegistry::tss_raw(), used by TSD key_set): value = raw SetStorage, time/observer = raw scalars
// Format is detected by checking meta->value_type->kind == TypeKind::Tuple.

inline bool is_tuple_format(const ViewData& vd) {
    return vd.meta && vd.meta->value_type &&
           vd.meta->value_type->kind == value::TypeKind::Tuple;
}

// Get the container time (element [0] of the time tuple, or raw engine_time_t)
inline engine_time_t* get_container_time(const ViewData& vd) {
    if (is_tuple_format(vd)) {
        auto time_view = make_time_view(vd);
        if (!time_view.valid()) return nullptr;
        return &time_view.as_tuple().at(0).as<engine_time_t>();
    }
    // Raw format: time_data is directly engine_time_t
    if (!vd.time_data) return nullptr;
    return static_cast<engine_time_t*>(vd.time_data);
}

// Get the container observers (element [0] of the observer tuple, or raw ObserverList)
inline ObserverList* get_container_observers(const ViewData& vd) {
    if (is_tuple_format(vd)) {
        auto observer_view = make_observer_view(vd);
        if (!observer_view.valid()) return nullptr;
        return static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
    }
    // Raw format: observer_data is directly ObserverList
    if (!vd.observer_data) return nullptr;
    return static_cast<ObserverList*>(vd.observer_data);
}

// Get the SetStorage (element [0] of the value tuple, or raw SetStorage)
inline value::SetStorage* get_set_storage(const ViewData& vd) {
    if (is_tuple_format(vd)) {
        auto value_view = make_value_view(vd);
        if (!value_view.valid()) return nullptr;
        return static_cast<value::SetStorage*>(value_view.as_tuple().at(0).data());
    }
    // Raw format: value_data is directly SetStorage
    if (!vd.value_data) return nullptr;
    return static_cast<value::SetStorage*>(vd.value_data);
}

// Get the set value::View (element [0] of the value tuple, or raw view)
inline value::View get_set_value_view(const ViewData& vd) {
    if (is_tuple_format(vd)) {
        auto value_view = make_value_view(vd);
        if (!value_view.valid()) return value::View{};
        return value_view.as_tuple().at(0);
    }
    // Raw format: value_data IS the set directly
    return make_value_view(vd);
}

// Get the set element TypeMeta from the value schema
inline const value::TypeMeta* get_set_element_type(const ViewData& vd) {
    if (!vd.meta || !vd.meta->value_type) return nullptr;
    if (is_tuple_format(vd)) {
        // Tuple format: fields[0].type is the SetStorage TypeMeta
        if (!vd.meta->value_type->fields) return nullptr;
        const value::TypeMeta* set_type = vd.meta->value_type->fields[0].type;
        return set_type ? set_type->element_type : nullptr;
    }
    // Raw format: value_type is the SetStorage TypeMeta directly
    return vd.meta->value_type->element_type;
}

// Update the nested is_empty TS[bool] child after set mutations.
// Compares current emptiness against stored is_empty value and only
// notifies observers if the value actually changed.
// Only applicable for tuple format (raw format has no is_empty child).
inline void update_is_empty(const ViewData& vd, engine_time_t current_time) {
    if (!is_tuple_format(vd)) return;  // Raw format has no is_empty child

    auto* set_storage = get_set_storage(vd);
    if (!set_storage) return;

    bool new_is_empty = (set_storage->size() == 0);

    // Navigate to is_empty child data (element [1] in each tuple)
    auto value_view = make_value_view(vd);
    bool* is_empty_ptr = static_cast<bool*>(value_view.as_tuple().at(1).data());

    if (*is_empty_ptr != new_is_empty) {
        *is_empty_ptr = new_is_empty;

        auto time_view = make_time_view(vd);
        auto* is_empty_time = &time_view.as_tuple().at(1).as<engine_time_t>();
        *is_empty_time = current_time;

        auto observer_view = make_observer_view(vd);
        auto* is_empty_obs = static_cast<ObserverList*>(observer_view.as_tuple().at(1).data());
        is_empty_obs->notify_modified(current_time);
    }
}

// ========== Standard ts_ops Functions ==========

engine_time_t last_modified_time(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->last_modified_time(*target);
    auto* t = get_container_time(vd);
    if (!t) return MIN_DT;
    return *t;
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->modified(*target, current_time);
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->valid(*target);
    return last_modified_time(vd) != MIN_DT;
}

bool all_valid(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->all_valid(*target);
    return valid(vd);
}

bool sampled(const ViewData& vd) {
    return vd.sampled;
}

value::View value(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->value(*target);
    // Return the set portion (element [0] of the value tuple)
    return get_set_value_view(vd);
}

value::View delta_value(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->delta_value(*target);
    return make_delta_view(vd);
}

bool has_delta(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->has_delta(*target);
    return vd.delta_data != nullptr;
}

void set_value(ViewData& vd, const value::View& src, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("set_value on invalid ViewData");
    }

    // Copy to the set portion (element [0] of value tuple)
    auto dst = get_set_value_view(vd);
    dst.copy_from(src);

    auto* t = get_container_time(vd);
    if (t) *t = current_time;

    auto* obs = get_container_observers(vd);
    if (obs) obs->notify_modified(current_time);

    update_is_empty(vd, current_time);
}

void apply_delta(ViewData& vd, const value::View& delta, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("apply_delta on invalid ViewData");
    }

    if (!delta.valid()) {
        return;  // Nothing to apply
    }

    auto set_v = get_set_value_view(vd);
    if (!set_v.valid()) {
        throw std::runtime_error("apply_delta: TSS has no valid storage");
    }

    auto set_view = set_v.as_set();

    if (delta.is_bundle()) {
        auto delta_bundle = delta.as_bundle();

        if (delta_bundle.has_field("removed")) {
            auto removed_view = delta_bundle.at("removed");
            if (removed_view.is_set()) {
                for (auto elem : removed_view.as_set()) {
                    set_view.remove(elem);
                }
            }
        }

        if (delta_bundle.has_field("added")) {
            auto added_view = delta_bundle.at("added");
            if (added_view.is_set()) {
                for (auto elem : added_view.as_set()) {
                    set_view.add(elem);
                }
            }
        }
    } else if (delta.is_set()) {
        set_view.clear();
        for (auto elem : delta.as_set()) {
            set_view.add(elem);
        }
    } else {
        throw std::runtime_error("apply_delta for TSS: delta must be a bundle with 'added'/'removed' fields or a set");
    }

    auto* t = get_container_time(vd);
    if (t) *t = current_time;

    auto* obs = get_container_observers(vd);
    if (obs) obs->notify_modified(current_time);

    update_is_empty(vd, current_time);
}

void invalidate(ViewData& vd) {
    auto* t = get_container_time(vd);
    if (t) *t = MIN_DT;
}

nb::object to_python(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->to_python(*target);
    if (!valid(vd)) return nb::none();
    // Convert just the set (element [0]), not the whole tuple
    auto v = get_set_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

// Cache for Python-level delta built during from_python.
// Keyed by delta_data pointer to support multiple concurrent TSS outputs.
// from_python populates this; delta_to_python reads it.
// This avoids the problem of reading destructed slot data from SetDelta.
static thread_local std::unordered_map<void*, nb::object> cached_py_deltas_;

nb::object delta_to_python(const ViewData& vd) {
    // Follow links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        // Check for REF rebind: if the LinkTarget has a REFBindingHelper that just
        // changed targets, compute full diff delta.
        // This matches Python's PythonTimeSeriesSetInput._prev_output which stores
        // a reference to the old output object and at delta_value time computes:
        //   old_pre_tick = (prev_output.values() | prev_output.removed()) - prev_output.added()
        //   added = new_values - old_pre_tick
        //   removed = old_pre_tick - new_values
        if (lt->ref_binding_) {
            auto* helper = static_cast<REFBindingHelper*>(lt->ref_binding_);
            if (helper->has_pending_delta_) {
                ViewData old_vd = helper->pending_old_vd_;
                engine_time_t rebind_time = helper->pending_rebind_time_;
                helper->has_pending_delta_ = false;

                // Read the new target's current values
                ViewData new_target_vd = make_view_data_from_link_target(*lt, vd.path);
                nb::object new_values_py = lt->ops->to_python(new_target_vd);

                // Read the old target's current values
                nb::object old_values_py = old_vd.ops->to_python(old_vd);

                // Check if the old target modified on the CURRENT tick.
                // If it didn't, its delta is empty (no added, no removed).
                // This matches Python where output._added/_removed are reset
                // after each engine cycle and are None if the output didn't tick.
                bool old_modified = old_vd.ops->modified(old_vd, rebind_time);

                // Reconstruct old pre-tick state:
                // old_pre_tick = (old_values | old_removed) - old_added
                // If old target didn't modify this tick, old_added and old_removed
                // are empty, so old_pre_tick = old_values.
                nb::set old_pre_tick;
                if (!old_values_py.is_none()) {
                    for (auto elem : old_values_py) { old_pre_tick.add(nb::borrow(elem)); }
                }

                if (old_modified) {
                    // Old target also ticked — need to back out its delta
                    nb::object old_delta_py = old_vd.ops->delta_to_python(old_vd);
                    nb::set old_added_set, old_removed_set;
                    if (!old_delta_py.is_none()) {
                        try {
                            nb::object old_added_attr = old_delta_py.attr("added");
                            nb::object old_removed_attr = old_delta_py.attr("removed");
                            for (auto elem : old_added_attr) { old_added_set.add(nb::borrow(elem)); }
                            for (auto elem : old_removed_attr) { old_removed_set.add(nb::borrow(elem)); }
                        } catch (...) {}
                    }
                    // Adjust: (old_values | old_removed) - old_added
                    for (auto elem : old_removed_set) { old_pre_tick.add(nb::borrow(elem)); }
                    nb::set adjusted;
                    for (auto elem : old_pre_tick) {
                        if (!old_added_set.contains(nb::borrow(elem))) {
                            adjusted.add(nb::borrow(elem));
                        }
                    }
                    old_pre_tick = std::move(adjusted);
                }

                // Build new_set from current new target values
                nb::set new_set;
                if (!new_values_py.is_none()) {
                    for (auto elem : new_values_py) { new_set.add(nb::borrow(elem)); }
                }

                // Compute added = new_values - old_pre_tick
                nb::set py_added;
                for (auto elem : new_set) {
                    if (!old_pre_tick.contains(nb::borrow(elem))) { py_added.add(nb::borrow(elem)); }
                }
                // Compute removed = old_pre_tick - new_values
                nb::set py_removed;
                for (auto elem : old_pre_tick) {
                    if (!new_set.contains(nb::borrow(elem))) { py_removed.add(nb::borrow(elem)); }
                }

                nb::module_ tss_mod = nb::module_::import_("hgraph._impl._types._tss");
                return tss_mod.attr("PythonSetDelta")(nb::frozenset(py_added), nb::frozenset(py_removed));
            }
        }
        auto delegated_vd = make_view_data_from_link_target(*lt, vd.path);
        return lt->ops->delta_to_python(delegated_vd);
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->delta_to_python(make_view_data_from_link(*rl, vd.path));
    }
    // Check time-series validity first
    if (!valid(vd)) return nb::none();

    // Return the cached Python delta built by from_python or set_add/set_remove.
    // Cache entries are managed by from_python and set_add/set_remove which clear
    // stale entries at the start of each new tick. Input-side staleness is guarded
    // by the modified() check in get_input_delta.
    if (vd.delta_data) {
        auto it = cached_py_deltas_.find(vd.delta_data);
        if (it != cached_py_deltas_.end()) {
            return it->second;
        }
    }

    // Fallback: delta was not set by from_python (e.g., direct add/remove calls)
    // Build from SetDelta added slots only (removed slots may have destructed keys)
    auto* set_delta = vd.delta_data ? static_cast<SetDelta*>(vd.delta_data) : nullptr;
    if (!set_delta || set_delta->empty()) return nb::none();

    auto* set_storage = get_set_storage(vd);
    if (!set_storage) return nb::none();
    const auto* elem_type = set_storage->element_type();
    if (!elem_type || !elem_type->ops) return nb::none();

    // Added slots are alive; removed slots remain accessible during current tick
    // (removed slots go to free list only in the next engine cycle)
    nb::set py_added;
    for (size_t slot : set_delta->added()) {
        if (set_storage->key_set().is_alive(slot)) {
            const void* elem = set_storage->key_set().key_at_slot(slot);
            py_added.add(elem_type->ops->to_python(elem, elem_type));
        }
    }

    nb::set py_removed;
    for (size_t slot : set_delta->removed()) {
        const void* elem = set_storage->key_set().key_at_slot(slot);
        if (elem) {
            py_removed.add(elem_type->ops->to_python(elem, elem_type));
        }
    }

    nb::module_ tss_mod = nb::module_::import_("hgraph._impl._types._tss");
    return tss_mod.attr("PythonSetDelta")(nb::frozenset(py_added), nb::frozenset(py_removed));
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    if (src.is_none()) {
        invalidate(vd);
        return;
    }
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("from_python on invalid ViewData");
    }

    auto* set_storage = get_set_storage(vd);
    auto* set_delta = vd.delta_data ? static_cast<SetDelta*>(vd.delta_data) : nullptr;
    const auto* elem_type = set_storage->element_type();
    if (!elem_type || !elem_type->ops) {
        throw std::runtime_error("TSS from_python: missing element type");
    }

    // Check if output was already modified this tick (by direct add/remove calls)
    auto* container_time = get_container_time(vd);
    bool already_modified_this_tick = container_time && (*container_time == current_time);

    // If source is an empty SetDelta and output was already modified this tick,
    // skip entirely to preserve the delta from direct add/remove calls.
    // This matches Python: apply_result with empty SetDelta on already-modified output is a no-op.
    if (already_modified_this_tick && nb::hasattr(src, "added") && nb::hasattr(src, "removed")) {
        auto src_added = src.attr("added");
        auto src_removed = src.attr("removed");
        if (nb::len(nb::borrow(src_added)) == 0 && nb::len(nb::borrow(src_removed)) == 0) {
            return;  // No-op: empty SetDelta on already-modified output
        }
    }

    // Clear previous tick's delta and cached Python delta
    if (set_delta) {
        set_delta->clear();
        cached_py_deltas_.erase(vd.delta_data);
    }

    // Track Python-level added/removed for delta_to_python
    nb::set py_added, py_removed;

    // Temp storage for element conversion
    std::vector<std::byte> temp(elem_type->size);
    bool any_change = false;

    // Case 1: SetDelta (has .added and .removed attributes)
    if (nb::hasattr(src, "added") && nb::hasattr(src, "removed")) {
        auto src_removed = src.attr("removed");
        auto src_added = src.attr("added");

        // Process removals first (only remove if present)
        for (auto item : src_removed) {
            nb::object obj = nb::cast<nb::object>(item);
            elem_type->ops->construct(temp.data(), elem_type);
            elem_type->ops->from_python(temp.data(), obj, elem_type);
            if (set_storage->contains(temp.data())) {
                set_storage->remove(temp.data());
                py_removed.add(obj);
                any_change = true;
            }
            elem_type->ops->destruct(temp.data(), elem_type);
        }

        // Process additions (only add if not present)
        for (auto item : src_added) {
            nb::object obj = nb::cast<nb::object>(item);
            elem_type->ops->construct(temp.data(), elem_type);
            elem_type->ops->from_python(temp.data(), obj, elem_type);
            if (!set_storage->contains(temp.data())) {
                set_storage->add(temp.data());
                py_added.add(obj);
                any_change = true;
            }
            elem_type->ops->destruct(temp.data(), elem_type);
        }
    }
    // Case 2: frozenset — compute diff against current set
    else if (nb::isinstance<nb::frozenset>(src)) {
        auto new_set = nb::cast<nb::frozenset>(src);

        // Collect current elements to check for removals (capture Python repr BEFORE erase)
        struct RemovalEntry {
            std::vector<std::byte> data;
            nb::object py_obj;
        };
        std::vector<RemovalEntry> to_remove;
        for (auto it = set_storage->begin(); it != set_storage->end(); ++it) {
            const void* elem = *it;
            nb::object py_elem = elem_type->ops->to_python(elem, elem_type);
            if (!new_set.contains(py_elem)) {
                RemovalEntry entry;
                entry.data.resize(elem_type->size);
                elem_type->ops->construct(entry.data.data(), elem_type);
                elem_type->ops->copy_assign(entry.data.data(), elem, elem_type);
                entry.py_obj = std::move(py_elem);
                to_remove.push_back(std::move(entry));
            }
        }
        for (auto& r : to_remove) {
            set_storage->remove(r.data.data());
            elem_type->ops->destruct(r.data.data(), elem_type);
            py_removed.add(r.py_obj);
            any_change = true;
        }

        // Add elements in new_set not already in current
        for (auto item : new_set) {
            nb::object obj = nb::cast<nb::object>(item);
            elem_type->ops->construct(temp.data(), elem_type);
            elem_type->ops->from_python(temp.data(), obj, elem_type);
            if (set_storage->add(temp.data())) {
                py_added.add(obj);
                any_change = true;
            }
            elem_type->ops->destruct(temp.data(), elem_type);
        }
    }
    // Case 3: set/list/tuple/dict — check for Removed markers; if none, treat as incremental adds
    else if (nb::isinstance<nb::set>(src) || nb::isinstance<nb::list>(src) || nb::isinstance<nb::tuple>(src) || nb::isinstance<nb::dict>(src)) {
        nb::module_ tss_mod = nb::module_::import_("hgraph._impl._types._tss");
        nb::object RemovedType = tss_mod.attr("Removed");

        // First pass: check if any Removed markers exist
        bool has_removed_markers = false;
        for (auto item : src) {
            if (nb::isinstance(nb::cast<nb::object>(item), RemovedType)) {
                has_removed_markers = true;
                break;
            }
        }

        if (has_removed_markers) {
            // Process as mix of adds and removes (per Python _tss.py:119-130)
            for (auto item : src) {
                nb::object obj = nb::cast<nb::object>(item);
                if (nb::isinstance(obj, RemovedType)) {
                    nb::object inner = obj.attr("item");
                    elem_type->ops->construct(temp.data(), elem_type);
                    elem_type->ops->from_python(temp.data(), inner, elem_type);
                    if (set_storage->contains(temp.data())) {
                        set_storage->remove(temp.data());
                        py_removed.add(inner);
                        any_change = true;
                    }
                    elem_type->ops->destruct(temp.data(), elem_type);
                } else {
                    elem_type->ops->construct(temp.data(), elem_type);
                    elem_type->ops->from_python(temp.data(), obj, elem_type);
                    if (!set_storage->contains(temp.data())) {
                        set_storage->add(temp.data());
                        py_added.add(obj);
                        any_change = true;
                    }
                    elem_type->ops->destruct(temp.data(), elem_type);
                }
            }
        } else {
            // No Removed markers — just add new elements (per Python _tss.py:121,129)
            // Python: _added = {r for r in v if r not in self._value}; _value.update(_added)
            for (auto item : src) {
                nb::object obj = nb::cast<nb::object>(item);
                elem_type->ops->construct(temp.data(), elem_type);
                elem_type->ops->from_python(temp.data(), obj, elem_type);
                if (!set_storage->contains(temp.data())) {
                    set_storage->add(temp.data());
                    py_added.add(obj);
                    any_change = true;
                }
                elem_type->ops->destruct(temp.data(), elem_type);
            }
        }
    }
    else {
        throw std::runtime_error("TSS from_python: unsupported type");
    }

    // Match Python _post_modify(): mark modified if any change OR if not yet valid
    bool was_valid = container_time && (*container_time != MIN_DT);
    bool should_mark = any_change || !was_valid;

    // Cache the Python delta for delta_to_python
    // Always cache when should_mark (even empty delta, matching Python _post_modify behavior)
    if (vd.delta_data && should_mark) {
        nb::module_ tss_mod = nb::module_::import_("hgraph._impl._types._tss");
        cached_py_deltas_[vd.delta_data] = tss_mod.attr("PythonSetDelta")(
            nb::frozenset(py_added), nb::frozenset(py_removed));
    }

    if (should_mark) {
        if (container_time) *container_time = current_time;

        auto* obs = get_container_observers(vd);
        if (obs) obs->notify_modified(current_time);

        // Update nested is_empty TS[bool] child
        update_is_empty(vd, current_time);
    }
}

TSView child_at(const ViewData& vd, size_t index, engine_time_t current_time) {
    // Sets don't have navigable children (elements are values, not TSValues)
    return TSView{};
}

TSView child_by_name(const ViewData& vd, const std::string& name, engine_time_t current_time) {
    return TSView{};
}

TSView child_by_key(const ViewData& vd, const value::View& key, engine_time_t current_time) {
    // Sets don't support key access (elements are values, not TSValues)
    return TSView{};
}

size_t child_count(const ViewData& vd) {
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->child_count(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->child_count(make_view_data_from_link(*rl, vd.path));
    }
    auto* storage = get_set_storage(vd);
    if (!storage) return 0;
    return storage->size();
}

value::View observer(const ViewData& vd) {
    return make_observer_view(vd);
}

void notify_observers(ViewData& vd, engine_time_t current_time) {
    auto* obs = get_container_observers(vd);
    if (obs) obs->notify_modified(current_time);
}

void bind(ViewData& vd, const ViewData& target) {
    // TSS binding is identical to scalar - store target in link storage
    if (!vd.link_data) {
        throw std::runtime_error("bind on set without link data");
    }

    if (vd.uses_link_target) {
        auto* lt = get_link_target(vd.link_data);
        if (!lt) {
            throw std::runtime_error("bind on TSS input with invalid link data");
        }

        // Set time-accounting chain (use container time = element [0] of time tuple)
        auto* t = get_container_time(vd);
        if (t) {
            lt->owner_time_ptr = t;
        }

        // Check if target is REF - need REFBindingHelper
        if (target.meta && target.meta->kind == TSKind::REF) {
            if (lt->ref_binding_) {
                auto* old_helper = static_cast<REFBindingHelper*>(lt->ref_binding_);
                old_helper->unsubscribe_all();
                if (lt->ref_binding_deleter_) {
                    lt->ref_binding_deleter_(lt->ref_binding_);
                }
                lt->ref_binding_ = nullptr;
                lt->ref_binding_deleter_ = nullptr;
                lt->is_linked = false;
                lt->value_data = nullptr;
                lt->time_data = nullptr;
                lt->observer_data = nullptr;
                lt->delta_data = nullptr;
                lt->link_data = nullptr;
                lt->ops = nullptr;
                lt->meta = nullptr;
            }

            auto* helper = new REFBindingHelper(lt, target, false);
            lt->ref_binding_ = helper;
            lt->ref_binding_deleter_ = &delete_ref_binding_helper;
            helper->subscribe_to_ref_source();
            helper->rebind(MIN_DT);
            return;
        }

        store_to_link_target(*lt, target);

        // Subscribe for time-accounting
        if (lt->observer_data) {
            auto* obs = static_cast<ObserverList*>(lt->observer_data);
            obs->add_observer(lt);
        }
    } else {
        auto* rl = get_ref_link(vd.link_data);
        if (!rl) {
            throw std::runtime_error("bind on TSS with invalid link data");
        }

        if (target.meta && target.meta->kind == TSKind::REF) {
            TSView target_view(target, MIN_DT);
            rl->bind_to_ref(target_view, MIN_DT);
        } else {
            store_link_target(*rl, target);
        }
    }
}

void unbind(ViewData& vd) {
    if (!vd.link_data) return;

    if (vd.uses_link_target) {
        auto* lt = get_link_target(vd.link_data);
        if (!lt) return;

        if (lt->ref_binding_) {
            auto* helper = static_cast<REFBindingHelper*>(lt->ref_binding_);
            helper->unsubscribe_all();
            lt->cleanup_ref_binding();
            lt->clear();
        } else if (lt->is_linked) {
            if (lt->observer_data) {
                auto* obs = static_cast<ObserverList*>(lt->observer_data);
                obs->remove_observer(lt);
                if (lt->active_notifier.owning_input != nullptr) {
                    obs->remove_observer(&lt->active_notifier);
                    lt->active_notifier.owning_input = nullptr;
                }
            }
            lt->clear();
        }
    } else {
        auto* rl = get_ref_link(vd.link_data);
        if (rl) {
            rl->unbind();
        }
    }
}

bool is_bound(const ViewData& vd) {
    if (!vd.link_data) return false;

    if (vd.uses_link_target) {
        auto* lt = get_link_target(vd.link_data);
        return lt && (lt->is_linked || lt->ref_binding_);
    } else {
        auto* rl = get_ref_link(vd.link_data);
        return rl && rl->target().is_linked;
    }
}

bool is_peered(const ViewData& vd) {
    // TSS is a scalar-like type, always peered when bound
    return is_bound(vd);
}

// ========== Set-Specific Mutation Operations ==========

bool set_add(ViewData& vd, const value::View& elem, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("set_add on invalid ViewData");
    }

    auto* storage = get_set_storage(vd);
    const auto* elem_type = storage->element_type();

    // Clear stale cached delta + SetDelta at start of new tick.
    // The output may not have been modified since a previous tick, so both the
    // C++ SetDelta and the Python cached_py_deltas_ entry can be stale.
    auto* container_time = get_container_time(vd);
    bool first_mod_this_tick = container_time && (*container_time != current_time);
    if (first_mod_this_tick && vd.delta_data) {
        auto* set_delta = static_cast<SetDelta*>(vd.delta_data);
        set_delta->clear();
        cached_py_deltas_.erase(vd.delta_data);
    }

    // Capture Python representation BEFORE mutation (for delta tracking)
    nb::object py_elem = elem_type->ops->to_python(elem.data(), elem_type);

    bool added = storage->add(elem.data());

    if (added) {
        // Update cached Python delta
        if (vd.delta_data) {
            auto it = cached_py_deltas_.find(vd.delta_data);
            if (it != cached_py_deltas_.end()) {
                nb::object added_attr = nb::borrow(it->second.attr("added"));
                nb::object removed_attr = nb::borrow(it->second.attr("removed"));
                nb::set existing_added = nb::steal<nb::set>(PySet_New(added_attr.ptr()));
                nb::set existing_removed = nb::steal<nb::set>(PySet_New(removed_attr.ptr()));
                if (existing_removed.contains(py_elem)) {
                    existing_removed.discard(py_elem);
                } else {
                    existing_added.add(py_elem);
                }
                nb::module_ tss_mod = nb::module_::import_("hgraph._impl._types._tss");
                it->second = tss_mod.attr("PythonSetDelta")(
                    nb::frozenset(existing_added), nb::frozenset(existing_removed));
            } else {
                nb::set py_added;
                py_added.add(py_elem);
                nb::module_ tss_mod = nb::module_::import_("hgraph._impl._types._tss");
                cached_py_deltas_[vd.delta_data] = tss_mod.attr("PythonSetDelta")(
                    nb::frozenset(py_added), nb::frozenset(nb::set()));
            }
        }

        auto* t = get_container_time(vd);
        if (t) *t = current_time;

        auto* obs = get_container_observers(vd);
        if (obs) obs->notify_modified(current_time);

        update_is_empty(vd, current_time);
    }

    return added;
}

bool set_remove(ViewData& vd, const value::View& elem, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("set_remove on invalid ViewData");
    }

    auto* storage = get_set_storage(vd);
    const auto* elem_type = storage->element_type();

    // Clear stale cached delta + SetDelta at start of new tick.
    // Same logic as set_add: check container_time to detect new tick.
    auto* container_time = get_container_time(vd);
    bool first_mod_this_tick = container_time && (*container_time != current_time);
    if (first_mod_this_tick && vd.delta_data) {
        auto* set_delta = static_cast<SetDelta*>(vd.delta_data);
        set_delta->clear();
        cached_py_deltas_.erase(vd.delta_data);
    }

    // Capture Python representation BEFORE removal (key will be destructed)
    nb::object py_elem = elem_type->ops->to_python(elem.data(), elem_type);

    bool removed = storage->remove(elem.data());

    if (removed) {
        // Update cached Python delta
        if (vd.delta_data) {
            auto it = cached_py_deltas_.find(vd.delta_data);
            if (it != cached_py_deltas_.end()) {
                nb::object added_attr = nb::borrow(it->second.attr("added"));
                nb::object removed_attr = nb::borrow(it->second.attr("removed"));
                nb::set existing_added = nb::steal<nb::set>(PySet_New(added_attr.ptr()));
                nb::set existing_removed = nb::steal<nb::set>(PySet_New(removed_attr.ptr()));
                if (existing_added.contains(py_elem)) {
                    existing_added.discard(py_elem);
                } else {
                    existing_removed.add(py_elem);
                }
                nb::module_ tss_mod = nb::module_::import_("hgraph._impl._types._tss");
                it->second = tss_mod.attr("PythonSetDelta")(
                    nb::frozenset(existing_added), nb::frozenset(existing_removed));
            } else {
                nb::set py_removed;
                py_removed.add(py_elem);
                nb::module_ tss_mod = nb::module_::import_("hgraph._impl._types._tss");
                cached_py_deltas_[vd.delta_data] = tss_mod.attr("PythonSetDelta")(
                    nb::frozenset(nb::set()), nb::frozenset(py_removed));
            }
        }

        auto* t = get_container_time(vd);
        if (t) *t = current_time;

        auto* obs = get_container_observers(vd);
        if (obs) obs->notify_modified(current_time);

        update_is_empty(vd, current_time);
    }

    return removed;
}

void set_clear(ViewData& vd, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("set_clear on invalid ViewData");
    }

    auto* storage = get_set_storage(vd);
    if (!storage) return;

    if (!storage->empty()) {
        storage->clear();

        auto* t = get_container_time(vd);
        if (t) *t = current_time;

        auto* obs = get_container_observers(vd);
        if (obs) obs->notify_modified(current_time);

        update_is_empty(vd, current_time);
    }
}

void set_active(ViewData& vd, value::View active_view, bool active, TSInput* input) {
    if (!active_view) return;

    // TSS active schema is just a bool (set elements are values, not time-series)
    *static_cast<bool*>(active_view.data()) = active;

    // Manage node-scheduling subscription for bound TSS
    if (vd.link_data) {
        if (vd.uses_link_target) {
            // TSInput: Use ActiveNotifier
            auto* lt = get_link_target(vd.link_data);
            if (lt && lt->is_linked && lt->observer_data) {
                auto* observers = static_cast<ObserverList*>(lt->observer_data);
                if (active) {
                    if (lt->active_notifier.owning_input == nullptr) {
                        lt->active_notifier.owning_input = input;
                        observers->add_observer(&lt->active_notifier);
                    }
                } else {
                    if (lt->active_notifier.owning_input != nullptr) {
                        observers->remove_observer(&lt->active_notifier);
                        lt->active_notifier.owning_input = nullptr;
                    }
                }
            } else if (lt && lt->ref_binding_) {
                // REF binding exists but hasn't resolved yet.
                // Set owning_input so REFBindingHelper::rebind() can subscribe later.
                if (active) {
                    if (lt->active_notifier.owning_input == nullptr) {
                        lt->active_notifier.owning_input = input;
                    }
                } else {
                    lt->active_notifier.owning_input = nullptr;
                }
            }
        } else {
            // TSOutput: subscribe input directly
            auto* rl = get_ref_link(vd.link_data);
            if (rl && rl->target().is_linked && rl->target().observer_data) {
                auto* observers = static_cast<ObserverList*>(rl->target().observer_data);
                if (active) {
                    observers->add_observer(input);
                } else {
                    observers->remove_observer(input);
                }
            }
        }
    }
}

void clear_caches() {
    cached_py_deltas_.clear();
}

} // namespace set_ops

// ============================================================================
// Dict Operations (TSD)
// ============================================================================

namespace dict_ops {

// Forward declarations for functions used by all_valid
TSView child_at(const ViewData& vd, size_t index, engine_time_t current_time);
size_t child_count(const ViewData& vd);

// Forward declarations for functions used by from_python
bool dict_remove(ViewData& vd, const value::View& key, engine_time_t current_time);
TSView dict_set(ViewData& vd, const value::View& key, const value::View& value, engine_time_t current_time);
TSView dict_create(ViewData& vd, const value::View& key, engine_time_t current_time);

// Helper: Get/set element time from a VarList slot.
// For scalar elements (TS, TSS, REF, SIGNAL), the VarList element is a plain engine_time_t.
// For composite elements (TSD, TSB, TSL), it is a tuple where at(0) is the container time.
inline engine_time_t get_elem_time(const value::ListView& time_list, size_t slot) {
    auto elem = time_list.at(slot);
    if (auto tpl = elem.try_as_tuple()) {
        return tpl->at(0).as<engine_time_t>();
    }
    return elem.as<engine_time_t>();
}

inline void set_elem_time(const value::ListView& time_list, size_t slot, engine_time_t t) {
    auto elem = time_list.at(slot);
    if (auto tpl = elem.try_as_tuple()) {
        tpl->at(0).as<engine_time_t>() = t;
    } else {
        elem.as<engine_time_t>() = t;
    }
}

// For TSD types:
// - value is map type
// - time is tuple[engine_time_t, var_list[element_times]]
// - observer is tuple[ObserverList, var_list[element_observers]]
// - delta is MapDelta
// - link is LinkTarget (TSInput) or REFLink (TSOutput) for collection-level binding

// Get the child-to-container notifier from the TSD's MapDelta.
// The notifier is owned by MapDelta and lives as long as the TSD.
inline MapDelta::ChildNotifier* get_or_create_child_notifier(const ViewData& vd) {
    auto* map_delta = vd.delta_data ? static_cast<MapDelta*>(vd.delta_data) : nullptr;
    if (!map_delta) return nullptr;

    auto time_view = make_time_view(vd);
    auto observer_view = make_observer_view(vd);
    if (!time_view.valid() || !observer_view.valid()) return nullptr;

    auto* container_time = &time_view.as_tuple().at(0).as<engine_time_t>();
    auto* container_obs = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
    return map_delta->get_child_notifier(container_time, container_obs);
}

// ========== TSD Link Layout Helpers ==========
// TSD link storage is a tuple[collection_link, var_list[element_link]].
// These helpers extract the collection-level link from the tuple, shadowing
// the outer-namespace helpers that assume link_data points directly to a
// REFLink/LinkTarget. Element-level ViewData (produced by child_at/child_by_key)
// has link_data pointing into the var_list, so outer helpers work for elements.

// Get the correct link schema for this TSD (input vs output)
inline const value::TypeMeta* get_dict_link_schema(const ViewData& vd) {
    if (!vd.meta) return nullptr;
    if (vd.uses_link_target) {
        return TSMetaSchemaCache::instance().get_input_link_schema(vd.meta);
    }
    return TSMetaSchemaCache::instance().get_link_schema(vd.meta);
}

// Extract collection-level link pointer from TSD's tuple link layout
// Returns pointer to the REFLink or LinkTarget at tuple.at(0)
inline void* get_collection_link_data(const ViewData& vd) {
    if (!vd.link_data) return nullptr;
    auto* schema = get_dict_link_schema(vd);
    if (!schema) return nullptr;
    value::View lv(vd.link_data, schema);
    if (!lv.valid()) return nullptr;
    return lv.as_tuple().at(0).data();
}

// Shadow outer-namespace get_active_link for TSD tuple layout
inline const REFLink* get_active_link(const ViewData& vd) {
    if (vd.uses_link_target) return nullptr;
    void* cld = get_collection_link_data(vd);
    auto* rl = get_ref_link(cld);
    return (rl && rl->target().valid()) ? rl : nullptr;
}

// Shadow outer-namespace get_active_link_target for TSD tuple layout
inline const LinkTarget* get_active_link_target(const ViewData& vd) {
    if (!vd.uses_link_target) return nullptr;
    auto* lt = get_link_target(get_collection_link_data(vd));
    return (lt && lt->valid()) ? lt : nullptr;
}

// Shadow outer-namespace resolve_delegation_target for TSD tuple layout
inline std::optional<ViewData> resolve_delegation_target(const ViewData& vd) {
    if (auto* lt = get_active_link_target(vd)) {
        return make_view_data_from_link_target(*lt, vd.path);
    }
    if (auto* rl = get_active_link(vd)) {
        return make_view_data_from_link(*rl, vd.path);
    }
    return std::nullopt;
}

engine_time_t last_modified_time(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->last_modified_time(*target);
    auto time_view = make_time_view(vd);
    if (!time_view.valid()) return MIN_DT;
    return time_view.as_tuple().at(0).as<engine_time_t>();
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->modified(*target, current_time);
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->valid(*target);
    return last_modified_time(vd) != MIN_DT;
}

bool all_valid(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->all_valid(*target);

    // First check if this dict itself is valid
    if (!valid(vd)) return false;
    if (!vd.meta || !vd.meta->element_ts) return false;

    // Use MIN_DT (smallest valid time) as the current_time parameter for child_at
    engine_time_t query_time = MIN_DT;

    // Check all elements using child_at which handles links properly
    size_t count = child_count(vd);
    for (size_t i = 0; i < count; ++i) {
        TSView child_view = child_at(vd, i, query_time);

        // Check if the child is all_valid
        if (!child_view || !child_view.all_valid()) {
            return false;
        }
    }

    return true;
}

bool sampled(const ViewData& vd) {
    // Return the sampled flag from ViewData
    // If linked, also check target's sampled flag (propagates through chain)
    if (auto* rl = get_active_link(vd)) {
        return vd.sampled || rl->target().ops->sampled(make_view_data_from_link(*rl, vd.path, vd.sampled));
    }
    return vd.sampled;
}

value::View value(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->value(make_view_data_from_link(*rl, vd.path, vd.sampled));
    }
    return make_value_view(vd);
}

value::View delta_value(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->delta_value(make_view_data_from_link(*rl, vd.path, vd.sampled));
    }
    return make_delta_view(vd);
}

bool has_delta(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->has_delta(make_view_data_from_link(*rl, vd.path, vd.sampled));
    }
    return vd.delta_data != nullptr;
}

void set_value(ViewData& vd, const value::View& src, engine_time_t current_time) {
    // If linked, delegate to target (write through)
    if (auto* rl = get_ref_link(get_collection_link_data(vd))) {
        if (rl->target().valid()) {
            ViewData target_vd = make_view_data_from_link(*rl, vd.path, vd.sampled);
            rl->target().ops->set_value(target_vd, src, current_time);
            return;
        }
    }

    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("set_value on invalid ViewData");
    }

    auto dst = make_value_view(vd);
    dst.copy_from(src);

    auto time_view = make_time_view(vd);
    time_view.as_tuple().at(0).as<engine_time_t>() = current_time;

    if (vd.observer_data) {
        auto observer_view = make_observer_view(vd);
        auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
        observers->notify_modified(current_time);
    }
}

void apply_delta(ViewData& vd, const value::View& delta, engine_time_t current_time) {
    // If linked, delegate to target
    if (auto* rl = get_ref_link(get_collection_link_data(vd))) {
        if (rl->target().valid()) {
            ViewData target_vd = make_view_data_from_link(*rl, vd.path);
            rl->target().ops->apply_delta(target_vd, delta, current_time);
            return;
        }
    }

    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("apply_delta on invalid ViewData");
    }

    if (!delta.valid()) {
        return;  // Nothing to apply
    }

    auto dst = make_value_view(vd);
    if (!dst.valid()) {
        throw std::runtime_error("apply_delta: TSD has no valid storage");
    }

    auto map_view = dst.as_map();

    // The delta should be a bundle-like structure with 'added', 'modified'/'updated', and 'removed' fields
    // For C++ MapDeltaStorage, it has added/updated/removed collections
    // For Python, it's typically a dict/object with these attributes
    if (delta.is_bundle()) {
        auto delta_bundle = delta.as_bundle();

        // Process removals first (to avoid removing newly added entries)
        if (delta_bundle.has_field("removed")) {
            auto removed_view = delta_bundle.at("removed");
            if (removed_view.is_set()) {
                // removed is a set of keys
                for (auto key : removed_view.as_set()) {
                    map_view.remove(key);
                }
            } else if (removed_view.is_list()) {
                // removed might be a list of keys
                for (auto key : removed_view.as_list()) {
                    map_view.remove(key);
                }
            }
        }

        // Process additions (new keys with values)
        if (delta_bundle.has_field("added")) {
            auto added_view = delta_bundle.at("added");
            if (added_view.is_map()) {
                // added is a map of key->value pairs
                for (auto [key, value] : added_view.as_map().items()) {
                    map_view.set_item(key, value);
                }
            }
        }

        // Process modifications/updates (existing keys with new values)
        // Try both "modified" and "updated" field names for compatibility
        value::View modified_view;
        if (delta_bundle.has_field("modified")) {
            modified_view = delta_bundle.at("modified");
        } else if (delta_bundle.has_field("updated")) {
            modified_view = delta_bundle.at("updated");
        }

        if (modified_view.valid() && modified_view.is_map()) {
            // modified/updated is a map of key->value pairs
            for (auto [key, value] : modified_view.as_map().items()) {
                map_view.set_item(key, value);
            }
        }
    } else if (delta.is_map()) {
        // If delta is just a map, treat it as "set all" (replace operation)
        // This is a fallback for simple cases
        map_view.clear();
        for (auto [key, value] : delta.as_map().items()) {
            map_view.set_item(key, value);
        }
    } else {
        throw std::runtime_error("apply_delta for TSD: delta must be a bundle with 'added'/'modified'/'removed' fields or a map");
    }

    // Update modification time
    auto time_view = make_time_view(vd);
    time_view.as_tuple().at(0).as<engine_time_t>() = current_time;

    // Notify observers
    if (vd.observer_data) {
        auto observer_view = make_observer_view(vd);
        auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
        observers->notify_modified(current_time);
    }
}

void invalidate(ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_ref_link(get_collection_link_data(vd))) {
        if (rl->target().valid()) {
            ViewData target_vd = make_view_data_from_link(*rl, vd.path);
            rl->target().ops->invalidate(target_vd);
            return;
        }
    }

    if (vd.time_data) {
        auto time_view = make_time_view(vd);
        time_view.as_tuple().at(0).as<engine_time_t>() = MIN_DT;
    }
    // Also reset key_time in MapDelta
    auto* map_delta = vd.delta_data ? static_cast<MapDelta*>(vd.delta_data) : nullptr;
    if (map_delta) map_delta->set_key_time(MIN_DT);
}

nb::object to_python(const ViewData& vd) {
    // If linked via LinkTarget (TSInput), delegate to target
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->to_python(make_view_data_from_link_target(*lt, vd.path));
    }
    // If linked via REFLink (TSOutput), delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->to_python(make_view_data_from_link(*rl, vd.path));
    }
    // Check time-series validity first (has value been set?)
    if (!valid(vd)) { return nb::none(); }

    // Python: frozendict({k: v.value for k, v in self.items() if v.valid})
    // Iterate elements, skip invalid, convert values to Python
    if (!vd.value_data || !vd.time_data || !vd.meta || !vd.meta->element_ts) {
        return nb::none();
    }

    auto* storage = static_cast<value::MapStorage*>(vd.value_data);
    auto time_view = make_time_view(vd);
    auto time_list = time_view.as_tuple().at(1).as_list();

    const auto* key_tm = storage->key_type();
    const auto* val_tm = storage->value_type();
    const TSMeta* elem_ts = vd.meta->element_ts;
    bool elem_is_ref = (elem_ts && elem_ts->kind == TSKind::REF);

    nb::dict result;

    auto& key_set = storage->key_set();
    auto* index_set = key_set.index_set();
    if (index_set) {
        for (auto slot : *index_set) {
            if (slot < time_list.size()) {
                if (elem_is_ref) {
                    // Element is REF-wrapped (TSD output alternative).
                    // The alternative's element time is not synced with the native's,
                    // so resolve the TSReference and check the native's validity.
                    auto* ref = static_cast<TSReference*>(storage->value_at_slot(slot));
                    if (ref && !ref->is_empty()) {
                        TSView target = ref->resolve(MIN_DT);
                        if (target && target.view_data().ops && target.view_data().ops->valid(target.view_data())) {
                            const void* key_data = key_set.key_at_slot(slot);
                            value::View key_view(key_data, key_tm);
                            nb::object py_val = target.view_data().ops->to_python(target.view_data());
                            if (!py_val.is_none()) {
                                result[key_view.to_python()] = py_val;
                            }
                        }
                    }
                } else {
                    engine_time_t elem_time = get_elem_time(time_list, slot);
                    if (elem_time != MIN_DT) {
                        const void* key_data = key_set.key_at_slot(slot);
                        value::View key_view(key_data, key_tm);

                        if (elem_ts->kind == TSKind::TSValue || elem_ts->kind == TSKind::SIGNAL) {
                            // Scalar element — use value directly from MapStorage
                            if (val_tm && val_tm->ops) {
                                void* val_data = storage->value_at_slot(slot);
                                value::View val_view(val_data, val_tm);
                                result[key_view.to_python()] = val_view.to_python();
                            }
                        } else {
                            // Composite element (TSD, TSB, TSL, TSS) — use TS-level to_python
                            auto observer_view = make_observer_view(vd);
                            auto observer_list = observer_view.as_tuple().at(1).as_list();
                            ViewData elem_vd;
                            elem_vd.path = vd.path.child(slot);
                            elem_vd.value_data = storage->value_at_slot(slot);
                            elem_vd.time_data = time_list.at(slot).data();
                            elem_vd.observer_data = (slot < observer_list.size()) ? observer_list.at(slot).data() : nullptr;
                            elem_vd.delta_data = nullptr;
                            elem_vd.uses_link_target = vd.uses_link_target;
                            elem_vd.ops = get_ts_ops(elem_ts);
                            elem_vd.meta = elem_ts;
                            nb::object py_val = elem_vd.ops->to_python(elem_vd);
                            if (!py_val.is_none()) {
                                result[key_view.to_python()] = py_val;
                            }
                        }
                    }
                }
            }
        }
    }

    nb::module_ frozendict_mod = nb::module_::import_("frozendict");
    return frozendict_mod.attr("frozendict")(result);
}

nb::object delta_to_python(const ViewData& vd) {
    // If linked via LinkTarget (TSInput), delegate to target
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->delta_to_python(make_view_data_from_link_target(*lt, vd.path));
    }
    // If linked via REFLink (TSOutput), delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->delta_to_python(make_view_data_from_link(*rl, vd.path));
    }
    // Check time-series validity first (has value been set?)
    if (!valid(vd)) { return nb::none(); }

    // TSD delta_value: frozendict of modified+valid elements' delta_values
    // Python: frozendict(chain(((k, v.delta_value) for k,v in items if v.modified and v.valid),
    //                          ((k, REMOVE) for k in removed_keys())))
    if (!vd.value_data || !vd.time_data || !vd.meta || !vd.meta->element_ts) {
        return nb::none();
    }

    auto* storage = static_cast<value::MapStorage*>(vd.value_data);
    auto time_view = make_time_view(vd);
    auto time_list = time_view.as_tuple().at(1).as_list();
    engine_time_t container_time = time_view.as_tuple().at(0).as<engine_time_t>();

    const auto* key_tm = storage->key_type();
    const auto* val_tm = storage->value_type();

    nb::dict result;

    // Iterate modified elements for delta output.
    // Two-pass approach:
    // Pass 1: MapDelta modified slots (catches structurally-added elements, especially
    //         REF elements whose targets may have been modified in a prior tick).
    // Pass 2: Time-based scan over ALL live elements (catches elements modified via
    //         forwarded outputs from nested graphs, which don't update MapDelta).
    auto& key_set = storage->key_set();
    const TSMeta* elem_ts = vd.meta->element_ts;
    bool elem_is_ref = (elem_ts && elem_ts->kind == TSKind::REF);

    MapDelta* map_delta_ptr = vd.delta_data ? static_cast<MapDelta*>(vd.delta_data) : nullptr;

    // Track slots already emitted to avoid duplicates between passes
    std::unordered_set<size_t> emitted_slots;

    // Pass 1: MapDelta modified slots
    if (map_delta_ptr && !map_delta_ptr->modified().empty()) {
        for (auto slot : map_delta_ptr->modified()) {
            if (slot < time_list.size()) {
                if (elem_is_ref) {
                    auto* ref = static_cast<TSReference*>(storage->value_at_slot(slot));
                    if (ref && !ref->is_empty()) {
                        TSView target = ref->resolve(container_time);
                        if (target && target.view_data().ops) {
                            const void* key_data = key_set.key_at_slot(slot);
                            value::View key_view(key_data, key_tm);
                            nb::object py_val = target.view_data().ops->to_python(target.view_data());
                            if (!py_val.is_none()) {
                                result[key_view.to_python()] = py_val;
                                emitted_slots.insert(slot);
                            }
                        }
                    }
                } else {
                    const void* key_data = key_set.key_at_slot(slot);
                    value::View key_view(key_data, key_tm);
                    if (val_tm && val_tm->ops) {
                        void* val_data = storage->value_at_slot(slot);
                        value::View val_view(val_data, val_tm);
                        result[key_view.to_python()] = val_view.to_python();
                        emitted_slots.insert(slot);
                    }
                }
            }
        }
    }

    // Pass 2: Time-based scan over ALL live elements
    // For REF elements: the TSReference itself may not change when the target is
    // modified (e.g., compute nodes in map_ write output but the REF stays the same).
    // We need the graph's actual evaluation_time to correctly filter, since
    // container_time only updates on structural changes (dict_create/dict_remove).
    engine_time_t ref_current_time = container_time;  // fallback
    if (elem_is_ref && vd.path.node()) {
        auto* g = vd.path.node()->graph();
        if (g) {
            ref_current_time = g->evaluation_time();
        }
    }

    auto* index_set = key_set.index_set();
    if (index_set) {
        for (auto slot : *index_set) {
            if (emitted_slots.count(slot)) continue;
            if (slot < time_list.size()) {
                engine_time_t elem_time = get_elem_time(time_list, slot);
                if (elem_is_ref) {
                    auto* ref = static_cast<TSReference*>(storage->value_at_slot(slot));
                    if (ref && !ref->is_empty()) {
                        TSView target = ref->resolve(ref_current_time);
                        if (target && target.view_data().ops) {
                            engine_time_t target_time = target.view_data().ops->last_modified_time(target.view_data());
                            if (target_time >= ref_current_time) {
                                const void* key_data = key_set.key_at_slot(slot);
                                value::View key_view(key_data, key_tm);
                                nb::object py_val = target.view_data().ops->to_python(target.view_data());
                                if (!py_val.is_none()) {
                                    result[key_view.to_python()] = py_val;
                                }
                            }
                        }
                    }
                } else if (elem_time >= container_time) {
                    const void* key_data = key_set.key_at_slot(slot);
                    value::View key_view(key_data, key_tm);
                    if (val_tm && val_tm->ops) {
                        void* val_data = storage->value_at_slot(slot);
                        value::View val_view(val_data, val_tm);
                        result[key_view.to_python()] = val_view.to_python();
                    }
                }
            }
        }
    }

    // Include REMOVE markers for removed keys (from MapDelta)
    if (vd.delta_data) {
        auto* map_delta = static_cast<MapDelta*>(vd.delta_data);
        const auto& removed_slots = map_delta->removed();
        if (!removed_slots.empty()) {
            // Import REMOVE sentinel
            nb::module_ tsd_mod = nb::module_::import_("hgraph._types._tsd_type");
            nb::object remove_sentinel = tsd_mod.attr("REMOVE");

            for (auto slot : removed_slots) {
                // Key data remains accessible at the slot during the current tick
                const void* key_data = key_set.key_at_slot(slot);
                if (key_data) {
                    value::View key_view(key_data, key_tm);
                    result[key_view.to_python()] = remove_sentinel;
                }
            }
        }
    }

    nb::module_ frozendict_mod = nb::module_::import_("frozendict");
    return frozendict_mod.attr("frozendict")(result);
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    // If linked, delegate to target (write through)
    if (auto* rl = get_ref_link(get_collection_link_data(vd))) {
        if (rl->target().valid()) {
            ViewData target_vd = make_view_data_from_link(*rl, vd.path);
            rl->target().ops->from_python(target_vd, src, current_time);
            return;
        }
    }

    if (src.is_none()) return;

    if (!vd.value_data || !vd.time_data || !vd.meta) {
        throw std::runtime_error("dict from_python on invalid ViewData");
    }

    // Clear delta if this is a new tick (lazy clearing)
    // The delta may contain stale data from the previous tick which would
    // cause incorrect add/remove cancellation in SetDelta::on_erase
    if (vd.delta_data) {
        auto time_view = make_time_view(vd);
        engine_time_t container_time = time_view.as_tuple().at(0).as<engine_time_t>();
        if (current_time > container_time) {
            // New tick - clear the delta
            auto* map_delta = static_cast<MapDelta*>(vd.delta_data);
            map_delta->clear();
        }
    }

    // Get key and element TypeMetas from the TSD meta
    const auto* key_tm = vd.meta->key_type;
    const auto* elem_ts = vd.meta->element_ts;
    if (!key_tm || !elem_ts) {
        throw std::runtime_error("dict from_python: missing key_type or element_ts in meta");
    }
    // value_type may be nullptr for composite element types (TSB, TSD, TSL inside TSD)
    const auto* val_tm = elem_ts->value_type;

    // Python: "if not self.valid and not v: self.key_set.mark_modified()"
    // Even empty dicts must mark the TSD as modified when it hasn't been set yet.
    auto py_len = nb::len(src);
    if (py_len == 0) {
        auto time_view = make_time_view(vd);
        engine_time_t container_time = time_view.as_tuple().at(0).as<engine_time_t>();
        // Not valid: container_time == MIN_DT means never been modified (MIN_DT = epoch = 0)
        if (container_time == MIN_DT) {
            // First tick with empty dict - stamp container time to mark as modified
            time_view.as_tuple().at(0).as<engine_time_t>() = current_time;
            // Also stamp key_time (Python: self.key_set.mark_modified())
            auto* map_delta = vd.delta_data ? static_cast<MapDelta*>(vd.delta_data) : nullptr;
            if (map_delta) map_delta->set_key_time(current_time);
            // Notify observers
            auto obs_view = make_observer_view(vd);
            auto* obs_list = static_cast<ObserverList*>(obs_view.as_tuple().at(0).data());
            if (obs_list) {
                obs_list->notify_modified(current_time);
            }
        }
        return;
    }

    // Two-pass approach: SET entries first, REMOVE entries second.
    // This prevents slot reuse from overwriting removed key data in MapDelta,
    // which would cause delta_to_python to report wrong keys for removed entries.
    auto items = src.attr("items")();

    // Collect remove entries for second pass
    std::vector<std::pair<nb::object, std::string>> remove_entries;

    // First pass: process SET entries
    for (auto item : items) {
        auto kv = nb::cast<nb::tuple>(item);
        nb::object py_key = kv[0];
        nb::object py_val = kv[1];

        // Skip None values (matches Python behavior)
        if (py_val.is_none()) continue;

        // Check for REMOVE/REMOVE_IF_EXISTS sentinel
        bool is_sentinel = false;
        std::string sentinel_name_str;
        if (nb::hasattr(py_val, "name")) {
            nb::object cls_name = py_val.type().attr("__name__");
            std::string_view cls_sv(nb::cast<const char*>(cls_name));
            if (cls_sv == "Sentinel") {
                is_sentinel = true;
                nb::object sname = py_val.attr("name");
                sentinel_name_str = nb::cast<std::string>(sname);
            }
        }
        if (is_sentinel) {
            // Defer to second pass
            remove_entries.emplace_back(std::move(py_key), std::move(sentinel_name_str));
            continue;
        }

        // Normal value - convert key and value, then use dict_set
        value::Value<> key_val(key_tm);
        key_val.view().from_python(py_key);

        if (val_tm && elem_ts->kind == TSKind::TSValue) {
            // Scalar element: use value-level conversion + dict_set
            value::Value<> elem_val(val_tm);
            elem_val.view().from_python(py_val);
            dict_set(vd, key_val.view(), elem_val.view(), current_time);
        } else {
            // Composite element (TSD, TSB, TSL, TSS, REF): create slot, use TS-level from_python
            TSView elem_view = dict_create(vd, key_val.view(), current_time);
            ViewData elem_vd = elem_view.view_data();
            get_ts_ops(elem_ts)->from_python(elem_vd, py_val, current_time);
        }
    }

    // Second pass: process REMOVE entries
    for (auto& [py_key, sentinel_name] : remove_entries) {
        std::string_view name_sv(sentinel_name);
        value::Value<> key_val(key_tm);
        key_val.view().from_python(py_key);

        if (name_sv == "REMOVE_IF_EXISTS") {
            auto* storage = static_cast<value::MapStorage*>(vd.value_data);
            if (storage->contains(key_val.view().data())) {
                dict_remove(vd, key_val.view(), current_time);
            }
        } else {
            dict_remove(vd, key_val.view(), current_time);
        }
    }
}

TSView child_at(const ViewData& vd, size_t slot, engine_time_t current_time) {
    // If linked, navigate through target
    // Set sampled flag if REF was rebound at current_time OR if parent was already sampled
    if (auto* rl = get_active_link(vd)) {
        bool is_sampled = vd.sampled || is_ref_sampled(*rl, current_time);
        ViewData target_vd = make_view_data_from_link(*rl, vd.path, is_sampled);
        TSView result = rl->target().ops->child_at(target_vd, slot, current_time);
        if (is_sampled && result.view_data().valid()) {
            result.view_data().sampled = true;
        }
        return result;
    }

    // TSD_KEY_SET_SLOT: navigate to the key_set child (TSS)
    if (slot == TSD_KEY_SET_SLOT) {
        TSDView dict_view(vd, current_time);
        TSSView tss = dict_view.key_set();
        return TSView(tss.view_data(), current_time);
    }

    if (!vd.meta || !vd.meta->element_ts) return TSView{};

    auto time_view = make_time_view(vd);
    auto observer_view = make_observer_view(vd);

    // 'slot' is a STORAGE SLOT index (same convention as child_by_key and ShortPath).
    // child_by_key stores storage slots in the path, and ShortPath::resolve() navigates
    // via child_at, so this must use storage-slot semantics for consistency.
    auto* storage = static_cast<value::MapStorage*>(vd.value_data);
    if (!storage->key_set().is_alive(slot)) return TSView{};

    const TSMeta* elem_meta = vd.meta->element_ts;

    ViewData elem_vd;
    elem_vd.path = vd.path.child(slot);
    elem_vd.value_data = storage->value_at_slot(slot);
    elem_vd.time_data = time_view.as_tuple().at(1).as_list().at(slot).data();
    elem_vd.observer_data = observer_view.as_tuple().at(1).as_list().at(slot).data();
    // For nested TSD elements, provide a child MapDelta for delta tracking.
    // This allows nested TSD key_set() and delta access to work correctly.
    if (elem_meta->kind == TSKind::TSD && vd.delta_data) {
        auto* parent_delta = static_cast<MapDelta*>(vd.delta_data);
        auto* inner_storage = static_cast<value::MapStorage*>(elem_vd.value_data);
        elem_vd.delta_data = parent_delta->get_or_create_child_map_delta(slot, inner_storage);
    } else {
        elem_vd.delta_data = nullptr;
    }
    elem_vd.sampled = vd.sampled;  // Propagate sampled flag from parent
    elem_vd.uses_link_target = vd.uses_link_target;  // Propagate link type flag
    elem_vd.ops = get_ts_ops(elem_meta);
    elem_vd.meta = elem_meta;

    // Set per-element link_data from the TSD's link var_list
    if (vd.link_data) {
        auto* link_schema = get_dict_link_schema(vd);
        if (link_schema) {
            value::View link_view(vd.link_data, link_schema);
            auto link_list = link_view.as_tuple().at(1).as_list();
            if (slot < link_list.size()) {
                elem_vd.link_data = link_list.at(slot).data();
            }
        }
    }

    return TSView(elem_vd, current_time);
}

TSView child_by_name(const ViewData& vd, const std::string& name, engine_time_t current_time) {
    // If linked, navigate through target
    // Set sampled flag if REF was rebound at current_time OR if parent was already sampled
    if (auto* rl = get_active_link(vd)) {
        bool is_sampled = vd.sampled || is_ref_sampled(*rl, current_time);
        ViewData target_vd = make_view_data_from_link(*rl, vd.path, is_sampled);
        TSView result = rl->target().ops->child_by_name(target_vd, name, current_time);
        if (is_sampled && result.view_data().valid()) {
            result.view_data().sampled = true;
        }
        return result;
    }
    // TSD uses keys, not names - would need key type conversion
    return TSView{};
}

TSView child_by_key(const ViewData& vd, const value::View& key, engine_time_t current_time) {
    // If linked, navigate through target
    // Set sampled flag if REF was rebound at current_time OR if parent was already sampled
    if (auto* rl = get_active_link(vd)) {
        bool is_sampled = vd.sampled || is_ref_sampled(*rl, current_time);
        ViewData target_vd = make_view_data_from_link(*rl, vd.path, is_sampled);
        TSView result = rl->target().ops->child_by_key(target_vd, key, current_time);
        if (is_sampled && result.view_data().valid()) {
            result.view_data().sampled = true;
        }
        return result;
    }

    if (!vd.meta || !vd.meta->element_ts) return TSView{};

    auto time_view = make_time_view(vd);
    auto observer_view = make_observer_view(vd);

    // Find the slot for this key
    auto* storage = static_cast<value::MapStorage*>(vd.value_data);
    size_t slot = storage->key_set().find(key.data());

    // Check if the key exists (slot is not empty)
    if (slot == static_cast<size_t>(-1)) {
        return TSView{};
    }

    const TSMeta* elem_meta = vd.meta->element_ts;

    ViewData elem_vd;
    elem_vd.path = vd.path.child(slot);
    elem_vd.value_data = storage->value_at_slot(slot);
    {
        auto time_list = time_view.as_tuple().at(1).as_list();
        auto obs_list = observer_view.as_tuple().at(1).as_list();
        elem_vd.time_data = time_list.at(slot).data();
        elem_vd.observer_data = obs_list.at(slot).data();
    }
    if (elem_meta->kind == TSKind::TSD && vd.delta_data) {
        auto* parent_delta = static_cast<MapDelta*>(vd.delta_data);
        auto* inner_storage = static_cast<value::MapStorage*>(elem_vd.value_data);
        elem_vd.delta_data = parent_delta->get_or_create_child_map_delta(slot, inner_storage);
    } else {
        elem_vd.delta_data = nullptr;
    }
    elem_vd.sampled = vd.sampled;  // Propagate sampled flag from parent
    elem_vd.uses_link_target = vd.uses_link_target;  // Propagate link type flag
    elem_vd.ops = get_ts_ops(elem_meta);
    elem_vd.meta = elem_meta;

    // Set per-element link_data from the TSD's link var_list
    if (vd.link_data) {
        auto* link_schema = get_dict_link_schema(vd);
        if (link_schema) {
            value::View link_view(vd.link_data, link_schema);
            auto link_list = link_view.as_tuple().at(1).as_list();
            if (slot < link_list.size()) {
                elem_vd.link_data = link_list.at(slot).data();
            }
        }
    }

    return TSView(elem_vd, current_time);
}

size_t child_count(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->child_count(make_view_data_from_link(*rl, vd.path, vd.sampled));
    }
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->child_count(make_view_data_from_link_target(*lt, vd.path));
    }
    auto value_view = make_value_view(vd);
    if (!value_view.valid()) return 0;
    return value_view.as_map().size();
}

value::View observer(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->observer(make_view_data_from_link(*rl, vd.path));
    }
    return make_observer_view(vd);
}

void notify_observers(ViewData& vd, engine_time_t current_time) {
    // If linked, delegate to target
    if (auto* rl = get_ref_link(get_collection_link_data(vd))) {
        if (rl->target().valid()) {
            ViewData target_vd = make_view_data_from_link(*rl, vd.path);
            rl->target().ops->notify_observers(target_vd, current_time);
            return;
        }
    }

    if (vd.observer_data) {
        auto observer_view = make_observer_view(vd);
        auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
        observers->notify_modified(current_time);
    }
}

void bind(ViewData& vd, const ViewData& target) {
    // TSD: Bind the entire dict to target (collection-level link)
    if (!vd.link_data) {
        throw std::runtime_error("bind on dict without link data");
    }

    if (vd.uses_link_target) {
        // TSInput: Use LinkTarget
        auto* lt = get_link_target(get_collection_link_data(vd));
        if (!lt) {
            throw std::runtime_error("bind on dict with invalid link data");
        }

        // Set time-accounting chain
        if (vd.time_data) {
            lt->owner_time_ptr = static_cast<engine_time_t*>(vd.time_data);
        }

        // Check if target is REF - need REFBindingHelper
        if (target.meta && target.meta->kind == TSKind::REF) {
            if (lt->ref_binding_) {
                auto* old_helper = static_cast<REFBindingHelper*>(lt->ref_binding_);
                old_helper->unsubscribe_all();
                if (lt->ref_binding_deleter_) {
                    lt->ref_binding_deleter_(lt->ref_binding_);
                }
                lt->ref_binding_ = nullptr;
                lt->ref_binding_deleter_ = nullptr;
                lt->is_linked = false;
                lt->value_data = nullptr;
                lt->time_data = nullptr;
                lt->observer_data = nullptr;
                lt->delta_data = nullptr;
                lt->link_data = nullptr;
                lt->ops = nullptr;
                lt->meta = nullptr;
            }

            auto* helper = new REFBindingHelper(lt, target, false);
            lt->ref_binding_ = helper;
            lt->ref_binding_deleter_ = &delete_ref_binding_helper;
            helper->subscribe_to_ref_source();
            helper->rebind(MIN_DT);
            return;
        }

        store_to_link_target(*lt, target);
        // Mark peered: binding happened at the dict level
        lt->peered = true;

        // Subscribe for time-accounting
        if (lt->observer_data) {
            auto* obs = static_cast<ObserverList*>(lt->observer_data);
            obs->add_observer(lt);
        }
    } else {
        // TSOutput: Use REFLink
        auto* rl = get_ref_link(get_collection_link_data(vd));
        if (!rl) {
            throw std::runtime_error("bind on dict with invalid link data");
        }
        store_link_target(*rl, target);
    }
}

void unbind(ViewData& vd) {
    if (!vd.link_data) {
        return;  // No-op if not linked
    }

    if (vd.uses_link_target) {
        // TSInput: Unsubscribe both chains then clear
        auto* lt = get_link_target(get_collection_link_data(vd));
        if (!lt) return;

        if (lt->ref_binding_) {
            auto* helper = static_cast<REFBindingHelper*>(lt->ref_binding_);
            helper->unsubscribe_all();
            lt->cleanup_ref_binding();
            lt->clear();
        } else if (lt->is_linked) {
            if (lt->observer_data) {
                auto* obs = static_cast<ObserverList*>(lt->observer_data);
                obs->remove_observer(lt);
                if (lt->active_notifier.owning_input != nullptr) {
                    obs->remove_observer(&lt->active_notifier);
                    lt->active_notifier.owning_input = nullptr;
                }
            }
            lt->clear();
        }
    } else {
        // TSOutput: Use REFLink
        auto* rl = get_ref_link(get_collection_link_data(vd));
        if (rl) {
            rl->unbind();
        }
    }
}

bool is_bound(const ViewData& vd) {
    if (vd.uses_link_target) {
        // TSInput: Use LinkTarget
        auto* lt = get_link_target(get_collection_link_data(vd));
        return lt && (lt->is_linked || lt->ref_binding_);
    } else {
        // TSOutput: Use REFLink
        auto* rl = get_ref_link(get_collection_link_data(vd));
        return rl && rl->target().is_linked;
    }
}

bool is_peered(const ViewData& vd) {
    // TSD: check collection-level LinkTarget's peered flag
    if (!vd.link_data || !vd.uses_link_target) return false;
    auto* lt = get_link_target(get_collection_link_data(vd));
    return lt && lt->peered;
}

// ========== Dict-Specific Mutation Operations ==========

bool dict_remove(ViewData& vd, const value::View& key, engine_time_t current_time) {
    // If linked, delegate to target (write through)
    if (auto* rl = get_ref_link(get_collection_link_data(vd))) {
        if (rl->target().valid()) {
            ViewData target_vd = make_view_data_from_link(*rl, vd.path);
            return rl->target().ops->dict_remove(target_vd, key, current_time);
        }
    }

    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("dict_remove on invalid ViewData");
    }

    // Get the MapStorage
    auto* storage = static_cast<value::MapStorage*>(vd.value_data);

    // Remove the key (MapDelta is notified via SlotObserver if registered)
    bool removed = storage->remove(key.data());

    if (removed) {
        // Update key set modification time (separate from container time)
        auto* map_delta = vd.delta_data ? static_cast<MapDelta*>(vd.delta_data) : nullptr;
        if (map_delta) map_delta->set_key_time(current_time);

        // Update container timestamp
        auto time_view = make_time_view(vd);
        time_view.as_tuple().at(0).as<engine_time_t>() = current_time;

        // Notify observers
        if (vd.observer_data) {
            auto observer_view = make_observer_view(vd);
            auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
            observers->notify_modified(current_time);
        }
    }

    return removed;
}

TSView dict_create(ViewData& vd, const value::View& key, engine_time_t current_time) {
    // If linked, delegate to target (write through)
    if (auto* rl = get_ref_link(get_collection_link_data(vd))) {
        if (rl->target().valid()) {
            ViewData target_vd = make_view_data_from_link(*rl, vd.path);
            return rl->target().ops->dict_create(target_vd, key, current_time);
        }
    }

    if (!vd.value_data || !vd.time_data || !vd.meta || !vd.meta->element_ts) {
        throw std::runtime_error("dict_create on invalid ViewData");
    }

    // Get the MapStorage and check if key exists
    auto* storage = static_cast<value::MapStorage*>(vd.value_data);
    size_t existing_slot = storage->key_set().find(key.data());

    if (existing_slot != static_cast<size_t>(-1)) {
        // Key already exists - return existing entry
        auto time_view = make_time_view(vd);
        auto observer_view = make_observer_view(vd);
        const TSMeta* elem_meta = vd.meta->element_ts;

        ViewData elem_vd;
        elem_vd.path = vd.path.child(existing_slot);
        elem_vd.value_data = storage->value_at_slot(existing_slot);
        elem_vd.time_data = time_view.as_tuple().at(1).as_list().at(existing_slot).data();
        elem_vd.observer_data = observer_view.as_tuple().at(1).as_list().at(existing_slot).data();
        if (elem_meta->kind == TSKind::TSD && vd.delta_data) {
            auto* parent_delta = static_cast<MapDelta*>(vd.delta_data);
            auto* inner_storage = static_cast<value::MapStorage*>(elem_vd.value_data);
            elem_vd.delta_data = parent_delta->get_or_create_child_map_delta(existing_slot, inner_storage);
        } else {
            elem_vd.delta_data = nullptr;
        }
        elem_vd.uses_link_target = vd.uses_link_target;
        elem_vd.ops = get_ts_ops(elem_meta);
        elem_vd.meta = elem_meta;

        // Set per-element link_data from the TSD's link var_list
        if (vd.link_data) {
            auto* link_schema = get_dict_link_schema(vd);
            if (link_schema) {
                value::View link_view(vd.link_data, link_schema);
                auto link_list = link_view.as_tuple().at(1).as_list();
                if (existing_slot < link_list.size()) {
                    elem_vd.link_data = link_list.at(existing_slot).data();
                }
            }
        }

        return TSView(elem_vd, current_time);
    }

    // Get time and observer views for the var_lists
    auto time_view = make_time_view(vd);
    auto observer_view = make_observer_view(vd);

    // Access the element var_lists (index 1 in the tuple)
    auto time_list = time_view.as_tuple().at(1).as_list();
    auto observer_list = observer_view.as_tuple().at(1).as_list();

    // Pre-resize time/observer VarLists to match KeySet capacity BEFORE
    // calling set_item. This is critical because set_item triggers
    // AlternativeStructuralObserver::on_insert which navigates to the
    // newly-created element via child_at/child_by_key, which accesses
    // the time/observer VarLists.
    storage->key_set().ensure_capacity(storage->key_set().size() + 1);
    size_t cap = storage->key_set().capacity();
    if (time_list.size() < cap) {
        time_list.resize(cap);
    }
    if (observer_list.size() < cap) {
        observer_list.resize(cap);
    }

    // Pre-resize link VarList to match capacity
    auto* link_schema = get_dict_link_schema(vd);
    value::View link_view;
    value::ListView link_list;
    if (vd.link_data && link_schema) {
        link_view = value::View(vd.link_data, link_schema);
        link_list = link_view.as_tuple().at(1).as_list();
        if (link_list.size() < cap) {
            link_list.resize(cap);
        }
    }

    // Create default value for the key (this will be set on the slot)
    const value::TypeMeta* value_type = storage->value_type();
    value::Value<> default_value(value_type);

    // Insert the key into MapStorage (allocates slot and value storage)
    // This calls set_item which handles both key and value storage
    storage->set_item(key.data(), default_value.data());

    // Get the slot that was allocated
    size_t slot = storage->key_set().find(key.data());
    if (slot == static_cast<size_t>(-1)) {
        throw std::runtime_error("dict_create: failed to insert key");
    }

    // Initialize the time at this slot to MIN_DT (unset)
    set_elem_time(time_list, slot, MIN_DT);

    // The observer at this slot is already default-constructed by resize

    // Add child→container propagation notifier to the element's ObserverList.
    // This ensures that direct element writes (via get_or_create + ts.value = x)
    // propagate modified state up to the container.
    if (auto* notifier = get_or_create_child_notifier(vd)) {
        auto* elem_obs = static_cast<ObserverList*>(observer_list.at(slot).data());
        if (elem_obs) {
            elem_obs->add_observer(notifier);
        }
    }

    // Update key set modification time (separate from container time)
    auto* map_delta = vd.delta_data ? static_cast<MapDelta*>(vd.delta_data) : nullptr;
    if (map_delta) map_delta->set_key_time(current_time);

    // Update container timestamp
    time_view.as_tuple().at(0).as<engine_time_t>() = current_time;

    // Notify container observers
    if (vd.observer_data) {
        auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
        observers->notify_modified(current_time);
    }

    // Build ViewData for the new element
    const TSMeta* elem_meta = vd.meta->element_ts;

    ViewData elem_vd;
    elem_vd.path = vd.path.child(slot);
    elem_vd.value_data = storage->value_at_slot(slot);
    elem_vd.time_data = time_list.at(slot).data();
    elem_vd.observer_data = observer_list.at(slot).data();
    if (elem_meta->kind == TSKind::TSD && vd.delta_data) {
        auto* parent_delta = static_cast<MapDelta*>(vd.delta_data);
        auto* inner_storage = static_cast<value::MapStorage*>(elem_vd.value_data);
        elem_vd.delta_data = parent_delta->get_or_create_child_map_delta(slot, inner_storage);
    } else {
        elem_vd.delta_data = nullptr;
    }
    elem_vd.uses_link_target = vd.uses_link_target;
    elem_vd.ops = get_ts_ops(elem_meta);
    elem_vd.meta = elem_meta;

    // Set per-element link_data from the TSD's link var_list
    if (link_list.valid() && slot < link_list.size()) {
        elem_vd.link_data = link_list.at(slot).data();
    }

    return TSView(elem_vd, current_time);
}

TSView dict_set(ViewData& vd, const value::View& key, const value::View& value, engine_time_t current_time) {
    // If linked, delegate to target (write through)
    if (auto* rl = get_ref_link(get_collection_link_data(vd))) {
        if (rl->target().valid()) {
            ViewData target_vd = make_view_data_from_link(*rl, vd.path);
            return rl->target().ops->dict_set(target_vd, key, value, current_time);
        }
    }

    if (!vd.value_data || !vd.time_data || !vd.meta || !vd.meta->element_ts) {
        throw std::runtime_error("dict_set on invalid ViewData");
    }

    // Get the MapStorage
    auto* storage = static_cast<value::MapStorage*>(vd.value_data);

    // Get time and observer views
    auto time_view = make_time_view(vd);
    auto observer_view = make_observer_view(vd);
    auto time_list = time_view.as_tuple().at(1).as_list();
    auto observer_list = observer_view.as_tuple().at(1).as_list();

    // Prepare link var_list (used below for both pre-resize and elem_vd)
    auto* link_schema = get_dict_link_schema(vd);
    value::View link_view;
    value::ListView link_list;
    if (vd.link_data && link_schema) {
        link_view = value::View(vd.link_data, link_schema);
        link_list = link_view.as_tuple().at(1).as_list();
    }

    // Check if key exists
    size_t slot = storage->key_set().find(key.data());
    bool is_new = (slot == static_cast<size_t>(-1));

    if (is_new) {
        // Pre-resize time/observer/link VarLists to match KeySet capacity BEFORE
        // calling set_item. This is critical because set_item triggers
        // AlternativeStructuralObserver::on_insert which navigates to the
        // newly-created element via child_at/child_by_key, which accesses
        // the time/observer VarLists. Without pre-resizing, the VarLists
        // would still be too small for the new slot.
        storage->key_set().ensure_capacity(storage->key_set().size() + 1);
        size_t cap = storage->key_set().capacity();
        if (time_list.size() < cap) {
            time_list.resize(cap);
        }
        if (observer_list.size() < cap) {
            observer_list.resize(cap);
        }
        if (link_list.valid() && link_list.size() < cap) {
            link_list.resize(cap);
        }

        // Now insert — observers can safely access any slot < capacity
        storage->set_item(key.data(), value.data());

        // Get the allocated slot
        slot = storage->key_set().find(key.data());
        if (slot == static_cast<size_t>(-1)) {
            throw std::runtime_error("dict_set: failed to insert key");
        }

        // Add child→container propagation notifier to the new element's ObserverList
        if (auto* notifier = get_or_create_child_notifier(vd)) {
            auto* elem_obs = static_cast<ObserverList*>(observer_list.at(slot).data());
            if (elem_obs) {
                elem_obs->add_observer(notifier);
            }
        }

        // Update key set modification time (new key added)
        auto* map_delta = vd.delta_data ? static_cast<MapDelta*>(vd.delta_data) : nullptr;
        if (map_delta) map_delta->set_key_time(current_time);
    } else {
        // Key exists - just update the value
        void* val_ptr = storage->value_at_slot(slot);
        const value::TypeMeta* value_type = storage->value_type();
        if (value_type && value_type->ops && value_type->ops->copy_assign) {
            value_type->ops->copy_assign(val_ptr, value.data(), value_type);
        }
        // Notify MapDelta of the update (so modified_keys/modified_values work)
        storage->key_set().observer_dispatcher().notify_update(slot);
    }

    // Update element timestamp
    set_elem_time(time_list, slot, current_time);

    // Notify element observers
    auto* elem_observers = static_cast<ObserverList*>(observer_list.at(slot).data());
    if (elem_observers) {
        elem_observers->notify_modified(current_time);
    }

    // Update container timestamp
    time_view.as_tuple().at(0).as<engine_time_t>() = current_time;

    // Notify container observers
    if (vd.observer_data) {
        auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
        observers->notify_modified(current_time);
    }

    // Build ViewData for the element
    const TSMeta* elem_meta = vd.meta->element_ts;

    ViewData elem_vd;
    elem_vd.path = vd.path.child(slot);
    elem_vd.value_data = storage->value_at_slot(slot);
    elem_vd.time_data = time_list.at(slot).data();
    elem_vd.observer_data = observer_list.at(slot).data();
    if (elem_meta->kind == TSKind::TSD && vd.delta_data) {
        auto* parent_delta = static_cast<MapDelta*>(vd.delta_data);
        auto* inner_storage = static_cast<value::MapStorage*>(elem_vd.value_data);
        elem_vd.delta_data = parent_delta->get_or_create_child_map_delta(slot, inner_storage);
    } else {
        elem_vd.delta_data = nullptr;
    }
    elem_vd.uses_link_target = vd.uses_link_target;
    elem_vd.ops = get_ts_ops(elem_meta);
    elem_vd.meta = elem_meta;

    // Set per-element link_data from the TSD's link var_list
    if (link_list.valid() && slot < link_list.size()) {
        elem_vd.link_data = link_list.at(slot).data();
    }

    return TSView(elem_vd, current_time);
}

void set_active(ViewData& vd, value::View active_view, bool active, TSInput* input) {
    if (!active_view || !vd.meta) return;

    // TSD active schema: tuple[bool, list[element_active]]
    value::TupleView tv = active_view.as_tuple();
    value::View root = tv[0];
    if (root) {
        *static_cast<bool*>(root.data()) = active;
    }

    // Set active for each element
    value::View element_list = tv[1];
    if (element_list && element_list.is_list()) {
        value::ListView lv = element_list.as_list();
        const TSMeta* elem_ts = vd.meta->element_ts;

        for (size_t i = 0; i < lv.size(); ++i) {
            value::View elem_active = lv[i];
            if (!elem_active) continue;

            if (elem_ts && (elem_ts->is_collection() || elem_ts->kind == TSKind::TSB)) {
                // Composite element - recurse
                ViewData elem_vd;
                elem_vd.meta = elem_ts;
                elem_vd.ops = get_ts_ops(elem_ts);
                // TODO: Get link data for this element if linked
                elem_vd.ops->set_active(elem_vd, elem_active, active, input);
            } else {
                // Scalar element - set directly
                *static_cast<bool*>(elem_active.data()) = active;
            }
        }
    }

    // Manage node-scheduling subscription for collection-level link
    if (vd.link_data) {
        if (vd.uses_link_target) {
            // TSInput: Use ActiveNotifier
            auto* lt = get_link_target(get_collection_link_data(vd));
            if (lt && lt->is_linked && lt->observer_data) {
                auto* observers = static_cast<ObserverList*>(lt->observer_data);
                if (active) {
                    if (lt->active_notifier.owning_input == nullptr) {
                        lt->active_notifier.owning_input = input;
                        observers->add_observer(&lt->active_notifier);
                    }
                } else {
                    if (lt->active_notifier.owning_input != nullptr) {
                        observers->remove_observer(&lt->active_notifier);
                        lt->active_notifier.owning_input = nullptr;
                    }
                }
            } else if (lt && lt->ref_binding_) {
                // REF binding exists but hasn't resolved yet.
                // Set owning_input so REFBindingHelper::rebind() can subscribe later.
                if (active) {
                    if (lt->active_notifier.owning_input == nullptr) {
                        lt->active_notifier.owning_input = input;
                    }
                } else {
                    lt->active_notifier.owning_input = nullptr;
                }
            }
        } else {
            // TSOutput: subscribe input directly
            auto* rl = get_ref_link(get_collection_link_data(vd));
            if (rl && rl->target().is_linked && rl->target().observer_data) {
                auto* observers = static_cast<ObserverList*>(rl->target().observer_data);
                if (active) {
                    observers->add_observer(input);
                } else {
                    observers->remove_observer(input);
                }
            }
        }
    }
}

} // namespace dict_ops

// ============================================================================
// Fixed Window Operations
// ============================================================================

namespace fixed_window_ops {

// Fixed windows use CyclicBufferStorage for values.
// Layout:
//   value_data -> CyclicBufferStorage (ring buffer of elements, capacity = period)
//   time_data  -> tuple[engine_time_t, CyclicBuffer[engine_time_t]]
//                 (container last_modified + parallel per-element timestamps)
//   delta_data -> tuple[element_value, bool has_removed]
//                 (removed value + flag, cleared each tick by TSValue::clear_delta_value)
//   observer_data -> ObserverList (container-level observers)

// Helpers to access the time tuple fields
static engine_time_t& container_time(const ViewData& vd) {
    // time_data is a tuple; first element is engine_time_t (container modified time)
    auto time_view = make_time_view(vd);
    return time_view.as_tuple().at(0).as<engine_time_t>();
}

static value::CyclicBufferStorage* time_buffer(const ViewData& vd) {
    // Second element of the time tuple is CyclicBuffer[engine_time_t]
    auto time_view = make_time_view(vd);
    return static_cast<value::CyclicBufferStorage*>(time_view.as_tuple().at(1).data());
}

// Helper to get the CyclicBufferStorage value schema for push_back operations
static const value::TypeMeta* value_buffer_schema(const ViewData& vd) {
    // The value was constructed with cyclic_buffer(meta->value_type, period)
    // We need the full schema (with element_type set) for CyclicBufferOps
    auto& cache = TSMetaSchemaCache::instance();
    // We can get it by looking up what schema was used to construct the Value
    // But we don't store it directly. We need to reconstruct it.
    // For cyclic_buffer, the schema has: element_type = meta->value_type, fixed_size = capacity
    // Use TypeRegistry to get/create it (cached)
    return value::TypeRegistry::instance()
        .cyclic_buffer(vd.meta->value_type, vd.meta->window.tick.period)
        .build();
}

static const value::TypeMeta* time_buffer_schema(const ViewData& vd) {
    auto& et_meta = TSMetaSchemaCache::instance();
    return value::TypeRegistry::instance()
        .cyclic_buffer(et_meta.engine_time_meta(), vd.meta->window.tick.period)
        .build();
}

engine_time_t last_modified_time(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->last_modified_time(*target);
    if (!vd.time_data) return MIN_DT;
    return container_time(vd);
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    return last_modified_time(vd) != MIN_DT;
}

bool all_valid(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->all_valid(*target);
    // Python: self.valid and self._length >= self._min_size
    if (!valid(vd)) return false;
    auto* buf = static_cast<value::CyclicBufferStorage*>(vd.value_data);
    if (!buf) return false;
    size_t min_sz = vd.meta ? vd.meta->window.tick.min_period : 0;
    return buf->size >= min_sz;
}

bool sampled(const ViewData& vd) {
    return vd.sampled;
}

value::View value(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->value(*target);
    // Return the raw CyclicBufferStorage pointer with the cyclic_buffer schema
    if (!vd.value_data || !vd.meta) return value::View{};
    return value::View(vd.value_data, value_buffer_schema(vd));
}

value::View delta_value(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->delta_value(*target);
    // delta_value is the most recently added element (if added this tick)
    // In delta_data: tuple[removed_element, has_removed_bool]
    // We don't store the newest value in delta — it's accessed from the ring buffer
    return value::View{};
}

bool has_delta(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->has_delta(*target);
    return vd.delta_data != nullptr;
}

void set_value(ViewData& vd, const value::View& src, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("set_value on invalid fixed window ViewData");
    }
    // Copy entire CyclicBuffer
    auto dst_schema = value_buffer_schema(vd);
    value::CyclicBufferOps::copy_assign(vd.value_data, src.data(), dst_schema);
    container_time(vd) = current_time;
    if (vd.observer_data) {
        auto* observers = static_cast<ObserverList*>(vd.observer_data);
        observers->notify_modified(current_time);
    }
}

void apply_delta(ViewData& vd, const value::View& delta, engine_time_t current_time) {
    set_value(vd, delta, current_time);
}

void invalidate(ViewData& vd) {
    if (vd.time_data) {
        container_time(vd) = MIN_DT;
    }
    // Clear the value buffer
    if (vd.value_data) {
        auto* buf = static_cast<value::CyclicBufferStorage*>(vd.value_data);
        buf->size = 0;
        buf->head = 0;
    }
    // Clear the time buffer
    auto* tbuf = time_buffer(vd);
    if (tbuf) {
        tbuf->size = 0;
        tbuf->head = 0;
    }
}

nb::object to_python(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->to_python(*target);

    if (!valid(vd)) return nb::none();
    auto* buf = static_cast<value::CyclicBufferStorage*>(vd.value_data);
    if (!buf || buf->size == 0) return nb::none();

    // Python: if length < min_size return None
    size_t min_sz = vd.meta ? vd.meta->window.tick.min_period : 0;
    if (buf->size < min_sz) return nb::none();

    // Build numpy array in logical order (oldest first)
    // Import numpy and create array
    auto np = nb::module_::import_("numpy");
    auto buf_schema = value_buffer_schema(vd);
    const auto* elem_type = vd.meta->value_type;

    // Build Python list in logical order, then convert to numpy
    nb::list elements;
    for (size_t i = 0; i < buf->size; ++i) {
        const void* elem = value::CyclicBufferOps::get_element_ptr_const(vd.value_data, i, buf_schema);
        if (elem_type && elem_type->ops && elem_type->ops->to_python) {
            elements.append(elem_type->ops->to_python(elem, elem_type));
        } else {
            elements.append(nb::none());
        }
    }
    return np.attr("array")(elements);
}

nb::object delta_to_python(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->delta_to_python(*target);

    if (!valid(vd)) return nb::none();
    auto* buf = static_cast<value::CyclicBufferStorage*>(vd.value_data);
    if (!buf || buf->size == 0) return nb::none();

    // delta_value = most recently added element, if added this tick
    // Check if the most recent element's timestamp matches current_time
    auto* tbuf = time_buffer(vd);
    if (!tbuf || tbuf->size == 0) return nb::none();

    // Get the newest timestamp (last logical element)
    auto tschema = time_buffer_schema(vd);
    const auto* newest_time_ptr = static_cast<const engine_time_t*>(
        value::CyclicBufferOps::get_element_ptr_const(tbuf, tbuf->size - 1, tschema));
    if (!newest_time_ptr || *newest_time_ptr != container_time(vd)) return nb::none();

    // Return the newest value
    auto buf_schema = value_buffer_schema(vd);
    const auto* elem_type = vd.meta->value_type;
    const void* newest = value::CyclicBufferOps::get_element_ptr_const(vd.value_data, buf->size - 1, buf_schema);
    if (elem_type && elem_type->ops && elem_type->ops->to_python) {
        return elem_type->ops->to_python(newest, elem_type);
    }
    return nb::none();
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    // TSW from_python: append a single scalar element to the ring buffer.
    // This matches Python's apply_result() behavior.
    if (src.is_none()) return;
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("from_python on invalid fixed window ViewData");
    }

    auto* buf = static_cast<value::CyclicBufferStorage*>(vd.value_data);
    auto buf_schema = value_buffer_schema(vd);
    const auto* elem_type = vd.meta->value_type;

    // If buffer is full, capture the evicted value in delta_data before overwriting
    if (buf->size == buf->capacity && vd.delta_data) {
        auto delta_view = make_delta_view(vd);
        auto tuple_v = delta_view.as_tuple();

        // Copy the oldest element (at head) to the removed value slot
        const void* oldest = value::CyclicBufferOps::get_element_ptr_const(vd.value_data, 0, buf_schema);
        auto removed_slot = tuple_v.at(0);  // removed value
        if (removed_slot && elem_type && elem_type->ops && elem_type->ops->copy_assign) {
            elem_type->ops->copy_assign(removed_slot.data(), oldest, elem_type);
        }
        // Set has_removed flag
        auto has_removed_v = tuple_v.at(1);
        if (has_removed_v) {
            *static_cast<bool*>(has_removed_v.data()) = true;
        }
    }

    // Convert Python scalar to temp storage, then push_back
    // Allocate temp on stack for small elements
    size_t elem_size = elem_type ? elem_type->size : 0;
    std::vector<std::byte> temp(elem_size);
    if (elem_type && elem_type->ops) {
        if (elem_type->ops->construct) {
            elem_type->ops->construct(temp.data(), elem_type);
        }
        if (elem_type->ops->from_python) {
            elem_type->ops->from_python(temp.data(), src, elem_type);
        }
    }

    // Push to value ring buffer
    value::CyclicBufferOps::push_back(vd.value_data, temp.data(), buf_schema);

    // Push timestamp to time ring buffer
    auto* tbuf = time_buffer(vd);
    if (tbuf) {
        auto tschema = time_buffer_schema(vd);
        value::CyclicBufferOps::push_back(tbuf, &current_time, tschema);
    }

    // Destruct temp
    if (elem_type && elem_type->ops && elem_type->ops->destruct) {
        elem_type->ops->destruct(temp.data(), elem_type);
    }

    // Update container modified time
    container_time(vd) = current_time;

    // Notify observers
    if (vd.observer_data) {
        auto* observers = static_cast<ObserverList*>(vd.observer_data);
        observers->notify_modified(current_time);
    }
}

TSView child_at(const ViewData& vd, size_t index, engine_time_t current_time) {
    return TSView{};
}

TSView child_by_name(const ViewData& vd, const std::string& name, engine_time_t current_time) {
    return TSView{};
}

TSView child_by_key(const ViewData& vd, const value::View& key, engine_time_t current_time) {
    return TSView{};
}

size_t child_count(const ViewData& vd) {
    return 0;
}

value::View observer(const ViewData& vd) {
    return make_observer_view(vd);
}

void notify_observers(ViewData& vd, engine_time_t current_time) {
    if (vd.observer_data) {
        auto* observers = static_cast<ObserverList*>(vd.observer_data);
        observers->notify_modified(current_time);
    }
}

void bind(ViewData& vd, const ViewData& target) {
    if (!vd.link_data) {
        throw std::runtime_error("bind on fixed window without link data");
    }

    if (vd.uses_link_target) {
        auto* lt = get_link_target(vd.link_data);
        if (!lt) {
            throw std::runtime_error("bind on TSW input with invalid link data");
        }
        store_to_link_target(*lt, target);

        // For TSW input, the owner_time_ptr should point to the container time
        // (first element of the time tuple)
        if (vd.time_data) {
            lt->owner_time_ptr = &container_time(vd);
        }

        if (lt->observer_data) {
            auto* obs = static_cast<ObserverList*>(lt->observer_data);
            obs->add_observer(lt);
        }
    } else {
        auto* rl = get_ref_link(vd.link_data);
        if (!rl) {
            throw std::runtime_error("bind on fixed window with invalid link data");
        }

        if (target.meta && target.meta->kind == TSKind::REF) {
            TSView target_view(target, MIN_DT);
            rl->bind_to_ref(target_view, MIN_DT);
        } else {
            store_link_target(*rl, target);
        }
    }
}

void unbind(ViewData& vd) {
    if (!vd.link_data) return;

    if (vd.uses_link_target) {
        auto* lt = get_link_target(vd.link_data);
        if (lt && lt->is_linked) {
            if (lt->observer_data) {
                auto* obs = static_cast<ObserverList*>(lt->observer_data);
                obs->remove_observer(lt);
                if (lt->active_notifier.owning_input != nullptr) {
                    obs->remove_observer(&lt->active_notifier);
                    lt->active_notifier.owning_input = nullptr;
                }
            }
            lt->clear();
        }
    } else {
        auto* rl = get_ref_link(vd.link_data);
        if (rl) {
            rl->unbind();
        }
    }
}

bool is_bound(const ViewData& vd) {
    if (!vd.link_data) return false;

    if (vd.uses_link_target) {
        auto* lt = get_link_target(vd.link_data);
        return lt && lt->is_linked;
    } else {
        auto* rl = get_ref_link(vd.link_data);
        return rl && rl->target().is_linked;
    }
}

bool is_peered(const ViewData& vd) {
    // TSW is a scalar-like type, always peered when bound
    return is_bound(vd);
}

// Window-specific operations using CyclicBufferStorage

const engine_time_t* window_value_times(const ViewData& vd) {
    // Delegate through links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->window_value_times(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->window_value_times(make_view_data_from_link(*rl, vd.path));
    }
    // Return pointer to the time buffer's raw data.
    // Note: The times are in a ring buffer, so they may not be contiguous in order.
    // For the Python wrapper, we'll convert in value_times() method.
    // For now, return nullptr — the wrapper uses value_times_count and iterates.
    return nullptr;
}

size_t window_value_times_count(const ViewData& vd) {
    // Delegate through links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->window_value_times_count(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->window_value_times_count(make_view_data_from_link(*rl, vd.path));
    }
    auto* buf = static_cast<value::CyclicBufferStorage*>(vd.value_data);
    return buf ? buf->size : 0;
}

engine_time_t window_first_modified_time(const ViewData& vd) {
    // Delegate through links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->window_first_modified_time(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->window_first_modified_time(make_view_data_from_link(*rl, vd.path));
    }
    // Oldest timestamp in the time buffer (logical index 0)
    auto* tbuf = time_buffer(vd);
    if (!tbuf || tbuf->size == 0) return MIN_DT;
    auto tschema = time_buffer_schema(vd);
    const auto* first_time = static_cast<const engine_time_t*>(
        value::CyclicBufferOps::get_element_ptr_const(tbuf, 0, tschema));
    return first_time ? *first_time : MIN_DT;
}

bool window_has_removed_value(const ViewData& vd) {
    // Delegate through links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->window_has_removed_value(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->window_has_removed_value(make_view_data_from_link(*rl, vd.path));
    }
    if (!vd.delta_data) return false;
    auto delta_view = make_delta_view(vd);
    auto has_removed_v = delta_view.as_tuple().at(1);
    return has_removed_v && *static_cast<const bool*>(has_removed_v.data());
}

value::View window_removed_value(const ViewData& vd) {
    // Delegate through links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->window_removed_value(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->window_removed_value(make_view_data_from_link(*rl, vd.path));
    }
    // Check has_removed inline (don't call window_has_removed_value which would re-delegate)
    if (!vd.delta_data) return value::View{};
    auto delta_view = make_delta_view(vd);
    auto has_removed_v = delta_view.as_tuple().at(1);
    if (!has_removed_v || !*static_cast<const bool*>(has_removed_v.data())) return value::View{};
    auto removed_v = delta_view.as_tuple().at(0);
    return removed_v;
}

size_t window_removed_value_count(const ViewData& vd) {
    // Delegate through links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->window_removed_value_count(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->window_removed_value_count(make_view_data_from_link(*rl, vd.path));
    }
    // Check has_removed inline (don't call window_has_removed_value which would re-delegate)
    if (!vd.delta_data) return 0;
    auto delta_view = make_delta_view(vd);
    auto has_removed_v = delta_view.as_tuple().at(1);
    return (has_removed_v && *static_cast<const bool*>(has_removed_v.data())) ? 1 : 0;
}

size_t window_size(const ViewData& vd) {
    return vd.meta ? vd.meta->window.tick.period : 0;
}

size_t window_min_size(const ViewData& vd) {
    return vd.meta ? vd.meta->window.tick.min_period : 0;
}

size_t window_length(const ViewData& vd) {
    // Delegate through links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->window_length(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->window_length(make_view_data_from_link(*rl, vd.path));
    }
    auto* buf = static_cast<value::CyclicBufferStorage*>(vd.value_data);
    return buf ? buf->size : 0;
}

void set_active(ViewData& vd, value::View active_view, bool active, TSInput* input) {
    if (!active_view) return;

    *static_cast<bool*>(active_view.data()) = active;

    if (vd.link_data) {
        if (vd.uses_link_target) {
            auto* lt = get_link_target(vd.link_data);
            if (lt && lt->is_linked && lt->observer_data) {
                auto* observers = static_cast<ObserverList*>(lt->observer_data);
                if (active) {
                    if (lt->active_notifier.owning_input == nullptr) {
                        lt->active_notifier.owning_input = input;
                        observers->add_observer(&lt->active_notifier);
                    }
                } else {
                    if (lt->active_notifier.owning_input != nullptr) {
                        observers->remove_observer(&lt->active_notifier);
                        lt->active_notifier.owning_input = nullptr;
                    }
                }
            }
        } else {
            auto* rl = get_ref_link(vd.link_data);
            if (rl && rl->target().is_linked && rl->target().observer_data) {
                auto* observers = static_cast<ObserverList*>(rl->target().observer_data);
                if (active) {
                    observers->add_observer(input);
                } else {
                    observers->remove_observer(input);
                }
            }
        }
    }
}

} // namespace fixed_window_ops

// ============================================================================
// Time Window Operations
// ============================================================================

namespace time_window_ops {

// Duration-based windows use QueueStorage for values and timestamps.
// Layout:
//   value_data  -> QueueStorage (unbounded queue of elements)
//   time_data   -> tuple[engine_time_t container_time,
//                        Queue[engine_time_t] timestamps,
//                        engine_time_t start_time,
//                        bool ready]
//   delta_data  -> tuple[bool has_removed,
//                        Queue[element_type] removed_values]
//   observer_data -> ObserverList (container-level observers)

// ========== Helpers to access time tuple fields ==========

static engine_time_t& container_time(const ViewData& vd) {
    auto time_view = make_time_view(vd);
    return time_view.as_tuple().at(0).as<engine_time_t>();
}

static value::QueueStorage* time_queue(const ViewData& vd) {
    auto time_view = make_time_view(vd);
    return static_cast<value::QueueStorage*>(time_view.as_tuple().at(1).data());
}

static engine_time_t& start_time_ref(const ViewData& vd) {
    auto time_view = make_time_view(vd);
    return time_view.as_tuple().at(2).as<engine_time_t>();
}

static bool& ready_flag(const ViewData& vd) {
    auto time_view = make_time_view(vd);
    return time_view.as_tuple().at(3).as<bool>();
}

// ========== Schema helpers (cached via TypeRegistry) ==========

static const value::TypeMeta* time_queue_schema(const ViewData& vd) {
    return value::TypeRegistry::instance()
        .queue(TSMetaSchemaCache::instance().engine_time_meta())
        .build();
}

static const value::TypeMeta* value_queue_schema(const ViewData& vd) {
    return value::TypeRegistry::instance()
        .queue(vd.meta->value_type)
        .build();
}

static const value::TypeMeta* removed_queue_schema(const ViewData& vd) {
    return value::TypeRegistry::instance()
        .queue(vd.meta->value_type)
        .build();
}

// ========== Delta helpers ==========

static bool& delta_has_removed(const ViewData& vd) {
    auto delta_view = make_delta_view(vd);
    return delta_view.as_tuple().at(0).as<bool>();
}

static value::QueueStorage* delta_removed_queue(const ViewData& vd) {
    auto delta_view = make_delta_view(vd);
    return static_cast<value::QueueStorage*>(delta_view.as_tuple().at(1).data());
}

// ========== Roll logic ==========
// Evicts elements whose timestamp < current_time - window_duration.
// Evicted values are stored in the delta removed queue.

static void roll(const ViewData& vd, engine_time_t current_time) {
    auto* val_q = static_cast<value::QueueStorage*>(vd.value_data);
    auto* time_q = time_queue(vd);
    if (!val_q || !time_q || time_q->size() == 0) return;

    engine_time_t cutoff = current_time - vd.meta->window.duration.time_range;

    auto tq_schema = time_queue_schema(vd);
    auto vq_schema = value_queue_schema(vd);

    bool any_removed = false;
    value::QueueStorage* removed_q = nullptr;
    const value::TypeMeta* rq_schema = nullptr;

    if (vd.delta_data) {
        removed_q = delta_removed_queue(vd);
        rq_schema = removed_queue_schema(vd);
    }

    // Pop elements from front while oldest timestamp < cutoff
    while (time_q->size() > 0) {
        const auto* oldest_time = static_cast<const engine_time_t*>(
            value::QueueOps::get_element_ptr_const(time_q, 0, tq_schema));
        if (!oldest_time || *oldest_time >= cutoff) break;

        // Capture removed value in delta queue
        if (removed_q && rq_schema) {
            const void* oldest_val = value::QueueOps::get_element_ptr_const(val_q, 0, vq_schema);
            value::QueueOps::push_back(removed_q, oldest_val, rq_schema);
            any_removed = true;
        }

        // Pop from both queues
        value::QueueOps::pop_front(val_q, vq_schema);
        value::QueueOps::pop_front(time_q, tq_schema);
    }

    if (any_removed && vd.delta_data) {
        delta_has_removed(vd) = true;
    }
}

// ========== Core ts_ops functions ==========

engine_time_t last_modified_time(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->last_modified_time(*target);
    if (!vd.time_data) return MIN_DT;
    return container_time(vd);
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    return last_modified_time(vd) != MIN_DT;
}

bool all_valid(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->all_valid(*target);
    // Python: self.ready (eval_time - start_time >= min_size)
    if (!valid(vd)) return false;
    return ready_flag(vd);
}

bool sampled(const ViewData& vd) {
    return vd.sampled;
}

value::View value(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->value(*target);
    if (!vd.value_data || !vd.meta) return value::View{};
    return value::View(vd.value_data, value_queue_schema(vd));
}

value::View delta_value(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->delta_value(*target);
    return value::View{};
}

bool has_delta(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->has_delta(*target);
    return vd.delta_data != nullptr;
}

void set_value(ViewData& vd, const value::View& src, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("set_value on invalid time window ViewData");
    }
    auto vq_schema = value_queue_schema(vd);
    value::QueueOps::copy_assign(vd.value_data, src.data(), vq_schema);
    container_time(vd) = current_time;
    if (vd.observer_data) {
        auto* observers = static_cast<ObserverList*>(vd.observer_data);
        observers->notify_modified(current_time);
    }
}

void apply_delta(ViewData& vd, const value::View& delta, engine_time_t current_time) {
    set_value(vd, delta, current_time);
}

void invalidate(ViewData& vd) {
    if (vd.time_data) {
        container_time(vd) = MIN_DT;
    }
    // Clear the value queue
    if (vd.value_data) {
        auto* val_q = static_cast<value::QueueStorage*>(vd.value_data);
        auto vq_schema = value_queue_schema(vd);
        value::QueueOps::clear(val_q, vq_schema);
    }
    // Clear the time queue
    auto* time_q = time_queue(vd);
    if (time_q) {
        auto tq_schema = time_queue_schema(vd);
        value::QueueOps::clear(time_q, tq_schema);
    }
}

nb::object to_python(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->to_python(*target);

    if (!valid(vd)) return nb::none();

    // Python: if not ready, return None
    if (!ready_flag(vd)) return nb::none();

    auto* val_q = static_cast<value::QueueStorage*>(vd.value_data);
    if (!val_q || val_q->size() == 0) return nb::none();

    // Build numpy array in logical order (oldest first)
    auto np = nb::module_::import_("numpy");
    auto vq_schema = value_queue_schema(vd);
    const auto* elem_type = vd.meta->value_type;

    nb::list elements;
    for (size_t i = 0; i < val_q->size(); ++i) {
        const void* elem = value::QueueOps::get_element_ptr_const(val_q, i, vq_schema);
        if (elem_type && elem_type->ops && elem_type->ops->to_python) {
            elements.append(elem_type->ops->to_python(elem, elem_type));
        } else {
            elements.append(nb::none());
        }
    }
    return np.attr("array")(elements);
}

nb::object delta_to_python(const ViewData& vd) {
    if (auto target = resolve_delegation_target(vd)) return target->ops->delta_to_python(*target);

    if (!valid(vd)) return nb::none();

    // Python: if not ready, return None
    if (!ready_flag(vd)) return nb::none();

    auto* val_q = static_cast<value::QueueStorage*>(vd.value_data);
    auto* time_q = time_queue(vd);
    if (!val_q || val_q->size() == 0 || !time_q || time_q->size() == 0) return nb::none();

    // Python: if times[-1] == evaluation_time, return value[-1]; else None
    auto tq_schema = time_queue_schema(vd);
    const auto* newest_time = static_cast<const engine_time_t*>(
        value::QueueOps::get_element_ptr_const(time_q, time_q->size() - 1, tq_schema));
    if (!newest_time || *newest_time != container_time(vd)) return nb::none();

    // Return the newest value
    auto vq_schema = value_queue_schema(vd);
    const auto* elem_type = vd.meta->value_type;
    const void* newest = value::QueueOps::get_element_ptr_const(val_q, val_q->size() - 1, vq_schema);
    if (elem_type && elem_type->ops && elem_type->ops->to_python) {
        return elem_type->ops->to_python(newest, elem_type);
    }
    return nb::none();
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    // Matches Python PythonTimeSeriesTimeWindowOutput.apply_result()
    if (src.is_none()) return;
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("from_python on invalid time window ViewData");
    }

    auto* val_q = static_cast<value::QueueStorage*>(vd.value_data);
    auto vq_schema = value_queue_schema(vd);
    const auto* elem_type = vd.meta->value_type;

    // On first write, set start_time
    auto& st = start_time_ref(vd);
    if (st == MIN_DT) {
        st = current_time;
    }

    // Update ready flag (monotonic — once true, stays true)
    auto& ready = ready_flag(vd);
    if (!ready) {
        ready = (current_time - st) >= vd.meta->window.duration.min_time_range;
    }

    // Convert Python scalar to temp storage
    size_t elem_size = elem_type ? elem_type->size : 0;
    std::vector<std::byte> temp(elem_size);
    if (elem_type && elem_type->ops) {
        if (elem_type->ops->construct) {
            elem_type->ops->construct(temp.data(), elem_type);
        }
        if (elem_type->ops->from_python) {
            elem_type->ops->from_python(temp.data(), src, elem_type);
        }
    }

    // Append to value queue
    value::QueueOps::push_back(val_q, temp.data(), vq_schema);

    // Append timestamp to time queue
    auto* time_q = time_queue(vd);
    if (time_q) {
        auto tq_schema = time_queue_schema(vd);
        value::QueueOps::push_back(time_q, &current_time, tq_schema);
    }

    // Destruct temp
    if (elem_type && elem_type->ops && elem_type->ops->destruct) {
        elem_type->ops->destruct(temp.data(), elem_type);
    }

    // Run roll logic to evict old elements
    roll(vd, current_time);

    // Update container modified time
    container_time(vd) = current_time;

    // Notify observers
    if (vd.observer_data) {
        auto* observers = static_cast<ObserverList*>(vd.observer_data);
        observers->notify_modified(current_time);
    }
}

TSView child_at(const ViewData& vd, size_t index, engine_time_t current_time) {
    return TSView{};
}

TSView child_by_name(const ViewData& vd, const std::string& name, engine_time_t current_time) {
    return TSView{};
}

TSView child_by_key(const ViewData& vd, const value::View& key, engine_time_t current_time) {
    return TSView{};
}

size_t child_count(const ViewData& vd) {
    return 0;
}

value::View observer(const ViewData& vd) {
    return make_observer_view(vd);
}

void notify_observers(ViewData& vd, engine_time_t current_time) {
    if (vd.observer_data) {
        auto* observers = static_cast<ObserverList*>(vd.observer_data);
        observers->notify_modified(current_time);
    }
}

void bind(ViewData& vd, const ViewData& target) {
    if (!vd.link_data) {
        throw std::runtime_error("bind on time window without link data");
    }

    if (vd.uses_link_target) {
        auto* lt = get_link_target(vd.link_data);
        if (!lt) {
            throw std::runtime_error("bind on TSW input with invalid link data");
        }
        store_to_link_target(*lt, target);

        // For TSW input, the owner_time_ptr points to the container time
        if (vd.time_data) {
            lt->owner_time_ptr = &container_time(vd);
        }

        if (lt->observer_data) {
            auto* obs = static_cast<ObserverList*>(lt->observer_data);
            obs->add_observer(lt);
        }
    } else {
        auto* rl = get_ref_link(vd.link_data);
        if (!rl) {
            throw std::runtime_error("bind on time window with invalid link data");
        }

        if (target.meta && target.meta->kind == TSKind::REF) {
            TSView target_view(target, MIN_DT);
            rl->bind_to_ref(target_view, MIN_DT);
        } else {
            store_link_target(*rl, target);
        }
    }
}

void unbind(ViewData& vd) {
    if (!vd.link_data) return;

    if (vd.uses_link_target) {
        auto* lt = get_link_target(vd.link_data);
        if (lt && lt->is_linked) {
            if (lt->observer_data) {
                auto* obs = static_cast<ObserverList*>(lt->observer_data);
                obs->remove_observer(lt);
                if (lt->active_notifier.owning_input != nullptr) {
                    obs->remove_observer(&lt->active_notifier);
                    lt->active_notifier.owning_input = nullptr;
                }
            }
            lt->clear();
        }
    } else {
        auto* rl = get_ref_link(vd.link_data);
        if (rl) {
            rl->unbind();
        }
    }
}

bool is_bound(const ViewData& vd) {
    if (!vd.link_data) return false;

    if (vd.uses_link_target) {
        auto* lt = get_link_target(vd.link_data);
        return lt && lt->is_linked;
    } else {
        auto* rl = get_ref_link(vd.link_data);
        return rl && rl->target().is_linked;
    }
}

bool is_peered(const ViewData& vd) {
    // TSW (time-based) is a scalar-like type, always peered when bound
    return is_bound(vd);
}

// ========== Window-specific operations using QueueStorage ==========

// Thread-local cache for value_times (QueueStorage isn't contiguous)
static thread_local std::vector<engine_time_t> cached_value_times_;

const engine_time_t* window_value_times(const ViewData& vd) {
    // Delegate through links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->window_value_times(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->window_value_times(make_view_data_from_link(*rl, vd.path));
    }

    auto* time_q = time_queue(vd);
    if (!time_q || time_q->size() == 0) return nullptr;

    // Build contiguous array from queue slots
    auto tq_schema = time_queue_schema(vd);
    cached_value_times_.clear();
    cached_value_times_.reserve(time_q->size());
    for (size_t i = 0; i < time_q->size(); ++i) {
        const auto* t = static_cast<const engine_time_t*>(
            value::QueueOps::get_element_ptr_const(time_q, i, tq_schema));
        cached_value_times_.push_back(*t);
    }
    return cached_value_times_.data();
}

size_t window_value_times_count(const ViewData& vd) {
    // Delegate through links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->window_value_times_count(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->window_value_times_count(make_view_data_from_link(*rl, vd.path));
    }

    auto* time_q = time_queue(vd);
    return time_q ? time_q->size() : 0;
}

engine_time_t window_first_modified_time(const ViewData& vd) {
    // Delegate through links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->window_first_modified_time(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->window_first_modified_time(make_view_data_from_link(*rl, vd.path));
    }

    // Oldest timestamp in the time queue (logical index 0)
    auto* time_q = time_queue(vd);
    if (!time_q || time_q->size() == 0) return MIN_DT;
    auto tq_schema = time_queue_schema(vd);
    const auto* first_time = static_cast<const engine_time_t*>(
        value::QueueOps::get_element_ptr_const(time_q, 0, tq_schema));
    return first_time ? *first_time : MIN_DT;
}

bool window_has_removed_value(const ViewData& vd) {
    // Delegate through links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->window_has_removed_value(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->window_has_removed_value(make_view_data_from_link(*rl, vd.path));
    }
    if (!vd.delta_data) return false;
    return delta_has_removed(vd);
}

value::View window_removed_value(const ViewData& vd) {
    // Delegate through links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->window_removed_value(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->window_removed_value(make_view_data_from_link(*rl, vd.path));
    }
    // Check has_removed inline (don't call window_has_removed_value which would re-delegate)
    if (!vd.delta_data) return value::View{};
    if (!delta_has_removed(vd)) return value::View{};
    // Return a view of the removed values QueueStorage
    auto* removed_q = delta_removed_queue(vd);
    if (!removed_q || removed_q->size() == 0) return value::View{};
    return value::View(removed_q, removed_queue_schema(vd));
}

size_t window_removed_value_count(const ViewData& vd) {
    // Delegate through links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->window_removed_value_count(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->window_removed_value_count(make_view_data_from_link(*rl, vd.path));
    }
    // Check has_removed inline
    if (!vd.delta_data || !delta_has_removed(vd)) return 0;
    auto* removed_q = delta_removed_queue(vd);
    return removed_q ? removed_q->size() : 0;
}

size_t window_size(const ViewData& vd) {
    return vd.meta ? static_cast<size_t>(vd.meta->window.duration.time_range.count()) : 0;
}

size_t window_min_size(const ViewData& vd) {
    return vd.meta ? static_cast<size_t>(vd.meta->window.duration.min_time_range.count()) : 0;
}

size_t window_length(const ViewData& vd) {
    // Delegate through links for inputs
    if (auto* lt = get_active_link_target(vd)) {
        return lt->ops->window_length(make_view_data_from_link_target(*lt, vd.path));
    }
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->window_length(make_view_data_from_link(*rl, vd.path));
    }
    auto* val_q = static_cast<value::QueueStorage*>(vd.value_data);
    return val_q ? val_q->size() : 0;
}

void set_active(ViewData& vd, value::View active_view, bool active, TSInput* input) {
    if (!active_view) return;

    *static_cast<bool*>(active_view.data()) = active;

    // Manage node-scheduling subscription
    if (vd.link_data) {
        if (vd.uses_link_target) {
            auto* lt = get_link_target(vd.link_data);
            if (lt && lt->is_linked && lt->observer_data) {
                auto* observers = static_cast<ObserverList*>(lt->observer_data);
                if (active) {
                    if (lt->active_notifier.owning_input == nullptr) {
                        lt->active_notifier.owning_input = input;
                        observers->add_observer(&lt->active_notifier);
                    }
                } else {
                    if (lt->active_notifier.owning_input != nullptr) {
                        observers->remove_observer(&lt->active_notifier);
                        lt->active_notifier.owning_input = nullptr;
                    }
                }
            } else if (lt && lt->ref_binding_) {
                lt->active_notifier.owning_input = active ? input : nullptr;
            }
        } else {
            auto* rl = get_ref_link(vd.link_data);
            if (rl && rl->target().is_linked && rl->target().observer_data) {
                auto* observers = static_cast<ObserverList*>(rl->target().observer_data);
                if (active) {
                    observers->add_observer(input);
                } else {
                    observers->remove_observer(input);
                }
            }
        }
    }
}

} // namespace time_window_ops

// ============================================================================
// Static ts_ops Tables
// ============================================================================

// Optional operation group macros for specialized TSKind slots.
// Each group is either NO_* (all nullptr) or TS_* (wired to namespace functions).

#define TS_NO_WINDOW_OPS \
    .window_value_times = nullptr, \
    .window_value_times_count = nullptr, \
    .window_first_modified_time = nullptr, \
    .window_has_removed_value = nullptr, \
    .window_removed_value = nullptr, \
    .window_removed_value_count = nullptr, \
    .window_size = nullptr, \
    .window_min_size = nullptr, \
    .window_length = nullptr

#define TS_WINDOW_OPS(ns) \
    .window_value_times = ns::window_value_times, \
    .window_value_times_count = ns::window_value_times_count, \
    .window_first_modified_time = ns::window_first_modified_time, \
    .window_has_removed_value = ns::window_has_removed_value, \
    .window_removed_value = ns::window_removed_value, \
    .window_removed_value_count = ns::window_removed_value_count, \
    .window_size = ns::window_size, \
    .window_min_size = ns::window_min_size, \
    .window_length = ns::window_length

#define TS_NO_SET_OPS \
    .set_add = nullptr, \
    .set_remove = nullptr, \
    .set_clear = nullptr

#define TS_SET_OPS(ns) \
    .set_add = ns::set_add, \
    .set_remove = ns::set_remove, \
    .set_clear = ns::set_clear

#define TS_NO_DICT_OPS \
    .dict_remove = nullptr, \
    .dict_create = nullptr, \
    .dict_set = nullptr

#define TS_DICT_OPS(ns) \
    .dict_remove = ns::dict_remove, \
    .dict_create = ns::dict_create, \
    .dict_set = ns::dict_set

// Single unified macro: common fields from ns, optional groups passed as parameters
#define MAKE_TS_OPS(ns, WINDOW, SET, DICT) ts_ops { \
    .ts_meta = [](const ViewData& vd) { return vd.meta; }, \
    .last_modified_time = ns::last_modified_time, \
    .modified = ns::modified, \
    .valid = ns::valid, \
    .all_valid = ns::all_valid, \
    .sampled = ns::sampled, \
    .value = ns::value, \
    .delta_value = ns::delta_value, \
    .has_delta = ns::has_delta, \
    .set_value = ns::set_value, \
    .apply_delta = ns::apply_delta, \
    .invalidate = ns::invalidate, \
    .to_python = ns::to_python, \
    .delta_to_python = ns::delta_to_python, \
    .from_python = ns::from_python, \
    .child_at = ns::child_at, \
    .child_by_name = ns::child_by_name, \
    .child_by_key = ns::child_by_key, \
    .child_count = ns::child_count, \
    .observer = ns::observer, \
    .notify_observers = ns::notify_observers, \
    .bind = ns::bind, \
    .unbind = ns::unbind, \
    .is_bound = ns::is_bound, \
    .is_peered = ns::is_peered, \
    .set_active = ns::set_active, \
    WINDOW, \
    SET, \
    DICT \
}

static const ts_ops scalar_ts_ops       = MAKE_TS_OPS(scalar_ops,       TS_NO_WINDOW_OPS, TS_NO_SET_OPS,          TS_NO_DICT_OPS);
static const ts_ops bundle_ts_ops       = MAKE_TS_OPS(bundle_ops,       TS_NO_WINDOW_OPS, TS_NO_SET_OPS,          TS_NO_DICT_OPS);
static const ts_ops list_ts_ops         = MAKE_TS_OPS(list_ops,         TS_NO_WINDOW_OPS, TS_NO_SET_OPS,          TS_NO_DICT_OPS);
static const ts_ops set_ts_ops          = MAKE_TS_OPS(set_ops,          TS_NO_WINDOW_OPS, TS_SET_OPS(set_ops),    TS_NO_DICT_OPS);
static const ts_ops dict_ts_ops         = MAKE_TS_OPS(dict_ops,         TS_NO_WINDOW_OPS, TS_NO_SET_OPS,          TS_DICT_OPS(dict_ops));
static const ts_ops fixed_window_ts_ops = MAKE_TS_OPS(fixed_window_ops, TS_WINDOW_OPS(fixed_window_ops), TS_NO_SET_OPS, TS_NO_DICT_OPS);
static const ts_ops time_window_ts_ops  = MAKE_TS_OPS(time_window_ops,  TS_WINDOW_OPS(time_window_ops),  TS_NO_SET_OPS, TS_NO_DICT_OPS);

#undef MAKE_TS_OPS
#undef TS_NO_WINDOW_OPS
#undef TS_WINDOW_OPS
#undef TS_NO_SET_OPS
#undef TS_SET_OPS
#undef TS_NO_DICT_OPS
#undef TS_DICT_OPS

// ============================================================================
// get_ts_ops Implementation
// ============================================================================

const ts_ops* get_ts_ops(TSKind kind) {
    switch (kind) {
        case TSKind::TSValue:
        case TSKind::SIGNAL:
        case TSKind::REF:
            return &scalar_ts_ops;

        case TSKind::TSW:
            // For TSW without TSMeta, default to fixed window
            // Use get_ts_ops(const TSMeta*) for proper selection
            return &fixed_window_ts_ops;

        case TSKind::TSB:
            return &bundle_ts_ops;

        case TSKind::TSL:
            return &list_ts_ops;

        case TSKind::TSS:
            return &set_ts_ops;

        case TSKind::TSD:
            return &dict_ts_ops;
    }

    // Should never reach here
    return &scalar_ts_ops;
}

const ts_ops* get_ts_ops(const TSMeta* meta) {
    if (!meta) return &scalar_ts_ops;

    // For TSW, select based on is_duration_based
    if (meta->kind == TSKind::TSW) {
        if (meta->is_duration_based) {
            return &time_window_ts_ops;
        } else {
            return &fixed_window_ts_ops;
        }
    }

    // For all other kinds, delegate to the kind-based version
    return get_ts_ops(meta->kind);
}

// Forward declarations for cache clearing from other translation units
void clear_constants_cache();
void clear_global_state_cache();
void clear_switch_node_builder_cache();

void clear_thread_local_caches() {
    set_ops::clear_caches();
    clear_constants_cache();
    clear_global_state_cache();
    clear_switch_node_builder_cache();
}

} // namespace hgraph
