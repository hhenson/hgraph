#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/observer_list.h>

namespace hgraph {

// ============================================================================
// REFLink Implementation
// ============================================================================

REFLink::REFLink(TSView ref_source, engine_time_t current_time)
    : ref_source_bound_(false) {
    bind_to_ref(std::move(ref_source), current_time);
}

REFLink::REFLink(REFLink&& other) noexcept
    : target_(std::move(other.target_))
    , ref_source_view_data_(std::move(other.ref_source_view_data_))
    , ref_source_bound_(other.ref_source_bound_) {
    other.ref_source_bound_ = false;
}

REFLink& REFLink::operator=(REFLink&& other) noexcept {
    if (this != &other) {
        // Unbind from current sources first
        unbind();

        target_ = std::move(other.target_);
        ref_source_view_data_ = std::move(other.ref_source_view_data_);
        ref_source_bound_ = other.ref_source_bound_;
        other.ref_source_bound_ = false;
    }
    return *this;
}

REFLink::~REFLink() {
    unbind();
}

void REFLink::bind_to_ref(TSView ref_source, engine_time_t current_time) {
    // Unbind from any previous source
    unbind();

    // Store the REF source's view data
    ref_source_view_data_ = ref_source.view_data();
    ref_source_bound_ = true;

    // Subscribe to the REF source for change notifications
    value::View obs_view = ref_source.observer();
    if (obs_view) {
        auto* obs_list = static_cast<ObserverList*>(obs_view.data());
        obs_list->add_observer(this);
    }

    // Initial bind to current target
    rebind_target(current_time);
}

void REFLink::unbind() {
    if (!ref_source_bound_) return;

    // Unsubscribe from REF source
    if (ref_source_view_data_.observer_data) {
        auto* obs_list = static_cast<ObserverList*>(ref_source_view_data_.observer_data);
        obs_list->remove_observer(this);
    }

    // Unsubscribe from current target
    if (target_.is_linked && target_.observer_data) {
        auto* target_obs = static_cast<ObserverList*>(target_.observer_data);
        target_obs->remove_observer(this);
    }

    // Clear state
    target_.clear();
    ref_source_view_data_ = ViewData{};
    ref_source_bound_ = false;
}

TSView REFLink::target_view(engine_time_t current_time) const {
    if (!target_.valid()) {
        return TSView{};  // Invalid view
    }

    // Construct ViewData from LinkTarget
    ViewData vd;
    vd.value_data = target_.value_data;
    vd.time_data = target_.time_data;
    vd.observer_data = target_.observer_data;
    vd.delta_data = target_.delta_data;
    vd.link_data = target_.link_data;
    vd.ops = target_.ops;
    vd.meta = target_.meta;
    // Path is not set - caller can set if needed

    return TSView(std::move(vd), current_time);
}

bool REFLink::modified(engine_time_t current_time) const {
    // Check if REF source changed (sampled semantics)
    // Use the REF source's last_modified_time instead of storing separately
    TSView ref_view(ref_source_view_data_, current_time);
    if (ref_view && ref_view.last_modified_time() >= current_time) {
        return true;  // Reference changed at current time
    }

    // Check if target changed
    TSView tv = target_view(current_time);
    if (!tv) {
        return false;
    }
    return tv.modified();
}

bool REFLink::valid() const {
    TSView tv = target_view(MIN_ST);  // Time doesn't matter for validity
    return tv.valid();
}

void REFLink::notify(engine_time_t et) {
    // Called when REF source changes
    // Need to rebind to the new target
    rebind_target(et);
}

void REFLink::rebind_target(engine_time_t current_time) {
    // Unsubscribe from old target
    if (target_.is_linked && target_.observer_data) {
        auto* target_obs = static_cast<ObserverList*>(target_.observer_data);
        target_obs->remove_observer(this);
    }

    // Clear old target
    target_.clear();

    // Get the TSReference from the REF source
    TSView ref_view(ref_source_view_data_, current_time);
    if (!ref_view || !ref_view.valid()) {
        return;  // No valid reference yet
    }

    // Read the TSReference value
    value::View ref_value = ref_view.value();
    if (!ref_value) {
        return;
    }

    // Get the TSReference
    const TSReference* ts_ref = static_cast<const TSReference*>(ref_value.data());
    if (!ts_ref || ts_ref->is_empty()) {
        return;  // Empty reference
    }

    if (ts_ref->is_peered()) {
        // Resolve the path to get the target
        TSView resolved = ts_ref->resolve(current_time);
        if (!resolved) {
            return;  // Failed to resolve
        }

        // Store target information in LinkTarget
        const ViewData& vd = resolved.view_data();
        target_.is_linked = true;
        target_.value_data = vd.value_data;
        target_.time_data = vd.time_data;
        target_.observer_data = vd.observer_data;
        target_.delta_data = vd.delta_data;
        target_.link_data = vd.link_data;
        target_.ops = vd.ops;
        target_.meta = vd.meta;

        // Subscribe to new target
        if (target_.observer_data) {
            auto* target_obs = static_cast<ObserverList*>(target_.observer_data);
            target_obs->add_observer(this);
        }
    }
    // NON_PEERED references would need different handling
    // TODO: Handle NON_PEERED references
}

engine_time_t REFLink::last_rebind_time() const noexcept {
    // Use the REF source's last_modified_time instead of storing separately
    // This saves storage and keeps the semantics identical
    if (!ref_source_bound_ || !ref_source_view_data_.valid()) {
        return MIN_ST;
    }
    TSView ref_view(ref_source_view_data_, MIN_ST);
    return ref_view.last_modified_time();
}

} // namespace hgraph
