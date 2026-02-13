#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/api/python/py_time_series.h>

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
    , ref_source_bound_(other.ref_source_bound_)
    , owner_time_ptr_(other.owner_time_ptr_)
    , parent_link_(other.parent_link_)
    , last_notify_time_(other.last_notify_time_) {
    other.ref_source_bound_ = false;
    other.owner_time_ptr_ = nullptr;
    other.parent_link_ = nullptr;
    other.last_notify_time_ = MIN_DT;
    // Move active_notifier state
    active_notifier_.owning_input = other.active_notifier_.owning_input;
    other.active_notifier_.owning_input = nullptr;
}

REFLink& REFLink::operator=(REFLink&& other) noexcept {
    if (this != &other) {
        // Unbind from current sources first
        unbind();

        target_ = std::move(other.target_);
        ref_source_view_data_ = std::move(other.ref_source_view_data_);
        ref_source_bound_ = other.ref_source_bound_;
        owner_time_ptr_ = other.owner_time_ptr_;
        parent_link_ = other.parent_link_;
        last_notify_time_ = other.last_notify_time_;
        active_notifier_.owning_input = other.active_notifier_.owning_input;

        other.ref_source_bound_ = false;
        other.owner_time_ptr_ = nullptr;
        other.parent_link_ = nullptr;
        other.last_notify_time_ = MIN_DT;
        other.active_notifier_.owning_input = nullptr;
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
    // Handle REF source unsubscription if bound
    if (ref_source_bound_) {
        // Unsubscribe from REF source
        if (ref_source_view_data_.observer_data) {
            auto* obs_list = static_cast<ObserverList*>(ref_source_view_data_.observer_data);
            obs_list->remove_observer(this);
        }
        ref_source_view_data_ = ViewData{};
        ref_source_bound_ = false;
    }

    // Handle target cleanup - unsubscribe both chains
    if (target_.is_linked && target_.observer_data) {
        auto* target_obs = static_cast<ObserverList*>(target_.observer_data);
        // Unsubscribe time-accounting (this REFLink)
        target_obs->remove_observer(this);
        // Unsubscribe node-scheduling if active
        if (active_notifier_.owning_input != nullptr) {
            target_obs->remove_observer(&active_notifier_);
            active_notifier_.owning_input = nullptr;
        }
    }

    // Clear target state
    target_.clear();
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
    TSView tv = target_view(MIN_DT);  // Time doesn't matter for validity
    return tv.valid();
}

const TSMeta* REFLink::dereferenced_meta() const noexcept {
    if (!ref_source_bound_) {
        return nullptr;
    }
    // The REF source's meta is a REF type, its element_ts is the dereferenced type
    const TSMeta* ref_meta = ref_source_view_data_.meta;
    if (ref_meta && ref_meta->kind == TSKind::REF) {
        return ref_meta->element_ts;
    }
    return nullptr;
}

void REFLink::notify(engine_time_t et) {
    // Time-accounting: always propagate up
    if (last_notify_time_ != et) {
        last_notify_time_ = et;
        if (owner_time_ptr_) *owner_time_ptr_ = et;
        if (parent_link_) parent_link_->notify(et);
    }

    // REF-specific: if REF source changed, rebind target
    if (ref_source_bound_) {
        rebind_target(et);
    }
}

void REFLink::rebind_target(engine_time_t current_time) {
    // Unsubscribe from old target (both chains)
    if (target_.is_linked && target_.observer_data) {
        auto* target_obs = static_cast<ObserverList*>(target_.observer_data);
        // Unsubscribe time-accounting
        target_obs->remove_observer(this);
        // Unsubscribe node-scheduling if active
        if (active_notifier_.owning_input != nullptr) {
            target_obs->remove_observer(&active_notifier_);
        }
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

    const void* data_ptr = ref_value.data();
    if (!data_ptr) {
        return;
    }

    // Cast to TSReference - legacy TimeSeriesReference is NOT supported
    const TSReference* ts_ref = static_cast<const TSReference*>(data_ptr);
    if (!ts_ref || ts_ref->is_empty()) {
        return;  // Empty reference
    }

    auto bind_to_view_data = [&](const ViewData& vd) {
        target_.is_linked = true;
        target_.value_data = vd.value_data;
        target_.time_data = vd.time_data;
        target_.observer_data = vd.observer_data;
        target_.delta_data = vd.delta_data;
        target_.link_data = vd.link_data;
        target_.ops = vd.ops;
        target_.meta = vd.meta;

        // Subscribe to new target - time-accounting chain (this REFLink)
        if (target_.observer_data) {
            auto* target_obs = static_cast<ObserverList*>(target_.observer_data);
            target_obs->add_observer(this);
            // Re-subscribe node-scheduling if active
            if (active_notifier_.owning_input != nullptr) {
                target_obs->add_observer(&active_notifier_);
            }
        }
    };

    if (ts_ref->is_peered()) {
        // Resolve the path to get the target
        TSView resolved = ts_ref->resolve(current_time);
        if (!resolved) {
            return;  // Failed to resolve
        }
        bind_to_view_data(resolved.view_data());
    } else if (ts_ref->is_python_bound()) {
        // PYTHON_BOUND: extract the output from the Python TimeSeriesReference
        // and bind to its ViewData if it's a C++ wrapper
        try {
            nb::object py_ref = ts_ref->python_object();
            if (nb::cast<bool>(py_ref.attr("has_output"))) {
                nb::object output_obj = py_ref.attr("output");
                if (nb::isinstance<PyTimeSeriesOutput>(output_obj)) {
                    auto& py_output = nb::cast<PyTimeSeriesOutput&>(output_obj);
                    const ViewData& vd = py_output.output_view().ts_view().view_data();
                    if (vd.value_data) {
                        bind_to_view_data(vd);
                    }
                }
            }
        } catch (...) {
            // Failed to extract - target_ remains invalid
        }
    }
    // NON_PEERED refs represent composite types - not handled by single REFLink
    // EMPTY references: target_ remains invalid (cleared above)
}

engine_time_t REFLink::last_rebind_time() const noexcept {
    // Use the REF source's last_modified_time instead of storing separately
    // This saves storage and keeps the semantics identical
    if (!ref_source_bound_ || !ref_source_view_data_.valid()) {
        return MIN_DT;
    }
    TSView ref_view(ref_source_view_data_, MIN_DT);
    return ref_view.last_modified_time();
}

} // namespace hgraph
