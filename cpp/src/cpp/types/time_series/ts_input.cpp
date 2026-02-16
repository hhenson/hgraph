#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/value/indexed_view.h>
#include <hgraph/types/node.h>

namespace hgraph {

// ============================================================================
// TSInput Implementation
// ============================================================================

TSInput::TSInput(const TSMeta* ts_meta, node_ptr owner)
    : value_(ts_meta, generate_input_link_schema(ts_meta))  // Use LinkTarget-based schema
    , active_(generate_active_schema(ts_meta))
    , meta_(ts_meta)
    , owning_node_(owner) {
}

TSInputView TSInput::view(engine_time_t current_time) {
    TSView ts_view = value_.ts_view(current_time);
    // Set the path on the view
    ts_view.view_data().path = root_path();
    // Mark that this view uses LinkTarget (not REFLink) for link storage
    ts_view.view_data().uses_link_target = true;
    // Pass the active view for hierarchical active state tracking
    TSInputView iv(std::move(ts_view), this, active_.view());
    // Carry the persistent bound output to the new view
    iv.set_bound_output(bound_output_);
    return iv;
}

TSInputView TSInput::view(engine_time_t current_time, const TSMeta* schema) {
    // For now, ignore schema parameter - inputs don't have alternatives
    // The schema conversion happens at the output side
    return view(current_time);
}

void TSInput::set_active(bool active) {
    // Get active view - return early if not available
    value::View av = active_view();
    if (!av || !meta_) return;

    // Build ViewData from TSValue for dispatch
    ViewData vd = value_.make_view_data();
    // TSInput uses LinkTarget-based link schema (not REFLink)
    vd.uses_link_target = true;

    // Dispatch through ts_ops table
    vd.ops->set_active(vd, av, active, this);

    // Handle signal multi-bind subscriptions (non-peered TSB → SIGNAL)
    for (auto& sub : signal_subscriptions_) {
        if (active) {
            sub->subscribe();
        } else {
            sub->unsubscribe();
        }
    }

    // Handle REF binding proxies (REF output → non-REF input)
    for (auto& proxy : ref_binding_proxies_) {
        if (active) {
            proxy->subscribe();
        } else {
            proxy->unsubscribe();
        }
    }
}

void TSInput::set_active(const std::string& field, bool active) {
    // Only valid for TSB inputs
    if (!meta_ || meta_->kind != TSKind::TSB) {
        return;
    }

    // Find the field index
    size_t field_index = meta_->field_count; // Invalid by default
    for (size_t i = 0; i < meta_->field_count; ++i) {
        if (meta_->fields[i].name == field) {
            field_index = i;
            break;
        }
    }

    if (field_index >= meta_->field_count) {
        return; // Field not found
    }

    // Navigate to the field's active state
    // Active schema for TSB is: tuple[bool, active_schema(field_0), active_schema(field_1), ...]
    // So the field's active state is at index (field_index + 1)
    value::View av = active_view();
    if (!av) return;

    value::TupleView tv = av.as_tuple();
    value::View field_active = tv[field_index + 1]; // +1 because index 0 is the container-level bool

    if (!field_active) return;

    // The field_active could be a simple bool or a nested tuple depending on the field's type.
    // Only TSD, TSL, and TSB have tuple active schemas (with container bool + child states).
    // TSS and TSW have scalar bool active schemas despite being collections.
    if (field_active.is_tuple()) {
        // Nested composite: first element is the active bool
        value::TupleView field_tv = field_active.as_tuple();
        value::View field_root = field_tv[0];
        if (field_root) {
            *static_cast<bool*>(field_root.data()) = active;
        }
    } else {
        // Scalar: just set the bool directly
        *static_cast<bool*>(field_active.data()) = active;
    }

    // Manage subscription for this field if bound
    // TSInput uses LinkTarget-based link schema: tuple[LinkTarget, link_schema(field_0), ...]
    value::View link_view = value_.link_view();
    if (link_view) {
        value::TupleView link_tuple = link_view.as_tuple();
        // Field's link is at index (field_index + 1) because index 0 is the bundle-level LinkTarget
        value::View field_link_view = link_tuple[field_index + 1];
        if (field_link_view) {
            auto* lt = static_cast<LinkTarget*>(field_link_view.data());
            if (lt) {
                // Detect TS→REF binding: REF field bound to non-REF target
                const TSMeta* field_ts = meta_->fields[field_index].ts_type;
                bool is_ts_to_ref = (field_ts && field_ts->kind == TSKind::REF &&
                                     lt->is_linked && lt->meta && lt->meta->kind != TSKind::REF);

                if (active) {
                    // Set owning_input so REFBindingHelper can schedule via active_notifier.
                    // This is critical for REF→REF bindings where lt->observer_data is nullptr
                    // (REFBindingHelper manages subscriptions) — without this, the
                    // REFBindingHelper cannot schedule the owning node when the REF changes.
                    lt->active_notifier.owning_input = this;

                    if (is_ts_to_ref) {
                        // TS→REF: The reference is valid from bind time.
                        // Fire initial notification at MIN_ST so the node evaluates at the first tick.
                        // Do NOT subscribe to the target's observer list (reference is fixed).
                        notify(MIN_ST);
                    } else if (lt->is_linked && lt->observer_data) {
                        auto* observers = static_cast<ObserverList*>(lt->observer_data);
                        observers->add_observer(this);
                        // Initial notification: if the output is already valid AND modified,
                        // fire notify to schedule the owning node (matches Python make_active behavior).
                        if (lt->ops) {
                            ViewData output_vd;
                            output_vd.value_data = lt->value_data;
                            output_vd.time_data = lt->time_data;
                            output_vd.meta = lt->meta;
                            output_vd.ops = lt->ops;
                            if (lt->ops->valid(output_vd)) {
                                auto* node = owning_node();
                                if (node && node->cached_evaluation_time_ptr()) {
                                    engine_time_t eval_time = *node->cached_evaluation_time_ptr();
                                    if (lt->ops->modified(output_vd, eval_time)) {
                                        engine_time_t lmt = lt->ops->last_modified_time(output_vd);
                                        notify(lmt);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    if (!is_ts_to_ref && lt->is_linked && lt->observer_data) {
                        auto* observers = static_cast<ObserverList*>(lt->observer_data);
                        observers->remove_observer(this);
                    }
                    lt->active_notifier.owning_input = nullptr;
                }
            }
        }
    }
}

TSInput::~TSInput() {
    for (auto& sub : signal_subscriptions_) {
        sub->unsubscribe();
    }
    for (auto& proxy : ref_binding_proxies_) {
        proxy->unsubscribe();
    }
}

void TSInput::notify(engine_time_t et) {
    if (owning_node_) {
        owning_node_->notify(et);
    }
}

void TSInput::add_signal_subscription(engine_time_t* signal_time_data, ObserverList* output_observers) {
    auto sub = std::make_unique<SignalSubscription>();
    sub->signal_time_data = signal_time_data;
    sub->output_observers = output_observers;
    sub->owning_node = owning_node_;
    signal_subscriptions_.push_back(std::move(sub));
}

// ============================================================================
// SignalSubscription Implementation
// ============================================================================

void SignalSubscription::notify(engine_time_t et) {
    if (signal_time_data) *signal_time_data = et;
    if (owning_node) owning_node->notify(et);
}

void SignalSubscription::subscribe() {
    if (output_observers && !subscribed) {
        output_observers->add_observer(this);
        subscribed = true;
    }
}

void SignalSubscription::unsubscribe() {
    if (output_observers && subscribed) {
        output_observers->remove_observer(this);
        subscribed = false;
    }
}

// ============================================================================
// RefBindingProxy Implementation
// ============================================================================

void RefBindingProxy::notify(engine_time_t et) {
    if (!input || !ref_output_vd.value_data || !input_vd.ops) return;
    auto* ts_ref = static_cast<const TSReference*>(ref_output_vd.value_data);
    if (!ts_ref) return;

    if (ts_ref->is_empty()) {
        auto* lt = static_cast<LinkTarget*>(input_vd.link_data);
        if (lt) lt->is_linked = false;
        input->notify(et);
        return;
    }

    if (ts_ref->is_peered()) {
        try {
            TSView target_view = ts_ref->resolve(et);
            const ViewData& target_vd = target_view.view_data();
            input_vd.ops->bind(input_vd, target_vd);
        } catch (...) {}
        input->notify(et);
    }
    // For PYTHON_BOUND refs: do NOT notify — the standard bind (LinkTarget)
    // already has a subscription that handles notification correctly.
}

void RefBindingProxy::subscribe() {
    if (ref_observers && !subscribed) {
        ref_observers->add_observer(this);
        subscribed = true;
    }
}

void RefBindingProxy::unsubscribe() {
    if (ref_observers && subscribed) {
        ref_observers->remove_observer(this);
        subscribed = false;
    }
}

void TSInput::add_ref_binding_proxy(ViewData ref_output_vd, ViewData input_field_vd, ObserverList* ref_observers) {
    auto proxy = std::make_unique<RefBindingProxy>();
    proxy->ref_output_vd = ref_output_vd;
    proxy->input_vd = input_field_vd;
    proxy->input = this;
    proxy->ref_observers = ref_observers;
    ref_binding_proxies_.push_back(std::move(proxy));
}

bool TSInput::active() const noexcept {
    value::View av = const_cast<TSInput*>(this)->active_view();
    if (!av) return false;

    // For composite types, the first element is the root-level bool
    // For scalar types, it's just a bool directly
    if (meta_->is_collection() || meta_->kind == TSKind::TSB) {
        // Composite: tuple[bool, ...]
        value::TupleView tv = av.as_tuple();
        value::View root = tv[0];
        if (root) {
            return *static_cast<const bool*>(root.data());
        }
        return false;
    } else {
        // Scalar: just bool
        return *static_cast<const bool*>(av.data());
    }
}

value::View TSInput::active_view() {
    return active_.view();
}

value::View TSInput::active_view() const {
    return const_cast<value::Value<>&>(active_).view();
}


// ============================================================================
// TSInputView Implementation
// ============================================================================

void TSInputView::bind(TSOutputView& output) {
    // 1. Create link at TSValue level
    ts_view_.bind(output.ts_view());

    // 2. Track the bound output for subscription management
    bound_output_ = output.output();

    // 3. Also store on the persistent TSInput for views created later
    if (input_) {
        input_->set_bound_output(bound_output_);
    }

    // 4. Peered tracking is handled by ts_ops::bind in the link structure
    //    (LinkTarget.peered flag set by list_ops/bundle_ops/dict_ops bind)

    // 5. Subscribe for notifications if active
    // For TS→REF binding (REF input bound to non-REF output), the reference
    // is fixed at bind time — do NOT subscribe to the target's observer list.
    // Python equivalent: PythonTimeSeriesReferenceInput.do_bind_output sets
    // _output=None, so make_active() skips subscription.
    if (input_ && input_->active()) {
        const auto& vd = ts_view_.view_data();
        const auto& out_vd = output.ts_view().view_data();
        bool is_ts_to_ref = (vd.meta && vd.meta->kind == TSKind::REF &&
                             out_vd.meta && out_vd.meta->kind != TSKind::REF);
        if (!is_ts_to_ref) {
            output.subscribe(input_);
        }
    }
}

void TSInputView::unbind() {
    // 1. Unsubscribe if we were active and bound
    if (bound_output_ && input_ && input_->active()) {
        // Get a view of the bound output at current time to unsubscribe
        TSOutputView output_view = bound_output_->view(ts_view_.current_time());
        output_view.unsubscribe(input_);
    }

    // 2. Clear the bound output reference
    bound_output_ = nullptr;

    // 3. Clear persistent reference on TSInput
    if (input_) {
        input_->set_bound_output(nullptr);
    }

    // 4. Remove link
    ts_view_.unbind();
}

void TSInputView::make_active() {
    if (!input_) return;

    // Set active flag for THIS specific position only (not the root bundle).
    // Python semantics: each input has its own _active flag; make_active only
    // affects the specific input, not siblings in the same bundle.
    if (active_view_) {
        if (active_view_.is_tuple()) {
            // Composite: tuple[bool, ...] — set root bool
            value::TupleView tv = active_view_.as_tuple();
            value::View root = tv[0];
            if (root) *static_cast<bool*>(root.data()) = true;
        } else {
            // Scalar: just bool
            *static_cast<bool*>(active_view_.data()) = true;
        }
    }

    // If we're bound, subscribe to the output
    // For TS→REF binding (REF input bound to non-REF output), skip subscription:
    // the reference is fixed at bind time and should NOT tick on underlying changes.
    // Python equivalent: PythonTimeSeriesReferenceInput._output is None for TS→REF,
    // so make_active() skips subscription.
    if (is_bound()) {
        const auto& vd = ts_view_.view_data();
        bool is_ts_to_ref = false;
        if (vd.meta && vd.meta->kind == TSKind::REF) {
            // Check if target (via LinkTarget) is non-REF
            if (vd.uses_link_target && vd.link_data) {
                auto* lt = static_cast<LinkTarget*>(vd.link_data);
                is_ts_to_ref = (lt && lt->is_linked && lt->meta &&
                                lt->meta->kind != TSKind::REF);
            }
        }

        if (bound_output_ && !is_ts_to_ref) {
            // Root-level view: use bound_output_ directly
            TSOutputView output_view = bound_output_->view(ts_view_.current_time());
            output_view.subscribe(input_);

            // Initial notification: if the output is already valid AND modified,
            // fire notify to schedule the owning node (matches Python make_active behavior).
            auto& ovd = output_view.ts_view().view_data();
            if (ovd.ops && ovd.ops->valid(ovd)) {
                engine_time_t eval_time = ts_view_.current_time();
                if (ovd.ops->modified(ovd, eval_time)) {
                    engine_time_t lmt = ovd.ops->last_modified_time(ovd);
                    input_->notify(lmt);
                }
            }
        } else if (!bound_output_ && !is_ts_to_ref) {
            // Field-level view: bound_output_ not propagated from parent.
            // Use the LinkTarget's active_notifier to subscribe to the output's
            // observer list (mirrors scalar_ops::set_active).
            const auto& vd2 = ts_view_.view_data();
            if (vd2.uses_link_target && vd2.link_data) {
                auto* lt = static_cast<LinkTarget*>(vd2.link_data);
                if (lt && lt->is_linked) {
                    if (lt->active_notifier.owning_input == nullptr) {
                        lt->active_notifier.owning_input = input_;
                    }
                    if (lt->observer_data) {
                        auto* observers = static_cast<ObserverList*>(lt->observer_data);
                        observers->add_observer(&lt->active_notifier);
                    }
                    // Initial notification: if the output is already valid AND modified
                    if (lt->ops && lt->value_data) {
                        ViewData output_vd;
                        output_vd.value_data = lt->value_data;
                        output_vd.time_data = lt->time_data;
                        output_vd.meta = lt->meta;
                        output_vd.ops = lt->ops;
                        output_vd.path = vd.path;
                        if (lt->ops->valid(output_vd)) {
                            engine_time_t eval_time = ts_view_.current_time();
                            if (lt->ops->modified(output_vd, eval_time)) {
                                engine_time_t lmt = lt->ops->last_modified_time(output_vd);
                                input_->notify(lmt);
                            }
                        }
                    }
                }
            }
        }
    }

    // REF inputs: fire initial notification if the reference is already valid.
    // This matches Python's PythonTimeSeriesReferenceInput.make_active() which calls
    // self.notify(self.last_modified_time) when self.valid is True.
    // Covers both standard bind (TS→REF) and deferred (Phase 2) paths.
    // The notify duplicate guard (_notify_time != time) prevents double-firing
    // if the subscription path already delivered the initial notification.
    const auto& vd = ts_view_.view_data();
    if (vd.meta && vd.meta->kind == TSKind::REF && vd.ops && vd.ops->valid(vd)) {
        engine_time_t lmt = vd.ops->last_modified_time(vd);
        input_->notify(lmt);
    }
}

void TSInputView::make_passive() {
    if (!input_) return;

    // Clear active flag for THIS specific position only (not the root bundle).
    if (active_view_) {
        if (active_view_.is_tuple()) {
            value::TupleView tv = active_view_.as_tuple();
            value::View root = tv[0];
            if (root) *static_cast<bool*>(root.data()) = false;
        } else {
            *static_cast<bool*>(active_view_.data()) = false;
        }
    }

    // If we're bound, unsubscribe from the output
    // For TS→REF binding, we never subscribed, so skip unsubscription.
    if (is_bound()) {
        const auto& vd = ts_view_.view_data();
        bool is_ts_to_ref = false;
        if (vd.meta && vd.meta->kind == TSKind::REF) {
            if (vd.uses_link_target && vd.link_data) {
                auto* lt = static_cast<LinkTarget*>(vd.link_data);
                is_ts_to_ref = (lt && lt->is_linked && lt->meta &&
                                lt->meta->kind != TSKind::REF);
            }
        }

        if (bound_output_ && !is_ts_to_ref) {
            // Root-level view: unsubscribe TSInput from output's observer list
            TSOutputView output_view = bound_output_->view(ts_view_.current_time());
            output_view.unsubscribe(input_);
        } else if (!bound_output_ && !is_ts_to_ref) {
            // Field-level view: bound_output_ not propagated from parent.
            // Use the LinkTarget's active_notifier to unsubscribe from the
            // output's observer list (mirrors scalar_ops::set_active(false)).
            const auto& vd2 = ts_view_.view_data();
            if (vd2.uses_link_target && vd2.link_data) {
                auto* lt = static_cast<LinkTarget*>(vd2.link_data);
                if (lt && lt->active_notifier.owning_input != nullptr) {
                    if (lt->observer_data) {
                        auto* observers = static_cast<ObserverList*>(lt->observer_data);
                        observers->remove_observer(&lt->active_notifier);
                    }
                    lt->active_notifier.owning_input = nullptr;
                }
            }
        }
    }
}

bool TSInputView::active() const {
    if (!active_view_) {
        // No active view, fall back to root active state
        return input_ ? input_->active() : false;
    }

    // Get the active state from the hierarchical active view
    const TSMeta* meta = ts_meta();
    if (!meta) return false;

    // The active schema depends on the TSKind:
    // - TSB, TSD, TSL: tuple[bool, ...] (hierarchical active state)
    // - TS, TSS, TSW, REF, SIGNAL: just bool (flat active state)
    // Check the actual view's schema rather than assuming from meta->is_collection()
    if (active_view_.is_tuple()) {
        // Composite: tuple[bool, ...]
        value::TupleView tv = active_view_.as_tuple();
        value::View root = tv[0];
        if (root) {
            return *static_cast<const bool*>(root.data());
        }
        return false;
    } else {
        // Scalar: just bool
        return *static_cast<const bool*>(active_view_.data());
    }
}

TSInputView TSInputView::field(const std::string& name) const {
    TSView child = ts_view_.field(name);

    // Navigate to the field's active state
    value::View child_active;
    if (active_view_) {
        const TSMeta* meta = ts_meta();
        if (meta && meta->kind == TSKind::TSB) {
            // Find the field index
            for (size_t i = 0; i < meta->field_count; ++i) {
                if (meta->fields[i].name == name) {
                    // Active schema for TSB is: tuple[bool, active_schema(field_0), active_schema(field_1), ...]
                    // Field's active state is at index (i + 1)
                    value::TupleView tv = active_view_.as_tuple();
                    child_active = tv[i + 1];
                    break;
                }
            }
        }
    }

    return TSInputView(std::move(child), input_, child_active);
}

TSInputView TSInputView::operator[](size_t index) const {
    TSView child = ts_view_[index];

    // Navigate to the child's active state
    value::View child_active;
    if (active_view_) {
        const TSMeta* meta = ts_meta();
        if (meta) {
            if (meta->kind == TSKind::TSB) {
                // For TSB, index maps to field
                // Active schema: tuple[bool, active_schema(field_0), active_schema(field_1), ...]
                value::TupleView tv = active_view_.as_tuple();
                child_active = tv[index + 1];
            } else if (meta->is_collection()) {
                // For TSL/TSD, each element has its own active state
                // Active schema: tuple[bool, list[element_active]]
                // The element active states are in a list at index 1
                value::TupleView tv = active_view_.as_tuple();
                value::View element_list = tv[1];
                if (element_list) {
                    // Access the element's active state from the list
                    value::ListView lv = element_list.as_list();
                    if (index < lv.size()) {
                        child_active = lv[index];
                    }
                }
            }
        }
    }

    return TSInputView(std::move(child), input_, child_active);
}

bool TSInputView::any_active() const {
    const TSMeta* meta = ts_meta();
    if (!meta) return false;

    // For scalar types, return same as active()
    if (meta->is_scalar_ts()) {
        return active();
    }

    // For TSB: check all fields
    if (meta->kind == TSKind::TSB) {
        for (size_t i = 0; i < meta->field_count; ++i) {
            TSInputView child = (*this)[i];
            if (child.active()) {
                return true;
            }
        }
        return false;
    }

    // For TSL/TSD: check all elements
    if (meta->is_collection()) {
        size_t count = size();
        for (size_t i = 0; i < count; ++i) {
            TSInputView child = (*this)[i];
            if (child.active()) {
                return true;
            }
        }
        return false;
    }

    // Default fallback
    return active();
}

bool TSInputView::all_active() const {
    const TSMeta* meta = ts_meta();
    if (!meta) return false;

    // For scalar types, return same as active()
    if (meta->is_scalar_ts()) {
        return active();
    }

    // For TSB: check all fields
    if (meta->kind == TSKind::TSB) {
        if (meta->field_count == 0) {
            return true; // Empty bundle - vacuously true
        }
        for (size_t i = 0; i < meta->field_count; ++i) {
            TSInputView child = (*this)[i];
            if (!child.active()) {
                return false;
            }
        }
        return true;
    }

    // For TSL/TSD: check all elements
    if (meta->is_collection()) {
        size_t count = size();
        if (count == 0) {
            return true; // Empty collection - vacuously true
        }
        for (size_t i = 0; i < count; ++i) {
            TSInputView child = (*this)[i];
            if (!child.active()) {
                return false;
            }
        }
        return true;
    }

    // Default fallback
    return active();
}

FQPath TSInputView::fq_path() const {
    if (!input_) {
        // No owner context - return empty FQPath
        return FQPath();
    }
    return input_->to_fq_path(ts_view_);
}

} // namespace hgraph
