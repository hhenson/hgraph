#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
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

    // The field_active could be a simple bool or a nested tuple depending on the field's type
    const TSMeta* field_ts = meta_->fields[field_index].ts_type;
    if (field_ts->is_collection() || field_ts->kind == TSKind::TSB) {
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
                if (active) {
                    // Set owning_input so REFBindingHelper can schedule via active_notifier.
                    // This is critical for REF→REF bindings where lt->observer_data is nullptr
                    // (REFBindingHelper manages subscriptions) — without this, the
                    // REFBindingHelper cannot schedule the owning node when the REF changes.
                    lt->active_notifier.owning_input = this;
                    if (lt->is_linked && lt->observer_data) {
                        auto* observers = static_cast<ObserverList*>(lt->observer_data);
                        observers->add_observer(this);
                    }
                } else {
                    if (lt->is_linked && lt->observer_data) {
                        auto* observers = static_cast<ObserverList*>(lt->observer_data);
                        observers->remove_observer(this);
                    }
                    lt->active_notifier.owning_input = nullptr;
                }
            }
        }
    }
}

void TSInput::notify(engine_time_t et) {
    // Called when a bound output changes
    // Schedule the owning node for execution
    if (owning_node_) {
        // Delegate to the node's notify method which handles scheduling
        owning_node_->notify(et);
    }
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
    if (input_ && input_->active()) {
        output.subscribe(input_);
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
    if (is_bound() && bound_output_) {
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
    if (is_bound() && bound_output_) {
        TSOutputView output_view = bound_output_->view(ts_view_.current_time());
        output_view.unsubscribe(input_);
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
