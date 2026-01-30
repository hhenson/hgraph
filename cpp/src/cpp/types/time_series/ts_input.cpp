#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/value/indexed_view.h>
#include <hgraph/types/node.h>

namespace hgraph {

// ============================================================================
// TSInput Implementation
// ============================================================================

TSInput::TSInput(const TSMeta* ts_meta, node_ptr owner)
    : value_(ts_meta)
    , active_(generate_active_schema(ts_meta))
    , meta_(ts_meta)
    , owning_node_(owner) {
}

TSInputView TSInput::view(engine_time_t current_time) {
    TSView ts_view = value_.ts_view(current_time);
    // Set the path on the view
    ts_view.view_data().path = root_path();
    // Pass the active view for hierarchical active state tracking
    return TSInputView(std::move(ts_view), this, active_.view());
}

TSInputView TSInput::view(engine_time_t current_time, const TSMeta* schema) {
    // For now, ignore schema parameter - inputs don't have alternatives
    // The schema conversion happens at the output side
    return view(current_time);
}

void TSInput::set_active(bool active) {
    // Set the root-level active state
    value::View av = active_view();
    if (!av) return;

    // For composite types, the first element is the root-level bool
    // For scalar types, it's just a bool directly
    if (meta_->is_collection() || meta_->kind == TSKind::TSB) {
        // Composite: tuple[bool, ...]
        value::TupleView tv = av.as_tuple();
        value::View root = tv[0];
        if (root) {
            *static_cast<bool*>(root.data()) = active;
        }

        // For TSL/TSD, also set element active states
        if (meta_->is_collection()) {
            value::View element_list = tv[1];
            if (element_list) {
                value::ListView lv = element_list.as_list();
                for (size_t i = 0; i < lv.size(); ++i) {
                    value::View elem_active = lv[i];
                    if (elem_active) {
                        // Element active could be bool (scalar) or tuple (composite)
                        const TSMeta* elem_ts = meta_->element_ts;
                        if (elem_ts && (elem_ts->is_collection() || elem_ts->kind == TSKind::TSB)) {
                            // Composite element: set first element (root bool)
                            value::TupleView elem_tv = elem_active.as_tuple();
                            value::View elem_root = elem_tv[0];
                            if (elem_root) {
                                *static_cast<bool*>(elem_root.data()) = active;
                            }
                        } else {
                            // Scalar element: set directly
                            *static_cast<bool*>(elem_active.data()) = active;
                        }
                    }
                }
            }
        }
    } else {
        // Scalar: just bool
        *static_cast<bool*>(av.data()) = active;
    }

    // TODO: If we have bound outputs, subscribe/unsubscribe from them
    // This requires tracking bound outputs at each position
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

    // 3. Subscribe for notifications if active
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

    // 3. Remove link
    ts_view_.unbind();
}

void TSInputView::make_active() {
    if (!input_) return;

    // Set active on the owning input
    input_->set_active(true);

    // If we're bound, subscribe to the output
    if (is_bound() && bound_output_) {
        TSOutputView output_view = bound_output_->view(ts_view_.current_time());
        output_view.subscribe(input_);
    }
}

void TSInputView::make_passive() {
    if (!input_) return;

    // Set passive on the owning input
    input_->set_active(false);

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

    // For composite types, the first element is the active bool at this level
    // For scalar types, it's just a bool directly
    if (meta->is_collection() || meta->kind == TSKind::TSB) {
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

} // namespace hgraph
