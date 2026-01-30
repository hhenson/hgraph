#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/observer_list.h>

namespace hgraph {

// ============================================================================
// TSOutput Implementation
// ============================================================================

TSOutput::TSOutput(const TSMeta* ts_meta, node_ptr owner, size_t port_index)
    : native_value_(ts_meta)
    , owning_node_(owner)
    , port_index_(port_index) {
}

TSOutputView TSOutput::view(engine_time_t current_time) {
    TSView ts_view = native_value_.ts_view(current_time);
    // Set the path on the view
    ts_view.view_data().path = root_path();
    return TSOutputView(std::move(ts_view), this);
}

TSOutputView TSOutput::view(engine_time_t current_time, const TSMeta* schema) {
    // Same schema as native - return native view
    if (schema == native_value_.meta()) {
        return view(current_time);
    }

    // Get or create alternative for this schema
    TSValue& alt = get_or_create_alternative(schema);
    TSView ts_view = alt.ts_view(current_time);
    // Set the path on the view (same root, different schema)
    ts_view.view_data().path = root_path();
    return TSOutputView(std::move(ts_view), this);
}

TSValue& TSOutput::get_or_create_alternative(const TSMeta* schema) {
    // Check for existing alternative
    auto it = alternatives_.find(schema);
    if (it != alternatives_.end()) {
        return it->second;
    }

    // Create new alternative with target schema
    auto [insert_it, inserted] = alternatives_.emplace(schema, TSValue(schema));
    TSValue& alt = insert_it->second;

    // Establish appropriate links from alternative to native
    // Use a dummy time since we're just setting up structure
    engine_time_t setup_time = MIN_ST;
    TSView alt_view = alt.ts_view(setup_time);
    TSView native_view = native_value_.ts_view(setup_time);

    establish_links_recursive(alt, alt_view, native_view, schema, native_value_.meta());

    return alt;
}

void TSOutput::establish_links_recursive(
    TSValue& alt,
    TSView alt_view,
    TSView native_view,
    const TSMeta* target_meta,
    const TSMeta* native_meta
) {
    // Case 1: Same type - create direct Link
    if (target_meta == native_meta) {
        alt_view.bind(native_view);
        return;
    }

    // Case 2: REF on native side (dereferencing needed)
    if (native_meta->kind == TSKind::REF) {
        const TSMeta* deref_meta = native_meta->element_ts;  // REF's element_ts is the referenced type

        // Get the REFLink from the alternative's link storage (inline storage)
        // The link schema uses REFLink at each position, providing stable addresses
        // for proper lifecycle management with two-phase removal
        void* link_data = alt_view.view_data().link_data;
        if (!link_data) {
            // No link storage available - fall back to direct bind
            alt_view.bind(native_view);
            return;
        }

        REFLink* ref_link = static_cast<REFLink*>(link_data);

        // Bind the REFLink to the REF source - this sets up:
        // 1. Subscription to REF source for rebind notifications
        // 2. Initial dereference to get current target
        ref_link->bind_to_ref(native_view, MIN_ST);

        if (target_meta == deref_meta) {
            // REF[X] → X: Simple REFLink dereference
            // The REFLink's target is now the dereferenced value
            if (ref_link->valid()) {
                TSView target = ref_link->target_view(MIN_ST);
                if (target) {
                    // For simple dereference, the alternative's value access
                    // goes through the REFLink's target
                    alt_view.bind(target);
                }
            }
        } else {
            // REF[X] → Y where X != Y: REFLink + nested conversion
            // Example: REF[TSD[str, TS[int]]] → TSD[str, REF[TS[int]]]
            //
            // The REFLink dereferences the outer REF to get the actual X
            // Then we recursively establish links for the X → Y conversion
            if (ref_link->valid()) {
                TSView linked_target = ref_link->target_view(MIN_ST);
                if (linked_target) {
                    // Now recursively establish links between alt_view and linked_target
                    // This handles the nested conversion (e.g., TS[int] → REF[TS[int]] for each element)
                    establish_links_recursive(alt, alt_view, linked_target, target_meta, deref_meta);
                }
            }
        }
        return;
    }

    // Case 3: TSValue → REF (wrapping)
    if (native_meta->kind == TSKind::TSValue && target_meta->kind == TSKind::REF) {
        // Store TSReference value constructed from native's path
        // The TSReference points to the native position
        ShortPath native_path = native_view.short_path();

        // Create a peered TSReference using factory method
        TSReference ref = TSReference::peered(native_path);

        // Get the value view from the alternative and set the TSReference
        // The alternative's value storage should be a TSReference type
        value::View alt_value = alt_view.value();
        if (alt_value) {
            // Copy the TSReference into the alternative's value storage
            // The value schema for REF types stores TSReference
            auto* ref_ptr = static_cast<TSReference*>(alt_value.data());
            if (ref_ptr) {
                *ref_ptr = std::move(ref);
            }
        }
        return;
    }

    // Case 4: Both are bundles - recurse into each field
    if (target_meta->kind == TSKind::TSB && native_meta->kind == TSKind::TSB) {
        size_t num_fields = target_meta->field_count;
        for (size_t i = 0; i < num_fields; ++i) {
            establish_links_recursive(
                alt,
                alt_view[i],
                native_view[i],
                target_meta->fields[i].ts_type,
                native_meta->fields[i].ts_type
            );
        }
        return;
    }

    // Case 5: Both are lists/dicts - recurse into elements
    if ((target_meta->kind == TSKind::TSL && native_meta->kind == TSKind::TSL) ||
        (target_meta->kind == TSKind::TSD && native_meta->kind == TSKind::TSD)) {
        // For collections, we need to:
        // 1. Subscribe to native structural changes
        // 2. For existing elements, recurse with element schemas

        // TODO: Implement structural change subscription
        // For now, handle existing elements
        const TSMeta* target_elem = target_meta->element_ts;
        const TSMeta* native_elem = native_meta->element_ts;

        // If element types are the same, bind the whole collection
        if (target_elem == native_elem) {
            alt_view.bind(native_view);
            return;
        }

        // Otherwise, elements need individual conversion
        // This is deferred until elements exist
        // TODO: Subscribe to native for structural changes and set up per-element links
        return;
    }

    // Default: Direct bind (may not be type-correct, but handles unknown cases)
    alt_view.bind(native_view);
}

// ============================================================================
// TSOutputView Implementation
// ============================================================================

void TSOutputView::subscribe(Notifiable* observer) {
    if (!observer) return;

    // Get the observer list for this position
    value::View obs_view = ts_view_.observer();
    if (!obs_view) return;

    // Cast to ObserverList and add
    auto* obs_list = static_cast<ObserverList*>(obs_view.data());
    obs_list->add_observer(observer);
}

void TSOutputView::unsubscribe(Notifiable* observer) {
    if (!observer) return;

    // Get the observer list for this position
    value::View obs_view = ts_view_.observer();
    if (!obs_view) return;

    // Cast to ObserverList and remove
    auto* obs_list = static_cast<ObserverList*>(obs_view.data());
    obs_list->remove_observer(observer);
}

TSOutputView TSOutputView::field(const std::string& name) const {
    TSView child = ts_view_.field(name);
    return TSOutputView(std::move(child), output_);
}

TSOutputView TSOutputView::operator[](size_t index) const {
    TSView child = ts_view_[index];
    return TSOutputView(std::move(child), output_);
}

} // namespace hgraph
