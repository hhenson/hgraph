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
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/set_delta.h>
#include <hgraph/types/value/map_storage.h>
#include <hgraph/types/value/set_storage.h>
#include <hgraph/types/node.h>

#include <stdexcept>

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

// Get REFLink pointer from link_data (for TSL/TSD)
// Note: Link schema now uses REFLink instead of LinkTarget for inline storage
inline REFLink* get_ref_link(void* link_data) {
    return link_data ? static_cast<REFLink*>(link_data) : nullptr;
}

inline const REFLink* get_ref_link(const void* link_data) {
    return link_data ? static_cast<const REFLink*>(link_data) : nullptr;
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

} // anonymous namespace

// ============================================================================
// Scalar Operations (TSValue, TSW, SIGNAL, REF)
// ============================================================================

namespace scalar_ops {

// For scalar TS types:
// - time is directly engine_time_t*
// - observer is directly ObserverList*
// - link is REFLink (stores target ViewData when bound)

// Helper: Check if this scalar is linked and get the REFLink
inline const REFLink* get_active_link(const ViewData& vd) {
    auto* rl = get_ref_link(vd.link_data);
    return (rl && rl->target().valid()) ? rl : nullptr;
}

engine_time_t last_modified_time(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->last_modified_time(make_view_data_from_link(*rl, vd.path));
    }
    if (!vd.time_data) return MIN_ST;
    return *static_cast<engine_time_t*>(vd.time_data);
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->modified(make_view_data_from_link(*rl, vd.path), current_time);
    }
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->valid(make_view_data_from_link(*rl, vd.path));
    }
    // Check against MIN_DT (the uninitialized sentinel), not MIN_ST (minimum start time)
    return last_modified_time(vd) != MIN_DT;
}

bool all_valid(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->all_valid(make_view_data_from_link(*rl, vd.path));
    }
    // Scalar has no children
    return valid(vd);
}

bool sampled(const ViewData& vd) {
    // Return the sampled flag from ViewData
    // This flag is set when navigating through a REFLink that was rebound
    return vd.sampled;
}

value::View value(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->value(make_view_data_from_link(*rl, vd.path));
    }
    return make_value_view(vd);
}

value::View delta_value(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->delta_value(make_view_data_from_link(*rl, vd.path));
    }
    // For scalar types, delta_value == value (the "event" is the value itself)
    return make_value_view(vd);
}

bool has_delta(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->has_delta(make_view_data_from_link(*rl, vd.path));
    }
    // Scalar types always have a delta when they have a value
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
        *static_cast<engine_time_t*>(vd.time_data) = MIN_ST;
    }
}

nb::object to_python(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->to_python(make_view_data_from_link(*rl, vd.path));
    }

    // Check time-series validity first (has value been set?)
    if (!valid(vd)) return nb::none();
    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

nb::object delta_to_python(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->delta_to_python(make_view_data_from_link(*rl, vd.path));
    }
    // For scalar types, delta_value == value (the "event" is the value itself)
    // Check time-series validity first (has value been set?)
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
    // For scalar TS types, binding stores the target ViewData in the REFLink.
    // This enables the scalar to delegate value/modified/valid checks to the target.
    if (!vd.link_data) {
        throw std::runtime_error("bind on scalar without link data");
    }
    auto* rl = get_ref_link(vd.link_data);
    if (!rl) {
        throw std::runtime_error("bind on scalar with invalid link data");
    }
    store_link_target(*rl, target);
}

void unbind(ViewData& vd) {
    // For scalar TS types, unbinding clears the REFLink target.
    if (!vd.link_data) return;

    auto* rl = get_ref_link(vd.link_data);
    if (rl) {
        rl->unbind();
    }
}

bool is_bound(const ViewData& vd) {
    if (!vd.link_data) return false;

    auto* rl = get_ref_link(vd.link_data);
    return rl && rl->target().is_linked;
}

void set_active(ViewData& vd, value::View active_view, bool active, TSInput* input) {
    if (!active_view) return;

    // Scalar active schema is just a bool
    *static_cast<bool*>(active_view.data()) = active;

    // Manage subscription for scalar input if bound
    if (vd.link_data) {
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

} // namespace scalar_ops

// ============================================================================
// Bundle Operations (TSB)
// ============================================================================

namespace bundle_ops {

// For TSB types:
// - value is bundle type
// - time is tuple[engine_time_t, field_times...]
// - observer is tuple[ObserverList, field_observers...]
// - link is tuple[REFLink, link_schema(field_0), link_schema(field_1), ...]
//   where element 0 is the bundle-level REFLink, and elements 1+ are per-field link data

// Helper: Get link data for a specific field (returns void* to field's link storage)
inline void* get_field_link_data(const ViewData& vd, size_t field_index) {
    if (!vd.link_data || !vd.meta || field_index >= vd.meta->field_count) {
        return nullptr;
    }
    auto link_schema = TSMetaSchemaCache::instance().get_link_schema(vd.meta);
    if (!link_schema) return nullptr;

    value::View link_view(vd.link_data, link_schema);
    // Field link is at index field_index + 1 (since element 0 is bundle-level REFLink)
    return link_view.as_tuple().at(field_index + 1).data();
}

// Helper: Get REFLink for a scalar field (field's link is just a REFLink)
inline REFLink* get_scalar_field_ref_link(const ViewData& vd, size_t field_index) {
    void* link_data = get_field_link_data(vd, field_index);
    if (!link_data) return nullptr;
    return static_cast<REFLink*>(link_data);
}

// Helper: Check if any field is linked (only checks scalar fields)
inline bool any_field_linked(const ViewData& vd) {
    if (!vd.link_data || !vd.meta) return false;
    auto link_schema = TSMetaSchemaCache::instance().get_link_schema(vd.meta);
    if (!link_schema) return false;

    value::View link_view(vd.link_data, link_schema);
    auto link_tuple = link_view.as_tuple();

    // Check each scalar field's link (starting at index 1)
    for (size_t i = 0; i < vd.meta->field_count; ++i) {
        const TSMeta* field_meta = vd.meta->fields[i].ts_type;
        if (field_meta && field_meta->is_scalar_ts()) {
            auto* rl = static_cast<const REFLink*>(link_tuple.at(i + 1).data());
            if (rl && rl->target().is_linked) {
                return true;
            }
        }
    }
    return false;
}

engine_time_t last_modified_time(const ViewData& vd) {
    auto time_view = make_time_view(vd);
    if (!time_view.valid()) return MIN_ST;
    return time_view.as_tuple().at(0).as<engine_time_t>();
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    // First check if the bundle's own time indicates validity
    if (last_modified_time(vd) != MIN_ST) {
        return true;
    }

    // For input bundles, check if any linked field is valid
    // (input bundles don't have their own time set - they delegate through links)
    if (!vd.link_data || !vd.meta) return false;
    auto link_schema = TSMetaSchemaCache::instance().get_link_schema(vd.meta);
    if (!link_schema) return false;

    value::View link_view(vd.link_data, link_schema);
    auto link_tuple = link_view.as_tuple();

    // Check each field's link - if any field is linked and valid, the bundle is valid
    for (size_t i = 0; i < vd.meta->field_count; ++i) {
        const TSMeta* field_meta = vd.meta->fields[i].ts_type;
        if (!field_meta) continue;

        if (field_meta->is_scalar_ts()) {
            // Scalar field: link is a REFLink
            auto* rl = static_cast<const REFLink*>(link_tuple.at(i + 1).data());
            if (rl && rl->target().is_linked && rl->target().ops) {
                ViewData field_vd = make_view_data_from_link(*rl, vd.path);
                if (rl->target().ops->valid(field_vd)) {
                    return true;
                }
            }
        } else {
            // Composite field (TSL, TSD, TSB): recursively check validity
            // The link data for composite fields is their nested link storage
            void* field_link_data = link_tuple.at(i + 1).data();
            if (field_link_data) {
                ViewData field_vd;
                field_vd.link_data = field_link_data;
                field_vd.meta = field_meta;
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
    if (!valid(vd)) return false;

    // Check all fields recursively
    if (!vd.meta) return false;

    auto value_view = make_value_view(vd);
    auto time_view = make_time_view(vd);
    auto observer_view = make_observer_view(vd);

    if (!time_view.valid()) return false;

    for (size_t i = 0; i < vd.meta->field_count; ++i) {
        const TSMeta* field_meta = vd.meta->fields[i].ts_type;
        auto ft = time_view.as_tuple().at(i + 1);

        if (field_meta->is_scalar_ts()) {
            // Check if this scalar field is linked
            auto* rl = get_scalar_field_ref_link(vd, i);
            if (rl && rl->target().valid()) {
                // Field is linked - delegate all_valid to target
                ViewData target_vd = make_view_data_from_link(*rl, vd.path.child(i));
                if (!target_vd.ops || !target_vd.ops->all_valid(target_vd)) {
                    return false;
                }
            } else {
                // Scalar field not linked - check timestamp directly
                engine_time_t field_time = ft.as<engine_time_t>();
                if (field_time == MIN_ST) return false;
            }
        } else {
            // Composite field - check timestamp first
            engine_time_t field_time = ft.as_tuple().at(0).as<engine_time_t>();
            if (field_time == MIN_ST) return false;

            // Construct child ViewData for recursive all_valid check
            ViewData field_vd;
            field_vd.path = vd.path.child(i);
            field_vd.value_data = value_view.as_bundle().at(i).data();
            field_vd.time_data = ft.data();
            field_vd.observer_data = observer_view.valid() ? observer_view.as_tuple().at(i + 1).data() : nullptr;
            field_vd.delta_data = nullptr;
            field_vd.sampled = vd.sampled;
            field_vd.link_data = get_field_link_data(vd, i);
            field_vd.ops = get_ts_ops(field_meta);
            field_vd.meta = field_meta;

            // Recursively check all_valid on composite fields
            if (!field_vd.ops || !field_vd.ops->all_valid(field_vd)) {
                return false;
            }
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
        time_view.as_tuple().at(0).as<engine_time_t>() = MIN_ST;
    }
}

nb::object to_python(const ViewData& vd) {
    // Check time-series validity first (has value been set?)
    if (!valid(vd)) return nb::none();

    // For input bundles with per-field links, we need to build the dict
    // by following the links to get actual values from the bound outputs
    if (vd.link_data && vd.meta) {
        auto* link_schema = TSMetaSchemaCache::instance().get_link_schema(vd.meta);
        if (link_schema) {
            value::View link_view(vd.link_data, link_schema);
            auto link_tuple = link_view.as_tuple();

            // Check if any field has a valid link (indicating this is a linked input bundle)
            bool has_links = false;
            for (size_t i = 0; i < vd.meta->field_count; ++i) {
                const TSMeta* field_meta = vd.meta->fields[i].ts_type;
                if (field_meta && field_meta->is_scalar_ts()) {
                    value::View field_link = link_tuple.at(i + 1);  // +1 for bundle-level link
                    auto* rl = static_cast<const REFLink*>(field_link.data());
                    if (rl && rl->target().is_linked) {
                        has_links = true;
                        break;
                    }
                }
            }

            if (has_links) {
                // Build dict from linked field values
                nb::dict result;
                for (size_t i = 0; i < vd.meta->field_count; ++i) {
                    const TSBFieldInfo& field_info = vd.meta->fields[i];
                    const TSMeta* field_meta = field_info.ts_type;
                    const char* field_name = field_info.name;

                    if (field_meta && field_meta->is_scalar_ts()) {
                        value::View field_link = link_tuple.at(i + 1);
                        auto* rl = static_cast<const REFLink*>(field_link.data());
                        if (rl && rl->target().is_linked && rl->target().ops) {
                            // Follow link to get value from target
                            ViewData target_vd = make_view_data_from_link(*rl, vd.path.child(i));
                            nb::object field_val = target_vd.ops->to_python(target_vd);
                            result[field_name] = field_val;
                        } else {
                            result[field_name] = nb::none();
                        }
                    } else {
                        // Composite field - get from local storage for now
                        // TODO: handle linked composite fields
                        result[field_name] = nb::none();
                    }
                }
                return result;
            }
        }
    }

    // No links or not an input bundle - use local value storage
    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

nb::object delta_to_python(const ViewData& vd) {
    // For TSB, delta equals value (no delta tracking)
    return to_python(vd);
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("from_python on invalid ViewData");
    }

    auto dst = make_value_view(vd);
    dst.from_python(src);

    auto time_view = make_time_view(vd);
    time_view.as_tuple().at(0).as<engine_time_t>() = current_time;

    // Notify bundle-level observers
    if (vd.observer_data) {
        auto observer_view = make_observer_view(vd);
        auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
        observers->notify_modified(current_time);

        // Also notify field-level observers (for subscribers bound to individual fields)
        // This matches Python behavior where setting tsb.value sets each field individually
        if (vd.meta) {
            auto observer_tuple = observer_view.as_tuple();
            for (size_t i = 0; i < vd.meta->field_count; ++i) {
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

    // Check if this scalar field is linked (composite fields have nested link storage)
    if (field_meta && field_meta->is_scalar_ts()) {
        auto* rl = get_scalar_field_ref_link(vd, index);
        if (rl && rl->target().valid()) {
            // Field is linked - return TSView pointing to target
            // Set sampled flag if REF was rebound at current_time OR if parent was already sampled
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

    // Set link_data for binding support
    // - For scalar fields: points to the REFLink (so binding stores target there)
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

void bind(ViewData& vd, const ViewData& target) {
    // TSB: Bind the entire bundle to target
    // This binds all fields to corresponding fields in the target
    if (!vd.link_data || !vd.meta) {
        throw std::runtime_error("bind on bundle without link data");
    }

    auto link_schema = TSMetaSchemaCache::instance().get_link_schema(vd.meta);
    if (!link_schema) {
        throw std::runtime_error("bind on bundle without link schema");
    }

    // For TSB, link_data points to tuple[REFLink, link_schema(field_0), ...]
    // Bind each field to the corresponding field in target
    value::View link_view(vd.link_data, link_schema);
    auto link_tuple = link_view.as_tuple();

    // Get target field data for each field
    for (size_t i = 0; i < vd.meta->field_count; ++i) {
        const TSMeta* field_meta = vd.meta->fields[i].ts_type;

        // Navigate to target's field
        // Note: We check structural validity (operator bool), not time-series validity (valid())
        // The field may not have a value yet, but the structure should be valid for binding
        TSView target_field = target.ops->child_at(target, i, MIN_ST);
        if (!target_field) continue;  // Skip if structurally invalid (no view data)

        if (field_meta && field_meta->is_scalar_ts()) {
            // Scalar field: bind directly to the REFLink
            auto* rl = static_cast<REFLink*>(link_tuple.at(i + 1).data());
            if (rl) {
                store_link_target(*rl, target_field.view_data());
            }
        } else {
            // Composite field: recursively bind using the nested link storage
            ViewData field_vd;
            field_vd.link_data = link_tuple.at(i + 1).data();
            field_vd.meta = field_meta;
            field_vd.ops = get_ts_ops(field_meta);
            if (field_vd.ops && field_vd.ops->bind) {
                field_vd.ops->bind(field_vd, target_field.view_data());
            }
        }
    }
}

void unbind(ViewData& vd) {
    if (!vd.link_data || !vd.meta) {
        return;  // No-op if not linked
    }

    auto link_schema = TSMetaSchemaCache::instance().get_link_schema(vd.meta);
    if (!link_schema) return;

    value::View link_view(vd.link_data, link_schema);
    auto link_tuple = link_view.as_tuple();

    for (size_t i = 0; i < vd.meta->field_count; ++i) {
        const TSMeta* field_meta = vd.meta->fields[i].ts_type;

        if (field_meta && field_meta->is_scalar_ts()) {
            // Scalar field: unbind the REFLink directly
            auto* rl = static_cast<REFLink*>(link_tuple.at(i + 1).data());
            if (rl) {
                rl->unbind();
            }
        } else {
            // Composite field: recursively unbind
            ViewData field_vd;
            field_vd.link_data = link_tuple.at(i + 1).data();
            field_vd.meta = field_meta;
            field_vd.ops = get_ts_ops(field_meta);
            if (field_vd.ops && field_vd.ops->unbind) {
                field_vd.ops->unbind(field_vd);
            }
        }
    }
}

bool is_bound(const ViewData& vd) {
    // TSB is considered bound if any field is bound
    return any_field_linked(vd);
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
    auto* link_schema = TSMetaSchemaCache::instance().get_link_schema(vd.meta);
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
            field_vd.ops = get_ts_ops(field_ts);
            // Get link data for this field (tuple index i+1 since element 0 is bundle REFLink)
            // For composite fields, link_tuple.at(i+1) contains the nested link schema directly,
            // not a REFLink. The data() pointer is the start of the nested link storage.
            if (link_tuple) {
                field_vd.link_data = link_tuple.at(i + 1).data();
            }
            field_vd.ops->set_active(field_vd, field_active, active, input);
        } else {
            // Scalar field - set directly
            *static_cast<bool*>(field_active.data()) = active;
        }

        // Manage subscription for bound scalar fields
        if (field_ts->is_scalar_ts() && link_tuple) {
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

} // namespace bundle_ops

// ============================================================================
// List Operations (TSL)
// ============================================================================

namespace list_ops {

// For TSL types:
// - value is list type
// - time is tuple[engine_time_t, list[element_times]]
// - observer is tuple[ObserverList, list[element_observers]]
// - link: For dynamic TSL, single REFLink for collection-level binding
//         For fixed-size TSL, fixed_list[REFLink] for per-element binding

// Helper: Check if this TSL has collection-level linking (dynamic TSL only)
// Fixed-size TSL uses per-element binding, so doesn't have collection-level link
inline const REFLink* get_active_link(const ViewData& vd) {
    // Only check for collection-level link if this is a dynamic TSL (fixed_size == 0)
    // Fixed-size TSL uses per-element binding via fixed_list[REFLink]
    if (vd.meta && vd.meta->fixed_size > 0) {
        return nullptr;  // Per-element binding, no collection-level link
    }
    auto* rl = get_ref_link(vd.link_data);
    return (rl && rl->target().valid()) ? rl : nullptr;
}

engine_time_t last_modified_time(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->last_modified_time(make_view_data_from_link(*rl, vd.path));
    }
    auto time_view = make_time_view(vd);
    if (!time_view.valid()) return MIN_ST;
    return time_view.as_tuple().at(0).as<engine_time_t>();
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->modified(make_view_data_from_link(*rl, vd.path), current_time);
    }
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    // If linked (dynamic TSL), delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->valid(make_view_data_from_link(*rl, vd.path));
    }

    // Check if the list's own time indicates validity
    if (last_modified_time(vd) != MIN_DT) {
        return true;
    }

    // For fixed-size TSL inputs with per-element binding, check if any element link is valid
    if (vd.meta && vd.meta->fixed_size > 0 && vd.link_data) {
        auto* link_schema = TSMetaSchemaCache::instance().get_link_schema(vd.meta);
        if (link_schema) {
            value::View link_view(vd.link_data, link_schema);
            auto link_list = link_view.as_list();

            for (size_t i = 0; i < link_list.size() && i < static_cast<size_t>(vd.meta->fixed_size); ++i) {
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

    return false;
}

bool all_valid(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->all_valid(make_view_data_from_link(*rl, vd.path));
    }
    if (!valid(vd)) return false;
    if (!vd.meta) return false;

    auto value_view = make_value_view(vd);
    auto time_view = make_time_view(vd);
    auto observer_view = make_observer_view(vd);

    if (!value_view.valid() || !time_view.valid()) return false;

    auto value_list = value_view.as_list();
    auto times_list = time_view.as_tuple().at(1).as_list();
    auto observer_list = observer_view.valid() ? observer_view.as_tuple().at(1).as_list() : value::ListView{};

    const TSMeta* elem_meta = vd.meta->element_ts;

    for (size_t i = 0; i < value_list.size(); ++i) {
        auto et = times_list.at(i);

        // For fixed-size TSL with per-element binding, check element link
        if (vd.meta->fixed_size > 0 && vd.link_data && elem_meta->is_scalar_ts()) {
            auto* link_schema = TSMetaSchemaCache::instance().get_link_schema(vd.meta);
            if (link_schema) {
                value::View link_view(vd.link_data, link_schema);
                auto link_list = link_view.as_list();
                if (i < link_list.size()) {
                    auto* rl = static_cast<const REFLink*>(link_list.at(i).data());
                    if (rl && rl->target().valid()) {
                        // Element is linked - delegate all_valid to target
                        ViewData target_vd = make_view_data_from_link(*rl, vd.path.child(i));
                        if (!target_vd.ops || !target_vd.ops->all_valid(target_vd)) {
                            return false;
                        }
                        continue;
                    }
                }
            }
        }

        if (elem_meta->is_scalar_ts()) {
            // Scalar element - check timestamp directly
            engine_time_t elem_time = et.as<engine_time_t>();
            if (elem_time == MIN_ST) return false;
        } else {
            // Composite element - check timestamp first
            engine_time_t elem_time = et.as_tuple().at(0).as<engine_time_t>();
            if (elem_time == MIN_ST) return false;

            // Construct child ViewData for recursive all_valid check
            ViewData elem_vd;
            elem_vd.path = vd.path.child(i);
            elem_vd.value_data = value_list.at(i).data();
            elem_vd.time_data = et.data();
            elem_vd.observer_data = observer_list.valid() && i < observer_list.size() ? observer_list.at(i).data() : nullptr;
            elem_vd.delta_data = nullptr;
            elem_vd.sampled = vd.sampled;
            elem_vd.ops = get_ts_ops(elem_meta);
            elem_vd.meta = elem_meta;

            // Recursively check all_valid on composite elements
            if (!elem_vd.ops || !elem_vd.ops->all_valid(elem_vd)) {
                return false;
            }
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
        time_view.as_tuple().at(0).as<engine_time_t>() = MIN_ST;
    }
}

nb::object to_python(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->to_python(make_view_data_from_link(*rl, vd.path));
    }
    // Check time-series validity first (has value been set?)
    if (!valid(vd)) return nb::none();
    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

nb::object delta_to_python(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->delta_to_python(make_view_data_from_link(*rl, vd.path));
    }
    return nb::none();
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

    auto dst = make_value_view(vd);
    dst.from_python(src);

    auto time_view = make_time_view(vd);
    time_view.as_tuple().at(0).as<engine_time_t>() = current_time;

    if (vd.observer_data) {
        auto observer_view = make_observer_view(vd);
        auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
        observers->notify_modified(current_time);
    }
}

TSView child_at(const ViewData& vd, size_t index, engine_time_t current_time) {
    // If linked, navigate through target
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

    if (!vd.meta || !vd.meta->element_ts) {
        return TSView{};
    }

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

    const TSMeta* elem_meta = vd.meta->element_ts;

    ViewData elem_vd;
    elem_vd.path = vd.path.child(index);
    elem_vd.value_data = value_list.at(index).data();
    elem_vd.time_data = time_view.as_tuple().at(1).as_list().at(index).data();
    elem_vd.observer_data = observer_view.as_tuple().at(1).as_list().at(index).data();
    elem_vd.delta_data = nullptr;
    elem_vd.sampled = vd.sampled;  // Propagate sampled flag from parent
    elem_vd.ops = get_ts_ops(elem_meta);
    elem_vd.meta = elem_meta;

    // For fixed-size TSL, set element's link_data to its REFLink in the fixed_list
    // This enables per-element binding for TSL inputs
    if (vd.link_data && vd.meta->fixed_size > 0) {
        auto* link_schema = TSMetaSchemaCache::instance().get_link_schema(vd.meta);
        if (link_schema && index < static_cast<size_t>(vd.meta->fixed_size)) {
            value::View link_view(vd.link_data, link_schema);
            elem_vd.link_data = link_view.as_list().at(index).data();
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
    // Lists don't have named children
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
    // Lists don't support key access
    return TSView{};
}

size_t child_count(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->child_count(make_view_data_from_link(*rl, vd.path, vd.sampled));
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
    return make_observer_view(vd);
}

void notify_observers(ViewData& vd, engine_time_t current_time) {
    // If linked, delegate to target
    if (auto* rl = get_ref_link(vd.link_data)) {
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
    // TSL: Bind the entire list to target (collection-level link)
    if (!vd.link_data) {
        throw std::runtime_error("bind on list without link data");
    }

    // For TSL, link_data points to a REFLink
    auto* rl = get_ref_link(vd.link_data);
    if (!rl) {
        throw std::runtime_error("bind on list with invalid link data");
    }

    // Store the target ViewData in the REFLink's internal LinkTarget
    store_link_target(*rl, target);
}

void unbind(ViewData& vd) {
    if (!vd.link_data) {
        return;  // No-op if not linked
    }

    auto* rl = get_ref_link(vd.link_data);
    if (rl) {
        rl->unbind();
    }
}

bool is_bound(const ViewData& vd) {
    auto* rl = get_ref_link(vd.link_data);
    return rl && rl->target().is_linked;
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

    // Manage subscription for collection-level link
    if (vd.link_data) {
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

} // namespace list_ops

// ============================================================================
// Set Operations (TSS)
// ============================================================================

namespace set_ops {

// For TSS types:
// - value is set type
// - time is engine_time_t
// - observer is ObserverList
// - delta is SetDelta

engine_time_t last_modified_time(const ViewData& vd) {
    if (!vd.time_data) return MIN_ST;
    return *static_cast<engine_time_t*>(vd.time_data);
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    return last_modified_time(vd) != MIN_DT;
}

bool all_valid(const ViewData& vd) {
    return valid(vd);
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
    return vd.delta_data != nullptr;
}

void set_value(ViewData& vd, const value::View& src, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("set_value on invalid ViewData");
    }

    auto dst = make_value_view(vd);
    dst.copy_from(src);

    *static_cast<engine_time_t*>(vd.time_data) = current_time;

    if (vd.observer_data) {
        auto* observers = static_cast<ObserverList*>(vd.observer_data);
        observers->notify_modified(current_time);
    }
}

void apply_delta(ViewData& vd, const value::View& delta, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("apply_delta on invalid ViewData");
    }

    if (!delta.valid()) {
        return;  // Nothing to apply
    }

    auto dst = make_value_view(vd);
    if (!dst.valid()) {
        throw std::runtime_error("apply_delta: TSS has no valid storage");
    }

    auto set_view = dst.as_set();

    // The delta should be a bundle-like structure with 'added' and 'removed' fields
    // For C++ SetDeltaStorage, it has added/removed collections
    // For Python, it's typically a dict/object with 'added' and 'removed' attributes
    if (delta.is_bundle()) {
        auto delta_bundle = delta.as_bundle();

        // Process removals first (to avoid removing newly added elements)
        if (delta_bundle.has_field("removed")) {
            auto removed_view = delta_bundle.at("removed");
            if (removed_view.is_set()) {
                for (auto elem : removed_view.as_set()) {
                    set_view.remove(elem);
                }
            }
        }

        // Process additions
        if (delta_bundle.has_field("added")) {
            auto added_view = delta_bundle.at("added");
            if (added_view.is_set()) {
                for (auto elem : added_view.as_set()) {
                    set_view.add(elem);
                }
            }
        }
    } else if (delta.is_set()) {
        // If delta is just a set, treat it as "set all" (replace operation)
        // This is a fallback for simple cases
        set_view.clear();
        for (auto elem : delta.as_set()) {
            set_view.add(elem);
        }
    } else {
        throw std::runtime_error("apply_delta for TSS: delta must be a bundle with 'added'/'removed' fields or a set");
    }

    // Update modification time
    *static_cast<engine_time_t*>(vd.time_data) = current_time;

    // Notify observers
    if (vd.observer_data) {
        auto* observers = static_cast<ObserverList*>(vd.observer_data);
        observers->notify_modified(current_time);
    }
}

void invalidate(ViewData& vd) {
    if (vd.time_data) {
        *static_cast<engine_time_t*>(vd.time_data) = MIN_ST;
    }
}

nb::object to_python(const ViewData& vd) {
    // Check time-series validity first (has value been set?)
    if (!valid(vd)) return nb::none();
    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

nb::object delta_to_python(const ViewData& vd) {
    // Check time-series validity first (has value been set?)
    if (!valid(vd)) return nb::none();
    auto d = make_delta_view(vd);
    if (!d.valid()) return nb::none();
    return d.to_python();
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("from_python on invalid ViewData");
    }

    auto dst = make_value_view(vd);
    dst.from_python(src);

    *static_cast<engine_time_t*>(vd.time_data) = current_time;

    if (vd.observer_data) {
        auto* observers = static_cast<ObserverList*>(vd.observer_data);
        observers->notify_modified(current_time);
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
    auto value_view = make_value_view(vd);
    if (!value_view.valid()) return 0;
    return value_view.as_set().size();
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
    // TSS doesn't support binding (set elements are values, not time-series)
    throw std::runtime_error("bind not supported for TSS types");
}

void unbind(ViewData& vd) {
    // TSS doesn't support unbinding
    throw std::runtime_error("unbind not supported for TSS types");
}

bool is_bound(const ViewData& vd) {
    // TSS is never bound
    return false;
}

// ========== Set-Specific Mutation Operations ==========

bool set_add(ViewData& vd, const value::View& elem, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("set_add on invalid ViewData");
    }

    // Get the SetStorage
    auto* storage = static_cast<value::SetStorage*>(vd.value_data);

    // Add the element (SetDelta is notified via SlotObserver if registered)
    bool added = storage->add(elem.data());

    if (added) {
        // Update timestamp
        *static_cast<engine_time_t*>(vd.time_data) = current_time;

        // Notify observers
        if (vd.observer_data) {
            auto* observers = static_cast<ObserverList*>(vd.observer_data);
            observers->notify_modified(current_time);
        }
    }

    return added;
}

bool set_remove(ViewData& vd, const value::View& elem, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("set_remove on invalid ViewData");
    }

    // Get the SetStorage
    auto* storage = static_cast<value::SetStorage*>(vd.value_data);

    // Remove the element (SetDelta is notified via SlotObserver if registered)
    bool removed = storage->remove(elem.data());

    if (removed) {
        // Update timestamp
        *static_cast<engine_time_t*>(vd.time_data) = current_time;

        // Notify observers
        if (vd.observer_data) {
            auto* observers = static_cast<ObserverList*>(vd.observer_data);
            observers->notify_modified(current_time);
        }
    }

    return removed;
}

void set_clear(ViewData& vd, engine_time_t current_time) {
    if (!vd.value_data || !vd.time_data) {
        throw std::runtime_error("set_clear on invalid ViewData");
    }

    // Get the SetStorage
    auto* storage = static_cast<value::SetStorage*>(vd.value_data);

    if (!storage->empty()) {
        // Clear all elements (SetDelta is notified via SlotObserver if registered)
        storage->clear();

        // Update timestamp
        *static_cast<engine_time_t*>(vd.time_data) = current_time;

        // Notify observers
        if (vd.observer_data) {
            auto* observers = static_cast<ObserverList*>(vd.observer_data);
            observers->notify_modified(current_time);
        }
    }
}

void set_active(ViewData& vd, value::View active_view, bool active, TSInput* input) {
    if (!active_view) return;

    // TSS active schema is just a bool (set elements are values, not time-series)
    *static_cast<bool*>(active_view.data()) = active;

    // TSS doesn't support binding, so no subscription management needed
}

} // namespace set_ops

// ============================================================================
// Dict Operations (TSD)
// ============================================================================

namespace dict_ops {

// For TSD types:
// - value is map type
// - time is tuple[engine_time_t, var_list[element_times]]
// - observer is tuple[ObserverList, var_list[element_observers]]
// - delta is MapDelta
// - link is REFLink (stores target ViewData when bound, can also handle REF→TS)

// Helper: Check if this TSD is linked and get the REFLink
inline const REFLink* get_active_link(const ViewData& vd) {
    auto* rl = get_ref_link(vd.link_data);
    return (rl && rl->target().valid()) ? rl : nullptr;
}

engine_time_t last_modified_time(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->last_modified_time(make_view_data_from_link(*rl, vd.path));
    }
    auto time_view = make_time_view(vd);
    if (!time_view.valid()) return MIN_ST;
    return time_view.as_tuple().at(0).as<engine_time_t>();
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->modified(make_view_data_from_link(*rl, vd.path), current_time);
    }
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->valid(make_view_data_from_link(*rl, vd.path));
    }
    return last_modified_time(vd) != MIN_DT;
}

bool all_valid(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->all_valid(make_view_data_from_link(*rl, vd.path));
    }
    if (!valid(vd)) return false;
    if (!vd.meta || !vd.meta->element_ts) return false;

    auto value_view = make_value_view(vd);
    auto time_view = make_time_view(vd);
    auto observer_view = make_observer_view(vd);

    if (!value_view.valid() || !time_view.valid()) return false;

    auto times_list = time_view.as_tuple().at(1).as_list();
    auto observer_list = observer_view.valid() ? observer_view.as_tuple().at(1).as_list() : value::ListView{};

    const TSMeta* elem_meta = vd.meta->element_ts;

    // Access the MapStorage directly to iterate over storage slots
    auto* storage = static_cast<value::MapStorage*>(vd.value_data);
    auto* index_set = storage->key_set().index_set();
    if (!index_set) return true;  // Empty map is all_valid if valid

    size_t logical_index = 0;
    for (auto it = index_set->begin(); it != index_set->end(); ++it, ++logical_index) {
        size_t storage_slot = *it;
        auto et = times_list.at(storage_slot);

        if (elem_meta->is_scalar_ts()) {
            // Scalar element - check timestamp directly
            engine_time_t elem_time = et.as<engine_time_t>();
            if (elem_time == MIN_ST) return false;
        } else {
            // Composite element - check timestamp first
            engine_time_t elem_time = et.as_tuple().at(0).as<engine_time_t>();
            if (elem_time == MIN_ST) return false;

            // Construct child ViewData for recursive all_valid check
            ViewData elem_vd;
            elem_vd.path = vd.path.child(logical_index);
            elem_vd.value_data = storage->value_at_slot(storage_slot);
            elem_vd.time_data = et.data();
            elem_vd.observer_data = observer_list.valid() && storage_slot < observer_list.size()
                                     ? observer_list.at(storage_slot).data() : nullptr;
            elem_vd.delta_data = nullptr;
            elem_vd.sampled = vd.sampled;
            elem_vd.ops = get_ts_ops(elem_meta);
            elem_vd.meta = elem_meta;

            // Recursively check all_valid on composite elements
            if (!elem_vd.ops || !elem_vd.ops->all_valid(elem_vd)) {
                return false;
            }
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
    if (auto* rl = get_ref_link(vd.link_data)) {
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
    if (auto* rl = get_ref_link(vd.link_data)) {
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
    if (auto* rl = get_ref_link(vd.link_data)) {
        if (rl->target().valid()) {
            ViewData target_vd = make_view_data_from_link(*rl, vd.path);
            rl->target().ops->invalidate(target_vd);
            return;
        }
    }

    if (vd.time_data) {
        auto time_view = make_time_view(vd);
        time_view.as_tuple().at(0).as<engine_time_t>() = MIN_ST;
    }
}

nb::object to_python(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->to_python(make_view_data_from_link(*rl, vd.path));
    }
    // Check time-series validity first (has value been set?)
    if (!valid(vd)) return nb::none();
    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

nb::object delta_to_python(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->delta_to_python(make_view_data_from_link(*rl, vd.path));
    }
    // Check time-series validity first (has value been set?)
    if (!valid(vd)) return nb::none();
    auto d = make_delta_view(vd);
    if (!d.valid()) return nb::none();
    return d.to_python();
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    // If linked, delegate to target (write through)
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

    auto dst = make_value_view(vd);
    dst.from_python(src);

    auto time_view = make_time_view(vd);
    time_view.as_tuple().at(0).as<engine_time_t>() = current_time;

    if (vd.observer_data) {
        auto observer_view = make_observer_view(vd);
        auto* observers = static_cast<ObserverList*>(observer_view.as_tuple().at(0).data());
        observers->notify_modified(current_time);
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

    if (!vd.meta || !vd.meta->element_ts) return TSView{};

    auto value_view = make_value_view(vd);
    auto time_view = make_time_view(vd);
    auto observer_view = make_observer_view(vd);

    auto value_map = value_view.as_map();
    if (slot >= value_map.size()) return TSView{};

    // Access the MapStorage directly to get value by slot
    // The slot here is the logical index in iteration order, need to convert to storage slot
    auto* storage = static_cast<value::MapStorage*>(vd.value_data);
    auto* index_set = storage->key_set().index_set();
    if (!index_set || slot >= index_set->size()) return TSView{};

    auto it = index_set->begin();
    std::advance(it, slot);
    size_t storage_slot = *it;

    const TSMeta* elem_meta = vd.meta->element_ts;

    ViewData elem_vd;
    elem_vd.path = vd.path.child(slot);
    elem_vd.value_data = storage->value_at_slot(storage_slot);
    elem_vd.time_data = time_view.as_tuple().at(1).as_list().at(storage_slot).data();
    elem_vd.observer_data = observer_view.as_tuple().at(1).as_list().at(storage_slot).data();
    elem_vd.delta_data = nullptr;
    elem_vd.sampled = vd.sampled;  // Propagate sampled flag from parent
    elem_vd.ops = get_ts_ops(elem_meta);
    elem_vd.meta = elem_meta;

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
    elem_vd.time_data = time_view.as_tuple().at(1).as_list().at(slot).data();
    elem_vd.observer_data = observer_view.as_tuple().at(1).as_list().at(slot).data();
    elem_vd.delta_data = nullptr;
    elem_vd.sampled = vd.sampled;  // Propagate sampled flag from parent
    elem_vd.ops = get_ts_ops(elem_meta);
    elem_vd.meta = elem_meta;

    return TSView(elem_vd, current_time);
}

size_t child_count(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* rl = get_active_link(vd)) {
        return rl->target().ops->child_count(make_view_data_from_link(*rl, vd.path, vd.sampled));
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
    if (auto* rl = get_ref_link(vd.link_data)) {
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

    // For TSD, link_data points to a REFLink
    auto* rl = get_ref_link(vd.link_data);
    if (!rl) {
        throw std::runtime_error("bind on dict with invalid link data");
    }

    // Store the target ViewData in the REFLink's internal LinkTarget
    store_link_target(*rl, target);
}

void unbind(ViewData& vd) {
    if (!vd.link_data) {
        return;  // No-op if not linked
    }

    auto* rl = get_ref_link(vd.link_data);
    if (rl) {
        rl->unbind();
    }
}

bool is_bound(const ViewData& vd) {
    auto* rl = get_ref_link(vd.link_data);
    return rl && rl->target().is_linked;
}

// ========== Dict-Specific Mutation Operations ==========

bool dict_remove(ViewData& vd, const value::View& key, engine_time_t current_time) {
    // If linked, delegate to target (write through)
    if (auto* rl = get_ref_link(vd.link_data)) {
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
    if (auto* rl = get_ref_link(vd.link_data)) {
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
        elem_vd.delta_data = nullptr;
        elem_vd.ops = get_ts_ops(elem_meta);
        elem_vd.meta = elem_meta;

        return TSView(elem_vd, current_time);
    }

    // Get time and observer views for the var_lists
    auto time_view = make_time_view(vd);
    auto observer_view = make_observer_view(vd);

    // Access the element var_lists (index 1 in the tuple)
    auto time_list = time_view.as_tuple().at(1).as_list();
    auto observer_list = observer_view.as_tuple().at(1).as_list();

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

    // Ensure var_lists can accommodate the slot
    // var_lists are dynamic (var_list type), so we can resize them
    if (slot >= time_list.size()) {
        time_list.resize(slot + 1);
    }
    if (slot >= observer_list.size()) {
        observer_list.resize(slot + 1);
    }

    // Initialize the time at this slot to MIN_ST (unset)
    time_list.at(slot).as<engine_time_t>() = MIN_ST;

    // The observer at this slot is already default-constructed by resize

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
    elem_vd.delta_data = nullptr;
    elem_vd.ops = get_ts_ops(elem_meta);
    elem_vd.meta = elem_meta;

    return TSView(elem_vd, current_time);
}

TSView dict_set(ViewData& vd, const value::View& key, const value::View& value, engine_time_t current_time) {
    // If linked, delegate to target (write through)
    if (auto* rl = get_ref_link(vd.link_data)) {
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

    // Check if key exists
    size_t slot = storage->key_set().find(key.data());
    bool is_new = (slot == static_cast<size_t>(-1));

    if (is_new) {
        // Insert new key with the provided value
        storage->set_item(key.data(), value.data());

        // Get the allocated slot
        slot = storage->key_set().find(key.data());
        if (slot == static_cast<size_t>(-1)) {
            throw std::runtime_error("dict_set: failed to insert key");
        }

        // Ensure var_lists can accommodate the slot
        if (slot >= time_list.size()) {
            time_list.resize(slot + 1);
        }
        if (slot >= observer_list.size()) {
            observer_list.resize(slot + 1);
        }
    } else {
        // Key exists - just update the value
        void* val_ptr = storage->value_at_slot(slot);
        const value::TypeMeta* value_type = storage->value_type();
        if (value_type && value_type->ops && value_type->ops->copy_assign) {
            value_type->ops->copy_assign(val_ptr, value.data(), value_type);
        }
    }

    // Update element timestamp
    time_list.at(slot).as<engine_time_t>() = current_time;

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
    elem_vd.delta_data = nullptr;
    elem_vd.ops = get_ts_ops(elem_meta);
    elem_vd.meta = elem_meta;

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

    // Manage subscription for collection-level link
    if (vd.link_data) {
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

} // namespace dict_ops

// ============================================================================
// Fixed Window Operations
// ============================================================================

namespace fixed_window_ops {

// Fixed windows use CyclicBufferStorage for values
// Time data points to a structure with:
// - engine_time_t (container last_modified_time)
// - std::vector<engine_time_t> (parallel array of timestamps)

// Storage structure for fixed window (matches TimeSeriesFixedWindowOutput layout)
struct FixedWindowData {
    value::CyclicBufferStorage* value_storage;   // From value_data
    std::vector<engine_time_t>* times;           // From time_data
    size_t capacity;                             // user_size + 1
    size_t user_size;                            // user-specified size
    size_t min_size;                             // minimum for all_valid
    bool* has_removed;                           // flag pointer
    engine_time_t* last_modified;                // container modification time
};

// Helper to get storage pointers from ViewData
// For fixed windows, the value_data holds CyclicBufferStorage
// time_data layout: [engine_time_t last_modified, size_t user_size, size_t min_size, bool has_removed, vector<times>...]
// For now, we'll work with the existing storage structure directly

engine_time_t last_modified_time(const ViewData& vd) {
    if (!vd.time_data) return MIN_ST;
    return *static_cast<engine_time_t*>(vd.time_data);
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    return last_modified_time(vd) != MIN_DT;
}

bool all_valid(const ViewData& vd) {
    if (!valid(vd)) return false;
    if (!vd.value_data || !vd.meta) return false;

    auto* storage = static_cast<const value::CyclicBufferStorage*>(vd.value_data);
    // For fixed windows, size params are in window.tick
    size_t min_size = vd.meta->window.tick.min_period;
    size_t user_size = vd.meta->window.tick.period;
    size_t current_len = std::min(storage->size, user_size);
    return current_len >= min_size;
}

bool sampled(const ViewData& vd) {
    // Return the sampled flag from ViewData
    return vd.sampled;
}

value::View value(const ViewData& vd) {
    return make_value_view(vd);
}

value::View delta_value(const ViewData& vd) {
    // TSW doesn't have standard delta tracking
    return value::View{};
}

bool has_delta(const ViewData& vd) {
    return false;
}

void set_value(ViewData& vd, const value::View& src, engine_time_t current_time) {
    throw std::runtime_error("set_value not supported for fixed window - use apply_delta");
}

void apply_delta(ViewData& vd, const value::View& delta, engine_time_t current_time) {
    // Would push to the cyclic buffer
    throw std::runtime_error("apply_delta for fixed window not yet implemented in view mode");
}

void invalidate(ViewData& vd) {
    if (vd.time_data) {
        *static_cast<engine_time_t*>(vd.time_data) = MIN_ST;
    }
    if (vd.value_data) {
        auto* storage = static_cast<value::CyclicBufferStorage*>(vd.value_data);
        storage->size = 0;
        storage->head = 0;
    }
}

nb::object to_python(const ViewData& vd) {
    if (!all_valid(vd)) return nb::none();
    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

nb::object delta_to_python(const ViewData& vd) {
    return nb::none();
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    throw std::runtime_error("from_python not supported for fixed window - use apply_delta");
}

TSView child_at(const ViewData& vd, size_t index, engine_time_t current_time) {
    // Windows don't have navigable children
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
    throw std::runtime_error("bind not supported for fixed window types");
}

void unbind(ViewData& vd) {
    throw std::runtime_error("unbind not supported for fixed window types");
}

bool is_bound(const ViewData& vd) {
    return false;
}

// Window-specific operations

const engine_time_t* window_value_times(const ViewData& vd) {
    if (!vd.delta_data) return nullptr;  // times stored in delta_data for windows
    auto* times = static_cast<const std::vector<engine_time_t>*>(vd.delta_data);
    return times->data();
}

size_t window_value_times_count(const ViewData& vd) {
    if (!vd.value_data || !vd.meta) return 0;
    auto* storage = static_cast<const value::CyclicBufferStorage*>(vd.value_data);
    return std::min(storage->size, vd.meta->window.tick.period);
}

engine_time_t window_first_modified_time(const ViewData& vd) {
    if (!vd.value_data || !vd.delta_data) return MIN_ST;
    auto* storage = static_cast<const value::CyclicBufferStorage*>(vd.value_data);
    if (storage->size == 0) return MIN_ST;
    auto* times = static_cast<const std::vector<engine_time_t>*>(vd.delta_data);
    return (*times)[storage->head];
}

bool window_has_removed_value(const ViewData& vd) {
    if (!vd.link_data) return false;  // has_removed flag stored in link_data for windows
    return *static_cast<const bool*>(vd.link_data);
}

value::View window_removed_value(const ViewData& vd) {
    if (!window_has_removed_value(vd)) return value::View{};
    if (!vd.value_data || !vd.meta) return value::View{};

    auto* storage = static_cast<const value::CyclicBufferStorage*>(vd.value_data);
    size_t capacity = vd.meta->window.tick.period + 1;
    // Removed value is at (head - 1 + capacity) % capacity
    size_t removed_pos = (storage->head + capacity - 1) % capacity;
    size_t elem_size = vd.meta->value_type->element_type->size;
    void* elem_ptr = static_cast<char*>(storage->data) + removed_pos * elem_size;
    return value::View(elem_ptr, vd.meta->value_type->element_type);
}

size_t window_removed_value_count(const ViewData& vd) {
    return window_has_removed_value(vd) ? 1 : 0;
}

size_t window_size(const ViewData& vd) {
    return vd.meta ? vd.meta->window.tick.period : 0;
}

size_t window_min_size(const ViewData& vd) {
    return vd.meta ? vd.meta->window.tick.min_period : 0;
}

size_t window_length(const ViewData& vd) {
    if (!vd.value_data || !vd.meta) return 0;
    auto* storage = static_cast<const value::CyclicBufferStorage*>(vd.value_data);
    return std::min(storage->size, vd.meta->window.tick.period);
}

void set_active(ViewData& vd, value::View active_view, bool active, TSInput* input) {
    if (!active_view) return;

    // TSW active schema is just a bool
    *static_cast<bool*>(active_view.data()) = active;

    // Manage subscription for window input if bound
    if (vd.link_data) {
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

} // namespace fixed_window_ops

// ============================================================================
// Time Window Operations
// ============================================================================

namespace time_window_ops {

// Time windows use deques for values and times
// value_data: std::deque<void*>
// time_data: engine_time_t (last_modified) followed by time window state
// delta_data: std::deque<engine_time_t>* (timestamps)
// link_data: std::vector<void*>* (removed values)

engine_time_t last_modified_time(const ViewData& vd) {
    if (!vd.time_data) return MIN_ST;
    return *static_cast<engine_time_t*>(vd.time_data);
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    return last_modified_time(vd) != MIN_DT;
}

bool all_valid(const ViewData& vd) {
    if (!valid(vd)) return false;
    if (!vd.delta_data || !vd.meta) return false;

    auto* times = static_cast<const std::deque<engine_time_t>*>(vd.delta_data);
    if (times->empty()) return false;

    engine_time_delta_t span = times->back() - times->front();
    return span >= vd.meta->window.duration.min_time_range;
}

bool sampled(const ViewData& vd) {
    // Return the sampled flag from ViewData
    return vd.sampled;
}

value::View value(const ViewData& vd) {
    return make_value_view(vd);
}

value::View delta_value(const ViewData& vd) {
    return value::View{};
}

bool has_delta(const ViewData& vd) {
    return false;
}

void set_value(ViewData& vd, const value::View& src, engine_time_t current_time) {
    throw std::runtime_error("set_value not supported for time window - use apply_delta");
}

void apply_delta(ViewData& vd, const value::View& delta, engine_time_t current_time) {
    throw std::runtime_error("apply_delta for time window not yet implemented in view mode");
}

void invalidate(ViewData& vd) {
    if (vd.time_data) {
        *static_cast<engine_time_t*>(vd.time_data) = MIN_ST;
    }
}

nb::object to_python(const ViewData& vd) {
    if (!all_valid(vd)) return nb::none();
    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

nb::object delta_to_python(const ViewData& vd) {
    return nb::none();
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    throw std::runtime_error("from_python not supported for time window - use apply_delta");
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
    throw std::runtime_error("bind not supported for time window types");
}

void unbind(ViewData& vd) {
    throw std::runtime_error("unbind not supported for time window types");
}

bool is_bound(const ViewData& vd) {
    return false;
}

// Window-specific operations

const engine_time_t* window_value_times(const ViewData& vd) {
    if (!vd.delta_data) return nullptr;
    auto* times = static_cast<const std::deque<engine_time_t>*>(vd.delta_data);
    // deque doesn't guarantee contiguous storage, but for typical use this works
    // In practice, we'd need a wrapper that copies to a vector
    return times->empty() ? nullptr : &(*times)[0];
}

size_t window_value_times_count(const ViewData& vd) {
    if (!vd.delta_data) return 0;
    auto* times = static_cast<const std::deque<engine_time_t>*>(vd.delta_data);
    return times->size();
}

engine_time_t window_first_modified_time(const ViewData& vd) {
    if (!vd.delta_data) return MIN_ST;
    auto* times = static_cast<const std::deque<engine_time_t>*>(vd.delta_data);
    return times->empty() ? MIN_ST : times->front();
}

bool window_has_removed_value(const ViewData& vd) {
    if (!vd.link_data) return false;
    auto* removed = static_cast<const std::vector<void*>*>(vd.link_data);
    return !removed->empty();
}

value::View window_removed_value(const ViewData& vd) {
    if (!window_has_removed_value(vd)) return value::View{};
    // For time windows, removed_value returns array of removed values
    // This would need a special view type for multiple elements
    return value::View{};  // Placeholder - to_python handles this differently
}

size_t window_removed_value_count(const ViewData& vd) {
    if (!vd.link_data) return 0;
    auto* removed = static_cast<const std::vector<void*>*>(vd.link_data);
    return removed->size();
}

size_t window_size(const ViewData& vd) {
    // For time windows, size is duration in microseconds
    return vd.meta ? static_cast<size_t>(vd.meta->window.duration.time_range.count()) : 0;
}

size_t window_min_size(const ViewData& vd) {
    return vd.meta ? static_cast<size_t>(vd.meta->window.duration.min_time_range.count()) : 0;
}

size_t window_length(const ViewData& vd) {
    if (!vd.delta_data) return 0;
    auto* times = static_cast<const std::deque<engine_time_t>*>(vd.delta_data);
    return times->size();
}

void set_active(ViewData& vd, value::View active_view, bool active, TSInput* input) {
    if (!active_view) return;

    // TSW active schema is just a bool
    *static_cast<bool*>(active_view.data()) = active;

    // Manage subscription for window input if bound
    if (vd.link_data) {
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

} // namespace time_window_ops

// ============================================================================
// Static ts_ops Tables
// ============================================================================

// Macro for non-window types (window ops are nullptr)
#define MAKE_TS_OPS(ns) ts_ops { \
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
    .set_active = ns::set_active, \
    .window_value_times = nullptr, \
    .window_value_times_count = nullptr, \
    .window_first_modified_time = nullptr, \
    .window_has_removed_value = nullptr, \
    .window_removed_value = nullptr, \
    .window_removed_value_count = nullptr, \
    .window_size = nullptr, \
    .window_min_size = nullptr, \
    .window_length = nullptr, \
    .set_add = nullptr, \
    .set_remove = nullptr, \
    .set_clear = nullptr, \
    .dict_remove = nullptr, \
    .dict_create = nullptr, \
    .dict_set = nullptr, \
}

// Macro for window types (includes window ops)
#define MAKE_WINDOW_TS_OPS(ns) ts_ops { \
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
    .set_active = ns::set_active, \
    .window_value_times = ns::window_value_times, \
    .window_value_times_count = ns::window_value_times_count, \
    .window_first_modified_time = ns::window_first_modified_time, \
    .window_has_removed_value = ns::window_has_removed_value, \
    .window_removed_value = ns::window_removed_value, \
    .window_removed_value_count = ns::window_removed_value_count, \
    .window_size = ns::window_size, \
    .window_min_size = ns::window_min_size, \
    .window_length = ns::window_length, \
    .set_add = nullptr, \
    .set_remove = nullptr, \
    .set_clear = nullptr, \
    .dict_remove = nullptr, \
    .dict_create = nullptr, \
    .dict_set = nullptr, \
}

// Macro for set types (includes set mutation ops)
#define MAKE_SET_TS_OPS(ns) ts_ops { \
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
    .set_active = ns::set_active, \
    .window_value_times = nullptr, \
    .window_value_times_count = nullptr, \
    .window_first_modified_time = nullptr, \
    .window_has_removed_value = nullptr, \
    .window_removed_value = nullptr, \
    .window_removed_value_count = nullptr, \
    .window_size = nullptr, \
    .window_min_size = nullptr, \
    .window_length = nullptr, \
    .set_add = ns::set_add, \
    .set_remove = ns::set_remove, \
    .set_clear = ns::set_clear, \
    .dict_remove = nullptr, \
    .dict_create = nullptr, \
    .dict_set = nullptr, \
}

// Macro for dict types (includes dict mutation ops)
#define MAKE_DICT_TS_OPS(ns) ts_ops { \
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
    .set_active = ns::set_active, \
    .window_value_times = nullptr, \
    .window_value_times_count = nullptr, \
    .window_first_modified_time = nullptr, \
    .window_has_removed_value = nullptr, \
    .window_removed_value = nullptr, \
    .window_removed_value_count = nullptr, \
    .window_size = nullptr, \
    .window_min_size = nullptr, \
    .window_length = nullptr, \
    .set_add = nullptr, \
    .set_remove = nullptr, \
    .set_clear = nullptr, \
    .dict_remove = ns::dict_remove, \
    .dict_create = ns::dict_create, \
    .dict_set = ns::dict_set, \
}

static const ts_ops scalar_ts_ops = MAKE_TS_OPS(scalar_ops);
static const ts_ops bundle_ts_ops = MAKE_TS_OPS(bundle_ops);
static const ts_ops list_ts_ops = MAKE_TS_OPS(list_ops);
static const ts_ops set_ts_ops = MAKE_SET_TS_OPS(set_ops);
static const ts_ops dict_ts_ops = MAKE_DICT_TS_OPS(dict_ops);
static const ts_ops fixed_window_ts_ops = MAKE_WINDOW_TS_OPS(fixed_window_ops);
static const ts_ops time_window_ts_ops = MAKE_WINDOW_TS_OPS(time_window_ops);

#undef MAKE_TS_OPS
#undef MAKE_SET_TS_OPS
#undef MAKE_DICT_TS_OPS
#undef MAKE_WINDOW_TS_OPS

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

} // namespace hgraph
