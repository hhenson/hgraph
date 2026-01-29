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
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/value/map_storage.h>
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

// Get LinkTarget pointer from link_data (for TSL/TSD)
inline LinkTarget* get_link_target(void* link_data) {
    return link_data ? static_cast<LinkTarget*>(link_data) : nullptr;
}

inline const LinkTarget* get_link_target(const void* link_data) {
    return link_data ? static_cast<const LinkTarget*>(link_data) : nullptr;
}

// Create ViewData from a LinkTarget
inline ViewData make_view_data_from_link(const LinkTarget& lt, const ShortPath& path) {
    ViewData vd;
    vd.path = path;
    vd.value_data = lt.value_data;
    vd.time_data = lt.time_data;
    vd.observer_data = lt.observer_data;
    vd.delta_data = lt.delta_data;
    vd.link_data = lt.link_data;
    vd.ops = lt.ops;
    vd.meta = lt.meta;
    return vd;
}

// Store ViewData into a LinkTarget
inline void store_link_target(LinkTarget& lt, const ViewData& target) {
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

engine_time_t last_modified_time(const ViewData& vd) {
    if (!vd.time_data) return MIN_ST;
    return *static_cast<engine_time_t*>(vd.time_data);
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    return last_modified_time(vd) != MIN_ST;
}

bool all_valid(const ViewData& vd) {
    // Scalar has no children
    return valid(vd);
}

bool sampled(const ViewData& vd) {
    // TODO: Implement for REF support
    return false;
}

value::View value(const ViewData& vd) {
    return make_value_view(vd);
}

value::View delta_value(const ViewData& vd) {
    // Scalar types (TSValue) don't have delta
    return value::View{};
}

bool has_delta(const ViewData& vd) {
    return false;
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
    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

nb::object delta_to_python(const ViewData& vd) {
    // Scalar types don't have delta
    return nb::none();
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
    // Scalar types don't support binding at this level
    // Binding is managed by the parent container
    throw std::runtime_error("bind not supported for scalar types");
}

void unbind(ViewData& vd) {
    // Scalar types don't support unbinding
    throw std::runtime_error("unbind not supported for scalar types");
}

bool is_bound(const ViewData& vd) {
    // Scalar types are never bound at this level
    return false;
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
// - link is fixed_list[LinkTarget] with one entry per field

// Helper: Get LinkTarget for a specific field index
inline LinkTarget* get_field_link_target(const ViewData& vd, size_t field_index) {
    if (!vd.link_data || !vd.meta || field_index >= vd.meta->field_count) {
        return nullptr;
    }
    auto link_schema = TSMetaSchemaCache::instance().get_link_schema(vd.meta);
    if (!link_schema) return nullptr;

    value::View link_view(vd.link_data, link_schema);
    auto link_list = link_view.as_list();
    if (field_index >= link_list.size()) return nullptr;

    return static_cast<LinkTarget*>(link_list.at(field_index).data());
}

// Helper: Check if any field is linked
inline bool any_field_linked(const ViewData& vd) {
    if (!vd.link_data || !vd.meta) return false;
    auto link_schema = TSMetaSchemaCache::instance().get_link_schema(vd.meta);
    if (!link_schema) return false;

    value::View link_view(vd.link_data, link_schema);
    auto link_list = link_view.as_list();
    for (size_t i = 0; i < link_list.size(); ++i) {
        auto* lt = static_cast<const LinkTarget*>(link_list.at(i).data());
        if (lt && lt->is_linked) {
            return true;
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
    return last_modified_time(vd) != MIN_ST;
}

bool all_valid(const ViewData& vd) {
    if (!valid(vd)) return false;

    // Check all fields
    if (!vd.meta) return false;
    auto time_view = make_time_view(vd);
    if (!time_view.valid()) return false;

    for (size_t i = 0; i < vd.meta->field_count; ++i) {
        const TSMeta* field_meta = vd.meta->fields[i].ts_type;
        auto ft = time_view.as_tuple().at(i + 1);

        engine_time_t field_time;
        if (field_meta->is_scalar_ts()) {
            field_time = ft.as<engine_time_t>();
        } else {
            field_time = ft.as_tuple().at(0).as<engine_time_t>();
        }

        if (field_time == MIN_ST) return false;
    }

    return true;
}

bool sampled(const ViewData& vd) {
    return false;
}

value::View value(const ViewData& vd) {
    return make_value_view(vd);
}

value::View delta_value(const ViewData& vd) {
    // TSB doesn't have delta
    return value::View{};
}

bool has_delta(const ViewData& vd) {
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
    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

nb::object delta_to_python(const ViewData& vd) {
    return nb::none();
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
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
    if (!vd.meta || index >= vd.meta->field_count) {
        return TSView{};
    }

    // Check if this field is linked
    if (auto* lt = get_field_link_target(vd, index)) {
        if (lt->valid()) {
            // Field is linked - return TSView pointing to target
            ViewData target_vd = make_view_data_from_link(*lt, vd.path.child(index));
            return TSView(target_vd, current_time);
        }
    }

    const TSMeta* field_meta = vd.meta->fields[index].ts_type;

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

    // For TSB, link_data points to fixed_list[LinkTarget]
    // Bind each field to the corresponding field in target
    value::View link_view(vd.link_data, link_schema);
    auto link_list = link_view.as_list();

    // Get target field data for each field
    for (size_t i = 0; i < link_list.size() && i < vd.meta->field_count; ++i) {
        auto* lt = static_cast<LinkTarget*>(link_list.at(i).data());
        if (lt) {
            // Navigate to target's field and store that
            TSView target_field = target.ops->child_at(target, i, MIN_ST);
            if (target_field.valid()) {
                store_link_target(*lt, target_field.view_data());
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
    auto link_list = link_view.as_list();
    for (size_t i = 0; i < link_list.size(); ++i) {
        auto* lt = static_cast<LinkTarget*>(link_list.at(i).data());
        if (lt) {
            lt->clear();
        }
    }
}

bool is_bound(const ViewData& vd) {
    // TSB is considered bound if any field is bound
    return any_field_linked(vd);
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
// - link is LinkTarget (stores target ViewData when bound)

// Helper: Check if this TSL is linked and get the target ViewData
inline const LinkTarget* get_active_link(const ViewData& vd) {
    auto* lt = get_link_target(vd.link_data);
    return (lt && lt->valid()) ? lt : nullptr;
}

engine_time_t last_modified_time(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->last_modified_time(make_view_data_from_link(*lt, vd.path));
    }
    auto time_view = make_time_view(vd);
    if (!time_view.valid()) return MIN_ST;
    return time_view.as_tuple().at(0).as<engine_time_t>();
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->modified(make_view_data_from_link(*lt, vd.path), current_time);
    }
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->valid(make_view_data_from_link(*lt, vd.path));
    }
    return last_modified_time(vd) != MIN_ST;
}

bool all_valid(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->all_valid(make_view_data_from_link(*lt, vd.path));
    }
    if (!valid(vd)) return false;
    if (!vd.meta) return false;

    auto time_view = make_time_view(vd);
    if (!time_view.valid()) return false;

    const TSMeta* elem_meta = vd.meta->element_ts;
    auto times_list = time_view.as_tuple().at(1).as_list();

    for (size_t i = 0; i < times_list.size(); ++i) {
        auto et = times_list.at(i);
        engine_time_t elem_time;
        if (elem_meta->is_scalar_ts()) {
            elem_time = et.as<engine_time_t>();
        } else {
            elem_time = et.as_tuple().at(0).as<engine_time_t>();
        }
        if (elem_time == MIN_ST) return false;
    }

    return true;
}

bool sampled(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->sampled(make_view_data_from_link(*lt, vd.path));
    }
    return false;
}

value::View value(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->value(make_view_data_from_link(*lt, vd.path));
    }
    return make_value_view(vd);
}

value::View delta_value(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->delta_value(make_view_data_from_link(*lt, vd.path));
    }
    return value::View{};
}

bool has_delta(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->has_delta(make_view_data_from_link(*lt, vd.path));
    }
    return false;
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
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->to_python(make_view_data_from_link(*lt, vd.path));
    }
    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

nb::object delta_to_python(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->delta_to_python(make_view_data_from_link(*lt, vd.path));
    }
    return nb::none();
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    // If linked, delegate to target (write through to target)
    if (auto* lt = get_link_target(vd.link_data)) {
        if (lt->valid()) {
            ViewData target_vd = make_view_data_from_link(*lt, vd.path);
            lt->ops->from_python(target_vd, src, current_time);
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
    if (auto* lt = get_active_link(vd)) {
        ViewData target_vd = make_view_data_from_link(*lt, vd.path);
        return lt->ops->child_at(target_vd, index, current_time);
    }

    if (!vd.meta || !vd.meta->element_ts) return TSView{};

    auto value_view = make_value_view(vd);
    auto time_view = make_time_view(vd);
    auto observer_view = make_observer_view(vd);

    auto value_list = value_view.as_list();
    if (index >= value_list.size()) return TSView{};

    const TSMeta* elem_meta = vd.meta->element_ts;

    ViewData elem_vd;
    elem_vd.path = vd.path.child(index);
    elem_vd.value_data = value_list.at(index).data();
    elem_vd.time_data = time_view.as_tuple().at(1).as_list().at(index).data();
    elem_vd.observer_data = observer_view.as_tuple().at(1).as_list().at(index).data();
    elem_vd.delta_data = nullptr;
    elem_vd.ops = get_ts_ops(elem_meta);
    elem_vd.meta = elem_meta;

    return TSView(elem_vd, current_time);
}

TSView child_by_name(const ViewData& vd, const std::string& name, engine_time_t current_time) {
    // If linked, navigate through target
    if (auto* lt = get_active_link(vd)) {
        ViewData target_vd = make_view_data_from_link(*lt, vd.path);
        return lt->ops->child_by_name(target_vd, name, current_time);
    }
    // Lists don't have named children
    return TSView{};
}

TSView child_by_key(const ViewData& vd, const value::View& key, engine_time_t current_time) {
    // If linked, navigate through target
    if (auto* lt = get_active_link(vd)) {
        ViewData target_vd = make_view_data_from_link(*lt, vd.path);
        return lt->ops->child_by_key(target_vd, key, current_time);
    }
    // Lists don't support key access
    return TSView{};
}

size_t child_count(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->child_count(make_view_data_from_link(*lt, vd.path));
    }
    auto value_view = make_value_view(vd);
    if (!value_view.valid()) return 0;
    return value_view.as_list().size();
}

value::View observer(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->observer(make_view_data_from_link(*lt, vd.path));
    }
    return make_observer_view(vd);
}

void notify_observers(ViewData& vd, engine_time_t current_time) {
    // If linked, delegate to target
    if (auto* lt = get_link_target(vd.link_data)) {
        if (lt->valid()) {
            ViewData target_vd = make_view_data_from_link(*lt, vd.path);
            lt->ops->notify_observers(target_vd, current_time);
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

    // For TSL, link_data points to a LinkTarget
    auto* lt = get_link_target(vd.link_data);
    if (!lt) {
        throw std::runtime_error("bind on list with invalid link data");
    }

    // Store the target ViewData in the LinkTarget
    store_link_target(*lt, target);
}

void unbind(ViewData& vd) {
    if (!vd.link_data) {
        return;  // No-op if not linked
    }

    auto* lt = get_link_target(vd.link_data);
    if (lt) {
        lt->clear();
    }
}

bool is_bound(const ViewData& vd) {
    auto* lt = get_link_target(vd.link_data);
    return lt && lt->is_linked;
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
    return last_modified_time(vd) != MIN_ST;
}

bool all_valid(const ViewData& vd) {
    return valid(vd);
}

bool sampled(const ViewData& vd) {
    return false;
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
    // TODO: Implement proper delta application for sets
    throw std::runtime_error("apply_delta for TSS not yet implemented");
}

void invalidate(ViewData& vd) {
    if (vd.time_data) {
        *static_cast<engine_time_t*>(vd.time_data) = MIN_ST;
    }
}

nb::object to_python(const ViewData& vd) {
    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

nb::object delta_to_python(const ViewData& vd) {
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
// - link is LinkTarget (stores target ViewData when bound)

// Helper: Check if this TSD is linked and get the target ViewData
inline const LinkTarget* get_active_link(const ViewData& vd) {
    auto* lt = get_link_target(vd.link_data);
    return (lt && lt->valid()) ? lt : nullptr;
}

engine_time_t last_modified_time(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->last_modified_time(make_view_data_from_link(*lt, vd.path));
    }
    auto time_view = make_time_view(vd);
    if (!time_view.valid()) return MIN_ST;
    return time_view.as_tuple().at(0).as<engine_time_t>();
}

bool modified(const ViewData& vd, engine_time_t current_time) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->modified(make_view_data_from_link(*lt, vd.path), current_time);
    }
    return last_modified_time(vd) >= current_time;
}

bool valid(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->valid(make_view_data_from_link(*lt, vd.path));
    }
    return last_modified_time(vd) != MIN_ST;
}

bool all_valid(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->all_valid(make_view_data_from_link(*lt, vd.path));
    }
    if (!valid(vd)) return false;
    // TODO: Check all value entries
    return true;
}

bool sampled(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->sampled(make_view_data_from_link(*lt, vd.path));
    }
    return false;
}

value::View value(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->value(make_view_data_from_link(*lt, vd.path));
    }
    return make_value_view(vd);
}

value::View delta_value(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->delta_value(make_view_data_from_link(*lt, vd.path));
    }
    return make_delta_view(vd);
}

bool has_delta(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->has_delta(make_view_data_from_link(*lt, vd.path));
    }
    return vd.delta_data != nullptr;
}

void set_value(ViewData& vd, const value::View& src, engine_time_t current_time) {
    // If linked, delegate to target (write through)
    if (auto* lt = get_link_target(vd.link_data)) {
        if (lt->valid()) {
            ViewData target_vd = make_view_data_from_link(*lt, vd.path);
            lt->ops->set_value(target_vd, src, current_time);
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
    if (auto* lt = get_link_target(vd.link_data)) {
        if (lt->valid()) {
            ViewData target_vd = make_view_data_from_link(*lt, vd.path);
            lt->ops->apply_delta(target_vd, delta, current_time);
            return;
        }
    }
    // TODO: Implement proper delta application for dicts
    throw std::runtime_error("apply_delta for TSD not yet implemented");
}

void invalidate(ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_link_target(vd.link_data)) {
        if (lt->valid()) {
            ViewData target_vd = make_view_data_from_link(*lt, vd.path);
            lt->ops->invalidate(target_vd);
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
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->to_python(make_view_data_from_link(*lt, vd.path));
    }
    auto v = make_value_view(vd);
    if (!v.valid()) return nb::none();
    return v.to_python();
}

nb::object delta_to_python(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->delta_to_python(make_view_data_from_link(*lt, vd.path));
    }
    auto d = make_delta_view(vd);
    if (!d.valid()) return nb::none();
    return d.to_python();
}

void from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    // If linked, delegate to target (write through)
    if (auto* lt = get_link_target(vd.link_data)) {
        if (lt->valid()) {
            ViewData target_vd = make_view_data_from_link(*lt, vd.path);
            lt->ops->from_python(target_vd, src, current_time);
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
    if (auto* lt = get_active_link(vd)) {
        ViewData target_vd = make_view_data_from_link(*lt, vd.path);
        return lt->ops->child_at(target_vd, slot, current_time);
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
    elem_vd.ops = get_ts_ops(elem_meta);
    elem_vd.meta = elem_meta;

    return TSView(elem_vd, current_time);
}

TSView child_by_name(const ViewData& vd, const std::string& name, engine_time_t current_time) {
    // If linked, navigate through target
    if (auto* lt = get_active_link(vd)) {
        ViewData target_vd = make_view_data_from_link(*lt, vd.path);
        return lt->ops->child_by_name(target_vd, name, current_time);
    }
    // TSD uses keys, not names - would need key type conversion
    return TSView{};
}

TSView child_by_key(const ViewData& vd, const value::View& key, engine_time_t current_time) {
    // If linked, navigate through target
    if (auto* lt = get_active_link(vd)) {
        ViewData target_vd = make_view_data_from_link(*lt, vd.path);
        return lt->ops->child_by_key(target_vd, key, current_time);
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
    elem_vd.ops = get_ts_ops(elem_meta);
    elem_vd.meta = elem_meta;

    return TSView(elem_vd, current_time);
}

size_t child_count(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->child_count(make_view_data_from_link(*lt, vd.path));
    }
    auto value_view = make_value_view(vd);
    if (!value_view.valid()) return 0;
    return value_view.as_map().size();
}

value::View observer(const ViewData& vd) {
    // If linked, delegate to target
    if (auto* lt = get_active_link(vd)) {
        return lt->ops->observer(make_view_data_from_link(*lt, vd.path));
    }
    return make_observer_view(vd);
}

void notify_observers(ViewData& vd, engine_time_t current_time) {
    // If linked, delegate to target
    if (auto* lt = get_link_target(vd.link_data)) {
        if (lt->valid()) {
            ViewData target_vd = make_view_data_from_link(*lt, vd.path);
            lt->ops->notify_observers(target_vd, current_time);
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

    // For TSD, link_data points to a LinkTarget
    auto* lt = get_link_target(vd.link_data);
    if (!lt) {
        throw std::runtime_error("bind on dict with invalid link data");
    }

    // Store the target ViewData in the LinkTarget
    store_link_target(*lt, target);
}

void unbind(ViewData& vd) {
    if (!vd.link_data) {
        return;  // No-op if not linked
    }

    auto* lt = get_link_target(vd.link_data);
    if (lt) {
        lt->clear();
    }
}

bool is_bound(const ViewData& vd) {
    auto* lt = get_link_target(vd.link_data);
    return lt && lt->is_linked;
}

} // namespace dict_ops

// ============================================================================
// Static ts_ops Tables
// ============================================================================

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
}

static const ts_ops scalar_ts_ops = MAKE_TS_OPS(scalar_ops);
static const ts_ops bundle_ts_ops = MAKE_TS_OPS(bundle_ops);
static const ts_ops list_ts_ops = MAKE_TS_OPS(list_ops);
static const ts_ops set_ts_ops = MAKE_TS_OPS(set_ops);
static const ts_ops dict_ts_ops = MAKE_TS_OPS(dict_ops);

#undef MAKE_TS_OPS

// ============================================================================
// get_ts_ops Implementation
// ============================================================================

const ts_ops* get_ts_ops(TSKind kind) {
    switch (kind) {
        case TSKind::TSValue:
        case TSKind::TSW:
        case TSKind::SIGNAL:
        case TSKind::REF:
            return &scalar_ts_ops;

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

} // namespace hgraph
