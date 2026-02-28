#include "ts_ops_internal.h"

namespace hgraph {

const TSMeta* meta_at_path(const TSMeta* root, const std::vector<size_t>& indices) {
    const TSMeta* meta = root;
    for (size_t index : indices) {
        if (!dispatch_meta_step_child(meta, index)) {
            return nullptr;
        }
    }
    return meta;
}

void bind_view_data_ops(ViewData& vd) {
    vd.ops = get_ts_ops(vd);

#ifndef NDEBUG
    if (vd.ops == nullptr) {
        throw std::runtime_error("bind_view_data_ops: resolved null ops table");
    }

    const TSMeta* dispatch_meta = meta_at_path(vd.meta, vd.path.indices);
    if (dispatch_meta == nullptr) {
        return;
    }

    const bool meta_is_ref_wrapper = dispatch_meta_is_ref(dispatch_meta);
    const bool ops_is_ref_wrapper = dispatch_ops_is_ref_wrapper(vd.ops);
    if (meta_is_ref_wrapper != ops_is_ref_wrapper) {
        throw std::runtime_error("bind_view_data_ops: REF wrapper meta/ops mismatch");
    }

    if (dispatch_meta_is_tsd(dispatch_meta) && vd.projection != ViewProjection::TSD_KEY_SET) {
        const TSMeta* element_meta = dispatch_meta->element_ts();
        if (dispatch_meta_is_ref(element_meta)) {
            if (vd.ops->delta_to_python != &op_delta_to_python_tsd_ref ||
                vd.ops->from_python != &op_from_python_tsd_ref) {
                throw std::runtime_error("bind_view_data_ops: expected TSD[REF] ops specialisation");
            }
        } else if (dispatch_meta_is_scalar_like(element_meta)) {
            if (vd.ops->delta_to_python != &op_delta_to_python_tsd_scalar ||
                vd.ops->from_python != &op_from_python_tsd_scalar) {
                throw std::runtime_error("bind_view_data_ops: expected TSD scalar ops specialisation");
            }
        } else {
            if (vd.ops->delta_to_python != &op_delta_to_python_tsd_nested ||
                vd.ops->from_python != &op_from_python_tsd_nested) {
                throw std::runtime_error("bind_view_data_ops: expected TSD nested ops specialisation");
            }
        }
    }

    if (dispatch_meta_is_ref(dispatch_meta)) {
        const TSMeta* element_meta = dispatch_meta->element_ts();
        auto expected_ref_payload = &op_ref_payload_to_python_scalar;
        if (dispatch_meta_is_tsb(element_meta)) {
            expected_ref_payload = &op_ref_payload_to_python_bundle;
        } else if (dispatch_meta_is_fixed_tsl(element_meta)) {
            expected_ref_payload = &op_ref_payload_to_python_list;
        } else if (dispatch_meta_is_dynamic_container(element_meta)) {
            expected_ref_payload = &op_ref_payload_to_python_dynamic;
        }

        if (vd.ops->ref_payload_to_python != expected_ref_payload) {
            throw std::runtime_error("bind_view_data_ops: expected REF payload ops specialisation");
        }
    }
#endif
}

size_t find_bundle_field_index(const TSMeta* bundle_meta, std::string_view field_name) {
    if (bundle_meta == nullptr || !dispatch_meta_is_tsb(bundle_meta) || bundle_meta->fields() == nullptr) {
        return static_cast<size_t>(-1);
    }

    for (size_t i = 0; i < bundle_meta->field_count(); ++i) {
        const char* name = bundle_meta->fields()[i].name;
        if (name != nullptr && field_name == name) {
            return i;
        }
    }
    return static_cast<size_t>(-1);
}

std::optional<View> navigate_const(View view, const std::vector<size_t>& indices) {
    View current = view;
    for (size_t index : indices) {
        if (!current.valid()) {
            return std::nullopt;
        }
        if (current.is_bundle()) {
            auto bundle = current.as_bundle();
            if (index >= bundle.size()) {
                return std::nullopt;
            }
            current = bundle.at(index);
        } else if (current.is_tuple()) {
            auto tuple = current.as_tuple();
            if (index >= tuple.size()) {
                return std::nullopt;
            }
            current = tuple.at(index);
        } else if (current.is_list()) {
            auto list = current.as_list();
            if (index >= list.size()) {
                return std::nullopt;
            }
            current = list.at(index);
        } else if (current.is_map()) {
            const auto map = current.as_map();
            const auto* storage = static_cast<const value::MapStorage*>(map.data());
            if (storage == nullptr || !storage->key_set().is_alive(index)) {
                return std::nullopt;
            }
            View key(storage->key_at_slot(index), map.key_type());
            current = map.at(key);
        } else {
            return std::nullopt;
        }
    }
    return current;
}

std::optional<ValueView> navigate_mut(ValueView view, const std::vector<size_t>& indices) {
    ValueView current = view;
    for (size_t index : indices) {
        if (!current.valid()) {
            return std::nullopt;
        }
        if (current.is_bundle()) {
            auto bundle = current.as_bundle();
            if (index >= bundle.size()) {
                return std::nullopt;
            }
            current = bundle.at(index);
        } else if (current.is_tuple()) {
            auto tuple = current.as_tuple();
            if (index >= tuple.size()) {
                return std::nullopt;
            }
            current = tuple.at(index);
        } else if (current.is_list()) {
            auto list = current.as_list();
            if (index >= list.size()) {
                return std::nullopt;
            }
            current = list.at(index);
        } else if (current.is_map()) {
            auto map = current.as_map();
            auto* storage = static_cast<value::MapStorage*>(map.data());
            if (storage == nullptr || !storage->key_set().is_alive(index)) {
                return std::nullopt;
            }
            View key(storage->key_at_slot(index), map.key_type());
            current = map.at(key);
        } else {
            return std::nullopt;
        }
    }
    return current;
}

void copy_view_data(ValueView dst, const View& src) {
    if (!dst.valid() || !src.valid()) {
        return;
    }
    if (dst.schema() != src.schema()) {
        throw std::runtime_error("TS scaffolding set_value schema mismatch");
    }
    dst.schema()->ops().copy(dst.data(), src.data(), dst.schema());
}

void clear_map_slot(value::ValueView map_view) {
    if (!map_view.valid() || !map_view.is_map()) {
        return;
    }

    auto map = map_view.as_map();
    if (map.size() == 0) {
        return;
    }

    std::vector<value::Value> keys;
    keys.reserve(map.size());
    for (View key : map.keys()) {
        keys.emplace_back(key.clone());
    }
    for (const auto& key : keys) {
        map.remove(key.view());
    }
}

bool view_matches_container_kind(const std::optional<View>& value, const TSMeta* container_meta) {
    if (!value.has_value() || !value->valid()) {
        return false;
    }
    if (dispatch_meta_is_tsd(container_meta)) {
        return value->is_map();
    }
    if (dispatch_meta_is_tss(container_meta)) {
        return value->is_set();
    }
    return false;
}

bool bridge_has_container_kind_value(const ViewData& previous_bridge,
                                     const ViewData& current_bridge,
                                     const TSMeta* container_meta) {
    return view_matches_container_kind(resolve_value_slot_const(previous_bridge), container_meta) ||
           view_matches_container_kind(resolve_value_slot_const(current_bridge), container_meta);
}

bool rebind_bridge_has_container_kind_value(const ViewData& vd,
                                            const TSMeta* self_meta,
                                            engine_time_t current_time,
                                            const TSMeta* container_meta) {
    ViewData previous_bridge{};
    ViewData current_bridge{};
    return resolve_rebind_bridge_views(vd, self_meta, current_time, previous_bridge, current_bridge) &&
           bridge_has_container_kind_value(previous_bridge, current_bridge, container_meta);
}

bool is_first_bind_rebind_tick(const LinkTarget* link_target, engine_time_t current_time) {
    return link_target != nullptr &&
           link_target->is_linked &&
           !link_target->has_previous_target &&
           link_target->last_rebind_time == current_time;
}

bool resolve_container_rebind_bridge_views(const ViewData& vd,
                                           const TSMeta* container_meta,
                                           engine_time_t current_time,
                                           bool require_kind_mismatch,
                                           ViewData& previous_bridge,
                                           ViewData& current_bridge) {
    if (container_meta == nullptr ||
        (!dispatch_meta_is_tss(container_meta) && !dispatch_meta_is_tsd(container_meta))) {
        return false;
    }

    if (!resolve_rebind_bridge_views(vd, container_meta, current_time, previous_bridge, current_bridge)) {
        return false;
    }

    if (!require_kind_mismatch) {
        return true;
    }

    const TSMeta* current_bridge_meta = meta_at_path(current_bridge.meta, current_bridge.path.indices);
    return current_bridge_meta == nullptr || dispatch_meta_ops(current_bridge_meta) != dispatch_meta_ops(container_meta);
}

bool resolve_rebind_current_bridge_view(const ViewData& vd,
                                        const TSMeta* self_meta,
                                        engine_time_t current_time,
                                        ViewData& current_bridge) {
    ViewData previous_bridge{};
    return resolve_rebind_bridge_views(vd, self_meta, current_time, previous_bridge, current_bridge);
}

bool tsd_child_was_visible_before_removal(const ViewData& child_vd) {
    View child_value = op_value(child_vd);
    if (!child_value.valid()) {
        return false;
    }

    const TSMeta* child_meta = meta_at_path(child_vd.meta, child_vd.path.indices);
    const bool ref_like_child =
        dispatch_meta_is_ref(child_meta) ||
        child_value.schema() == ts_reference_meta();

    if (ref_like_child) {
        ViewData bound_target{};
        if (resolve_bound_target_view_data(child_vd, bound_target)) {
            if (op_valid(bound_target) || op_last_modified_time(bound_target) > MIN_DT) {
                return true;
            }
        }

        ViewData previous_bound_target{};
        if (resolve_previous_bound_target_view_data(child_vd, previous_bound_target)) {
            if (op_valid(previous_bound_target) || op_last_modified_time(previous_bound_target) > MIN_DT) {
                return true;
            }
        }

        try {
            TimeSeriesReference ref = nb::cast<TimeSeriesReference>(child_value.to_python());
            if (ref.is_valid()) {
                return true;
            }
            if (const ViewData* bound = ref.bound_view(); bound != nullptr) {
                if (op_valid(*bound) || op_last_modified_time(*bound) > MIN_DT) {
                    return true;
                }
            }
            return false;
        } catch (...) {
            return false;
        }
    }

    return op_valid(child_vd);
}

std::optional<ValueView> resolve_delta_slot_mut(ViewData& vd) {
    auto* delta_root = static_cast<Value*>(vd.delta_data);
    if (delta_root == nullptr || delta_root->schema() == nullptr) {
        return std::nullopt;
    }
    if (!delta_root->has_value()) {
        delta_root->emplace();
    }
    auto delta_path = ts_path_to_delta_path(vd.meta, vd.path.indices);
    if (!delta_path.has_value()) {
        return std::nullopt;
    }
    if (delta_path->empty()) {
        return delta_root->view();
    }
    return navigate_mut(delta_root->view(), *delta_path);
}

TSSDeltaSlots resolve_tss_delta_slots(ViewData& vd) {
    TSSDeltaSlots out{};
    auto maybe_slot = resolve_delta_slot_mut(vd);
    if (!maybe_slot.has_value()) {
        return out;
    }

    out.slot = *maybe_slot;
    if (!out.slot.valid() || !out.slot.is_tuple()) {
        return out;
    }

    auto tuple = out.slot.as_tuple();
    if (tuple.size() > 0) {
        out.added_set = tuple.at(0);
    }
    if (tuple.size() > 1) {
        out.removed_set = tuple.at(1);
    }
    return out;
}

TSDDeltaSlots resolve_tsd_delta_slots(ViewData& vd) {
    TSDDeltaSlots out{};
    auto maybe_slot = resolve_delta_slot_mut(vd);
    if (!maybe_slot.has_value()) {
        return out;
    }

    out.slot = *maybe_slot;
    if (!out.slot.valid() || !out.slot.is_tuple()) {
        return out;
    }

    auto tuple = out.slot.as_tuple();
    if (tuple.size() > 0) {
        out.changed_values_map = tuple.at(0);
    }
    if (tuple.size() > 1) {
        out.added_set = tuple.at(1);
    }
    if (tuple.size() > 2) {
        out.removed_set = tuple.at(2);
    }
    return out;
}

TSWTickDeltaSlots resolve_tsw_tick_delta_slots(ViewData& vd) {
    TSWTickDeltaSlots out{};
    auto maybe_slot = resolve_delta_slot_mut(vd);
    if (!maybe_slot.has_value()) {
        return out;
    }

    out.slot = *maybe_slot;
    if (!out.slot.valid() || !out.slot.is_tuple()) {
        return out;
    }

    auto tuple = out.slot.as_tuple();
    if (tuple.size() > 0) {
        out.removed_value = tuple.at(0);
    }
    if (tuple.size() > 1) {
        out.has_removed = tuple.at(1);
    }
    return out;
}

TSWDurationDeltaSlots resolve_tsw_duration_delta_slots(ViewData& vd) {
    TSWDurationDeltaSlots out{};
    auto maybe_slot = resolve_delta_slot_mut(vd);
    if (!maybe_slot.has_value()) {
        return out;
    }

    out.slot = *maybe_slot;
    if (!out.slot.valid() || !out.slot.is_tuple()) {
        return out;
    }

    auto tuple = out.slot.as_tuple();
    if (tuple.size() > 0) {
        out.has_removed = tuple.at(0);
    }
    if (tuple.size() > 1) {
        out.removed_values = tuple.at(1);
    }
    return out;
}

bool set_view_empty(ValueView v) {
    return !v.valid() || !v.is_set() || v.as_set().size() == 0;
}

bool map_view_empty(ValueView v) {
    return !v.valid() || !v.is_map() || v.as_map().size() == 0;
}

bool has_delta_descendants(const TSMeta* meta) {
    if (meta == nullptr) {
        return false;
    }

    if (dispatch_meta_is_tss(meta) || dispatch_meta_is_tsd(meta) || dispatch_meta_is_tsw(meta)) {
        return true;
    }

    if (dispatch_meta_is_tsb(meta)) {
        for (size_t i = 0; i < meta->field_count(); ++i) {
            if (has_delta_descendants(meta->fields()[i].ts_type)) {
                return true;
            }
        }
        return false;
    }

    if (dispatch_meta_is_tsl(meta)) {
        return has_delta_descendants(meta->element_ts());
    }

    return false;
}

std::optional<std::vector<size_t>> ts_path_to_delta_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    if (root_meta == nullptr || !has_delta_descendants(root_meta)) {
        return std::nullopt;
    }

    std::vector<size_t> out;
    const TSMeta* current = root_meta;

    for (size_t index : ts_path) {
        while (dispatch_meta_is_ref(current)) {
            current = current->element_ts();
        }

        if (current == nullptr) {
            return std::nullopt;
        }

        if (dispatch_meta_is_tsb(current)) {
            if (current->fields() == nullptr || index >= current->field_count()) {
                return std::nullopt;
            }
            const TSMeta* child = current->fields()[index].ts_type;
            if (!has_delta_descendants(child)) {
                return std::nullopt;
            }
            out.push_back(index);
            current = child;
            continue;
        }

        if (dispatch_meta_is_tsl(current)) {
            const TSMeta* child = current->element_ts();
            if (!has_delta_descendants(child)) {
                return std::nullopt;
            }
            out.push_back(index);
            current = child;
            continue;
        }

        if (dispatch_meta_is_tsd(current)) {
            const TSMeta* child = current->element_ts();
            if (!has_delta_descendants(child)) {
                return std::nullopt;
            }
            out.push_back(3);
            out.push_back(index);
            current = child;
            continue;
        }

        return std::nullopt;
    }

    return out;
}

const value::MapStorage* map_storage_for_read(const value::MapView& map) {
    if (!map.valid() || !map.is_map()) {
        return nullptr;
    }
    return static_cast<const value::MapStorage*>(map.data());
}

std::optional<size_t> map_slot_for_key(const value::MapView& map, const View& key) {
    if (!key.valid()) {
        return std::nullopt;
    }
    const auto* storage = map_storage_for_read(map);
    if (storage == nullptr) {
        return std::nullopt;
    }

    // Strict schema-typed lookup only: mismatched key schemas are treated as
    // not found to avoid masking conversion/type bugs.
    if (key.schema() != map.key_type()) {
        return std::nullopt;
    }

    const size_t slot = storage->key_set().find(key.data());
    if (slot == static_cast<size_t>(-1)) {
        return std::nullopt;
    }
    return slot;
}

bool set_contains_key_relaxed(const value::SetView& set, const View& key) {
    if (!key.valid()) {
        return false;
    }
    if (key.schema() != set.element_type()) {
        return false;
    }
    return set.contains(key);
}

bool view_is_set_and_contains_key_relaxed(const View& maybe_set, const View& key) {
    return maybe_set.valid() && maybe_set.is_set() && set_contains_key_relaxed(maybe_set.as_set(), key);
}

std::optional<Value> map_key_at_slot(const value::MapView& map, size_t slot_index) {
    const auto* storage = map_storage_for_read(map);
    if (storage == nullptr || !storage->key_set().is_alive(slot_index)) {
        return std::nullopt;
    }
    View key(storage->key_at_slot(slot_index), map.key_type());
    return key.clone();
}

Value canonical_map_key_for_slot(const value::MapView& map, size_t slot_index, const View& fallback_key) {
    if (auto key_value = map_key_at_slot(map, slot_index); key_value.has_value()) {
        return std::move(*key_value);
    }
    return fallback_key.clone();
}

void mark_tsd_parent_child_modified(ViewData child_vd, engine_time_t current_time) {
    if (child_vd.path.indices.empty()) {
        return;
    }

    const std::vector<size_t> full_path = child_vd.path.indices;
    for (size_t depth = 0; depth < full_path.size(); ++depth) {
        std::vector<size_t> tsd_path(full_path.begin(), full_path.begin() + depth);
        const TSMeta* parent_meta = meta_at_path(child_vd.meta, tsd_path);
        if (!dispatch_meta_is_tsd(parent_meta)) {
            continue;
        }

        ViewData tsd_vd = child_vd;
        tsd_vd.path.indices = std::move(tsd_path);
        const size_t child_slot = full_path[depth];

        auto maybe_parent_value = resolve_value_slot_const(tsd_vd);
        if (!maybe_parent_value.has_value() || !maybe_parent_value->valid() || !maybe_parent_value->is_map()) {
            continue;
        }

        auto parent_map = maybe_parent_value->as_map();
        auto maybe_key = map_key_at_slot(parent_map, child_slot);
        if (!maybe_key.has_value()) {
            continue;
        }
        const View key = maybe_key->view();

        ViewData tsd_child_vd = tsd_vd;
        tsd_child_vd.path.indices.push_back(child_slot);

        auto slots = resolve_tsd_delta_slots(tsd_vd);
        clear_tsd_delta_if_new_tick(tsd_vd, current_time, slots);

        if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
            View child_value = op_value(tsd_child_vd);
            if (child_value.valid()) {
                slots.changed_values_map.as_map().set(key, child_value);
            } else {
                slots.changed_values_map.as_map().remove(key);
            }
        }

        if (slots.removed_set.valid() && slots.removed_set.is_set()) {
            slots.removed_set.as_set().remove(key);
        }
    }
}

bool tss_delta_empty(const TSSDeltaSlots& slots) {
    return set_view_empty(slots.added_set) && set_view_empty(slots.removed_set);
}

bool tsd_delta_empty(const TSDDeltaSlots& slots) {
    return map_view_empty(slots.changed_values_map) && set_view_empty(slots.added_set) && set_view_empty(slots.removed_set);
}

void clear_tss_delta_slots(TSSDeltaSlots slots) {
    if (slots.added_set.valid() && slots.added_set.is_set()) {
        slots.added_set.as_set().clear();
    }
    if (slots.removed_set.valid() && slots.removed_set.is_set()) {
        slots.removed_set.as_set().clear();
    }
}

void clear_tsd_delta_slots(TSDDeltaSlots slots) {
    clear_map_slot(slots.changed_values_map);
    if (slots.added_set.valid() && slots.added_set.is_set()) {
        slots.added_set.as_set().clear();
    }
    if (slots.removed_set.valid() && slots.removed_set.is_set()) {
        slots.removed_set.as_set().clear();
    }
    if (slots.slot.valid() && slots.slot.is_tuple()) {
        auto tuple = slots.slot.as_tuple();
        if (tuple.size() > 3) {
            ValueView child_deltas = tuple.at(3);
            if (child_deltas.valid() && child_deltas.is_list()) {
                child_deltas.as_list().clear();
            }
        }
    }
}

void clear_tsw_tick_delta_slots(TSWTickDeltaSlots slots) {
    if (slots.has_removed.valid() && slots.has_removed.is_scalar_type<bool>()) {
        slots.has_removed.as<bool>() = false;
    }
}

void clear_tsw_duration_delta_slots(TSWDurationDeltaSlots slots) {
    if (slots.has_removed.valid() && slots.has_removed.is_scalar_type<bool>()) {
        slots.has_removed.as<bool>() = false;
    }
    if (slots.removed_values.valid() && slots.removed_values.is_queue()) {
        auto removed_values = slots.removed_values.as_queue();
        value::QueueOps::clear(removed_values.data(), removed_values.schema());
    }
}

void clear_tss_delta_if_new_tick(ViewData& vd, engine_time_t current_time, TSSDeltaSlots slots) {
    if (!slots.slot.valid()) {
        return;
    }
    if (direct_last_modified_time(vd) < current_time) {
        clear_tss_delta_slots(slots);
    }
}

void clear_tsd_delta_if_new_tick(ViewData& vd, engine_time_t current_time, TSDDeltaSlots slots) {
    if (!slots.slot.valid()) {
        return;
    }
    if (direct_last_modified_time(vd) < current_time) {
        clear_tsd_delta_slots(slots);
    }
}

void clear_tsw_delta_if_new_tick(ViewData& vd, engine_time_t current_time) {
    if (direct_last_modified_time(vd) >= current_time) {
        return;
    }

    bind_view_data_ops(vd);
    const ts_ops* self_ops = vd.ops;
    if (self_ops == nullptr || self_ops->delta_value == nullptr) {
        return;
    }

    if (self_ops->delta_value == &op_delta_value_tsw_duration) {
        clear_tsw_duration_delta_slots(resolve_tsw_duration_delta_slots(vd));
        return;
    }

    if (self_ops->delta_value == &op_delta_value_tsw_tick) {
        clear_tsw_tick_delta_slots(resolve_tsw_tick_delta_slots(vd));
        return;
    }

    if (self_ops->delta_value == &op_delta_value_tsw) {
        // Generic TSW ops path (should not be selected on bound views): clear both
        // shapes defensively and rely on slot validation.
        clear_tsw_tick_delta_slots(resolve_tsw_tick_delta_slots(vd));
        clear_tsw_duration_delta_slots(resolve_tsw_duration_delta_slots(vd));
    }
}

std::optional<Value> value_from_python(const value::TypeMeta* type, const nb::object& src) {
    if (type == nullptr) {
        return std::nullopt;
    }
    Value out(type);
    out.emplace();
    type->ops().from_python(out.data(), src, type);
    return out;
}

nb::object attr_or_call(const nb::object& obj, const char* name) {
    nb::object attr = nb::getattr(obj, name, nb::none());
    if (attr.is_none()) {
        return nb::none();
    }
    if (nb::hasattr(attr, "__call__")) {
        return attr();
    }
    return attr;
}

nb::object python_set_delta(const nb::object& added, const nb::object& removed) {
    auto PythonSetDelta = nb::module_::import_("hgraph._impl._types._tss").attr("PythonSetDelta");
    return PythonSetDelta(added, removed);
}



}  // namespace hgraph
