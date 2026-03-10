#include "ts_ops_internal.h"

#include <hgraph/types/constants.h>

#include <cstdio>
#include <cstdlib>
#include <string>

namespace hgraph {
namespace {

[[maybe_unused]] std::optional<value::View> map_key_for_index(const value::View& map_view, size_t index) {
    if (!map_view.valid() || !map_view.is_map()) {
        return std::nullopt;
    }

    auto map = map_view.as_map();
    if (const auto* storage = static_cast<const value::MapStorage*>(map.data()); storage != nullptr) {
        const auto& key_set = storage->key_set();
        if (key_set.is_alive(index)) {
            return value::View(storage->key_at_slot(index), map.key_type());
        }
    }

    return std::nullopt;
}

void append_tsd_keys_python(const value::View& value, nb::list& out, nb::set& seen) {
    if (!value.valid() || !value.is_map()) {
        return;
    }

    for (value::View key : value.as_map().keys()) {
        nb::object key_obj = key.to_python();
        if (PySet_Contains(seen.ptr(), key_obj.ptr()) == 1) {
            continue;
        }
        seen.add(key_obj);
        out.append(key_obj);
    }
}

void append_unique_key(nb::list& out, nb::set& seen, const nb::object& key_obj) {
    if (PySet_Contains(seen.ptr(), key_obj.ptr()) == 1) {
        return;
    }
    seen.add(key_obj);
    out.append(key_obj);
}

nb::list tsd_key_set_delta_keys(const TSView& view, bool added) {
    nb::list keys;
    TSView key_set_view = view;
    key_set_view.view_data().projection = ViewProjection::TSD_KEY_SET;
    nb::object key_set_delta = key_set_view.delta_to_python();
    if (key_set_delta.is_none()) {
        return keys;
    }

    const char* member_name = added ? "added" : "removed";
    nb::object member = nb::getattr(key_set_delta, member_name, nb::none());
    if (member.is_none()) {
        return keys;
    }
    if (PyCallable_Check(member.ptr()) != 0) {
        member = member();
    }
    if (member.is_none()) {
        return keys;
    }

    for (auto item_h : nb::cast<nb::iterable>(member)) {
        keys.append(nb::cast<nb::object>(item_h));
    }
    return keys;
}

}  // namespace

engine_time_t resolve_notify_time(node_ptr owner, engine_time_t fallback) {
    if (owner != nullptr) {
        if (const engine_time_t* et = owner->cached_evaluation_time_ptr(); et != nullptr && *et != MIN_DT) {
            return *et;
        }
        if (auto g = owner->graph(); g != nullptr) {
            const engine_time_t graph_time = g->evaluation_time();
            if (graph_time != MIN_DT) {
                return graph_time;
            }
        }
    }
    return fallback;
}

bool input_kind_requires_bound_validity(const TSInputView& input_view) {
    const TSMeta* meta = input_view.ts_meta();
    const TSKind kind = meta != nullptr ? meta->kind : input_view.as_ts_view().kind();
    return kind == TSKind::TSValue || kind == TSKind::SIGNAL;
}

std::optional<ViewData> resolve_input_bound_target_view_data(const TSInputView& input_view) {
    const ViewData& vd = input_view.as_ts_view().view_data();
    const TSMeta* meta = input_view.ts_meta();
    ViewData target{};
    if (meta != nullptr && meta->kind == TSKind::REF) {
        if (!resolve_direct_bound_view_data(vd, target)) {
            return std::nullopt;
        }
        return target;
    }
    if (!resolve_bound_target_view_data(vd, target)) {
        return std::nullopt;
    }
    return target;
}

bool input_has_effective_bound_target(const TSInputView& input_view) {
    if (input_view.is_bound()) {
        return true;
    }
    return resolve_input_bound_target_view_data(input_view).has_value();
}

const engine_time_t* resolve_bound_view_current_time_ptr(const ViewData& vd) {
    if (vd.engine_time_ptr != nullptr) {
        return vd.engine_time_ptr;
    }
    node_ptr owner = vd.owner_node();
    if (owner == nullptr) {
        return nullptr;
    }
    if (const engine_time_t* et = owner->cached_evaluation_time_ptr(); et != nullptr) {
        return et;
    }
    graph_ptr g = owner->graph();
    return g != nullptr ? g->cached_evaluation_time_ptr() : nullptr;
}

engine_time_t resolve_bound_view_current_time(const ViewData& vd) {
    if (const engine_time_t* et = resolve_bound_view_current_time_ptr(vd); et != nullptr && *et != MIN_DT) {
        return *et;
    }

    node_ptr owner = vd.owner_node();
    if (owner == nullptr) {
        return MIN_DT;
    }
    graph_ptr g = owner->graph();
    if (g == nullptr) {
        return MIN_DT;
    }

    const engine_time_t graph_time = g->evaluation_time();
    if (graph_time != MIN_DT) {
        return graph_time;
    }

    if (auto api = g->evaluation_engine_api(); api != nullptr) {
        return api->start_time();
    }
    return MIN_DT;
}

bool ts_meta_is_ref(const TSMeta* meta) {
    const ts_ops* ops = get_ts_ops(meta);
    return meta != nullptr && ops != nullptr && ops->kind == TSKind::REF;
}

bool ts_meta_is_bundle(const TSMeta* meta) {
    const ts_ops* ops = get_ts_ops(meta);
    return meta != nullptr && ops != nullptr && ops->bundle_ops() != nullptr;
}

bool ts_meta_is_dict(const TSMeta* meta) {
    const ts_ops* ops = get_ts_ops(meta);
    return meta != nullptr && ops != nullptr && ops->dict_ops() != nullptr;
}

const TSMeta* ts_strip_ref_meta(const TSMeta* meta) {
    while (ts_meta_is_ref(meta)) {
        meta = meta->element_ts();
    }
    return meta;
}

const TSMeta* ts_bundle_meta_with_fields(const TSView& view) {
    const TSMeta* meta = view.ts_meta();
    if (!ts_meta_is_bundle(meta) || meta->fields() == nullptr) {
        return nullptr;
    }
    return meta;
}

std::optional<size_t> ts_bundle_field_index(const TSView& view, std::string_view name) {
    const TSMeta* meta = ts_bundle_meta_with_fields(view);
    if (meta == nullptr) {
        return std::nullopt;
    }

    const size_t index = find_bundle_field_index(meta, name);
    if (index == static_cast<size_t>(-1)) {
        return std::nullopt;
    }
    return index;
}

nb::list ts_bundle_field_names(const TSView& view) {
    nb::list out;
    const TSMeta* meta = ts_bundle_meta_with_fields(view);
    if (meta == nullptr) {
        return out;
    }

    for (size_t i = 0; i < meta->field_count(); ++i) {
        out.append(nb::str(meta->fields()[i].name));
    }
    return out;
}

bool ts_list_child_effectively_modified(const TSView& child) {
    if (!child) {
        return false;
    }

    const auto* meta = child.ts_meta();
    const bool ref_valued_tsd =
        meta != nullptr &&
        meta->kind == TSKind::TSD &&
        meta->element_ts() != nullptr &&
        meta->element_ts()->kind == TSKind::REF;
    if (!ref_valued_tsd) {
        return child.modified();
    }

    value::View delta = child.delta_payload();
    if (delta.valid() && delta.is_tuple()) {
        auto tuple = delta.as_tuple();
        if (tuple.size() > 1) {
            value::View added = tuple.at(1);
            if (added.valid() && added.is_set() && added.as_set().size() > 0) {
                return true;
            }
        }
        if (tuple.size() > 2) {
            value::View removed = tuple.at(2);
            if (removed.valid() && removed.is_set() && removed.as_set().size() > 0) {
                return true;
            }
        }
    }

    return false;
}

std::vector<size_t> ts_list_filtered_indices(const TSView& view, TSCollectionFilter filter) {
    std::vector<size_t> out;
    if (!view.is_list()) {
        return out;
    }

    const size_t count = view.as_list().count();
    out.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        if (filter == TSCollectionFilter::All) {
            out.push_back(i);
            continue;
        }

        TSView child = view.child_at(i);
        if (!child) {
            continue;
        }

        if (filter == TSCollectionFilter::Valid) {
            if (child.valid()) {
                out.push_back(i);
            }
            continue;
        }

        if (ts_list_child_effectively_modified(child)) {
            out.push_back(i);
        }
    }
    return out;
}

std::vector<size_t> ts_bundle_filtered_indices(const TSView& view, TSCollectionFilter filter) {
    std::vector<size_t> out;
    const TSMeta* meta = ts_bundle_meta_with_fields(view);
    if (meta == nullptr) {
        return out;
    }

    out.reserve(meta->field_count());
    for (size_t i = 0; i < meta->field_count(); ++i) {
        if (filter == TSCollectionFilter::All) {
            out.push_back(i);
            continue;
        }

        TSView child = view.child_at(i);
        if (!child) {
            continue;
        }

        if (filter == TSCollectionFilter::Valid) {
            if (child.valid()) {
                out.push_back(i);
            }
            continue;
        }

        if (child.modified()) {
            out.push_back(i);
        }
    }
    return out;
}

value::Value tsd_key_from_python(const nb::object& key, const TSMeta* meta) {
    const auto* key_schema = meta != nullptr ? meta->key_type() : nullptr;
    if (key_schema == nullptr) {
        return {};
    }

    value::Value key_val(key_schema);
    key_val.emplace();
    key_schema->ops().from_python(key_val.data(), key, key_schema);
    return key_val;
}

value::View ts_local_navigation_value(const TSView& view) {
    if (auto local = resolve_value_slot_const(view.view_data()); local.has_value()) {
        return *local;
    }
    return {};
}

nb::list tsd_keys_python(const TSView& view, bool include_local_fallback) {
    nb::list out;
    nb::set seen;
    append_tsd_keys_python(view.value(), out, seen);
    if (include_local_fallback) {
        append_tsd_keys_python(ts_local_navigation_value(view), out, seen);
    }
    return out;
}

nb::list tsd_delta_keys_slot(const TSView& view, size_t tuple_index, bool expect_map) {
    nb::list out;
    nb::set seen;
    const auto* meta = view.ts_meta();
    const bool ref_valued =
        meta != nullptr && meta->element_ts() != nullptr && meta->element_ts()->kind == TSKind::REF;

    bool tuple_slot_compatible = false;

    value::View delta = view.delta_payload();
    if (delta.valid() && delta.is_tuple()) {
        auto tuple = delta.as_tuple();
        if (tuple_index < tuple.size()) {
            value::View slot = tuple.at(tuple_index);
            if (slot.valid()) {
                if (expect_map && slot.is_map()) {
                    tuple_slot_compatible = true;
                    for (value::View key : slot.as_map().keys()) {
                        append_unique_key(out, seen, key.to_python());
                    }
                }
                if (!expect_map && slot.is_set()) {
                    tuple_slot_compatible = true;
                    for (value::View key : slot.as_set()) {
                        append_unique_key(out, seen, key.to_python());
                    }
                }
            }
        }
    }

    const nb::list key_set_added = tsd_key_set_delta_keys(view, true);
    const nb::list key_set_removed = tsd_key_set_delta_keys(view, false);
    if (HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_TSD_MOD_KEYS_DETAIL")) {
        const std::string tuple_repr = nb::cast<std::string>(nb::repr(out));
        const std::string added_repr = nb::cast<std::string>(nb::repr(key_set_added));
        const std::string removed_repr = nb::cast<std::string>(nb::repr(key_set_removed));
        const std::string delta_repr = nb::cast<std::string>(nb::repr(view.delta_to_python()));
        std::fprintf(stderr,
                     "[tsd_delta_keys_slot] path=%s idx=%zu expect_map=%d ref_valued=%d tuple_ok=%d tuple=%s keyset_added=%s keyset_removed=%s delta=%s\n",
                     view.short_path().to_string().c_str(),
                     tuple_index,
                     expect_map ? 1 : 0,
                     ref_valued ? 1 : 0,
                     tuple_slot_compatible ? 1 : 0,
                     tuple_repr.c_str(),
                     added_repr.c_str(),
                     removed_repr.c_str(),
                     delta_repr.c_str());
    }

    // Key-set bridge deltas can expose rebind additions/removals that are not
    // present in the raw tuple slots.
    if (tuple_index == 1) {
        if (ref_valued) {
            out = nb::list{};
            seen = nb::set{};
            for (const auto& k : key_set_added) {
                append_unique_key(out, seen, nb::cast<nb::object>(k));
            }
            return out;
        }
        for (const auto& k : key_set_added) {
            append_unique_key(out, seen, nb::cast<nb::object>(k));
        }
        return out;
    }

    if (tuple_index == 2) {
        if (nb::len(key_set_removed) > 0) {
            out = nb::list{};
            seen = nb::set{};
            for (const auto& k : key_set_removed) {
                append_unique_key(out, seen, nb::cast<nb::object>(k));
            }
            return out;
        }
        if (tuple_slot_compatible) {
            return out;
        }
    }

    if (tuple_index != 0 && tuple_slot_compatible) {
        return out;
    }

    // Bridge-derived deltas can be materialized only in delta_to_python()
    // and not in the raw tuple.
    if (tuple_index != 0 && tuple_index != 2) {
        return out;
    }

    // For slot 0, derive modified keys from delta_to_python() so carry-forward
    // bridge entries (for example rebind snapshots) are preserved.
    const bool prefer_python_delta_for_slot0 = tuple_index == 0;
    const auto append_ref_added_keys_if_empty = [&]() {
        if (!prefer_python_delta_for_slot0 || !ref_valued || nb::len(out) != 0) {
            return;
        }
        for (const auto& k : key_set_added) {
            append_unique_key(out, seen, nb::cast<nb::object>(k));
        }
    };

    // If we successfully parsed a non-slot-0 tuple and it just had no entries,
    // preserve that. Only fall through when tuple data was unavailable/incompatible.
    if (!prefer_python_delta_for_slot0 && tuple_slot_compatible) {
        return out;
    }

    nb::object delta_obj = view.delta_to_python();
    if (delta_obj.is_none()) {
        append_ref_added_keys_if_empty();
        return out;
    }

    if (prefer_python_delta_for_slot0) {
        out = nb::list{};
        seen = nb::set{};
    }

    auto emit_from_pair = [tuple_index, &out, &seen](const nb::object& key_obj, const nb::object& value_obj) {
        const bool is_remove = ts_python_is_remove_marker(value_obj);
        if (tuple_index == 2 && is_remove) {
            append_unique_key(out, seen, key_obj);
        }
        if (tuple_index == 0 && !is_remove) {
            append_unique_key(out, seen, key_obj);
        }
    };

    if (nb::isinstance<nb::dict>(delta_obj)) {
        nb::dict delta_dict = nb::cast<nb::dict>(delta_obj);
        for (const auto& kv : delta_dict) {
            emit_from_pair(nb::cast<nb::object>(kv.first), nb::cast<nb::object>(kv.second));
        }
        append_ref_added_keys_if_empty();
        return out;
    }

    auto items_attr = nb::getattr(delta_obj, "items", nb::none());
    if (items_attr.is_none()) {
        append_ref_added_keys_if_empty();
        return out;
    }
    for (const auto& kv : nb::iter(items_attr())) {
        emit_from_pair(nb::cast<nb::object>(kv[0]), nb::cast<nb::object>(kv[1]));
    }
    append_ref_added_keys_if_empty();
    return out;
}

bool ts_python_is_remove_marker(const nb::object& obj) {
    auto remove = get_remove();
    auto remove_if_exists = get_remove_if_exists();
    if (obj.is(remove) || obj.is(remove_if_exists)) {
        return true;
    }

    nb::object current = nb::getattr(obj, "name", nb::none());
    for (size_t depth = 0; depth < 4 && !current.is_none(); ++depth) {
        if (current.is(remove) || current.is(remove_if_exists)) {
            return true;
        }
        if (nb::isinstance<nb::str>(current)) {
            std::string name = nb::cast<std::string>(current);
            return name == "REMOVE" || name == "REMOVE_IF_EXISTS";
        }
        current = nb::getattr(current, "name", nb::none());
    }
    return false;
}

bool ts_python_is_remove_if_exists_marker(const nb::object& obj) {
    auto remove_if_exists = get_remove_if_exists();
    if (obj.is(remove_if_exists)) {
        return true;
    }
    nb::object current = nb::getattr(obj, "name", nb::none());
    for (size_t depth = 0; depth < 4 && !current.is_none(); ++depth) {
        if (current.is(remove_if_exists)) {
            return true;
        }
        if (nb::isinstance<nb::str>(current)) {
            return nb::cast<std::string>(current) == "REMOVE_IF_EXISTS";
        }
        current = nb::getattr(current, "name", nb::none());
    }
    return false;
}

std::optional<TSView> tsd_previous_child_for_key(const TSView& parent_view, const value::View& key) {
    if (!key.valid()) {
        return std::nullopt;
    }

    if (auto removed_snapshot =
            resolve_tsd_removed_child_snapshot(parent_view.view_data(), key, parent_view.current_time());
        removed_snapshot.has_value()) {
        return removed_snapshot;
    }

    ViewData previous{};
    if (!resolve_previous_bound_target_view_data(parent_view.view_data(), previous)) {
        return std::nullopt;
    }

    TSView previous_root(previous, parent_view.view_data().engine_time_ptr);
    TSView previous_child = previous_root.as_dict().at_key(key);
    if (!previous_child) {
        return std::nullopt;
    }
    return previous_child;
}

}  // namespace hgraph
