#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/py_ts_runtime_internal.h>
#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/time_series/ts_ops.h>

#include <fmt/format.h>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <type_traits>
#include <vector>

namespace hgraph
{
namespace
{
    engine_time_t resolve_runtime_time_for_output(const TSOutputView& view) {
        const ViewData& vd = view.as_ts_view().view_data();
        if (node_ptr owner = vd.path.node; owner != nullptr) {
            if (const engine_time_t* et = owner->cached_evaluation_time_ptr(); et != nullptr && *et != MIN_DT) {
                return *et;
            }
        }
        return view.current_time();
    }

    engine_time_t resolve_runtime_time_for_input(const TSInputView& view) {
        const ViewData& vd = view.as_ts_view().view_data();
        if (node_ptr owner = vd.path.node; owner != nullptr) {
            if (const engine_time_t* et = owner->cached_evaluation_time_ptr(); et != nullptr && *et != MIN_DT) {
                return *et;
            }
        }
        return view.current_time();
    }

    bool is_remove_marker(const nb::object &obj);

    value::Value key_from_python_with_meta(const nb::object &key, const TSMeta *meta) {
        const auto *key_schema = meta != nullptr ? meta->key_type() : nullptr;
        if (key_schema == nullptr) {
            return {};
        }

        value::Value key_val(key_schema);
        key_val.emplace();
        key_schema->ops().from_python(key_val.data(), key, key_schema);
        return key_val;
    }

    nb::list tsd_keys_python(const TSView &view) {
        nb::list out;
        value::View value = view.value();
        if (!value.valid() || !value.is_map()) {
            return out;
        }
        for (value::View key : value.as_map().keys()) {
            out.append(key.to_python());
        }
        return out;
    }

    nb::list tsd_delta_keys_slot(const TSView &view, size_t tuple_index, bool expect_map) {
        nb::list out;
        nb::set seen;
        const auto *meta = view.ts_meta();
        const bool ref_valued =
            meta != nullptr && meta->element_ts() != nullptr && meta->element_ts()->kind == TSKind::REF;
        auto append_unique = [&](const nb::object& key_obj) {
            if (PySet_Contains(seen.ptr(), key_obj.ptr()) == 1) {
                return;
            }
            seen.add(key_obj);
            out.append(key_obj);
        };

        auto key_set_delta_keys = [&](bool added) {
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
        };

        bool tuple_slot_compatible = false;

        value::View delta = view.delta_value();
        if (delta.valid() && delta.is_tuple()) {
            auto tuple = delta.as_tuple();
            if (tuple_index < tuple.size()) {
                value::View slot = tuple.at(tuple_index);
                if (slot.valid()) {
                    if (expect_map && slot.is_map()) {
                        tuple_slot_compatible = true;
                        for (value::View key : slot.as_map().keys()) {
                            append_unique(key.to_python());
                        }
                    }
                    if (!expect_map && slot.is_set()) {
                        tuple_slot_compatible = true;
                        for (value::View key : slot.as_set()) {
                            append_unique(key.to_python());
                        }
                    }
                }
            }
        }

        const nb::list key_set_added = key_set_delta_keys(true);
        const nb::list key_set_removed = key_set_delta_keys(false);
        if (std::getenv("HGRAPH_DEBUG_TSD_MOD_KEYS_DETAIL") != nullptr) {
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
                for (const auto &k : key_set_added) {
                    append_unique(nb::cast<nb::object>(k));
                }
                return out;
            }
            for (const auto &k : key_set_added) {
                append_unique(nb::cast<nb::object>(k));
            }
            return out;
        }

        if (tuple_index == 2) {
            out = nb::list{};
            seen = nb::set{};
            for (const auto &k : key_set_removed) {
                append_unique(nb::cast<nb::object>(k));
            }
            return out;
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

        // If we successfully parsed a non-slot-0 tuple and it just had no entries,
        // preserve that. Only fall through when tuple data was unavailable/incompatible.
        if (!prefer_python_delta_for_slot0 && tuple_slot_compatible) {
            return out;
        }

        nb::object delta_obj = view.delta_to_python();
        if (delta_obj.is_none()) {
            if (prefer_python_delta_for_slot0 && ref_valued) {
                for (const auto &k : key_set_added) {
                    append_unique(nb::cast<nb::object>(k));
                }
            }
            return out;
        }

        if (prefer_python_delta_for_slot0) {
            out = nb::list{};
            seen = nb::set{};
        }

        auto emit_from_pair = [tuple_index, &append_unique](const nb::object &key_obj, const nb::object &value_obj) {
            const bool is_remove = is_remove_marker(value_obj);
            if (tuple_index == 2 && is_remove) {
                append_unique(key_obj);
            }
            if (tuple_index == 0 && !is_remove) {
                append_unique(key_obj);
            }
        };

        if (nb::isinstance<nb::dict>(delta_obj)) {
            nb::dict delta_dict = nb::cast<nb::dict>(delta_obj);
            for (const auto &kv : delta_dict) {
                emit_from_pair(nb::cast<nb::object>(kv.first), nb::cast<nb::object>(kv.second));
            }
            return out;
        }

        auto items_attr = nb::getattr(delta_obj, "items", nb::none());
        if (items_attr.is_none()) {
            return out;
        }
        for (const auto &kv : nb::iter(items_attr())) {
            emit_from_pair(nb::cast<nb::object>(kv[0]), nb::cast<nb::object>(kv[1]));
        }
        return out;
    }

    bool list_contains_py_object(const nb::list &list_obj, const nb::object &value) {
        return PySequence_Contains(list_obj.ptr(), value.ptr()) == 1;
    }

    template <typename DictViewT>
    nb::list dict_values_for_keys(DictViewT dict_view, const nb::list &keys) {
        nb::list out;
        const auto *meta = dict_view.ts_meta();
        for (const auto &key_item : keys) {
            nb::object key = nb::cast<nb::object>(key_item);
            auto key_val = key_from_python_with_meta(key, meta);
            if (key_val.schema() == nullptr) {
                continue;
            }
            auto child = dict_view.at_key(key_val.view());
            if (child) {
                if constexpr (std::is_same_v<DictViewT, TSDOutputView>) {
                    out.append(wrap_output_view(std::move(child)));
                } else {
                    out.append(wrap_input_view(std::move(child)));
                }
            }
        }
        return out;
    }

    template <typename DictViewT>
    nb::list dict_items_for_keys(DictViewT dict_view, const nb::list &keys) {
        nb::list out;
        const auto *meta = dict_view.ts_meta();
        for (const auto &key_item : keys) {
            nb::object key = nb::cast<nb::object>(key_item);
            auto key_val = key_from_python_with_meta(key, meta);
            if (key_val.schema() == nullptr) {
                continue;
            }
            auto child = dict_view.at_key(key_val.view());
            if (child) {
                if constexpr (std::is_same_v<DictViewT, TSDOutputView>) {
                    out.append(nb::make_tuple(key, wrap_output_view(std::move(child))));
                } else {
                    out.append(nb::make_tuple(key, wrap_input_view(std::move(child))));
                }
            }
        }
        return out;
    }

    bool is_remove_marker(const nb::object &obj) {
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

    bool is_remove_if_exists_marker(const nb::object &obj) {
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

    bool same_view_identity(const ViewData& lhs, const ViewData& rhs) {
        return lhs.value_data == rhs.value_data &&
               lhs.time_data == rhs.time_data &&
               lhs.delta_data == rhs.delta_data &&
               lhs.observer_data == rhs.observer_data &&
               lhs.link_data == rhs.link_data &&
               lhs.path.indices == rhs.path.indices;
    }

    bool child_rebound_this_tick(const TSView& child_view) {
        ViewData previous{};
        if (!resolve_previous_bound_target_view_data(child_view.view_data(), previous)) {
            return false;
        }

        ViewData current{};
        if (!resolve_bound_target_view_data(child_view.view_data(), current)) {
            return false;
        }

        return !same_view_identity(previous, current);
    }
}  // namespace

    // ===== PyTimeSeriesDictOutput Implementation =====

    value::Value PyTimeSeriesDictOutput::key_from_python(const nb::object &key) const {
        return key_from_python_with_meta(key, output_view().ts_meta());
    }

    size_t PyTimeSeriesDictOutput::size() const {
        return output_view().as_dict().count();
    }

    nb::object PyTimeSeriesDictOutput::get_item(const nb::object &item) const {
        if (get_key_set_id().is(item)) {
            return key_set();
        }
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            throw nb::key_error();
        }

        auto child = output_view().as_dict().at_key(key_val.view());
        if (!child) {
            throw nb::key_error();
        }
        return wrap_output_view(std::move(child));
    }

    nb::object PyTimeSeriesDictOutput::get(const nb::object &item, const nb::object &default_value) const {
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            return default_value;
        }
        auto child = output_view().as_dict().at_key(key_val.view());
        return child ? wrap_output_view(std::move(child)) : default_value;
    }

    nb::object PyTimeSeriesDictOutput::get_or_create(const nb::object &key) {
        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            return nb::none();
        }
        auto child = output_view().as_dict().create(key_val.view());
        return child ? wrap_output_view(std::move(child)) : nb::none();
    }

    void PyTimeSeriesDictOutput::create(const nb::object &item) {
        auto key_val = key_from_python(item);
        if (key_val.schema() != nullptr) {
            (void)output_view().as_dict().create(key_val.view());
        }
    }

    void PyTimeSeriesDictOutput::clear() {
        auto dict = output_view().as_dict();
        value::View current = output_view().as_ts_view().value();
        if (!current.valid() || !current.is_map()) {
            return;
        }

        std::vector<value::Value> keys;
        keys.reserve(current.as_map().size());
        for (value::View key : current.as_map().keys()) {
            keys.emplace_back(key.clone());
        }

        for (const auto& key : keys) {
            dict.remove(key.view());
        }
    }

    nb::object PyTimeSeriesDictOutput::iter() const {
        return nb::iter(keys());
    }

    bool PyTimeSeriesDictOutput::contains(const nb::object &item) const {
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            return false;
        }
        value::View value = view().value();
        return value.valid() && value.is_map() && value.as_map().contains(key_val.view());
    }

    nb::object PyTimeSeriesDictOutput::key_set() const {
        TSOutputView key_set = output_view();
        key_set.set_current_time(resolve_runtime_time_for_output(key_set));
        key_set.as_ts_view().view_data().projection = ViewProjection::TSD_KEY_SET;
        return nb::cast(PyTimeSeriesSetOutput(std::move(key_set)));
    }

    nb::object PyTimeSeriesDictOutput::keys() const {
        return tsd_keys_python(output_view().as_ts_view());
    }

    nb::object PyTimeSeriesDictOutput::values() const {
        return dict_values_for_keys(output_view().as_dict(), nb::cast<nb::list>(keys()));
    }

    nb::object PyTimeSeriesDictOutput::items() const {
        return dict_items_for_keys(output_view().as_dict(), nb::cast<nb::list>(keys()));
    }

    nb::object PyTimeSeriesDictOutput::delta_value() const {
        TSOutputView view = output_view();
        auto dict = view.as_dict();
        const auto* meta = dict.ts_meta();
        const bool ref_valued =
            meta != nullptr && meta->element_ts() != nullptr && meta->element_ts()->kind == TSKind::REF;
        const bool debug_delta_simple = std::getenv("HGRAPH_DEBUG_PY_TSD_DELTA_SIMPLE") != nullptr;
        nb::object bridge_delta_obj = view.as_ts_view().delta_to_python();
        const auto lookup_bridge_delta = [&](const nb::object& key) -> nb::object {
            if (bridge_delta_obj.is_none()) {
                return nb::none();
            }
            nb::object get_fn = nb::getattr(bridge_delta_obj, "get", nb::none());
            if (get_fn.is_none()) {
                return nb::none();
            }
            return get_fn(key, nb::none());
        };

        nb::dict out;
        nb::set added_key_set;
        nb::list added_keys_list = nb::cast<nb::list>(added_keys());
        for (const auto& key_item : added_keys_list) {
            added_key_set.add(nb::cast<nb::object>(key_item));
        }

        nb::set current_keys;
        nb::list current_keys_list = nb::cast<nb::list>(keys());
        for (const auto& key_item : current_keys_list) {
            current_keys.add(nb::cast<nb::object>(key_item));
        }

        nb::set previous_keys;
        ViewData previous_bound_keys{};
        if (resolve_previous_bound_target_view_data(view.as_ts_view().view_data(), previous_bound_keys) &&
            previous_bound_keys.ops != nullptr &&
            previous_bound_keys.ops->value != nullptr) {
            value::View previous_value = previous_bound_keys.ops->value(previous_bound_keys);
            if (previous_value.valid() && previous_value.is_map()) {
                for (value::View prev_key : previous_value.as_map().keys()) {
                    previous_keys.add(prev_key.to_python());
                }
            }
        }

        for (const auto& key_item : nb::cast<nb::list>(modified_keys())) {
            nb::object key = nb::cast<nb::object>(key_item);
            auto key_val = key_from_python_with_meta(key, meta);
            if (key_val.schema() == nullptr) {
                continue;
            }

            auto child = dict.at_key(key_val.view());
            if (!child) {
                nb::object bridge_value = lookup_bridge_delta(key);
                if (!bridge_value.is_none() && !is_remove_marker(bridge_value)) {
                    out[key] = std::move(bridge_value);
                }
                continue;
            }
            const bool child_modified = child.modified();
            nb::object child_delta = child.as_ts_view().delta_to_python();
            const bool in_added_set = PySet_Contains(added_key_set.ptr(), key.ptr()) == 1;
            const bool in_current_set = PySet_Contains(current_keys.ptr(), key.ptr()) == 1;
            const bool in_previous_set = PySet_Contains(previous_keys.ptr(), key.ptr()) == 1;
            const bool structural_added = in_added_set && in_current_set && !in_previous_set;
            const bool rebound = child_rebound_this_tick(child.as_ts_view());
            if (child_delta.is_none() && (ref_valued || structural_added || rebound || !child_modified)) {
                child_delta = child.as_ts_view().to_python();
            }
            if (child_delta.is_none()) {
                nb::object bridge_value = lookup_bridge_delta(key);
                if (!bridge_value.is_none() && !is_remove_marker(bridge_value)) {
                    out[key] = std::move(bridge_value);
                }
                continue;
            }
            out[key] = std::move(child_delta);
        }

        nb::list removed_keys_list = nb::cast<nb::list>(removed_keys());
        if (debug_delta_simple) {
            std::fprintf(stderr,
                         "[py_tsd_delta] path=%s now=%lld added=%s current=%s removed=%s\n",
                         view.as_ts_view().short_path().to_string().c_str(),
                         static_cast<long long>(view.current_time().time_since_epoch().count()),
                         nb::cast<std::string>(nb::repr(added_keys_list)).c_str(),
                         nb::cast<std::string>(nb::repr(current_keys_list)).c_str(),
                         nb::cast<std::string>(nb::repr(removed_keys_list)).c_str());
        }

        value::View previous_map{};
        ViewData previous_bound{};
        if (resolve_previous_bound_target_view_data(view.as_ts_view().view_data(), previous_bound) &&
            previous_bound.ops != nullptr &&
            previous_bound.ops->value != nullptr) {
            value::View previous_value = previous_bound.ops->value(previous_bound);
            if (previous_value.valid() && previous_value.is_map()) {
                previous_map = previous_value;
            }
        }

        for (const auto& key_item : removed_keys_list) {
            nb::object key = nb::cast<nb::object>(key_item);
            if (PyDict_Contains(out.ptr(), key.ptr()) == 1) {
                // Preserve already materialized modified/carry-forward payloads.
                continue;
            }
            if (PySet_Contains(current_keys.ptr(), key.ptr()) == 1) {
                continue;
            }
            if (PySet_Contains(added_key_set.ptr(), key.ptr()) == 1) {
                // Python parity: keys that were added and removed within the same
                // cycle (or removed without a visible value) should not emit REMOVE.
                continue;
            }
            nb::object bridge_value = lookup_bridge_delta(key);
            if (!bridge_value.is_none() && !is_remove_marker(bridge_value)) {
                continue;
            }
            auto key_val = key_from_python_with_meta(key, meta);
            if (key_val.schema() == nullptr) {
                continue;
            }

            bool include_remove = false;
            if (previous_map.valid()) {
                const value::MapView previous_entries = previous_map.as_map();
                if (previous_entries.contains(key_val.view())) {
                    value::View previous_child = previous_entries.at(key_val.view());
                    include_remove = previous_child.valid();
                }
            } else {
                include_remove = true;
            }

            if (include_remove) {
                out[key] = get_remove();
            }
        }

        return get_frozendict()(std::move(out));
    }

    nb::object PyTimeSeriesDictOutput::modified_keys() const {
        TSOutputView view = output_view();
        nb::list out = tsd_delta_keys_slot(view.as_ts_view(), 0, true);

        auto dict = view.as_dict();
        const auto *meta = dict.ts_meta();
        const bool ref_valued =
            meta != nullptr && meta->element_ts() != nullptr && meta->element_ts()->kind == TSKind::REF;
        if (ref_valued) {
            const bool debug_child = std::getenv("HGRAPH_DEBUG_TSD_MOD_KEYS_CHILD") != nullptr;
            nb::list current_keys = nb::cast<nb::list>(keys());
            nb::set current_key_set;
            for (const auto &key_item : current_keys) {
                current_key_set.add(nb::cast<nb::object>(key_item));
            }

            TSOutputView key_set_view = view;
            key_set_view.as_ts_view().view_data().projection = ViewProjection::TSD_KEY_SET;
            const bool key_set_modified = key_set_view.modified();
            if (debug_child) {
                std::fprintf(stderr,
                             "[py_tsd_mod_keys_keyset_out] path=%s now=%lld key_set_modified=%d\n",
                             view.as_ts_view().short_path().to_string().c_str(),
                             static_cast<long long>(view.current_time().time_since_epoch().count()),
                             key_set_modified ? 1 : 0);
            }

            nb::set previous_key_set;
            ViewData previous_bound{};
            if (resolve_previous_bound_target_view_data(view.as_ts_view().view_data(), previous_bound) &&
                previous_bound.ops != nullptr &&
                previous_bound.ops->value != nullptr) {
                value::View previous_value = previous_bound.ops->value(previous_bound);
                if (previous_value.valid() && previous_value.is_map()) {
                    for (value::View prev_key : previous_value.as_map().keys()) {
                        previous_key_set.add(prev_key.to_python());
                    }
                }
            }

            nb::set structural_added;
            for (const auto &key_item : tsd_delta_keys_slot(view.as_ts_view(), 1, false)) {
                nb::object key = nb::cast<nb::object>(key_item);
                const bool in_current = PySet_Contains(current_key_set.ptr(), key.ptr()) == 1;
                const bool in_previous = PySet_Contains(previous_key_set.ptr(), key.ptr()) == 1;
                const bool structural_allowed = key_set_modified && in_current && !in_previous;
                if (debug_child) {
                    std::fprintf(stderr,
                                 "[py_tsd_mod_keys_struct_out] path=%s key=%s in_current=%d in_previous=%d allow=%d\n",
                                 view.as_ts_view().short_path().to_string().c_str(),
                                 nb::cast<std::string>(nb::repr(key)).c_str(),
                                 in_current ? 1 : 0,
                                 in_previous ? 1 : 0,
                                 structural_allowed ? 1 : 0);
                }
                if (structural_allowed) {
                    structural_added.add(key);
                }
            }

            nb::set resolved_modified_set;
            for (const auto &key_item : current_keys) {
                nb::object key = nb::cast<nb::object>(key_item);
                auto key_val = key_from_python_with_meta(key, meta);
                if (key_val.schema() == nullptr) {
                    continue;
                }
                auto child = dict.at_key(key_val.view());
                if (!child) {
                    continue;
                }

                bool child_modified = child.modified();
                bool resolved_modified = false;
                bool resolved_has_delta = false;
                ViewData resolved{};
                if (resolve_bound_target_view_data(child.as_ts_view().view_data(), resolved) &&
                    resolved.ops != nullptr &&
                    resolved.ops->modified != nullptr) {
                    resolved_modified = resolved.ops->modified(resolved, view.current_time());
                    if (resolved.ops->delta_to_python != nullptr) {
                        nb::object resolved_delta = resolved.ops->delta_to_python(resolved, view.current_time());
                        resolved_has_delta = !resolved_delta.is_none();
                    }
                }
                const bool effective_modified = child_modified || resolved_modified;

                if (debug_child) {
                    value::View child_delta_view = child.as_ts_view().delta_value();
                    const bool child_delta_valid = child_delta_view.valid();
                    std::string child_delta_repr = "<none>";
                    if (child_delta_valid) {
                        try {
                            child_delta_repr = child_delta_view.to_string();
                        } catch (...) {}
                    }
                    const std::string resolved_path = resolved.ops != nullptr
                                                          ? resolved.path.to_string()
                                                          : std::string{"<none>"};
                    std::fprintf(stderr,
                                 "[py_tsd_mod_keys_child_in] path=%s key=%s child=%s child_mod=%d child_delta_valid=%d child_delta=%s resolved=%s resolved_mod=%d resolved_has_delta=%d effective=%d\n",
                                 view.as_ts_view().short_path().to_string().c_str(),
                                 nb::cast<std::string>(nb::repr(key)).c_str(),
                                 child.short_path().to_string().c_str(),
                                 child_modified ? 1 : 0,
                                 child_delta_valid ? 1 : 0,
                                 child_delta_repr.c_str(),
                                 resolved_path.c_str(),
                                 resolved_modified ? 1 : 0,
                                 resolved_has_delta ? 1 : 0,
                                 effective_modified ? 1 : 0);
                }

                if (effective_modified) {
                    resolved_modified_set.add(key);
                }
            }

            nb::set raw_delta_keys;
            for (const auto &key_item : out) {
                raw_delta_keys.add(nb::cast<nb::object>(key_item));
            }

            nb::list filtered;
            nb::set seen;
            auto append_if_selected = [&](const nb::object &key) {
                if (PySet_Contains(current_key_set.ptr(), key.ptr()) != 1) {
                    return;
                }
                const bool include =
                    PySet_Contains(raw_delta_keys.ptr(), key.ptr()) == 1 ||
                    PySet_Contains(structural_added.ptr(), key.ptr()) == 1 ||
                    PySet_Contains(resolved_modified_set.ptr(), key.ptr()) == 1;
                if (!include) {
                    return;
                }
                if (PySet_Contains(seen.ptr(), key.ptr()) == 1) {
                    return;
                }
                seen.add(key);
                filtered.append(key);
            };

            for (const auto &key_item : current_keys) {
                append_if_selected(nb::cast<nb::object>(key_item));
            }
            out = std::move(filtered);
        }
        if (std::getenv("HGRAPH_DEBUG_TSD_MOD_KEYS") != nullptr) {
            const std::string keys_repr = nb::cast<std::string>(nb::repr(out));
            std::fprintf(stderr,
                         "[py_tsd_mod_keys_out] path=%s now=%lld keys=%s\n",
                         view.as_ts_view().short_path().to_string().c_str(),
                         static_cast<long long>(view.current_time().time_since_epoch().count()),
                         keys_repr.c_str());
        }
        return out;
    }

    nb::object PyTimeSeriesDictOutput::modified_values() const {
        return dict_values_for_keys(output_view().as_dict(), nb::cast<nb::list>(modified_keys()));
    }

    nb::object PyTimeSeriesDictOutput::modified_items() const {
        return dict_items_for_keys(output_view().as_dict(), nb::cast<nb::list>(modified_keys()));
    }

    bool PyTimeSeriesDictOutput::was_modified(const nb::object &key) const {
        return list_contains_py_object(nb::cast<nb::list>(modified_keys()), key);
    }

    nb::object PyTimeSeriesDictOutput::valid_keys() const {
        nb::list out;
        auto dict = output_view().as_dict();
        for (const auto &key_item : nb::cast<nb::list>(keys())) {
            nb::object key = nb::cast<nb::object>(key_item);
            auto key_val = key_from_python(key);
            if (key_val.schema() == nullptr) {
                continue;
            }
            auto child = dict.at_key(key_val.view());
            if (child && child.valid()) {
                out.append(key);
            }
        }
        return out;
    }

    nb::object PyTimeSeriesDictOutput::valid_values() const {
        return dict_values_for_keys(output_view().as_dict(), nb::cast<nb::list>(valid_keys()));
    }

    nb::object PyTimeSeriesDictOutput::valid_items() const {
        return dict_items_for_keys(output_view().as_dict(), nb::cast<nb::list>(valid_keys()));
    }

    nb::object PyTimeSeriesDictOutput::added_keys() const {
        return tsd_delta_keys_slot(output_view().as_ts_view(), 1, false);
    }

    nb::object PyTimeSeriesDictOutput::added_values() const {
        return dict_values_for_keys(output_view().as_dict(), nb::cast<nb::list>(added_keys()));
    }

    nb::object PyTimeSeriesDictOutput::added_items() const {
        return dict_items_for_keys(output_view().as_dict(), nb::cast<nb::list>(added_keys()));
    }

    bool PyTimeSeriesDictOutput::has_added() const {
        return nb::len(nb::cast<nb::list>(added_keys())) > 0;
    }

    bool PyTimeSeriesDictOutput::was_added(const nb::object &key) const {
        return list_contains_py_object(nb::cast<nb::list>(added_keys()), key);
    }

    nb::object PyTimeSeriesDictOutput::removed_keys() const {
        return tsd_delta_keys_slot(output_view().as_ts_view(), 2, false);
    }

    nb::object PyTimeSeriesDictOutput::removed_values() const {
        return dict_values_for_keys(output_view().as_dict(), nb::cast<nb::list>(removed_keys()));
    }

    nb::object PyTimeSeriesDictOutput::removed_items() const {
        return dict_items_for_keys(output_view().as_dict(), nb::cast<nb::list>(removed_keys()));
    }

    bool PyTimeSeriesDictOutput::has_removed() const {
        return nb::len(nb::cast<nb::list>(removed_keys())) > 0;
    }

    bool PyTimeSeriesDictOutput::was_removed(const nb::object &key) const {
        return list_contains_py_object(nb::cast<nb::list>(removed_keys()), key);
    }

    nb::object PyTimeSeriesDictOutput::key_from_value(const nb::object &value) const {
        auto *wrapped = nb::inst_ptr<PyTimeSeriesOutput>(value);
        if (wrapped == nullptr) {
            return nb::none();
        }
        const auto &target = wrapped->output_view().short_path();

        auto dict = output_view().as_dict();
        for (const auto &key_item : nb::cast<nb::list>(keys())) {
            nb::object key = nb::cast<nb::object>(key_item);
            auto key_val = key_from_python(key);
            if (key_val.schema() == nullptr) {
                continue;
            }
            auto child = dict.at_key(key_val.view());
            if (!child) {
                continue;
            }
            const auto &path = child.short_path();
            if (path.node == target.node && path.port_type == target.port_type && path.indices == target.indices) {
                return key;
            }
        }
        return nb::none();
    }

    nb::str PyTimeSeriesDictOutput::py_str() const {
        auto str = fmt::format("TimeSeriesDictOutput@{:p}[size={}, valid={}]",
                               static_cast<const void *>(&output_view()), size(), output_view().valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesDictOutput::py_repr() const {
        return py_str();
    }

    void PyTimeSeriesDictOutput::set_item(const nb::object &key, const nb::object &value) {
        if (is_remove_marker(value)) {
            auto key_val = key_from_python(key);
            if (key_val.schema() == nullptr) {
                return;
            }

            auto dict = output_view().as_dict();
            if (is_remove_if_exists_marker(value)) {
                dict.remove(key_val.view());
                return;
            }

            if (!dict.remove(key_val.view())) {
                throw nb::key_error();
            }
            return;
        }

        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            throw nb::key_error();
        }
        auto child = output_view().as_dict().create(key_val.view());
        if (!child) {
            throw std::runtime_error("Failed to create TSD output child");
        }
        child.from_python(value);
    }

    void PyTimeSeriesDictOutput::del_item(const nb::object &key) {
        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            throw nb::key_error();
        }
        if (!output_view().as_dict().remove(key_val.view())) {
            throw nb::key_error();
        }
    }

    nb::object PyTimeSeriesDictOutput::pop(const nb::object &key, const nb::object &default_value) {
        if (!contains(key)) {
            return default_value;
        }
        nb::object out = get_item(key);
        del_item(key);
        return out;
    }

    nb::object PyTimeSeriesDictOutput::get_ref(const nb::object &key, const nb::object &requester) {
        TSOutputView base = output_view();
        TSOutputView out = runtime_tsd_get_ref_output(base, key, requester);
        return out ? wrap_output_view(std::move(out)) : nb::none();
    }

    void PyTimeSeriesDictOutput::release_ref(const nb::object &key, const nb::object &requester) {
        TSOutputView base = output_view();
        runtime_tsd_release_ref_output(base, key, requester);
    }

    // ===== PyTimeSeriesDictInput Implementation =====

    value::Value PyTimeSeriesDictInput::key_from_python(const nb::object &key) const {
        return key_from_python_with_meta(key, input_view().ts_meta());
    }

    size_t PyTimeSeriesDictInput::size() const {
        return input_view().as_dict().count();
    }

    nb::object PyTimeSeriesDictInput::get_item(const nb::object &item) const {
        if (get_key_set_id().is(item)) {
            return key_set();
        }
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            throw nb::key_error();
        }
        auto child = input_view().as_dict().at_key(key_val.view());
        if (!child) {
            if (std::getenv("HGRAPH_DEBUG_TSD_INPUT_CREATE") != nullptr) {
                value::View local = input_view().as_ts_view().value();
                const value::TypeMeta* map_key = (local.valid() && local.is_map()) ? local.as_map().key_type() : nullptr;
                value::View local_nav{};
                const ViewData& vd = input_view().as_ts_view().view_data();
                auto* value_root = static_cast<const value::Value*>(vd.value_data);
                if (value_root != nullptr && value_root->has_value()) {
                    local_nav = value_root->view();
                    for (size_t index : vd.path.indices) {
                        if (!local_nav.valid() || !local_nav.is_tuple()) {
                            local_nav = {};
                            break;
                        }
                        auto tuple = local_nav.as_tuple();
                        if (index >= tuple.size()) {
                            local_nav = {};
                            break;
                        }
                        local_nav = tuple.at(index);
                    }
                }
                const value::TypeMeta* nav_map_key = (local_nav.valid() && local_nav.is_map())
                                                         ? local_nav.as_map().key_type()
                                                         : nullptr;
                std::fprintf(stderr,
                             "[tsd_input.get_item] miss key=%s path=%s bound=%d local_valid=%d local_is_map=%d local_size=%zu key_schema=%p map_key=%p local_nav_valid=%d local_nav_is_map=%d local_nav_size=%zu local_nav_map_key=%p\n",
                             nb::cast<std::string>(nb::str(item)).c_str(),
                             input_view().short_path().to_string().c_str(),
                             input_view().is_bound() ? 1 : 0,
                             local.valid() ? 1 : 0,
                             (local.valid() && local.is_map()) ? 1 : 0,
                             (local.valid() && local.is_map()) ? local.as_map().size() : 0UL,
                             static_cast<const void*>(key_val.schema()),
                             static_cast<const void*>(map_key),
                             local_nav.valid() ? 1 : 0,
                             (local_nav.valid() && local_nav.is_map()) ? 1 : 0,
                             (local_nav.valid() && local_nav.is_map()) ? local_nav.as_map().size() : 0UL,
                             static_cast<const void*>(nav_map_key));
            }
            throw nb::key_error();
        }
        return wrap_input_view(std::move(child));
    }

    nb::object PyTimeSeriesDictInput::get(const nb::object &item, const nb::object &default_value) const {
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            return default_value;
        }
        auto child = input_view().as_dict().at_key(key_val.view());
        return child ? wrap_input_view(std::move(child)) : default_value;
    }

    nb::object PyTimeSeriesDictInput::get_or_create(const nb::object &key) {
        auto value = get(key, nb::none());
        if (!value.is_none()) {
            return value;
        }
        create(key);
        return get(key, nb::none());
    }

    void PyTimeSeriesDictInput::create(const nb::object &item) {
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            return;
        }

        TSView child = input_view().as_ts_view().as_dict().create(key_val.view());
        if (std::getenv("HGRAPH_DEBUG_TSD_INPUT_CREATE") != nullptr) {
            value::View local = input_view().as_ts_view().value();
            const value::TypeMeta* key_type = (local.valid() && local.is_map()) ? local.as_map().key_type() : nullptr;
            const value::TypeMeta* value_type = (local.valid() && local.is_map()) ? local.as_map().value_type() : nullptr;
            const bool contains = local.valid() && local.is_map() && local.as_map().contains(key_val.view());
            const bool debug_dispatch = std::getenv("HGRAPH_DEBUG_TSD_CREATE_DISPATCH") != nullptr;
            std::fprintf(stderr,
                         "[tsd_input.create] key=%s child=%d bound=%d local_valid=%d local_is_map=%d local_size=%zu key_schema=%p map_key=%p value_type=%p contains=%d dbg_dispatch=%d\n",
                         nb::cast<std::string>(nb::str(item)).c_str(),
                         child ? 1 : 0,
                         input_view().is_bound() ? 1 : 0,
                         local.valid() ? 1 : 0,
                         (local.valid() && local.is_map()) ? 1 : 0,
                         (local.valid() && local.is_map()) ? local.as_map().size() : 0UL,
                         static_cast<const void*>(key_val.schema()),
                         static_cast<const void*>(key_type),
                         static_cast<const void*>(value_type),
                         contains ? 1 : 0,
                         debug_dispatch ? 1 : 0);
        }

    }

    nb::object PyTimeSeriesDictInput::iter() const {
        return nb::iter(keys());
    }

    bool PyTimeSeriesDictInput::contains(const nb::object &item) const {
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            return false;
        }
        value::View value = view().value();
        return value.valid() && value.is_map() && value.as_map().contains(key_val.view());
    }

    nb::object PyTimeSeriesDictInput::key_set() const {
        TSInputView key_set = input_view();
        key_set.set_current_time(resolve_runtime_time_for_input(key_set));
        key_set.as_ts_view().view_data().projection = ViewProjection::TSD_KEY_SET;
        return nb::cast(PyTimeSeriesSetInput(std::move(key_set)));
    }

    nb::object PyTimeSeriesDictInput::keys() const {
        return tsd_keys_python(input_view().as_ts_view());
    }

    nb::object PyTimeSeriesDictInput::values() const {
        return dict_values_for_keys(input_view().as_dict(), nb::cast<nb::list>(keys()));
    }

    nb::object PyTimeSeriesDictInput::items() const {
        return dict_items_for_keys(input_view().as_dict(), nb::cast<nb::list>(keys()));
    }

    nb::object PyTimeSeriesDictInput::delta_value() const {
        TSInputView view = input_view();
        auto dict = view.as_dict();
        const auto* meta = dict.ts_meta();
        const bool ref_valued =
            meta != nullptr && meta->element_ts() != nullptr && meta->element_ts()->kind == TSKind::REF;
        nb::object bridge_delta_obj = view.as_ts_view().delta_to_python();
        const auto lookup_bridge_delta = [&](const nb::object& key) -> nb::object {
            if (bridge_delta_obj.is_none()) {
                return nb::none();
            }
            nb::object get_fn = nb::getattr(bridge_delta_obj, "get", nb::none());
            if (get_fn.is_none()) {
                return nb::none();
            }
            return get_fn(key, nb::none());
        };

        nb::dict out;
        nb::set added_key_set;
        for (const auto& key_item : nb::cast<nb::list>(added_keys())) {
            added_key_set.add(nb::cast<nb::object>(key_item));
        }

        nb::set current_keys;
        for (const auto& key_item : nb::cast<nb::list>(keys())) {
            current_keys.add(nb::cast<nb::object>(key_item));
        }

        nb::set previous_keys;
        ViewData previous_bound_keys{};
        if (resolve_previous_bound_target_view_data(view.as_ts_view().view_data(), previous_bound_keys) &&
            previous_bound_keys.ops != nullptr &&
            previous_bound_keys.ops->value != nullptr) {
            value::View previous_value = previous_bound_keys.ops->value(previous_bound_keys);
            if (previous_value.valid() && previous_value.is_map()) {
                for (value::View prev_key : previous_value.as_map().keys()) {
                    previous_keys.add(prev_key.to_python());
                }
            }
        }

        for (const auto& key_item : nb::cast<nb::list>(modified_keys())) {
            nb::object key = nb::cast<nb::object>(key_item);
            auto key_val = key_from_python_with_meta(key, meta);
            if (key_val.schema() == nullptr) {
                continue;
            }

            auto child = dict.at_key(key_val.view());
            if (!child) {
                nb::object bridge_value = lookup_bridge_delta(key);
                if (!bridge_value.is_none() && !is_remove_marker(bridge_value)) {
                    out[key] = std::move(bridge_value);
                }
                continue;
            }
            const bool child_modified = child.modified();
            nb::object child_delta = child.as_ts_view().delta_to_python();
            const bool in_added_set = PySet_Contains(added_key_set.ptr(), key.ptr()) == 1;
            const bool in_current_set = PySet_Contains(current_keys.ptr(), key.ptr()) == 1;
            const bool in_previous_set = PySet_Contains(previous_keys.ptr(), key.ptr()) == 1;
            const bool structural_added = in_added_set && in_current_set && !in_previous_set;
            const bool rebound = child_rebound_this_tick(child.as_ts_view());
            if (child_delta.is_none() && (ref_valued || structural_added || rebound || !child_modified)) {
                child_delta = child.as_ts_view().to_python();
            }
            if (child_delta.is_none()) {
                nb::object bridge_value = lookup_bridge_delta(key);
                if (!bridge_value.is_none() && !is_remove_marker(bridge_value)) {
                    out[key] = std::move(bridge_value);
                }
                continue;
            }
            out[key] = std::move(child_delta);
        }

        value::View previous_map{};
        ViewData previous_bound{};
        if (resolve_previous_bound_target_view_data(view.as_ts_view().view_data(), previous_bound) &&
            previous_bound.ops != nullptr &&
            previous_bound.ops->value != nullptr) {
            value::View previous_value = previous_bound.ops->value(previous_bound);
            if (previous_value.valid() && previous_value.is_map()) {
                previous_map = previous_value;
            }
        }

        for (const auto& key_item : nb::cast<nb::list>(removed_keys())) {
            nb::object key = nb::cast<nb::object>(key_item);
            if (PyDict_Contains(out.ptr(), key.ptr()) == 1) {
                // Preserve already materialized modified/carry-forward payloads.
                continue;
            }
            if (PySet_Contains(current_keys.ptr(), key.ptr()) == 1) {
                continue;
            }
            if (PySet_Contains(added_key_set.ptr(), key.ptr()) == 1) {
                // Keys that churn within the same cycle should not surface as REMOVE.
                continue;
            }
            nb::object bridge_value = lookup_bridge_delta(key);
            if (!bridge_value.is_none() && !is_remove_marker(bridge_value)) {
                continue;
            }
            auto key_val = key_from_python_with_meta(key, meta);
            if (key_val.schema() == nullptr) {
                continue;
            }

            bool include_remove = false;
            if (previous_map.valid()) {
                const value::MapView previous_entries = previous_map.as_map();
                if (previous_entries.contains(key_val.view())) {
                    value::View previous_child = previous_entries.at(key_val.view());
                    include_remove = previous_child.valid();
                }
            } else {
                include_remove = true;
            }

            if (include_remove) {
                out[key] = get_remove();
            }
        }

        return get_frozendict()(std::move(out));
    }

    nb::object PyTimeSeriesDictInput::modified_keys() const {
        TSInputView view = input_view();
        nb::list out = tsd_delta_keys_slot(view.as_ts_view(), 0, true);

        auto dict = view.as_dict();
        const auto *meta = dict.ts_meta();
        const bool ref_valued =
            meta != nullptr && meta->element_ts() != nullptr && meta->element_ts()->kind == TSKind::REF;
        if (ref_valued) {
            const bool debug_child = std::getenv("HGRAPH_DEBUG_TSD_MOD_KEYS_CHILD") != nullptr;
            nb::list current_keys = nb::cast<nb::list>(keys());
            nb::set current_key_set;
            for (const auto &key_item : current_keys) {
                current_key_set.add(nb::cast<nb::object>(key_item));
            }

            TSInputView key_set_view = view;
            key_set_view.as_ts_view().view_data().projection = ViewProjection::TSD_KEY_SET;
            const bool key_set_modified = key_set_view.modified();
            if (debug_child) {
                std::fprintf(stderr,
                             "[py_tsd_mod_keys_keyset_in] path=%s now=%lld key_set_modified=%d\n",
                             view.as_ts_view().short_path().to_string().c_str(),
                             static_cast<long long>(view.current_time().time_since_epoch().count()),
                             key_set_modified ? 1 : 0);
            }

            nb::set previous_key_set;
            ViewData previous_bound{};
            if (resolve_previous_bound_target_view_data(view.as_ts_view().view_data(), previous_bound) &&
                previous_bound.ops != nullptr &&
                previous_bound.ops->value != nullptr) {
                value::View previous_value = previous_bound.ops->value(previous_bound);
                if (previous_value.valid() && previous_value.is_map()) {
                    for (value::View prev_key : previous_value.as_map().keys()) {
                        previous_key_set.add(prev_key.to_python());
                    }
                }
            }

            nb::set structural_added;
            for (const auto &key_item : tsd_delta_keys_slot(view.as_ts_view(), 1, false)) {
                nb::object key = nb::cast<nb::object>(key_item);
                const bool in_current = PySet_Contains(current_key_set.ptr(), key.ptr()) == 1;
                const bool in_previous = PySet_Contains(previous_key_set.ptr(), key.ptr()) == 1;
                const bool structural_allowed = key_set_modified && in_current && !in_previous;
                if (debug_child) {
                    std::fprintf(stderr,
                                 "[py_tsd_mod_keys_struct_in] path=%s key=%s in_current=%d in_previous=%d allow=%d\n",
                                 view.as_ts_view().short_path().to_string().c_str(),
                                 nb::cast<std::string>(nb::repr(key)).c_str(),
                                 in_current ? 1 : 0,
                                 in_previous ? 1 : 0,
                                 structural_allowed ? 1 : 0);
                }
                if (structural_allowed) {
                    structural_added.add(key);
                }
            }

            nb::set resolved_modified_set;
            for (const auto &key_item : current_keys) {
                nb::object key = nb::cast<nb::object>(key_item);
                auto key_val = key_from_python_with_meta(key, meta);
                if (key_val.schema() == nullptr) {
                    continue;
                }
                auto child = dict.at_key(key_val.view());
                if (!child) {
                    continue;
                }

                const bool child_modified = child.modified();
                bool resolved_modified = false;
                bool resolved_has_delta = false;
                ViewData resolved{};
                if (resolve_bound_target_view_data(child.as_ts_view().view_data(), resolved) &&
                    resolved.ops != nullptr &&
                    resolved.ops->modified != nullptr) {
                    resolved_modified = resolved.ops->modified(resolved, view.current_time());
                    if (resolved.ops->delta_to_python != nullptr) {
                        nb::object resolved_delta = resolved.ops->delta_to_python(resolved, view.current_time());
                        resolved_has_delta = !resolved_delta.is_none();
                    }
                }
                const bool effective_modified = child_modified || resolved_modified;

                if (debug_child) {
                    value::View child_delta_view = child.as_ts_view().delta_value();
                    const bool child_delta_valid = child_delta_view.valid();
                    std::string child_delta_repr = "<none>";
                    if (child_delta_valid) {
                        try {
                            child_delta_repr = child_delta_view.to_string();
                        } catch (...) {}
                    }
                    const std::string resolved_path = resolved.ops != nullptr
                                                          ? resolved.path.to_string()
                                                          : std::string{"<none>"};
                    std::fprintf(stderr,
                                 "[py_tsd_mod_keys_child_in] path=%s key=%s child=%s child_mod=%d child_delta_valid=%d child_delta=%s resolved=%s resolved_mod=%d resolved_has_delta=%d effective=%d\n",
                                 view.as_ts_view().short_path().to_string().c_str(),
                                 nb::cast<std::string>(nb::repr(key)).c_str(),
                                 child.short_path().to_string().c_str(),
                                 child_modified ? 1 : 0,
                                 child_delta_valid ? 1 : 0,
                                 child_delta_repr.c_str(),
                                 resolved_path.c_str(),
                                 resolved_modified ? 1 : 0,
                                 resolved_has_delta ? 1 : 0,
                                 effective_modified ? 1 : 0);
                }

                if (effective_modified) {
                    resolved_modified_set.add(key);
                }
            }

            nb::set raw_delta_keys;
            for (const auto &key_item : out) {
                raw_delta_keys.add(nb::cast<nb::object>(key_item));
            }

            nb::list filtered;
            nb::set seen;
            auto append_if_selected = [&](const nb::object &key) {
                if (PySet_Contains(current_key_set.ptr(), key.ptr()) != 1) {
                    return;
                }
                const bool include =
                    PySet_Contains(raw_delta_keys.ptr(), key.ptr()) == 1 ||
                    PySet_Contains(structural_added.ptr(), key.ptr()) == 1 ||
                    PySet_Contains(resolved_modified_set.ptr(), key.ptr()) == 1;
                if (!include) {
                    return;
                }
                if (PySet_Contains(seen.ptr(), key.ptr()) == 1) {
                    return;
                }
                seen.add(key);
                filtered.append(key);
            };

            for (const auto &key_item : current_keys) {
                append_if_selected(nb::cast<nb::object>(key_item));
            }
            out = std::move(filtered);
        }
        if (std::getenv("HGRAPH_DEBUG_TSD_MOD_KEYS") != nullptr) {
            const std::string keys_repr = nb::cast<std::string>(nb::repr(out));
            std::fprintf(stderr,
                         "[py_tsd_mod_keys_in] path=%s now=%lld keys=%s\n",
                         view.as_ts_view().short_path().to_string().c_str(),
                         static_cast<long long>(view.current_time().time_since_epoch().count()),
                         keys_repr.c_str());
        }
        return out;
    }

    nb::object PyTimeSeriesDictInput::modified_values() const {
        return dict_values_for_keys(input_view().as_dict(), nb::cast<nb::list>(modified_keys()));
    }

    nb::object PyTimeSeriesDictInput::modified_items() const {
        return dict_items_for_keys(input_view().as_dict(), nb::cast<nb::list>(modified_keys()));
    }

    bool PyTimeSeriesDictInput::was_modified(const nb::object &key) const {
        return list_contains_py_object(nb::cast<nb::list>(modified_keys()), key);
    }

    nb::object PyTimeSeriesDictInput::valid_keys() const {
        nb::list out;
        auto dict = input_view().as_dict();
        for (const auto &key_item : nb::cast<nb::list>(keys())) {
            nb::object key = nb::cast<nb::object>(key_item);
            auto key_val = key_from_python(key);
            if (key_val.schema() == nullptr) {
                continue;
            }
            auto child = dict.at_key(key_val.view());
            if (child && child.valid()) {
                out.append(key);
            }
        }
        return out;
    }

    nb::object PyTimeSeriesDictInput::valid_values() const {
        return dict_values_for_keys(input_view().as_dict(), nb::cast<nb::list>(valid_keys()));
    }

    nb::object PyTimeSeriesDictInput::valid_items() const {
        return dict_items_for_keys(input_view().as_dict(), nb::cast<nb::list>(valid_keys()));
    }

    nb::object PyTimeSeriesDictInput::added_keys() const {
        return tsd_delta_keys_slot(input_view().as_ts_view(), 1, false);
    }

    nb::object PyTimeSeriesDictInput::added_values() const {
        return dict_values_for_keys(input_view().as_dict(), nb::cast<nb::list>(added_keys()));
    }

    nb::object PyTimeSeriesDictInput::added_items() const {
        return dict_items_for_keys(input_view().as_dict(), nb::cast<nb::list>(added_keys()));
    }

    bool PyTimeSeriesDictInput::has_added() const {
        return nb::len(nb::cast<nb::list>(added_keys())) > 0;
    }

    bool PyTimeSeriesDictInput::was_added(const nb::object &key) const {
        return list_contains_py_object(nb::cast<nb::list>(added_keys()), key);
    }

    nb::object PyTimeSeriesDictInput::removed_keys() const {
        return tsd_delta_keys_slot(input_view().as_ts_view(), 2, false);
    }

    nb::object PyTimeSeriesDictInput::removed_values() const {
        return dict_values_for_keys(input_view().as_dict(), nb::cast<nb::list>(removed_keys()));
    }

    nb::object PyTimeSeriesDictInput::removed_items() const {
        return dict_items_for_keys(input_view().as_dict(), nb::cast<nb::list>(removed_keys()));
    }

    bool PyTimeSeriesDictInput::has_removed() const {
        return nb::len(nb::cast<nb::list>(removed_keys())) > 0;
    }

    bool PyTimeSeriesDictInput::was_removed(const nb::object &key) const {
        return list_contains_py_object(nb::cast<nb::list>(removed_keys()), key);
    }

    nb::object PyTimeSeriesDictInput::key_from_value(const nb::object &value) const {
        auto *wrapped = nb::inst_ptr<PyTimeSeriesInput>(value);
        if (wrapped == nullptr) {
            return nb::none();
        }
        const auto &target = wrapped->input_view().short_path();

        auto dict = input_view().as_dict();
        for (const auto &key_item : nb::cast<nb::list>(keys())) {
            nb::object key = nb::cast<nb::object>(key_item);
            auto key_val = key_from_python(key);
            if (key_val.schema() == nullptr) {
                continue;
            }
            auto child = dict.at_key(key_val.view());
            if (!child) {
                continue;
            }
            const auto &path = child.short_path();
            if (path.node == target.node && path.port_type == target.port_type && path.indices == target.indices) {
                return key;
            }
        }
        return nb::none();
    }

    nb::str PyTimeSeriesDictInput::py_str() const {
        auto str = fmt::format("TimeSeriesDictInput@{:p}[size={}, valid={}]",
                               static_cast<const void *>(&input_view()), size(), input_view().valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesDictInput::py_repr() const {
        return py_str();
    }

    void PyTimeSeriesDictInput::on_key_added(const nb::object &key) {
        if (!input_view().is_bound()) {
            return;
        }

        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            return;
        }

        auto child_input = input_view().as_dict().at_key(key_val.view());
        if (!child_input) {
            return;
        }

        auto out_obj = output();
        if (!nb::isinstance<PyTimeSeriesOutput>(out_obj)) {
            return;
        }

        auto &py_output = nb::cast<PyTimeSeriesOutput &>(out_obj);
        if (py_output.output_view().as_ts_view().kind() != TSKind::TSD) {
            return;
        }

        auto child_output = py_output.output_view().as_dict().at_key(key_val.view());
        if (!child_output) {
            return;
        }

        child_input.bind(child_output);
        if (input_view().active()) {
            child_input.make_active();
        }
    }

    void PyTimeSeriesDictInput::on_key_removed(const nb::object &key) {
        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            return;
        }

        auto child_input = input_view().as_dict().at_key(key_val.view());
        if (!child_input) {
            return;
        }

        if (child_input.active()) {
            child_input.make_passive();
        }
        child_input.unbind();
        (void)input_view().as_ts_view().as_dict().remove(key_val.view());
    }

    // ===== Nanobind Registration =====

    void tsd_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictOutput, PyTimeSeriesOutput>(m, "TimeSeriesDictOutput")
            .def("__contains__", &PyTimeSeriesDictOutput::contains, "key"_a)
            .def("__getitem__", &PyTimeSeriesDictOutput::get_item, "key"_a)
            .def("__setitem__", &PyTimeSeriesDictOutput::set_item, "key"_a, "value"_a)
            .def("__delitem__", &PyTimeSeriesDictOutput::del_item, "key"_a)
            .def("__len__", &PyTimeSeriesDictOutput::size)
            .def("pop", &PyTimeSeriesDictOutput::pop, "key"_a, "default"_a = nb::none())
            .def("get", &PyTimeSeriesDictOutput::get, "key"_a, "default"_a = nb::none())
            .def("get_or_create", &PyTimeSeriesDictOutput::get_or_create, "key"_a)
            .def("create", &PyTimeSeriesDictOutput::create, "key"_a)
            .def("clear", &PyTimeSeriesDictOutput::clear)
            .def("__iter__", &PyTimeSeriesDictOutput::iter)
            .def("keys", &PyTimeSeriesDictOutput::keys)
            .def("values", &PyTimeSeriesDictOutput::values)
            .def("items", &PyTimeSeriesDictOutput::items)
            .def("valid_keys", &PyTimeSeriesDictOutput::valid_keys)
            .def("valid_values", &PyTimeSeriesDictOutput::valid_values)
            .def("valid_items", &PyTimeSeriesDictOutput::valid_items)
            .def("added_keys", &PyTimeSeriesDictOutput::added_keys)
            .def("added_values", &PyTimeSeriesDictOutput::added_values)
            .def("added_items", &PyTimeSeriesDictOutput::added_items)
            .def("was_added", &PyTimeSeriesDictOutput::was_added, "key"_a)
            .def_prop_ro("has_added", &PyTimeSeriesDictOutput::has_added)
            .def("modified_keys", &PyTimeSeriesDictOutput::modified_keys)
            .def("modified_values", &PyTimeSeriesDictOutput::modified_values)
            .def("modified_items", &PyTimeSeriesDictOutput::modified_items)
            .def("was_modified", &PyTimeSeriesDictOutput::was_modified, "key"_a)
            .def("removed_keys", &PyTimeSeriesDictOutput::removed_keys)
            .def("removed_values", &PyTimeSeriesDictOutput::removed_values)
            .def("removed_items", &PyTimeSeriesDictOutput::removed_items)
            .def("was_removed", &PyTimeSeriesDictOutput::was_removed, "key"_a)
            .def("key_from_value", &PyTimeSeriesDictOutput::key_from_value, "value"_a)
            .def_prop_ro("has_removed", &PyTimeSeriesDictOutput::has_removed)
            .def("get_ref", &PyTimeSeriesDictOutput::get_ref, "key"_a, "requester"_a)
            .def("release_ref", &PyTimeSeriesDictOutput::release_ref, "key"_a, "requester"_a)
            .def_prop_ro("key_set", &PyTimeSeriesDictOutput::key_set)
            .def("__str__", &PyTimeSeriesDictOutput::py_str)
            .def("__repr__", &PyTimeSeriesDictOutput::py_repr);

        nb::class_<PyTimeSeriesDictInput, PyTimeSeriesInput>(m, "TimeSeriesDictInput")
            .def("__contains__", &PyTimeSeriesDictInput::contains, "key"_a)
            .def("__getitem__", &PyTimeSeriesDictInput::get_item, "key"_a)
            .def("__len__", &PyTimeSeriesDictInput::size)
            .def("__iter__", &PyTimeSeriesDictInput::iter)
            .def("get", &PyTimeSeriesDictInput::get, "key"_a, "default"_a = nb::none())
            .def("get_or_create", &PyTimeSeriesDictInput::get_or_create, "key"_a)
            .def("create", &PyTimeSeriesDictInput::create, "key"_a)
            .def("keys", &PyTimeSeriesDictInput::keys)
            .def("values", &PyTimeSeriesDictInput::values)
            .def("items", &PyTimeSeriesDictInput::items)
            .def("valid_keys", &PyTimeSeriesDictInput::valid_keys)
            .def("valid_values", &PyTimeSeriesDictInput::valid_values)
            .def("valid_items", &PyTimeSeriesDictInput::valid_items)
            .def("added_keys", &PyTimeSeriesDictInput::added_keys)
            .def("added_values", &PyTimeSeriesDictInput::added_values)
            .def("added_items", &PyTimeSeriesDictInput::added_items)
            .def("was_added", &PyTimeSeriesDictInput::was_added, "key"_a)
            .def_prop_ro("has_added", &PyTimeSeriesDictInput::has_added)
            .def("modified_keys", &PyTimeSeriesDictInput::modified_keys)
            .def("modified_values", &PyTimeSeriesDictInput::modified_values)
            .def("modified_items", &PyTimeSeriesDictInput::modified_items)
            .def("was_modified", &PyTimeSeriesDictInput::was_modified, "key"_a)
            .def("removed_keys", &PyTimeSeriesDictInput::removed_keys)
            .def("removed_values", &PyTimeSeriesDictInput::removed_values)
            .def("removed_items", &PyTimeSeriesDictInput::removed_items)
            .def("was_removed", &PyTimeSeriesDictInput::was_removed, "key"_a)
            .def("on_key_added", &PyTimeSeriesDictInput::on_key_added, "key"_a)
            .def("on_key_removed", &PyTimeSeriesDictInput::on_key_removed, "key"_a)
            .def("key_from_value", &PyTimeSeriesDictInput::key_from_value, "value"_a)
            .def_prop_ro("has_removed", &PyTimeSeriesDictInput::has_removed)
            .def_prop_ro("key_set", &PyTimeSeriesDictInput::key_set)
            .def("__str__", &PyTimeSeriesDictInput::py_str)
            .def("__repr__", &PyTimeSeriesDictInput::py_repr);
    }
}  // namespace hgraph
