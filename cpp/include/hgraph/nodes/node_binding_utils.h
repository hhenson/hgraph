#pragma once

#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>

#include <cassert>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace hgraph {

enum class RefBindOrder {
    RefValueThenBoundTarget,
    BoundTargetThenRefValue,
};

inline const engine_time_t* node_time_ptr(const Node& node) {
    if (const auto* et = node.cached_evaluation_time_ptr(); et != nullptr) {
        return et;
    }
    auto g = node.graph();
    return g != nullptr ? g->cached_evaluation_time_ptr() : nullptr;
}

inline engine_time_t node_time(const Node& node) {
    if (const auto* et = node.cached_evaluation_time_ptr(); et != nullptr) {
        return *et;
    }
    auto g = node.graph();
    return g != nullptr ? g->evaluation_time() : MIN_DT;
}

inline std::string key_repr(const value::View& key, const value::TypeMeta* key_type_meta) {
    if (!key.valid() || key_type_meta == nullptr) {
        return "<invalid key>";
    }
    nb::object py_key = key_type_meta->ops().to_python(key.data(), key_type_meta);
    return nb::cast<std::string>(nb::repr(py_key));
}

inline bool resolve_ref_value_target_view_data(const TSView& ref_view, ViewData& out_target) {
    const TSMeta* meta = ref_view.ts_meta();
    if (meta == nullptr || meta->kind != TSKind::REF) {
        return false;
    }

    value::View payload = ref_view.value();
    if (!payload.valid()) {
        return false;
    }

    const auto& ref = *static_cast<const TimeSeriesReference*>(payload.data());
    if (const ViewData* target = ref.bound_view();
        target != nullptr && !same_view_identity(*target, ref_view.view_data())) {
        out_target = *target;
        return true;
    }

    return false;
}

inline std::optional<ViewData> resolve_effective_view_data(const TSView& start_view,
                                                           RefBindOrder ref_bind_order = RefBindOrder::BoundTargetThenRefValue,
                                                           size_t max_depth = 8) {
    if (!start_view) {
        return std::nullopt;
    }

    ViewData cursor = start_view.view_data();
    const engine_time_t* current_time_ptr = start_view.view_data().engine_time_ptr;

    for (size_t depth = 0; depth < max_depth; ++depth) {
        const auto advance_to_bound_target = [&]() -> bool {
            ViewData bound_target{};
            if (!resolve_bound_target_view_data(cursor, bound_target) ||
                same_view_identity(bound_target, cursor)) {
                return false;
            }
            cursor = std::move(bound_target);
            return true;
        };

        const auto advance_to_ref_target = [&]() -> bool {
            ViewData ref_target{};
            if (!resolve_ref_value_target_view_data(TSView(cursor, current_time_ptr), ref_target)) {
                return false;
            }
            cursor = std::move(ref_target);
            return true;
        };

        bool advanced = false;
        if (ref_bind_order == RefBindOrder::BoundTargetThenRefValue) {
            advanced = advance_to_bound_target() || advance_to_ref_target();
        } else {
            advanced = advance_to_ref_target() || advance_to_bound_target();
        }

        if (!advanced) {
            break;
        }
    }

    return cursor;
}

inline TSView resolve_effective_view(const TSView& start_view,
                                     RefBindOrder ref_bind_order = RefBindOrder::BoundTargetThenRefValue,
                                     size_t max_depth = 8) {
    auto resolved = resolve_effective_view_data(start_view, ref_bind_order, max_depth);
    if (!resolved.has_value()) {
        return {};
    }
    return TSView(*resolved, start_view.view_data().engine_time_ptr);
}

inline std::optional<ViewData> resolve_non_ref_target_view_data(
    const TSView& start_view,
    RefBindOrder ref_bind_order = RefBindOrder::RefValueThenBoundTarget,
    size_t max_depth = 64) {
    auto resolved = resolve_effective_view_data(start_view, ref_bind_order, max_depth);
    if (!resolved.has_value()) {
        return std::nullopt;
    }

    TSView resolved_view(*resolved, start_view.view_data().engine_time_ptr);
    const TSMeta* resolved_meta = resolved_view.ts_meta();
    if (resolved_meta == nullptr || resolved_meta->kind == TSKind::REF) {
        return std::nullopt;
    }

    return resolved;
}

inline std::optional<ViewData> resolve_outer_binding_target(const TSView& outer_any) {
    if (!outer_any) {
        return std::nullopt;
    }

    ViewData bound_target{};
    const TSMeta* outer_meta = outer_any.ts_meta();
    if (outer_meta != nullptr && outer_meta->kind == TSKind::REF) {
        if (resolve_bound_target_view_data(outer_any.view_data(), bound_target)) {
            return bound_target;
        }
        ViewData ref_target{};
        if (resolve_ref_value_target_view_data(outer_any, ref_target)) {
            return ref_target;
        }
        return std::nullopt;
    }

    if (resolve_bound_target_view_data(outer_any.view_data(), bound_target)) {
        return bound_target;
    }
    return outer_any.view_data();
}

struct BindingTargetComparison {
    std::optional<ViewData> current_inner_target;
    std::optional<ViewData> desired_outer_target;
    bool                    binding_changed{false};
};

inline BindingTargetComparison compare_binding_targets(const TSInputView& inner_ts, const TSView& outer_any) {
    BindingTargetComparison out{};

    ViewData current_bound_target{};
    if (resolve_bound_target_view_data(inner_ts.as_ts_view().view_data(), current_bound_target)) {
        out.current_inner_target = current_bound_target;
    }

    out.desired_outer_target = resolve_outer_binding_target(outer_any);
    out.binding_changed =
        out.current_inner_target.has_value() != out.desired_outer_target.has_value() ||
        (out.current_inner_target.has_value() && out.desired_outer_target.has_value() &&
         !same_view_identity(*out.current_inner_target, *out.desired_outer_target));

    return out;
}

inline TSInputView node_input_field(Node& node, std::string_view name) {
    auto root = node.input();
    if (!root) {
        return {};
    }
    auto bundle_opt = root.try_as_bundle();
    if (!bundle_opt.has_value()) {
        return {};
    }
    return bundle_opt->field(name);
}

inline TSInputView node_inner_ts_input(Node& node, bool fallback_to_first = false) {
    auto root = node.input();
    if (!root) {
        return {};
    }

    auto bundle_opt = root.try_as_bundle();
    if (!bundle_opt.has_value()) {
        return {};
    }

    auto ts = bundle_opt->field("ts");
    if (!ts && fallback_to_first && bundle_opt->count() > 0) {
        ts = bundle_opt->at(0);
    }
    return ts;
}

template <typename KeySetT>
inline bool collect_tsd_key_set(const TSInputView& keys_view, KeySetT& out) {
    out.clear();
    value::View keys_value = keys_view.value();
    if (!keys_value.valid()) {
        return true;
    }

    if (keys_value.is_set()) {
        for (value::View key : keys_value.as_set()) {
            out.insert(key.clone());
        }
        return true;
    }

    if (keys_value.is_map()) {
        for (value::View key : keys_value.as_map().keys()) {
            out.insert(key.clone());
        }
        return true;
    }

    return false;
}

inline std::optional<value::Value> key_value_from_python(const nb::object& key_obj, const value::TypeMeta* key_type_meta) {
    if (key_type_meta == nullptr) {
        return std::nullopt;
    }
    value::Value key_value(key_type_meta);
    key_value.emplace();
    key_type_meta->ops().from_python(key_value.data(), key_obj, key_type_meta);
    return key_value;
}

template <typename OnAdded, typename OnRemoved>
inline bool for_each_tsd_key_delta(const TSInputView& keys_view,
                                   const value::TypeMeta* key_type_meta,
                                   OnAdded&& on_added,
                                   OnRemoved&& on_removed) {
    if (key_type_meta == nullptr) {
        return false;
    }

    value::View delta = keys_view.delta_value().value();
    if (!delta.valid() || !delta.is_tuple()) {
        return false;
    }
    auto tuple = delta.as_tuple();

    bool has_delta = false;
    if (tuple.size() > 0) {
        value::View added_view = tuple.at(0);
        if (added_view.valid() && added_view.is_set()) {
            for (value::View key : added_view.as_set()) {
                if (!key.valid()) {
                    continue;
                }
#ifndef NDEBUG
                if (key_type_meta != nullptr) {
                    assert(key.schema() == key_type_meta && "for_each_tsd_key_delta: key schema mismatch");
                }
#endif
                on_added(key);
                has_delta = true;
            }
        }
    }
    if (tuple.size() > 1) {
        value::View removed_view = tuple.at(1);
        if (removed_view.valid() && removed_view.is_set()) {
            for (value::View key : removed_view.as_set()) {
                if (!key.valid()) {
                    continue;
                }
#ifndef NDEBUG
                if (key_type_meta != nullptr) {
                    assert(key.schema() == key_type_meta && "for_each_tsd_key_delta: key schema mismatch");
                }
#endif
                on_removed(key);
                has_delta = true;
            }
        }
    }

    return has_delta;
}

inline bool extract_tsd_key_delta(const TSInputView& keys_view,
                                  const value::TypeMeta* key_type_meta,
                                  std::vector<value::Value>& added_out,
                                  std::vector<value::Value>& removed_out) {
    added_out.clear();
    removed_out.clear();
    return for_each_tsd_key_delta(
        keys_view,
        key_type_meta,
        [&](value::View key) { added_out.emplace_back(key.clone()); },
        [&](value::View key) { removed_out.emplace_back(key.clone()); });
}

inline bool extract_tsd_key_delta_from_tsd(const TSView& tsd_view,
                                           const value::TypeMeta* key_type_meta,
                                           std::vector<value::Value>& added_out,
                                           std::vector<value::Value>& removed_out) {
    added_out.clear();
    removed_out.clear();
    if (!tsd_view || key_type_meta == nullptr) {
        return false;
    }

    value::View delta = tsd_view.delta_value().value();
    if (!delta.valid() || !delta.is_tuple()) {
        return false;
    }
    auto tuple = delta.as_tuple();

    value::SetView removed_set{};
    if (tuple.size() > 2) {
        value::View removed_view = tuple.at(2);
        if (removed_view.valid() && removed_view.is_set()) {
            removed_set = removed_view.as_set();
            for (value::View key : removed_set) {
                if (!key.valid()) {
                    continue;
                }
                removed_out.emplace_back(key.clone());
            }
        }
    }

    if (tuple.size() > 1) {
        value::View added_view = tuple.at(1);
        if (added_view.valid() && added_view.is_set()) {
            value::SetView added_set = added_view.as_set();
            for (value::View key : added_set) {
                if (!key.valid()) {
                    continue;
                }
                if (removed_set.valid() &&
                    key.schema() == removed_set.element_type() &&
                    removed_set.contains(key)) {
                    continue;
                }
                added_out.emplace_back(key.clone());
            }
        }
    }

    return !added_out.empty() || !removed_out.empty();
}

inline bool extract_tsd_key_delta_from_tsd(const TSInputView& tsd_input,
                                           const value::TypeMeta* key_type_meta,
                                           std::vector<value::Value>& added_out,
                                           std::vector<value::Value>& removed_out) {
    if (!tsd_input) {
        added_out.clear();
        removed_out.clear();
        return false;
    }
    return extract_tsd_key_delta_from_tsd(tsd_input.as_ts_view(), key_type_meta, added_out, removed_out);
}

inline bool extract_tsd_changed_keys(const TSView& tsd_view,
                                     const value::TypeMeta* key_type_meta,
                                     std::vector<value::Value>& out) {
    out.clear();
    if (!tsd_view || key_type_meta == nullptr) {
        return false;
    }

    const TSMeta* meta = tsd_view.ts_meta();
    while (meta != nullptr && meta->kind == TSKind::REF) {
        meta = meta->element_ts();
    }
    if (meta == nullptr || meta->kind != TSKind::TSD) {
        return false;
    }

    TSDView tsd_dict(tsd_view);
    for (value::View key : tsd_dict.modified_keys()) {
        if (!key.valid()) {
            continue;
        }
        out.emplace_back(key.clone());
    }
    return true;
}

inline bool extract_tsd_changed_keys(const TSInputView& tsd_input,
                                     const value::TypeMeta* key_type_meta,
                                     std::vector<value::Value>& out) {
    if (!tsd_input) {
        out.clear();
        return false;
    }
    return extract_tsd_changed_keys(tsd_input.as_ts_view(), key_type_meta, out);
}

inline bool extract_tsd_changed_value_for_key(const TSInputView& input_view,
                                              const value::View& key,
                                              value::Value& out_value) {
    if (!input_view || !key.valid()) {
        return false;
    }

    value::View delta = input_view.delta_value().value();
    if (!delta.valid() || !delta.is_tuple()) {
        return false;
    }
    auto tuple = delta.as_tuple();
    if (tuple.size() == 0) {
        return false;
    }

    value::View changed_slot = tuple.at(0);
    if (!changed_slot.valid() || !changed_slot.is_map()) {
        return false;
    }
    const value::MapView changed_map = changed_slot.as_map();
    if (key.schema() != changed_map.key_type() || !changed_map.contains(key)) {
        return false;
    }

    value::View changed_value;
    try {
        changed_value = changed_map.at(key);
    } catch (const std::runtime_error&) {
        return false;
    }
    if (!changed_value.valid()) {
        return false;
    }

    out_value = changed_value.clone();
    return true;
}

inline bool keyed_delta_lookup_cache_matches(const KeyedDeltaLookupCacheEntry& cache,
                                             const ViewData& view_data,
                                             const value::TypeMeta* key_type_meta,
                                             engine_time_t evaluation_time) {
    return cache.value_data == view_data.value_data &&
           cache.delta_data == view_data.delta_data &&
           cache.observer_data == view_data.observer_data &&
           cache.link_data == view_data.link_data &&
           cache.path == view_data.path.indices &&
           cache.key_type_meta == key_type_meta &&
           cache.evaluation_time == evaluation_time;
}

inline void populate_keyed_delta_lookup_cache(KeyedDeltaLookupCacheEntry& cache,
                                              const TSInputView& input_view,
                                              const value::TypeMeta* key_type_meta) {
    cache.clear();
    cache.key_type_meta = key_type_meta;
    cache.evaluation_time = input_view.current_time();

    if (!input_view || key_type_meta == nullptr) {
        return;
    }

    const ViewData& view_data = input_view.as_ts_view().view_data();
    cache.value_data = view_data.value_data;
    cache.delta_data = view_data.delta_data;
    cache.observer_data = view_data.observer_data;
    cache.link_data = view_data.link_data;
    cache.path = view_data.path.indices;

    value::View delta_view = input_view.delta_value().value();
    if (!delta_view.valid() || !delta_view.is_tuple()) {
        return;
    }

    auto tuple = delta_view.as_tuple();
    if (tuple.size() == 0) {
        return;
    }

    value::View changed_slot = tuple.at(0);
    if (!changed_slot.valid() || !changed_slot.is_map()) {
        return;
    }

    const value::MapView changed_map = changed_slot.as_map();
    for (value::View map_key : changed_map.keys()) {
        if (!map_key.valid()) {
            continue;
        }
        if (key_type_meta != nullptr && map_key.schema() != key_type_meta) {
            continue;
        }
        value::View map_value;
        try {
            map_value = changed_map.at(map_key);
        } catch (const std::runtime_error&) {
            // TSD delta maps can contain removal-only keys without a value payload.
            continue;
        }
        if (!map_value.valid()) {
            continue;
        }
        cache.values.insert_or_assign(map_key.clone(), map_value.clone());
    }
}

inline std::optional<value::Value> lookup_keyed_delta_value(const TSInputView& input_view,
                                                            const value::View& key,
                                                            const value::TypeMeta* key_type_meta) {
    if (!input_view || !key.valid() || key_type_meta == nullptr) {
        return std::nullopt;
    }

    const ViewData& view_data = input_view.as_ts_view().view_data();
    const engine_time_t evaluation_time = input_view.current_time();
    auto* cache_root = static_cast<PythonValueCacheNode*>(view_data.python_value_cache_data);
    if (cache_root == nullptr) {
        KeyedDeltaLookupCacheEntry transient_cache;
        populate_keyed_delta_lookup_cache(transient_cache, input_view, key_type_meta);
        auto it = transient_cache.values.find(key);
        if (it == transient_cache.values.end()) {
            return std::nullopt;
        }
        return it->second.view().clone();
    }

    KeyedDeltaLookupCacheEntry* cache = cache_root->keyed_delta_lookup_cache();
    if (!keyed_delta_lookup_cache_matches(*cache, view_data, key_type_meta, evaluation_time)) {
        populate_keyed_delta_lookup_cache(*cache, input_view, key_type_meta);
    }

    auto it = cache->values.find(key);
    if (it == cache->values.end()) {
        return std::nullopt;
    }
    return it->second.view().clone();
}

inline TSView resolve_tsd_child_view(const TSInputView& tsd_input, const value::View& key) {
    if (!tsd_input || !key.valid()) {
        return {};
    }

    const engine_time_t* input_time_ptr = tsd_input.as_ts_view().view_data().engine_time_ptr;
    const auto normalize_child = [input_time_ptr](TSView child) -> TSView {
        if (!child) {
            return {};
        }
        if (child.valid()) {
            return child;
        }
        ViewData resolved_target{};
        if (resolve_bound_target_view_data(child.view_data(), resolved_target)) {
            return TSView(resolved_target, input_time_ptr);
        }
        return child;
    };

    auto tsd_opt = tsd_input.try_as_dict();
    if (!tsd_opt.has_value()) {
        return {};
    }

    TSView direct_child = normalize_child(tsd_opt->as_ts_view().as_dict().at_key(key));
    if (direct_child && direct_child.valid()) {
        return direct_child;
    }

    ViewData bound_target{};
    if (resolve_bound_target_view_data(tsd_opt->as_ts_view().view_data(), bound_target)) {
        TSView bound_child = normalize_child(TSView(bound_target, input_time_ptr).child_by_key(key));
        if (bound_child && bound_child.valid()) {
            return bound_child;
        }
        if (bound_child) {
            return bound_child;
        }
    }

    return direct_child;
}

template <typename ResolveDirectViewFn, typename LocalValuesAccessorFn>
inline TSView resolve_keyed_view_with_delta_fallback(const value::View& key,
                                                     const TSInputView& outer_input,
                                                     const TSInputView& inner_ts,
                                                     const value::TypeMeta* key_type_meta,
                                                     ResolveDirectViewFn&& resolve_direct_view,
                                                     LocalValuesAccessorFn&& local_values_accessor,
                                                     bool* has_outer_key = nullptr,
                                                     bool* outer_key_valid = nullptr,
                                                     bool* used_local_fallback = nullptr,
                                                     std::optional<value::Value>* fallback_delta = nullptr,
                                                     int* stage_id = nullptr) {
    if (has_outer_key != nullptr) {
        *has_outer_key = false;
    }
    if (outer_key_valid != nullptr) {
        *outer_key_valid = false;
    }
    if (used_local_fallback != nullptr) {
        *used_local_fallback = false;
    }
    if (fallback_delta != nullptr) {
        fallback_delta->reset();
    }
    if (stage_id != nullptr) {
        *stage_id = 1;
    }

    if (!outer_input || !inner_ts || !key.valid()) {
        return {};
    }

    TSView outer_key_view = resolve_direct_view(outer_input, key);
    const bool has_key = static_cast<bool>(outer_key_view);
    const bool key_valid = has_key && outer_key_view.valid();
    if (has_outer_key != nullptr) {
        *has_outer_key = has_key;
    }
    if (outer_key_valid != nullptr) {
        *outer_key_valid = key_valid;
    }
    if (key_valid) {
        return outer_key_view;
    }

    // Delta fallback is only relevant on modified ticks with a delta payload.
    if (!outer_input.modified() || !outer_input.has_delta() || key_type_meta == nullptr) {
        return outer_key_view;
    }

    if (stage_id != nullptr) {
        *stage_id = 2;
    }
    auto delta_value = hgraph::lookup_keyed_delta_value(outer_input, key, key_type_meta);
    const TSMeta* inner_meta = inner_ts.ts_meta();
    const TSMeta* fallback_meta =
        (inner_meta != nullptr && inner_meta->kind == TSKind::REF) ? inner_meta->element_ts() : inner_meta;
    if (!delta_value.has_value() || fallback_meta == nullptr) {
        return outer_key_view;
    }

    if (stage_id != nullptr) {
        *stage_id = 3;
    }
    auto& local_values = local_values_accessor();
    auto it = local_values.find(key);
    if (it == local_values.end()) {
        auto [inserted_it, _] = local_values.emplace(key.clone(), std::make_unique<TSValue>(fallback_meta));
        it = inserted_it;
    }

    if (stage_id != nullptr) {
        *stage_id = 4;
    }
    TSView staged_view = it->second->ts_view(inner_ts.as_ts_view().view_data().engine_time_ptr);
    staged_view.set_value(delta_value->view());
    if (used_local_fallback != nullptr) {
        *used_local_fallback = true;
    }
    if (fallback_delta != nullptr) {
        *fallback_delta = std::move(*delta_value);
    }
    return staged_view;
}

inline void bind_inner_from_outer(const TSView& outer_any,
                                  TSInputView inner_any,
                                  RefBindOrder ref_bind_order = RefBindOrder::RefValueThenBoundTarget) {
    if (!inner_any) {
        return;
    }

    if (!outer_any) {
        inner_any.unbind();
        return;
    }

    const engine_time_t* inner_time_ptr = inner_any.as_ts_view().view_data().engine_time_ptr;

    const auto bind_from_outer = [&]() {
        inner_any.as_ts_view().bind(TSView(outer_any.view_data(), inner_time_ptr));
    };

    const auto bind_bound_target_if_present = [&]() -> bool {
        ViewData bound_target{};
        if (!resolve_bound_target_view_data(outer_any.view_data(), bound_target)) {
            return false;
        }
        inner_any.as_ts_view().bind(TSView(bound_target, inner_time_ptr));
        return true;
    };

    const auto bind_ref_value_if_present = [&]() -> bool {
        value::View ref_view = outer_any.value();
        if (!ref_view.valid()) {
            return false;
        }
        const auto& ref = *static_cast<const TimeSeriesReference*>(ref_view.data());
        ref.bind_input(inner_any);
        return true;
    };

    const TSMeta* outer_meta = outer_any.ts_meta();
    if (outer_meta != nullptr && outer_meta->kind == TSKind::REF) {
        if (ref_bind_order == RefBindOrder::BoundTargetThenRefValue) {
            if (bind_bound_target_if_present()) {
                return;
            }
            if (bind_ref_value_if_present()) {
                return;
            }
        } else {
            if (bind_ref_value_if_present()) {
                return;
            }
            if (bind_bound_target_if_present()) {
                return;
            }
        }

        bind_from_outer();
        return;
    }

    if (!bind_bound_target_if_present()) {
        bind_from_outer();
    }
}

}  // namespace hgraph
