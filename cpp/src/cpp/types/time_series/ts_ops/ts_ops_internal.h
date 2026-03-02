#pragma once

#include <hgraph/types/time_series/ts_ops.h>

#include <hgraph/types/time_series/link_observer_registry.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/python_value_cache_stats.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/value/cyclic_buffer_ops.h>
#include <hgraph/types/value/map_storage.h>
#include <hgraph/types/value/queue_ops.h>
#include <hgraph/types/value/type_registry.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#ifndef HGRAPH_DEBUG_ENV_ENABLED
#define HGRAPH_DEBUG_ENV_ENABLED(NAME_LITERAL)                                                \
    ([]() -> bool {                                                                           \
        static const bool enabled = std::getenv(NAME_LITERAL) != nullptr;                    \
        return enabled;                                                                       \
    }())
#endif

namespace hgraph {

using value::View;
using value::Value;
using value::ValueView;

[[nodiscard]] inline bool allow_pretick_delta(const ViewData& vd, engine_time_t current_time) noexcept {
    return current_time == MIN_DT &&
           (vd.delta_semantics == DeltaSemantics::AllowPreTickDelta || vd.engine_time_ptr == nullptr);
}

struct TSSDeltaSlots {
    ValueView slot;
    ValueView added_set;
    ValueView removed_set;
};

struct TSDDeltaSlots {
    ValueView slot;
    ValueView changed_values_map;
    ValueView added_set;
    ValueView removed_set;
};

struct TSWTickDeltaSlots {
    ValueView slot;
    ValueView removed_value;
    ValueView has_removed;
};

struct TSWDurationDeltaSlots {
    ValueView slot;
    ValueView has_removed;
    ValueView removed_values;
};

struct TsdRemovedChildSnapshotRecord {
    std::vector<size_t> parent_path;
    engine_time_t time{MIN_DT};
    value::Value key;
    std::shared_ptr<TSValue> snapshot;
};

struct TsdRemovedChildSnapshotState {
    std::unordered_map<void*, std::vector<TsdRemovedChildSnapshotRecord>> entries;
};

struct TsdVisibleKeyHistoryRecord {
    std::vector<size_t> parent_path;
    value::Value key;
    engine_time_t last_seen{MIN_DT};
};

struct TsdVisibleKeyHistoryState {
    std::unordered_map<void*, std::vector<TsdVisibleKeyHistoryRecord>> entries;
};

struct RefUnboundItemChangeRecord {
    std::vector<size_t> path;
    engine_time_t time{MIN_DT};
    std::vector<size_t> changed_indices;
};

struct RefUnboundItemChangeState {
    std::unordered_map<void*, std::vector<RefUnboundItemChangeRecord>> entries;
};

struct TSDKeySetDeltaState {
    bool has_delta_tuple{false};
    bool has_changed_values_map{false};
    bool has_added{false};
    bool has_removed{false};
};

struct TSDKeySetBridgeState {
    ViewData previous_bridge{};
    ViewData current_bridge{};
    ViewData previous_source{};
    ViewData current_source{};
    bool has_bridge{false};
    bool has_previous_source{false};
    bool has_current_source{false};
};

constexpr std::string_view k_tsd_removed_snapshot_state_key{
    TSLinkObserverRegistry::kTsdRemovedChildSnapshotsKey};
constexpr std::string_view k_tsd_visible_key_history_state_key{
    TSLinkObserverRegistry::kTsdVisibleKeyHistoryKey};
constexpr std::string_view k_ref_unbound_item_change_state_key{
    TSLinkObserverRegistry::kRefUnboundItemChangesKey};

extern const ts_window_ops k_window_tick_ops;
extern const ts_window_ops k_window_duration_ops;
extern const ts_set_ops k_set_ops;
extern const ts_dict_ops k_dict_ops;
extern const ts_list_ops k_list_ops;
extern const ts_bundle_ops k_bundle_ops;

TSView op_child_at(const ViewData& vd, size_t index, engine_time_t current_time);
TSView op_child_by_name(const ViewData& vd, std::string_view name, engine_time_t current_time);
TSView op_child_by_key(const ViewData& vd, const View& key, engine_time_t current_time);
size_t op_list_size(const ViewData& vd);
size_t op_bundle_size(const ViewData& vd);
View op_observer(const ViewData& vd);
void op_notify_observers(ViewData& vd, engine_time_t current_time);
void op_bind(ViewData& vd, const ViewData& target, engine_time_t current_time);
void op_unbind(ViewData& vd, engine_time_t current_time);
bool op_is_bound(const ViewData& vd);
void set_active_flag(value::ValueView active_view, bool active);
bool active_flag_value(value::ValueView active_view);
void op_set_active(ViewData& vd, ValueView active_view, bool active, TSInput* input);
void op_copy_scalar(ViewData dst, const ViewData& src, engine_time_t current_time);
void op_copy_ref(ViewData dst, const ViewData& src, engine_time_t current_time);
void op_copy_tss(ViewData dst, const ViewData& src, engine_time_t current_time);
void op_copy_tsd(ViewData dst, const ViewData& src, engine_time_t current_time);
void op_copy_tsl(ViewData dst, const ViewData& src, engine_time_t current_time);
void op_copy_tsb(ViewData dst, const ViewData& src, engine_time_t current_time);
const TSMeta* op_ts_meta(const ViewData& vd);
const TSMeta* op_ts_meta_tsd_key_set(const ViewData& vd);
ViewData dispatch_view_for_path(const ViewData& view);
engine_time_t dispatch_last_modified_time(const ViewData& view);
bool dispatch_modified(const ViewData& view, engine_time_t current_time);
bool dispatch_valid(const ViewData& view);
bool meta_is_scalar_non_ref(const TSMeta* meta);
bool meta_is_scalar_like_or_ref(const TSMeta* meta);
bool rebind_bridge_has_container_meta_value(const ViewData& vd,
                                            const TSMeta* self_meta,
                                            engine_time_t current_time,
                                            const TSMeta* container_meta);
bool view_matches_container_meta_value(const std::optional<View>& value, const TSMeta* container_meta);
bool modified_default_tail(const ViewData& vd, engine_time_t current_time);
std::optional<bool> modified_from_key_set_source(const ViewData& vd,
                                                 engine_time_t current_time,
                                                 bool debug_keyset_bridge,
                                                 bool enable_bridge_logic);
std::optional<bool> valid_from_key_set_source(const ViewData& vd, bool debug_keyset_valid);
bool valid_from_resolved_slot(const ViewData& vd, const TSMeta* self_meta, bool ref_wrapper_mode);
engine_time_t last_modified_fallback_no_dispatch(const ViewData& vd,
                                                 bool allow_ops_dispatch,
                                                 bool include_wrapper_time,
                                                 bool include_map_children,
                                                 bool include_static_children);
bool modified_fallback_no_dispatch(const ViewData& vd, engine_time_t current_time, bool allow_ops_dispatch);
bool valid_fallback_no_dispatch(const ViewData& vd, bool allow_ops_dispatch);
bool resolve_signal_source_view(const ViewData& vd,
                                const LinkTarget& signal_link,
                                ViewData& source_view,
                                bool& source_is_signal);
engine_time_t signal_last_modified_time(const ViewData& vd,
                                        const LinkTarget& signal_link,
                                        engine_time_t base_time);
std::optional<bool> signal_valid_override(const ViewData& vd, const LinkTarget& signal_link);
engine_time_t op_last_modified_time(const ViewData& vd);
engine_time_t op_last_modified_tsvalue(const ViewData& vd);
engine_time_t op_last_modified_ref(const ViewData& vd);
engine_time_t op_last_modified_signal(const ViewData& vd);
engine_time_t op_last_modified_tsw(const ViewData& vd);
engine_time_t op_last_modified_tss(const ViewData& vd);
engine_time_t op_last_modified_tsd(const ViewData& vd);
engine_time_t op_last_modified_tsb(const ViewData& vd);
engine_time_t op_last_modified_tsl(const ViewData& vd);
bool op_modified(const ViewData& vd, engine_time_t current_time);
bool op_modified_ref(const ViewData& vd, engine_time_t current_time);
bool op_modified_ref_scalar(const ViewData& vd, engine_time_t current_time);
bool op_modified_ref_static_container(const ViewData& vd, engine_time_t current_time);
bool op_modified_ref_dynamic_container(const ViewData& vd, engine_time_t current_time);
bool op_modified_tsvalue(const ViewData& vd, engine_time_t current_time);
bool op_modified_signal(const ViewData& vd, engine_time_t current_time);
bool op_modified_tsw(const ViewData& vd, engine_time_t current_time);
bool op_modified_tss(const ViewData& vd, engine_time_t current_time);
bool op_modified_tsd(const ViewData& vd, engine_time_t current_time);
bool op_modified_tsd_key_set(const ViewData& vd, engine_time_t current_time);
bool op_modified_tsb(const ViewData& vd, engine_time_t current_time);
bool op_modified_tsl(const ViewData& vd, engine_time_t current_time);
bool op_valid(const ViewData& vd);
bool op_valid_tsvalue(const ViewData& vd);
bool op_valid_ref(const ViewData& vd);
bool op_valid_ref_scalar(const ViewData& vd);
bool op_valid_ref_static_container(const ViewData& vd);
bool op_valid_ref_dynamic_container(const ViewData& vd);
bool op_valid_signal(const ViewData& vd);
bool op_valid_tsw(const ViewData& vd);
bool op_valid_tss(const ViewData& vd);
bool op_valid_tsd(const ViewData& vd);
bool op_valid_tsd_key_set(const ViewData& vd);
bool op_valid_tsb(const ViewData& vd);
bool op_valid_tsl(const ViewData& vd);
bool op_all_valid(const ViewData& vd);
bool op_all_valid_tsw(const ViewData& vd);
bool op_all_valid_tsw_tick(const ViewData& vd);
bool op_all_valid_tsw_duration(const ViewData& vd);
bool op_all_valid_tsb(const ViewData& vd);
bool op_all_valid_tsl(const ViewData& vd);
bool op_sampled(const ViewData& vd);
View op_value(const ViewData& vd);
View op_value_non_ref(const ViewData& vd);
View op_value_ref(const ViewData& vd);
View op_delta_value(const ViewData& vd);
View op_delta_value_scalar(const ViewData& vd);
View op_delta_value_container(const ViewData& vd);
View op_delta_value_tsw(const ViewData& vd);
View op_delta_value_tsw_tick(const ViewData& vd);
View op_delta_value_tsw_duration(const ViewData& vd);
bool op_has_delta(const ViewData& vd);
bool op_has_delta_default(const ViewData& vd);
bool op_has_delta_scalar(const ViewData& vd);
bool op_has_delta_tss(const ViewData& vd);
bool op_has_delta_tsd(const ViewData& vd);
bool op_has_delta_tsd_key_set(const ViewData& vd);
void op_set_value(ViewData& vd, const View& src, engine_time_t current_time);
void op_apply_delta(ViewData& vd, const View& delta, engine_time_t current_time);
void op_apply_delta_scalar(ViewData& vd, const View& delta, engine_time_t current_time);
void op_apply_delta_container(ViewData& vd, const View& delta, engine_time_t current_time);
void op_invalidate(ViewData& vd);
void op_invalidate_tsd(ViewData& vd);
size_t static_container_child_count(const TSMeta* meta);
bool link_target_points_to_unbound_ref_composite(const ViewData& vd, const LinkTarget* payload);
bool is_unpeered_static_container_view(const ViewData& vd, const TSMeta* current);
engine_time_t extract_time_value(const View& time_view);
engine_time_t* extract_time_ptr(ValueView time_view);
std::optional<View> resolve_link_view(const ViewData& vd, const std::vector<size_t>& ts_path);
const value::TypeMeta* link_target_meta();
const value::TypeMeta* ref_link_meta();
const value::TypeMeta* ts_reference_meta();
LinkTarget* resolve_link_target(const ViewData& vd, const std::vector<size_t>& ts_path);
REFLink* resolve_ref_link(const ViewData& vd, const std::vector<size_t>& ts_path);
engine_time_t* resolve_owner_time_ptr(ViewData& vd);
void ensure_tsd_child_time_slot(ViewData& vd, size_t child_slot);
std::optional<ValueView> resolve_tsd_child_link_list(ViewData& vd);
void ensure_tsd_child_link_slot(ViewData& vd, size_t child_slot);
bool is_valid_list_slot(const value::ListView& list, size_t slot);
void clear_list_slot(value::ListView list, size_t slot);
void compact_tsd_child_link_slot(ViewData& vd, size_t child_slot);
std::optional<ValueView> resolve_tsd_child_time_list(ViewData& vd);
void compact_tsd_child_time_slot(ViewData& vd, size_t child_slot);
void ensure_tsd_child_delta_slot(ViewData& vd, size_t child_slot);
void compact_tsd_child_delta_slot(ViewData& vd, size_t child_slot);
LinkTarget* resolve_parent_link_target(const ViewData& vd);
const TSMeta* resolve_meta_or_ancestor(const ViewData& vd, bool& used_ancestor);
std::vector<size_t> link_residual_ts_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path);
ViewProjection merge_projection(ViewProjection requested, ViewProjection resolved);
std::optional<ViewData> resolve_bound_view_data(const ViewData& vd);
TSInput* notifier_as_live_input(Notifiable* notifier);
engine_time_t view_evaluation_time(const ViewData& vd);
bool is_prefix_path(const std::vector<size_t>& lhs, const std::vector<size_t>& rhs);
bool paths_related(const std::vector<size_t>& lhs, const std::vector<size_t>& rhs);
bool is_static_container_meta(const TSMeta* meta);
bool observer_under_static_ref_container(const LinkTarget& observer);
bool signal_input_has_bind_impl(const ViewData& vd, const TSMeta* current_meta, const LinkTarget* signal_link);
engine_time_t resolve_input_current_time(const TSInput* input);
void notify_activation_if_modified(LinkTarget* payload, TSInput* input);
void notify_activation_if_modified(REFLink* payload, TSInput* input);
void unregister_link_target_observer(const LinkTarget& link_target);
void unregister_ref_link_observer(const REFLink& ref_link);
void register_link_target_observer(const LinkTarget& link_target);
void refresh_dynamic_ref_binding_for_link_target(LinkTarget* link_target, bool sampled, engine_time_t current_time);
bool view_path_contains_tsd_ancestor(const ViewData& view);
void register_ref_link_observer(const REFLink& ref_link, const ViewData* observer_view = nullptr);
bool suppress_static_ref_child_notification(const LinkTarget& observer, engine_time_t current_time);
void notify_link_target_observers(const ViewData& target_view, engine_time_t current_time);
ts_ops make_common_ops(TSKind kind);
ts_ops make_tsw_tick_ops();
ts_ops make_tsw_duration_ops();
ts_ops make_tss_ops();
ts_ops make_tsd_ops();
ts_ops make_tsl_ops();
ts_ops make_tsb_ops();
void copy_tss(ViewData dst, const ViewData& src, engine_time_t current_time);
void copy_tsd(ViewData dst, const ViewData& src, engine_time_t current_time);
void copy_view_data_value_impl(ViewData dst, const ViewData& src, engine_time_t current_time);
const TSMeta* meta_at_path(const TSMeta* root, const std::vector<size_t>& indices);
void bind_view_data_ops(ViewData& vd);
size_t find_bundle_field_index(const TSMeta* bundle_meta, std::string_view field_name);
std::optional<View> navigate_const(View view, const std::vector<size_t>& indices);
std::optional<ValueView> navigate_mut(ValueView view, const std::vector<size_t>& indices);
void copy_view_data(ValueView dst, const View& src);
void clear_map_slot(value::ValueView map_view);
bool tsd_child_was_visible_before_removal(const ViewData& child_vd);
std::optional<ValueView> resolve_delta_slot_mut(ViewData& vd);
TSSDeltaSlots resolve_tss_delta_slots(ViewData& vd);
TSDDeltaSlots resolve_tsd_delta_slots(ViewData& vd);
TSWTickDeltaSlots resolve_tsw_tick_delta_slots(ViewData& vd);
TSWDurationDeltaSlots resolve_tsw_duration_delta_slots(ViewData& vd);
bool set_view_empty(ValueView v);
bool map_view_empty(ValueView v);
bool has_delta_descendants(const TSMeta* meta);
nb::object* resolve_python_value_cache_slot(ViewData& vd, bool create);
const nb::object* resolve_python_value_cache_slot(const ViewData& vd);
PythonDeltaCacheEntry* resolve_python_delta_cache_slot(ViewData& vd, bool create);
const PythonDeltaCacheEntry* resolve_python_delta_cache_slot(const ViewData& vd);
void seed_python_value_cache_slot(ViewData& vd, const nb::object& value);
void seed_python_value_cache_slot_from_view(ViewData& vd, const View& value);
void invalidate_python_value_cache(ViewData& vd);
std::optional<std::vector<size_t>> ts_path_to_delta_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path);
const value::MapStorage* map_storage_for_read(const value::MapView& map);
std::optional<size_t> map_slot_for_key(const value::MapView& map, const View& key);
bool set_contains_key_relaxed(const value::SetView& set, const View& key);
bool view_is_set_and_contains_key_relaxed(const View& maybe_set, const View& key);
std::optional<Value> map_key_at_slot(const value::MapView& map, size_t slot_index);
Value canonical_map_key_for_slot(const value::MapView& map, size_t slot_index, const View& fallback_key);
void mark_tsd_parent_child_modified(ViewData child_vd, engine_time_t current_time);
bool tss_delta_empty(const TSSDeltaSlots& slots);
bool tsd_delta_empty(const TSDDeltaSlots& slots);
void clear_tss_delta_slots(TSSDeltaSlots slots);
void clear_tsd_delta_slots(TSDDeltaSlots slots);
void clear_tsw_tick_delta_slots(TSWTickDeltaSlots slots);
void clear_tsw_duration_delta_slots(TSWDurationDeltaSlots slots);
void clear_tss_delta_if_new_tick(ViewData& vd, engine_time_t current_time, TSSDeltaSlots slots);
void clear_tsd_delta_if_new_tick(ViewData& vd, engine_time_t current_time, TSDDeltaSlots slots);
void clear_tsw_delta_if_new_tick(ViewData& vd, engine_time_t current_time);
std::optional<Value> value_from_python(const value::TypeMeta* type, const nb::object& src);
nb::object attr_or_call(const nb::object& obj, const char* name);
nb::object python_set_delta(const nb::object& added, const nb::object& removed);
std::shared_ptr<TsdRemovedChildSnapshotState> ensure_tsd_removed_snapshot_state(TSLinkObserverRegistry* registry);
std::shared_ptr<TsdVisibleKeyHistoryState> ensure_tsd_visible_key_history_state(TSLinkObserverRegistry* registry);
bool key_matches_relaxed(const value::View& lhs, const value::View& rhs);
void mark_tsd_visible_key_history(const ViewData& parent_view, const value::View& key, engine_time_t current_time);
bool has_tsd_visible_key_history(const ViewData& parent_view, const value::View& key);
void clear_tsd_visible_key_history(const ViewData& parent_view, const value::View& key);
std::vector<size_t> ts_path_to_link_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path);
std::vector<size_t> ts_path_to_time_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path);
std::vector<size_t> ts_path_to_observer_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path);
std::vector<std::vector<size_t>> time_stamp_paths_for_ts_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path);
const ts_ops* get_ts_ops(TSKind kind);
const ts_ops* get_ts_ops(const TSMeta* meta);
const ts_ops* get_ts_ops(const ViewData& view);
const ts_ops* default_ts_ops();

inline const ts_ops* dispatch_meta_ops(const TSMeta* meta) {
    return meta != nullptr ? get_ts_ops(meta) : nullptr;
}

enum class DispatchMetaPathKind : uint8_t {
    Unknown = 0,
    ScalarLike,
    Ref,
    TSS,
    TSD,
    TSB,
    TSLFixed,
    TSLDynamic,
    TSW,
};

inline DispatchMetaPathKind dispatch_meta_path_kind(const TSMeta* meta) {
    const ts_ops* ops = dispatch_meta_ops(meta);
    if (ops == nullptr) {
        return DispatchMetaPathKind::Unknown;
    }
    switch (ops->kind) {
        case TSKind::REF:
            return DispatchMetaPathKind::Ref;
        case TSKind::TSS:
            return DispatchMetaPathKind::TSS;
        case TSKind::TSD:
            return DispatchMetaPathKind::TSD;
        case TSKind::TSB:
            return DispatchMetaPathKind::TSB;
        case TSKind::TSL:
            return meta != nullptr && meta->fixed_size() > 0
                       ? DispatchMetaPathKind::TSLFixed
                       : DispatchMetaPathKind::TSLDynamic;
        case TSKind::TSW:
            return DispatchMetaPathKind::TSW;
        case TSKind::TSValue:
        case TSKind::SIGNAL:
            return DispatchMetaPathKind::ScalarLike;
        default:
            return DispatchMetaPathKind::Unknown;
    }
}

inline bool dispatch_ops_is_tsw_duration(const ts_ops* ops) {
    return ops != nullptr && ops->window_ops() == &k_window_duration_ops;
}

inline bool dispatch_ops_is_ref_wrapper(const ts_ops* ops) {
    return ops != nullptr && ops->value == &op_value_ref;
}

inline bool dispatch_meta_is_ref(const TSMeta* meta) {
    return dispatch_ops_is_ref_wrapper(dispatch_meta_ops(meta));
}

inline bool dispatch_meta_is_tsw(const TSMeta* meta) {
    return dispatch_meta_path_kind(meta) == DispatchMetaPathKind::TSW;
}

inline bool dispatch_meta_is_signal(const TSMeta* meta) {
    if (const ts_ops* ops = dispatch_meta_ops(meta); ops != nullptr) {
        return ops->kind == TSKind::SIGNAL;
    }
    return false;
}

inline bool dispatch_meta_is_tsvalue(const TSMeta* meta) {
    if (const ts_ops* ops = dispatch_meta_ops(meta); ops != nullptr) {
        return ops->kind == TSKind::TSValue;
    }
    return false;
}

inline bool dispatch_meta_is_tss(const TSMeta* meta) {
    return dispatch_meta_path_kind(meta) == DispatchMetaPathKind::TSS;
}

inline bool dispatch_meta_is_tsd(const TSMeta* meta) {
    return dispatch_meta_path_kind(meta) == DispatchMetaPathKind::TSD;
}

inline bool dispatch_meta_is_tsb(const TSMeta* meta) {
    return dispatch_meta_path_kind(meta) == DispatchMetaPathKind::TSB;
}

inline bool dispatch_meta_is_tsl(const TSMeta* meta) {
    const DispatchMetaPathKind kind = dispatch_meta_path_kind(meta);
    return kind == DispatchMetaPathKind::TSLFixed || kind == DispatchMetaPathKind::TSLDynamic;
}

inline bool dispatch_meta_is_fixed_tsl(const TSMeta* meta) {
    return dispatch_meta_path_kind(meta) == DispatchMetaPathKind::TSLFixed;
}

inline bool dispatch_meta_is_static_container(const TSMeta* meta) {
    const DispatchMetaPathKind kind = dispatch_meta_path_kind(meta);
    return kind == DispatchMetaPathKind::TSB || kind == DispatchMetaPathKind::TSLFixed;
}

inline bool dispatch_meta_is_dynamic_container(const TSMeta* meta) {
    const DispatchMetaPathKind kind = dispatch_meta_path_kind(meta);
    return kind == DispatchMetaPathKind::TSD || kind == DispatchMetaPathKind::TSS;
}

inline bool dispatch_meta_is_container_like(const TSMeta* meta) {
    const DispatchMetaPathKind kind = dispatch_meta_path_kind(meta);
    return kind == DispatchMetaPathKind::TSD ||
           kind == DispatchMetaPathKind::TSS ||
           kind == DispatchMetaPathKind::TSB ||
           kind == DispatchMetaPathKind::TSLFixed ||
           kind == DispatchMetaPathKind::TSLDynamic;
}

inline bool dispatch_meta_is_scalar_like(const TSMeta* meta) {
    const DispatchMetaPathKind kind = dispatch_meta_path_kind(meta);
    return kind == DispatchMetaPathKind::ScalarLike ||
           kind == DispatchMetaPathKind::TSW ||
           dispatch_meta_is_ref(meta);
}

inline const TSMeta* dispatch_meta_strip_ref(const TSMeta* meta) {
    while (dispatch_meta_is_ref(meta)) {
        meta = meta->element_ts();
    }
    return meta;
}

inline bool dispatch_meta_step_child_no_ref(const TSMeta*& meta, size_t index) {
    if (meta == nullptr) {
        return false;
    }

    switch (dispatch_meta_path_kind(meta)) {
        case DispatchMetaPathKind::TSB:
            if (meta->fields() == nullptr || index >= meta->field_count()) {
                return false;
            }
            meta = meta->fields()[index].ts_type;
            return true;
        case DispatchMetaPathKind::TSLFixed:
        case DispatchMetaPathKind::TSLDynamic:
        case DispatchMetaPathKind::TSD:
            meta = meta->element_ts();
            return true;
        default:
            return false;
    }
}

inline bool dispatch_meta_step_child(const TSMeta*& meta, size_t index) {
    meta = dispatch_meta_strip_ref(meta);
    return dispatch_meta_step_child_no_ref(meta, index);
}

inline const TSMeta* dispatch_meta_child_no_ref(const TSMeta* meta, size_t index) {
    const TSMeta* out = meta;
    if (!dispatch_meta_step_child_no_ref(out, index)) {
        return nullptr;
    }
    return out;
}

inline const TSMeta* dispatch_meta_child(const TSMeta* meta, size_t index) {
    const TSMeta* out = meta;
    if (!dispatch_meta_step_child(out, index)) {
        return nullptr;
    }
    return out;
}

void store_to_link_target(LinkTarget& target, const ViewData& source);
void store_to_ref_link(REFLink& target, const ViewData& source);
bool resolve_direct_bound_view_data(const ViewData& source, ViewData& out);
bool resolve_bound_target_view_data(const ViewData& source, ViewData& out);
bool resolve_previous_bound_target_view_data(const ViewData& source, ViewData& out);
void copy_view_data_value(ViewData& dst, const ViewData& src, engine_time_t current_time);
void notify_ts_link_observers(const ViewData& target_view, engine_time_t current_time);
void register_ts_link_observer(LinkTarget& observer);
void unregister_ts_link_observer(LinkTarget& observer);
void register_ts_ref_link_observer(REFLink& observer);
void unregister_ts_ref_link_observer(REFLink& observer);
void register_ts_active_link_observer(LinkTarget& observer);
void unregister_ts_active_link_observer(LinkTarget& observer);
void register_ts_active_ref_link_observer(REFLink& observer);
void unregister_ts_active_ref_link_observer(REFLink& observer);
void reset_ts_link_observers();
nb::object op_to_python(const ViewData& vd);
nb::object op_to_python_default(const ViewData& vd);
nb::object op_to_python_ref(const ViewData& vd);
nb::object op_to_python_tss(const ViewData& vd);
nb::object op_to_python_tsd(const ViewData& vd);
nb::object op_to_python_tsd_key_set(const ViewData& vd);
nb::object op_to_python_tsw(const ViewData& vd);
nb::object op_to_python_tsw_tick(const ViewData& vd);
nb::object op_to_python_tsw_duration(const ViewData& vd);
nb::object op_to_python_tsl(const ViewData& vd);
nb::object op_to_python_tsb(const ViewData& vd);
nb::object op_delta_to_python(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_default(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsvalue(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_ref(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_ref_scalar(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_ref_list(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_ref_bundle(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_ref_dynamic(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tss(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsd(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsd_scalar(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsd_nested(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsd_key_set(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsd_ref(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsw(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsw_tick(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsw_duration(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsl(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsb(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsd_impl(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsd_scalar_impl(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsd_nested_impl(const ViewData& vd, engine_time_t current_time);
nb::object op_delta_to_python_tsd_ref_impl(const ViewData& vd, engine_time_t current_time);
nb::object op_ref_payload_to_python(const TimeSeriesReference& ref,
                                    const TSMeta* expected_meta,
                                    engine_time_t current_time,
                                    bool include_unmodified);
nb::object op_ref_payload_to_python_scalar(const TimeSeriesReference& ref,
                                           const TSMeta* expected_meta,
                                           engine_time_t current_time,
                                           bool include_unmodified);
nb::object op_ref_payload_to_python_list(const TimeSeriesReference& ref,
                                         const TSMeta* expected_meta,
                                         engine_time_t current_time,
                                         bool include_unmodified);
nb::object op_ref_payload_to_python_bundle(const TimeSeriesReference& ref,
                                           const TSMeta* expected_meta,
                                           engine_time_t current_time,
                                           bool include_unmodified);
nb::object op_ref_payload_to_python_dynamic(const TimeSeriesReference& ref,
                                            const TSMeta* expected_meta,
                                            engine_time_t current_time,
                                            bool include_unmodified);
nb::object computed_delta_to_python_with_refs(const DeltaView& delta, engine_time_t current_time);
nb::object stored_delta_to_python_with_refs(const View& view, engine_time_t current_time);
nb::object tsd_ref_payload_to_python(const TimeSeriesReference& ref,
                                     const TSMeta* expected_meta,
                                     engine_time_t current_time,
                                     bool include_unmodified);
nb::object tsd_ref_view_payload_to_python(const ViewData& ref_child,
                                          const TSMeta* ref_meta,
                                          engine_time_t current_time,
                                          bool include_unmodified,
                                          bool debug_ref_payload);
bool tsd_has_delta_payload_view(const View& view);
bool tsd_has_delta_payload(const DeltaView& delta);
void tsd_remove_empty_mapping_payloads(nb::dict& delta_out);
void tsd_update_visible_key_history_from_delta(const ViewData& data,
                                               const TSMeta* current,
                                               const nb::dict& delta_out,
                                               engine_time_t current_time);
void tsd_emit_map_delta_plain(const ViewData& vd,
                              const ViewData* data,
                              const TSMeta* current,
                              engine_time_t current_time,
                              bool wrapper_modified,
                              bool resolved_modified,
                              bool debug_tsd_delta,
                              bool debug_ref_payload,
                              const View& changed_values,
                              const View& added_keys,
                              const View& removed_keys,
                              nb::dict& delta_out);
void tsd_emit_map_delta_plain_scalar(const ViewData& vd,
                                     const ViewData* data,
                                     const TSMeta* current,
                                     engine_time_t current_time,
                                     bool wrapper_modified,
                                     bool resolved_modified,
                                     bool debug_tsd_delta,
                                     bool debug_ref_payload,
                                     const View& changed_values,
                                     const View& added_keys,
                                     const View& removed_keys,
                                     nb::dict& delta_out);
void tsd_emit_map_delta_plain_nested(const ViewData& vd,
                                     const ViewData* data,
                                     const TSMeta* current,
                                     engine_time_t current_time,
                                     bool wrapper_modified,
                                     bool resolved_modified,
                                     bool debug_tsd_delta,
                                     bool debug_ref_payload,
                                     const View& changed_values,
                                     const View& added_keys,
                                     const View& removed_keys,
                                     nb::dict& delta_out);
void tsd_emit_map_delta_ref_elements(const ViewData& vd,
                                     const ViewData* data,
                                     const TSMeta* current,
                                     engine_time_t current_time,
                                     bool wrapper_modified,
                                     bool resolved_modified,
                                     bool debug_tsd_delta,
                                     bool debug_ref_payload,
                                     const View& changed_values,
                                     const View& added_keys,
                                     const View& removed_keys,
                                     nb::dict& delta_out);
std::optional<nb::object> maybe_tsd_key_set_delta_to_python(const ViewData& vd,
                                                            engine_time_t current_time,
                                                            bool debug_delta_kind,
                                                            bool debug_keyset_bridge,
                                                            bool emit_first_bind_all_added,
                                                            bool allow_bridge_fallback);
std::shared_ptr<RefUnboundItemChangeState> ensure_ref_unbound_item_change_state(TSLinkObserverRegistry* registry);
bool unbound_ref_item_changed_this_tick(const ViewData& item_view, size_t item_index, engine_time_t current_time);
void record_tsd_removed_child_snapshot(const ViewData& parent_view,
                                       const View& key,
                                       const ViewData& child_view,
                                       engine_time_t current_time);
void op_from_python(ViewData& vd, const nb::object& src, engine_time_t current_time);
void op_from_python_scalar(ViewData& vd, const nb::object& src, engine_time_t current_time);
void op_from_python_ref(ViewData& vd, const nb::object& src, engine_time_t current_time);
void op_from_python_tsw(ViewData& vd, const nb::object& src, engine_time_t current_time);
void op_from_python_tsw_tick(ViewData& vd, const nb::object& src, engine_time_t current_time);
void op_from_python_tsw_duration(ViewData& vd, const nb::object& src, engine_time_t current_time);
void op_from_python_tss(ViewData& vd, const nb::object& src, engine_time_t current_time);
void op_from_python_tsl(ViewData& vd, const nb::object& src, engine_time_t current_time);
void op_from_python_tsb(ViewData& vd, const nb::object& src, engine_time_t current_time);
void op_from_python_tsd(ViewData& vd, const nb::object& src, engine_time_t current_time);
void op_from_python_tsd_scalar(ViewData& vd, const nb::object& src, engine_time_t current_time);
void op_from_python_tsd_nested(ViewData& vd, const nb::object& src, engine_time_t current_time);
void op_from_python_tsd_ref(ViewData& vd, const nb::object& src, engine_time_t current_time);
void op_from_python_tsd_impl(ViewData& vd,
                             const nb::object& src,
                             engine_time_t current_time,
                             const TSMeta* current);
engine_time_t rebind_time_for_view(const ViewData& vd);
bool same_view_identity(const ViewData& lhs, const ViewData& rhs);
bool same_or_descendant_view(const ViewData& base, const ViewData& candidate);
bool ref_child_payload_valid(const ViewData& ref_child_vd);
bool container_child_valid_for_aggregation(const ViewData& child_vd);
bool ref_child_rebound_this_tick(const ViewData& ref_child);
engine_time_t direct_last_modified_time(const ViewData& vd);
engine_time_t ref_wrapper_last_modified_time_on_read_path(const ViewData& vd);
bool resolve_ref_bound_target_view_data(const ViewData& ref_view, ViewData& out);
std::optional<ViewData> resolve_ref_ancestor_descendant_view_data(const ViewData& vd);
void refresh_dynamic_ref_binding(const ViewData& vd, engine_time_t current_time);
bool is_tsd_key_set_projection(const ViewData& vd);
bool resolve_tsd_key_set_projection_view(const ViewData& vd, ViewData& out);
bool resolve_tsd_key_set_source(const ViewData& vd, ViewData& out);
bool resolve_tsd_key_set_bridge_source(const ViewData& vd, ViewData& out);
TSDKeySetDeltaState tsd_key_set_delta_state(const ViewData& source);
bool tsd_key_set_has_added_or_removed(const ViewData& source);
bool tsd_key_set_has_added_or_removed_this_tick(const ViewData& source, engine_time_t current_time);
bool tsd_key_set_modified_this_tick(const ViewData& source, engine_time_t current_time);
nb::object tsd_key_set_to_python(const ViewData& source);
nb::object tsd_key_set_delta_to_python(const ViewData& source);
nb::object tsd_key_set_all_added_to_python(const ViewData& source);
nb::object tsd_key_set_bridge_delta_to_python(const ViewData& previous_data, const ViewData& current_data);
nb::object tsd_key_set_unbind_delta_to_python(const ViewData& previous_data);
bool is_same_view_data(const ViewData& lhs, const ViewData& rhs);
bool resolve_read_view_data(const ViewData& vd, ViewData& out);
bool resolve_read_view_data(const ViewData& vd, const TSMeta* self_meta, ViewData& out);
void stamp_time_paths(ViewData& vd, engine_time_t current_time);
void set_leaf_time_path(ViewData& vd, engine_time_t time_value);
std::optional<ValueView> resolve_value_slot_mut(ViewData& vd);
std::optional<View> resolve_value_slot_const(const ViewData& vd);
bool has_local_ref_wrapper_value(const ViewData& vd);
bool has_bound_ref_static_children(const ViewData& vd);
bool assign_ref_value_from_bound_static_children(ViewData& vd);
bool assign_ref_value_from_target(ViewData& vd, const ViewData& target);
void clear_ref_value(ViewData& vd);
void clear_ref_container_ancestor_cache(ViewData& vd);
void apply_fallback_from_python_write(ViewData& vd, const nb::object& src, engine_time_t current_time);
void notify_if_static_container_children_changed(bool changed, const ViewData& vd, engine_time_t current_time);
void record_unbound_ref_item_changes(const ViewData& source,
                                     const std::vector<size_t>& changed_indices,
                                     engine_time_t current_time);
bool op_dict_remove(ViewData& vd, const View& key, engine_time_t current_time);
TSView op_dict_create(ViewData& vd, const View& key, engine_time_t current_time);
TSView op_dict_set(ViewData& vd, const View& key, const View& value, engine_time_t current_time);
bool op_set_add(ViewData& vd, const View& elem, engine_time_t current_time);
bool op_set_remove(ViewData& vd, const View& elem, engine_time_t current_time);
void op_set_clear(ViewData& vd, engine_time_t current_time);
const engine_time_t* op_window_value_times_tick(const ViewData& vd);
const engine_time_t* op_window_value_times_duration(const ViewData& vd);
size_t op_window_value_times_count_tick(const ViewData& vd);
size_t op_window_value_times_count_duration(const ViewData& vd);
engine_time_t op_window_first_modified_time_tick(const ViewData& vd);
engine_time_t op_window_first_modified_time_duration(const ViewData& vd);
bool op_window_has_removed_value_tick(const ViewData& vd);
bool op_window_has_removed_value_duration(const ViewData& vd);
View op_window_removed_value_tick(const ViewData& vd);
View op_window_removed_value_duration(const ViewData& vd);
size_t op_window_removed_value_count_tick(const ViewData& vd);
size_t op_window_removed_value_count_duration(const ViewData& vd);
size_t op_window_size_tick(const ViewData& vd);
size_t op_window_size_duration(const ViewData& vd);
size_t op_window_min_size_tick(const ViewData& vd);
size_t op_window_min_size_duration(const ViewData& vd);
size_t op_window_length(const ViewData& vd);

// --- Declarations added during common.cpp split (cross-file calls) ---

// link_notification.cpp
void unregister_active_link_target_observer(const LinkTarget& observer);
void register_active_link_target_observer(const LinkTarget& observer);
void unregister_active_ref_link_observer(const REFLink& observer);
void register_active_ref_link_observer(const REFLink& ref_link, const ViewData* observer_view = nullptr);

// path_meta_utils.cpp
bool view_matches_container_kind(const std::optional<View>& value, const TSMeta* container_meta);
bool rebind_bridge_has_container_kind_value(const ViewData& vd,
                                            const TSMeta* self_meta,
                                            engine_time_t current_time,
                                            const TSMeta* container_meta);
bool is_first_bind_rebind_tick(const LinkTarget* link_target, engine_time_t current_time);
bool resolve_container_rebind_bridge_views(const ViewData& vd,
                                           const TSMeta* container_meta,
                                           engine_time_t current_time,
                                           bool require_kind_mismatch,
                                           ViewData& previous_bridge,
                                           ViewData& current_bridge);
bool resolve_rebind_current_bridge_view(const ViewData& vd,
                                        const TSMeta* self_meta,
                                        engine_time_t current_time,
                                        ViewData& current_bridge);

// view_resolution.cpp
void collect_static_descendant_ts_paths(const TSMeta* node_meta,
                                        std::vector<size_t>& current_ts_path,
                                        std::vector<std::vector<size_t>>& out);
bool resolve_rebind_bridge_views(const ViewData& vd,
                                 const TSMeta* self_meta,
                                 engine_time_t current_time,
                                 ViewData& previous_resolved,
                                 ViewData& current_resolved);
std::optional<std::vector<size_t>> remap_residual_indices_for_bound_view(
    const ViewData& local_view,
    const ViewData& bound_view,
    const std::vector<size_t>& residual_indices);

// core_ops.cpp
bool reset_root_value_and_delta_on_none(ViewData& vd, const nb::object& src, engine_time_t current_time);

// bridge_delta_ops.cpp
TSDKeySetBridgeState resolve_tsd_key_set_bridge_state(const ViewData& vd,
                                                      engine_time_t current_time);
bool try_container_bridge_delta_to_python(const ViewData& vd,
                                          const TSMeta* container_meta,
                                          engine_time_t current_time,
                                          bool require_kind_mismatch,
                                          bool debug_bridge,
                                          nb::object& out_delta);
bool try_tsd_bridge_delta_to_python(const ViewData& vd,
                                    const TSMeta* container_meta,
                                    engine_time_t current_time,
                                    bool require_kind_mismatch,
                                    bool debug_bridge,
                                    nb::object& out_delta);
bool try_tss_bridge_delta_to_python(const ViewData& vd,
                                    const TSMeta* container_meta,
                                    engine_time_t current_time,
                                    bool require_kind_mismatch,
                                    bool debug_bridge,
                                    nb::object& out_delta);

// --- Template functions (must be inline in header) ---

template <typename Fn>
void for_each_named_bundle_field(const TSMeta* bundle_meta, Fn&& fn) {
    if (bundle_meta == nullptr || !dispatch_meta_is_tsb(bundle_meta) || bundle_meta->fields() == nullptr) {
        return;
    }
    for (size_t i = 0; i < bundle_meta->field_count(); ++i) {
        const char* field_name = bundle_meta->fields()[i].name;
        if (field_name == nullptr) {
            continue;
        }
        fn(i, field_name);
    }
}

template <typename Fn>
void for_each_map_key_slot(const value::MapView& map, Fn fn) {
    const auto* storage = map_storage_for_read(map);
    if (storage == nullptr) {
        return;
    }
    const value::TypeMeta* key_type = map.key_type();
    for (size_t slot : storage->key_set()) {
        fn(View(storage->key_at_slot(slot), key_type), slot);
    }
}

}  // namespace hgraph
