#pragma once

#include <hgraph/types/time_series/ts_ops.h>

#include <hgraph/types/time_series/link_observer_registry.h>
#include <hgraph/types/time_series/link_target.h>
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

namespace hgraph {

using value::View;
using value::Value;
using value::ValueView;

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

struct TsdRemovedChildSnapshotState;
struct TsdVisibleKeyHistoryState;
struct RefUnboundItemChangeState;

struct TSDKeySetDeltaState {
    bool has_delta_tuple{false};
    bool has_changed_values_map{false};
    bool has_added{false};
    bool has_removed{false};
};

extern const ts_window_ops k_window_ops;
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
const TSMeta* op_ts_meta(const ViewData& vd);
engine_time_t op_last_modified_time(const ViewData& vd);
bool op_modified(const ViewData& vd, engine_time_t current_time);
bool op_valid(const ViewData& vd);
bool op_all_valid(const ViewData& vd);
bool op_sampled(const ViewData& vd);
View op_value(const ViewData& vd);
View op_delta_value(const ViewData& vd);
bool op_has_delta(const ViewData& vd);
void op_set_value(ViewData& vd, const View& src, engine_time_t current_time);
void op_apply_delta(ViewData& vd, const View& delta, engine_time_t current_time);
void op_invalidate(ViewData& vd);
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
void register_ref_link_observer(const REFLink& ref_link, const ViewData* observer_view);
bool suppress_static_ref_child_notification(const LinkTarget& observer, engine_time_t current_time);
void notify_link_target_observers(const ViewData& target_view, engine_time_t current_time);
ts_ops make_common_ops(TSKind kind);
ts_ops make_tsw_ops();
ts_ops make_tss_ops();
ts_ops make_tsd_ops();
ts_ops make_tsl_ops();
ts_ops make_tsb_ops();
void copy_tss(ViewData dst, const ViewData& src, engine_time_t current_time);
void copy_tsd(ViewData dst, const ViewData& src, engine_time_t current_time);
void copy_view_data_value_impl(ViewData dst, const ViewData& src, engine_time_t current_time);
const TSMeta* meta_at_path(const TSMeta* root, const std::vector<size_t>& indices);
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
bool is_scalar_like_ts_kind(TSKind kind);
bool has_delta_descendants(const TSMeta* meta);
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
std::vector<std::vector<size_t>> time_stamp_paths_for_ts_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path);
const ts_ops* get_ts_ops(TSKind kind);
const ts_ops* get_ts_ops(const TSMeta* meta);
const ts_ops* default_ts_ops();
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
void reset_ts_link_observers();
nb::object op_to_python(const ViewData& vd);
nb::object op_delta_to_python(const ViewData& vd, engine_time_t current_time);
std::shared_ptr<RefUnboundItemChangeState> ensure_ref_unbound_item_change_state(TSLinkObserverRegistry* registry);
bool unbound_ref_item_changed_this_tick(const ViewData& item_view, size_t item_index, engine_time_t current_time);
void record_tsd_removed_child_snapshot(const ViewData& parent_view,
                                       const View& key,
                                       const ViewData& child_view,
                                       engine_time_t current_time);
void op_from_python(ViewData& vd, const nb::object& src, engine_time_t current_time);
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
bool op_dict_remove(ViewData& vd, const View& key, engine_time_t current_time);
TSView op_dict_create(ViewData& vd, const View& key, engine_time_t current_time);
TSView op_dict_set(ViewData& vd, const View& key, const View& value, engine_time_t current_time);
bool op_set_add(ViewData& vd, const View& elem, engine_time_t current_time);
bool op_set_remove(ViewData& vd, const View& elem, engine_time_t current_time);
void op_set_clear(ViewData& vd, engine_time_t current_time);
const engine_time_t* op_window_value_times(const ViewData& vd);
size_t op_window_value_times_count(const ViewData& vd);
engine_time_t op_window_first_modified_time(const ViewData& vd);
bool op_window_has_removed_value(const ViewData& vd);
View op_window_removed_value(const ViewData& vd);
size_t op_window_removed_value_count(const ViewData& vd);
size_t op_window_size(const ViewData& vd);
size_t op_window_min_size(const ViewData& vd);
size_t op_window_length(const ViewData& vd);

}  // namespace hgraph
