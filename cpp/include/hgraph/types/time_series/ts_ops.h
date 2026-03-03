#pragma once

#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/value/value.h>

#include <nanobind/nanobind.h>

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace nb = nanobind;

namespace hgraph {

class TSInput;
class TSInputView;
class TSView;
struct TimeSeriesReference;

/**
 * Kind-specific window extension operations.
 */
struct ts_window_ops {
    const engine_time_t* (*value_times)(const ViewData& vd);
    size_t (*value_times_count)(const ViewData& vd);
    engine_time_t (*first_modified_time)(const ViewData& vd);
    bool (*has_removed_value)(const ViewData& vd);
    value::View (*removed_value)(const ViewData& vd);
    size_t (*removed_value_count)(const ViewData& vd);
    size_t (*size)(const ViewData& vd);
    size_t (*min_size)(const ViewData& vd);
    size_t (*length)(const ViewData& vd);
};

/**
 * Kind-specific TSS extension operations.
 */
struct ts_set_ops {
    bool (*add)(ViewData& vd, const value::View& elem, engine_time_t current_time);
    bool (*remove)(ViewData& vd, const value::View& elem, engine_time_t current_time);
    void (*clear)(ViewData& vd, engine_time_t current_time);
};

/**
 * Kind-specific TSD extension operations.
 */
struct ts_dict_ops {
    bool (*remove)(ViewData& vd, const value::View& key, engine_time_t current_time);
    TSView (*create)(ViewData& vd, const value::View& key, engine_time_t current_time);
    TSView (*set)(ViewData& vd, const value::View& key, const value::View& value, engine_time_t current_time);
};

/**
 * Kind-specific TSL extension operations.
 */
struct ts_list_ops {
    TSView (*at)(const ViewData& vd, size_t index, engine_time_t current_time);
    size_t (*size)(const ViewData& vd);
};

/**
 * Kind-specific TSB extension operations.
 */
struct ts_bundle_ops {
    TSView (*at)(const ViewData& vd, size_t index, engine_time_t current_time);
    TSView (*at_name)(const ViewData& vd, std::string_view name, engine_time_t current_time);
    size_t (*size)(const ViewData& vd);
};

/**
 * Type-series operation table used by TSView.
 *
 * Compacted layout:
 * - common operations are always present
 * - kind-specific operation families are bound as direct pointers
 *   when the table is created
 */
struct ts_ops {
    const TSMeta* (*ts_meta)(const ViewData& vd);

    engine_time_t (*last_modified_time)(const ViewData& vd);
    bool (*modified)(const ViewData& vd, engine_time_t current_time);
    bool (*valid)(const ViewData& vd);
    bool (*all_valid)(const ViewData& vd);
    bool (*sampled)(const ViewData& vd);

    value::View (*value)(const ViewData& vd);
    value::View (*delta_value)(const ViewData& vd);
    bool (*has_delta)(const ViewData& vd);

    void (*set_value)(ViewData& vd, const value::View& src, engine_time_t current_time);
    void (*apply_delta)(ViewData& vd, const value::View& delta, engine_time_t current_time);
    void (*invalidate)(ViewData& vd);

    nb::object (*to_python)(const ViewData& vd);
    nb::object (*delta_to_python)(const ViewData& vd, engine_time_t current_time);
    void (*from_python)(ViewData& vd, const nb::object& src, engine_time_t current_time);
    nb::object (*ref_payload_to_python)(const TimeSeriesReference& ref,
                                        const TSMeta* expected_meta,
                                        engine_time_t current_time,
                                        bool include_unmodified);

    value::View (*observer)(const ViewData& vd);
    void (*notify_observers)(ViewData& vd, engine_time_t current_time);

    void (*bind)(ViewData& vd, const ViewData& target, engine_time_t current_time);
    void (*unbind)(ViewData& vd, engine_time_t current_time);
    bool (*is_bound)(const ViewData& vd);

    void (*set_active)(ViewData& vd, value::ValueView active_view, bool active, TSInput* input);
    void (*copy_value)(ViewData dst, const ViewData& src, engine_time_t current_time);

    TSKind kind;
    const ts_window_ops* window;
    const ts_set_ops* set;
    const ts_dict_ops* dict;
    const ts_list_ops* list;
    const ts_bundle_ops* bundle;

    [[nodiscard]] const ts_window_ops* window_ops() const noexcept {
        return window;
    }

    [[nodiscard]] const ts_set_ops* set_ops() const noexcept {
        return set;
    }

    [[nodiscard]] const ts_dict_ops* dict_ops() const noexcept {
        return dict;
    }

    [[nodiscard]] const ts_list_ops* list_ops() const noexcept {
        return list;
    }

    [[nodiscard]] const ts_bundle_ops* bundle_ops() const noexcept {
        return bundle;
    }
};

enum class TSCollectionFilter : uint8_t {
    All = 0,
    Valid = 1,
    Modified = 2,
};

[[nodiscard]] inline bool is_delta_to_python_cacheable(const ViewData& vd) noexcept {
    // Keep delta cache conservative: sampled/projection/link-target reads can
    // diverge from direct local-path delta semantics.
    return !vd.sampled && !vd.uses_link_target && vd.projection == ViewProjection::NONE;
}

/**
 * Retrieve ts_ops by static kind discriminator.
 */
HGRAPH_EXPORT const ts_ops* get_ts_ops(TSKind kind);

/**
 * Retrieve ts_ops by concrete metadata.
 * For TSW this enables tick-vs-duration specializations.
 */
HGRAPH_EXPORT const ts_ops* get_ts_ops(const TSMeta* meta);

/**
 * Default operation table used by scaffolding TSValue/TSView types.
 */
HGRAPH_EXPORT const ts_ops* default_ts_ops();

/**
 * Helpers for explicit discriminator-based bind paths.
 */
HGRAPH_EXPORT void store_to_link_target(LinkTarget& target, const ViewData& source);
HGRAPH_EXPORT void store_to_ref_link(REFLink& target, const ViewData& source);
HGRAPH_EXPORT bool resolve_direct_bound_view_data(const ViewData& source, ViewData& out);
HGRAPH_EXPORT bool resolve_bound_target_view_data(const ViewData& source, ViewData& out);
HGRAPH_EXPORT bool resolve_previous_bound_target_view_data(const ViewData& source, ViewData& out);
HGRAPH_EXPORT engine_time_t resolve_notify_time(node_ptr owner, engine_time_t fallback);
HGRAPH_EXPORT bool input_kind_requires_bound_validity(const TSInputView& input_view);
HGRAPH_EXPORT std::optional<ViewData> resolve_input_bound_target_view_data(const TSInputView& input_view);
HGRAPH_EXPORT bool input_has_effective_bound_target(const TSInputView& input_view);
HGRAPH_EXPORT const engine_time_t* resolve_bound_view_current_time_ptr(const ViewData& vd);
HGRAPH_EXPORT engine_time_t resolve_bound_view_current_time(const ViewData& vd);
HGRAPH_EXPORT bool ts_meta_is_ref(const TSMeta* meta);
HGRAPH_EXPORT bool ts_meta_is_bundle(const TSMeta* meta);
HGRAPH_EXPORT bool ts_meta_is_dict(const TSMeta* meta);
HGRAPH_EXPORT const TSMeta* ts_strip_ref_meta(const TSMeta* meta);
HGRAPH_EXPORT const TSMeta* ts_bundle_meta_with_fields(const TSView& view);
HGRAPH_EXPORT std::optional<size_t> ts_bundle_field_index(const TSView& view, std::string_view name);
HGRAPH_EXPORT nb::list ts_bundle_field_names(const TSView& view);
HGRAPH_EXPORT bool ts_list_child_effectively_modified(const TSView& child);
HGRAPH_EXPORT std::vector<size_t> ts_list_filtered_indices(const TSView& view, TSCollectionFilter filter);
HGRAPH_EXPORT std::vector<size_t> ts_bundle_filtered_indices(const TSView& view, TSCollectionFilter filter);
HGRAPH_EXPORT value::Value tsd_key_from_python(const nb::object& key, const TSMeta* meta);
HGRAPH_EXPORT value::View ts_local_navigation_value(const TSView& view);
HGRAPH_EXPORT nb::list tsd_keys_python(const TSView& view, bool include_local_fallback);
HGRAPH_EXPORT nb::list tsd_delta_keys_slot(const TSView& view, size_t tuple_index, bool expect_map);
HGRAPH_EXPORT bool ts_python_is_remove_marker(const nb::object& obj);
HGRAPH_EXPORT bool ts_python_is_remove_if_exists_marker(const nb::object& obj);
HGRAPH_EXPORT std::optional<TSView> tsd_previous_child_for_key(const TSView& parent_view,
                                                                const value::View& key);
HGRAPH_EXPORT std::optional<TSView> resolve_tsd_removed_child_snapshot(
    const ViewData& parent_view,
    const value::View& key,
    engine_time_t current_time);
HGRAPH_EXPORT void copy_view_data_value(ViewData& dst, const ViewData& src, engine_time_t current_time);
HGRAPH_EXPORT void notify_ts_link_observers(const ViewData& target_view, engine_time_t current_time);
HGRAPH_EXPORT nb::object op_to_python(const ViewData& vd);
HGRAPH_EXPORT nb::object op_delta_to_python(const ViewData& vd, engine_time_t current_time);
HGRAPH_EXPORT void op_from_python(ViewData& vd, const nb::object& src, engine_time_t current_time);

/**
 * Register/unregister a link observer against endpoint-owned TS observer registries.
 */
HGRAPH_EXPORT void register_ts_link_observer(LinkTarget& observer);
HGRAPH_EXPORT void unregister_ts_link_observer(LinkTarget& observer);
HGRAPH_EXPORT void register_ts_ref_link_observer(REFLink& observer);
HGRAPH_EXPORT void unregister_ts_ref_link_observer(REFLink& observer);
HGRAPH_EXPORT void register_ts_active_link_observer(LinkTarget& observer);
HGRAPH_EXPORT void unregister_ts_active_link_observer(LinkTarget& observer);
HGRAPH_EXPORT void register_ts_active_ref_link_observer(REFLink& observer);
HGRAPH_EXPORT void unregister_ts_active_ref_link_observer(REFLink& observer);

/**
 * Compatibility no-op (registries are endpoint-owned and auto-reset with endpoint lifetime).
 */
HGRAPH_EXPORT void reset_ts_link_observers();

}  // namespace hgraph
