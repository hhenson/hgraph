#pragma once

#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/value/value.h>

#include <nanobind/nanobind.h>

#include <string_view>

namespace nb = nanobind;

namespace hgraph {

class TSInput;
class TSView;

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
 * - kind-specific operation families are stored in a tagged union
 *   selected by `kind`
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

    value::View (*observer)(const ViewData& vd);
    void (*notify_observers)(ViewData& vd, engine_time_t current_time);

    void (*bind)(ViewData& vd, const ViewData& target, engine_time_t current_time);
    void (*unbind)(ViewData& vd, engine_time_t current_time);
    bool (*is_bound)(const ViewData& vd);

    void (*set_active)(ViewData& vd, value::ValueView active_view, bool active, TSInput* input);

    TSKind kind;

    struct ts_none_ops {
        uint8_t reserved;
    };

    union specific_ops {
        ts_none_ops none;
        ts_window_ops window;
        ts_set_ops set;
        ts_dict_ops dict;
        ts_list_ops list;
        ts_bundle_ops bundle;
    } specific;

    [[nodiscard]] const ts_window_ops* window_ops() const noexcept {
        return kind == TSKind::TSW ? &specific.window : nullptr;
    }

    [[nodiscard]] const ts_set_ops* set_ops() const noexcept {
        return kind == TSKind::TSS ? &specific.set : nullptr;
    }

    [[nodiscard]] const ts_dict_ops* dict_ops() const noexcept {
        return kind == TSKind::TSD ? &specific.dict : nullptr;
    }

    [[nodiscard]] const ts_list_ops* list_ops() const noexcept {
        return kind == TSKind::TSL ? &specific.list : nullptr;
    }

    [[nodiscard]] const ts_bundle_ops* bundle_ops() const noexcept {
        return kind == TSKind::TSB ? &specific.bundle : nullptr;
    }
};

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
HGRAPH_EXPORT void copy_view_data_value(ViewData& dst, const ViewData& src, engine_time_t current_time);
HGRAPH_EXPORT void notify_ts_link_observers(const ViewData& target_view, engine_time_t current_time);

/**
 * Register/unregister a link observer against endpoint-owned TS observer registries.
 */
HGRAPH_EXPORT void register_ts_link_observer(LinkTarget& observer);
HGRAPH_EXPORT void unregister_ts_link_observer(LinkTarget& observer);
HGRAPH_EXPORT void register_ts_ref_link_observer(REFLink& observer);
HGRAPH_EXPORT void unregister_ts_ref_link_observer(REFLink& observer);

/**
 * Compatibility no-op (registries are endpoint-owned and auto-reset with endpoint lifetime).
 */
HGRAPH_EXPORT void reset_ts_link_observers();

}  // namespace hgraph
