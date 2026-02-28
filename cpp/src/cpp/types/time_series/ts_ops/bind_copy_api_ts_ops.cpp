#include "ts_ops_internal.h"

namespace hgraph {

namespace {

using common_ops_customizer = void (*)(ts_ops&);
constexpr size_t k_common_ops_customizer_count = static_cast<size_t>(TSKind::SIGNAL) + size_t{1};

bool meta_kind_is_scalar_like(const TSMeta* meta) {
    if (meta == nullptr) {
        return true;
    }
    switch (meta->kind) {
        case TSKind::TSValue:
        case TSKind::REF:
        case TSKind::SIGNAL:
        case TSKind::TSW:
            return true;
        default:
            return false;
    }
}

bool meta_kind_is_ref_bundle_static(const TSMeta* meta) {
    if (meta == nullptr) {
        return false;
    }
    return meta->kind == TSKind::TSB;
}

bool meta_kind_is_ref_list_static(const TSMeta* meta) {
    if (meta == nullptr) {
        return false;
    }
    return meta->kind == TSKind::TSL && meta->fixed_size() > 0;
}

bool meta_kind_is_ref_dynamic_container(const TSMeta* meta) {
    if (meta == nullptr) {
        return false;
    }
    return meta->kind == TSKind::TSS || meta->kind == TSKind::TSD;
}

ts_ops make_base_common_ops(TSKind kind) {
    ts_ops out{
        &op_ts_meta,
        &op_last_modified_time,
        &op_modified,
        &op_valid,
        &op_valid,
        &op_sampled,
        &op_value_non_ref,
        &op_delta_value_container,
        &op_has_delta_default,
        &op_set_value,
        &op_apply_delta_container,
        &op_invalidate,
        &op_to_python_default,
        &op_delta_to_python_default,
        &op_from_python_scalar,
        nullptr,
        &op_observer,
        &op_notify_observers,
        &op_bind,
        &op_unbind,
        &op_is_bound,
        &op_set_active,
        &op_copy_scalar,
        kind,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
    };
    return out;
}

void configure_tsvalue_common_ops(ts_ops& out) {
    out.last_modified_time = &op_last_modified_tsvalue;
    out.valid = &op_valid_tsvalue;
    out.modified = &op_modified_tsvalue;
    out.delta_to_python = &op_delta_to_python_tsvalue;
    out.has_delta = &op_has_delta_scalar;
    out.delta_value = &op_delta_value_scalar;
    out.apply_delta = &op_apply_delta_scalar;
}

void configure_tss_common_ops(ts_ops& out) {
    out.last_modified_time = &op_last_modified_tss;
    out.valid = &op_valid_tss;
    out.modified = &op_modified_tss;
    out.to_python = &op_to_python_tss;
    out.delta_to_python = &op_delta_to_python_tss;
    out.delta_value = &op_delta_value_container;
    out.apply_delta = &op_apply_delta_container;
    out.from_python = &op_from_python_tss;
}

void configure_tsd_common_ops(ts_ops& out) {
    out.last_modified_time = &op_last_modified_tsd;
    out.valid = &op_valid_tsd;
    out.modified = &op_modified_tsd;
    out.to_python = &op_to_python_tsd;
    out.delta_to_python = &op_delta_to_python_tsd_impl;
    out.delta_value = &op_delta_value_container;
    out.apply_delta = &op_apply_delta_container;
    out.invalidate = &op_invalidate_tsd;
}

void configure_tsl_common_ops(ts_ops& out) {
    out.last_modified_time = &op_last_modified_tsl;
    out.valid = &op_valid_tsl;
    out.modified = &op_modified_tsl;
    out.all_valid = &op_all_valid_tsl;
    out.to_python = &op_to_python_tsl;
    out.delta_to_python = &op_delta_to_python_tsl;
    out.delta_value = &op_delta_value_container;
    out.apply_delta = &op_apply_delta_container;
    out.from_python = &op_from_python_tsl;
}

void configure_tsw_common_ops(ts_ops& out) {
    out.last_modified_time = &op_last_modified_tsw;
    out.valid = &op_valid_tsw;
    out.modified = &op_modified_tsw;
    out.to_python = &op_to_python_tsw;
    out.delta_to_python = &op_delta_to_python_tsw;
    out.apply_delta = &op_apply_delta_container;
    out.from_python = &op_from_python_tsw;
}

void configure_tsb_common_ops(ts_ops& out) {
    out.last_modified_time = &op_last_modified_tsb;
    out.valid = &op_valid_tsb;
    out.modified = &op_modified_tsb;
    out.all_valid = &op_all_valid_tsb;
    out.to_python = &op_to_python_tsb;
    out.delta_to_python = &op_delta_to_python_tsb;
    out.delta_value = &op_delta_value_container;
    out.apply_delta = &op_apply_delta_container;
    out.from_python = &op_from_python_tsb;
}

void configure_ref_common_ops(ts_ops& out) {
    out.valid = &op_valid_ref;
    out.value = &op_value_ref;
    out.last_modified_time = &op_last_modified_ref;
    out.modified = &op_modified_ref;
    out.to_python = &op_to_python_ref;
    out.copy_value = &op_copy_ref;
    out.delta_to_python = &op_delta_to_python_ref;
    out.has_delta = &op_has_delta_scalar;
    out.delta_value = &op_delta_value_scalar;
    out.apply_delta = &op_apply_delta_scalar;
    out.from_python = &op_from_python_ref;
    out.ref_payload_to_python = &op_ref_payload_to_python;
}

void configure_signal_common_ops(ts_ops& out) {
    out.last_modified_time = &op_last_modified_signal;
    out.valid = &op_valid_signal;
    out.modified = &op_modified_signal;
    out.has_delta = &op_has_delta_scalar;
    out.delta_value = &op_delta_value_scalar;
    out.apply_delta = &op_apply_delta_scalar;
}

constexpr common_ops_customizer k_common_ops_customizers[k_common_ops_customizer_count] = {
    &configure_tsvalue_common_ops,   // TSKind::TSValue
    &configure_tss_common_ops,       // TSKind::TSS
    &configure_tsd_common_ops,       // TSKind::TSD
    &configure_tsl_common_ops,       // TSKind::TSL
    &configure_tsw_common_ops,       // TSKind::TSW
    &configure_tsb_common_ops,       // TSKind::TSB
    &configure_ref_common_ops,       // TSKind::REF
    &configure_signal_common_ops,    // TSKind::SIGNAL
};

}  // namespace

ts_ops make_common_ops(TSKind kind) {
    ts_ops out = make_base_common_ops(kind);
    const size_t index = static_cast<size_t>(kind);
    if (index < k_common_ops_customizer_count) {
        if (const common_ops_customizer customize = k_common_ops_customizers[index]; customize != nullptr) {
            customize(out);
        }
    }
    return out;
}

ts_ops make_tsw_tick_ops() {
    ts_ops out = make_common_ops(TSKind::TSW);
    out.all_valid = &op_all_valid_tsw_tick;
    out.to_python = &op_to_python_tsw_tick;
    out.delta_to_python = &op_delta_to_python_tsw_tick;
    out.delta_value = &op_delta_value_tsw_tick;
    out.from_python = &op_from_python_tsw_tick;
    out.window = &k_window_tick_ops;
    return out;
}

ts_ops make_tsw_duration_ops() {
    ts_ops out = make_common_ops(TSKind::TSW);
    out.all_valid = &op_all_valid_tsw_duration;
    out.to_python = &op_to_python_tsw_duration;
    out.delta_to_python = &op_delta_to_python_tsw_duration;
    out.delta_value = &op_delta_value_tsw_duration;
    out.from_python = &op_from_python_tsw_duration;
    out.window = &k_window_duration_ops;
    return out;
}

ts_ops make_tss_ops() {
    ts_ops out = make_common_ops(TSKind::TSS);
    out.copy_value = &op_copy_tss;
    out.has_delta = &op_has_delta_tss;
    out.set = &k_set_ops;
    return out;
}

ts_ops make_tsd_ops() {
    ts_ops out = make_common_ops(TSKind::TSD);
    out.copy_value = &op_copy_tsd;
    out.has_delta = &op_has_delta_tsd;
    out.dict = &k_dict_ops;
    out.delta_to_python = &op_delta_to_python_tsd_impl;
    out.from_python = &op_from_python_tsd;
    return out;
}

ts_ops make_tsd_ref_ops() {
    ts_ops out = make_tsd_ops();
    out.delta_to_python = &op_delta_to_python_tsd_ref;
    out.from_python = &op_from_python_tsd_ref;
    return out;
}

ts_ops make_tsd_scalar_ops() {
    ts_ops out = make_tsd_ops();
    out.delta_to_python = &op_delta_to_python_tsd_scalar;
    out.from_python = &op_from_python_tsd_scalar;
    return out;
}

ts_ops make_tsd_nested_ops() {
    ts_ops out = make_tsd_ops();
    out.delta_to_python = &op_delta_to_python_tsd_nested;
    out.from_python = &op_from_python_tsd_nested;
    return out;
}

ts_ops make_ref_scalar_ops() {
    ts_ops out = make_common_ops(TSKind::REF);
    out.valid = &op_valid_ref_scalar;
    out.modified = &op_modified_ref_scalar;
    out.delta_to_python = &op_delta_to_python_ref_scalar;
    out.ref_payload_to_python = &op_ref_payload_to_python_scalar;
    return out;
}

ts_ops make_ref_list_ops() {
    ts_ops out = make_common_ops(TSKind::REF);
    out.valid = &op_valid_ref_static_container;
    out.modified = &op_modified_ref_static_container;
    out.delta_to_python = &op_delta_to_python_ref_list;
    out.ref_payload_to_python = &op_ref_payload_to_python_list;
    return out;
}

ts_ops make_ref_bundle_ops() {
    ts_ops out = make_common_ops(TSKind::REF);
    out.valid = &op_valid_ref_static_container;
    out.modified = &op_modified_ref_static_container;
    out.delta_to_python = &op_delta_to_python_ref_bundle;
    out.ref_payload_to_python = &op_ref_payload_to_python_bundle;
    return out;
}

ts_ops make_ref_dynamic_container_ops() {
    ts_ops out = make_common_ops(TSKind::REF);
    out.valid = &op_valid_ref_dynamic_container;
    out.modified = &op_modified_ref_dynamic_container;
    out.delta_to_python = &op_delta_to_python_ref_dynamic;
    out.ref_payload_to_python = &op_ref_payload_to_python_dynamic;
    return out;
}

ts_ops make_tsd_key_set_projection_ops() {
    ts_ops out = make_tsd_ops();
    out.ts_meta = &op_ts_meta_tsd_key_set;
    out.modified = &op_modified_tsd_key_set;
    out.valid = &op_valid_tsd_key_set;
    out.has_delta = &op_has_delta_tsd_key_set;
    out.to_python = &op_to_python_tsd_key_set;
    out.delta_to_python = &op_delta_to_python_tsd_key_set;
    return out;
}

ts_ops make_tsl_ops() {
    ts_ops out = make_common_ops(TSKind::TSL);
    out.copy_value = &op_copy_tsl;
    out.list = &k_list_ops;
    return out;
}

ts_ops make_tsb_ops() {
    ts_ops out = make_common_ops(TSKind::TSB);
    out.copy_value = &op_copy_tsb;
    out.bundle = &k_bundle_ops;
    return out;
}

const ts_ops k_ts_value_ops = make_common_ops(TSKind::TSValue);
const ts_ops k_tss_ops = make_tss_ops();
const ts_ops k_tsd_ops = make_tsd_ops();
const ts_ops k_tsd_scalar_ops = make_tsd_scalar_ops();
const ts_ops k_tsd_nested_ops = make_tsd_nested_ops();
const ts_ops k_tsd_ref_ops = make_tsd_ref_ops();
const ts_ops k_tsd_key_set_projection_ops = make_tsd_key_set_projection_ops();
const ts_ops k_tsl_ops = make_tsl_ops();
const ts_ops k_tsb_ops = make_tsb_ops();
const ts_ops k_ref_ops = make_common_ops(TSKind::REF);
const ts_ops k_ref_scalar_ops = make_ref_scalar_ops();
const ts_ops k_ref_list_ops = make_ref_list_ops();
const ts_ops k_ref_bundle_ops = make_ref_bundle_ops();
const ts_ops k_ref_dynamic_container_ops = make_ref_dynamic_container_ops();
const ts_ops k_signal_ops = make_common_ops(TSKind::SIGNAL);
const ts_ops k_tsw_tick_ops = make_tsw_tick_ops();
const ts_ops k_tsw_duration_ops = make_tsw_duration_ops();

namespace {

const ts_ops* const k_ops_by_kind[] = {
    &k_ts_value_ops,   // TSKind::TSValue
    &k_tss_ops,        // TSKind::TSS
    &k_tsd_ops,        // TSKind::TSD
    &k_tsl_ops,        // TSKind::TSL
    &k_tsw_tick_ops,   // TSKind::TSW
    &k_tsb_ops,        // TSKind::TSB
    &k_ref_ops,        // TSKind::REF
    &k_signal_ops,     // TSKind::SIGNAL
};

static_assert(
    (sizeof(k_ops_by_kind) / sizeof(k_ops_by_kind[0])) == (static_cast<size_t>(TSKind::SIGNAL) + size_t{1}),
    "k_ops_by_kind must cover all TSKind values");

}  // namespace

const ts_ops* get_ts_ops(TSKind kind) {
    const size_t index = static_cast<size_t>(kind);
    if (index < (sizeof(k_ops_by_kind) / sizeof(k_ops_by_kind[0]))) {
        return k_ops_by_kind[index];
    }
    return &k_ts_value_ops;
}

const ts_ops* get_ts_ops(const TSMeta* meta) {
    if (meta == nullptr) {
        return &k_ts_value_ops;
    }

    if (meta->kind == TSKind::TSD) {
        const TSMeta* element_meta = meta->element_ts();
        if (element_meta != nullptr && element_meta->kind == TSKind::REF) {
            return &k_tsd_ref_ops;
        }
        if (meta_kind_is_scalar_like(element_meta)) {
            return &k_tsd_scalar_ops;
        }
        return &k_tsd_nested_ops;
    }

    if (meta->kind == TSKind::REF) {
        const TSMeta* element_meta = meta->element_ts();
        if (meta_kind_is_ref_bundle_static(element_meta)) {
            return &k_ref_bundle_ops;
        }
        if (meta_kind_is_ref_list_static(element_meta)) {
            return &k_ref_list_ops;
        }
        if (meta_kind_is_ref_dynamic_container(element_meta)) {
            return &k_ref_dynamic_container_ops;
        }
        return &k_ref_scalar_ops;
    }

    const ts_ops* out = get_ts_ops(meta->kind);
    if (meta->is_duration_based() && out == &k_tsw_tick_ops) {
        return &k_tsw_duration_ops;
    }
    return out;
}

const ts_ops* get_ts_ops(const ViewData& view) {
    const TSMeta* meta = meta_at_path(view.meta, view.path.indices);
    if (view.projection == ViewProjection::TSD_KEY_SET &&
        meta != nullptr &&
        dispatch_meta_is_tsd(meta)) {
        return &k_tsd_key_set_projection_ops;
    }
    return get_ts_ops(meta);
}

const ts_ops* default_ts_ops() {
    return &k_ts_value_ops;
}

}  // namespace hgraph
