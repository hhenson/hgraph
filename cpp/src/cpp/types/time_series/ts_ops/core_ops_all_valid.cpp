#include "ts_ops_internal.h"

namespace hgraph {

bool op_all_valid_tsw_tick(const ViewData& vd) {
    const TSMeta* self_meta = vd.meta;
    if (self_meta == nullptr) {
        return op_all_valid(vd);
    }
    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return false;
    }
    const ViewData* data = &resolved;
    ViewData dispatch_data = dispatch_view_for_path(*data);
    const ts_ops* self_ops = get_ts_ops(self_meta);
    if (dispatch_data.ops != nullptr &&
        dispatch_data.ops != self_ops &&
        dispatch_data.ops->all_valid != nullptr) {
        return dispatch_data.ops->all_valid(dispatch_data);
    }
    if (!dispatch_valid(*data)) {
        return false;
    }
    const TSMeta* current = data->meta;
    if (current == nullptr) {
        return false;
    }

    View window_value = op_value(*data);
    const size_t length =
        window_value.valid() && window_value.is_cyclic_buffer() ? window_value.as_cyclic_buffer().size() : 0;
    return length >= current->min_period();
}

bool op_all_valid_tsw_duration(const ViewData& vd) {
    const TSMeta* self_meta = vd.meta;
    if (self_meta == nullptr) {
        return op_all_valid(vd);
    }
    ViewData resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return false;
    }
    const ViewData* data = &resolved;
    ViewData dispatch_data = dispatch_view_for_path(*data);
    const ts_ops* self_ops = get_ts_ops(self_meta);
    if (dispatch_data.ops != nullptr &&
        dispatch_data.ops != self_ops &&
        dispatch_data.ops->all_valid != nullptr) {
        return dispatch_data.ops->all_valid(dispatch_data);
    }
    if (!dispatch_valid(*data)) {
        return false;
    }

    auto* time_root = static_cast<const Value*>(data->time_data);
    if (time_root == nullptr || !time_root->has_value()) {
        return false;
    }
    auto time_path = ts_path_to_time_path(data->root_meta, data->path_indices());
    if (time_path.empty()) {
        return false;
    }
    time_path.pop_back();
    std::optional<View> maybe_time;
    if (time_path.empty()) {
        maybe_time = time_root->view();
    } else {
        maybe_time = navigate_const(time_root->view(), time_path);
    }
    if (!maybe_time.has_value() || !maybe_time->valid() || !maybe_time->is_tuple()) {
        return false;
    }
    auto tuple = maybe_time->as_tuple();
    if (tuple.size() < 4) {
        return false;
    }
    View ready = tuple.at(3);
    return ready.valid() && ready.is_scalar_type<bool>() && ready.as<bool>();
}

bool op_all_valid_tsw(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->all_valid != nullptr) {
        return dispatch_vd.ops->all_valid(dispatch_vd);
    }
    return op_all_valid_tsw_tick(dispatch_vd);
}

bool op_all_valid_tsb(const ViewData& vd) {
    const TSMeta* self_meta = vd.meta;
    if (self_meta == nullptr) {
        return op_all_valid(vd);
    }

    const size_t n = static_container_child_count(self_meta);
    for (size_t i = 0; i < n; ++i) {
        ViewData child = make_child_view_data(vd, i);
        if (!container_child_valid_for_aggregation(child)) {
            return false;
        }
    }
    return true;
}

bool op_all_valid_tsl(const ViewData& vd) {
    const TSMeta* self_meta = vd.meta;
    if (self_meta == nullptr || self_meta->fixed_size() == 0) {
        return op_all_valid(vd);
    }

    const size_t n = static_container_child_count(self_meta);
    for (size_t i = 0; i < n; ++i) {
        ViewData child = make_child_view_data(vd, i);
        if (!container_child_valid_for_aggregation(child)) {
            return false;
        }
    }
    return true;
}

bool op_all_valid(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->all_valid != nullptr) {
        return dispatch_vd.ops->all_valid(dispatch_vd);
    }
    return dispatch_valid(dispatch_vd);
}


}  // namespace hgraph
