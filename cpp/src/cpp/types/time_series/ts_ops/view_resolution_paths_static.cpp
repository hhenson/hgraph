#include "ts_ops_internal.h"

namespace hgraph {

size_t static_container_child_count(const TSMeta* meta) {
    if (meta == nullptr) {
        return 0;
    }

    if (dispatch_meta_is_tsb(meta)) {
        return meta->field_count();
    }
    if (dispatch_meta_is_tsl(meta)) {
        return meta->fixed_size();
    }
    return 0;
}

bool link_target_points_to_unbound_ref_composite(const ViewData& vd, const LinkTarget* payload) {
    if (payload == nullptr || !payload->is_linked) {
        return false;
    }

    ViewData target = payload->as_view_data(vd.sampled);
    const TSMeta* target_meta = target.meta;
    if (!dispatch_meta_is_ref(target_meta)) {
        return false;
    }

    auto local = resolve_value_slot_const(target);
    if (!local.has_value() || !local->valid() || local->schema() != ts_reference_meta()) {
        return false;
    }

    TimeSeriesReference ref = *static_cast<const TimeSeriesReference*>(local->data());
    return ref.is_unbound() && !ref.is_empty();
}

bool is_unpeered_static_container_view(const ViewData& vd, const TSMeta* current) {
    if (!vd.uses_link_target || current == nullptr) {
        return false;
    }

    if (!dispatch_meta_is_static_container(current)) {
        return false;
    }

    if (LinkTarget* payload = resolve_link_target(vd); payload != nullptr) {
        return !payload->is_linked || link_target_points_to_unbound_ref_composite(vd, payload);
    }
    return false;
}

void collect_static_descendant_ts_paths(const TSMeta* node_meta,
                                        std::vector<size_t>& current_ts_path,
                                        std::vector<std::vector<size_t>>& out) {
    if (node_meta == nullptr) {
        return;
    }

    if (dispatch_meta_is_tsb(node_meta)) {
        if (node_meta->fields() == nullptr) {
            return;
        }
        for (size_t i = 0; i < node_meta->field_count(); ++i) {
            current_ts_path.push_back(i);
            out.push_back(current_ts_path);
            collect_static_descendant_ts_paths(node_meta->fields()[i].ts_type, current_ts_path, out);
            current_ts_path.pop_back();
        }
        return;
    }

    if (dispatch_meta_is_fixed_tsl(node_meta)) {
        for (size_t i = 0; i < node_meta->fixed_size(); ++i) {
            current_ts_path.push_back(i);
            out.push_back(current_ts_path);
            collect_static_descendant_ts_paths(node_meta->element_ts(), current_ts_path, out);
            current_ts_path.pop_back();
        }
    }
}

}  // namespace hgraph
