#include "ts_ops_internal.h"

namespace hgraph {

bool op_sampled(const ViewData& vd) {
    return vd.sampled;
}

View op_value_non_ref(const ViewData& vd) {
    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return {};
    }
    const ViewData* data = &resolved;

    auto* value_root = static_cast<const Value*>(data->value_data);
    if (value_root == nullptr || !value_root->has_value()) {
        return {};
    }
    auto maybe = navigate_const(value_root->view(), data->path.indices);
    return maybe.has_value() ? *maybe : View{};
}

View op_value_ref(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return op_value_non_ref(vd);
    }

    const bool    debug_ref_value = std::getenv("HGRAPH_DEBUG_REF_VALUE_PATH") != nullptr;
    const TSMeta* element_meta = self_meta->element_ts();
    const bool    static_ref_container =
        element_meta != nullptr && dispatch_meta_is_static_container(element_meta);

    // REF[TSD]/REF[TSB]/REF[TSL-fixed] wrappers can be driven by child bindings
    // without a direct parent bind. Materialize the composite REF payload so
    // Python-facing stubs observe REF[child_0, child_1, ...] semantics.
    if (vd.uses_link_target && static_ref_container && has_bound_ref_static_children(vd)) {
        ViewData mutable_vd = vd;
        if (assign_ref_value_from_bound_static_children(mutable_vd)) {
            if (auto synthesized = resolve_value_slot_const(vd);
                synthesized.has_value() && synthesized->valid()) {
                return *synthesized;
            }
        }
    }

    if (auto local = resolve_value_slot_const(vd); local.has_value() && local->valid()) {
        if (local->schema() == ts_reference_meta()) {
            TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
            if (!ref.is_empty()) {
                if (vd.uses_link_target) {
                    // Linked REF inputs can cache a local self-reference to the bound
                    // peer REF endpoint. In that case, expose the peer payload instead
                    // of the wrapper reference itself.
                    const auto same_bound_endpoint = [](const ViewData& lhs, const ViewData& rhs) {
                        return lhs.value_data == rhs.value_data &&
                               lhs.time_data == rhs.time_data &&
                               lhs.observer_data == rhs.observer_data &&
                               lhs.delta_data == rhs.delta_data &&
                               lhs.link_data == rhs.link_data &&
                               lhs.python_value_cache_data == rhs.python_value_cache_data &&
                               lhs.path.indices == rhs.path.indices;
                    };
                    if (const ViewData* local_target = ref.bound_view(); local_target != nullptr) {
                        if (auto peer = resolve_bound_view_data(vd); peer.has_value()) {
                            const bool same_endpoint = same_bound_endpoint(*local_target, *peer);
                            if (debug_ref_value) {
                                std::fprintf(stderr,
                                             "[op_value_ref] path=%s local_target=%s peer=%s same=%d\n",
                                             vd.path.to_string().c_str(),
                                             local_target->path.to_string().c_str(),
                                             peer->path.to_string().c_str(),
                                             same_endpoint ? 1 : 0);
                            }
                            if (!same_endpoint) {
                                return *local;
                            }
                        } else {
                            if (debug_ref_value) {
                                std::fprintf(stderr,
                                             "[op_value_ref] path=%s local_target=%s peer=<none>\n",
                                             vd.path.to_string().c_str(),
                                             local_target->path.to_string().c_str());
                            }
                            return *local;
                        }
                    } else {
                        return *local;
                    }
                } else {
                    return *local;
                }
            }
        } else {
            return *local;
        }
    }

    if (auto bound = resolve_bound_view_data(vd); bound.has_value()) {
        ViewData resolved = *bound;
        bind_view_data_ops(resolved);
        const bool same_view = resolved.value_data == vd.value_data &&
                               resolved.time_data == vd.time_data &&
                               resolved.link_data == vd.link_data &&
                               resolved.python_value_cache_data == vd.python_value_cache_data &&
                               resolved.path.indices == vd.path.indices;
        if (!same_view) {
            const TSMeta* resolved_meta = meta_at_path(resolved.meta, resolved.path.indices);
            const bool resolved_is_ref_wrapper = dispatch_meta_is_ref(resolved_meta);
            if (resolved_is_ref_wrapper) {
                if (resolved.ops != nullptr && resolved.ops->value != nullptr) {
                    return resolved.ops->value(resolved);
                }
                return op_value(resolved);
            }
        }
    }

    return {};
}

View op_value(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->value != nullptr) {
        return dispatch_vd.ops->value(dispatch_vd);
    }
    return op_value_non_ref(dispatch_vd);
}

View op_delta_value_scalar(const ViewData& vd) {
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return {};
    }
    return op_value(vd);
}

View op_delta_value_container(const ViewData& vd) {
    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return {};
    }
    const ViewData* data = &resolved;

    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return {};
    }

    auto* delta_root = static_cast<const Value*>(data->delta_data);
    if (delta_root != nullptr && delta_root->has_value()) {
        if (auto delta_path = ts_path_to_delta_path(data->meta, data->path.indices); delta_path.has_value()) {
            std::optional<View> maybe;
            if (delta_path->empty()) {
                maybe = delta_root->view();
            } else {
                maybe = navigate_const(delta_root->view(), *delta_path);
            }
            if (maybe.has_value()) {
                return *maybe;
            }
        }
    }

    return {};
}

View op_delta_value_tsw_tick(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return {};
    }
    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return {};
    }
    const ViewData* data = &resolved;
    ViewData dispatch_data = dispatch_view_for_path(*data);
    const ts_ops* self_ops = get_ts_ops(self_meta);
    if (dispatch_data.ops != nullptr &&
        dispatch_data.ops != self_ops &&
        dispatch_data.ops->delta_value != nullptr) {
        return dispatch_data.ops->delta_value(dispatch_data);
    }
    const TSMeta*   current = meta_at_path(data->meta, data->path.indices);
    if (current == nullptr) {
        return {};
    }

    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return {};
    }

    View window_value = op_value(*data);
    if (!window_value.valid()) {
        return {};
    }
    if (!window_value.is_cyclic_buffer() || window_value.as_cyclic_buffer().size() == 0) {
        return {};
    }
    auto buffer = window_value.as_cyclic_buffer();
    const void* newest =
        value::CyclicBufferOps::get_element_ptr_const(buffer.data(), buffer.size() - 1, buffer.schema());
    return newest != nullptr ? View(newest, current->value_type) : View{};
}

View op_delta_value_tsw_duration(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return {};
    }
    ViewData resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return {};
    }
    const ViewData* data = &resolved;
    ViewData dispatch_data = dispatch_view_for_path(*data);
    const ts_ops* self_ops = get_ts_ops(self_meta);
    if (dispatch_data.ops != nullptr &&
        dispatch_data.ops != self_ops &&
        dispatch_data.ops->delta_value != nullptr) {
        return dispatch_data.ops->delta_value(dispatch_data);
    }
    const TSMeta* current = meta_at_path(data->meta, data->path.indices);
    if (current == nullptr) {
        return {};
    }

    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return {};
    }

    View window_value = op_value(*data);
    if (!window_value.valid()) {
        return {};
    }

    auto* time_root = static_cast<const Value*>(data->time_data);
    if (time_root == nullptr || !time_root->has_value()) {
        return {};
    }
    auto time_path = ts_path_to_time_path(data->meta, data->path.indices);
    if (time_path.empty()) {
        return {};
    }
    time_path.pop_back();
    std::optional<View> maybe_time;
    if (time_path.empty()) {
        maybe_time = time_root->view();
    } else {
        maybe_time = navigate_const(time_root->view(), time_path);
    }
    if (!maybe_time.has_value() || !maybe_time->valid() || !maybe_time->is_tuple()) {
        return {};
    }
    auto tuple = maybe_time->as_tuple();
    if (tuple.size() < 4) {
        return {};
    }
    View ready = tuple.at(3);
    if (!ready.valid() || !ready.is_scalar_type<bool>() || !ready.as<bool>()) {
        return {};
    }
    if (!window_value.is_queue() || window_value.as_queue().size() == 0) {
        return {};
    }
    auto queue = window_value.as_queue();
    const void* newest = value::QueueOps::get_element_ptr_const(queue.data(), queue.size() - 1, queue.schema());
    return newest != nullptr ? View(newest, current->value_type) : View{};
}

View op_delta_value_tsw(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->delta_value != nullptr) {
        return dispatch_vd.ops->delta_value(dispatch_vd);
    }
    return op_delta_value_tsw_tick(dispatch_vd);
}

View op_delta_value(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->delta_value != nullptr) {
        return dispatch_vd.ops->delta_value(dispatch_vd);
    }
    return op_delta_value_container(dispatch_vd);
}

bool op_has_delta_default(const ViewData& vd) {
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return false;
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_has_added_or_removed(key_set_source);
    }

    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return false;
    }
    const ViewData* data = &resolved;

    auto* delta_root = static_cast<const Value*>(data->delta_data);
    if (delta_root != nullptr && delta_root->has_value()) {
        return true;
    }

    return false;
}

bool op_has_delta(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->has_delta != nullptr) {
        return dispatch_vd.ops->has_delta(dispatch_vd);
    }
    return op_has_delta_default(dispatch_vd);
}

bool op_has_delta_scalar(const ViewData& vd) {
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return false;
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_has_added_or_removed(key_set_source);
    }

    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return false;
    }
    const ViewData* data = &resolved;

    auto* delta_root = static_cast<const Value*>(data->delta_data);
    if (delta_root != nullptr && delta_root->has_value()) {
        return true;
    }

    return dispatch_valid(*data);
}

bool op_has_delta_tss(const ViewData& vd) {
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return false;
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_has_added_or_removed(key_set_source);
    }

    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return false;
    }
    const ViewData* data = &resolved;

    auto* delta_root = static_cast<const Value*>(data->delta_data);
    if (delta_root == nullptr || !delta_root->has_value()) {
        return false;
    }

    std::optional<View> maybe_delta;
    if (data->path.indices.empty()) {
        maybe_delta = delta_root->view();
    } else {
        maybe_delta = navigate_const(delta_root->view(), data->path.indices);
    }
    if (!maybe_delta.has_value() || !maybe_delta->valid() || !maybe_delta->is_tuple()) {
        return false;
    }

    auto tuple = maybe_delta->as_tuple();
    const bool has_added =
        tuple.size() > 0 && tuple.at(0).valid() && tuple.at(0).is_set() && tuple.at(0).as_set().size() > 0;
    const bool has_removed =
        tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set() && tuple.at(1).as_set().size() > 0;
    return has_added || has_removed;
}

bool op_has_delta_tsd(const ViewData& vd) {
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return false;
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_has_added_or_removed(key_set_source);
    }

    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return false;
    }
    const ViewData* data = &resolved;

    auto* delta_root = static_cast<const Value*>(data->delta_data);
    if (delta_root == nullptr || !delta_root->has_value()) {
        return false;
    }

    std::optional<View> maybe_delta;
    if (data->path.indices.empty()) {
        maybe_delta = delta_root->view();
    } else {
        maybe_delta = navigate_const(delta_root->view(), data->path.indices);
    }
    if (!maybe_delta.has_value() || !maybe_delta->valid() || !maybe_delta->is_tuple()) {
        return false;
    }

    auto tuple = maybe_delta->as_tuple();
    const bool has_changed =
        tuple.size() > 0 && tuple.at(0).valid() && tuple.at(0).is_map() && tuple.at(0).as_map().size() > 0;
    const bool has_added =
        tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set() && tuple.at(1).as_set().size() > 0;
    const bool has_removed =
        tuple.size() > 2 && tuple.at(2).valid() && tuple.at(2).is_set() && tuple.at(2).as_set().size() > 0;
    return has_changed || has_added || has_removed;
}

void op_set_value(ViewData& vd, const View& src, engine_time_t current_time) {
    auto* value_root = static_cast<Value*>(vd.value_data);
    if (value_root == nullptr || value_root->schema() == nullptr) {
        return;
    }
    invalidate_python_value_cache(vd);

    if (!value_root->has_value()) {
        value_root->emplace();
    }

    if (vd.path.indices.empty()) {
        if (!src.valid()) {
            value_root->reset();
        } else {
            if (value_root->schema() != src.schema()) {
                throw std::runtime_error(
                    "TS scaffolding set_value root schema mismatch at path " + vd.path.to_string());
            }
            value_root->schema()->ops().copy(value_root->data(), src.data(), value_root->schema());
        }
    } else {
        auto maybe_dst = navigate_mut(value_root->view(), vd.path.indices);
        if (maybe_dst.has_value() && src.valid()) {
            copy_view_data(*maybe_dst, src);
        }
    }
    mark_tsd_parent_child_modified(vd, current_time);
    stamp_time_paths(vd, current_time);
    notify_link_target_observers(vd, current_time);
}

void op_apply_delta_scalar(ViewData& vd, const View& delta, engine_time_t current_time) {
    op_set_value(vd, delta, current_time);
}

void op_apply_delta_container(ViewData& vd, const View& delta, engine_time_t current_time) {
    auto* delta_root = static_cast<Value*>(vd.delta_data);
    if (delta_root == nullptr || delta_root->schema() == nullptr) {
        return;
    }
    invalidate_python_value_cache(vd);

    if (!delta_root->has_value()) {
        delta_root->emplace();
    }

    if (vd.path.indices.empty()) {
        if (!delta.valid()) {
            delta_root->reset();
        } else if (delta_root->schema() == delta.schema()) {
            delta_root->schema()->ops().copy(delta_root->data(), delta.data(), delta_root->schema());
        }
    } else {
        auto maybe_dst = navigate_mut(delta_root->view(), vd.path.indices);
        if (maybe_dst.has_value() && delta.valid()) {
            copy_view_data(*maybe_dst, delta);
        }
    }

    stamp_time_paths(vd, current_time);
    notify_link_target_observers(vd, current_time);
}

void op_apply_delta(ViewData& vd, const View& delta, engine_time_t current_time) {
    bind_view_data_ops(vd);
    if (vd.ops != nullptr && vd.ops->apply_delta != nullptr) {
        vd.ops->apply_delta(vd, delta, current_time);
    } else {
        op_apply_delta_container(vd, delta, current_time);
    }
}

void op_invalidate(ViewData& vd) {
    auto* value_root = static_cast<Value*>(vd.value_data);
    if (value_root == nullptr) {
        return;
    }
    invalidate_python_value_cache(vd);

    const engine_time_t current_time = view_evaluation_time(vd);

    if (vd.path.indices.empty()) {
        value_root->reset();
    }

    if (current_time != MIN_DT) {
        notify_link_target_observers(vd, current_time);
    }
}

void op_invalidate_tsd(ViewData& vd) {
    auto* value_root = static_cast<Value*>(vd.value_data);
    if (value_root == nullptr) {
        return;
    }
    invalidate_python_value_cache(vd);

    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT) {
        auto maybe_current = resolve_value_slot_const(vd);
        if (maybe_current.has_value() && maybe_current->valid() && maybe_current->is_map()) {
            auto slots = resolve_tsd_delta_slots(vd);
            clear_tsd_delta_if_new_tick(vd, current_time, slots);

            const auto current_map = maybe_current->as_map();
            const bool has_added_set = slots.added_set.valid() && slots.added_set.is_set();
            if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                auto removed_set = slots.removed_set.as_set();
                for (View key : current_map.keys()) {
                    if (has_added_set && slots.added_set.as_set().contains(key)) {
                        continue;
                    }
                    removed_set.add(key);
                }
            }
            if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                auto changed_map = slots.changed_values_map.as_map();
                for (View key : current_map.keys()) {
                    changed_map.remove(key);
                }
            }
        }
    }

    if (vd.path.indices.empty()) {
        value_root->reset();
    }

    if (current_time != MIN_DT) {
        notify_link_target_observers(vd, current_time);
    }
}

bool reset_root_value_and_delta_on_none(ViewData& vd,
                                        const nb::object& src,
                                        engine_time_t current_time) {
    if (!vd.path.indices.empty() || !src.is_none()) {
        return false;
    }
    invalidate_python_value_cache(vd);

    if (auto* value_root = static_cast<Value*>(vd.value_data); value_root != nullptr) {
        value_root->reset();
    }
    if (auto* delta_root = static_cast<Value*>(vd.delta_data); delta_root != nullptr) {
        delta_root->reset();
    }
    stamp_time_paths(vd, current_time);
    notify_link_target_observers(vd, current_time);
    return true;
}

}  // namespace hgraph
