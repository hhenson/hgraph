#include "ts_ops_internal.h"

namespace hgraph {

TSView op_child_at(const ViewData& vd, size_t index, engine_time_t current_time) {
    (void)current_time;
    ViewData child = vd;
    child.path.indices.push_back(index);
    child.ops = get_ts_ops(meta_at_path(child.meta, child.path.indices));
    child.engine_time_ptr = vd.engine_time_ptr;
    return TSView(child, child.engine_time_ptr);
}

TSView op_child_by_name(const ViewData& vd, std::string_view name, engine_time_t current_time) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || !dispatch_meta_is_tsb(current) || current->fields() == nullptr) {
        return {};
    }

    for (size_t i = 0; i < current->field_count(); ++i) {
        if (name == current->fields()[i].name) {
            return op_child_at(vd, i, current_time);
        }
    }
    return {};
}

TSView op_child_by_key(const ViewData& vd, const View& key, engine_time_t current_time) {
    View v = op_value(vd);
    if (!v.valid() || !v.is_map()) {
        return {};
    }

    auto map = v.as_map();
    auto slot = map_slot_for_key(map, key);
    if (slot.has_value()) {
        return op_child_at(vd, *slot, current_time);
    }

    return {};
}

size_t op_list_size(const ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || !dispatch_meta_is_tsl(current)) {
        return 0;
    }

    if (current->fixed_size() > 0) {
        return current->fixed_size();
    }

    View v = op_value(vd);
    return (v.valid() && v.is_list()) ? v.as_list().size() : 0;
}

size_t op_bundle_size(const ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || !dispatch_meta_is_tsb(current)) {
        return 0;
    }
    return current->field_count();
}
View op_observer(const ViewData& vd) {
    auto* observer_root = static_cast<const Value*>(vd.observer_data);
    if (observer_root == nullptr || !observer_root->has_value()) {
        return {};
    }

    const auto observer_path = ts_path_to_observer_path(vd.meta, vd.path.indices);
    std::optional<View> maybe_observer;
    if (observer_path.empty()) {
        maybe_observer = observer_root->view();
    } else {
        maybe_observer = navigate_const(observer_root->view(), observer_path);
    }
    if (!maybe_observer.has_value() || !maybe_observer->valid()) {
        return {};
    }

    // Observer nodes for containers are tuple[container_observer, children...].
    if (maybe_observer->is_tuple()) {
        auto tuple = maybe_observer->as_tuple();
        if (tuple.size() > 0) {
            return tuple.at(0);
        }
    }

    return *maybe_observer;
}

void op_notify_observers(ViewData& vd, engine_time_t current_time) {
    notify_link_target_observers(vd, current_time);
}

void op_bind(ViewData& vd, const ViewData& target, engine_time_t current_time) {
    const bool debug_op_bind = std::getenv("HGRAPH_DEBUG_OP_BIND") != nullptr;
    bool used_ancestor_meta = false;
    const TSMeta* current = resolve_meta_or_ancestor(vd, used_ancestor_meta);
    const TSMeta* target_meta = meta_at_path(target.meta, target.path.indices);
    const bool signal_descendant_bind = used_ancestor_meta && dispatch_meta_is_signal(current);
    const bool current_is_ref_value =
        current != nullptr &&
        dispatch_meta_is_tsvalue(current) &&
        current->value_type != nullptr &&
        current->value_type == ts_reference_meta();
    if (debug_op_bind) {
        const char* node_name = (vd.path.node != nullptr) ? vd.path.node->signature().name.c_str() : "<none>";
        std::fprintf(stderr,
                     "[op_bind] node=%s vd=%s kind=%d val_ref=%d val_type=%s target=%s target_kind=%d uses_lt=%d used_anc=%d sig_desc=%d now=%lld\n",
                     node_name,
                     vd.path.to_string().c_str(),
                     current != nullptr ? static_cast<int>(current->kind) : -1,
                     current_is_ref_value ? 1 : 0,
                     (current != nullptr && current->value_type != nullptr && current->value_type->name != nullptr)
                         ? current->value_type->name
                         : "<none>",
                     target.path.to_string().c_str(),
                     target_meta != nullptr ? static_cast<int>(target_meta->kind) : -1,
                     vd.uses_link_target ? 1 : 0,
                     used_ancestor_meta ? 1 : 0,
                     signal_descendant_bind ? 1 : 0,
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }

    if (vd.uses_link_target) {
        auto* link_target = resolve_link_target(vd, vd.path.indices);
        if (link_target == nullptr) {
            return;
        }
        link_target->observer_view = vd;
        unregister_link_target_observer(*link_target);
        link_target->owner_time_ptr = resolve_owner_time_ptr(vd);
        link_target->parent_link = resolve_parent_link_target(vd);
        ViewData bind_target = target;
        if (dispatch_meta_is_signal(current)) {
            const bool signal_has_impl = signal_input_has_bind_impl(vd, current, link_target);
            const bool key_set_capable_target = [&]() {
                if (target_meta == nullptr) {
                    return false;
                }
                if (dispatch_meta_is_dynamic_container(target_meta)) {
                    return true;
                }
                if (dispatch_meta_is_ref(target_meta)) {
                    const TSMeta* element_meta = target_meta->element_ts();
                    return dispatch_meta_is_dynamic_container(element_meta);
                }
                return false;
            }();
            const bool count_signal_binding =
                vd.path.node != nullptr &&
                vd.path.node->signature().name == "count_impl";
            if (!signal_has_impl && key_set_capable_target && count_signal_binding) {
                bind_target.projection = ViewProjection::TSD_KEY_SET;
            }
            if (debug_op_bind) {
                std::fprintf(stderr,
                             "[op_bind]  signal_bind has_impl=%d count_binding=%d keyset_projection=%d\n",
                             signal_has_impl ? 1 : 0,
                             count_signal_binding ? 1 : 0,
                             bind_target.projection == ViewProjection::TSD_KEY_SET ? 1 : 0);
            }
        }
        // Non-REF scalar consumers bound to REF wrappers are driven by
        // resolved-target writes/rebind ticks. REF consumers must still observe
        // REF wrapper writes so empty->bound transitions propagate (for example
        // nested tsd_get_items REF->REF chains). Static container consumers
        // (TSB/fixed TSL) also need wrapper writes because unbound REF static
        // payloads are surfaced through wrapper-local state.
        const bool target_is_ref_wrapper = dispatch_meta_is_ref(target_meta);
        const bool observer_is_ref_wrapper = dispatch_meta_is_ref(current);
        const bool observer_is_signal = dispatch_meta_is_signal(current);
        const bool observer_ref_to_nonref_target = observer_is_ref_wrapper && !target_is_ref_wrapper;
        const bool observer_is_static_container =
            dispatch_meta_is_static_container(current);
        link_target->notify_on_ref_wrapper_write =
            !target_is_ref_wrapper || observer_is_ref_wrapper || observer_is_static_container || observer_is_signal;
        link_target->observer_is_signal = observer_is_signal;
        link_target->observer_ref_to_nonref_target = observer_ref_to_nonref_target;
        if (debug_op_bind) {
            std::fprintf(stderr,
                         "[op_bind]  lt=%p notify_on_ref_wrapper_write=%d ref_to_nonref=%d\n",
                         static_cast<void*>(link_target),
                         link_target->notify_on_ref_wrapper_write ? 1 : 0,
                         link_target->observer_ref_to_nonref_target ? 1 : 0);
        }

        if (signal_descendant_bind) {
            if (!link_target->is_linked) {
                link_target->bind(bind_target, current_time);
            } else {
                const ViewData normalized_target = [&bind_target]() {
                    ViewData out = bind_target;
                    out.sampled = false;
                    return out;
                }();

                const auto same_target = [&normalized_target](const ViewData& candidate) {
                    return same_view_identity(candidate, normalized_target);
                };

                const ViewData primary_target = link_target->as_view_data(false);
                const bool duplicate_primary = same_target(primary_target);
                const bool duplicate_fan_in = std::any_of(
                    link_target->fan_in_targets.begin(),
                    link_target->fan_in_targets.end(),
                    same_target);

                if (!duplicate_primary && !duplicate_fan_in) {
                    link_target->fan_in_targets.push_back(normalized_target);
                }

                if (current_time != MIN_DT) {
                    link_target->last_rebind_time = current_time;
                    if (link_target->owner_time_ptr != nullptr && *link_target->owner_time_ptr < current_time) {
                        *link_target->owner_time_ptr = current_time;
                    }
                }
            }

            link_target->peered = false;
            link_target->has_resolved_target = false;
            link_target->resolved_target = {};
            register_link_target_observer(*link_target);
            return;
        }

        link_target->bind(bind_target, current_time);
        refresh_dynamic_ref_binding(vd, current_time);
        register_link_target_observer(*link_target);

        // TSB root bind is peered (container slot). Field binds are un-peered.
        link_target->peered = dispatch_meta_is_tsb(current);

        if (link_target->peered && current != nullptr && current->fields() != nullptr) {
            for (size_t i = 0; i < current->field_count(); ++i) {
                std::vector<size_t> child_path = vd.path.indices;
                child_path.push_back(i);
                if (LinkTarget* child_link = resolve_link_target(vd, child_path); child_link != nullptr) {
                    unregister_link_target_observer(*child_link);
                    child_link->unbind();
                    child_link->peered = false;
                }
            }
        }

        // When binding a child of a non-peered container (TSB or fixed-size TSL),
        // unbind the parent container link so scheduling is driven by children.
        if (!vd.path.indices.empty()) {
            std::vector<size_t> parent_path = vd.path.indices;
            parent_path.pop_back();
            const TSMeta* parent_meta = meta_at_path(vd.meta, parent_path);
            const bool non_peer_parent = dispatch_meta_is_static_container(parent_meta);
            if (non_peer_parent) {
                if (LinkTarget* parent_link = resolve_link_target(vd, parent_path); parent_link != nullptr) {
                    if (dispatch_meta_is_tsb(parent_meta)) {
                        // Any direct child bind transitions TSB from peered to un-peered.
                        unregister_link_target_observer(*parent_link);
                        parent_link->unbind();
                        parent_link->peered = false;
                    } else {
                        bool keep_parent_bound = false;

                        const bool static_container_parent =
                            parent_link->is_linked &&
                            dispatch_meta_is_fixed_tsl(parent_meta);

                        if (static_container_parent) {
                            const size_t child_index = vd.path.indices.back();
                            const auto& parent_target_path = parent_link->target_path.indices;
                            const auto& child_target_path = target.path.indices;
                            const TSMeta* parent_target_meta =
                                meta_at_path(target.meta, parent_target_path);

                            keep_parent_bound =
                                child_target_path.size() == parent_target_path.size() + 1 &&
                                std::equal(parent_target_path.begin(), parent_target_path.end(), child_target_path.begin()) &&
                                child_target_path.back() == child_index &&
                                parent_link->value_data == target.value_data &&
                                parent_link->time_data == target.time_data &&
                                parent_link->observer_data == target.observer_data &&
                                parent_link->delta_data == target.delta_data &&
                                parent_link->link_data == target.link_data &&
                                !dispatch_meta_is_ref(parent_target_meta);
                        }

                        parent_link->peered = keep_parent_bound;
                        if (!keep_parent_bound) {
                            unregister_link_target_observer(*parent_link);
                            parent_link->unbind();
                            parent_link->peered = false;
                        }
                    }
                }
            }
        }

        if (dispatch_meta_is_ref(current)) {
            if (!dispatch_meta_is_ref(target_meta)) {
                assign_ref_value_from_target(vd, target);
            } else {
                clear_ref_value(vd);
            }
        }
    } else {
        auto* ref_link = resolve_ref_link(vd, vd.path.indices);
        if (ref_link == nullptr) {
            return;
        }
        const bool was_linked = ref_link->is_linked;
        unregister_ref_link_observer(*ref_link);
        store_to_ref_link(*ref_link, target);
        if (current_time != MIN_DT && was_linked) {
            ref_link->last_rebind_time = current_time;
        } else if (!was_linked) {
            ref_link->last_rebind_time = MIN_DT;
        }
        register_ref_link_observer(*ref_link, &vd);

        if (dispatch_meta_is_ref(current)) {
            const TSMeta* target_meta = meta_at_path(target.meta, target.path.indices);
            if (!dispatch_meta_is_ref(target_meta)) {
                assign_ref_value_from_target(vd, target);
            } else {
                clear_ref_value(vd);
            }
        }
    }

    if (!dispatch_meta_is_ref(current)) {
        clear_ref_container_ancestor_cache(vd);
    }
}

void op_unbind(ViewData& vd, engine_time_t current_time) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);

    if (vd.uses_link_target) {
        auto* lt = resolve_link_target(vd, vd.path.indices);
        if (lt == nullptr) {
            return;
        }
        lt->observer_view = vd;
        unregister_link_target_observer(*lt);
        lt->unbind(current_time);
        lt->peered = false;
    } else {
        auto* ref_link = resolve_ref_link(vd, vd.path.indices);
        if (ref_link == nullptr) {
            return;
        }
        const bool was_linked = ref_link->is_linked;
        unregister_ref_link_observer(*ref_link);
        ref_link->unbind();
        if (current_time != MIN_DT && was_linked) {
            ref_link->last_rebind_time = current_time;
        }
    }

    if (dispatch_meta_is_ref(current)) {
        clear_ref_value(vd);
    } else {
        clear_ref_container_ancestor_cache(vd);
    }
}

bool op_is_bound(const ViewData& vd) {
    if (vd.uses_link_target) {
        if (LinkTarget* payload = resolve_link_target(vd, vd.path.indices); payload != nullptr && payload->is_linked) {
            return true;
        }

        ViewData bound{};
        if (resolve_bound_target_view_data(vd, bound)) {
            // Peered container links (for example TSB root binds) allow child
            // value reads via ancestor resolution, but child links are still
            // considered unbound.
            if (!vd.path.indices.empty()) {
                for (size_t depth = vd.path.indices.size(); depth > 0; --depth) {
                    const std::vector<size_t> parent_path(
                        vd.path.indices.begin(),
                        vd.path.indices.begin() + static_cast<std::ptrdiff_t>(depth - 1));
                    if (LinkTarget* parent = resolve_link_target(vd, parent_path);
                        parent != nullptr && parent->is_linked) {
                        return !parent->peered;
                    }
                }
            }
            return true;
        }
        return false;
    }
    if (REFLink* payload = resolve_ref_link(vd, vd.path.indices); payload != nullptr) {
        return payload->is_linked;
    }
    return false;
}

void set_active_flag(value::ValueView active_view, bool active) {
    if (!active_view.valid()) {
        return;
    }

    if (active_view.is_scalar_type<bool>()) {
        active_view.as<bool>() = active;
        return;
    }

    if (active_view.is_tuple()) {
        auto tuple = active_view.as_tuple();
        if (tuple.size() > 0) {
            auto head = tuple.at(0);
            if (head.valid() && head.is_scalar_type<bool>()) {
                head.as<bool>() = active;
            }
        }
    }
}

bool active_flag_value(value::ValueView active_view) {
    if (!active_view.valid()) {
        return false;
    }

    if (active_view.is_scalar_type<bool>()) {
        return active_view.as<bool>();
    }

    if (active_view.is_tuple()) {
        auto tuple = active_view.as_tuple();
        if (tuple.size() > 0) {
            auto head = tuple.at(0);
            if (head.valid() && head.is_scalar_type<bool>()) {
                return head.as<bool>();
            }
        }
    }
    return false;
}

void op_set_active(ViewData& vd, ValueView active_view, bool active, TSInput* input) {
    const bool was_active = active_flag_value(active_view);
    set_active_flag(active_view, active);
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    const bool ref_local_wrapper = active &&
                                   dispatch_meta_is_ref(current) &&
                                   has_local_ref_wrapper_value(vd);
    const bool ref_static_children = active &&
                                     dispatch_meta_is_ref(current) &&
                                     has_bound_ref_static_children(vd);
    auto set_link_target_active = [input](LinkTarget* target, bool make_active) {
        if (target == nullptr) {
            return;
        }
        Notifiable* new_target = make_active ? static_cast<Notifiable*>(input) : nullptr;
        Notifiable* previous_target = target->active_notifier.target();
        if (previous_target == new_target) {
            return;
        }
        if (target->is_linked && previous_target != nullptr) {
            unregister_active_link_target_observer(*target);
        }
        target->active_notifier.set_target(new_target);
        if (target->is_linked && new_target != nullptr) {
            register_active_link_target_observer(*target);
        }
    };

    if (vd.uses_link_target) {
        if (LinkTarget* payload = resolve_link_target(vd, vd.path.indices); payload != nullptr) {
            if (ref_local_wrapper && input != nullptr) {
                const engine_time_t current_time = resolve_input_current_time(input);
                if (current_time != MIN_DT) {
                    input->notify(current_time);
                }
            }

            if (ref_static_children) {
                if (payload->is_linked && payload->active_notifier.active()) {
                    unregister_active_link_target_observer(*payload);
                }
                payload->active_notifier.set_target(nullptr);

                const TSMeta* element_meta = current != nullptr ? current->element_ts() : nullptr;
                size_t child_count = 0;
                if (element_meta != nullptr) {
                    if (dispatch_meta_is_tsb(element_meta) && element_meta->fields() != nullptr) {
                        child_count = element_meta->field_count();
                    } else if (dispatch_meta_is_fixed_tsl(element_meta)) {
                        child_count = element_meta->fixed_size();
                    }
                }

                for (size_t i = 0; i < child_count; ++i) {
                    std::vector<size_t> child_path = vd.path.indices;
                    child_path.push_back(i);
                    if (LinkTarget* child_link = resolve_link_target(vd, child_path); child_link != nullptr) {
                        set_link_target_active(child_link, active);
                        if (active) {
                            notify_activation_if_modified(child_link, input);
                        }
                    }
                }

                if (input != nullptr &&
                    input->meta() != nullptr &&
                    dispatch_meta_is_ref(input->meta())) {
                    const engine_time_t current_time = resolve_input_current_time(input);
                    if (current_time != MIN_DT) {
                        input->notify(current_time);
                    }
                }
                return;
            }

            set_link_target_active(payload, active);

            if (!payload->is_linked) {
                if (active &&
                    input != nullptr &&
                    input->meta() != nullptr &&
                    dispatch_meta_is_ref(input->meta())) {
                    const engine_time_t current_time = resolve_input_current_time(input);
                    if (current_time != MIN_DT) {
                        input->notify(current_time);
                    }
                }
                return;
            }

            if (active) {
                if (!was_active && payload->last_rebind_time == MIN_DT) {
                    const engine_time_t current_time = resolve_input_current_time(input);
                    if (current_time != MIN_DT) {
                        payload->last_rebind_time = current_time;
                        if (payload->owner_time_ptr != nullptr && *payload->owner_time_ptr < current_time) {
                            *payload->owner_time_ptr = current_time;
                        }
                    }
                }
                notify_activation_if_modified(payload, input);
            }
        }
    } else {
        if (REFLink* payload = resolve_ref_link(vd, vd.path.indices); payload != nullptr) {
            Notifiable* new_target = active ? static_cast<Notifiable*>(input) : nullptr;
            Notifiable* previous_target = payload->active_notifier.target();
            if (previous_target != new_target) {
                if (payload->is_linked && previous_target != nullptr) {
                    unregister_active_ref_link_observer(*payload);
                }
                payload->active_notifier.set_target(new_target);
                if (payload->is_linked && new_target != nullptr) {
                    register_active_ref_link_observer(*payload, &vd);
                }
            }
            if (active) {
                notify_activation_if_modified(payload, input);
            }
        }
    }
}

void op_copy_scalar(ViewData dst, const ViewData& src, engine_time_t current_time) {
    op_set_value(dst, op_value(src), current_time);
}

void op_copy_ref(ViewData dst, const ViewData& src, engine_time_t current_time) {
    if (auto local = resolve_value_slot_const(src);
        local.has_value() && local->valid() && local->schema() == ts_reference_meta()) {
        op_set_value(dst, *local, current_time);
        return;
    }
    op_set_value(dst, op_value(src), current_time);
}

void op_copy_tss(ViewData dst, const ViewData& src, engine_time_t current_time) {
    copy_tss(dst, src, current_time);
}

void op_copy_tsd(ViewData dst, const ViewData& src, engine_time_t current_time) {
    copy_tsd(dst, src, current_time);
}

void op_copy_tsl(ViewData dst, const ViewData& src, engine_time_t current_time) {
    const size_t n = std::min(op_list_size(dst), op_list_size(src));
    for (size_t i = 0; i < n; ++i) {
        TSView src_child = op_child_at(src, i, current_time);
        TSView dst_child = op_child_at(dst, i, current_time);
        if (!src_child || !dst_child) {
            continue;
        }
        if (!op_valid(src_child.view_data())) {
            op_invalidate(dst_child.view_data());
            continue;
        }
        copy_view_data_value_impl(dst_child.view_data(), src_child.view_data(), current_time);
    }
}

void op_copy_tsb(ViewData dst, const ViewData& src, engine_time_t current_time) {
    const TSMeta* dst_meta = meta_at_path(dst.meta, dst.path.indices);
    if (dst_meta == nullptr) {
        return;
    }

    for (size_t i = 0; i < dst_meta->field_count(); ++i) {
        TSView src_child = op_child_at(src, i, current_time);
        TSView dst_child = op_child_at(dst, i, current_time);
        if (!src_child || !dst_child) {
            continue;
        }
        if (!op_valid(src_child.view_data())) {
            op_invalidate(dst_child.view_data());
            continue;
        }
        copy_view_data_value_impl(dst_child.view_data(), src_child.view_data(), current_time);
    }
}

namespace {

using common_ops_customizer = void (*)(ts_ops&);
constexpr size_t k_common_ops_customizer_count = static_cast<size_t>(TSKind::SIGNAL) + size_t{1};

ts_ops make_base_common_ops(TSKind kind) {
    ts_ops out{
        &op_ts_meta,
        &op_last_modified_time,
        &op_modified,
        &op_valid,
        &op_all_valid,
        &op_sampled,
        &op_value,
        &op_delta_value,
        &op_has_delta,
        &op_set_value,
        &op_apply_delta,
        &op_invalidate,
        &op_to_python,
        &op_delta_to_python,
        &op_from_python,
        &op_observer,
        &op_notify_observers,
        &op_bind,
        &op_unbind,
        &op_is_bound,
        &op_set_active,
        &op_copy_scalar,
        kind,
        {},
    };
    out.specific.none = ts_ops::ts_none_ops{0};
    return out;
}

void configure_tsvalue_common_ops(ts_ops& out) {
    out.last_modified_time = &op_last_modified_tsvalue;
    out.valid = &op_valid_tsvalue;
    out.modified = &op_modified_tsvalue;
    out.has_delta = &op_has_delta_scalar;
    out.delta_value = &op_delta_value_scalar;
    out.apply_delta = &op_apply_delta_scalar;
}

void configure_tss_common_ops(ts_ops& out) {
    out.last_modified_time = &op_last_modified_tss;
    out.valid = &op_valid_tss;
    out.modified = &op_modified_tss;
    out.delta_value = &op_delta_value_container;
    out.apply_delta = &op_apply_delta_container;
}

void configure_tsd_common_ops(ts_ops& out) {
    out.last_modified_time = &op_last_modified_tsd;
    out.valid = &op_valid_tsd;
    out.modified = &op_modified_tsd;
    out.delta_value = &op_delta_value_container;
    out.apply_delta = &op_apply_delta_container;
    out.invalidate = &op_invalidate_tsd;
}

void configure_tsl_common_ops(ts_ops& out) {
    out.last_modified_time = &op_last_modified_tsl;
    out.valid = &op_valid_tsl;
    out.modified = &op_modified_tsl;
    out.all_valid = &op_all_valid_tsl;
    out.delta_value = &op_delta_value_container;
    out.apply_delta = &op_apply_delta_container;
}

void configure_tsw_common_ops(ts_ops& out) {
    out.last_modified_time = &op_last_modified_tsw;
    out.all_valid = &op_all_valid_tsw;
    out.delta_value = &op_delta_value_tsw;
    out.apply_delta = &op_apply_delta_container;
}

void configure_tsb_common_ops(ts_ops& out) {
    out.last_modified_time = &op_last_modified_tsb;
    out.valid = &op_valid_tsb;
    out.modified = &op_modified_tsb;
    out.all_valid = &op_all_valid_tsb;
    out.delta_value = &op_delta_value_container;
    out.apply_delta = &op_apply_delta_container;
}

void configure_ref_common_ops(ts_ops& out) {
    out.valid = &op_valid_ref;
    out.value = &op_value_ref;
    out.last_modified_time = &op_last_modified_ref;
    out.modified = &op_modified_ref;
    out.copy_value = &op_copy_ref;
    out.has_delta = &op_has_delta_scalar;
    out.delta_value = &op_delta_value_scalar;
    out.apply_delta = &op_apply_delta_scalar;
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

ts_ops make_tsw_ops() {
    ts_ops out = make_common_ops(TSKind::TSW);
    out.specific.window = k_window_ops;
    return out;
}

ts_ops make_tss_ops() {
    ts_ops out = make_common_ops(TSKind::TSS);
    out.copy_value = &op_copy_tss;
    out.has_delta = &op_has_delta_tss;
    out.specific.set = k_set_ops;
    return out;
}

ts_ops make_tsd_ops() {
    ts_ops out = make_common_ops(TSKind::TSD);
    out.copy_value = &op_copy_tsd;
    out.has_delta = &op_has_delta_tsd;
    out.specific.dict = k_dict_ops;
    return out;
}

ts_ops make_tsl_ops() {
    ts_ops out = make_common_ops(TSKind::TSL);
    out.copy_value = &op_copy_tsl;
    out.specific.list = k_list_ops;
    return out;
}

ts_ops make_tsb_ops() {
    ts_ops out = make_common_ops(TSKind::TSB);
    out.copy_value = &op_copy_tsb;
    out.specific.bundle = k_bundle_ops;
    return out;
}

void copy_tss(ViewData dst, const ViewData& src, engine_time_t current_time) {
    auto maybe_dst = resolve_value_slot_mut(dst);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || !maybe_dst->is_set()) {
        return;
    }

    auto dst_set = maybe_dst->as_set();
    const View src_value = op_value(src);

    std::vector<Value> source_values;
    if (src_value.valid() && src_value.is_set()) {
        auto src_set = src_value.as_set();
        source_values.reserve(src_set.size());
        for (View elem : src_set) {
            source_values.emplace_back(elem.clone());
        }
    }

    std::vector<Value> to_remove;
    to_remove.reserve(dst_set.size());
    for (View elem : dst_set) {
        bool keep = false;
        for (const auto& src_elem : source_values) {
            const View src_view = src_elem.view();
            if (src_view.schema() == elem.schema() && src_view.equals(elem)) {
                keep = true;
                break;
            }
        }
        if (!keep) {
            to_remove.emplace_back(elem.clone());
        }
    }

    std::vector<Value> to_add;
    to_add.reserve(source_values.size());
    for (const auto& src_elem : source_values) {
        if (!dst_set.contains(src_elem.view())) {
            to_add.emplace_back(src_elem.view().clone());
        }
    }

    auto slots = resolve_tss_delta_slots(dst);
    clear_tss_delta_if_new_tick(dst, current_time, slots);

    bool changed = false;
    for (const auto& elem : to_remove) {
        changed = dst_set.remove(elem.view()) || changed;
    }
    for (const auto& elem : to_add) {
        changed = dst_set.add(elem.view()) || changed;
    }

    if (slots.added_set.valid() && slots.added_set.is_set()) {
        auto added = slots.added_set.as_set();
        for (const auto& elem : to_add) {
            added.add(elem.view());
        }
    }
    if (slots.removed_set.valid() && slots.removed_set.is_set()) {
        auto removed = slots.removed_set.as_set();
        for (const auto& elem : to_remove) {
            removed.add(elem.view());
        }
    }

    if (changed) {
        stamp_time_paths(dst, current_time);
        notify_link_target_observers(dst, current_time);
    }
}

void copy_tsd(ViewData dst, const ViewData& src, engine_time_t current_time) {
    auto maybe_dst = resolve_value_slot_mut(dst);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || !maybe_dst->is_map()) {
        return;
    }
    auto dst_map = maybe_dst->as_map();

    const View src_value = op_value(src);

    std::vector<Value> source_keys;
    if (src_value.valid() && src_value.is_map()) {
        auto src_map = src_value.as_map();
        source_keys.reserve(src_map.size());
        for (View key : src_map.keys()) {
            source_keys.emplace_back(key.clone());
        }
    }

    std::vector<Value> to_remove;
    to_remove.reserve(dst_map.size());
    const auto keys_match = [](const View& a, const View& b) -> bool {
        if (!a.valid() || !b.valid()) {
            return false;
        }
        if (a.equals(b)) {
            return true;
        }
        try {
            return a.to_string() == b.to_string();
        } catch (...) {
            return false;
        }
    };
    for (View key : dst_map.keys()) {
        bool keep = false;
        for (const auto& source_key : source_keys) {
            const View source_view = source_key.view();
            if (keys_match(source_view, key)) {
                keep = true;
                break;
            }
        }
        if (!keep) {
            to_remove.emplace_back(key.clone());
        }
    }

    auto slots = resolve_tsd_delta_slots(dst);
    clear_tsd_delta_if_new_tick(dst, current_time, slots);
    bool changed = false;
    const bool debug_copy_keys = std::getenv("HGRAPH_DEBUG_COPY_TSD_KEYS") != nullptr;

    for (const auto& key : to_remove) {
        const auto removed_slot = map_slot_for_key(dst_map, key.view());
        bool removed_was_valid = false;
        if (removed_slot.has_value()) {
            ViewData child_vd = dst;
            child_vd.path.indices.push_back(*removed_slot);
            removed_was_valid = tsd_child_was_visible_before_removal(child_vd);
            record_tsd_removed_child_snapshot(dst, key.view(), child_vd, current_time);
        }
        dst_map.remove(key.view());
        changed = true;
        if (removed_slot.has_value()) {
            compact_tsd_child_time_slot(dst, *removed_slot);
            compact_tsd_child_delta_slot(dst, *removed_slot);
            compact_tsd_child_link_slot(dst, *removed_slot);
        }
        if (slots.removed_set.valid() && slots.removed_set.is_set()) {
            slots.removed_set.as_set().add(key.view());
        }
        if (!removed_was_valid && slots.added_set.valid() && slots.added_set.is_set()) {
            // Marker for key-set change without visible value removal.
            slots.added_set.as_set().add(key.view());
        }
        if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
            slots.changed_values_map.as_map().remove(key.view());
        }
        stamp_time_paths(dst, current_time);
    }

    const value::TypeMeta* value_type = dst_map.value_type();
    if (debug_copy_keys) {
        std::fprintf(stderr,
                     "[copy_tsd_keys] path=%s now=%lld src_keys=%zu dst_keys=%zu\n",
                     dst.path.to_string().c_str(),
                     static_cast<long long>(current_time.time_since_epoch().count()),
                     source_keys.size(),
                     dst_map.size());
        for (View key : dst_map.keys()) {
            std::fprintf(stderr, "[copy_tsd_keys]  dst_key=%s\n", key.to_string().c_str());
        }
    }
    for (const auto& key : source_keys) {
        const bool existed = map_slot_for_key(dst_map, key.view()).has_value();
        if (debug_copy_keys) {
            std::fprintf(stderr,
                         "[copy_tsd_keys]  src_key=%s existed=%d\n",
                         key.view().to_string().c_str(),
                         existed ? 1 : 0);
        }
        if (!existed) {
            if (value_type == nullptr) {
                continue;
            }
            value::Value default_value(value_type);
            default_value.emplace();
            dst_map.set(key.view(), default_value.view());
            stamp_time_paths(dst, current_time);
            changed = true;
        }

        auto slot = map_slot_for_key(dst_map, key.view());
        if (!slot.has_value()) {
            continue;
        }
        Value canonical_key_value = canonical_map_key_for_slot(dst_map, *slot, key.view());
        const View canonical_key = canonical_key_value.view();

        ensure_tsd_child_time_slot(dst, *slot);
        ensure_tsd_child_delta_slot(dst, *slot);
        ensure_tsd_child_link_slot(dst, *slot);
        if (!existed && slots.added_set.valid() && slots.added_set.is_set()) {
            slots.added_set.as_set().add(canonical_key);
        }

        TSView src_child = op_child_by_key(src, key.view(), current_time);
        TSView dst_child = op_child_at(dst, *slot, current_time);
        if (!src_child || !dst_child) {
            continue;
        }

        const bool source_child_modified = op_modified(src_child.view_data(), current_time);
        if (!source_child_modified && existed) {
            if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                slots.changed_values_map.as_map().remove(canonical_key);
            }
            if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                slots.removed_set.as_set().remove(canonical_key);
            }
            continue;
        }

        const engine_time_t before = op_last_modified_time(dst_child.view_data());
        copy_view_data_value_impl(dst_child.view_data(), src_child.view_data(), current_time);
        const bool child_changed = op_last_modified_time(dst_child.view_data()) > before;
        if (child_changed || !existed) {
            changed = true;
        }

        if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
            if (child_changed || !existed) {
                View child_value = op_value(dst_child.view_data());
                if (child_value.valid()) {
                    slots.changed_values_map.as_map().set(canonical_key, child_value);
                }
            } else {
                slots.changed_values_map.as_map().remove(canonical_key);
            }
        }
        if (slots.removed_set.valid() && slots.removed_set.is_set()) {
            slots.removed_set.as_set().remove(canonical_key);
        }
    }

    if (changed) {
        notify_link_target_observers(dst, current_time);
    }
}

void copy_view_data_value_impl(ViewData dst, const ViewData& src, engine_time_t current_time) {
    const TSMeta* dst_meta = meta_at_path(dst.meta, dst.path.indices);
    const TSMeta* src_meta = meta_at_path(src.meta, src.path.indices);
    if (dst_meta == nullptr || src_meta == nullptr) {
        return;
    }

    if (dispatch_meta_ops(dst_meta) != dispatch_meta_ops(src_meta)) {
        throw std::runtime_error("copy_view_data_value: source/destination schema kinds differ");
    }

    const ts_ops* dst_ops = get_ts_ops(dst_meta);
    if (dst_ops->copy_value == nullptr) {
        return;
    }
    dst_ops->copy_value(dst, src, current_time);
}

const ts_ops k_ts_value_ops = make_common_ops(TSKind::TSValue);
const ts_ops k_tss_ops = make_tss_ops();
const ts_ops k_tsd_ops = make_tsd_ops();
const ts_ops k_tsl_ops = make_tsl_ops();
const ts_ops k_tsb_ops = make_tsb_ops();
const ts_ops k_ref_ops = make_common_ops(TSKind::REF);
const ts_ops k_signal_ops = make_common_ops(TSKind::SIGNAL);
const ts_ops k_tsw_tick_ops = make_tsw_ops();
const ts_ops k_tsw_duration_ops = make_tsw_ops();

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
    const ts_ops* out = get_ts_ops(meta->kind);
    if (meta->is_duration_based() && out == &k_tsw_tick_ops) {
        return &k_tsw_duration_ops;
    }
    return out;
}

const ts_ops* default_ts_ops() {
    return &k_ts_value_ops;
}

void store_to_link_target(LinkTarget& target, const ViewData& source) {
    target.bind(source);
}

void store_to_ref_link(REFLink& target, const ViewData& source) {
    const TSMeta* source_meta = meta_at_path(source.meta, source.path.indices);
    if (dispatch_meta_is_ref(source_meta)) {
        target.bind_to_ref(source);
    } else {
        target.bind(source);
    }
}

bool resolve_direct_bound_view_data(const ViewData& source, ViewData& out) {
    if (auto rebound = resolve_bound_view_data(source); rebound.has_value()) {
        out = std::move(*rebound);
        bind_view_data_ops(out);
        return true;
    }
    out = source;
    bind_view_data_ops(out);
    return false;
}

bool resolve_bound_target_view_data(const ViewData& source, ViewData& out) {
    out = source;
    bind_view_data_ops(out);
    bool followed = false;

    for (size_t depth = 0; depth < 64; ++depth) {
        auto rebound = resolve_bound_view_data(out);
        if (!rebound.has_value()) {
            return followed;
        }

        const ViewData next = std::move(rebound.value());
        if (is_same_view_data(next, out)) {
            return false;
        }

        out = next;
        bind_view_data_ops(out);
        followed = true;
    }

    return false;
}

bool resolve_previous_bound_target_view_data(const ViewData& source, ViewData& out) {
    out = source;
    bind_view_data_ops(out);
    if (!source.uses_link_target) {
        return false;
    }

    if (LinkTarget* target = resolve_link_target(source, source.path.indices);
        target != nullptr && target->has_previous_target) {
        out = target->previous_view_data(source.sampled);
        out.projection = merge_projection(source.projection, out.projection);
        bind_view_data_ops(out);
        return true;
    }

    if (!source.path.indices.empty()) {
        for (size_t depth = source.path.indices.size(); depth > 0; --depth) {
            const std::vector<size_t> parent_path(
                source.path.indices.begin(),
                source.path.indices.begin() + static_cast<std::ptrdiff_t>(depth - 1));
            LinkTarget* parent = resolve_link_target(source, parent_path);
            if (parent == nullptr || !parent->has_previous_target) {
                continue;
            }

            ViewData previous = parent->previous_view_data(source.sampled);
            previous.projection = merge_projection(source.projection, previous.projection);

            const std::vector<size_t> residual(
                source.path.indices.begin() + static_cast<std::ptrdiff_t>(parent_path.size()),
                source.path.indices.end());
            if (!residual.empty()) {
                ViewData local_parent = source;
                local_parent.path.indices = parent_path;
                if (auto mapped = remap_residual_indices_for_bound_view(local_parent, previous, residual); mapped.has_value()) {
                    previous.path.indices.insert(previous.path.indices.end(), mapped->begin(), mapped->end());
                } else {
                    previous.path.indices.insert(previous.path.indices.end(), residual.begin(), residual.end());
                }
            }

            out = std::move(previous);
            bind_view_data_ops(out);
            return true;
        }
    }

    return false;
}

std::optional<TSView> resolve_tsd_removed_child_snapshot(const ViewData& parent_view,
                                                         const value::View& key,
                                                         engine_time_t current_time) {
    if (!key.valid() ||
        current_time == MIN_DT ||
        parent_view.value_data == nullptr) {
        return std::nullopt;
    }

    std::vector<ViewData> lookup_views;
    lookup_views.push_back(parent_view);

    const TSMeta* expected_parent_meta = meta_at_path(parent_view.meta, parent_view.path.indices);
    const TSMeta* expected_child_meta =
        dispatch_meta_is_tsd(expected_parent_meta)
            ? expected_parent_meta->element_ts()
            : nullptr;

    ViewData bound_parent{};
    if (resolve_bound_target_view_data(parent_view, bound_parent) &&
        !same_view_identity(bound_parent, parent_view)) {
        lookup_views.push_back(bound_parent);
    }

    for (const ViewData& lookup_view : lookup_views) {
        if (lookup_view.link_observer_registry == nullptr) {
            continue;
        }

        std::shared_ptr<void> existing =
            lookup_view.link_observer_registry->feature_state(k_tsd_removed_snapshot_state_key);
        if (!existing) {
            continue;
        }

        auto state = std::static_pointer_cast<TsdRemovedChildSnapshotState>(std::move(existing));
        auto by_parent = state->entries.find(lookup_view.value_data);
        if (by_parent == state->entries.end()) {
            continue;
        }

        auto& records = by_parent->second;
        records.erase(
            std::remove_if(
                records.begin(),
                records.end(),
                [current_time](const TsdRemovedChildSnapshotRecord& record) {
                    return record.time < current_time;
                }),
            records.end());
        if (records.empty()) {
            state->entries.erase(by_parent);
            continue;
        }

        for (const auto& record : records) {
            if (record.time != current_time ||
                record.parent_path != lookup_view.path.indices ||
                !key_matches_relaxed(record.key.view(), key) ||
                !record.snapshot) {
                continue;
            }

            TSView view = record.snapshot->ts_view(parent_view.engine_time_ptr);
            if (expected_child_meta != nullptr) {
                const TSMeta* snapshot_meta = view.ts_meta();
                if (snapshot_meta == nullptr || dispatch_meta_ops(snapshot_meta) != dispatch_meta_ops(expected_child_meta)) {
                    if (dispatch_meta_is_ref(snapshot_meta)) {
                        ViewData resolved_snapshot{};
                        if (resolve_bound_target_view_data(view.view_data(), resolved_snapshot)) {
                            const TSMeta* resolved_meta =
                                meta_at_path(resolved_snapshot.meta, resolved_snapshot.path.indices);
                            if (resolved_meta != nullptr && dispatch_meta_ops(resolved_meta) == dispatch_meta_ops(expected_child_meta)) {
                                TSView resolved_view(resolved_snapshot, parent_view.engine_time_ptr);
                                resolved_view.view_data().sampled = true;
                                return resolved_view;
                            }
                        }
                    }
                    continue;
                }
            }

            view.view_data().sampled = true;
            return view;
        }
    }

    return std::nullopt;
}

void copy_view_data_value(ViewData& dst, const ViewData& src, engine_time_t current_time) {
    copy_view_data_value_impl(dst, src, current_time);
}

void notify_ts_link_observers(const ViewData& target_view, engine_time_t current_time) {
    notify_link_target_observers(target_view, current_time);
}

void register_ts_link_observer(LinkTarget& observer) {
    register_link_target_observer(observer);
}

void unregister_ts_link_observer(LinkTarget& observer) {
    unregister_link_target_observer(observer);
}

void register_ts_ref_link_observer(REFLink& observer) {
    register_ref_link_observer(observer);
}

void unregister_ts_ref_link_observer(REFLink& observer) {
    unregister_ref_link_observer(observer);
}

void register_ts_active_link_observer(LinkTarget& observer) {
    register_active_link_target_observer(observer);
}

void unregister_ts_active_link_observer(LinkTarget& observer) {
    unregister_active_link_target_observer(observer);
}

void register_ts_active_ref_link_observer(REFLink& observer) {
    register_active_ref_link_observer(observer);
}

void unregister_ts_active_ref_link_observer(REFLink& observer) {
    unregister_active_ref_link_observer(observer);
}

void reset_ts_link_observers() {
    // Registries are endpoint-owned and do not require process-global cleanup.
}




}  // namespace hgraph
