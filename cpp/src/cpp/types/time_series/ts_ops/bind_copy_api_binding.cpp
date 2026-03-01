#include "ts_ops_internal.h"

namespace hgraph {

namespace {

uint8_t build_link_observer_notify_policy(const TSMeta* observer_meta, const TSMeta* target_meta) {
    const bool target_is_ref_wrapper = dispatch_meta_is_ref(target_meta);
    const bool target_is_dynamic_container = dispatch_meta_is_dynamic_container(target_meta);
    const bool observer_is_ref_wrapper = dispatch_meta_is_ref(observer_meta);
    const bool observer_is_signal = dispatch_meta_is_signal(observer_meta);
    const bool observer_ref_to_nonref_target = observer_is_ref_wrapper && !target_is_ref_wrapper;
    const bool observer_is_static_container = dispatch_meta_is_static_container(observer_meta);
    const bool notify_on_ref_wrapper_write =
        !target_is_ref_wrapper || observer_is_ref_wrapper || observer_is_static_container || observer_is_signal;

    uint8_t notify_policy = 0;
    if (observer_ref_to_nonref_target && target_is_dynamic_container) {
        notify_policy |= link_observer_notify_policy_bit(LinkObserverNotifyPolicy::RefToNonRefDynamicTarget);
    }
    if (observer_is_signal && observer_is_ref_wrapper && !target_is_ref_wrapper) {
        notify_policy |= link_observer_notify_policy_bit(LinkObserverNotifyPolicy::SignalRefToNonRefTarget);
    }
    if (observer_is_signal) {
        notify_policy |= link_observer_notify_policy_bit(LinkObserverNotifyPolicy::SignalWrapperWrite);
    }
    if (observer_is_signal && observer_is_ref_wrapper) {
        notify_policy |= link_observer_notify_policy_bit(LinkObserverNotifyPolicy::SignalRefWrapperWrite);
    }
    if (!notify_on_ref_wrapper_write) {
        notify_policy |= link_observer_notify_policy_bit(LinkObserverNotifyPolicy::NonRefObserverWrapperWrite);
    }
    return notify_policy;
}

}  // namespace

void op_bind(ViewData& vd, const ViewData& target, engine_time_t current_time) {
    invalidate_python_value_cache(vd);
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
        bind_view_data_ops(bind_target);
        // Non-REF scalar consumers bound to REF wrappers are driven by
        // resolved-target writes/rebind ticks. REF consumers must still observe
        // REF wrapper writes so empty->bound transitions propagate (for example
        // nested tsd_get_items REF->REF chains). Static container consumers
        // (TSB/fixed TSL) also need wrapper writes because unbound REF static
        // payloads are surfaced through wrapper-local state.
        link_target->notify_policy = build_link_observer_notify_policy(current, target_meta);
        link_target->notify_on_ref_wrapper_write = link_observer_notifies_on_ref_wrapper_write(*link_target);
        link_target->observer_is_signal = link_observer_has_notify_policy(
            *link_target, LinkObserverNotifyPolicy::SignalWrapperWrite);
        link_target->observer_ref_to_nonref_target = link_observer_has_notify_policy(
            *link_target, LinkObserverNotifyPolicy::RefToNonRefDynamicTarget) ||
                                               link_observer_has_notify_policy(
            *link_target, LinkObserverNotifyPolicy::SignalRefToNonRefTarget);
        if (debug_op_bind) {
            std::fprintf(stderr,
                         "[op_bind]  lt=%p notify_on_ref_wrapper_write=%d ref_to_nonref=%d notify_policy=0x%x\n",
                         static_cast<void*>(link_target),
                         link_target->notify_on_ref_wrapper_write ? 1 : 0,
                         link_target->observer_ref_to_nonref_target ? 1 : 0,
                         static_cast<unsigned int>(link_target->notify_policy));
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
                                parent_link->python_value_cache_data == target.python_value_cache_data &&
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
    invalidate_python_value_cache(vd);
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

}  // namespace hgraph
