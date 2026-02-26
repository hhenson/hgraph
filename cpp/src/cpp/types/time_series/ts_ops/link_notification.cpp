#include "ts_ops_internal.h"

namespace hgraph {

template <typename T, typename IsLiveFn>
T* as_live_observer(Notifiable* notifier, IsLiveFn&& is_live) {
    if (notifier == nullptr) {
        return nullptr;
    }
    T* candidate = reinterpret_cast<T*>(notifier);
    return is_live(candidate) ? candidate : nullptr;
}

TSInput* notifier_as_live_input(Notifiable* notifier) {
    // Treat notifier pointers as opaque addresses first; some observer slots can
    // transiently contain stale pointers during teardown and RTTI probes would
    // touch invalid object memory.
    return as_live_observer<TSInput>(notifier, is_live_ts_input);
}

engine_time_t view_evaluation_time(const ViewData& vd) {
    return vd.engine_time_ptr != nullptr ? *vd.engine_time_ptr : MIN_DT;
}

bool is_prefix_path(const std::vector<size_t>& lhs, const std::vector<size_t>& rhs) {
    if (lhs.size() > rhs.size()) {
        return false;
    }
    return std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

bool paths_related(const std::vector<size_t>& lhs, const std::vector<size_t>& rhs) {
    return is_prefix_path(lhs, rhs) || is_prefix_path(rhs, lhs);
}

bool is_static_container_meta(const TSMeta* meta) {
    return meta != nullptr && meta->kind == TSKind::TSB;
}

bool find_link_target_path(const ViewData& root_view,
                           const TSMeta* root_meta,
                           const LinkTarget* needle,
                           std::vector<size_t>& out_path) {
    if (needle == nullptr || root_meta == nullptr) {
        return false;
    }

    std::vector<size_t> path;
    std::function<bool(const TSMeta*)> visit = [&](const TSMeta* meta) -> bool {
        if (LinkTarget* current = resolve_link_target(root_view, path); current == needle) {
            out_path = path;
            return true;
        }

        const TSMeta* cursor = meta;
        while (cursor != nullptr && cursor->kind == TSKind::REF) {
            cursor = cursor->element_ts();
        }
        if (cursor == nullptr) {
            return false;
        }

        switch (cursor->kind) {
            case TSKind::TSB:
                if (cursor->fields() == nullptr) {
                    return false;
                }
                for (size_t i = 0; i < cursor->field_count(); ++i) {
                    path.push_back(i);
                    if (visit(cursor->fields()[i].ts_type)) {
                        return true;
                    }
                    path.pop_back();
                }
                return false;

            case TSKind::TSL:
                if (cursor->fixed_size() == 0) {
                    return false;
                }
                for (size_t i = 0; i < cursor->fixed_size(); ++i) {
                    path.push_back(i);
                    if (visit(cursor->element_ts())) {
                        return true;
                    }
                    path.pop_back();
                }
                return false;

            default:
                return false;
        }
    };

    return visit(root_meta);
}

bool observer_under_static_ref_container(const LinkTarget& observer) {
    auto* active_input = notifier_as_live_input(observer.active_notifier.target());
    if (active_input == nullptr || active_input->meta() == nullptr) {
        return false;
    }

    TSView root = active_input->view();
    root.set_current_time_ptr(&MIN_DT);
    if (!root) {
        return false;
    }

    std::vector<size_t> observer_path;
    if (!find_link_target_path(root.view_data(), active_input->meta(), &observer, observer_path)) {
        return false;
    }
    if (observer_path.empty()) {
        return false;
    }

    std::vector<size_t> parent_path = observer_path;
    parent_path.pop_back();
    const TSMeta* parent_meta = meta_at_path(active_input->meta(), parent_path);
    if (parent_meta == nullptr || parent_meta->kind != TSKind::REF) {
        return false;
    }
    return is_static_container_meta(parent_meta->element_ts());
}

bool signal_input_has_bind_impl(const ViewData& vd, const TSMeta* current_meta, const LinkTarget* signal_link) {
    const bool debug_signal_impl = std::getenv("HGRAPH_DEBUG_SIGNAL_IMPL") != nullptr;
    if (current_meta == nullptr || current_meta->kind != TSKind::SIGNAL) {
        return false;
    }
    if (signal_link == nullptr) {
        return false;
    }

    auto* signal_input = notifier_as_live_input(signal_link->active_notifier.target());
    if (signal_input == nullptr) {
        if (debug_signal_impl) {
            std::fprintf(stderr,
                         "[signal_impl] path=%s no_active_input\n",
                         vd.path.to_string().c_str());
        }
        return false;
    }

    if (vd.path.indices.empty()) {
        return false;
    }

    const bool has_impl = signal_input->signal_input_has_impl(vd.path.indices);
    if (debug_signal_impl) {
        std::fprintf(stderr,
                     "[signal_impl] path=%s has_impl=%d\n",
                     vd.path.to_string().c_str(),
                     has_impl ? 1 : 0);
    }
    return has_impl;
}

engine_time_t resolve_input_current_time(const TSInput* input) {
    if (input == nullptr) {
        return MIN_DT;
    }
    node_ptr owner = input->owning_node();
    if (owner == nullptr) {
        return MIN_DT;
    }
    if (const engine_time_t* et = owner->cached_evaluation_time_ptr(); et != nullptr) {
        if (*et != MIN_DT) {
            return *et;
        }
    }
    graph_ptr g = owner->graph();
    if (g == nullptr) {
        return MIN_DT;
    }
    if (auto api = g->evaluation_engine_api(); api != nullptr) {
        return api->start_time();
    }
    return g->evaluation_time();
}

void notify_activation_if_modified(LinkTarget* payload, TSInput* input) {
    if (payload == nullptr || input == nullptr || !payload->is_linked || payload->ops == nullptr) {
        return;
    }

    const bool debug_activate = std::getenv("HGRAPH_DEBUG_ACTIVATE") != nullptr;
    const engine_time_t current_time = resolve_input_current_time(input);
    if (current_time == MIN_DT) {
        if (debug_activate) {
            std::fprintf(stderr,
                         "[activate_lt] current=MIN_DT linked=1 rebind=%lld\n",
                         static_cast<long long>(payload->last_rebind_time.time_since_epoch().count()));
        }
        return;
    }

    const bool sampled_on_rebind = payload->last_rebind_time == current_time;
    ViewData target_vd = payload->as_view_data(false);
    if (!payload->ops->valid(target_vd)) {
        if (sampled_on_rebind && payload->has_previous_target) {
            payload->notify_active(current_time);
        }
        if (debug_activate) {
            std::fprintf(stderr,
                         "[activate_lt] current=%lld target_valid=0 sampled_on_rebind=%d has_prev=%d rebind=%lld\n",
                         static_cast<long long>(current_time.time_since_epoch().count()),
                         sampled_on_rebind ? 1 : 0,
                         payload->has_previous_target ? 1 : 0,
                         static_cast<long long>(payload->last_rebind_time.time_since_epoch().count()));
        }
        return;
    }
    const bool target_modified = payload->ops->modified(target_vd, current_time);
    if (debug_activate) {
        std::fprintf(stderr,
                     "[activate_lt] current=%lld target_modified=%d sampled_on_rebind=%d rebind=%lld\n",
                     static_cast<long long>(current_time.time_since_epoch().count()),
                     target_modified ? 1 : 0,
                     sampled_on_rebind ? 1 : 0,
                     static_cast<long long>(payload->last_rebind_time.time_since_epoch().count()));
    }
    if (!target_modified && !sampled_on_rebind) {
        return;
    }
    payload->notify_active(sampled_on_rebind ? current_time : payload->ops->last_modified_time(target_vd));
}

void notify_activation_if_modified(REFLink* payload, TSInput* input) {
    if (payload == nullptr || input == nullptr || !payload->valid()) {
        return;
    }

    const engine_time_t current_time = resolve_input_current_time(input);
    if (current_time == MIN_DT) {
        return;
    }
    const bool sampled_on_rebind = payload->last_rebind_time == current_time;
    ViewData target_vd = payload->resolved_view_data();
    if (target_vd.ops == nullptr) {
        return;
    }
    if (!target_vd.ops->valid(target_vd)) {
        if (sampled_on_rebind) {
            payload->notify_active(current_time);
        }
        return;
    }

    const bool target_modified = target_vd.ops->modified(target_vd, current_time);
    if (!target_modified && !sampled_on_rebind) {
        return;
    }
    payload->notify_active(sampled_on_rebind ? current_time : target_vd.ops->last_modified_time(target_vd));
}

ObserverList* observer_list_from_node(ValueView node, bool materialize) {
    const value::TypeMeta* observer_list_type = value::TypeRegistry::instance().get_by_name("ObserverList");
    if (observer_list_type == nullptr) {
        return nullptr;
    }

    ValueView slot = node;
    if (slot.is_tuple()) {
        auto tuple = slot.as_tuple();
        if (tuple.size() == 0) {
            return nullptr;
        }
        slot = tuple.at(0);
        if (!slot.valid()) {
            if (!materialize || tuple.schema() == nullptr || tuple.schema()->field_count == 0) {
                return nullptr;
            }
            const value::TypeMeta* slot_type = tuple.schema()->fields[0].type;
            if (slot_type == nullptr) {
                return nullptr;
            }
            Value materialized(slot_type);
            materialized.emplace();
            tuple.set(0, materialized.view());
            slot = tuple.at(0);
        }
    }

    if (!slot.valid() || slot.schema() != observer_list_type) {
        return nullptr;
    }
    return static_cast<ObserverList*>(slot.data());
}

const ObserverList* observer_list_from_node(View node) {
    const value::TypeMeta* observer_list_type = value::TypeRegistry::instance().get_by_name("ObserverList");
    if (observer_list_type == nullptr) {
        return nullptr;
    }

    View slot = node;
    if (slot.is_tuple()) {
        auto tuple = slot.as_tuple();
        if (tuple.size() == 0) {
            return nullptr;
        }
        slot = tuple.at(0);
    }

    if (!slot.valid() || slot.schema() != observer_list_type) {
        return nullptr;
    }
    return static_cast<const ObserverList*>(slot.data());
}

bool ensure_observer_path_materialized(ValueView observer_root,
                                       const TSMeta* root_meta,
                                       const std::vector<size_t>& ts_path) {
    ValueView current = observer_root;
    const TSMeta* meta = root_meta;

    for (size_t index : ts_path) {
        while (meta != nullptr && meta->kind == TSKind::REF) {
            meta = meta->element_ts();
        }
        if (meta == nullptr) {
            return false;
        }

        if (!current.valid() || !current.is_tuple()) {
            return false;
        }
        auto tuple = current.as_tuple();

        switch (meta->kind) {
            case TSKind::TSB: {
                const size_t child_index = index + 1;
                if (child_index >= tuple.size()) {
                    return false;
                }
                ValueView child = tuple.at(child_index);
                if (!child.valid()) {
                    if (tuple.schema() == nullptr || child_index >= tuple.schema()->field_count) {
                        return false;
                    }
                    const value::TypeMeta* child_type = tuple.schema()->fields[child_index].type;
                    if (child_type == nullptr) {
                        return false;
                    }
                    Value materialized(child_type);
                    materialized.emplace();
                    tuple.set(child_index, materialized.view());
                    child = tuple.at(child_index);
                }
                current = child;
                if (meta->fields() == nullptr || index >= meta->field_count()) {
                    return false;
                }
                meta = meta->fields()[index].ts_type;
                break;
            }
            case TSKind::TSL:
            case TSKind::TSD: {
                if (tuple.size() < 2) {
                    return false;
                }
                ValueView children = tuple.at(1);
                if (!children.valid()) {
                    if (tuple.schema() == nullptr || tuple.schema()->field_count < 2) {
                        return false;
                    }
                    const value::TypeMeta* children_type = tuple.schema()->fields[1].type;
                    if (children_type == nullptr) {
                        return false;
                    }
                    Value materialized(children_type);
                    materialized.emplace();
                    tuple.set(1, materialized.view());
                    children = tuple.at(1);
                }
                if (!children.valid() || !children.is_list()) {
                    return false;
                }
                auto list = children.as_list();
                const size_t old_size = list.size();
                if (index >= old_size) {
                    list.resize(index + 1);
                    const value::TypeMeta* list_type = list.schema();
                    if (list_type == nullptr) {
                        return false;
                    }
                    for (size_t i = old_size; i < list.size(); ++i) {
                        list_type->ops().set_at(list.data(), i, nullptr, list_type);
                    }
                }

                ValueView child = list.at(index);
                if (!child.valid()) {
                    const value::TypeMeta* child_type = list.element_type();
                    if (child_type == nullptr) {
                        return false;
                    }
                    Value materialized(child_type);
                    materialized.emplace();
                    list.set(index, materialized.view());
                    child = list.at(index);
                }
                current = child;
                meta = meta->element_ts();
                break;
            }
            default:
                return false;
        }
    }

    return true;
}

ObserverList* resolve_observer_list_for_path_mut(const ViewData& target_view,
                                                 const std::vector<size_t>& ts_path,
                                                 bool materialize) {
    auto* observer_root = static_cast<Value*>(target_view.observer_data);
    if (observer_root == nullptr || observer_root->schema() == nullptr) {
        return nullptr;
    }
    if (!observer_root->has_value()) {
        if (!materialize) {
            return nullptr;
        }
        observer_root->emplace();
    }

    if (materialize &&
        !ensure_observer_path_materialized(observer_root->view(), target_view.meta, ts_path)) {
        return nullptr;
    }

    const auto observer_path = ts_path_to_observer_path(target_view.meta, ts_path);
    std::optional<ValueView> maybe_node =
        observer_path.empty() ? std::optional<ValueView>{observer_root->view()}
                              : navigate_mut(observer_root->view(), observer_path);
    if (!maybe_node.has_value()) {
        return nullptr;
    }
    return observer_list_from_node(*maybe_node, materialize);
}

void subscribe_target_observer(const ViewData& target_view, Notifiable* observer) {
    if (observer == nullptr || target_view.meta == nullptr || target_view.observer_data == nullptr) {
        return;
    }
    if (ObserverList* list = resolve_observer_list_for_path_mut(target_view, target_view.path.indices, true);
        list != nullptr) {
        list->subscribe(observer);
    }
}

void unsubscribe_target_observer(const ViewData& target_view, Notifiable* observer) {
    if (observer == nullptr || target_view.meta == nullptr || target_view.observer_data == nullptr) {
        return;
    }
    if (ObserverList* list = resolve_observer_list_for_path_mut(target_view, target_view.path.indices, false);
        list != nullptr) {
        list->unsubscribe(observer);
    }
}

void unregister_link_target_observer(const LinkTarget& link_target) {
    auto* target = const_cast<LinkTarget*>(&link_target);
    auto unregister_target = [target](const ViewData& view) {
        unsubscribe_target_observer(view, target);
    };

    if (link_target.is_linked) {
        unregister_target(link_target.as_view_data(false));
    }
    for (const ViewData& fan_in_target : link_target.fan_in_targets) {
        unregister_target(fan_in_target);
    }
    if (link_target.has_resolved_target && link_target.resolved_target.meta != nullptr) {
        unregister_target(link_target.resolved_target);
    }
}

void unregister_ref_link_observer(const REFLink& ref_link) {
    auto* target = const_cast<REFLink*>(&ref_link);
    if (ref_link.source.meta != nullptr) {
        unsubscribe_target_observer(ref_link.source, target);
    }
    if (ref_link.target.meta != nullptr &&
        !same_view_identity(ref_link.source, ref_link.target)) {
        unsubscribe_target_observer(ref_link.target, target);
    }
}

void unregister_active_link_target_observer(const LinkTarget&) {}

void unregister_active_ref_link_observer(const REFLink&) {}

void register_link_target_observer(const LinkTarget& link_target) {
    if (!link_target.is_linked) {
        return;
    }

    auto* target = const_cast<LinkTarget*>(&link_target);
    auto register_target = [target](const ViewData& view) {
        subscribe_target_observer(view, target);
    };

    register_target(link_target.as_view_data(false));
    for (const ViewData& extra_target : link_target.fan_in_targets) {
        register_target(extra_target);
    }
    if (link_target.has_resolved_target && link_target.resolved_target.meta != nullptr) {
        register_target(link_target.resolved_target);
    }
}

void register_active_link_target_observer(const LinkTarget&) {}

void refresh_dynamic_ref_binding_for_link_target(LinkTarget* link_target, bool sampled, engine_time_t current_time) {
    if (link_target == nullptr || !link_target->is_linked) {
        return;
    }
    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;

    ViewData source_view = link_target->as_view_data(sampled);
    const auto resolve_rebind_stamp = [&](const ViewData& source) {
        if (current_time != MIN_DT) {
            return current_time;
        }
        engine_time_t stamp = direct_last_modified_time(source);
        if (stamp == MIN_DT) {
            stamp = view_evaluation_time(source);
        }
        return stamp;
    };
    ViewData resolved_target{};
    const bool has_resolved_target =
        resolve_read_view_data(source_view, nullptr, resolved_target) &&
        !same_view_identity(resolved_target, source_view);
    if (debug_keyset_bridge) {
        const TSMeta* source_meta = meta_at_path(source_view.meta, source_view.path.indices);
        std::fprintf(stderr,
                     "[refresh_ref] source=%s source_kind=%d linked=%d prev=%d resolved=%d -> has_resolved=%d resolved_path=%s\n",
                     source_view.path.to_string().c_str(),
                     source_meta != nullptr ? static_cast<int>(source_meta->kind) : -1,
                     link_target->is_linked ? 1 : 0,
                     link_target->has_previous_target ? 1 : 0,
                     link_target->has_resolved_target ? 1 : 0,
                     has_resolved_target ? 1 : 0,
                     has_resolved_target ? resolved_target.path.to_string().c_str() : "<none>");
    }

    if (!has_resolved_target) {
        const bool had_resolved = link_target->has_resolved_target;
        ViewData previous_target{};
        if (had_resolved) {
            previous_target = link_target->resolved_target;
        }

        link_target->has_resolved_target = false;
        link_target->resolved_target = {};
        if (had_resolved) {
            // Preserve bridge context when a REF transitions from a concrete
            // target to empty so downstream consumers can emit removals.
            link_target->has_previous_target = true;
            link_target->previous_target = previous_target;
            unregister_link_target_observer(*link_target);
            register_link_target_observer(*link_target);

            engine_time_t stamp = resolve_rebind_stamp(source_view);
            if (stamp != MIN_DT) {
                link_target->last_rebind_time = stamp;
                if (link_target->owner_time_ptr != nullptr && *link_target->owner_time_ptr < stamp) {
                    *link_target->owner_time_ptr = stamp;
                }
            }
        } else if (!link_target->has_previous_target) {
            link_target->has_previous_target = false;
            link_target->previous_target = {};
        }
        return;
    }

    if (!link_target->has_resolved_target) {
        // Transition from unresolved -> resolved is a rebind from empty. Keep
        // bridge state as "no previous target" so key_set consumers can emit
        // full added snapshots on reactivation.
        link_target->has_previous_target = false;
        link_target->previous_target = {};
        link_target->has_resolved_target = has_resolved_target;
        link_target->resolved_target = has_resolved_target ? resolved_target : ViewData{};
        if (has_resolved_target) {
            unregister_link_target_observer(*link_target);
            register_link_target_observer(*link_target);
        }

        engine_time_t stamp = resolve_rebind_stamp(source_view);
        if (stamp != MIN_DT) {
            link_target->last_rebind_time = stamp;
            if (link_target->owner_time_ptr != nullptr && *link_target->owner_time_ptr < stamp) {
                *link_target->owner_time_ptr = stamp;
            }
        }
        return;
    }

    bool changed = has_resolved_target != link_target->has_resolved_target;
    if (!changed && has_resolved_target) {
        changed = !same_view_identity(resolved_target, link_target->resolved_target);
    }
    if (!changed) {
        return;
    }

    if (link_target->has_resolved_target) {
        link_target->has_previous_target = true;
        link_target->previous_target = link_target->resolved_target;
    } else {
        link_target->has_previous_target = false;
        link_target->previous_target = {};
    }

    link_target->has_resolved_target = has_resolved_target;
    link_target->resolved_target = has_resolved_target ? resolved_target : ViewData{};
    unregister_link_target_observer(*link_target);
    register_link_target_observer(*link_target);

    engine_time_t stamp = resolve_rebind_stamp(source_view);
    if (stamp != MIN_DT) {
        link_target->last_rebind_time = stamp;
        if (link_target->owner_time_ptr != nullptr && *link_target->owner_time_ptr < stamp) {
            *link_target->owner_time_ptr = stamp;
        }
    }
}

bool view_path_contains_tsd_ancestor(const ViewData& view) {
    const TSMeta* current = view.meta;
    if (current == nullptr) {
        return false;
    }
    if (current->kind == TSKind::TSD) {
        return true;
    }

    for (size_t index : view.path.indices) {
        while (current != nullptr && current->kind == TSKind::REF) {
            current = current->element_ts();
        }
        if (current == nullptr) {
            return false;
        }
        if (current->kind == TSKind::TSD) {
            return true;
        }

        switch (current->kind) {
            case TSKind::TSB:
                if (current->fields() == nullptr || index >= current->field_count()) {
                    return false;
                }
                current = current->fields()[index].ts_type;
                break;
            case TSKind::TSL:
            case TSKind::TSD:
                current = current->element_ts();
                break;
            default:
                return false;
        }
    }

    return current != nullptr && current->kind == TSKind::TSD;
}

void register_ref_link_observer(const REFLink& ref_link, const ViewData* observer_view) {
    if (!ref_link.is_linked) {
        return;
    }

    (void)observer_view;
    auto* target = const_cast<REFLink*>(&ref_link);
    const ViewData& target_view = ref_link.has_target() ? ref_link.target : ref_link.source;
    subscribe_target_observer(target_view, target);
}

void register_active_ref_link_observer(const REFLink& ref_link, const ViewData* observer_view) {
    (void)ref_link;
    (void)observer_view;
}

bool suppress_static_ref_child_notification(const LinkTarget& observer, engine_time_t current_time) {
    if (observer.parent_link == nullptr) {
        return false;
    }
    if (!observer.active_notifier.active()) {
        return false;
    }
    if (observer.owner_time_ptr != nullptr) {
        return false;
    }
    if (observer.last_rebind_time == current_time) {
        return false;
    }
    if (!observer_under_static_ref_container(observer)) {
        return false;
    }
    // Static REF child links that observe resolved targets through non-wrapper
    // write paths (notify_on_ref_wrapper_write==false) must continue
    // propagating child writes after activation.
    if (observer.has_resolved_target && !observer.notify_on_ref_wrapper_write) {
        return false;
    }
    return true;
}

namespace {

template <typename LinkFn, typename RefFn>
void append_observer_list_registrations(
    const ObserverList* observer_list,
    const std::vector<size_t>& path,
    LinkFn& on_link_observer,
    RefFn& on_ref_observer) {
    if (observer_list == nullptr) {
        return;
    }

    for (Notifiable* observer : observer_list->observers) {
        // Keep pointer probing address-only, then validate liveness via
        // endpoint-owned registries before use.
        if (LinkTarget* link_target = as_live_observer<LinkTarget>(observer, is_live_link_target);
            link_target != nullptr) {
            on_link_observer(path, link_target);
            continue;
        }
        if (REFLink* ref_link = as_live_observer<REFLink>(observer, is_live_ref_link);
            ref_link != nullptr) {
            on_ref_observer(path, ref_link);
        }
    }
}

template <typename LinkFn, typename RefFn>
void append_observer_node_registrations(
    const View& observer_node,
    const std::vector<size_t>& path,
    LinkFn& on_link_observer,
    RefFn& on_ref_observer) {
    append_observer_list_registrations(
        observer_list_from_node(observer_node),
        path,
        on_link_observer,
        on_ref_observer);
}

bool advance_observer_subtree_node(const TSMeta*& meta, View& observer_node, size_t child_index) {
    while (meta != nullptr && meta->kind == TSKind::REF) {
        meta = meta->element_ts();
    }
    if (meta == nullptr || !observer_node.valid() || !observer_node.is_tuple()) {
        return false;
    }

    auto tuple = observer_node.as_tuple();
    switch (meta->kind) {
        case TSKind::TSB: {
            if (meta->fields() == nullptr || child_index >= meta->field_count()) {
                return false;
            }
            const size_t observer_child_index = child_index + 1;
            if (observer_child_index >= tuple.size()) {
                return false;
            }
            View child = tuple.at(observer_child_index);
            if (!child.valid()) {
                return false;
            }
            observer_node = child;
            meta = meta->fields()[child_index].ts_type;
            return true;
        }
        case TSKind::TSL:
        case TSKind::TSD: {
            if (tuple.size() < 2) {
                return false;
            }
            View children = tuple.at(1);
            if (!children.valid() || !children.is_list()) {
                return false;
            }
            auto list = children.as_list();
            if (child_index >= list.size()) {
                return false;
            }
            View child = list.at(child_index);
            if (!child.valid()) {
                return false;
            }
            observer_node = child;
            meta = meta->element_ts();
            return true;
        }
        default:
            return false;
    }
}

template <typename LinkFn, typename RefFn>
void collect_observer_subtree_registrations(
    const TSMeta* meta,
    const View& observer_node,
    std::vector<size_t>& path,
    bool include_current,
    LinkFn& on_link_observer,
    RefFn& on_ref_observer) {
    if (include_current) {
        append_observer_node_registrations(
            observer_node,
            path,
            on_link_observer,
            on_ref_observer);
    }

    const TSMeta* cursor = meta;
    while (cursor != nullptr && cursor->kind == TSKind::REF) {
        cursor = cursor->element_ts();
    }
    if (cursor == nullptr || !observer_node.valid() || !observer_node.is_tuple()) {
        return;
    }

    auto tuple = observer_node.as_tuple();
    switch (cursor->kind) {
        case TSKind::TSB:
            if (cursor->fields() == nullptr) {
                return;
            }
            for (size_t i = 0; i < cursor->field_count(); ++i) {
                const size_t child_slot = i + 1;
                if (child_slot >= tuple.size()) {
                    return;
                }
                View child = tuple.at(child_slot);
                if (!child.valid()) {
                    continue;
                }
                path.push_back(i);
                collect_observer_subtree_registrations(
                    cursor->fields()[i].ts_type,
                    child,
                    path,
                    true,
                    on_link_observer,
                    on_ref_observer);
                path.pop_back();
            }
            return;

        case TSKind::TSL:
        case TSKind::TSD: {
            if (tuple.size() < 2) {
                return;
            }
            View children = tuple.at(1);
            if (!children.valid() || !children.is_list()) {
                return;
            }
            auto list = children.as_list();
            for (size_t i = 0; i < list.size(); ++i) {
                View child = list.at(i);
                if (!child.valid()) {
                    continue;
                }
                path.push_back(i);
                collect_observer_subtree_registrations(
                    cursor->element_ts(),
                    child,
                    path,
                    true,
                    on_link_observer,
                    on_ref_observer);
                path.pop_back();
            }
            return;
        }
        default:
            return;
    }
}

template <typename LinkFn, typename RefFn>
void collect_related_observers(
    const ViewData& root_view,
    const std::vector<size_t>& target_path,
    LinkFn& on_link_observer,
    RefFn& on_ref_observer) {
    auto* observer_root = static_cast<const Value*>(root_view.observer_data);
    if (observer_root == nullptr || !observer_root->has_value() || root_view.meta == nullptr) {
        return;
    }

    const View root_node = observer_root->view();
    const TSMeta* cursor_meta = root_view.meta;
    View cursor_node = root_node;
    std::vector<size_t> cursor_path;
    cursor_path.reserve(target_path.size());

    // Collect ancestor registrations (including target path).
    append_observer_node_registrations(
        cursor_node,
        cursor_path,
        on_link_observer,
        on_ref_observer);
    bool reached_target = true;
    for (size_t index : target_path) {
        if (!advance_observer_subtree_node(cursor_meta, cursor_node, index)) {
            reached_target = false;
            break;
        }
        cursor_path.push_back(index);
        append_observer_node_registrations(
            cursor_node,
            cursor_path,
            on_link_observer,
            on_ref_observer);
    }
    if (!reached_target) {
        return;
    }

    // Collect strict descendants of target_path.
    collect_observer_subtree_registrations(
        cursor_meta,
        cursor_node,
        cursor_path,
        false,
        on_link_observer,
        on_ref_observer);
}

}  // namespace

namespace {

bool should_skip_projection_observer_notification(
    const LinkTarget& observer,
    const ViewData& target_view,
    const std::vector<size_t>& observer_path,
    engine_time_t current_time,
    bool debug_notify) {
    if (observer.projection == ViewProjection::NONE) {
        return false;
    }

    ViewData projected = target_view;
    projected.path.indices = observer_path;
    projected.projection = observer.projection;
    projected.sampled = false;
    if (projected.ops == nullptr || !projected.ops->modified(projected, current_time)) {
        if (debug_notify) {
            std::fprintf(stderr, "[notify_obs]   skip projection unmodified\n");
        }
        return true;
    }
    return false;
}

bool should_skip_ancestor_to_descendant_notification(
    const ViewData& target_view,
    const std::vector<size_t>& observer_path,
    engine_time_t current_time,
    bool debug_notify) {
    const bool ancestor_to_descendant =
        is_prefix_path(target_view.path.indices, observer_path) &&
        target_view.path.indices.size() < observer_path.size();
    if (!ancestor_to_descendant) {
        return false;
    }

    ViewData observed = target_view;
    observed.path.indices = observer_path;
    observed.sampled = false;
    if (observed.ops == nullptr) {
        return true;
    }

    const TSMeta* target_meta = meta_at_path(target_view.meta, target_view.path.indices);
    const bool target_is_ref = target_meta != nullptr && target_meta->kind == TSKind::REF;
    const TSMeta* observed_meta = meta_at_path(observed.meta, observed.path.indices);
    const bool observed_is_ref = observed_meta != nullptr && observed_meta->kind == TSKind::REF;
    const bool is_valid = observed.ops->valid(observed);
    const bool is_modified = observed.ops->modified(observed, current_time);
    if (!target_is_ref && is_valid && !is_modified) {
        if (debug_notify) {
            std::fprintf(stderr,
                         "[notify_obs]   skip ancestor->desc valid=%d mod=%d observed_is_ref=%d target_is_ref=%d path=%s\n",
                         is_valid ? 1 : 0,
                         is_modified ? 1 : 0,
                         observed_is_ref ? 1 : 0,
                         target_is_ref ? 1 : 0,
                         observed.path.to_string().c_str());
        }
        return true;
    }
    return false;
}

bool should_skip_descendant_to_ancestor_notification(
    const LinkTarget& observer,
    const ViewData& target_view,
    const std::vector<size_t>& observer_path,
    engine_time_t current_time,
    bool debug_notify) {
    const bool descendant_to_ancestor =
        is_prefix_path(observer_path, target_view.path.indices) &&
        observer_path.size() < target_view.path.indices.size();
    if (!descendant_to_ancestor) {
        return false;
    }

    if (observer.notify_on_ref_wrapper_write) {
        ViewData observer_view = observer.as_view_data(false);
        observer_view.sampled = false;
        if (observer_view.ops != nullptr &&
            !observer_view.ops->modified(observer_view, current_time)) {
            if (debug_notify) {
                std::fprintf(stderr,
                             "[notify_obs]   skip descendant->ancestor observer-unmodified path=%s\n",
                             observer_view.path.to_string().c_str());
            }
            return true;
        }
    }

    ViewData observed = target_view;
    observed.path.indices = observer_path;
    observed.sampled = false;

    // Do not bubble descendant writes to ancestor observers when the ancestor
    // view itself is not modified on this tick.
    if (observed.ops != nullptr &&
        !observed.ops->modified(observed, current_time)) {
        if (debug_notify) {
            std::fprintf(stderr, "[notify_obs]   skip descendant->ancestor unmodified\n");
        }
        return true;
    }
    return false;
}

void maybe_enqueue_link_observer(
    const ViewData& target_view,
    const std::vector<size_t>& observer_path,
    LinkTarget* observer,
    engine_time_t current_time,
    bool debug_notify,
    std::unordered_set<LinkTarget*>& dedupe,
    std::vector<LinkTarget*>& observers) {
    if (observer == nullptr || !is_live_link_target(observer)) {
        return;
    }
    if (debug_notify) {
        std::fprintf(stderr,
                     "[notify_obs]  reg path=%s linked=%d obs=%p\n",
                     ShortPath{target_view.path.node, target_view.path.port_type, observer_path}.to_string().c_str(),
                     observer->is_linked ? 1 : 0,
                     static_cast<void*>(observer));
    }
    if (!paths_related(observer_path, target_view.path.indices)) {
        if (debug_notify) {
            std::fprintf(stderr, "[notify_obs]   skip unrelated\n");
        }
        return;
    }
    if (should_skip_projection_observer_notification(*observer, target_view, observer_path, current_time, debug_notify)) {
        return;
    }
    if (should_skip_ancestor_to_descendant_notification(target_view, observer_path, current_time, debug_notify)) {
        return;
    }
    if (should_skip_descendant_to_ancestor_notification(*observer, target_view, observer_path, current_time, debug_notify)) {
        return;
    }
    if (dedupe.insert(observer).second) {
        observers.push_back(observer);
        if (debug_notify) {
            std::fprintf(stderr, "[notify_obs]   enqueue observer\n");
        }
    }
}

void maybe_enqueue_ref_link_observer(
    const ViewData& target_view,
    const std::vector<size_t>& observer_path,
    REFLink* observer,
    engine_time_t current_time,
    std::unordered_set<REFLink*>& dedupe,
    std::vector<REFLink*>& ref_observers) {
    if (observer == nullptr || !is_live_ref_link(observer)) {
        return;
    }

    if (!paths_related(observer_path, target_view.path.indices)) {
        return;
    }
    const bool ancestor_to_descendant =
        is_prefix_path(target_view.path.indices, observer_path) &&
        target_view.path.indices.size() < observer_path.size();
    if (ancestor_to_descendant) {
        ViewData observed = target_view;
        observed.path.indices = observer_path;
        observed.sampled = false;
        if (observed.ops == nullptr) {
            return;
        }
        const TSMeta* target_meta = meta_at_path(target_view.meta, target_view.path.indices);
        const bool target_is_ref = target_meta != nullptr && target_meta->kind == TSKind::REF;
        const bool is_valid = observed.ops->valid(observed);
        const bool is_modified = observed.ops->modified(observed, current_time);
        if (!target_is_ref && is_valid && !is_modified) {
            return;
        }
    }
    if (dedupe.insert(observer).second) {
        ref_observers.push_back(observer);
    }
}

bool signal_ref_observer_modified_for_wrapper_write(
    LinkTarget& observer,
    engine_time_t current_time) {
    bool observer_modified = true;
    if (auto* active_input = notifier_as_live_input(observer.active_notifier.target());
        active_input != nullptr && active_input->meta() != nullptr) {
        TSView input_root = active_input->view();
        if (input_root) {
            std::vector<size_t> observer_path;
            if (find_link_target_path(input_root.view_data(), active_input->meta(), &observer, observer_path)) {
                ViewData observer_input = input_root.view_data();
                observer_input.path.indices = observer_path;
                observer_input.sampled = false;
                if (observer_input.ops != nullptr) {
                    observer_modified = observer_input.ops->modified(observer_input, current_time);
                }
            }
        }
    }
    return observer_modified;
}

bool should_skip_ref_to_nonref_dynamic_target_write(
    const LinkTarget& observer,
    const ViewData& target_view,
    const TSMeta* target_meta,
    bool target_is_ref_wrapper,
    engine_time_t current_time,
    bool debug_notify) {
    if (!(observer.observer_ref_to_nonref_target &&
          observer.has_resolved_target &&
          !target_is_ref_wrapper)) {
        return false;
    }

    const bool target_is_dynamic_container =
        target_meta != nullptr &&
        (target_meta->kind == TSKind::TSS || target_meta->kind == TSKind::TSD);
    if (!target_is_dynamic_container) {
        return false;
    }

    const bool observer_rebind_tick = observer.last_rebind_time == current_time;
    if (observer_rebind_tick) {
        return false;
    }

    if (debug_notify) {
        ViewData observer_view_dbg = observer.as_view_data(false);
        std::string resolved_path{"<none>"};
        if (observer.has_resolved_target) {
            resolved_path = observer.resolved_target.path.to_string();
        }
        std::fprintf(stderr,
                     "[notify_obs]  skip ref->nonref target write obs=%p obs_path=%s has_resolved=%d resolved=%s target=%s\n",
                     static_cast<const void*>(&observer),
                     observer_view_dbg.path.to_string().c_str(),
                     observer.has_resolved_target ? 1 : 0,
                     resolved_path.c_str(),
                     target_view.path.to_string().c_str());
    }
    return true;
}

bool should_skip_signal_ref_to_nonref_target_write(
    const LinkTarget& observer,
    const ViewData& target_view,
    bool target_is_ref_wrapper,
    engine_time_t current_time,
    bool debug_notify) {
    if (!(observer.observer_is_signal &&
          observer.meta != nullptr &&
          observer.meta->kind == TSKind::REF &&
          !target_is_ref_wrapper)) {
        return false;
    }

    const bool from_resolved_target =
        observer.has_resolved_target &&
        same_or_descendant_view(observer.resolved_target, target_view);
    const bool observer_rebind_tick = observer.last_rebind_time == current_time;
    if (from_resolved_target || observer_rebind_tick) {
        return false;
    }

    if (debug_notify) {
        std::fprintf(stderr,
                     "[notify_obs]  skip signal ref->nonref target write obs=%p\n",
                     static_cast<const void*>(&observer));
    }
    return true;
}

bool should_skip_signal_wrapper_write_for_unmodified_observer(
    const LinkTarget& observer,
    const ViewData& target_view,
    bool target_is_ref_wrapper,
    engine_time_t current_time,
    bool debug_notify) {
    if (!(target_is_ref_wrapper && observer.observer_is_signal)) {
        return false;
    }

    const bool source_rebind_tick = rebind_time_for_view(target_view) == current_time;
    const bool observer_rebind_tick = observer.last_rebind_time == current_time;
    const bool wrapper_write_tick = direct_last_modified_time(target_view) == current_time;
    if (source_rebind_tick || observer_rebind_tick || wrapper_write_tick) {
        return false;
    }

    if (debug_notify) {
        std::fprintf(stderr,
                     "[notify_obs]  skip signal wrapper write for unmodified observer obs=%p\n",
                     static_cast<const void*>(&observer));
    }
    return true;
}

bool should_skip_signal_ref_wrapper_write_for_unmodified_observer(
    LinkTarget& observer,
    bool target_is_ref_wrapper,
    const TSMeta* observer_meta,
    engine_time_t current_time,
    bool debug_notify) {
    if (!(target_is_ref_wrapper &&
          observer.observer_is_signal &&
          observer_meta != nullptr &&
          observer_meta->kind == TSKind::REF)) {
        return false;
    }

    if (signal_ref_observer_modified_for_wrapper_write(observer, current_time)) {
        return false;
    }

    if (debug_notify) {
        std::fprintf(stderr,
                     "[notify_obs]  skip ref-wrapper write for signal/ref observer obs=%p (observer unmodified)\n",
                     static_cast<void*>(&observer));
    }
    return true;
}

bool should_skip_ref_wrapper_write_for_nonref_observer(
    const LinkTarget& observer,
    const ViewData& target_view,
    bool target_is_ref_wrapper,
    engine_time_t current_time,
    bool debug_notify) {
    if (observer.notify_on_ref_wrapper_write) {
        return false;
    }

    const bool notify_from_resolved_target =
        observer.has_resolved_target &&
        same_or_descendant_view(observer.resolved_target, target_view);
    const bool source_rebind_tick = rebind_time_for_view(target_view) == current_time;
    const bool observer_rebind_tick = observer.last_rebind_time == current_time;
    const bool rebind_tick = source_rebind_tick || observer_rebind_tick;
    if (!(target_is_ref_wrapper &&
          observer.has_resolved_target &&
          !notify_from_resolved_target &&
          !rebind_tick)) {
        return false;
    }

    if (debug_notify) {
        std::fprintf(stderr,
                     "[notify_obs]  skip ref-wrapper write for non-ref observer obs=%p\n",
                     static_cast<const void*>(&observer));
    }
    return true;
}

void prime_static_ref_child_rebind_time(
    LinkTarget& observer,
    engine_time_t current_time) {
    // Static REF child links (for example REF[TSB] field binds) use child
    // notifications for initial activation, then should remain quiescent unless
    // rebind occurs.
    if (observer.owner_time_ptr == nullptr &&
        observer_under_static_ref_container(observer) &&
        observer.last_rebind_time == MIN_DT) {
        observer.last_rebind_time = current_time;
    }
}

void debug_log_pre_notify_observer(
    const LinkTarget& observer,
    const ViewData& observer_view) {
    const TSMeta* observer_meta = meta_at_path(observer_view.meta, observer_view.path.indices);
    const auto* active_input = notifier_as_live_input(observer.active_notifier.target());
    std::string active_path{"<none>"};
    int active_kind = -1;
    if (active_input != nullptr) {
        active_path = active_input->root_path().to_string();
        active_kind = active_input->meta() != nullptr ? static_cast<int>(active_input->meta()->kind) : -1;
    }
    std::fprintf(stderr,
                 "[notify_obs]  pre-notify obs=%p obs_path=%s obs_kind=%d active_notifier=%p active_path=%s active_kind=%d parent=%p owner_time_ptr=%p notify_on_ref_wrapper_write=%d has_resolved=%d rebind=%lld\n",
                 static_cast<const void*>(&observer),
                 observer_view.path.to_string().c_str(),
                 observer_meta != nullptr ? static_cast<int>(observer_meta->kind) : -1,
                 static_cast<void*>(observer.active_notifier.target()),
                 active_path.c_str(),
                 active_kind,
                 static_cast<void*>(observer.parent_link),
                 static_cast<void*>(observer.owner_time_ptr),
                 observer.notify_on_ref_wrapper_write ? 1 : 0,
                 observer.has_resolved_target ? 1 : 0,
                 static_cast<long long>(observer.last_rebind_time.time_since_epoch().count()));
}

void notify_link_observer(LinkTarget& observer, engine_time_t current_time, bool debug_notify) {
    observer.notify(current_time);
    if (observer.active_notifier.active()) {
        observer.notify_active(current_time);
    }
    if (debug_notify) {
        std::fprintf(stderr, "[notify_obs]  notified obs=%p\n", static_cast<void*>(&observer));
    }
}

void dispatch_link_observers(
    const ViewData& target_view,
    engine_time_t current_time,
    bool debug_notify,
    const std::vector<LinkTarget*>& observers) {
    const TSMeta* target_meta = meta_at_path(target_view.meta, target_view.path.indices);
    const bool target_is_ref_wrapper = target_meta != nullptr && target_meta->kind == TSKind::REF;

    for (LinkTarget* observer : observers) {
        if (observer == nullptr || !is_live_link_target(observer)) {
            continue;
        }
        if (!observer->is_linked) {
            continue;
        }

        // Keep REF-resolved bridge state up to date on source writes so
        // consumers can observe bind/unbind transitions in the same tick.
        refresh_dynamic_ref_binding_for_link_target(observer, false, current_time);

        if (should_skip_ref_to_nonref_dynamic_target_write(
                *observer, target_view, target_meta, target_is_ref_wrapper, current_time, debug_notify)) {
            continue;
        }
        if (should_skip_signal_ref_to_nonref_target_write(
                *observer, target_view, target_is_ref_wrapper, current_time, debug_notify)) {
            continue;
        }
        if (should_skip_signal_wrapper_write_for_unmodified_observer(
                *observer, target_view, target_is_ref_wrapper, current_time, debug_notify)) {
            continue;
        }

        ViewData observer_view = observer->as_view_data(false);
        observer_view.sampled = false;
        const TSMeta* observer_meta = meta_at_path(observer_view.meta, observer_view.path.indices);
        if (should_skip_signal_ref_wrapper_write_for_unmodified_observer(
                *observer, target_is_ref_wrapper, observer_meta, current_time, debug_notify)) {
            continue;
        }
        if (should_skip_ref_wrapper_write_for_nonref_observer(
                *observer, target_view, target_is_ref_wrapper, current_time, debug_notify)) {
            continue;
        }

        prime_static_ref_child_rebind_time(*observer, current_time);
        if (suppress_static_ref_child_notification(*observer, current_time)) {
            if (debug_notify) {
                std::fprintf(stderr, "[notify_obs]  suppress static-ref child notify obs=%p\n", static_cast<void*>(observer));
            }
            continue;
        }

        if (debug_notify) {
            debug_log_pre_notify_observer(*observer, observer_view);
        }
        notify_link_observer(*observer, current_time, debug_notify);
    }
}

void dispatch_ref_link_observers(
    engine_time_t current_time,
    const std::vector<REFLink*>& ref_observers) {
    for (REFLink* observer : ref_observers) {
        if (observer != nullptr && is_live_ref_link(observer) && observer->is_linked) {
            observer->notify_time(current_time);
            if (observer->active_notifier.active()) {
                observer->notify_active(current_time);
            }
        }
    }
}

}  // namespace

void notify_link_target_observers(const ViewData& target_view, engine_time_t current_time) {
    if (target_view.meta == nullptr || target_view.observer_data == nullptr) {
        return;
    }
    const bool debug_notify = std::getenv("HGRAPH_DEBUG_NOTIFY_OBS") != nullptr;
    if (debug_notify) {
        std::fprintf(stderr,
                     "[notify_obs] target=%s value_data=%p now=%lld\n",
                     target_view.path.to_string().c_str(),
                     target_view.value_data,
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }

    std::vector<LinkTarget*> observers;
    std::unordered_set<LinkTarget*> link_dedupe;
    std::vector<REFLink*> ref_observers;
    std::unordered_set<REFLink*> ref_dedupe;

    auto on_link_observer = [&](const std::vector<size_t>& observer_path, LinkTarget* observer) {
        maybe_enqueue_link_observer(
            target_view,
            observer_path,
            observer,
            current_time,
            debug_notify,
            link_dedupe,
            observers);
    };
    auto on_ref_observer = [&](const std::vector<size_t>& observer_path, REFLink* observer) {
        maybe_enqueue_ref_link_observer(
            target_view,
            observer_path,
            observer,
            current_time,
            ref_dedupe,
            ref_observers);
    };
    collect_related_observers(target_view, target_view.path.indices, on_link_observer, on_ref_observer);

    dispatch_link_observers(target_view, current_time, debug_notify, observers);

    dispatch_ref_link_observers(current_time, ref_observers);
}



}  // namespace hgraph

