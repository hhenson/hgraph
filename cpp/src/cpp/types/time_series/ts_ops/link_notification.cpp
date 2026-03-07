#include "ts_ops_internal.h"
#include <cstdint>

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
    return dispatch_meta_is_tsb(meta);
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
        while (dispatch_meta_is_ref(cursor)) {
            cursor = cursor->element_ts();
        }
        if (cursor == nullptr) {
            return false;
        }

        if (dispatch_meta_is_tsb(cursor)) {
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
        }

        if (dispatch_meta_is_fixed_tsl(cursor)) {
            for (size_t i = 0; i < cursor->fixed_size(); ++i) {
                path.push_back(i);
                if (visit(cursor->element_ts())) {
                    return true;
                }
                path.pop_back();
            }
            return false;
        }

        return false;
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
    if (!dispatch_meta_is_ref(parent_meta)) {
        return false;
    }
    return is_static_container_meta(parent_meta->element_ts());
}

bool signal_input_has_bind_impl(const ViewData& vd, const TSMeta* current_meta, const LinkTarget* signal_link) {
    const bool debug_signal_impl = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_SIGNAL_IMPL");
    if (!dispatch_meta_is_signal(current_meta)) {
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

    const bool debug_activate = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_ACTIVATE");
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

    auto schema_ptr_looks_valid = [](const value::TypeMeta* schema) -> bool {
        if (schema == nullptr) {
            return false;
        }
        const auto addr = reinterpret_cast<std::uintptr_t>(schema);
        if (addr < 4096) {
            return false;
        }
        if ((addr % alignof(value::TypeMeta)) != 0) {
            return false;
        }
        return true;
    };

    ValueView slot = node;
    if (!slot.valid() || !schema_ptr_looks_valid(slot.schema())) {
        return nullptr;
    }

    if (slot.schema()->kind == value::TypeKind::Tuple) {
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

    if (!slot.valid() || !schema_ptr_looks_valid(slot.schema()) || slot.schema() != observer_list_type) {
        return nullptr;
    }
    return static_cast<ObserverList*>(slot.data());
}

const ObserverList* observer_list_from_node(View node) {
    const value::TypeMeta* observer_list_type = value::TypeRegistry::instance().get_by_name("ObserverList");
    if (observer_list_type == nullptr) {
        return nullptr;
    }

    auto schema_ptr_looks_valid = [](const value::TypeMeta* schema) -> bool {
        if (schema == nullptr) {
            return false;
        }
        const auto addr = reinterpret_cast<std::uintptr_t>(schema);
        if (addr < 4096) {
            return false;
        }
        if ((addr % alignof(value::TypeMeta)) != 0) {
            return false;
        }
        return true;
    };

    View slot = node;
    if (!slot.valid() || !schema_ptr_looks_valid(slot.schema())) {
        return nullptr;
    }

    if (slot.schema()->kind == value::TypeKind::Tuple) {
        auto tuple = slot.as_tuple();
        if (tuple.size() == 0) {
            return nullptr;
        }
        slot = tuple.at(0);
    }

    if (!slot.valid() || !schema_ptr_looks_valid(slot.schema()) || slot.schema() != observer_list_type) {
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
        while (dispatch_meta_is_ref(meta)) {
            meta = meta->element_ts();
        }
        if (meta == nullptr) {
            return false;
        }

        if (!current.valid() || !current.is_tuple()) {
            return false;
        }
        auto tuple = current.as_tuple();

        if (dispatch_meta_is_tsb(meta)) {
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
            continue;
        }

        if (dispatch_meta_is_tsl(meta) || dispatch_meta_is_tsd(meta)) {
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
            continue;
        }

        return false;
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
    const bool debug_keyset_bridge = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_KEYSET_BRIDGE");

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
    if (dispatch_meta_is_tsd(current)) {
        return true;
    }

    for (size_t index : view.path.indices) {
        while (dispatch_meta_is_ref(current)) {
            current = current->element_ts();
        }
        if (current == nullptr) {
            return false;
        }
        if (dispatch_meta_is_tsd(current)) {
            return true;
        }

        if (dispatch_meta_is_tsb(current)) {
            if (current->fields() == nullptr || index >= current->field_count()) {
                return false;
            }
            current = current->fields()[index].ts_type;
            continue;
        }

        if (dispatch_meta_is_tsl(current) || dispatch_meta_is_tsd(current)) {
            current = current->element_ts();
            continue;
        }

        return false;
    }

    return dispatch_meta_is_tsd(current);
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
}  // namespace hgraph
