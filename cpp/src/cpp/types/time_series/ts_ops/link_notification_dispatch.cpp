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

const ObserverList* observer_list_from_node(View node);
bool find_link_target_path(const ViewData& root_view,
                           const TSMeta* root_meta,
                           const LinkTarget* needle,
                           std::vector<size_t>& out_path);

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
    while (dispatch_meta_is_ref(meta)) {
        meta = meta->element_ts();
    }
    if (meta == nullptr || !observer_node.valid() || !observer_node.is_tuple()) {
        return false;
    }

    auto tuple = observer_node.as_tuple();
    if (dispatch_meta_is_tsb(meta)) {
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

    if (dispatch_meta_is_tsl(meta) || dispatch_meta_is_tsd(meta)) {
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

    return false;
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
    while (dispatch_meta_is_ref(cursor)) {
        cursor = cursor->element_ts();
    }
    if (cursor == nullptr || !observer_node.valid() || !observer_node.is_tuple()) {
        return;
    }

    auto tuple = observer_node.as_tuple();
    if (dispatch_meta_is_tsb(cursor)) {
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
    }

    if (dispatch_meta_is_tsl(cursor) || dispatch_meta_is_tsd(cursor)) {
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
    const bool target_is_ref = dispatch_meta_is_ref(target_meta);
    const TSMeta* observed_meta = meta_at_path(observed.meta, observed.path.indices);
    const bool observed_is_ref = dispatch_meta_is_ref(observed_meta);
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
        const bool target_is_ref = dispatch_meta_is_ref(target_meta);
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
        dispatch_meta_is_tss(target_meta) || dispatch_meta_is_tsd(target_meta);
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
          dispatch_meta_is_ref(observer.meta) &&
          !target_is_ref_wrapper)) {
        return false;
    }

    // Until a resolved target is established, signal observers need direct
    // target writes to bootstrap modified propagation.
    if (!observer.has_resolved_target) {
        return false;
    }

    const bool from_resolved_target =
        same_or_descendant_view(observer.resolved_target, target_view) ||
        same_or_descendant_view(target_view, observer.resolved_target);
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
    const ViewData& target_view,
    bool target_is_ref_wrapper,
    const TSMeta* observer_meta,
    engine_time_t current_time,
    bool debug_notify) {
    if (!(target_is_ref_wrapper &&
          observer.observer_is_signal &&
          observer_meta != nullptr &&
          dispatch_meta_is_ref(observer_meta))) {
        return false;
    }

    if (!observer.has_resolved_target) {
        return false;
    }

    const bool notify_from_resolved_target =
        same_or_descendant_view(observer.resolved_target, target_view) ||
        same_or_descendant_view(target_view, observer.resolved_target);
    const bool source_rebind_tick = rebind_time_for_view(target_view) == current_time;
    const bool observer_rebind_tick = observer.last_rebind_time == current_time;
    if (notify_from_resolved_target || source_rebind_tick || observer_rebind_tick) {
        return false;
    }

    if (signal_ref_observer_modified_for_wrapper_write(observer, current_time)) {
        return false;
    }

    if (debug_notify) {
        std::fprintf(stderr,
                     "[notify_obs]  skip ref-wrapper write for signal/ref observer obs=%p (off-resolved unmodified)\n",
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
    const bool target_is_ref_wrapper = dispatch_meta_is_ref(target_meta);

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
                *observer, target_view, target_is_ref_wrapper, observer_meta, current_time, debug_notify)) {
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
