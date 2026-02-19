#include <hgraph/types/time_series/ts_ops.h>

#include <hgraph/types/time_series/link_observer_registry.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/value/map_storage.h>
#include <hgraph/types/value/type_registry.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <optional>
#include <stdexcept>
#include <unordered_set>
#include <vector>

namespace hgraph {
namespace {

using value::View;
using value::Value;
using value::ValueView;

LinkTarget* resolve_link_target(const ViewData& vd, const std::vector<size_t>& ts_path);
bool resolve_read_view_data(const ViewData& vd, const TSMeta* self_meta, ViewData& out);
bool same_view_identity(const ViewData& lhs, const ViewData& rhs);
bool same_or_descendant_view(const ViewData& base, const ViewData& candidate);
engine_time_t direct_last_modified_time(const ViewData& vd);
engine_time_t rebind_time_for_view(const ViewData& vd);
const TSMeta* meta_at_path(const TSMeta* root, const std::vector<size_t>& indices);

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
    auto* active_input = dynamic_cast<TSInput*>(observer.active_notifier);
    if (active_input == nullptr || active_input->meta() == nullptr) {
        return false;
    }

    TSView root = active_input->view(MIN_DT);
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

    ViewData target_vd = payload->as_view_data(false);
    if (!payload->ops->valid(target_vd)) {
        if (debug_activate) {
            std::fprintf(stderr,
                         "[activate_lt] current=%lld target_valid=0 rebind=%lld\n",
                         static_cast<long long>(current_time.time_since_epoch().count()),
                         static_cast<long long>(payload->last_rebind_time.time_since_epoch().count()));
        }
        return;
    }
    const bool target_modified = payload->ops->modified(target_vd, current_time);
    const bool sampled_on_rebind = payload->last_rebind_time == current_time;
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
    payload->notify(sampled_on_rebind ? current_time : payload->ops->last_modified_time(target_vd));
}

void notify_activation_if_modified(REFLink* payload, TSInput* input) {
    if (payload == nullptr || input == nullptr || !payload->valid()) {
        return;
    }

    const engine_time_t current_time = resolve_input_current_time(input);
    if (current_time == MIN_DT) {
        return;
    }
    // REF inputs activate when the reference is valid, even if the bound target
    // did not tick on this cycle.
    if (!payload->modified(current_time) && !payload->valid()) {
        return;
    }
    payload->notify(current_time);
}

void unregister_link_target_observer_from_registry(const LinkTarget& link_target, TSLinkObserverRegistry* registry) {
    if (registry == nullptr || registry->entries.empty()) {
        return;
    }

    for (auto it = registry->entries.begin(); it != registry->entries.end();) {
        auto& registrations = it->second;
        registrations.erase(
            std::remove_if(
                registrations.begin(),
                registrations.end(),
                [&link_target](const LinkObserverRegistration& registration) {
                    return registration.link_target == &link_target;
                }),
            registrations.end());

        if (registrations.empty()) {
            it = registry->entries.erase(it);
        } else {
            ++it;
        }
    }
}

void unregister_ref_link_observer_from_registry(const REFLink& ref_link, TSLinkObserverRegistry* registry) {
    if (registry == nullptr || registry->ref_entries.empty()) {
        return;
    }

    for (auto it = registry->ref_entries.begin(); it != registry->ref_entries.end();) {
        auto& registrations = it->second;
        registrations.erase(
            std::remove_if(
                registrations.begin(),
                registrations.end(),
                [&ref_link](const REFLinkObserverRegistration& registration) {
                    return registration.ref_link == &ref_link;
                }),
            registrations.end());

        if (registrations.empty()) {
            it = registry->ref_entries.erase(it);
        } else {
            ++it;
        }
    }
}

void unregister_link_target_observer(const LinkTarget& link_target) {
    TSLinkObserverRegistry* direct_registry = link_target.link_observer_registry;
    TSLinkObserverRegistry* resolved_registry = link_target.has_resolved_target ? link_target.resolved_target.link_observer_registry : nullptr;

    unregister_link_target_observer_from_registry(link_target, direct_registry);
    if (resolved_registry != direct_registry) {
        unregister_link_target_observer_from_registry(link_target, resolved_registry);
    }
}

void unregister_ref_link_observer(const REFLink& ref_link) {
    TSLinkObserverRegistry* source_registry =
        ref_link.source.meta != nullptr ? ref_link.source.link_observer_registry : nullptr;
    TSLinkObserverRegistry* target_registry =
        ref_link.target.meta != nullptr ? ref_link.target.link_observer_registry : nullptr;

    unregister_ref_link_observer_from_registry(ref_link, source_registry);
    if (target_registry != source_registry) {
        unregister_ref_link_observer_from_registry(ref_link, target_registry);
    }
}

void register_link_target_observer_entry(TSLinkObserverRegistry* registry,
                                         void* value_data,
                                         const std::vector<size_t>& path,
                                         LinkTarget* target) {
    if (registry == nullptr || value_data == nullptr || target == nullptr) {
        return;
    }

    auto& registrations = registry->entries[value_data];

    const auto existing = std::find_if(
        registrations.begin(),
        registrations.end(),
        [target](const LinkObserverRegistration& registration) {
            return registration.link_target == target;
        });

    if (existing != registrations.end()) {
        existing->path = path;
        return;
    }

    registrations.push_back(LinkObserverRegistration{path, target});
}

void register_ref_link_observer_entry(TSLinkObserverRegistry* registry,
                                      void* value_data,
                                      const std::vector<size_t>& path,
                                      REFLink* target) {
    if (registry == nullptr || value_data == nullptr || target == nullptr) {
        return;
    }

    auto& registrations = registry->ref_entries[value_data];

    const auto existing = std::find_if(
        registrations.begin(),
        registrations.end(),
        [target](const REFLinkObserverRegistration& registration) {
            return registration.ref_link == target;
        });

    if (existing != registrations.end()) {
        existing->path = path;
        return;
    }

    registrations.push_back(REFLinkObserverRegistration{path, target});
}

void register_link_target_observer(const LinkTarget& link_target) {
    if (!link_target.is_linked) {
        return;
    }

    auto* mutable_target = const_cast<LinkTarget*>(&link_target);
    auto register_target_view = [mutable_target](const ViewData& target_view) {
        register_link_target_observer_entry(
            target_view.link_observer_registry,
            target_view.value_data,
            target_view.path.indices,
            mutable_target);
    };

    register_target_view(link_target.as_view_data(false));
    for (const ViewData& extra_target : link_target.fan_in_targets) {
        register_target_view(extra_target);
    }

    if (link_target.has_resolved_target && link_target.resolved_target.value_data != nullptr) {
        register_target_view(link_target.resolved_target);
    }
}

void refresh_dynamic_ref_binding_for_link_target(LinkTarget* link_target, bool sampled, engine_time_t current_time) {
    if (link_target == nullptr || !link_target->is_linked) {
        return;
    }
    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;

    ViewData source_view = link_target->as_view_data(sampled);
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

            engine_time_t stamp = current_time != MIN_DT ? current_time : direct_last_modified_time(source_view);
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

        engine_time_t stamp = current_time != MIN_DT ? current_time : direct_last_modified_time(source_view);
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

    engine_time_t stamp = current_time != MIN_DT ? current_time : direct_last_modified_time(source_view);
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

void register_ref_link_observer(const REFLink& ref_link) {
    if (!ref_link.is_linked) {
        return;
    }

    auto* mutable_target = const_cast<REFLink*>(&ref_link);
    const ViewData& target_view = ref_link.has_target() ? ref_link.target : ref_link.source;
    register_ref_link_observer_entry(
        target_view.link_observer_registry,
        target_view.value_data,
        target_view.path.indices,
        mutable_target);
}

bool suppress_static_ref_child_notification(const LinkTarget& observer, engine_time_t current_time) {
    if (observer.parent_link == nullptr) {
        return false;
    }
    if (observer.parent_link->active_notifier != nullptr) {
        return false;
    }
    if (observer.active_notifier == nullptr) {
        return false;
    }
    if (observer.owner_time_ptr != nullptr) {
        return false;
    }
    if (observer.last_rebind_time == current_time) {
        return false;
    }
    if (observer.meta != nullptr && observer.meta->kind == TSKind::REF) {
        return false;
    }
    if (!observer_under_static_ref_container(observer)) {
        return false;
    }
    return true;
}

void notify_link_target_observers(const ViewData& target_view, engine_time_t current_time) {
    if (target_view.value_data == nullptr || target_view.link_observer_registry == nullptr) {
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
    auto it = target_view.link_observer_registry->entries.find(target_view.value_data);

    std::unordered_set<LinkTarget*> dedupe;
    if (it != target_view.link_observer_registry->entries.end()) {
        for (const LinkObserverRegistration& registration : it->second) {
            if (registration.link_target == nullptr) {
                continue;
            }
            if (debug_notify) {
                std::fprintf(stderr,
                             "[notify_obs]  reg path=%s linked=%d obs=%p\n",
                             ShortPath{target_view.path.node, target_view.path.port_type, registration.path}.to_string().c_str(),
                             registration.link_target->is_linked ? 1 : 0,
                             static_cast<void*>(registration.link_target));
            }
            if (!paths_related(registration.path, target_view.path.indices)) {
                if (debug_notify) {
                    std::fprintf(stderr, "[notify_obs]   skip unrelated\n");
                }
                continue;
            }
            // Projection observers (for example, TSD key_set viewed as SIGNAL)
            // must only tick when that projection is modified, not on every
            // write to the backing root path.
            if (registration.link_target->projection != ViewProjection::NONE) {
                ViewData projected = target_view;
                projected.path.indices = registration.path;
                projected.projection = registration.link_target->projection;
                projected.sampled = false;
                if (projected.ops == nullptr || !projected.ops->modified(projected, current_time)) {
                    if (debug_notify) {
                        std::fprintf(stderr, "[notify_obs]   skip projection unmodified\n");
                    }
                    continue;
                }
            }
            // When notifying from an ancestor path (e.g. TSD root), suppress
            // descendant notifications unless the descendant actually changed or
            // became invalid at this tick. This avoids falsely ticking unrelated
            // keyed bindings on root-level container writes.
            const bool ancestor_to_descendant =
                is_prefix_path(target_view.path.indices, registration.path) &&
                target_view.path.indices.size() < registration.path.size();
            if (ancestor_to_descendant) {
                ViewData observed = target_view;
                observed.path.indices = registration.path;
                observed.sampled = false;
                if (observed.ops == nullptr) {
                    continue;
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
                    continue;
                }
            }
            const bool descendant_to_ancestor =
                is_prefix_path(registration.path, target_view.path.indices) &&
                registration.path.size() < target_view.path.indices.size();
            if (descendant_to_ancestor) {
                if (registration.link_target->notify_on_ref_wrapper_write) {
                    ViewData observer_view = registration.link_target->as_view_data(false);
                    observer_view.sampled = false;
                    if (observer_view.ops != nullptr &&
                        !observer_view.ops->modified(observer_view, current_time)) {
                        if (debug_notify) {
                            std::fprintf(stderr,
                                         "[notify_obs]   skip descendant->ancestor observer-unmodified path=%s\n",
                                         observer_view.path.to_string().c_str());
                        }
                        continue;
                    }
                }

                ViewData observed = target_view;
                observed.path.indices = registration.path;
                observed.sampled = false;

                // Do not bubble descendant writes to ancestor observers when the
                // ancestor view itself is not modified on this tick. This is
                // important for REF[TSD]-style wrappers which should not tick on
                // unrelated key updates.
                if (observed.ops != nullptr && !observed.ops->modified(observed, current_time)) {
                    if (debug_notify) {
                        std::fprintf(stderr, "[notify_obs]   skip descendant->ancestor unmodified\n");
                    }
                    continue;
                }

                if (registration.link_target->active_notifier != nullptr &&
                    view_path_contains_tsd_ancestor(target_view)) {
                    auto* active_input = dynamic_cast<TSInput*>(registration.link_target->active_notifier);
                    if (active_input != nullptr &&
                        active_input->meta() != nullptr &&
                        active_input->meta()->kind == TSKind::SIGNAL &&
                        active_input->owning_node() != nullptr &&
                        active_input->owning_node()->signature().active_inputs.has_value()) {
                        if (debug_notify) {
                            std::fprintf(stderr, "[notify_obs]   skip signal descendant->ancestor suppression\n");
                        }
                        continue;
                    }
                }
            }
            if (dedupe.insert(registration.link_target).second) {
                observers.push_back(registration.link_target);
                if (debug_notify) {
                    std::fprintf(stderr, "[notify_obs]   enqueue observer\n");
                }
            }
        }
    }

    for (LinkTarget* observer : observers) {
        if (observer != nullptr && observer->is_linked) {
            // Keep REF-resolved bridge state up to date on source writes so
            // consumers can observe bind/unbind transitions in the same tick.
            refresh_dynamic_ref_binding_for_link_target(observer, false, current_time);
            if (!observer->notify_on_ref_wrapper_write) {
                const TSMeta* target_meta = meta_at_path(target_view.meta, target_view.path.indices);
                const bool target_is_ref_wrapper = target_meta != nullptr && target_meta->kind == TSKind::REF;
                const bool notify_from_resolved_target =
                    observer->has_resolved_target &&
                    same_or_descendant_view(observer->resolved_target, target_view);
                const bool source_rebind_tick = rebind_time_for_view(target_view) == current_time;
                const bool observer_rebind_tick = observer->last_rebind_time == current_time;
                const bool rebind_tick = source_rebind_tick || observer_rebind_tick;
                if (target_is_ref_wrapper && !notify_from_resolved_target && !rebind_tick) {
                    if (debug_notify) {
                        std::fprintf(stderr,
                                     "[notify_obs]  skip ref-wrapper write for non-ref observer obs=%p\n",
                                     static_cast<void*>(observer));
                    }
                    continue;
                }
            }
            if (suppress_static_ref_child_notification(*observer, current_time)) {
                if (debug_notify) {
                    std::fprintf(stderr, "[notify_obs]  suppress static-ref child notify obs=%p\n", static_cast<void*>(observer));
                }
                continue;
            }
            if (debug_notify) {
                ViewData observer_view = observer->as_view_data(false);
                const TSMeta* observer_meta = meta_at_path(observer_view.meta, observer_view.path.indices);
                const auto* active_input = dynamic_cast<TSInput*>(observer->active_notifier);
                std::string active_path{"<none>"};
                int active_kind = -1;
                if (active_input != nullptr) {
                    active_path = active_input->root_path().to_string();
                    active_kind = active_input->meta() != nullptr ? static_cast<int>(active_input->meta()->kind) : -1;
                }
                std::fprintf(stderr,
                             "[notify_obs]  pre-notify obs=%p obs_path=%s obs_kind=%d active_notifier=%p active_path=%s active_kind=%d parent=%p owner_time_ptr=%p notify_on_ref_wrapper_write=%d has_resolved=%d rebind=%lld\n",
                             static_cast<void*>(observer),
                             observer_view.path.to_string().c_str(),
                             observer_meta != nullptr ? static_cast<int>(observer_meta->kind) : -1,
                             static_cast<void*>(observer->active_notifier),
                             active_path.c_str(),
                             active_kind,
                             static_cast<void*>(observer->parent_link),
                             static_cast<void*>(observer->owner_time_ptr),
                             observer->notify_on_ref_wrapper_write ? 1 : 0,
                             observer->has_resolved_target ? 1 : 0,
                             static_cast<long long>(observer->last_rebind_time.time_since_epoch().count()));
            }
            observer->notify(current_time);
            if (debug_notify) {
                std::fprintf(stderr, "[notify_obs]  notified obs=%p\n", static_cast<void*>(observer));
            }
        }
    }

    std::vector<REFLink*> ref_observers;
    auto ref_it = target_view.link_observer_registry->ref_entries.find(target_view.value_data);
    if (ref_it == target_view.link_observer_registry->ref_entries.end()) {
        return;
    }

    std::unordered_set<REFLink*> ref_dedupe;
    for (const REFLinkObserverRegistration& registration : ref_it->second) {
        if (registration.ref_link == nullptr) {
            continue;
        }
        if (!paths_related(registration.path, target_view.path.indices)) {
            continue;
        }
        const bool ancestor_to_descendant =
            is_prefix_path(target_view.path.indices, registration.path) &&
            target_view.path.indices.size() < registration.path.size();
        if (ancestor_to_descendant) {
            ViewData observed = target_view;
            observed.path.indices = registration.path;
            observed.sampled = false;
            if (observed.ops == nullptr) {
                continue;
            }
            const TSMeta* target_meta = meta_at_path(target_view.meta, target_view.path.indices);
            const bool target_is_ref = target_meta != nullptr && target_meta->kind == TSKind::REF;
            const TSMeta* observed_meta = meta_at_path(observed.meta, observed.path.indices);
            const bool observed_is_ref = observed_meta != nullptr && observed_meta->kind == TSKind::REF;
            const bool is_valid = observed.ops->valid(observed);
            const bool is_modified = observed.ops->modified(observed, current_time);
            if (!target_is_ref && is_valid && !is_modified) {
                continue;
            }
        }
        if (ref_dedupe.insert(registration.ref_link).second) {
            ref_observers.push_back(registration.ref_link);
        }
    }

    for (REFLink* observer : ref_observers) {
        if (observer != nullptr && observer->is_linked) {
            observer->notify(current_time);
        }
    }
}

const TSMeta* meta_at_path(const TSMeta* root, const std::vector<size_t>& indices) {
    const TSMeta* meta = root;
    for (size_t index : indices) {
        while (meta != nullptr && meta->kind == TSKind::REF) {
            meta = meta->element_ts();
        }

        if (meta == nullptr) {
            return nullptr;
        }

        switch (meta->kind) {
            case TSKind::TSB:
                if (index >= meta->field_count() || meta->fields() == nullptr) {
                    return nullptr;
                }
                meta = meta->fields()[index].ts_type;
                break;
            case TSKind::TSL:
            case TSKind::TSD:
                meta = meta->element_ts();
                break;
            default:
                return nullptr;
        }
    }
    return meta;
}

size_t find_bundle_field_index(const TSMeta* bundle_meta, std::string_view field_name) {
    if (bundle_meta == nullptr || bundle_meta->kind != TSKind::TSB || bundle_meta->fields() == nullptr) {
        return static_cast<size_t>(-1);
    }

    for (size_t i = 0; i < bundle_meta->field_count(); ++i) {
        const char* name = bundle_meta->fields()[i].name;
        if (name != nullptr && field_name == name) {
            return i;
        }
    }
    return static_cast<size_t>(-1);
}

std::optional<View> navigate_const(View view, const std::vector<size_t>& indices) {
    View current = view;
    for (size_t index : indices) {
        if (!current.valid()) {
            return std::nullopt;
        }
        if (current.is_bundle()) {
            auto bundle = current.as_bundle();
            if (index >= bundle.size()) {
                return std::nullopt;
            }
            current = bundle.at(index);
        } else if (current.is_tuple()) {
            auto tuple = current.as_tuple();
            if (index >= tuple.size()) {
                return std::nullopt;
            }
            current = tuple.at(index);
        } else if (current.is_list()) {
            auto list = current.as_list();
            if (index >= list.size()) {
                return std::nullopt;
            }
            current = list.at(index);
        } else if (current.is_map()) {
            const auto map = current.as_map();
            const auto* storage = static_cast<const value::MapStorage*>(map.data());
            if (storage == nullptr || !storage->key_set().is_alive(index)) {
                return std::nullopt;
            }
            View key(storage->key_at_slot(index), map.key_type());
            current = map.at(key);
        } else {
            return std::nullopt;
        }
    }
    return current;
}

std::optional<ValueView> navigate_mut(ValueView view, const std::vector<size_t>& indices) {
    ValueView current = view;
    for (size_t index : indices) {
        if (!current.valid()) {
            return std::nullopt;
        }
        if (current.is_bundle()) {
            auto bundle = current.as_bundle();
            if (index >= bundle.size()) {
                return std::nullopt;
            }
            current = bundle.at(index);
        } else if (current.is_tuple()) {
            auto tuple = current.as_tuple();
            if (index >= tuple.size()) {
                return std::nullopt;
            }
            current = tuple.at(index);
        } else if (current.is_list()) {
            auto list = current.as_list();
            if (index >= list.size()) {
                return std::nullopt;
            }
            current = list.at(index);
        } else if (current.is_map()) {
            auto map = current.as_map();
            auto* storage = static_cast<value::MapStorage*>(map.data());
            if (storage == nullptr || !storage->key_set().is_alive(index)) {
                return std::nullopt;
            }
            View key(storage->key_at_slot(index), map.key_type());
            current = map.at(key);
        } else {
            return std::nullopt;
        }
    }
    return current;
}

void copy_view_data(ValueView dst, const View& src) {
    if (!dst.valid() || !src.valid()) {
        return;
    }
    if (dst.schema() != src.schema()) {
        throw std::runtime_error("TS scaffolding set_value schema mismatch");
    }
    dst.schema()->ops().copy(dst.data(), src.data(), dst.schema());
}

void clear_map_slot(value::ValueView map_view) {
    if (!map_view.valid() || !map_view.is_map()) {
        return;
    }

    auto map = map_view.as_map();
    if (map.size() == 0) {
        return;
    }

    std::vector<value::Value> keys;
    keys.reserve(map.size());
    for (View key : map.keys()) {
        keys.emplace_back(key.clone());
    }
    for (const auto& key : keys) {
        map.remove(key.view());
    }
}

struct TSSDeltaSlots {
    ValueView slot;
    ValueView added_set;
    ValueView removed_set;
};

struct TSDDeltaSlots {
    ValueView slot;
    ValueView changed_values_map;
    ValueView added_set;
    ValueView removed_set;
};

std::optional<std::vector<size_t>> ts_path_to_delta_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path);
const value::TypeMeta* ts_reference_meta();
std::optional<View> resolve_value_slot_const(const ViewData& vd);
std::optional<ViewData> resolve_bound_view_data(const ViewData& vd);
bool op_modified(const ViewData& vd, engine_time_t current_time);
bool op_valid(const ViewData& vd);
View op_value(const ViewData& vd);
void clear_tsd_delta_if_new_tick(ViewData& vd, engine_time_t current_time, TSDDeltaSlots slots);

std::optional<ValueView> resolve_delta_slot_mut(ViewData& vd) {
    auto* delta_root = static_cast<Value*>(vd.delta_data);
    if (delta_root == nullptr || delta_root->schema() == nullptr) {
        return std::nullopt;
    }
    if (!delta_root->has_value()) {
        delta_root->emplace();
    }
    auto delta_path = ts_path_to_delta_path(vd.meta, vd.path.indices);
    if (!delta_path.has_value()) {
        return std::nullopt;
    }
    if (delta_path->empty()) {
        return delta_root->view();
    }
    return navigate_mut(delta_root->view(), *delta_path);
}

TSSDeltaSlots resolve_tss_delta_slots(ViewData& vd) {
    TSSDeltaSlots out{};
    auto maybe_slot = resolve_delta_slot_mut(vd);
    if (!maybe_slot.has_value()) {
        return out;
    }

    out.slot = *maybe_slot;
    if (!out.slot.valid() || !out.slot.is_tuple()) {
        return out;
    }

    auto tuple = out.slot.as_tuple();
    if (tuple.size() > 0) {
        out.added_set = tuple.at(0);
    }
    if (tuple.size() > 1) {
        out.removed_set = tuple.at(1);
    }
    return out;
}

TSDDeltaSlots resolve_tsd_delta_slots(ViewData& vd) {
    TSDDeltaSlots out{};
    auto maybe_slot = resolve_delta_slot_mut(vd);
    if (!maybe_slot.has_value()) {
        return out;
    }

    out.slot = *maybe_slot;
    if (!out.slot.valid() || !out.slot.is_tuple()) {
        return out;
    }

    auto tuple = out.slot.as_tuple();
    if (tuple.size() > 0) {
        out.changed_values_map = tuple.at(0);
    }
    if (tuple.size() > 1) {
        out.added_set = tuple.at(1);
    }
    if (tuple.size() > 2) {
        out.removed_set = tuple.at(2);
    }
    return out;
}

bool set_view_empty(ValueView v) {
    return !v.valid() || !v.is_set() || v.as_set().size() == 0;
}

bool map_view_empty(ValueView v) {
    return !v.valid() || !v.is_map() || v.as_map().size() == 0;
}

bool is_scalar_like_ts_kind(TSKind kind) {
    return kind == TSKind::TSValue || kind == TSKind::REF || kind == TSKind::SIGNAL || kind == TSKind::TSW;
}

bool has_delta_descendants(const TSMeta* meta) {
    if (meta == nullptr) {
        return false;
    }

    switch (meta->kind) {
        case TSKind::TSS:
        case TSKind::TSD:
            return true;
        case TSKind::TSB:
            for (size_t i = 0; i < meta->field_count(); ++i) {
                if (has_delta_descendants(meta->fields()[i].ts_type)) {
                    return true;
                }
            }
            return false;
        case TSKind::TSL:
            return has_delta_descendants(meta->element_ts());
        default:
            return false;
    }
}

std::optional<std::vector<size_t>> ts_path_to_delta_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    if (root_meta == nullptr || !has_delta_descendants(root_meta)) {
        return std::nullopt;
    }

    std::vector<size_t> out;
    const TSMeta* current = root_meta;

    for (size_t index : ts_path) {
        while (current != nullptr && current->kind == TSKind::REF) {
            current = current->element_ts();
        }

        if (current == nullptr) {
            return std::nullopt;
        }

        switch (current->kind) {
            case TSKind::TSB: {
                if (current->fields() == nullptr || index >= current->field_count()) {
                    return std::nullopt;
                }
                const TSMeta* child = current->fields()[index].ts_type;
                if (!has_delta_descendants(child)) {
                    return std::nullopt;
                }
                out.push_back(index);
                current = child;
                break;
            }

            case TSKind::TSL: {
                const TSMeta* child = current->element_ts();
                if (!has_delta_descendants(child)) {
                    return std::nullopt;
                }
                out.push_back(index);
                current = child;
                break;
            }

            case TSKind::TSD: {
                const TSMeta* child = current->element_ts();
                if (!has_delta_descendants(child)) {
                    return std::nullopt;
                }
                out.push_back(3);
                out.push_back(index);
                current = child;
                break;
            }

            default:
                return std::nullopt;
        }
    }

    return out;
}

const value::MapStorage* map_storage_for_read(const value::MapView& map) {
    if (!map.valid() || !map.is_map()) {
        return nullptr;
    }
    return static_cast<const value::MapStorage*>(map.data());
}

template <typename Fn>
void for_each_map_key_slot(const value::MapView& map, Fn fn) {
    const auto* storage = map_storage_for_read(map);
    if (storage == nullptr) {
        return;
    }
    const value::TypeMeta* key_type = map.key_type();
    for (size_t slot : storage->key_set()) {
        fn(View(storage->key_at_slot(slot), key_type), slot);
    }
}

std::optional<size_t> map_slot_for_key(const value::MapView& map, const View& key) {
    if (!key.valid() || key.schema() != map.key_type()) {
        return std::nullopt;
    }
    const auto* storage = map_storage_for_read(map);
    if (storage == nullptr) {
        return std::nullopt;
    }
    const size_t slot = storage->key_set().find(key.data());
    if (slot == static_cast<size_t>(-1)) {
        return std::nullopt;
    }
    return slot;
}

std::optional<Value> map_key_at_slot(const value::MapView& map, size_t slot_index) {
    const auto* storage = map_storage_for_read(map);
    if (storage == nullptr || !storage->key_set().is_alive(slot_index)) {
        return std::nullopt;
    }
    View key(storage->key_at_slot(slot_index), map.key_type());
    return key.clone();
}

void mark_tsd_parent_child_modified(ViewData child_vd, engine_time_t current_time) {
    if (child_vd.path.indices.empty()) {
        return;
    }

    const std::vector<size_t> full_path = child_vd.path.indices;
    for (size_t depth = 0; depth < full_path.size(); ++depth) {
        std::vector<size_t> tsd_path(full_path.begin(), full_path.begin() + depth);
        const TSMeta* parent_meta = meta_at_path(child_vd.meta, tsd_path);
        if (parent_meta == nullptr || parent_meta->kind != TSKind::TSD) {
            continue;
        }

        ViewData tsd_vd = child_vd;
        tsd_vd.path.indices = std::move(tsd_path);
        const size_t child_slot = full_path[depth];

        auto maybe_parent_value = resolve_value_slot_const(tsd_vd);
        if (!maybe_parent_value.has_value() || !maybe_parent_value->valid() || !maybe_parent_value->is_map()) {
            continue;
        }

        auto parent_map = maybe_parent_value->as_map();
        auto maybe_key = map_key_at_slot(parent_map, child_slot);
        if (!maybe_key.has_value()) {
            continue;
        }
        const View key = maybe_key->view();

        ViewData tsd_child_vd = tsd_vd;
        tsd_child_vd.path.indices.push_back(child_slot);

        auto slots = resolve_tsd_delta_slots(tsd_vd);
        clear_tsd_delta_if_new_tick(tsd_vd, current_time, slots);

        if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
            View child_value = op_value(tsd_child_vd);
            if (child_value.valid()) {
                slots.changed_values_map.as_map().set(key, child_value);
            } else {
                slots.changed_values_map.as_map().remove(key);
            }
        }

        if (slots.removed_set.valid() && slots.removed_set.is_set()) {
            slots.removed_set.as_set().remove(key);
        }
    }
}

engine_time_t graph_start_time_for_view(const ViewData& vd) {
    node_ptr node = vd.path.node;
    if (node == nullptr) {
        return MIN_DT;
    }
    graph_ptr graph = node->graph();
    if (graph == nullptr) {
        return MIN_DT;
    }
    auto api = graph->evaluation_engine_api();
    return api ? api->start_time() : MIN_DT;
}

engine_time_t evaluation_time_for_view(const ViewData& vd) {
    node_ptr node = vd.path.node;
    if (node == nullptr) {
        return MIN_DT;
    }
    if (const engine_time_t* et = node->cached_evaluation_time_ptr(); et != nullptr && *et != MIN_DT) {
        return *et;
    }
    graph_ptr graph = node->graph();
    if (graph == nullptr) {
        return MIN_DT;
    }
    return graph->evaluation_time();
}

bool tss_delta_empty(const TSSDeltaSlots& slots) {
    return set_view_empty(slots.added_set) && set_view_empty(slots.removed_set);
}

bool tsd_delta_empty(const TSDDeltaSlots& slots) {
    return map_view_empty(slots.changed_values_map) && set_view_empty(slots.added_set) && set_view_empty(slots.removed_set);
}

engine_time_t op_last_modified_time(const ViewData& vd);
engine_time_t direct_last_modified_time(const ViewData& vd);
size_t op_list_size(const ViewData& vd);

void clear_tss_delta_slots(TSSDeltaSlots slots) {
    if (slots.added_set.valid() && slots.added_set.is_set()) {
        slots.added_set.as_set().clear();
    }
    if (slots.removed_set.valid() && slots.removed_set.is_set()) {
        slots.removed_set.as_set().clear();
    }
}

void clear_tsd_delta_slots(TSDDeltaSlots slots) {
    clear_map_slot(slots.changed_values_map);
    if (slots.added_set.valid() && slots.added_set.is_set()) {
        slots.added_set.as_set().clear();
    }
    if (slots.removed_set.valid() && slots.removed_set.is_set()) {
        slots.removed_set.as_set().clear();
    }
    if (slots.slot.valid() && slots.slot.is_tuple()) {
        auto tuple = slots.slot.as_tuple();
        if (tuple.size() > 3) {
            ValueView child_deltas = tuple.at(3);
            if (child_deltas.valid() && child_deltas.is_list()) {
                child_deltas.as_list().clear();
            }
        }
    }
}

void clear_tss_delta_if_new_tick(ViewData& vd, engine_time_t current_time, TSSDeltaSlots slots) {
    if (!slots.slot.valid()) {
        return;
    }
    if (direct_last_modified_time(vd) < current_time) {
        clear_tss_delta_slots(slots);
    }
}

void clear_tsd_delta_if_new_tick(ViewData& vd, engine_time_t current_time, TSDDeltaSlots slots) {
    if (!slots.slot.valid()) {
        return;
    }
    if (direct_last_modified_time(vd) < current_time) {
        clear_tsd_delta_slots(slots);
    }
}

std::optional<Value> value_from_python(const value::TypeMeta* type, const nb::object& src) {
    if (type == nullptr) {
        return std::nullopt;
    }
    Value out(type);
    out.emplace();
    type->ops().from_python(out.data(), src, type);
    return out;
}

nb::object attr_or_call(const nb::object& obj, const char* name) {
    nb::object attr = nb::getattr(obj, name, nb::none());
    if (attr.is_none()) {
        return nb::none();
    }
    if (nb::hasattr(attr, "__call__")) {
        return attr();
    }
    return attr;
}

nb::object python_set_delta(const nb::object& added, const nb::object& removed) {
    auto PythonSetDelta = nb::module_::import_("hgraph._impl._types._tss").attr("PythonSetDelta");
    return PythonSetDelta(added, removed);
}

std::vector<size_t> ts_path_to_link_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    std::vector<size_t> out;
    const TSMeta* meta = root_meta;
    bool crossed_dynamic_boundary = false;

    if (meta == nullptr) {
        return out;
    }

    if (ts_path.empty()) {
        if (meta->kind == TSKind::REF) {
            out.push_back(0);  // REF root link slot.
        } else if (meta->kind == TSKind::TSB || meta->kind == TSKind::TSL || meta->kind == TSKind::TSD) {
            out.push_back(0);  // container link slot.
        }
        return out;
    }

    for (size_t index : ts_path) {
        while (meta != nullptr && meta->kind == TSKind::REF) {
            out.push_back(1);  // descend into referred link tree.
            meta = meta->element_ts();
        }

        if (meta == nullptr) {
            break;
        }

        if (crossed_dynamic_boundary) {
            switch (meta->kind) {
                case TSKind::TSB:
                    if (meta->fields() == nullptr || index >= meta->field_count()) {
                        return out;
                    }
                    meta = meta->fields()[index].ts_type;
                    break;
                case TSKind::TSL:
                case TSKind::TSD:
                    meta = meta->element_ts();
                    break;
                default:
                    return out;
            }
            continue;
        }

        switch (meta->kind) {
            case TSKind::TSB:
                out.push_back(index + 1);  // slot 0 reserved for container link.
                if (index >= meta->field_count() || meta->fields() == nullptr) {
                    return out;
                }
                meta = meta->fields()[index].ts_type;
                break;
            case TSKind::TSL:
                if (meta->fixed_size() > 0) {
                    out.push_back(1);
                    out.push_back(index);
                }
                if (meta->fixed_size() == 0) {
                    crossed_dynamic_boundary = true;
                }
                meta = meta->element_ts();
                break;
            case TSKind::TSD:
                out.push_back(1);  // per-key child link list in slot 1.
                out.push_back(index);
                meta = meta->element_ts();
                break;
            default:
                return out;
        }
    }

    // TSB/fixed-TSL/REF nodes have a root link slot at 0.
    if (!crossed_dynamic_boundary && meta != nullptr) {
        if (meta->kind == TSKind::REF) {
            out.push_back(0);
        } else if (meta->kind == TSKind::TSB) {
            out.push_back(0);
        } else if (meta->kind == TSKind::TSD) {
            out.push_back(0);
        } else if (meta->kind == TSKind::TSL && meta->fixed_size() > 0) {
            out.push_back(0);
        }
    }

    return out;
}

std::vector<size_t> ts_path_to_time_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    std::vector<size_t> out;
    const TSMeta* meta = root_meta;

    if (meta == nullptr) {
        return out;
    }

    if (ts_path.empty()) {
        if (meta->kind == TSKind::TSB || meta->kind == TSKind::TSL || meta->kind == TSKind::TSD) {
            out.push_back(0);  // container time slot
        }
        return out;
    }

    for (size_t index : ts_path) {
        while (meta != nullptr && meta->kind == TSKind::REF) {
            meta = meta->element_ts();
        }

        if (meta == nullptr) {
            break;
        }

        switch (meta->kind) {
            case TSKind::TSB:
                out.push_back(index + 1);
                if (index >= meta->field_count() || meta->fields() == nullptr) {
                    return out;
                }
                meta = meta->fields()[index].ts_type;
                break;

            case TSKind::TSL:
                out.push_back(1);
                out.push_back(index);
                meta = meta->element_ts();
                break;

            case TSKind::TSD:
                out.push_back(1);
                out.push_back(index);
                meta = meta->element_ts();
                break;

            default:
                return out;
        }
    }

    return out;
}

std::vector<std::vector<size_t>> time_stamp_paths_for_ts_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    std::vector<std::vector<size_t>> out;
    if (root_meta == nullptr) {
        return out;
    }

    if (root_meta->kind == TSKind::TSB || root_meta->kind == TSKind::TSL || root_meta->kind == TSKind::TSD) {
        out.push_back({0});  // root container timestamp
    } else {
        out.push_back({});
    }

    const TSMeta* meta = root_meta;
    std::vector<size_t> current_path;
    for (size_t index : ts_path) {
        while (meta != nullptr && meta->kind == TSKind::REF) {
            meta = meta->element_ts();
        }

        if (meta == nullptr) {
            break;
        }

        switch (meta->kind) {
            case TSKind::TSB:
                current_path.push_back(index + 1);
                out.push_back(current_path);
                if (index >= meta->field_count() || meta->fields() == nullptr) {
                    return out;
                }
                meta = meta->fields()[index].ts_type;
                break;

            case TSKind::TSL:
                current_path.push_back(1);
                current_path.push_back(index);
                out.push_back(current_path);
                meta = meta->element_ts();
                break;

            case TSKind::TSD:
                current_path.push_back(1);
                current_path.push_back(index);
                out.push_back(current_path);
                meta = meta->element_ts();
                break;

            default:
                return out;
        }
    }

    return out;
}

size_t static_container_child_count(const TSMeta* meta) {
    if (meta == nullptr) {
        return 0;
    }

    switch (meta->kind) {
        case TSKind::TSB:
            return meta->field_count();
        case TSKind::TSL:
            return meta->fixed_size();
        default:
            return 0;
    }
}

std::optional<View> resolve_value_slot_const(const ViewData& vd);

bool link_target_points_to_unbound_ref_composite(const ViewData& vd, const LinkTarget* payload) {
    if (payload == nullptr || !payload->is_linked) {
        return false;
    }

    ViewData target = payload->as_view_data(vd.sampled);
    const TSMeta* target_meta = meta_at_path(target.meta, target.path.indices);
    if (target_meta == nullptr || target_meta->kind != TSKind::REF) {
        return false;
    }

    auto local = resolve_value_slot_const(target);
    if (!local.has_value() || !local->valid() || local->schema() != ts_reference_meta()) {
        return false;
    }

    TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
    return ref.is_unbound() && !ref.is_empty();
}

bool is_unpeered_static_container_view(const ViewData& vd, const TSMeta* current) {
    if (!vd.uses_link_target || current == nullptr) {
        return false;
    }

    if (current->kind != TSKind::TSB && (current->kind != TSKind::TSL || current->fixed_size() == 0)) {
        return false;
    }

    if (LinkTarget* payload = resolve_link_target(vd, vd.path.indices); payload != nullptr) {
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

    switch (node_meta->kind) {
        case TSKind::TSB:
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

        case TSKind::TSL:
            if (node_meta->fixed_size() == 0) {
                return;
            }
            for (size_t i = 0; i < node_meta->fixed_size(); ++i) {
                current_ts_path.push_back(i);
                out.push_back(current_ts_path);
                collect_static_descendant_ts_paths(node_meta->element_ts(), current_ts_path, out);
                current_ts_path.pop_back();
            }
            return;

        default:
            return;
    }
}

engine_time_t extract_time_value(const View& time_view) {
    if (!time_view.valid()) {
        return MIN_DT;
    }

    if (time_view.is_scalar_type<engine_time_t>()) {
        return time_view.as<engine_time_t>();
    }

    if (time_view.is_tuple()) {
        auto tuple = time_view.as_tuple();
        if (tuple.size() > 0) {
            View head = tuple.at(0);
            if (head.valid() && head.is_scalar_type<engine_time_t>()) {
                return head.as<engine_time_t>();
            }
        }
    }

    return MIN_DT;
}

engine_time_t* extract_time_ptr(ValueView time_view) {
    if (!time_view.valid()) {
        return nullptr;
    }

    if (time_view.is_scalar_type<engine_time_t>()) {
        return &time_view.as<engine_time_t>();
    }

    if (time_view.is_tuple()) {
        auto tuple = time_view.as_tuple();
        if (tuple.size() > 0) {
            ValueView head = tuple.at(0);
            if (head.valid() && head.is_scalar_type<engine_time_t>()) {
                return &head.as<engine_time_t>();
            }
        }
    }

    return nullptr;
}

std::optional<View> resolve_link_view(const ViewData& vd, const std::vector<size_t>& ts_path) {
    auto* link_root = static_cast<Value*>(vd.link_data);
    if (link_root == nullptr || !link_root->has_value()) {
        return std::nullopt;
    }

    auto link_path = ts_path_to_link_path(vd.meta, ts_path);
    auto link_view = navigate_const(link_root->view(), link_path);
    if (!link_view.has_value() || !link_view->valid()) {
        return std::nullopt;
    }
    return link_view;
}

const value::TypeMeta* link_target_meta() {
    return value::TypeRegistry::instance().get_by_name("LinkTarget");
}

const value::TypeMeta* ref_link_meta() {
    return value::TypeRegistry::instance().get_by_name("REFLink");
}

const value::TypeMeta* ts_reference_meta() {
    return value::TypeRegistry::instance().get_by_name("TimeSeriesReference");
}

LinkTarget* resolve_link_target(const ViewData& vd, const std::vector<size_t>& ts_path) {
    auto link_view = resolve_link_view(vd, ts_path);
    if (!link_view.has_value() || !link_view->valid() || link_view->schema() == nullptr) {
        return nullptr;
    }

    if (link_target_meta() == nullptr || link_view->schema() != link_target_meta()) {
        return nullptr;
    }
    return static_cast<LinkTarget*>(const_cast<void*>(link_view->data()));
}

REFLink* resolve_ref_link(const ViewData& vd, const std::vector<size_t>& ts_path) {
    auto link_view = resolve_link_view(vd, ts_path);
    if (!link_view.has_value() || !link_view->valid() || link_view->schema() == nullptr) {
        return nullptr;
    }

    if (ref_link_meta() == nullptr || link_view->schema() != ref_link_meta()) {
        return nullptr;
    }
    return static_cast<REFLink*>(const_cast<void*>(link_view->data()));
}

engine_time_t* resolve_owner_time_ptr(ViewData& vd) {
    auto* time_root = static_cast<Value*>(vd.time_data);
    if (time_root == nullptr || time_root->schema() == nullptr) {
        return nullptr;
    }

    if (!time_root->has_value()) {
        time_root->emplace();
    }

    auto time_path = ts_path_to_time_path(vd.meta, vd.path.indices);
    std::optional<ValueView> time_slot;
    if (time_path.empty()) {
        time_slot = time_root->view();
    } else {
        time_slot = navigate_mut(time_root->view(), time_path);
    }

    if (!time_slot.has_value()) {
        return nullptr;
    }

    return extract_time_ptr(*time_slot);
}

void ensure_tsd_child_time_slot(ViewData& vd, size_t child_slot) {
    auto* time_root = static_cast<Value*>(vd.time_data);
    if (time_root == nullptr || time_root->schema() == nullptr) {
        return;
    }

    if (!time_root->has_value()) {
        time_root->emplace();
    }

    auto parent_time_path = ts_path_to_time_path(vd.meta, vd.path.indices);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta != nullptr && self_meta->kind == TSKind::TSD &&
        vd.path.indices.empty() &&
        !parent_time_path.empty() && parent_time_path.back() == 0) {
        // Only root TSD views map directly to the container-time slot (0).
        // Child TSD paths like [slot=0] also end with 0, but that 0 is the
        // child index and must not be stripped.
        parent_time_path.pop_back();
    }
    std::optional<ValueView> maybe_parent_time;
    if (parent_time_path.empty()) {
        maybe_parent_time = time_root->view();
    } else {
        maybe_parent_time = navigate_mut(time_root->view(), parent_time_path);
    }
    if (!maybe_parent_time.has_value() || !maybe_parent_time->valid() || !maybe_parent_time->is_tuple()) {
        return;
    }

    auto parent_tuple = maybe_parent_time->as_tuple();
    if (parent_tuple.size() < 2) {
        return;
    }

    ValueView children = parent_tuple.at(1);
    if (!children.valid() || !children.is_list()) {
        return;
    }

    auto list = children.as_list();
    const size_t old_size = list.size();
    if (child_slot >= list.size()) {
        list.resize(child_slot + 1);
        const value::TypeMeta* list_type = list.schema();
        for (size_t i = old_size; i < list.size(); ++i) {
            list_type->ops().set_at(list.data(), i, nullptr, list_type);
        }
    }

    ValueView slot = list.at(child_slot);
    if (!slot.valid()) {
        const value::TypeMeta* element_type = list.element_type();
        if (element_type != nullptr) {
            Value materialized(element_type);
            materialized.emplace();
            list.set(child_slot, materialized.view());
        }
    }
}

std::optional<ValueView> resolve_tsd_child_link_list(ViewData& vd) {
    auto* link_root = static_cast<Value*>(vd.link_data);
    if (link_root == nullptr || link_root->schema() == nullptr) {
        return std::nullopt;
    }
    if (!link_root->has_value()) {
        link_root->emplace();
    }

    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr || self_meta->kind != TSKind::TSD) {
        return std::nullopt;
    }

    auto parent_link_path = ts_path_to_link_path(vd.meta, vd.path.indices);
    if (parent_link_path.empty() || parent_link_path.back() != 0) {
        return std::nullopt;
    }
    parent_link_path.pop_back();

    std::optional<ValueView> maybe_parent_link =
        parent_link_path.empty() ? std::optional<ValueView>{link_root->view()}
                                 : navigate_mut(link_root->view(), parent_link_path);
    if (!maybe_parent_link.has_value() || !maybe_parent_link->valid() || !maybe_parent_link->is_tuple()) {
        return std::nullopt;
    }

    auto tuple = maybe_parent_link->as_tuple();
    if (tuple.size() < 2) {
        return std::nullopt;
    }

    ValueView children = tuple.at(1);
    if (!children.valid() || !children.is_list()) {
        return std::nullopt;
    }

    return children;
}

void ensure_tsd_child_link_slot(ViewData& vd, size_t child_slot) {
    auto maybe_children = resolve_tsd_child_link_list(vd);
    if (!maybe_children.has_value()) {
        return;
    }

    auto list = maybe_children->as_list();
    const size_t old_size = list.size();
    if (child_slot >= list.size()) {
        list.resize(child_slot + 1);
        const value::TypeMeta* list_type = list.schema();
        for (size_t i = old_size; i < list.size(); ++i) {
            list_type->ops().set_at(list.data(), i, nullptr, list_type);
        }
    }

    ValueView slot = list.at(child_slot);
    if (!slot.valid()) {
        const value::TypeMeta* element_type = list.element_type();
        if (element_type != nullptr) {
            Value materialized(element_type);
            materialized.emplace();
            list.set(child_slot, materialized.view());
        }
    }
}

bool is_valid_list_slot(const value::ListView& list, size_t slot) {
    if (!list.valid() || !list.is_list() || slot >= list.size()) {
        return false;
    }
    const value::TypeMeta* schema = list.schema();
    return schema->ops().at(list.data(), slot, schema) != nullptr;
}

void clear_list_slot(value::ListView list, size_t slot) {
    if (!list.valid() || !list.is_list() || slot >= list.size()) {
        return;
    }

    const value::TypeMeta* schema = list.schema();
    schema->ops().set_at(list.data(), slot, nullptr, schema);

    while (list.size() > 0) {
        const size_t tail = list.size() - 1;
        if (is_valid_list_slot(list, tail)) {
            break;
        }
        list.pop_back();
    }
}

void compact_tsd_child_link_slot(ViewData& vd, size_t child_slot) {
    auto maybe_children = resolve_tsd_child_link_list(vd);
    if (!maybe_children.has_value()) {
        return;
    }

    auto list = maybe_children->as_list();
    clear_list_slot(list, child_slot);
}

std::optional<ValueView> resolve_tsd_child_time_list(ViewData& vd) {
    auto* time_root = static_cast<Value*>(vd.time_data);
    if (time_root == nullptr || time_root->schema() == nullptr) {
        return std::nullopt;
    }
    if (!time_root->has_value()) {
        time_root->emplace();
    }

    auto parent_time_path = ts_path_to_time_path(vd.meta, vd.path.indices);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta != nullptr && self_meta->kind == TSKind::TSD &&
        vd.path.indices.empty() &&
        !parent_time_path.empty() && parent_time_path.back() == 0) {
        parent_time_path.pop_back();
    }

    std::optional<ValueView> maybe_parent_time =
        parent_time_path.empty() ? std::optional<ValueView>{time_root->view()}
                                 : navigate_mut(time_root->view(), parent_time_path);
    if (!maybe_parent_time.has_value() || !maybe_parent_time->valid() || !maybe_parent_time->is_tuple()) {
        return std::nullopt;
    }

    auto tuple = maybe_parent_time->as_tuple();
    if (tuple.size() < 2) {
        return std::nullopt;
    }

    ValueView children = tuple.at(1);
    if (!children.valid() || !children.is_list()) {
        return std::nullopt;
    }
    return children;
}

void compact_tsd_child_time_slot(ViewData& vd, size_t child_slot) {
    auto maybe_children = resolve_tsd_child_time_list(vd);
    if (!maybe_children.has_value()) {
        return;
    }

    auto list = maybe_children->as_list();
    clear_list_slot(list, child_slot);
}

void ensure_tsd_child_delta_slot(ViewData& vd, size_t child_slot) {
    auto maybe_slot = resolve_delta_slot_mut(vd);
    if (!maybe_slot.has_value() || !maybe_slot->valid() || !maybe_slot->is_tuple()) {
        return;
    }

    auto tuple = maybe_slot->as_tuple();
    if (tuple.size() < 4) {
        return;
    }

    ValueView children = tuple.at(3);
    if (!children.valid() || !children.is_list()) {
        return;
    }

    auto list = children.as_list();
    const size_t old_size = list.size();
    if (child_slot >= list.size()) {
        list.resize(child_slot + 1);
        const value::TypeMeta* list_type = list.schema();
        for (size_t i = old_size; i < list.size(); ++i) {
            list_type->ops().set_at(list.data(), i, nullptr, list_type);
        }
    }

    // Child delta slots are list elements of tuple schema. List resize only
    // constructs storage; it may leave slots invalid. Materialize the slot so
    // nested TSD delta tracking has a concrete tuple payload to mutate.
    ValueView slot = list.at(child_slot);
    if (!slot.valid()) {
        const value::TypeMeta* element_type = list.element_type();
        if (element_type != nullptr) {
            Value materialized(element_type);
            materialized.emplace();
            list.set(child_slot, materialized.view());
        }
    }
}

void compact_tsd_child_delta_slot(ViewData& vd, size_t child_slot) {
    auto maybe_slot = resolve_delta_slot_mut(vd);
    if (!maybe_slot.has_value() || !maybe_slot->valid() || !maybe_slot->is_tuple()) {
        return;
    }

    auto tuple = maybe_slot->as_tuple();
    if (tuple.size() < 4) {
        return;
    }

    ValueView children = tuple.at(3);
    if (!children.valid() || !children.is_list()) {
        return;
    }

    auto list = children.as_list();
    clear_list_slot(list, child_slot);
}

LinkTarget* resolve_parent_link_target(const ViewData& vd) {
    if (vd.path.indices.empty()) {
        return nullptr;
    }

    LinkTarget* self = resolve_link_target(vd, vd.path.indices);
    std::vector<size_t> parent_path = vd.path.indices;
    parent_path.pop_back();
    LinkTarget* parent = resolve_link_target(vd, parent_path);
    if (parent == nullptr || self == nullptr) {
        return parent;
    }

    if (parent == self) {
        return nullptr;
    }

    // Parent links must form a strict ancestor chain. Reject cyclic candidates.
    std::unordered_set<const LinkTarget*> visited;
    for (const LinkTarget* cursor = parent; cursor != nullptr; cursor = cursor->parent_link) {
        if (cursor == self) {
            return nullptr;
        }
        if (!visited.insert(cursor).second) {
            return nullptr;
        }
    }

    return parent;
}

const TSMeta* resolve_meta_or_ancestor(const ViewData& vd, bool& used_ancestor) {
    used_ancestor = false;
    if (const TSMeta* current = meta_at_path(vd.meta, vd.path.indices); current != nullptr) {
        return current;
    }

    std::vector<size_t> ancestor = vd.path.indices;
    while (!ancestor.empty()) {
        ancestor.pop_back();
        if (const TSMeta* current = meta_at_path(vd.meta, ancestor); current != nullptr) {
            used_ancestor = true;
            return current;
        }
    }

    return nullptr;
}

std::vector<size_t> link_residual_ts_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    std::vector<size_t> residual;
    const TSMeta* current = root_meta;
    bool collecting_residual = false;

    for (size_t index : ts_path) {
        while (!collecting_residual && current != nullptr && current->kind == TSKind::REF) {
            current = current->element_ts();
        }

        if (collecting_residual || current == nullptr) {
            residual.push_back(index);
            continue;
        }

        switch (current->kind) {
            case TSKind::TSB:
                if (current->fields() == nullptr || index >= current->field_count()) {
                    collecting_residual = true;
                    residual.push_back(index);
                    current = nullptr;
                } else {
                    current = current->fields()[index].ts_type;
                }
                break;

            case TSKind::TSL:
                if (current->fixed_size() > 0) {
                    current = current->element_ts();
                } else {
                    collecting_residual = true;
                    residual.push_back(index);
                    current = current->element_ts();
                }
                break;

            case TSKind::TSD:
                collecting_residual = true;
                residual.push_back(index);
                current = current->element_ts();
                break;
            default:
                collecting_residual = true;
                residual.push_back(index);
                break;
        }
    }

    return residual;
}

std::optional<View> resolve_value_slot_const(const ViewData& vd);
std::optional<ViewData> resolve_ref_ancestor_descendant_view_data(const ViewData& vd);
ViewProjection merge_projection(ViewProjection requested, ViewProjection resolved) {
    return requested != ViewProjection::NONE ? requested : resolved;
}

std::optional<std::vector<size_t>> remap_residual_indices_for_bound_view(
    const ViewData& local_view,
    const ViewData& bound_view,
    const std::vector<size_t>& residual_indices) {
    if (residual_indices.empty()) {
        return std::vector<size_t>{};
    }

    const TSMeta* current = meta_at_path(local_view.meta, local_view.path.indices);
    std::vector<size_t> local_path = local_view.path.indices;
    std::vector<size_t> bound_path = bound_view.path.indices;
    std::vector<size_t> mapped;
    mapped.reserve(residual_indices.size());

    for (size_t index : residual_indices) {
        while (current != nullptr && current->kind == TSKind::REF) {
            current = current->element_ts();
        }
        if (current == nullptr) {
            return std::nullopt;
        }

        switch (current->kind) {
            case TSKind::TSB: {
                if (current->fields() == nullptr || index >= current->field_count()) {
                    return std::nullopt;
                }
                mapped.push_back(index);
                local_path.push_back(index);
                bound_path.push_back(index);
                current = current->fields()[index].ts_type;
                break;
            }
            case TSKind::TSL: {
                mapped.push_back(index);
                local_path.push_back(index);
                bound_path.push_back(index);
                current = current->element_ts();
                break;
            }
            case TSKind::TSD: {
                ViewData local_container = local_view;
                local_container.path.indices = local_path;
                ViewData bound_container = bound_view;
                bound_container.path.indices = bound_path;

                auto local_value = resolve_value_slot_const(local_container);
                auto bound_value = resolve_value_slot_const(bound_container);
                if (!local_value.has_value() || !bound_value.has_value() ||
                    !local_value->valid() || !bound_value->valid() ||
                    !local_value->is_map() || !bound_value->is_map()) {
                    return std::nullopt;
                }

                auto local_map = local_value->as_map();
                auto bound_map = bound_value->as_map();

                std::optional<View> key_for_local_slot;
                for (View key : local_map.keys()) {
                    auto slot = map_slot_for_key(local_map, key);
                    if (slot.has_value() && *slot == index) {
                        key_for_local_slot = key;
                        break;
                    }
                }
                if (!key_for_local_slot.has_value()) {
                    return std::nullopt;
                }

                auto bound_slot = map_slot_for_key(bound_map, *key_for_local_slot);
                if (!bound_slot.has_value()) {
                    return std::nullopt;
                }

                mapped.push_back(*bound_slot);
                local_path.push_back(index);
                bound_path.push_back(*bound_slot);
                current = current->element_ts();
                break;
            }
            default: {
                mapped.push_back(index);
                local_path.push_back(index);
                bound_path.push_back(index);
                current = nullptr;
                break;
            }
        }
    }

    return mapped;
}

std::optional<ViewData> resolve_bound_view_data(const ViewData& vd) {
    const bool debug_resolve = std::getenv("HGRAPH_DEBUG_RESOLVE") != nullptr;
    const auto target_can_accept_residual = [](const TSMeta* meta) {
        while (meta != nullptr && meta->kind == TSKind::REF) {
            meta = meta->element_ts();
        }
        if (meta == nullptr) {
            return false;
        }
        return meta->kind == TSKind::TSB || meta->kind == TSKind::TSL || meta->kind == TSKind::TSD;
    };
    const auto log_resolve = [&](const char* tag, const ViewData& out) {
        if (!debug_resolve) {
            return;
        }
        const TSMeta* in_meta = vd.meta != nullptr ? meta_at_path(vd.meta, vd.path.indices) : nullptr;
        const TSMeta* out_meta = out.meta != nullptr ? meta_at_path(out.meta, out.path.indices) : nullptr;
        std::fprintf(stderr,
                     "[resolve] %s in=%s uses_lt=%d kind=%d -> out=%s out_uses_lt=%d out_kind=%d\n",
                     tag,
                     vd.path.to_string().c_str(),
                     vd.uses_link_target ? 1 : 0,
                     in_meta != nullptr ? static_cast<int>(in_meta->kind) : -1,
                     out.path.to_string().c_str(),
                     out.uses_link_target ? 1 : 0,
                     out_meta != nullptr ? static_cast<int>(out_meta->kind) : -1);
    };

    if (vd.uses_link_target) {
        if (LinkTarget* target = resolve_link_target(vd, vd.path.indices);
            target != nullptr && target->is_linked) {
            ViewData resolved = target->as_view_data(vd.sampled);
            resolved.projection = merge_projection(vd.projection, resolved.projection);
            size_t residual_len = 0;
            const TSMeta* resolved_meta = meta_at_path(resolved.meta, resolved.path.indices);
            if (resolved.path.indices.size() < vd.path.indices.size() &&
                target_can_accept_residual(resolved_meta)) {
                const auto residual = link_residual_ts_path(vd.meta, vd.path.indices);
                residual_len = residual.size();
                if (!residual.empty()) {
                    if (auto mapped = remap_residual_indices_for_bound_view(vd, resolved, residual); mapped.has_value()) {
                        resolved.path.indices.insert(resolved.path.indices.end(), mapped->begin(), mapped->end());
                    } else {
                        resolved.path.indices.insert(resolved.path.indices.end(), residual.begin(), residual.end());
                    }
                }
            }
            if (std::getenv("HGRAPH_DEBUG_REF_ANCESTOR") != nullptr) {
                std::fprintf(stderr,
                             "[resolve_lt] in_path=%s link_target_path=%s residual_len=%zu out_path=%s out_value_data=%p\n",
                             vd.path.to_string().c_str(),
                             target->target_path.to_string().c_str(),
                             residual_len,
                             resolved.path.to_string().c_str(),
                             resolved.value_data);
            }
            log_resolve("lt-direct", resolved);
            return resolved;
        }

        // Child links in containers can remain unbound while an ancestor link is
        // bound at the container root. Resolve through the nearest linked
        // ancestor and append the residual child path.
        if (!vd.path.indices.empty()) {
            for (size_t depth = vd.path.indices.size(); depth > 0; --depth) {
                const std::vector<size_t> parent_path(vd.path.indices.begin(), vd.path.indices.begin() + depth - 1);
                LinkTarget* parent = resolve_link_target(vd, parent_path);
                if (parent == nullptr || !parent->is_linked) {
                    continue;
                }

                ViewData resolved = parent->as_view_data(vd.sampled);
                resolved.projection = merge_projection(vd.projection, resolved.projection);
                const std::vector<size_t> residual(
                    vd.path.indices.begin() + static_cast<std::ptrdiff_t>(parent_path.size()),
                    vd.path.indices.end());
                ViewData local_parent = vd;
                local_parent.path.indices = parent_path;
                if (auto mapped = remap_residual_indices_for_bound_view(local_parent, resolved, residual); mapped.has_value()) {
                    resolved.path.indices.insert(resolved.path.indices.end(), mapped->begin(), mapped->end());
                } else {
                    resolved.path.indices.insert(resolved.path.indices.end(), residual.begin(), residual.end());
                }
                log_resolve("lt-ancestor", resolved);
                return resolved;
            }
        }
    } else {
        const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
        if (current != nullptr && current->kind == TSKind::REF) {
            // For per-slot REF values (notably TSD dynamic children), prefer the
            // local reference payload over shared REFLink indirection.
            if (auto local = resolve_value_slot_const(vd);
                local.has_value() && local->valid() && local->schema() == ts_reference_meta()) {
                TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
                if (ref.is_empty()) {
                    return std::nullopt;
                }
            }
        }

        // Prefer local REF wrapper payloads on ancestor paths (e.g. TSD slot
        // descendants) before consulting REFLink alternative storage.
        if (auto resolved_ref_ancestor = resolve_ref_ancestor_descendant_view_data(vd);
            resolved_ref_ancestor.has_value()) {
            log_resolve("ref-value-ancestor", *resolved_ref_ancestor);
            return resolved_ref_ancestor;
        }

        // REFLink dereference is alternative storage for REF paths only.
        // Non-REF container roots (e.g. TSD of REF values) must not resolve via
        // REFLink, otherwise reads collapse to a child scalar target.
        const auto path_traverses_ref = [&vd]() -> bool {
            const TSMeta* cursor = vd.meta;
            for (size_t index : vd.path.indices) {
                if (cursor != nullptr && cursor->kind == TSKind::REF) {
                    return true;
                }
                if (cursor == nullptr) {
                    return false;
                }

                switch (cursor->kind) {
                    case TSKind::TSB:
                        if (cursor->fields() == nullptr || index >= cursor->field_count()) {
                            return false;
                        }
                        cursor = cursor->fields()[index].ts_type;
                        break;
                    case TSKind::TSL:
                    case TSKind::TSD:
                        cursor = cursor->element_ts();
                        break;
                    case TSKind::REF:
                        return true;
                    default:
                        return false;
                }
            }
            return cursor != nullptr && cursor->kind == TSKind::REF;
        };

        if (!path_traverses_ref()) {
            return std::nullopt;
        }

        REFLink* direct_ref_link = resolve_ref_link(vd, vd.path.indices);
        if (debug_resolve) {
            std::fprintf(stderr,
                         "[resolve-ref] direct path=%s link=%p linked=%d\n",
                         vd.path.to_string().c_str(),
                         static_cast<void*>(direct_ref_link),
                         (direct_ref_link != nullptr && direct_ref_link->is_linked) ? 1 : 0);
        }

        if (direct_ref_link != nullptr && direct_ref_link->is_linked) {
            REFLink* ref_link = direct_ref_link;
            ViewData resolved = ref_link->resolved_view_data();
            // Direct REF links already represent the fully resolved target for
            // this exact path. Appending residual indices here would duplicate
            // keyed TSD slots (for example out/1 -> out/1/1) and collapse
            // nested REF[TSD] reads to scalar children.
            resolved.sampled = resolved.sampled || vd.sampled;
            resolved.projection = merge_projection(vd.projection, resolved.projection);
            log_resolve("ref-direct", resolved);
            return resolved;
        }

        // Like LinkTarget, REF-linked container roots can service descendant
        // reads/writes by carrying the unresolved child suffix.
        if (!vd.path.indices.empty()) {
            for (size_t depth = vd.path.indices.size(); depth > 0; --depth) {
                const std::vector<size_t> parent_path(vd.path.indices.begin(), vd.path.indices.begin() + depth - 1);
                REFLink* parent = resolve_ref_link(vd, parent_path);
                if (debug_resolve) {
                    ViewData parent_vd = vd;
                    parent_vd.path.indices = parent_path;
                    std::fprintf(stderr,
                                 "[resolve-ref] ancestor path=%s link=%p linked=%d\n",
                                 parent_vd.path.to_string().c_str(),
                                 static_cast<void*>(parent),
                                 (parent != nullptr && parent->is_linked) ? 1 : 0);
                }
                if (parent == nullptr || !parent->is_linked) {
                    continue;
                }

                ViewData resolved = parent->resolved_view_data();
                resolved.path.indices.insert(
                    resolved.path.indices.end(),
                    vd.path.indices.begin() + static_cast<std::ptrdiff_t>(parent_path.size()),
                    vd.path.indices.end());
                resolved.sampled = resolved.sampled || vd.sampled;
                resolved.projection = merge_projection(vd.projection, resolved.projection);
                log_resolve("ref-ancestor", resolved);
                return resolved;
            }
        }
    }

    return std::nullopt;
}

std::optional<View> resolve_value_slot_const(const ViewData& vd);
bool resolve_read_view_data(const ViewData& vd, const TSMeta* self_meta, ViewData& out);

engine_time_t rebind_time_for_view(const ViewData& vd) {
    if (vd.uses_link_target) {
        engine_time_t out = MIN_DT;
        if (LinkTarget* target = resolve_link_target(vd, vd.path.indices); target != nullptr) {
            if (target->has_previous_target) {
                out = std::max(out, target->last_rebind_time);
            }
        }

        auto is_static_container_parent = [](const TSMeta* meta) {
            if (meta == nullptr) {
                return false;
            }
            if (meta->kind == TSKind::TSB) {
                return true;
            }
            if (meta->kind == TSKind::TSL && meta->fixed_size() > 0) {
                return true;
            }
            if (meta->kind == TSKind::REF) {
                const TSMeta* element = meta->element_ts();
                return element != nullptr &&
                       (element->kind == TSKind::TSB ||
                        (element->kind == TSKind::TSL && element->fixed_size() > 0));
            }
            return false;
        };

        // Descendant views only inherit ancestor rebind markers for static
        // container ancestry (TSB / fixed-size TSL). Propagating dynamic
        // container rebinds (for example TSD key-set churn) over-reports
        // unchanged children as modified.
        if (!vd.path.indices.empty()) {
            for (size_t depth = vd.path.indices.size(); depth > 0; --depth) {
                std::vector<size_t> parent_path(
                    vd.path.indices.begin(),
                    vd.path.indices.begin() + static_cast<std::ptrdiff_t>(depth - 1));
                const TSMeta* parent_meta = meta_at_path(vd.meta, parent_path);
                if (!is_static_container_parent(parent_meta)) {
                    continue;
                }
                if (LinkTarget* parent = resolve_link_target(vd, parent_path);
                    parent != nullptr && parent->is_linked && parent->has_previous_target) {
                    out = std::max(out, parent->last_rebind_time);
                }
            }
        }
        return out;
    }

    engine_time_t out = MIN_DT;
    if (REFLink* ref_link = resolve_ref_link(vd, vd.path.indices); ref_link != nullptr) {
        out = std::max(out, ref_link->last_rebind_time);
    }
    if (!vd.path.indices.empty()) {
        for (size_t depth = vd.path.indices.size(); depth > 0; --depth) {
            std::vector<size_t> parent_path(
                vd.path.indices.begin(),
                vd.path.indices.begin() + static_cast<std::ptrdiff_t>(depth - 1));
            if (REFLink* parent = resolve_ref_link(vd, parent_path); parent != nullptr) {
                out = std::max(out, parent->last_rebind_time);
            }
        }
    }
    return out;
}

bool same_view_identity(const ViewData& lhs, const ViewData& rhs) {
    return lhs.value_data == rhs.value_data &&
           lhs.time_data == rhs.time_data &&
           lhs.observer_data == rhs.observer_data &&
           lhs.delta_data == rhs.delta_data &&
           lhs.link_data == rhs.link_data &&
           lhs.link_observer_registry == rhs.link_observer_registry &&
           lhs.projection == rhs.projection &&
           lhs.path.indices == rhs.path.indices;
}

bool same_or_descendant_view(const ViewData& base, const ViewData& candidate) {
    return base.value_data == candidate.value_data &&
           base.time_data == candidate.time_data &&
           base.observer_data == candidate.observer_data &&
           base.delta_data == candidate.delta_data &&
           base.link_data == candidate.link_data &&
           base.link_observer_registry == candidate.link_observer_registry &&
           base.projection == candidate.projection &&
           is_prefix_path(base.path.indices, candidate.path.indices);
}

engine_time_t direct_last_modified_time(const ViewData& vd) {
    auto* time_root = static_cast<const Value*>(vd.time_data);
    if (time_root == nullptr || !time_root->has_value()) {
        return MIN_DT;
    }

    const auto time_path = ts_path_to_time_path(vd.meta, vd.path.indices);
    std::optional<View> maybe_time;
    if (time_path.empty()) {
        maybe_time = time_root->view();
    } else {
        maybe_time = navigate_const(time_root->view(), time_path);
    }
    if (!maybe_time.has_value()) {
        return MIN_DT;
    }
    return extract_time_value(*maybe_time);
}

engine_time_t ref_wrapper_last_modified_time_on_read_path(const ViewData& vd) {
    // Rebind on REF wrappers must be visible to both REF and non-REF consumers
    // that resolve through those wrappers (e.g. DEFAULT[TIME_SERIES_TYPE] sinks).
    const bool include_wrapper_time = true;

    engine_time_t out = MIN_DT;
    ViewData probe = vd;
    probe.sampled = probe.sampled || vd.sampled;

    for (size_t depth = 0; depth < 64; ++depth) {
        const TSMeta* current = meta_at_path(probe.meta, probe.path.indices);
        const bool current_is_ref = current != nullptr && current->kind == TSKind::REF;

        auto rebound = resolve_bound_view_data(probe);

        if (current_is_ref && include_wrapper_time) {
            const bool has_rebound_target =
                rebound.has_value() && !same_view_identity(*rebound, probe);
            const TSMeta* element_meta = current != nullptr ? current->element_ts() : nullptr;
            const bool static_ref_container =
                element_meta != nullptr &&
                (element_meta->kind == TSKind::TSB ||
                 (element_meta->kind == TSKind::TSL && element_meta->fixed_size() > 0));

            // Non-REF consumers should observe REF wrapper *rebinds*, but not
            // wrapper-local rewrites that preserve target identity.
            if (has_rebound_target) {
                engine_time_t rebind_time = rebind_time_for_view(probe);
                if (probe.uses_link_target) {
                    if (LinkTarget* link_target = resolve_link_target(probe, probe.path.indices);
                        link_target != nullptr &&
                        link_target->is_linked &&
                        !link_target->has_previous_target) {
                        rebind_time = std::max(rebind_time, link_target->last_rebind_time);
                    }
                }
                out = std::max(out, rebind_time);
            } else if (static_ref_container) {
                // Static REF containers (for example REF[TSB]) can update local
                // reference payload without exposing a single bound_view target.
                // Surface wrapper-local time so consumers can sample unchanged
                // siblings on switch-style graph resets.
                out = std::max(out, direct_last_modified_time(probe));
            }
        }

        if (rebound.has_value()) {
            if (same_view_identity(*rebound, probe)) {
                return out;
            }
            probe = *rebound;
            probe.sampled = probe.sampled || vd.sampled;
            continue;
        }

        if (!current_is_ref) {
            return out;
        }

        auto local = resolve_value_slot_const(probe);
        if (!local.has_value() || !local->valid() || local->schema() != ts_reference_meta()) {
            return out;
        }

        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
        const ViewData* bound = ref.bound_view();
        if (bound == nullptr) {
            return out;
        }

        probe = *bound;
        probe.sampled = probe.sampled || vd.sampled;
    }

    return out;
}

bool resolve_ref_bound_target_view_data(const ViewData& ref_view, ViewData& out) {
    const bool debug_ref_ancestor = std::getenv("HGRAPH_DEBUG_REF_ANCESTOR") != nullptr;
    const TSMeta* ref_meta = meta_at_path(ref_view.meta, ref_view.path.indices);
    if (ref_meta == nullptr || ref_meta->kind != TSKind::REF) {
        return false;
    }

    auto ref_value = resolve_value_slot_const(ref_view);
    if (!ref_value.has_value() || !ref_value->valid() || ref_value->schema() != ts_reference_meta()) {
        return false;
    }

    TimeSeriesReference ref = nb::cast<TimeSeriesReference>(ref_value->to_python());
    const ViewData* bound = ref.bound_view();
    if (bound == nullptr) {
        return false;
    }

    out = *bound;
    out.sampled = out.sampled || ref_view.sampled;
    out.projection = merge_projection(ref_view.projection, out.projection);
    if (debug_ref_ancestor) {
        std::string bound_repr{"<none>"};
        if (bound->ops != nullptr && bound->ops->to_python != nullptr) {
            try {
                bound_repr = nb::cast<std::string>(nb::repr(bound->ops->to_python(*bound)));
            } catch (...) {
                bound_repr = "<repr_error>";
            }
        }
        std::fprintf(stderr,
                     "[ref_bound] ref_path=%s ref_value_data=%p bound_value_data=%p bound_path=%s bound=%s\n",
                     ref_view.path.to_string().c_str(),
                     ref_value->data(),
                     bound->value_data,
                     bound->path.to_string().c_str(),
                     bound_repr.c_str());
    }
    return true;
}

bool resolve_unbound_ref_item_view_data(const TimeSeriesReference& ref,
                                        const std::vector<size_t>& residual_path,
                                        size_t residual_offset,
                                        ViewData& out) {
    if (!ref.is_unbound()) {
        return false;
    }
    if (residual_offset >= residual_path.size()) {
        return false;
    }

    const auto& items = ref.items();
    const size_t index = residual_path[residual_offset];
    if (index >= items.size()) {
        return false;
    }

    const TimeSeriesReference& item_ref = items[index];
    if (const ViewData* bound = item_ref.bound_view(); bound != nullptr) {
        out = *bound;
        out.path.indices.insert(
            out.path.indices.end(),
            residual_path.begin() + static_cast<std::ptrdiff_t>(residual_offset + 1),
            residual_path.end());
        return true;
    }

    return resolve_unbound_ref_item_view_data(item_ref, residual_path, residual_offset + 1, out);
}

bool split_path_at_first_ref_ancestor(const TSMeta* root_meta,
                                      const std::vector<size_t>& ts_path,
                                      size_t& ref_depth_out) {
    if (root_meta == nullptr) {
        return false;
    }
    if (ts_path.empty()) {
        if (root_meta->kind == TSKind::REF) {
            ref_depth_out = 0;
            return true;
        }
        return false;
    }

    const TSMeta* current = root_meta;
    for (size_t depth = 0; depth < ts_path.size(); ++depth) {
        if (current != nullptr && current->kind == TSKind::REF) {
            ref_depth_out = depth;
            return true;
        }

        if (current == nullptr) {
            return false;
        }

        const size_t index = ts_path[depth];
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

    return false;
}

std::optional<ViewData> resolve_ref_ancestor_descendant_view_data(const ViewData& vd) {
    const bool debug_ref_ancestor = std::getenv("HGRAPH_DEBUG_REF_ANCESTOR") != nullptr;
    size_t ref_depth = 0;
    if (!split_path_at_first_ref_ancestor(vd.meta, vd.path.indices, ref_depth)) {
        return std::nullopt;
    }

    ViewData ref_view = vd;
    ref_view.path.indices.assign(vd.path.indices.begin(), vd.path.indices.begin() + static_cast<std::ptrdiff_t>(ref_depth));
    std::vector<size_t> residual_path(
        vd.path.indices.begin() + static_cast<std::ptrdiff_t>(ref_depth),
        vd.path.indices.end());

    ViewData resolved_ref{};
    if (!resolve_ref_bound_target_view_data(ref_view, resolved_ref)) {
        auto local = resolve_value_slot_const(ref_view);
        if (!local.has_value() || !local->valid() || local->schema() != ts_reference_meta()) {
            return std::nullopt;
        }

        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
        if (!resolve_unbound_ref_item_view_data(ref, residual_path, 0, resolved_ref)) {
            return std::nullopt;
        }
    } else {
        resolved_ref.path.indices.insert(
            resolved_ref.path.indices.end(),
            residual_path.begin(),
            residual_path.end());
    }

    resolved_ref.sampled = resolved_ref.sampled || vd.sampled;
    resolved_ref.projection = merge_projection(vd.projection, resolved_ref.projection);
    if (debug_ref_ancestor) {
        std::fprintf(stderr,
                     "[ref_ancestor] in_path=%s ref_path=%s out_path=%s out_value_data=%p\n",
                     vd.path.to_string().c_str(),
                     ref_view.path.to_string().c_str(),
                     resolved_ref.path.to_string().c_str(),
                     resolved_ref.value_data);
    }
    return resolved_ref;
}

void refresh_dynamic_ref_binding(const ViewData& vd, engine_time_t current_time) {
    if (!vd.uses_link_target) {
        return;
    }

    LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
    if (link_target == nullptr || !link_target->is_linked) {
        return;
    }
    refresh_dynamic_ref_binding_for_link_target(link_target, vd.sampled, current_time);
}

bool resolve_rebind_bridge_views(const ViewData& vd,
                                 const TSMeta* self_meta,
                                 engine_time_t current_time,
                                 ViewData& previous_resolved,
                                 ViewData& current_resolved) {
    if (!vd.uses_link_target) {
        return false;
    }

    LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
    if (link_target == nullptr ||
        !link_target->has_previous_target ||
        link_target->last_rebind_time != current_time ||
        !link_target->is_linked) {
        return false;
    }

    ViewData previous_view = link_target->previous_view_data(vd.sampled);
    ViewData current_view = link_target->has_resolved_target ? link_target->resolved_target : link_target->as_view_data(vd.sampled);
    current_view.sampled = current_view.sampled || vd.sampled;

    const auto resolve_or_empty_ref = [&](const ViewData& bridge_view, ViewData& out) {
        if (resolve_read_view_data(bridge_view, self_meta, out)) {
            return true;
        }

        // Bridge transitions to/from empty REF wrappers should still be surfaced
        // to container adapters so they can emit removal/addition deltas.
        const TSMeta* bridge_meta = meta_at_path(bridge_view.meta, bridge_view.path.indices);
        if (bridge_meta == nullptr || bridge_meta->kind != TSKind::REF) {
            return false;
        }

        auto local = resolve_value_slot_const(bridge_view);
        if (!local.has_value() || !local->valid() || local->schema() != ts_reference_meta()) {
            return false;
        }

        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
        if (!ref.is_empty()) {
            return false;
        }

        out = bridge_view;
        out.sampled = out.sampled || vd.sampled;
        return true;
    };

    if (!resolve_or_empty_ref(previous_view, previous_resolved)) {
        return false;
    }
    if (!resolve_or_empty_ref(current_view, current_resolved)) {
        return false;
    }
    return true;
}

nb::object tsd_bridge_delta_to_python(const ViewData& previous_data,
                                      const ViewData& current_data,
                                      engine_time_t current_time) {
    nb::dict delta_out;

    const auto bridge_entry_to_python = [](const View& entry) -> nb::object {
        if (!entry.valid()) {
            return nb::none();
        }
        if (entry.schema() != ts_reference_meta()) {
            return entry.to_python();
        }

        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(entry.to_python());
        if (const ViewData* target = ref.bound_view(); target != nullptr &&
            target->ops != nullptr && target->ops->to_python != nullptr) {
            return target->ops->to_python(*target);
        }
        return nb::none();
    };

    auto previous_value = resolve_value_slot_const(previous_data);
    auto current_value = resolve_value_slot_const(current_data);

    View previous_added_keys{};
    View previous_removed_keys{};
    if (op_modified(previous_data, current_time)) {
        if (auto* previous_delta_root = static_cast<const Value*>(previous_data.delta_data);
            previous_delta_root != nullptr && previous_delta_root->has_value()) {
            if (auto delta_path = ts_path_to_delta_path(previous_data.meta, previous_data.path.indices);
                delta_path.has_value()) {
                std::optional<View> maybe_delta;
                if (delta_path->empty()) {
                    maybe_delta = previous_delta_root->view();
                } else {
                    maybe_delta = navigate_const(previous_delta_root->view(), *delta_path);
                }

                if (maybe_delta.has_value() && maybe_delta->valid() && maybe_delta->is_tuple()) {
                    auto tuple = maybe_delta->as_tuple();
                    if (tuple.size() > 1) {
                        previous_added_keys = tuple.at(1);
                    }
                    if (tuple.size() > 2) {
                        previous_removed_keys = tuple.at(2);
                    }
                }
            }
        }
    }

    const bool has_previous = previous_value.has_value() && previous_value->valid() && previous_value->is_map();
    const bool has_current = current_value.has_value() && current_value->valid() && current_value->is_map();
    const auto entry_visible = [&](const value::MapView& map, const View& key) -> bool {
        if (!map.contains(key)) {
            return false;
        }
        return !bridge_entry_to_python(map.at(key)).is_none();
    };

    if (has_current) {
        const auto current_map = current_value->as_map();
        if (has_previous) {
            const auto previous_map = previous_value->as_map();
            for (View key : current_map.keys()) {
                View current_entry = current_map.at(key);
                bool include = !previous_map.contains(key);
                if (!include) {
                    View previous_entry = previous_map.at(key);
                    include = !(previous_entry.schema() == current_entry.schema() && previous_entry.equals(current_entry));
                }
                if (include) {
                    nb::object py_entry = bridge_entry_to_python(current_entry);
                    if (!py_entry.is_none()) {
                        delta_out[key.to_python()] = std::move(py_entry);
                    }
                }
            }
        } else {
            for (View key : current_map.keys()) {
                nb::object py_entry = bridge_entry_to_python(current_map.at(key));
                if (!py_entry.is_none()) {
                    delta_out[key.to_python()] = std::move(py_entry);
                }
            }
        }
    }

    if (has_previous) {
        const auto previous_map = previous_value->as_map();
        nb::object remove = get_remove();
        const auto is_prev_cycle_add = [&](const View& key) -> bool {
            return previous_added_keys.valid() && previous_added_keys.is_set() && previous_added_keys.as_set().contains(key);
        };
        const auto emit_remove_if_missing_from_current = [&](const View& key) {
            if (is_prev_cycle_add(key)) {
                return;
            }
            const bool in_prev = previous_map.contains(key);
            const bool prev_visible = in_prev && entry_visible(previous_map, key);
            const bool in_curr_visible = has_current && entry_visible(current_value->as_map(), key);
            if (in_prev && !prev_visible) {
                return;
            }
            if (in_curr_visible) {
                return;
            }
            delta_out[key.to_python()] = remove;
        };

        if (has_current) {
            for (View key : previous_map.keys()) {
                emit_remove_if_missing_from_current(key);
            }
        } else {
            for (View key : previous_map.keys()) {
                emit_remove_if_missing_from_current(key);
            }
        }

        if (previous_removed_keys.valid() && previous_removed_keys.is_set()) {
            for (View key : previous_removed_keys.as_set()) {
                if (!previous_map.contains(key)) {
                    continue;
                }
                emit_remove_if_missing_from_current(key);
            }
        }
    }

    return get_frozendict()(delta_out);
}

nb::object tss_bridge_delta_to_python(const ViewData& previous_data,
                                      const ViewData& current_data,
                                      engine_time_t current_time) {
    nb::set added_set;
    nb::set removed_set;

    auto previous_value = resolve_value_slot_const(previous_data);
    auto current_value = resolve_value_slot_const(current_data);

    const bool has_previous = previous_value.has_value() && previous_value->valid() && previous_value->is_set();
    const bool has_current = current_value.has_value() && current_value->valid() && current_value->is_set();

    value::SetView previous_added_on_tick{};
    value::SetView previous_removed_on_tick{};
    if (has_previous && op_last_modified_time(previous_data) == current_time) {
        auto* delta_root = static_cast<const Value*>(previous_data.delta_data);
        if (delta_root != nullptr && delta_root->has_value()) {
            std::optional<View> maybe_delta;
            if (previous_data.path.indices.empty()) {
                maybe_delta = delta_root->view();
            } else {
                maybe_delta = navigate_const(delta_root->view(), previous_data.path.indices);
            }
            if (maybe_delta.has_value() && maybe_delta->valid() && maybe_delta->is_tuple()) {
                auto tuple = maybe_delta->as_tuple();
                if (tuple.size() > 0 && tuple.at(0).valid() && tuple.at(0).is_set()) {
                    previous_added_on_tick = tuple.at(0).as_set();
                }
                if (tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set()) {
                    previous_removed_on_tick = tuple.at(1).as_set();
                }
            }
        }
    }

    const auto existed_before_tick = [&](const View& elem) {
        if (!has_previous) {
            return false;
        }
        const auto previous_set = previous_value->as_set();
        const bool in_previous_current = previous_set.contains(elem);
        const bool added_on_tick =
            previous_added_on_tick.valid() && previous_added_on_tick.contains(elem);
        const bool removed_on_tick =
            previous_removed_on_tick.valid() && previous_removed_on_tick.contains(elem);
        return (in_previous_current && !added_on_tick) || removed_on_tick;
    };

    if (has_current) {
        auto current_set = current_value->as_set();
        for (View elem : current_set) {
            if (!existed_before_tick(elem)) {
                added_set.add(elem.to_python());
            }
        }
    }

    if (has_previous) {
        auto previous_set = previous_value->as_set();
        const bool has_current_set = has_current;
        auto current_set = has_current_set ? current_value->as_set() : value::SetView{};

        for (View elem : previous_set) {
            if (previous_added_on_tick.valid() && previous_added_on_tick.contains(elem)) {
                continue;
            }
            if (!has_current_set || !current_set.contains(elem)) {
                removed_set.add(elem.to_python());
            }
        }
        if (previous_removed_on_tick.valid()) {
            for (View elem : previous_removed_on_tick) {
                if (!has_current_set || !current_set.contains(elem)) {
                    removed_set.add(elem.to_python());
                }
            }
        }
    }

    return python_set_delta(nb::frozenset(added_set), nb::frozenset(removed_set));
}

bool is_tsd_key_set_projection(const ViewData& vd) {
    return vd.projection == ViewProjection::TSD_KEY_SET;
}

bool resolve_tsd_key_set_projection_view(const ViewData& vd, ViewData& out) {
    if (!is_tsd_key_set_projection(vd)) {
        return false;
    }
    out = vd;
    return true;
}

bool resolve_tsd_key_set_source(const ViewData& vd, ViewData& out) {
    if (is_tsd_key_set_projection(vd)) {
        ViewData source = vd;
        source.projection = ViewProjection::NONE;

        const TSMeta* source_meta = meta_at_path(source.meta, source.path.indices);
        if (source_meta == nullptr || source_meta->kind != TSKind::TSD) {
            return false;
        }

        if (!resolve_read_view_data(source, source_meta, out)) {
            return false;
        }

        out.projection = ViewProjection::NONE;
        const TSMeta* current = meta_at_path(out.meta, out.path.indices);
        return current != nullptr && current->kind == TSKind::TSD;
    }

    // Explicit key_set bridge: graph TSS endpoint linked to backing TSD source.
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr || self_meta->kind != TSKind::TSS) {
        return false;
    }

    if (!resolve_read_view_data(vd, self_meta, out)) {
        return false;
    }

    out.projection = ViewProjection::NONE;
    const TSMeta* current = meta_at_path(out.meta, out.path.indices);
    return current != nullptr && current->kind == TSKind::TSD;
}

// Bridge paths can carry raw TSD views (projection::NONE) when rebinding
// between concrete and empty REF states. Treat those as key_set sources for
// bridge delta synthesis only.
bool resolve_tsd_key_set_bridge_source(const ViewData& vd, ViewData& out) {
    if (resolve_tsd_key_set_source(vd, out)) {
        return true;
    }

    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr || self_meta->kind != TSKind::TSD) {
        return false;
    }

    if (!resolve_read_view_data(vd, self_meta, out)) {
        return false;
    }

    out.projection = ViewProjection::NONE;
    const TSMeta* current = meta_at_path(out.meta, out.path.indices);
    return current != nullptr && current->kind == TSKind::TSD;
}

struct TSDKeySetDeltaState {
    bool has_delta_tuple{false};
    bool has_changed_values_map{false};
    bool has_added{false};
    bool has_removed{false};
};

TSDKeySetDeltaState tsd_key_set_delta_state(const ViewData& source) {
    TSDKeySetDeltaState state{};

    auto* delta_root = static_cast<const Value*>(source.delta_data);
    if (delta_root == nullptr || !delta_root->has_value()) {
        return state;
    }

    std::optional<View> maybe_delta;
    if (source.path.indices.empty()) {
        maybe_delta = delta_root->view();
    } else {
        maybe_delta = navigate_const(delta_root->view(), source.path.indices);
    }
    if (!maybe_delta.has_value() || !maybe_delta->valid() || !maybe_delta->is_tuple()) {
        return state;
    }

    state.has_delta_tuple = true;
    auto tuple = maybe_delta->as_tuple();
    state.has_changed_values_map =
        tuple.size() > 0 && tuple.at(0).valid() && tuple.at(0).is_map() && tuple.at(0).as_map().size() > 0;
    state.has_added = tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set() && tuple.at(1).as_set().size() > 0;
    state.has_removed =
        tuple.size() > 2 && tuple.at(2).valid() && tuple.at(2).is_set() && tuple.at(2).as_set().size() > 0;
    return state;
}

bool tsd_key_set_has_added_or_removed(const ViewData& source) {
    const auto state = tsd_key_set_delta_state(source);
    if (!state.has_delta_tuple) {
        return false;
    }
    return state.has_added || state.has_removed;
}

bool tsd_key_set_modified_this_tick(const ViewData& source, engine_time_t current_time) {
    if (op_last_modified_time(source) != current_time) {
        return false;
    }

    const auto state = tsd_key_set_delta_state(source);
    if (!state.has_delta_tuple) {
        return false;
    }

    if (state.has_added || state.has_removed) {
        return true;
    }

    // Python marks key_set as modified on:
    // 1) initial empty materialization (invalid -> empty valid)
    // 2) same-tick key churn with no net add/remove (add then remove)
    // In both cases, changed_values_map remains empty.
    if (state.has_changed_values_map) {
        return false;
    }

    auto value = resolve_value_slot_const(source);
    return value.has_value() && value->valid() && value->is_map();
}

nb::object tsd_key_set_to_python(const ViewData& source) {
    nb::set keys_out;
    auto value = resolve_value_slot_const(source);
    if (value.has_value() && value->valid() && value->is_map()) {
        for (View key : value->as_map().keys()) {
            keys_out.add(key.to_python());
        }
    }
    return nb::frozenset(keys_out);
}

nb::object tsd_key_set_delta_to_python(const ViewData& source) {
    nb::set added_out;
    nb::set removed_out;

    auto* delta_root = static_cast<const Value*>(source.delta_data);
    if (delta_root != nullptr && delta_root->has_value()) {
        std::optional<View> maybe_delta;
        if (source.path.indices.empty()) {
            maybe_delta = delta_root->view();
        } else {
            maybe_delta = navigate_const(delta_root->view(), source.path.indices);
        }

        if (maybe_delta.has_value() && maybe_delta->valid() && maybe_delta->is_tuple()) {
            auto tuple = maybe_delta->as_tuple();
            View removed_keys_view;
            if (tuple.size() > 2 && tuple.at(2).valid() && tuple.at(2).is_set()) {
                removed_keys_view = tuple.at(2);
                for (View elem : tuple.at(2).as_set()) {
                    removed_out.add(elem.to_python());
                }
            }
            const bool has_removed_keys = removed_keys_view.valid() && removed_keys_view.is_set();

            if (tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set()) {
                for (View elem : tuple.at(1).as_set()) {
                    // Invalid-key removals are encoded as add+remove markers in the
                    // TSD delta slot so key_set consumers can still observe removal.
                    if (has_removed_keys && removed_keys_view.as_set().contains(elem)) {
                        continue;
                    }
                    added_out.add(elem.to_python());
                }
            }
        }
    }

    return python_set_delta(nb::frozenset(added_out), nb::frozenset(removed_out));
}

nb::object tsd_key_set_all_added_to_python(const ViewData& source) {
    nb::set added_out;
    auto value = resolve_value_slot_const(source);
    if (value.has_value() && value->valid() && value->is_map()) {
        for (View key : value->as_map().keys()) {
            added_out.add(key.to_python());
        }
    }
    return python_set_delta(nb::frozenset(added_out), nb::frozenset(nb::set{}));
}

nb::object tsd_key_set_bridge_delta_to_python(const ViewData& previous_data, const ViewData& current_data) {
    nb::set added_set;
    nb::set removed_set;

    auto previous_value = resolve_value_slot_const(previous_data);
    auto current_value = resolve_value_slot_const(current_data);

    const bool has_previous = previous_value.has_value() && previous_value->valid() && previous_value->is_map();
    const bool has_current = current_value.has_value() && current_value->valid() && current_value->is_map();

    if (has_current) {
        auto current_map = current_value->as_map();
        if (has_previous) {
            auto previous_map = previous_value->as_map();
            for (View key : current_map.keys()) {
                if (!previous_map.contains(key)) {
                    added_set.add(key.to_python());
                }
            }
        } else {
            for (View key : current_map.keys()) {
                added_set.add(key.to_python());
            }
        }
    }

    if (has_previous) {
        auto previous_map = previous_value->as_map();
        if (has_current) {
            auto current_map = current_value->as_map();
            for (View key : previous_map.keys()) {
                if (!current_map.contains(key)) {
                    removed_set.add(key.to_python());
                }
            }
        } else {
            for (View key : previous_map.keys()) {
                removed_set.add(key.to_python());
            }
        }
    }

    return python_set_delta(nb::frozenset(added_set), nb::frozenset(removed_set));
}

nb::object tsd_key_set_unbind_delta_to_python(const ViewData& previous_data) {
    nb::set removed_set;

    auto previous_value = resolve_value_slot_const(previous_data);
    if (!(previous_value.has_value() && previous_value->valid() && previous_value->is_map())) {
        return python_set_delta(nb::frozenset(nb::set{}), nb::frozenset(removed_set));
    }

    nb::set added_on_tick;
    nb::set removed_on_tick;
    auto* delta_root = static_cast<const Value*>(previous_data.delta_data);
    if (delta_root != nullptr && delta_root->has_value()) {
        std::optional<View> maybe_delta;
        if (previous_data.path.indices.empty()) {
            maybe_delta = delta_root->view();
        } else {
            maybe_delta = navigate_const(delta_root->view(), previous_data.path.indices);
        }

        if (maybe_delta.has_value() && maybe_delta->valid() && maybe_delta->is_tuple()) {
            auto tuple = maybe_delta->as_tuple();
            if (tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set()) {
                for (View elem : tuple.at(1).as_set()) {
                    added_on_tick.add(elem.to_python());
                }
            }
            if (tuple.size() > 2 && tuple.at(2).valid() && tuple.at(2).is_set()) {
                for (View elem : tuple.at(2).as_set()) {
                    removed_on_tick.add(elem.to_python());
                }
            }
        }
    }

    // Reconstruct the key-set visible before this tick:
    // previous_keys = (current_keys - added_on_tick) U removed_on_tick.
    for (View key : previous_value->as_map().keys()) {
        nb::object py_key = key.to_python();
        if (!added_on_tick.contains(py_key)) {
            removed_set.add(py_key);
        }
    }
    for (nb::handle py_key : removed_on_tick) {
        removed_set.add(py_key);
    }

    return python_set_delta(nb::frozenset(nb::set{}), nb::frozenset(removed_set));
}

bool is_same_view_data(const ViewData& lhs, const ViewData& rhs) {
    return lhs.value_data == rhs.value_data &&
           lhs.time_data == rhs.time_data &&
           lhs.observer_data == rhs.observer_data &&
           lhs.delta_data == rhs.delta_data &&
           lhs.link_data == rhs.link_data &&
           lhs.projection == rhs.projection &&
           lhs.path.indices == rhs.path.indices;
}

// Resolve the view used for reads. For non-REF consumers, this transparently
// dereferences REF bindings so TS adapters observe concrete target values.
bool resolve_read_view_data(const ViewData& vd, const TSMeta* self_meta, ViewData& out) {
    out = vd;
    out.sampled = out.sampled || vd.sampled;

    for (size_t depth = 0; depth < 64; ++depth) {
        if (auto rebound = resolve_bound_view_data(out); rebound.has_value()) {
            const ViewData next = std::move(rebound.value());
            if (is_same_view_data(next, out)) {
                return false;
            }
            out = next;
            out.sampled = out.sampled || vd.sampled;

            // REF views expose the reference object itself. For REF consumers we
            // stop after resolving the direct bind chain.
            if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
                return true;
            }
            continue;
        }

        if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
            return true;
        }

        const TSMeta* current = meta_at_path(out.meta, out.path.indices);
        if (current == nullptr || current->kind != TSKind::REF) {
            if (auto through_ref_ancestor = resolve_ref_ancestor_descendant_view_data(out);
                through_ref_ancestor.has_value()) {
                const ViewData next = std::move(*through_ref_ancestor);
                if (is_same_view_data(next, out)) {
                    return false;
                }
                out = next;
                out.sampled = out.sampled || vd.sampled;
                continue;
            }
            return true;
        }

        if (auto ref_value = resolve_value_slot_const(out);
            ref_value.has_value() && ref_value->valid() && ref_value->schema() == ts_reference_meta()) {
            TimeSeriesReference ref = nb::cast<TimeSeriesReference>(ref_value->to_python());
            if (const ViewData* target = ref.bound_view(); target != nullptr) {
                if (std::getenv("HGRAPH_DEBUG_REF_ANCESTOR") != nullptr) {
                    std::string target_repr{"<none>"};
                    if (target->ops != nullptr && target->ops->to_python != nullptr) {
                        try {
                            target_repr = nb::cast<std::string>(nb::repr(target->ops->to_python(*target)));
                        } catch (...) {
                            target_repr = "<repr_error>";
                        }
                    }
                    std::fprintf(stderr,
                                 "[resolve_read_ref] path=%s ref_value_data=%p target_value_data=%p target_path=%s target=%s\n",
                                 out.path.to_string().c_str(),
                                 ref_value->data(),
                                 target->value_data,
                                 target->path.to_string().c_str(),
                                 target_repr.c_str());
                }
                out = *target;
                out.sampled = out.sampled || vd.sampled;
                out.projection = merge_projection(vd.projection, out.projection);
                continue;
            }

            // Empty local REF values are placeholders used by REF->REF bind.
            // Fall through to binding resolution if a link exists.
            if (!ref.is_empty()) {
                const bool self_static_container =
                    self_meta != nullptr &&
                    (self_meta->kind == TSKind::TSB ||
                     (self_meta->kind == TSKind::TSL && self_meta->fixed_size() > 0));
                if (self_static_container) {
                    // Static container consumers can be driven by unbound REF
                    // payloads (for example switch-style REF[TSB] wrappers).
                    // Keep reads on the local container view so child paths can
                    // resolve through REF descendants.
                    out = vd;
                    out.sampled = out.sampled || vd.sampled;
                    return true;
                }
                return false;
            }
        }
        return false;
    }

    return false;
}

void stamp_time_paths(ViewData& vd, engine_time_t current_time) {
    auto* time_root = static_cast<Value*>(vd.time_data);
    if (time_root == nullptr || time_root->schema() == nullptr) {
        return;
    }

    if (!time_root->has_value()) {
        time_root->emplace();
    }

    auto stamp_one_time_path = [&](const std::vector<size_t>& path) {
        std::optional<ValueView> slot;
        if (path.empty()) {
            slot = time_root->view();
        } else {
            slot = navigate_mut(time_root->view(), path);
        }
        if (!slot.has_value()) {
            return;
        }
        if (engine_time_t* et_ptr = extract_time_ptr(*slot); et_ptr != nullptr) {
            *et_ptr = current_time;
        }
    };

    for (const auto& path : time_stamp_paths_for_ts_path(vd.meta, vd.path.indices)) {
        stamp_one_time_path(path);
    }

    // For static containers (TSB, fixed-size TSL), writing a parent value should also
    // advance child timestamps so child.modified mirrors Python semantics.
    const TSMeta* target_meta = meta_at_path(vd.meta, vd.path.indices);
    if (target_meta != nullptr && (target_meta->kind == TSKind::TSB ||
                                   (target_meta->kind == TSKind::TSL && target_meta->fixed_size() > 0))) {
        std::vector<std::vector<size_t>> descendant_ts_paths;
        std::vector<size_t>              current_ts_path = vd.path.indices;
        collect_static_descendant_ts_paths(target_meta, current_ts_path, descendant_ts_paths);
        for (const auto& descendant_ts_path : descendant_ts_paths) {
            for (const auto& time_path : time_stamp_paths_for_ts_path(vd.meta, descendant_ts_path)) {
                stamp_one_time_path(time_path);
            }
        }
    }
}

void set_leaf_time_path(ViewData& vd, engine_time_t time_value) {
    auto* time_root = static_cast<Value*>(vd.time_data);
    if (time_root == nullptr || time_root->schema() == nullptr || !time_root->has_value()) {
        return;
    }

    const auto time_path = ts_path_to_time_path(vd.meta, vd.path.indices);
    std::optional<ValueView> slot;
    if (time_path.empty()) {
        slot = time_root->view();
    } else {
        slot = navigate_mut(time_root->view(), time_path);
    }
    if (!slot.has_value()) {
        return;
    }
    if (engine_time_t* et_ptr = extract_time_ptr(*slot); et_ptr != nullptr) {
        *et_ptr = time_value;
    }
}

std::optional<ValueView> resolve_value_slot_mut(ViewData& vd) {
    auto* value_root = static_cast<Value*>(vd.value_data);
    if (value_root == nullptr || value_root->schema() == nullptr) {
        return std::nullopt;
    }
    if (!value_root->has_value()) {
        value_root->emplace();
    }
    if (vd.path.indices.empty()) {
        return value_root->view();
    }
    return navigate_mut(value_root->view(), vd.path.indices);
}

std::optional<View> resolve_value_slot_const(const ViewData& vd) {
    auto* value_root = static_cast<const Value*>(vd.value_data);
    if (value_root == nullptr || !value_root->has_value()) {
        return std::nullopt;
    }
    if (vd.path.indices.empty()) {
        return value_root->view();
    }
    return navigate_const(value_root->view(), vd.path.indices);
}

bool has_local_ref_wrapper_value(const ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || current->kind != TSKind::REF) {
        return false;
    }

    auto local = resolve_value_slot_const(vd);
    if (!local.has_value() || !local->valid() || local->schema() != ts_reference_meta()) {
        return false;
    }

    TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
    return !ref.is_empty();
}

bool has_bound_ref_static_children(const ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || current->kind != TSKind::REF) {
        return false;
    }

    const TSMeta* element = current->element_ts();
    if (element == nullptr) {
        return false;
    }

    if (element->kind == TSKind::TSB && element->fields() != nullptr) {
        for (size_t i = 0; i < element->field_count(); ++i) {
            std::vector<size_t> child_path = vd.path.indices;
            child_path.push_back(i);
            if (LinkTarget* child = resolve_link_target(vd, child_path); child != nullptr && child->is_linked) {
                return true;
            }
        }
    } else if (element->kind == TSKind::TSL && element->fixed_size() > 0) {
        for (size_t i = 0; i < element->fixed_size(); ++i) {
            std::vector<size_t> child_path = vd.path.indices;
            child_path.push_back(i);
            if (LinkTarget* child = resolve_link_target(vd, child_path); child != nullptr && child->is_linked) {
                return true;
            }
        }
    }

    return false;
}

bool assign_ref_value_from_bound_static_children(ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || current->kind != TSKind::REF) {
        return false;
    }

    const TSMeta* element = current->element_ts();
    if (element == nullptr) {
        return false;
    }

    size_t child_count = 0;
    if (element->kind == TSKind::TSB && element->fields() != nullptr) {
        child_count = element->field_count();
    } else if (element->kind == TSKind::TSL && element->fixed_size() > 0) {
        child_count = element->fixed_size();
    } else {
        return false;
    }

    std::vector<TimeSeriesReference> child_refs;
    child_refs.reserve(child_count);
    bool has_non_empty_child = false;

    for (size_t i = 0; i < child_count; ++i) {
        ViewData child = vd;
        child.path.indices.push_back(i);

        TimeSeriesReference child_ref = TimeSeriesReference::make();
        if (auto bound = resolve_bound_view_data(child); bound.has_value()) {
            const TSMeta* bound_meta = meta_at_path(bound->meta, bound->path.indices);
            if (bound_meta != nullptr && bound_meta->kind == TSKind::REF) {
                View bound_value = op_value(*bound);
                if (bound_value.valid() && bound_value.schema() == ts_reference_meta()) {
                    child_ref = nb::cast<TimeSeriesReference>(bound_value.to_python());
                } else if (op_valid(*bound)) {
                    child_ref = TimeSeriesReference::make(*bound);
                }
            } else {
                child_ref = TimeSeriesReference::make(*bound);
            }
        } else {
            View child_value = op_value(child);
            if (child_value.valid()) {
                if (child_value.schema() == ts_reference_meta()) {
                    child_ref = nb::cast<TimeSeriesReference>(child_value.to_python());
                } else {
                    child_ref = TimeSeriesReference::make(child);
                }
            } else if (op_valid(child)) {
                child_ref = TimeSeriesReference::make(child);
            }
        }

        has_non_empty_child = has_non_empty_child || !child_ref.is_empty();
        child_refs.push_back(std::move(child_ref));
    }

    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || maybe_dst->schema() != ts_reference_meta()) {
        return false;
    }

    if (has_non_empty_child) {
        maybe_dst->from_python(nb::cast(TimeSeriesReference::make(std::move(child_refs))));
    } else {
        maybe_dst->from_python(nb::cast(TimeSeriesReference::make()));
    }
    return true;
}

bool assign_ref_value_from_target(ViewData& vd, const ViewData& target) {
    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || maybe_dst->schema() != ts_reference_meta()) {
        return false;
    }
    maybe_dst->from_python(nb::cast(TimeSeriesReference::make(target)));
    return true;
}

void clear_ref_value(ViewData& vd) {
    if (std::getenv("HGRAPH_DEBUG_REF_BIND_PATH") != nullptr) {
        std::fprintf(stderr,
                     "[clear_ref_value] path=%s root_reset=%d\n",
                     vd.path.to_string().c_str(),
                     vd.path.indices.empty() ? 1 : 0);
    }
    auto* value_root = static_cast<Value*>(vd.value_data);
    if (value_root == nullptr) {
        return;
    }
    if (vd.path.indices.empty()) {
        value_root->reset();
        return;
    }
    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || maybe_dst->schema() != ts_reference_meta()) {
        return;
    }
    maybe_dst->from_python(nb::cast(TimeSeriesReference::make()));
}

void clear_ref_container_ancestor_cache(ViewData& vd) {
    if (vd.path.indices.empty()) {
        return;
    }

    for (size_t depth = 0; depth < vd.path.indices.size(); ++depth) {
        std::vector<size_t> ancestor_path(vd.path.indices.begin(),
                                          vd.path.indices.begin() + static_cast<std::ptrdiff_t>(depth));
        const TSMeta* ancestor_meta = meta_at_path(vd.meta, ancestor_path);
        if (ancestor_meta == nullptr || ancestor_meta->kind != TSKind::REF) {
            continue;
        }

        const TSMeta* element_meta = ancestor_meta->element_ts();
        const bool static_ref_container =
            element_meta != nullptr &&
            (element_meta->kind == TSKind::TSB ||
             (element_meta->kind == TSKind::TSL && element_meta->fixed_size() > 0));
        if (!static_ref_container) {
            break;
        }

        ViewData ancestor = vd;
        ancestor.path.indices = std::move(ancestor_path);
        clear_ref_value(ancestor);
        break;
    }
}

const TSMeta* op_ts_meta(const ViewData& vd) {
    if (is_tsd_key_set_projection(vd)) {
        const TSMeta* source_meta = meta_at_path(vd.meta, vd.path.indices);
        if (source_meta != nullptr && source_meta->kind == TSKind::TSD && source_meta->key_type() != nullptr) {
            return TSTypeRegistry::instance().tss(source_meta->key_type());
        }
    }
    return meta_at_path(vd.meta, vd.path.indices);
}

engine_time_t op_last_modified_time(const ViewData& vd) {
    refresh_dynamic_ref_binding(vd, MIN_DT);
    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    if (debug_keyset_bridge && is_tsd_key_set_projection(vd) && vd.uses_link_target) {
        if (LinkTarget* lt = resolve_link_target(vd, vd.path.indices); lt != nullptr) {
            std::fprintf(stderr,
                         "[keyset_lmt] path=%s linked=%d prev=%d resolved=%d rebind=%lld\n",
                         vd.path.to_string().c_str(),
                         lt->is_linked ? 1 : 0,
                         lt->has_previous_target ? 1 : 0,
                         lt->has_resolved_target ? 1 : 0,
                         static_cast<long long>(lt->last_rebind_time.time_since_epoch().count()));
        } else {
            std::fprintf(stderr,
                         "[keyset_lmt] path=%s link_target=<none>\n",
                         vd.path.to_string().c_str());
        }
    }
    const engine_time_t rebind_time = rebind_time_for_view(vd);
    const engine_time_t ref_wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
    const engine_time_t base_time = std::max(rebind_time, ref_wrapper_time);

    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (debug_keyset_bridge && self_meta != nullptr && self_meta->kind == TSKind::TSS) {
        int linked = -1;
        int prev = -1;
        int resolved = -1;
        if (vd.uses_link_target) {
            if (LinkTarget* lt = resolve_link_target(vd, vd.path.indices); lt != nullptr) {
                linked = lt->is_linked ? 1 : 0;
                prev = lt->has_previous_target ? 1 : 0;
                resolved = lt->has_resolved_target ? 1 : 0;
            }
        }
        std::fprintf(stderr,
                     "[keyset_lmt] enter path=%s base=%lld rebind=%lld ref_wrapper=%lld linked=%d prev=%d resolved=%d\n",
                     vd.path.to_string().c_str(),
                     static_cast<long long>(base_time.time_since_epoch().count()),
                     static_cast<long long>(rebind_time.time_since_epoch().count()),
                     static_cast<long long>(ref_wrapper_time.time_since_epoch().count()),
                     linked,
                     prev,
                     resolved);
    }
    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
        engine_time_t out = base_time;

        auto* time_root = static_cast<const Value*>(vd.time_data);
        if (time_root != nullptr && time_root->has_value()) {
            auto time_path = ts_path_to_time_path(vd.meta, vd.path.indices);
            std::optional<View> maybe_time;
            if (time_path.empty()) {
                maybe_time = time_root->view();
            } else {
                maybe_time = navigate_const(time_root->view(), time_path);
            }
            if (maybe_time.has_value()) {
                out = std::max(extract_time_value(*maybe_time), out);
            }
        }
        return out;
    }

    if (self_meta != nullptr && self_meta->kind == TSKind::SIGNAL && vd.uses_link_target) {
        if (LinkTarget* signal_link = resolve_link_target(vd, vd.path.indices); signal_link != nullptr) {
            if (signal_link->owner_time_ptr != nullptr) {
                return std::max(base_time, *signal_link->owner_time_ptr);
            }
            return base_time;
        }
    }

    if (is_unpeered_static_container_view(vd, self_meta)) {
        engine_time_t out = base_time;
        const size_t n = static_container_child_count(self_meta);
        for (size_t i = 0; i < n; ++i) {
            ViewData child = vd;
            child.path.indices.push_back(i);
            out = std::max(out, op_last_modified_time(child));
        }
        return out;
    }

    ViewData      resolved{};
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
        return base_time;
    }
    const ViewData* data = &resolved;

    auto* time_root = static_cast<const Value*>(data->time_data);
    if (time_root == nullptr || !time_root->has_value()) {
        return base_time;
    }

    auto time_path = ts_path_to_time_path(data->meta, data->path.indices);
    std::optional<View> maybe_time;
    if (time_path.empty()) {
        maybe_time = time_root->view();
    } else {
        maybe_time = navigate_const(time_root->view(), time_path);
    }

    engine_time_t out = base_time;
    if (maybe_time.has_value()) {
        out = std::max(extract_time_value(*maybe_time), base_time);
    }

    const TSMeta* current = meta_at_path(data->meta, data->path.indices);
    if (current != nullptr && current->kind == TSKind::TSD) {
        auto value = resolve_value_slot_const(*data);
        if (value.has_value() && value->valid() && value->is_map()) {
            for_each_map_key_slot(value->as_map(), [&](View /*key*/, size_t slot) {
                ViewData child = *data;
                child.path.indices.push_back(slot);
                out = std::max(out, op_last_modified_time(child));
            });
        }
    }

    return out;
}

bool op_modified(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    const bool debug_op_modified = std::getenv("HGRAPH_DEBUG_OP_MODIFIED") != nullptr;

    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (debug_op_modified) {
        std::fprintf(stderr,
                     "[op_mod] path=%s kind=%d uses_lt=%d sampled=%d now=%lld\n",
                     vd.path.to_string().c_str(),
                     self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                     vd.uses_link_target ? 1 : 0,
                     vd.sampled ? 1 : 0,
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }
    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
        const TSMeta* element_meta = self_meta->element_ts();
        const bool static_ref_container =
            element_meta != nullptr &&
            (element_meta->kind == TSKind::TSB ||
             (element_meta->kind == TSKind::TSL && element_meta->fixed_size() > 0));
        const bool dynamic_ref_container =
            element_meta != nullptr &&
            (element_meta->kind == TSKind::TSD || element_meta->kind == TSKind::TSS);
        bool suppress_wrapper_local_time = vd.uses_link_target && dynamic_ref_container;
        bool resolved_target_modified = false;
        std::string resolved_target_path{"<none>"};
        if (vd.uses_link_target &&
            !dynamic_ref_container &&
            (!static_ref_container || !has_bound_ref_static_children(vd))) {
            ViewData resolved_target{};
            if (resolve_bound_target_view_data(vd, resolved_target) &&
                !same_view_identity(resolved_target, vd)) {
                suppress_wrapper_local_time = true;
                resolved_target_path = resolved_target.path.to_string();
                if (resolved_target.ops != nullptr && resolved_target.ops->modified != nullptr) {
                    resolved_target_modified = resolved_target.ops->modified(resolved_target, current_time);
                }
            }
        }
        if (!suppress_wrapper_local_time && vd.uses_link_target && static_ref_container) {
            if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
                link_target != nullptr && link_target->peered && !has_bound_ref_static_children(vd)) {
                // Peered REF[TSS/TSB]-style wrappers should not tick on child value updates.
                // They tick on bind/rebind, while un-peered child links drive child ticks directly.
                suppress_wrapper_local_time = true;
            }
        }
        const bool debug_ref_modified = std::getenv("HGRAPH_DEBUG_REF_MODIFIED") != nullptr;

        engine_time_t local_time = MIN_DT;
        if (auto* time_root = static_cast<const Value*>(vd.time_data);
            time_root != nullptr && time_root->has_value()) {
            auto time_path = ts_path_to_time_path(vd.meta, vd.path.indices);
            std::optional<View> maybe_time;
            if (time_path.empty()) {
                maybe_time = time_root->view();
            } else {
                maybe_time = navigate_const(time_root->view(), time_path);
            }
            if (maybe_time.has_value()) {
                local_time = extract_time_value(*maybe_time);
            }
        }

        const engine_time_t rebind_time = rebind_time_for_view(vd);
        if (debug_ref_modified) {
            int linked = -1;
            int peered = -1;
            int has_children = has_bound_ref_static_children(vd) ? 1 : 0;
            if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices); link_target != nullptr) {
                linked = link_target->is_linked ? 1 : 0;
                peered = link_target->peered ? 1 : 0;
            }
            std::fprintf(stderr,
                         "[ref_mod] path=%s sampled=%d local=%lld now=%lld rebind=%lld suppress=%d resolved_mod=%d resolved_path=%s linked=%d peered=%d has_children=%d\n",
                         vd.path.to_string().c_str(),
                         vd.sampled ? 1 : 0,
                         static_cast<long long>(local_time.time_since_epoch().count()),
                         static_cast<long long>(current_time.time_since_epoch().count()),
                         static_cast<long long>(rebind_time.time_since_epoch().count()),
                         suppress_wrapper_local_time ? 1 : 0,
                         resolved_target_modified ? 1 : 0,
                         resolved_target_path.c_str(),
                         linked,
                         peered,
                         has_children);
        }
        if (vd.sampled ||
            rebind_time == current_time ||
            resolved_target_modified ||
            (!suppress_wrapper_local_time &&
             local_time == current_time)) {
            return true;
        }

        // First bind (empty -> linked) must tick REF.modified once so callers
        // can initialize bound state. rebind_time_for_view() only tracks
        // rebinds with a previous target, so handle first-bind explicitly.
        if (vd.uses_link_target) {
            if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
                link_target != nullptr &&
                link_target->is_linked &&
                !link_target->has_previous_target) {
                if (link_target->last_rebind_time == current_time) {
                    return true;
                }
                const engine_time_t start_time = graph_start_time_for_view(vd);
                if (start_time != MIN_DT && current_time == start_time) {
                    return true;
                }
            }
        }

        if (static_ref_container) {
            const bool has_unpeered_children =
                !vd.uses_link_target || has_bound_ref_static_children(vd);
            if (has_unpeered_children) {
                const size_t n = static_container_child_count(element_meta);
                for (size_t i = 0; i < n; ++i) {
                    ViewData child = vd;
                    child.path.indices.push_back(i);
                    if (vd.uses_link_target) {
                        LinkTarget* child_link = resolve_link_target(vd, child.path.indices);
                        if (child_link == nullptr || !child_link->is_linked) {
                            continue;
                        }
                    }
                    if (op_modified(child, current_time)) {
                        return true;
                    }
                }
            }
        }

        // Type-erased REF wrappers can route concrete bindings through child[0].
        // When present, that child drives modified state.
        if (vd.uses_link_target) {
            ViewData child = vd;
            child.path.indices.push_back(0);
            if (LinkTarget* child_link = resolve_link_target(vd, child.path.indices);
                child_link != nullptr && child_link->is_linked) {
                return op_modified(child, current_time);
            }
        }

        // Dynamic REF container rebind/unbind (e.g. REF[TSD]) should surface as
        // modified on the transition tick so container adapters can emit bridge
        // deltas (add/remove snapshots) even when wrapper local time is unchanged.
        if (vd.uses_link_target && element_meta != nullptr &&
            (element_meta->kind == TSKind::TSD || element_meta->kind == TSKind::TSS)) {
            ViewData previous_bridge{};
            ViewData current_bridge{};
            if (resolve_rebind_bridge_views(vd, self_meta, current_time, previous_bridge, current_bridge)) {
                if (element_meta->kind == TSKind::TSD) {
                    auto previous_value = resolve_value_slot_const(previous_bridge);
                    auto current_value = resolve_value_slot_const(current_bridge);
                    const bool has_previous =
                        previous_value.has_value() && previous_value->valid() && previous_value->is_map();
                    const bool has_current =
                        current_value.has_value() && current_value->valid() && current_value->is_map();
                    if (has_previous || has_current) {
                        return true;
                    }
                } else {
                    auto previous_value = resolve_value_slot_const(previous_bridge);
                    auto current_value = resolve_value_slot_const(current_bridge);
                    const bool has_previous =
                        previous_value.has_value() && previous_value->valid() && previous_value->is_set();
                    const bool has_current =
                        current_value.has_value() && current_value->valid() && current_value->is_set();
                    if (has_previous || has_current) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // On static-container rebinding (for example REF[TSB] graph resets), one
    // child can rebind while siblings stay bound to the same targets. Python
    // samples the full container on that cycle, so treat siblings as modified
    // when any sibling link rebinds this tick.
    if (vd.uses_link_target && !vd.path.indices.empty()) {
        std::vector<size_t> parent_path(
            vd.path.indices.begin(),
            vd.path.indices.end() - static_cast<std::ptrdiff_t>(1));
        const TSMeta* parent_meta = meta_at_path(vd.meta, parent_path);
        const bool static_parent =
            parent_meta != nullptr &&
            (parent_meta->kind == TSKind::TSB ||
             (parent_meta->kind == TSKind::TSL && parent_meta->fixed_size() > 0));
        if (static_parent) {
            const size_t child_index = vd.path.indices.back();
            const size_t child_count = static_container_child_count(parent_meta);
            for (size_t i = 0; i < child_count; ++i) {
                if (i == child_index) {
                    continue;
                }
                std::vector<size_t> sibling_path = parent_path;
                sibling_path.push_back(i);
                if (LinkTarget* sibling_link = resolve_link_target(vd, sibling_path);
                    sibling_link != nullptr &&
                    sibling_link->is_linked &&
                    sibling_link->has_previous_target &&
                    sibling_link->last_rebind_time == current_time) {
                    return true;
                }
            }
        }
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        const bool key_set_modified = tsd_key_set_modified_this_tick(key_set_source, current_time);
        if (debug_keyset_bridge) {
            std::fprintf(stderr,
                         "[keyset_mod] direct path=%s self_kind=%d source=%s modified=%d\n",
                         vd.path.to_string().c_str(),
                         self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                         key_set_source.path.to_string().c_str(),
                         key_set_modified ? 1 : 0);
        }
        if (key_set_modified) {
            return true;
        }

        // REF[TSD] -> REF[TSS] key_set bridges can keep resolving to the
        // previous concrete TSD while the REF wrapper has already rebound to
        // empty. In that case, use bridge state to expose the rebind tick.
        const bool key_set_projection = is_tsd_key_set_projection(vd);
        if ((self_meta != nullptr && self_meta->kind == TSKind::TSS) || key_set_projection) {
            ViewData previous_bridge{};
            ViewData current_bridge{};
            if (resolve_rebind_bridge_views(vd, self_meta, current_time, previous_bridge, current_bridge)) {
                ViewData previous_source{};
                ViewData current_source{};
                const bool has_previous_source = resolve_tsd_key_set_bridge_source(previous_bridge, previous_source);
                const bool has_current_source = resolve_tsd_key_set_bridge_source(current_bridge, current_source);
                if (debug_keyset_bridge) {
                    std::fprintf(stderr,
                                 "[keyset_mod] bridge path=%s prev=%s curr=%s has_prev=%d has_curr=%d\n",
                                 vd.path.to_string().c_str(),
                                 previous_bridge.path.to_string().c_str(),
                                 current_bridge.path.to_string().c_str(),
                                 has_previous_source ? 1 : 0,
                                 has_current_source ? 1 : 0);
                }
                if (has_previous_source || has_current_source) {
                    return true;
                }
            } else {
                LinkTarget* lt = resolve_link_target(vd, vd.path.indices);
                if (debug_keyset_bridge && lt != nullptr) {
                    const bool first_bind = lt->is_linked &&
                                            !lt->has_previous_target &&
                                            lt->last_rebind_time == current_time;
                    std::fprintf(stderr,
                                 "[keyset_mod] no_bridge path=%s linked=%d prev=%d resolved=%d rebind=%lld now=%lld first_bind=%d\n",
                                 vd.path.to_string().c_str(),
                                 lt->is_linked ? 1 : 0,
                                 lt->has_previous_target ? 1 : 0,
                                 lt->has_resolved_target ? 1 : 0,
                                 static_cast<long long>(lt->last_rebind_time.time_since_epoch().count()),
                                 static_cast<long long>(current_time.time_since_epoch().count()),
                                 first_bind ? 1 : 0);
                }
                if (lt != nullptr && lt->is_linked &&
                    !lt->has_previous_target &&
                    lt->last_rebind_time == current_time) {
                    // First bind from an empty REF wrapper to a concrete TSD should
                    // tick key_set immediately even before a previous-target bridge exists.
                    if (debug_keyset_bridge) {
                        std::fprintf(stderr,
                                     "[keyset_mod] first_bind path=%s linked=%d prev=%d rebind=%lld now=%lld\n",
                                     vd.path.to_string().c_str(),
                                     lt->is_linked ? 1 : 0,
                                     lt->has_previous_target ? 1 : 0,
                                     static_cast<long long>(lt->last_rebind_time.time_since_epoch().count()),
                                     static_cast<long long>(current_time.time_since_epoch().count()));
                    }
                    return true;
                }
            }
        }
        return false;
    }

    // Explicit key_set bridges can transition between concrete TSD and empty REF.
    // Treat such rebind ticks as modified so delta adapters can emit removals/additions.
    if (self_meta != nullptr && self_meta->kind == TSKind::TSS) {
        ViewData previous_bridge{};
        ViewData current_bridge{};
        if (resolve_rebind_bridge_views(vd, self_meta, current_time, previous_bridge, current_bridge)) {
            ViewData previous_source{};
            ViewData current_source{};
            if (resolve_tsd_key_set_bridge_source(previous_bridge, previous_source) ||
                resolve_tsd_key_set_bridge_source(current_bridge, current_source)) {
                return true;
            }
        }
    }

    // REF[TSD] bridge rebind/unbind should count as modified for container
    // adapters so removal/addition deltas are emitted on the transition tick.
    if (self_meta != nullptr && self_meta->kind == TSKind::TSD) {
        ViewData previous_bridge{};
        ViewData current_bridge{};
        if (resolve_rebind_bridge_views(vd, self_meta, current_time, previous_bridge, current_bridge)) {
            auto previous_value = resolve_value_slot_const(previous_bridge);
            auto current_value = resolve_value_slot_const(current_bridge);
            const bool has_previous = previous_value.has_value() && previous_value->valid() && previous_value->is_map();
            const bool has_current = current_value.has_value() && current_value->valid() && current_value->is_map();
            if (has_previous || has_current) {
                return true;
            }
        }

        if (vd.uses_link_target) {
            if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
                link_target != nullptr &&
                link_target->is_linked &&
                !link_target->has_previous_target &&
                link_target->last_rebind_time == current_time) {
                ViewData current_view =
                    link_target->has_resolved_target ? link_target->resolved_target : link_target->as_view_data(vd.sampled);
                auto current_value = resolve_value_slot_const(current_view);
                const bool has_current =
                    current_value.has_value() && current_value->valid() && current_value->is_map();
                if (has_current) {
                    return true;
                }
            }
        }
    }

    if (self_meta != nullptr &&
        (self_meta->kind == TSKind::TSB ||
         (self_meta->kind == TSKind::TSL && self_meta->fixed_size() > 0))) {
        const size_t n = static_container_child_count(self_meta);
        for (size_t i = 0; i < n; ++i) {
            ViewData child = vd;
            child.path.indices.push_back(i);
            if (op_modified(child, current_time)) {
                return true;
            }
        }
    }

    const engine_time_t last_modified = op_last_modified_time(vd);
    if (vd.sampled || last_modified == current_time) {
        return true;
    }

    if (vd.uses_link_target) {
        if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
            link_target != nullptr &&
            link_target->has_previous_target &&
            link_target->modified(current_time) &&
            !op_valid(vd)) {
            return true;
        }
    }

    return false;
}

bool op_valid(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    const bool debug_keyset_valid = std::getenv("HGRAPH_DEBUG_KEYSET_VALID") != nullptr;
    const engine_time_t current_time = evaluation_time_for_view(vd);
    if (current_time != MIN_DT) {
        refresh_dynamic_ref_binding(vd, current_time);
    }
    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
        if (auto local = resolve_value_slot_const(vd);
            local.has_value() && local->valid() && local->schema() == ts_reference_meta()) {
            TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
            if (ref.is_empty()) {
                if (!vd.uses_link_target) {
                    return true;
                }

                // Placeholder empty wrappers on linked REF inputs should mirror
                // source REF validity (an empty REF payload is still valid).
                if (auto bound = resolve_bound_view_data(vd); bound.has_value()) {
                    if (auto source_local = resolve_value_slot_const(*bound);
                        source_local.has_value() && source_local->valid() &&
                        source_local->schema() == ts_reference_meta()) {
                        return true;
                    }
                }
            } else {
                return true;
            }
        }

        const TSMeta* element_meta = self_meta->element_ts();
        if (element_meta != nullptr &&
            (element_meta->kind == TSKind::TSB ||
             (element_meta->kind == TSKind::TSL && element_meta->fixed_size() > 0))) {
            const size_t n = static_container_child_count(element_meta);
            for (size_t i = 0; i < n; ++i) {
                ViewData child = vd;
                child.path.indices.push_back(i);
                if (op_valid(child)) {
                    return true;
                }
            }
        }

        // Type-erased REF wrappers can route concrete bindings through child[0].
        if (vd.uses_link_target) {
            ViewData child = vd;
            child.path.indices.push_back(0);
            if (LinkTarget* child_link = resolve_link_target(vd, child.path.indices);
                child_link != nullptr && child_link->is_linked) {
                return op_valid(child);
            }
        }
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        auto value = resolve_value_slot_const(key_set_source);
        if (debug_keyset_valid) {
            std::fprintf(stderr,
                         "[keyset_valid] direct path=%s source=%s has_value=%d valid=%d is_map=%d\n",
                         vd.path.to_string().c_str(),
                         key_set_source.path.to_string().c_str(),
                         value.has_value() ? 1 : 0,
                         (value.has_value() && value->valid()) ? 1 : 0,
                         (value.has_value() && value->valid() && value->is_map()) ? 1 : 0);
        }
        return value.has_value() && value->valid() && value->is_map();
    }

    if (self_meta != nullptr && self_meta->kind == TSKind::TSD && vd.uses_link_target && current_time != MIN_DT) {
        ViewData previous_bridge{};
        ViewData current_bridge{};
        if (resolve_rebind_bridge_views(vd, self_meta, current_time, previous_bridge, current_bridge)) {
            auto previous_value = resolve_value_slot_const(previous_bridge);
            auto current_value = resolve_value_slot_const(current_bridge);
            const bool has_previous = previous_value.has_value() && previous_value->valid() && previous_value->is_map();
            const bool has_current = current_value.has_value() && current_value->valid() && current_value->is_map();
            if (has_previous || has_current) {
                return true;
            }
        }
    }

    if (self_meta != nullptr && self_meta->kind == TSKind::TSS && vd.uses_link_target) {
        if (LinkTarget* lt = resolve_link_target(vd, vd.path.indices);
            lt != nullptr && lt->is_linked && lt->has_previous_target) {
            return true;
        }
    }

    if (debug_keyset_valid && self_meta != nullptr && self_meta->kind == TSKind::TSS) {
        auto bound = resolve_bound_view_data(vd);
        if (bound.has_value()) {
            const TSMeta* bound_meta = meta_at_path(bound->meta, bound->path.indices);
            auto local = resolve_value_slot_const(*bound);
            int is_empty_ref = 0;
            if (local.has_value() && local->valid() && local->schema() == ts_reference_meta()) {
                TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
                is_empty_ref = ref.is_empty() ? 1 : 0;
            }
            std::fprintf(stderr,
                         "[keyset_valid] fallback path=%s bound=%s bound_kind=%d local_ref_empty=%d\n",
                         vd.path.to_string().c_str(),
                         bound->path.to_string().c_str(),
                         bound_meta != nullptr ? static_cast<int>(bound_meta->kind) : -1,
                         is_empty_ref);
        } else {
            std::fprintf(stderr,
                         "[keyset_valid] fallback path=%s bound=<none>\n",
                         vd.path.to_string().c_str());
        }
    }

    if (self_meta != nullptr && self_meta->kind == TSKind::SIGNAL && vd.uses_link_target) {
        if (LinkTarget* signal_link = resolve_link_target(vd, vd.path.indices); signal_link != nullptr) {
            if (signal_link->owner_time_ptr != nullptr && *signal_link->owner_time_ptr > MIN_DT) {
                return true;
            }
        }
    }

    if (is_unpeered_static_container_view(vd, self_meta)) {
        const size_t n = static_container_child_count(self_meta);
        for (size_t i = 0; i < n; ++i) {
            ViewData child = vd;
            child.path.indices.push_back(i);
            if (op_valid(child)) {
                return true;
            }
        }
        return false;
    }

    ViewData      resolved{};
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
        return false;
    }
    if (self_meta != nullptr && self_meta->kind == TSKind::REF && same_view_identity(resolved, vd)) {
        return false;
    }
    const ViewData* data = &resolved;

    auto* value_root = static_cast<const Value*>(data->value_data);
    if (value_root == nullptr || !value_root->has_value()) {
        return false;
    }
    auto maybe = navigate_const(value_root->view(), data->path.indices);
    if (!maybe.has_value() || !maybe->valid()) {
        return false;
    }

    // A default-constructed value slot is not valid until the underlying view has been stamped.
    return op_last_modified_time(*data) > MIN_DT;
}

bool op_all_valid(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta != nullptr && (self_meta->kind == TSKind::TSB ||
                                 (self_meta->kind == TSKind::TSL && self_meta->fixed_size() > 0))) {
        const size_t n = static_container_child_count(self_meta);
        for (size_t i = 0; i < n; ++i) {
            ViewData child = vd;
            child.path.indices.push_back(i);
            if (!op_valid(child)) {
                return false;
            }
        }
        return true;
    }
    return op_valid(vd);
}

bool op_sampled(const ViewData& vd) {
    return vd.sampled;
}

View op_value(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
        const bool debug_ref_value = std::getenv("HGRAPH_DEBUG_REF_VALUE_PATH") != nullptr;
        const TSMeta* element_meta = self_meta->element_ts();
        const bool static_ref_container =
            element_meta != nullptr &&
            (element_meta->kind == TSKind::TSB ||
             (element_meta->kind == TSKind::TSL && element_meta->fixed_size() > 0));

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
            const ViewData& resolved = *bound;
            const bool same_view = resolved.value_data == vd.value_data &&
                                   resolved.time_data == vd.time_data &&
                                   resolved.link_data == vd.link_data &&
                                   resolved.path.indices == vd.path.indices;
            if (!same_view) {
                const TSMeta* bound_meta = meta_at_path(resolved.meta, resolved.path.indices);
                if (bound_meta != nullptr && bound_meta->kind == TSKind::REF) {
                    return op_value(resolved);
                }
            }
        }

        return {};
    }

    ViewData resolved{};
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
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

View op_delta_value(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
        return op_value(vd);
    }

    ViewData resolved{};
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
        return {};
    }
    const ViewData* data = &resolved;

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

    const TSMeta* current = meta_at_path(data->meta, data->path.indices);
    if (current != nullptr && is_scalar_like_ts_kind(current->kind)) {
        return op_value(*data);
    }

    return {};
}

bool op_has_delta(const ViewData& vd) {
    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_has_added_or_removed(key_set_source);
    }

    ViewData      resolved{};
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
        return false;
    }
    const ViewData* data = &resolved;

    const TSMeta* current = meta_at_path(data->meta, data->path.indices);

    auto* delta_root = static_cast<const Value*>(data->delta_data);
    if (delta_root != nullptr && delta_root->has_value()) {
        if (current != nullptr && (current->kind == TSKind::TSS || current->kind == TSKind::TSD)) {
            std::optional<View> maybe_delta;
            if (data->path.indices.empty()) {
                maybe_delta = delta_root->view();
            } else {
                maybe_delta = navigate_const(delta_root->view(), data->path.indices);
            }
            if (maybe_delta.has_value() && maybe_delta->valid() && maybe_delta->is_tuple()) {
                auto tuple = maybe_delta->as_tuple();
                if (current->kind == TSKind::TSS) {
                    const bool has_added = tuple.size() > 0 && tuple.at(0).valid() && tuple.at(0).is_set() &&
                                           tuple.at(0).as_set().size() > 0;
                    const bool has_removed = tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set() &&
                                             tuple.at(1).as_set().size() > 0;
                    return has_added || has_removed;
                }

                const bool has_changed = tuple.size() > 0 && tuple.at(0).valid() && tuple.at(0).is_map() &&
                                         tuple.at(0).as_map().size() > 0;
                const bool has_added = tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set() &&
                                       tuple.at(1).as_set().size() > 0;
                const bool has_removed = tuple.size() > 2 && tuple.at(2).valid() && tuple.at(2).is_set() &&
                                         tuple.at(2).as_set().size() > 0;
                return has_changed || has_added || has_removed;
            }
            return false;
        }
        return true;
    }

    if (current != nullptr &&
        (current->kind == TSKind::TSValue || current->kind == TSKind::REF || current->kind == TSKind::SIGNAL)) {
        return op_valid(*data);
    }

    return false;
}

void op_set_value(ViewData& vd, const View& src, engine_time_t current_time) {
    auto* value_root = static_cast<Value*>(vd.value_data);
    if (value_root == nullptr || value_root->schema() == nullptr) {
        return;
    }

    if (!value_root->has_value()) {
        value_root->emplace();
    }

    if (vd.path.indices.empty()) {
        if (!src.valid()) {
            value_root->reset();
        } else {
            if (value_root->schema() != src.schema()) {
                throw std::runtime_error("TS scaffolding set_value root schema mismatch");
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

void op_apply_delta(ViewData& vd, const View& delta, engine_time_t current_time) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current != nullptr &&
        (current->kind == TSKind::TSValue || current->kind == TSKind::REF || current->kind == TSKind::SIGNAL)) {
        op_set_value(vd, delta, current_time);
        return;
    }

    auto* delta_root = static_cast<Value*>(vd.delta_data);
    if (delta_root == nullptr || delta_root->schema() == nullptr) {
        return;
    }

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

void op_invalidate(ViewData& vd) {
    auto* value_root = static_cast<Value*>(vd.value_data);
    if (value_root == nullptr) {
        return;
    }

    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    const engine_time_t current_time = evaluation_time_for_view(vd);

    if (current != nullptr && current->kind == TSKind::TSD && current_time != MIN_DT) {
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

nb::object op_to_python(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
        // Python contract for REF is the reference object itself (not dereferenced payload).
        if (auto local = resolve_value_slot_const(vd);
            local.has_value() && local->valid() && local->schema() == ts_reference_meta()) {
            return local->to_python();
        }
        if (auto bound = resolve_bound_view_data(vd); bound.has_value()) {
            return nb::cast(TimeSeriesReference::make(*bound));
        }
        return nb::cast(TimeSeriesReference::make());
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_to_python(key_set_source);
    }

    const TSMeta* current = self_meta;
    if (current != nullptr && current->kind == TSKind::TSL) {
        const size_t n = op_list_size(vd);
        nb::list out;
        for (size_t i = 0; i < n; ++i) {
            ViewData child = vd;
            child.path.indices.push_back(i);
            out.append(op_valid(child) ? op_to_python(child) : nb::none());
        }
        return nb::module_::import_("builtins").attr("tuple")(out);
    }

    if (current != nullptr && current->kind == TSKind::TSB) {
        nb::dict out;
        if (current->fields() == nullptr) {
            return out;
        }
        for (size_t i = 0; i < current->field_count(); ++i) {
            ViewData child = vd;
            child.path.indices.push_back(i);
            if (!op_valid(child)) {
                continue;
            }

            const char* field_name = current->fields()[i].name;
            if (field_name == nullptr) {
                continue;
            }
            out[nb::str(field_name)] = op_to_python(child);
        }

        // Mirror Python TSB.value semantics: if schema has a scalar_type,
        // materialize that scalar instance from field values.
        try {
            nb::object schema_py = current->python_type();
            if (!schema_py.is_none()) {
                nb::object scalar_type_fn = nb::getattr(schema_py, "scalar_type", nb::none());
                if (!scalar_type_fn.is_none() && PyCallable_Check(scalar_type_fn.ptr()) != 0) {
                    nb::object scalar_type = scalar_type_fn();
                    if (!scalar_type.is_none()) {
                        PyObject* empty_args = PyTuple_New(0);
                        PyObject* scalar_obj = PyObject_Call(scalar_type.ptr(), empty_args, out.ptr());
                        Py_DECREF(empty_args);
                        if (scalar_obj != nullptr) {
                            return nb::steal<nb::object>(scalar_obj);
                        }
                        PyErr_Clear();
                    }
                }
            }
        } catch (...) {
            // Fall back to dict representation when scalar materialization fails.
        }
        return out;
    }

    if (current != nullptr && current->kind == TSKind::TSS) {
        View v = op_value(vd);
        if (v.valid() && v.is_set()) {
            return v.to_python();
        }
        return nb::frozenset(nb::set{});
    }

    if (current != nullptr && current->kind == TSKind::TSD) {
        nb::dict out;
        View v = op_value(vd);
        if (v.valid() && v.is_map()) {
            for_each_map_key_slot(v.as_map(), [&](View key, size_t slot) {
                ViewData child = vd;
                child.path.indices.push_back(slot);
                const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
                if (child_meta == nullptr) {
                    if (!op_valid(child)) {
                        return;
                    }
                    nb::object child_py = op_to_python(child);
                    if (child_py.is_none()) {
                        return;
                    }
                    out[key.to_python()] = std::move(child_py);
                    return;
                }

                if (child_meta->kind != TSKind::REF && !op_valid(child)) {
                    return;
                }

                nb::object child_py = op_to_python(child);
                if (child_meta != nullptr && child_meta->kind == TSKind::REF && child_py.is_none()) {
                    // Keep key-space stable internally, but hide unresolved REF entries
                    // from Python-facing value snapshots.
                    return;
                }
                if (child_meta->kind != TSKind::REF && child_py.is_none()) {
                    return;
                }
                out[key.to_python()] = std::move(child_py);
            });
        }
        return get_frozendict()(out);
    }

    View v = op_value(vd);
    return v.valid() ? v.to_python() : nb::none();
}

nb::object op_delta_to_python(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    const bool debug_delta_kind = std::getenv("HGRAPH_DEBUG_DELTA_KIND") != nullptr;

    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
        if (!op_modified(vd, current_time)) {
            return nb::none();
        }
        View v = op_delta_value(vd);
        return v.valid() ? v.to_python() : nb::none();
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        if (debug_delta_kind) {
            std::fprintf(stderr,
                         "[delta_kind] keyset path=%s self_kind=%d proj=%d uses_lt=%d now=%lld\n",
                         vd.path.to_string().c_str(),
                         self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                         static_cast<int>(vd.projection),
                         vd.uses_link_target ? 1 : 0,
                         static_cast<long long>(current_time.time_since_epoch().count()));
        }
        if (debug_keyset_bridge) {
            std::fprintf(stderr,
                         "[keyset_delta] direct path=%s self_kind=%d uses_lt=%d source=%s\n",
                         vd.path.to_string().c_str(),
                         self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                         vd.uses_link_target ? 1 : 0,
                         key_set_source.path.to_string().c_str());
        }
        ViewData previous_bridge{};
        ViewData current_bridge{};
        if (self_meta != nullptr &&
            resolve_rebind_bridge_views(vd, self_meta, current_time, previous_bridge, current_bridge)) {
            ViewData previous_source{};
            ViewData current_source{};
            const bool has_previous_source = resolve_tsd_key_set_bridge_source(previous_bridge, previous_source);
            const bool has_current_source = resolve_tsd_key_set_bridge_source(current_bridge, current_source);
            if (has_previous_source || has_current_source) {
                if (debug_keyset_bridge) {
                    std::fprintf(stderr,
                                 "[keyset_delta] bridge path=%s prev=%s curr=%s has_prev=%d has_curr=%d\n",
                                 vd.path.to_string().c_str(),
                                 (has_previous_source ? previous_source : previous_bridge).path.to_string().c_str(),
                                 (has_current_source ? current_source : current_bridge).path.to_string().c_str(),
                                 has_previous_source ? 1 : 0,
                                 has_current_source ? 1 : 0);
                }
                if (has_previous_source && !has_current_source) {
                    return tsd_key_set_unbind_delta_to_python(previous_source);
                }
                return tsd_key_set_bridge_delta_to_python(
                    has_previous_source ? previous_source : previous_bridge,
                    has_current_source ? current_source : current_bridge);
            }
        }

        // First bind from empty REF -> concrete TSD has no previous bridge yet.
        // Emit full "added" snapshot so key_set consumers observe immediate adds.
        const bool key_set_projection = is_tsd_key_set_projection(vd);
        if ((self_meta != nullptr && self_meta->kind == TSKind::TSS) || key_set_projection) {
            if (LinkTarget* lt = resolve_link_target(vd, vd.path.indices);
                lt != nullptr && lt->is_linked &&
                !lt->has_previous_target &&
                lt->last_rebind_time == current_time) {
                if (debug_keyset_bridge) {
                    std::fprintf(stderr,
                                 "[keyset_delta] first_bind path=%s linked=%d prev=%d rebind=%lld now=%lld\n",
                                 vd.path.to_string().c_str(),
                                 lt->is_linked ? 1 : 0,
                                 lt->has_previous_target ? 1 : 0,
                                 static_cast<long long>(lt->last_rebind_time.time_since_epoch().count()),
                                 static_cast<long long>(current_time.time_since_epoch().count()));
                }
                return tsd_key_set_all_added_to_python(key_set_source);
            }
        }

        if (!op_modified(vd, current_time)) {
            return nb::none();
        }
        return tsd_key_set_delta_to_python(key_set_source);
    }

    const bool key_set_consumer =
        self_meta != nullptr &&
        (self_meta->kind == TSKind::TSS ||
         self_meta->kind == TSKind::SIGNAL ||
         is_tsd_key_set_projection(vd));

    if (key_set_consumer) {
        ViewData previous_bridge{};
        ViewData current_bridge{};
        if (resolve_rebind_bridge_views(vd, self_meta, current_time, previous_bridge, current_bridge)) {
            ViewData previous_source{};
            ViewData current_source{};
            const bool has_previous_source = resolve_tsd_key_set_bridge_source(previous_bridge, previous_source);
            const bool has_current_source = resolve_tsd_key_set_bridge_source(current_bridge, current_source);
            if (debug_keyset_bridge) {
                std::fprintf(stderr,
                             "[keyset_delta] fallback path=%s prev_bridge=%s curr_bridge=%s has_prev=%d has_curr=%d\n",
                             vd.path.to_string().c_str(),
                             previous_bridge.path.to_string().c_str(),
                             current_bridge.path.to_string().c_str(),
                             has_previous_source ? 1 : 0,
                             has_current_source ? 1 : 0);
            }
            if (has_previous_source || has_current_source) {
                if (has_previous_source && !has_current_source) {
                    return tsd_key_set_unbind_delta_to_python(previous_source);
                }
                return tsd_key_set_bridge_delta_to_python(
                    has_previous_source ? previous_source : previous_bridge,
                    has_current_source ? current_source : current_bridge);
            }
        } else if (debug_keyset_bridge) {
            if (vd.uses_link_target) {
                if (LinkTarget* lt = resolve_link_target(vd, vd.path.indices); lt != nullptr) {
                    const TSMeta* target_meta = meta_at_path(lt->meta, lt->target_path.indices);
                    std::fprintf(stderr,
                                 "[keyset_delta] no_bridge path=%s self_kind=%d uses_lt=%d linked=%d prev=%d resolved=%d source_kind=%d last_rebind=%lld now=%lld\n",
                                 vd.path.to_string().c_str(),
                                 self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                                 vd.uses_link_target ? 1 : 0,
                                 lt->is_linked ? 1 : 0,
                                 lt->has_previous_target ? 1 : 0,
                                 lt->has_resolved_target ? 1 : 0,
                                 target_meta != nullptr ? static_cast<int>(target_meta->kind) : -1,
                                 static_cast<long long>(lt->last_rebind_time.time_since_epoch().count()),
                                 static_cast<long long>(current_time.time_since_epoch().count()));
                    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
                        for (size_t i = 0; i < 3; ++i) {
                            std::vector<size_t> probe_path = vd.path.indices;
                            probe_path.push_back(i);
                            if (LinkTarget* child_lt = resolve_link_target(vd, probe_path); child_lt != nullptr) {
                                std::fprintf(stderr,
                                             "[keyset_delta] probe_child path=%s idx=%zu child_linked=%d child_kind=%d\n",
                                             vd.path.to_string().c_str(),
                                             i,
                                             child_lt->is_linked ? 1 : 0,
                                             child_lt->meta != nullptr ? static_cast<int>(child_lt->meta->kind) : -1);
                            } else {
                                std::fprintf(stderr,
                                             "[keyset_delta] probe_child path=%s idx=%zu child=<none>\n",
                                             vd.path.to_string().c_str(),
                                             i);
                            }
                        }
                    }
                } else {
                    std::fprintf(stderr,
                                 "[keyset_delta] no_bridge path=%s self_kind=%d uses_lt=%d link_target=<none>\n",
                                 vd.path.to_string().c_str(),
                                 self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                                 vd.uses_link_target ? 1 : 0);
                }
            }
            std::fprintf(stderr,
                         "[keyset_delta] no_bridge path=%s self_kind=%d uses_lt=%d\n",
                         vd.path.to_string().c_str(),
                         self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                         vd.uses_link_target ? 1 : 0);
        }
    }

	    const auto delta_view_to_python = [current_time](const View& view) -> nb::object {
		        if (!view.valid()) {
		            return nb::none();
		        }
		        if (view.schema() == ts_reference_meta()) {
	            TimeSeriesReference ref = nb::cast<TimeSeriesReference>(view.to_python());
	            if (const ViewData* target = ref.bound_view(); target != nullptr) {
	                if (target->ops != nullptr) {
	                    if (target->ops->modified(*target, current_time)) {
	                        nb::object delta_obj = op_delta_to_python(*target, current_time);
	                        if (!delta_obj.is_none()) {
	                            return delta_obj;
	                        }
	                    }

	                    ViewData sampled_target = *target;
	                    sampled_target.sampled = true;
	                    nb::object sampled_delta = op_delta_to_python(sampled_target, current_time);
	                    if (!sampled_delta.is_none()) {
	                        return sampled_delta;
	                    }
	                }
	                return op_to_python(*target);
	            }
	            return nb::none();
	        }
		        return view.to_python();
	    };
	    const auto has_delta_payload = [](const View& view) -> bool {
	        if (!view.valid()) {
	            return false;
	        }
	        if (view.schema() == ts_reference_meta()) {
	            TimeSeriesReference ref = nb::cast<TimeSeriesReference>(view.to_python());
	            return ref.bound_view() != nullptr;
	        }
	        return true;
	    };

    if (self_meta != nullptr && (self_meta->kind == TSKind::TSS || self_meta->kind == TSKind::TSD)) {
        ViewData previous_bridge{};
        ViewData current_bridge{};
        if (resolve_rebind_bridge_views(vd, self_meta, current_time, previous_bridge, current_bridge)) {
            const TSMeta* current_bridge_meta = meta_at_path(current_bridge.meta, current_bridge.path.indices);
            const bool current_is_concrete_container =
                current_bridge_meta != nullptr && current_bridge_meta->kind == self_meta->kind;
            if (!current_is_concrete_container) {
                if (self_meta->kind == TSKind::TSS) {
                    return tss_bridge_delta_to_python(previous_bridge, current_bridge, current_time);
                }
                return tsd_bridge_delta_to_python(previous_bridge, current_bridge, current_time);
            }
        }
    }

    ViewData resolved{};
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
        return nb::none();
    }
    const ViewData* data = &resolved;

    const TSMeta* current = meta_at_path(data->meta, data->path.indices);
    if (debug_delta_kind) {
        std::fprintf(stderr,
                     "[delta_kind] path=%s self_kind=%d resolved_kind=%d self_proj=%d resolved_proj=%d uses_lt=%d now=%lld\n",
                     vd.path.to_string().c_str(),
                     self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                     current != nullptr ? static_cast<int>(current->kind) : -1,
                     static_cast<int>(vd.projection),
                     static_cast<int>(data->projection),
                     vd.uses_link_target ? 1 : 0,
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }
    if (current != nullptr && (current->kind == TSKind::TSS || current->kind == TSKind::TSD)) {
        const bool debug_tsd_bridge = std::getenv("HGRAPH_DEBUG_TSD_BRIDGE") != nullptr;
        ViewData previous_bridge{};
        ViewData current_bridge{};
        if (resolve_rebind_bridge_views(vd, current, current_time, previous_bridge, current_bridge)) {
            if (debug_tsd_bridge) {
                std::fprintf(stderr,
                             "[tsd_bridge_dbg] path=%s kind=%d prev=%s curr=%s now=%lld curr_modified=%d\n",
                             vd.path.to_string().c_str(),
                             static_cast<int>(current->kind),
                             previous_bridge.path.to_string().c_str(),
                             current_bridge.path.to_string().c_str(),
                             static_cast<long long>(current_time.time_since_epoch().count()),
                             op_modified(current_bridge, current_time) ? 1 : 0);
            }
            // Python parity: when bindings change, container REF deltas are computed
            // from full previous/current snapshots (not current native delta only).
            if (current->kind == TSKind::TSS) {
                return tss_bridge_delta_to_python(previous_bridge, current_bridge, current_time);
            }
            return tsd_bridge_delta_to_python(previous_bridge, current_bridge, current_time);
        }
    }

    if (current != nullptr && current->kind == TSKind::TSW) {
        if (!op_modified(vd, current_time)) {
            return nb::none();
        }

        if (current->is_duration_based()) {
            const engine_time_t start_time = graph_start_time_for_view(*data);
            if (start_time == MIN_DT || current_time - start_time < current->min_time_range()) {
                return nb::none();
            }
        }

        View v = op_value(*data);
        return v.valid() ? v.to_python() : nb::none();
    }

    if (current != nullptr && current->kind == TSKind::TSS) {
        const bool wrapper_modified = op_modified(vd, current_time);
        const bool resolved_modified = op_modified(*data, current_time);
        if (!wrapper_modified && !resolved_modified) {
            return nb::none();
        }

        nb::set added_set;
        nb::set removed_set;
        bool has_native_delta = false;

        auto* delta_root = static_cast<const Value*>(data->delta_data);
        if (delta_root != nullptr && delta_root->has_value()) {
            std::optional<View> maybe_delta;
            if (data->path.indices.empty()) {
                maybe_delta = delta_root->view();
            } else {
                maybe_delta = navigate_const(delta_root->view(), data->path.indices);
            }

            if (maybe_delta.has_value() && maybe_delta->valid() && maybe_delta->is_tuple()) {
                auto tuple = maybe_delta->as_tuple();
                if (tuple.size() > 0) {
                    View added = tuple.at(0);
                    if (added.valid() && added.is_set()) {
                        for (View elem : added.as_set()) {
                            added_set.add(elem.to_python());
                        }
                        has_native_delta = has_native_delta || added.as_set().size() > 0;
                    }
                }
                if (tuple.size() > 1) {
                    View removed = tuple.at(1);
                    if (removed.valid() && removed.is_set()) {
                        for (View elem : removed.as_set()) {
                            removed_set.add(elem.to_python());
                        }
                        has_native_delta = has_native_delta || removed.as_set().size() > 0;
                    }
                }
            }
        }

        bool sampled_like = data->sampled;
        if (!sampled_like && wrapper_modified && !resolved_modified) {
            sampled_like = true;
        }
        if (!sampled_like) {
            const engine_time_t wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
            if (wrapper_time == current_time && !has_native_delta) {
                sampled_like = true;
            }
        }

        if (sampled_like) {
            added_set = nb::set();
            removed_set = nb::set();
            View current_value = op_value(*data);
            if (current_value.valid()) {
                if (current_value.is_set()) {
                    for (View elem : current_value.as_set()) {
                        added_set.add(elem.to_python());
                    }
                } else if (current_value.is_map()) {
                    for (View key : current_value.as_map().keys()) {
                        added_set.add(key.to_python());
                    }
                }
            }
        }

        return python_set_delta(nb::frozenset(added_set), nb::frozenset(removed_set));
    }

    if (current != nullptr && current->kind == TSKind::TSD) {
        const bool debug_tsd_delta = std::getenv("HGRAPH_DEBUG_TSD_DELTA") != nullptr;
        const bool wrapper_modified = op_modified(vd, current_time);
        const bool resolved_modified = op_modified(*data, current_time);
        if (!wrapper_modified && !resolved_modified) {
            if (debug_tsd_delta) {
                std::fprintf(stderr,
                             "[tsd_delta_dbg] path=%s wrapper_modified=0 resolved_modified=0 now=%lld\n",
                             vd.path.to_string().c_str(),
                             static_cast<long long>(current_time.time_since_epoch().count()));
            }
            return nb::none();
        }

        nb::dict delta_out;
        View changed_values;
        View added_keys;
        View removed_keys;
        auto* delta_root = static_cast<const Value*>(data->delta_data);
        if (delta_root != nullptr && delta_root->has_value()) {
            std::optional<View> maybe_delta;
            if (data->path.indices.empty()) {
                maybe_delta = delta_root->view();
            } else {
                maybe_delta = navigate_const(delta_root->view(), data->path.indices);
            }

            if (maybe_delta.has_value() && maybe_delta->valid() && maybe_delta->is_tuple()) {
                auto tuple = maybe_delta->as_tuple();
                if (tuple.size() > 0) {
                    changed_values = tuple.at(0);
                }
                if (tuple.size() > 1) {
                    added_keys = tuple.at(1);
                }
                if (tuple.size() > 2) {
                    removed_keys = tuple.at(2);
                }
            }
        }

        auto current_value = resolve_value_slot_const(*data);
        if (current_value.has_value() && current_value->valid() && current_value->is_map()) {
            const auto value_map = current_value->as_map();
            const TSMeta* element_meta = current->element_ts();
            const bool nested_element = element_meta != nullptr && !is_scalar_like_ts_kind(element_meta->kind);

            const engine_time_t rebind_time = rebind_time_for_view(vd);
            const engine_time_t wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
            const bool has_changed_map =
                changed_values.valid() && changed_values.is_map() && changed_values.as_map().size() > 0;

            bool sampled_like = data->sampled;
            if (!sampled_like && wrapper_modified && !resolved_modified && !has_changed_map) {
                sampled_like = true;
            }
            if (!sampled_like &&
                vd.uses_link_target &&
                rebind_time == current_time) {
                if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices); link_target != nullptr) {
                    // First bind should sample current visible values and avoid carrying
                    // stale remove markers from an unrelated previous binding.
                    sampled_like = !link_target->has_previous_target;
                }
            }
            if (!sampled_like && wrapper_time == current_time && !has_changed_map) {
                sampled_like = true;
            }

            if (!sampled_like && removed_keys.valid() && removed_keys.is_set()) {
                auto set = removed_keys.as_set();
                const bool has_added_set = added_keys.valid() && added_keys.is_set();
                nb::object remove = get_remove();
                for (View key : set) {
                    bool in_added_set = has_added_set && added_keys.as_set().contains(key);
                    if (!in_added_set && has_added_set) {
                        for (View added_key : added_keys.as_set()) {
                            if (added_key.equals(key)) {
                                in_added_set = true;
                                break;
                            }
                        }
                    }
                    if (debug_tsd_delta) {
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] remove_probe path=%s key=%s has_added=%d in_added=%d\n",
                                     vd.path.to_string().c_str(),
                                     key.to_string().c_str(),
                                     has_added_set ? 1 : 0,
                                     in_added_set ? 1 : 0);
                    }
                    if (in_added_set) {
                        continue;
                    }
                    delta_out[key.to_python()] = remove;
                }
            }

            if (debug_tsd_delta) {
                size_t changed_size = 0;
                size_t added_size = 0;
                size_t removed_size = 0;
                if (changed_values.valid() && changed_values.is_map()) {
                    changed_size = changed_values.as_map().size();
                }
                if (added_keys.valid() && added_keys.is_set()) {
                    added_size = added_keys.as_set().size();
                }
                if (removed_keys.valid() && removed_keys.is_set()) {
                    removed_size = removed_keys.as_set().size();
                }
                std::fprintf(stderr,
                             "[tsd_delta_dbg] path=%s kind=%d elem=%d modified=1 sampled=%d sampled_like=%d rebind=%lld wrapper=%lld changed=%zu added=%zu removed=%zu now=%lld\n",
                             vd.path.to_string().c_str(),
                             static_cast<int>(current->kind),
                             element_meta != nullptr ? static_cast<int>(element_meta->kind) : -1,
                             data->sampled ? 1 : 0,
                             sampled_like ? 1 : 0,
                             static_cast<long long>(rebind_time.time_since_epoch().count()),
                             static_cast<long long>(wrapper_time.time_since_epoch().count()),
                             changed_size,
                             added_size,
                             removed_size,
                             static_cast<long long>(current_time.time_since_epoch().count()));
                if (added_keys.valid() && added_keys.is_set()) {
                    for (View dbg_key : added_keys.as_set()) {
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] added_key path=%s key=%s\n",
                                     vd.path.to_string().c_str(),
                                     dbg_key.to_string().c_str());
                    }
                }
                if (removed_keys.valid() && removed_keys.is_set()) {
                    for (View dbg_key : removed_keys.as_set()) {
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] removed_key path=%s key=%s\n",
                                     vd.path.to_string().c_str(),
                                     dbg_key.to_string().c_str());
                    }
                }
                if (changed_values.valid() && changed_values.is_map()) {
                    for (View dbg_key : changed_values.as_map().keys()) {
                        std::string key_s{"<key>"};
                        std::string val_s{"<value>"};
                        bool entry_valid = false;
                        bool entry_is_map = false;
                        size_t entry_map_size = 0;
                        try {
                            key_s = dbg_key.to_string();
                        } catch (...) {}
                        try {
                            View entry = changed_values.as_map().at(dbg_key);
                            entry_valid = entry.valid();
                            entry_is_map = entry.valid() && entry.is_map();
                            entry_map_size = entry_is_map ? entry.as_map().size() : 0;
                            val_s = entry.to_string();
                        } catch (...) {}
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] changed_entry path=%s key=%s valid=%d is_map=%d map_size=%zu value=%s\n",
                                     vd.path.to_string().c_str(),
                                     key_s.c_str(),
                                     entry_valid ? 1 : 0,
                                     entry_is_map ? 1 : 0,
                                     entry_map_size,
                                     val_s.c_str());
                    }
                }
            }

            if (!sampled_like && has_changed_map) {
                const auto changed_map = changed_values.as_map();
                for (View key : changed_map.keys()) {
                    auto slot = map_slot_for_key(value_map, key);
                    if (!slot.has_value()) {
                        continue;
                    }

                    ViewData child = *data;
                    child.path.indices.push_back(*slot);

                    if (nested_element) {
                        if (!op_valid(child)) {
                            continue;
                        }
                        nb::object child_delta = op_delta_to_python(child, current_time);
                        if (!child_delta.is_none()) {
                            delta_out[key.to_python()] = std::move(child_delta);
                        }
                    } else {
                        if (!op_valid(child)) {
                            continue;
                        }
                        View child_delta = op_delta_value(child);
                        if (!has_delta_payload(child_delta)) {
                            continue;
                        }
                        nb::object child_delta_py = delta_view_to_python(child_delta);
                        if (!child_delta_py.is_none()) {
                            delta_out[key.to_python()] = std::move(child_delta_py);
                        }
                    }
                }
            } else {
                const bool include_unmodified = sampled_like;
                for_each_map_key_slot(value_map, [&](View key, size_t slot) {
                    if (debug_tsd_delta) {
                        bool entry_valid = false;
                        bool entry_is_map = false;
                        size_t entry_map_size = 0;
                        std::string key_s{"<key>"};
                        std::string entry_s{"<entry>"};
                        try {
                            key_s = key.to_string();
                        } catch (...) {}
                        try {
                            View entry = value_map.at(key);
                            entry_valid = entry.valid();
                            entry_is_map = entry.valid() && entry.is_map();
                            entry_map_size = entry_is_map ? entry.as_map().size() : 0;
                            entry_s = entry.to_string();
                        } catch (...) {}
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] value_entry path=%s key=%s slot=%zu valid=%d is_map=%d map_size=%zu value=%s\n",
                                     data->path.to_string().c_str(),
                                     key_s.c_str(),
                                     slot,
                                     entry_valid ? 1 : 0,
                                     entry_is_map ? 1 : 0,
                                     entry_map_size,
                                     entry_s.c_str());
                    }
                    ViewData child = *data;
                    child.path.indices.push_back(slot);
                    child.sampled = false;
                    if (include_unmodified) {
                        if (!op_valid(child)) {
                            return;
                        }
                        const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
                        if (child_meta != nullptr && child_meta->kind == TSKind::REF) {
                            View child_value = op_value(child);
                            if (!has_delta_payload(child_value)) {
                                return;
                            }
                            nb::object entry_py = delta_view_to_python(child_value);
                            if (entry_py.is_none()) {
                                return;
                            }
                            delta_out[key.to_python()] = std::move(entry_py);
                            return;
                        }

                        ViewData sampled_child = child;
                        sampled_child.sampled = true;
                        nb::object entry_py = op_delta_to_python(sampled_child, current_time);
                        if (entry_py.is_none()) {
                            entry_py = op_to_python(child);
                        }
                        if (entry_py.is_none()) {
                            return;
                        }
                        delta_out[key.to_python()] = std::move(entry_py);
                        return;
                    }
                    if (!op_valid(child)) {
                        return;
                    }
                    if (debug_tsd_delta && !include_unmodified) {
                        const engine_time_t child_last = op_last_modified_time(child);
                        std::string key_s{"<key>"};
                        try {
                            key_s = key.to_string();
                        } catch (...) {}
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] child_probe path=%s key=%s slot=%zu last=%lld now=%lld\n",
                                     child.path.to_string().c_str(),
                                     key_s.c_str(),
                                     slot,
                                     static_cast<long long>(child_last.time_since_epoch().count()),
                                     static_cast<long long>(current_time.time_since_epoch().count()));
                    }
                    if (!include_unmodified && !op_modified(child, current_time)) {
                        return;
                    }
                    const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
                    if (child_meta != nullptr && is_scalar_like_ts_kind(child_meta->kind)) {
                        View child_delta = op_delta_value(child);
                        if (has_delta_payload(child_delta)) {
                            nb::object child_delta_py = delta_view_to_python(child_delta);
                            if (!child_delta_py.is_none()) {
                                delta_out[key.to_python()] = std::move(child_delta_py);
                            }
                        }
                    } else {
                        nb::object child_delta = op_delta_to_python(child, current_time);
                        if (!child_delta.is_none()) {
                            delta_out[key.to_python()] = std::move(child_delta);
                        }
                    }
                });
            }

        }

        if (debug_tsd_delta) {
            std::string out_repr{"<repr_error>"};
            try {
                out_repr = nb::cast<std::string>(nb::repr(delta_out));
            } catch (...) {}
            std::fprintf(stderr,
                         "[tsd_delta_dbg] final_delta path=%s out=%s\n",
                         vd.path.to_string().c_str(),
                         out_repr.c_str());
        }
        return get_frozendict()(delta_out);
    }

    if (current != nullptr && current->kind == TSKind::TSL) {
        ViewData previous_bridge{};
        ViewData current_bridge{};
        if (resolve_rebind_bridge_views(vd, self_meta, current_time, previous_bridge, current_bridge)) {
            nb::dict delta_out;
            const size_t n = op_list_size(current_bridge);
            for (size_t i = 0; i < n; ++i) {
                ViewData child = current_bridge;
                child.path.indices.push_back(i);
                if (!op_valid(child)) {
                    continue;
                }
                View child_value = op_value(child);
                if (child_value.valid()) {
                    delta_out[nb::int_(i)] = delta_view_to_python(child_value);
                }
            }
            return delta_out;
        }
    }

    if (current != nullptr && current->kind == TSKind::TSL) {
        nb::dict delta_out;
        const engine_time_t wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
        const engine_time_t rebind_time = rebind_time_for_view(vd);
        const bool wrapper_ticked =
            wrapper_time == current_time ||
            rebind_time == current_time;
        const bool debug_tsl_delta = std::getenv("HGRAPH_DEBUG_TSL_DELTA") != nullptr;
        if (debug_tsl_delta) {
            int has_bound = 0;
            int bound_kind = -1;
            ViewData bound_dbg{};
            if (resolve_bound_target_view_data(vd, bound_dbg)) {
                has_bound = 1;
                if (const TSMeta* bm = meta_at_path(bound_dbg.meta, bound_dbg.path.indices); bm != nullptr) {
                    bound_kind = static_cast<int>(bm->kind);
                }
            }
            std::fprintf(stderr,
                         "[tsl_delta] path=%s now=%lld wrapper_ticked=%d wrapper_time=%lld rebind=%lld has_bound=%d bound_kind=%d\n",
                         vd.path.to_string().c_str(),
                         static_cast<long long>(current_time.time_since_epoch().count()),
                         wrapper_ticked ? 1 : 0,
                         static_cast<long long>(wrapper_time.time_since_epoch().count()),
                         static_cast<long long>(rebind_time.time_since_epoch().count()),
                         has_bound,
                         bound_kind);
        }

        bool sample_all = wrapper_ticked;
        if (sample_all) {
            const size_t n = op_list_size(*data);
            for (size_t i = 0; i < n; ++i) {
                ViewData child = *data;
                child.path.indices.push_back(i);
                if (op_modified(child, current_time)) {
                    sample_all = false;
                    break;
                }
            }
        }

        if (sample_all) {
            const size_t n = op_list_size(*data);
            for (size_t i = 0; i < n; ++i) {
                ViewData child = *data;
                child.path.indices.push_back(i);
                if (debug_tsl_delta) {
                    std::fprintf(stderr,
                                 "[tsl_delta]  sampled_child path=%s valid=%d modified=%d\n",
                                 child.path.to_string().c_str(),
                                 op_valid(child) ? 1 : 0,
                                 op_modified(child, current_time) ? 1 : 0);
                }
                if (!op_valid(child)) {
                    continue;
                }

                ViewData sampled_child = child;
                sampled_child.sampled = true;
                nb::object child_delta = op_delta_to_python(sampled_child, current_time);
                if (child_delta.is_none()) {
                    View child_value = op_value(child);
                    if (child_value.valid()) {
                        child_delta = delta_view_to_python(child_value);
                    }
                }
                if (!child_delta.is_none()) {
                    delta_out[nb::int_(i)] = std::move(child_delta);
                }
            }
            return delta_out;
        }

        const size_t n = op_list_size(*data);
        for (size_t i = 0; i < n; ++i) {
            ViewData child = *data;
            child.path.indices.push_back(i);
            if (debug_tsl_delta) {
                std::fprintf(stderr,
                             "[tsl_delta]  child path=%s valid=%d modified=%d\n",
                             child.path.to_string().c_str(),
                             op_valid(child) ? 1 : 0,
                             op_modified(child, current_time) ? 1 : 0);
            }
            if (!op_modified(child, current_time) || !op_valid(child)) {
                continue;
            }
            View child_delta = op_delta_value(child);
            if (child_delta.valid()) {
                delta_out[nb::int_(i)] = delta_view_to_python(child_delta);
            }
        }
        return delta_out;
    }

    if (current != nullptr && current->kind == TSKind::TSB) {
        nb::dict delta_out;
        if (current->fields() == nullptr) {
            return delta_out;
        }

        ViewData previous_bridge{};
        ViewData current_bridge{};
        if (resolve_rebind_bridge_views(vd, self_meta, current_time, previous_bridge, current_bridge)) {
            const TSMeta* bridge_meta = meta_at_path(current_bridge.meta, current_bridge.path.indices);
            if (bridge_meta != nullptr && bridge_meta->kind == TSKind::TSB && bridge_meta->fields() != nullptr) {
                for (size_t i = 0; i < bridge_meta->field_count(); ++i) {
                    ViewData child = current_bridge;
                    child.path.indices.push_back(i);
                    if (!op_valid(child)) {
                        continue;
                    }

                    const char* field_name = bridge_meta->fields()[i].name;
                    if (field_name == nullptr) {
                        continue;
                    }

                    ViewData sampled_child = child;
                    sampled_child.sampled = true;
                    nb::object child_delta = op_delta_to_python(sampled_child, current_time);
                    if (child_delta.is_none()) {
                        nb::object child_value = op_to_python(child);
                        if (child_value.is_none()) {
                            continue;
                        }
                        child_delta = std::move(child_value);
                    }
                    delta_out[nb::str(field_name)] = std::move(child_delta);
                }
                if (PyDict_Size(delta_out.ptr()) == 0) {
                    return nb::none();
                }
                return delta_out;
            }
        }

        const engine_time_t wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
        const engine_time_t rebind_time = rebind_time_for_view(vd);
        const bool wrapper_ticked =
            wrapper_time == current_time ||
            rebind_time == current_time;
        bool suppress_wrapper_sampling = false;
        if (wrapper_ticked &&
            rebind_time != current_time &&
            !vd.path.indices.empty()) {
            std::vector<size_t> parent_path = vd.path.indices;
            parent_path.pop_back();
            const TSMeta* parent_meta = meta_at_path(vd.meta, parent_path);
            if (parent_meta != nullptr && parent_meta->kind == TSKind::TSD) {
                ViewData bound_target{};
                if (resolve_bound_target_view_data(vd, bound_target)) {
                    const TSMeta* bound_meta = meta_at_path(bound_target.meta, bound_target.path.indices);
                    suppress_wrapper_sampling = bound_meta != nullptr && bound_meta->kind == TSKind::REF;
                }
            }
        }
        if (std::getenv("HGRAPH_DEBUG_TSB_DELTA") != nullptr) {
            int has_bound = 0;
            int bound_kind = -1;
            ViewData bound_dbg{};
            if (resolve_bound_target_view_data(vd, bound_dbg)) {
                has_bound = 1;
                if (const TSMeta* bm = meta_at_path(bound_dbg.meta, bound_dbg.path.indices); bm != nullptr) {
                    bound_kind = static_cast<int>(bm->kind);
                }
            }
            int has_prev = 0;
            int prev_kind = -1;
            int same_prev = -1;
            ViewData prev_dbg{};
            if (resolve_previous_bound_target_view_data(vd, prev_dbg)) {
                has_prev = 1;
                if (const TSMeta* pm = meta_at_path(prev_dbg.meta, prev_dbg.path.indices); pm != nullptr) {
                    prev_kind = static_cast<int>(pm->kind);
                }
                if (has_bound) {
                    same_prev =
                        prev_dbg.value_data == bound_dbg.value_data &&
                        prev_dbg.time_data == bound_dbg.time_data &&
                        prev_dbg.observer_data == bound_dbg.observer_data &&
                        prev_dbg.delta_data == bound_dbg.delta_data &&
                        prev_dbg.link_data == bound_dbg.link_data &&
                        prev_dbg.path.indices == bound_dbg.path.indices ? 1 : 0;
                }
            }
            std::fprintf(stderr,
                         "[tsb_delta] path=%s now=%lld wrapper_ticked=%d wrapper_time=%lld rebind=%lld suppress=%d has_bound=%d bound_kind=%d has_prev=%d prev_kind=%d same_prev=%d\n",
                         vd.path.to_string().c_str(),
                         static_cast<long long>(current_time.time_since_epoch().count()),
                         wrapper_ticked ? 1 : 0,
                         static_cast<long long>(wrapper_time.time_since_epoch().count()),
                         static_cast<long long>(rebind_time.time_since_epoch().count()),
                         suppress_wrapper_sampling ? 1 : 0,
                         has_bound,
                         bound_kind,
                         has_prev,
                         prev_kind,
                         same_prev);
        }
        if (wrapper_ticked && !suppress_wrapper_sampling) {
            for (size_t i = 0; i < current->field_count(); ++i) {
                ViewData child = *data;
                child.path.indices.push_back(i);
                if (!op_valid(child)) {
                    continue;
                }

                const char* field_name = current->fields()[i].name;
                if (field_name == nullptr) {
                    continue;
                }

                ViewData sampled_child = child;
                sampled_child.sampled = true;
                nb::object child_delta = op_delta_to_python(sampled_child, current_time);
                if (child_delta.is_none()) {
                    View child_value = op_value(child);
                    if (child_value.valid()) {
                        child_delta = delta_view_to_python(child_value);
                    }
                }
                if (!child_delta.is_none()) {
                    delta_out[nb::str(field_name)] = std::move(child_delta);
                }
            }
            if (PyDict_Size(delta_out.ptr()) == 0) {
                return nb::none();
            }
            return delta_out;
        }

        for (size_t i = 0; i < current->field_count(); ++i) {
            ViewData child = *data;
            child.path.indices.push_back(i);
            if (!op_modified(child, current_time) || !op_valid(child)) {
                continue;
            }

            const char* field_name = current->fields()[i].name;
            if (field_name == nullptr) {
                continue;
            }

            const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
            if (child_meta != nullptr && is_scalar_like_ts_kind(child_meta->kind)) {
                View child_delta = op_delta_value(child);
                if (!has_delta_payload(child_delta)) {
                    continue;
                }
                nb::object child_delta_py = delta_view_to_python(child_delta);
                if (!child_delta_py.is_none()) {
                    delta_out[nb::str(field_name)] = std::move(child_delta_py);
                }
                continue;
            }

            nb::object child_delta = op_delta_to_python(child, current_time);
            if (!child_delta.is_none()) {
                delta_out[nb::str(field_name)] = std::move(child_delta);
            }
        }
        if (PyDict_Size(delta_out.ptr()) == 0) {
            return nb::none();
        }
        return delta_out;
    }

    if (current != nullptr && current->kind == TSKind::TSValue) {
        if (!op_modified(vd, current_time)) {
            return nb::none();
        }
        View v = op_delta_value(*data);
        return v.valid() ? v.to_python() : nb::none();
    }

    View v = op_delta_value(vd);
    return v.valid() ? v.to_python() : nb::none();
}

void op_from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    const bool debug_ref_from = std::getenv("HGRAPH_DEBUG_REF_FROM") != nullptr;

    if (current != nullptr && current->kind == TSKind::REF) {
        auto maybe_dst = resolve_value_slot_mut(vd);
        if (!maybe_dst.has_value()) {
            return;
        }

        nb::object normalized_src = src.is_none() ? nb::cast(TimeSeriesReference::make()) : src;
        bool same_ref_identity = false;
        TimeSeriesReference incoming_ref = TimeSeriesReference::make();
        bool incoming_ref_valid = false;

        if (maybe_dst->valid() && maybe_dst->schema() == ts_reference_meta()) {
            TimeSeriesReference existing_ref = nb::cast<TimeSeriesReference>(maybe_dst->to_python());
            incoming_ref = nb::cast<TimeSeriesReference>(normalized_src);
            incoming_ref_valid = true;
            same_ref_identity = (existing_ref == incoming_ref);
        }

        if (same_ref_identity) {
            // Python REF output semantics tick on assignment, even when the
            // reference identity is unchanged.
            stamp_time_paths(vd, current_time);
            mark_tsd_parent_child_modified(vd, current_time);
            notify_link_target_observers(vd, current_time);
            return;
        }

        if (debug_ref_from) {
            std::string in_repr{"<repr_error>"};
            try {
                in_repr = nb::cast<std::string>(nb::repr(normalized_src));
            } catch (...) {}
            std::fprintf(stderr,
                         "[ref_from] path=%s before_valid=%d same=%d in=%s\n",
                         vd.path.to_string().c_str(),
                         maybe_dst->valid() ? 1 : 0,
                         same_ref_identity ? 1 : 0,
                         in_repr.c_str());
        }

        maybe_dst->from_python(normalized_src);

        if (auto* ref_link = resolve_ref_link(vd, vd.path.indices); ref_link != nullptr) {
            unregister_ref_link_observer(*ref_link);

            bool has_bound_target = false;
            if (maybe_dst->valid() && maybe_dst->schema() == ts_reference_meta()) {
                TimeSeriesReference ref =
                    incoming_ref_valid ? incoming_ref : nb::cast<TimeSeriesReference>(maybe_dst->to_python());
                if (const ViewData* bound = ref.bound_view(); bound != nullptr) {
                    store_to_ref_link(*ref_link, *bound);
                    has_bound_target = true;
                }
            }

            if (!has_bound_target) {
                ref_link->unbind();
            }

            if (current_time != MIN_DT) {
                ref_link->last_rebind_time = current_time;
            }

            if (has_bound_target) {
                register_ref_link_observer(*ref_link);
            }
        }

        if (debug_ref_from && maybe_dst->valid()) {
            std::string out_repr{"<repr_error>"};
            try {
                out_repr = nb::cast<std::string>(nb::repr(maybe_dst->to_python()));
            } catch (...) {}
            std::fprintf(stderr,
                         "[ref_from] path=%s after=%s\n",
                         vd.path.to_string().c_str(),
                         out_repr.c_str());
        }

        stamp_time_paths(vd, current_time);
        mark_tsd_parent_child_modified(vd, current_time);
        notify_link_target_observers(vd, current_time);
        return;
    }

    if (current != nullptr && current->kind == TSKind::TSS) {
        if (vd.path.indices.empty() && src.is_none()) {
            if (auto* value_root = static_cast<Value*>(vd.value_data); value_root != nullptr) {
                value_root->reset();
            }
            if (auto* delta_root = static_cast<Value*>(vd.delta_data); delta_root != nullptr) {
                delta_root->reset();
            }
            stamp_time_paths(vd, current_time);
            notify_link_target_observers(vd, current_time);
            return;
        }

        auto maybe_set = resolve_value_slot_mut(vd);
        if (!maybe_set.has_value() || !maybe_set->valid() || !maybe_set->is_set()) {
            return;
        }

        const bool was_valid = op_valid(vd);
        auto slots = resolve_tss_delta_slots(vd);
        clear_tss_delta_if_new_tick(vd, current_time, slots);

        auto set = maybe_set->as_set();
        const value::TypeMeta* element_type = set.element_type();
        if (element_type == nullptr) {
            return;
        }

        auto apply_add = [&](const View& elem) -> bool {
            if (!elem.valid()) {
                return false;
            }
            if (!set.add(elem)) {
                return false;
            }

            if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                auto removed = slots.removed_set.as_set();
                if (removed.contains(elem)) {
                    removed.remove(elem);
                    return true;
                }
            }
            if (slots.added_set.valid() && slots.added_set.is_set()) {
                slots.added_set.as_set().add(elem);
            }
            return true;
        };

        auto apply_remove = [&](const View& elem) -> bool {
            if (!elem.valid()) {
                return false;
            }
            if (!set.remove(elem)) {
                return false;
            }

            if (slots.added_set.valid() && slots.added_set.is_set()) {
                auto added = slots.added_set.as_set();
                if (added.contains(elem)) {
                    added.remove(elem);
                    return true;
                }
            }
            if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                slots.removed_set.as_set().add(elem);
            }
            return true;
        };

        auto apply_add_object = [&](const nb::object& obj) -> bool {
            auto maybe_value = value_from_python(element_type, obj);
            if (!maybe_value.has_value()) {
                return false;
            }
            return apply_add(maybe_value->view());
        };

        auto apply_remove_object = [&](const nb::object& obj) -> bool {
            auto maybe_value = value_from_python(element_type, obj);
            if (!maybe_value.has_value()) {
                return false;
            }
            return apply_remove(maybe_value->view());
        };

        bool changed = false;
        bool handled = false;

        nb::object added_attr = attr_or_call(src, "added");
        nb::object removed_attr = attr_or_call(src, "removed");
        if (!added_attr.is_none() || !removed_attr.is_none()) {
            handled = true;
            if (!removed_attr.is_none()) {
                for (const auto& item : nb::iter(removed_attr)) {
                    changed = apply_remove_object(nb::cast<nb::object>(item)) || changed;
                }
            }
            if (!added_attr.is_none()) {
                for (const auto& item : nb::iter(added_attr)) {
                    changed = apply_add_object(nb::cast<nb::object>(item)) || changed;
                }
            }
        }

        if (!handled && nb::isinstance<nb::dict>(src)) {
            nb::dict as_dict = nb::cast<nb::dict>(src);
            if (as_dict.contains("added") || as_dict.contains("removed")) {
                handled = true;
                if (as_dict.contains("removed")) {
                    for (const auto& item : nb::iter(as_dict["removed"])) {
                        changed = apply_remove_object(nb::cast<nb::object>(item)) || changed;
                    }
                }
                if (as_dict.contains("added")) {
                    for (const auto& item : nb::iter(as_dict["added"])) {
                        changed = apply_add_object(nb::cast<nb::object>(item)) || changed;
                    }
                }
            }
        }

        if (!handled && nb::isinstance<nb::frozenset>(src)) {
            handled = true;

            std::vector<Value> target_values;
            for (const auto& item : nb::iter(src)) {
                auto maybe_value = value_from_python(element_type, nb::cast<nb::object>(item));
                if (maybe_value.has_value()) {
                    target_values.emplace_back(std::move(*maybe_value));
                }
            }

            std::vector<Value> existing_values;
            existing_values.reserve(set.size());
            for (View elem : set) {
                existing_values.emplace_back(elem.clone());
            }

            for (const auto& elem : existing_values) {
                bool keep = false;
                for (const auto& target : target_values) {
                    if (target.view().schema() == elem.view().schema() && target.view().equals(elem.view())) {
                        keep = true;
                        break;
                    }
                }
                if (!keep) {
                    changed = apply_remove(elem.view()) || changed;
                }
            }

            for (const auto& target : target_values) {
                changed = apply_add(target.view()) || changed;
            }
        }

        if (!handled) {
            nb::object removed_cls = get_removed();
            for (const auto& item : nb::iter(src)) {
                nb::object obj = nb::cast<nb::object>(item);
                if (nb::isinstance(obj, removed_cls)) {
                    changed = apply_remove_object(nb::cast<nb::object>(obj.attr("item"))) || changed;
                } else {
                    changed = apply_add_object(obj) || changed;
                }
            }
        }

        if (changed || !was_valid) {
            stamp_time_paths(vd, current_time);
            notify_link_target_observers(vd, current_time);
        }
        return;
    }

    if (current != nullptr && current->kind == TSKind::TSL) {
        if (vd.path.indices.empty() && src.is_none()) {
            if (auto* value_root = static_cast<Value*>(vd.value_data); value_root != nullptr) {
                value_root->reset();
            }
            if (auto* delta_root = static_cast<Value*>(vd.delta_data); delta_root != nullptr) {
                delta_root->reset();
            }
            stamp_time_paths(vd, current_time);
            notify_link_target_observers(vd, current_time);
            return;
        }

        bool changed = false;
        const size_t child_count = op_list_size(vd);

        auto apply_child = [&](size_t index, const nb::object& child_obj) {
            if (child_obj.is_none()) {
                return;
            }
            if (current->fixed_size() > 0 && index >= child_count) {
                return;
            }
            ViewData child_vd = vd;
            child_vd.path.indices.push_back(index);
            const engine_time_t before = op_last_modified_time(child_vd);
            op_from_python(child_vd, child_obj, current_time);
            changed = changed || (op_last_modified_time(child_vd) > before);
        };

        bool handled = false;
        if (nb::isinstance<nb::dict>(src)) {
            handled = true;
            nb::dict mapping = nb::cast<nb::dict>(src);
            for (const auto& kv : mapping) {
                ssize_t index = nb::cast<ssize_t>(nb::cast<nb::object>(kv.first));
                if (index < 0) {
                    continue;
                }
                apply_child(static_cast<size_t>(index), nb::cast<nb::object>(kv.second));
            }
        } else if (nb::isinstance<nb::list>(src) || nb::isinstance<nb::tuple>(src)) {
            handled = true;
            if (current->fixed_size() > 0) {
                const size_t provided_size = static_cast<size_t>(nb::len(src));
                if (provided_size != child_count) {
                    throw nb::value_error(
                        fmt::format("Expected {} elements, got {}", child_count, provided_size).c_str());
                }
            }
            size_t index = 0;
            for (const auto& item : nb::iter(src)) {
                apply_child(index++, nb::cast<nb::object>(item));
            }
        }

        if (!handled) {
            auto maybe_dst = resolve_value_slot_mut(vd);
            if (!maybe_dst.has_value()) {
                return;
            }
            maybe_dst->from_python(src);
            stamp_time_paths(vd, current_time);
            notify_link_target_observers(vd, current_time);
            return;
        }

        if (changed) {
            notify_link_target_observers(vd, current_time);
        }
        return;
    }

    if (current != nullptr && current->kind == TSKind::TSB) {
        if (vd.path.indices.empty() && src.is_none()) {
            if (auto* value_root = static_cast<Value*>(vd.value_data); value_root != nullptr) {
                value_root->reset();
            }
            if (auto* delta_root = static_cast<Value*>(vd.delta_data); delta_root != nullptr) {
                delta_root->reset();
            }
            stamp_time_paths(vd, current_time);
            notify_link_target_observers(vd, current_time);
            return;
        }

        bool changed = false;
        auto apply_child = [&](size_t index, const nb::object& child_obj) {
            if (child_obj.is_none()) {
                return;
            }
            ViewData child_vd = vd;
            child_vd.path.indices.push_back(index);
            if (debug_ref_from) {
                const TSMeta* child_meta = meta_at_path(child_vd.meta, child_vd.path.indices);
                std::fprintf(stderr,
                             "[tsb_from] child path=%s kind=%d\n",
                             child_vd.path.to_string().c_str(),
                             child_meta != nullptr ? static_cast<int>(child_meta->kind) : -1);
            }
            const engine_time_t before = op_last_modified_time(child_vd);
            op_from_python(child_vd, child_obj, current_time);
            changed = changed || (op_last_modified_time(child_vd) > before);
        };

        bool handled = false;
        if (current->fields() != nullptr) {
            nb::object item_attr = nb::getattr(src, "items", nb::none());
            if (!item_attr.is_none()) {
                handled = true;
                nb::iterator items = nb::iter(item_attr());
                for (const auto& kv : items) {
                    std::string field_name = nb::cast<std::string>(nb::cast<nb::object>(kv[0]));
                    const size_t index = find_bundle_field_index(current, field_name);
                    if (index == static_cast<size_t>(-1)) {
                        continue;
                    }
                    if (debug_ref_from) {
                        std::string v_repr{"<repr_error>"};
                        try {
                            v_repr = nb::cast<std::string>(nb::repr(nb::cast<nb::object>(kv[1])));
                        } catch (...) {}
                        std::fprintf(stderr,
                                     "[tsb_from] path=%s field=%s idx=%zu v=%s\n",
                                     vd.path.to_string().c_str(),
                                     field_name.c_str(),
                                     index,
                                     v_repr.c_str());
                    }
                    apply_child(index, nb::cast<nb::object>(kv[1]));
                }
            } else {
                for (size_t i = 0; i < current->field_count(); ++i) {
                    const char* field_name = current->fields()[i].name;
                    if (field_name == nullptr) {
                        continue;
                    }
                    nb::object child_obj = nb::getattr(src, field_name, nb::none());
                    if (!child_obj.is_none()) {
                        handled = true;
                        apply_child(i, child_obj);
                    }
                }
            }
        }

        if (!handled) {
            auto maybe_dst = resolve_value_slot_mut(vd);
            if (!maybe_dst.has_value()) {
                return;
            }
            maybe_dst->from_python(src);
            stamp_time_paths(vd, current_time);
            notify_link_target_observers(vd, current_time);
            return;
        }

        if (changed) {
            notify_link_target_observers(vd, current_time);
        }
        return;
    }

    if (current != nullptr && current->kind == TSKind::TSD) {
        if (vd.path.indices.empty() && src.is_none()) {
            if (auto* value_root = static_cast<Value*>(vd.value_data); value_root != nullptr) {
                value_root->reset();
            }
            if (auto* delta_root = static_cast<Value*>(vd.delta_data); delta_root != nullptr) {
                delta_root->reset();
            }
            stamp_time_paths(vd, current_time);
            notify_link_target_observers(vd, current_time);
            return;
        }

        auto maybe_dst = resolve_value_slot_mut(vd);
        if (!maybe_dst.has_value() || !maybe_dst->valid() || !maybe_dst->is_map()) {
            return;
        }

        const bool was_valid = op_valid(vd);
        auto dst_map = maybe_dst->as_map();
        auto slots = resolve_tsd_delta_slots(vd);
        clear_tsd_delta_if_new_tick(vd, current_time, slots);

        nb::object item_attr = nb::getattr(src, "items", nb::none());
        nb::iterator items = item_attr.is_none() ? nb::iter(src) : nb::iter(item_attr());

        const value::TypeMeta* key_type = current->key_type();
        const value::TypeMeta* value_type = current->element_ts() != nullptr ? current->element_ts()->value_type : nullptr;
        nb::object remove = get_remove();
        nb::object remove_if_exists = get_remove_if_exists();
        bool changed = false;

        for (const auto& kv : items) {
            value::Value key_value(key_type);
            key_value.emplace();
            key_type->ops().from_python(key_value.data(), kv[0], key_type);
            View key = key_value.view();

            nb::object value_obj = kv[1];
            if (value_obj.is_none()) {
                continue;
            }
            if (std::getenv("HGRAPH_DEBUG_TSD_FROM") != nullptr) {
                std::string key_s = nb::cast<std::string>(nb::repr(kv[0]));
                std::string val_s = nb::cast<std::string>(nb::repr(value_obj));
                const char* node_name = "<none>";
                if (vd.path.node != nullptr) {
                    node_name = vd.path.node->signature().name.c_str();
                }
                std::fprintf(stderr,
                             "[tsd_from] node=%s path=%s key=%s value=%s now=%lld\n",
                             node_name,
                             vd.path.to_string().c_str(),
                             key_s.c_str(),
                             val_s.c_str(),
                             static_cast<long long>(current_time.time_since_epoch().count()));
            }

            const bool is_remove = value_obj.is(remove);
            const bool is_remove_if_exists = value_obj.is(remove_if_exists);
            if (is_remove || is_remove_if_exists) {
                const bool existed = dst_map.contains(key);
                if (!existed) {
                    if (is_remove) {
                        throw nb::key_error("TSD key not found for REMOVE");
                    }
                    continue;
                }

                const auto removed_slot = map_slot_for_key(dst_map, key);
                bool removed_was_visible = false;
                if (removed_slot.has_value()) {
                    ViewData child_vd = vd;
                    child_vd.path.indices.push_back(*removed_slot);
                    View child_value = op_value(child_vd);
                    if (child_value.valid()) {
                        if (child_value.schema() == ts_reference_meta()) {
                            try {
                                TimeSeriesReference ref = nb::cast<TimeSeriesReference>(child_value.to_python());
                                removed_was_visible = !ref.is_empty();
                            } catch (...) {
                                removed_was_visible = true;
                            }
                        } else {
                            removed_was_visible = op_valid(child_vd);
                        }
                    }
                }

                bool added_this_cycle = false;
                if (slots.added_set.valid() && slots.added_set.is_set()) {
                    auto added = slots.added_set.as_set();
                    added_this_cycle = added.contains(key);
                    if (added_this_cycle) {
                        added.remove(key);
                    }
                }

                dst_map.remove(key);
                changed = true;
                if (removed_slot.has_value()) {
                    compact_tsd_child_time_slot(vd, *removed_slot);
                    compact_tsd_child_delta_slot(vd, *removed_slot);
                    compact_tsd_child_link_slot(vd, *removed_slot);
                }

                if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                    slots.changed_values_map.as_map().remove(key);
                }

                if (!added_this_cycle && removed_was_visible &&
                    slots.removed_set.valid() && slots.removed_set.is_set()) {
                    slots.removed_set.as_set().add(key);
                }
                continue;
            }

            if (value_type == nullptr) {
                continue;
            }

            const TSMeta* element_meta = current->element_ts();
            const bool scalar_like_element = element_meta == nullptr || is_scalar_like_ts_kind(element_meta->kind);

            if (scalar_like_element) {
                value::Value value_value(value_type);
                value_value.emplace();
                value_type->ops().from_python(value_value.data(), value_obj, value_type);

                const bool existed = dst_map.contains(key);
                dst_map.set(key, value_value.view());
                changed = true;

                // Scalar-like TSD children do not recurse through op_from_python, so
                // stamp their child time path explicitly to keep child valid/modified
                // semantics aligned with Python runtime views.
                auto slot = map_slot_for_key(dst_map, key);
                if (slot.has_value()) {
                    ensure_tsd_child_time_slot(vd, *slot);
                    ensure_tsd_child_link_slot(vd, *slot);
                    ViewData child_vd = vd;
                    child_vd.path.indices.push_back(*slot);
                    stamp_time_paths(child_vd, current_time);
                }

                if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                    slots.changed_values_map.as_map().set(key, value_value.view());
                }

                if (!existed && slots.added_set.valid() && slots.added_set.is_set()) {
                    slots.added_set.as_set().add(key);
                }

                if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                    slots.removed_set.as_set().remove(key);
                }
                continue;
            }

            const bool existed = dst_map.contains(key);
            if (!existed) {
                // Create a default child slot, then apply child delta/value recursively.
                value::Value blank_value(value_type);
                blank_value.emplace();
                dst_map.set(key, blank_value.view());
                if (slots.added_set.valid() && slots.added_set.is_set()) {
                    slots.added_set.as_set().add(key);
                }
            }

            auto slot = map_slot_for_key(dst_map, key);
            if (!slot.has_value()) {
                continue;
            }

            ensure_tsd_child_time_slot(vd, *slot);
            ensure_tsd_child_delta_slot(vd, *slot);
            ensure_tsd_child_link_slot(vd, *slot);
            ViewData child_vd = vd;
            child_vd.path.indices.push_back(*slot);
            const engine_time_t before = op_last_modified_time(child_vd);
            op_from_python(child_vd, value_obj, current_time);
            const bool child_changed = op_last_modified_time(child_vd) > before;

            if (child_changed || !existed) {
                changed = true;
                if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                    View child_value = op_value(child_vd);
                    if (child_value.valid()) {
                        slots.changed_values_map.as_map().set(key, child_value);
                    }
                }
            }

            if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                slots.removed_set.as_set().remove(key);
            }
        }

        if (changed || !was_valid) {
            stamp_time_paths(vd, current_time);
            notify_link_target_observers(vd, current_time);
        }
        return;
    }

    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value()) {
        return;
    }

    if (vd.path.indices.empty() && src.is_none()) {
        auto* value_root = static_cast<Value*>(vd.value_data);
        if (value_root != nullptr) {
            value_root->reset();
            stamp_time_paths(vd, current_time);
            notify_link_target_observers(vd, current_time);
        }
        return;
    }

    if (src.is_none()) {
        // Non-root TS assignments of None invalidate the leaf while still
        // ticking parent containers in this cycle.
        maybe_dst->from_python(src);
        stamp_time_paths(vd, current_time);
        set_leaf_time_path(vd, MIN_DT);
        mark_tsd_parent_child_modified(vd, current_time);
        notify_link_target_observers(vd, current_time);
        return;
    }

    maybe_dst->from_python(src);
    stamp_time_paths(vd, current_time);
    mark_tsd_parent_child_modified(vd, current_time);
    notify_link_target_observers(vd, current_time);
}

TSView op_child_at(const ViewData& vd, size_t index, engine_time_t current_time) {
    ViewData child = vd;
    child.path.indices.push_back(index);
    return TSView(child, current_time);
}

TSView op_child_by_name(const ViewData& vd, std::string_view name, engine_time_t current_time) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || current->kind != TSKind::TSB || current->fields() == nullptr) {
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
    if (current == nullptr || current->kind != TSKind::TSL) {
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
    if (current == nullptr || current->kind != TSKind::TSB) {
        return 0;
    }
    return current->field_count();
}
View op_observer(const ViewData& vd) {
    auto* observer_root = static_cast<const Value*>(vd.observer_data);
    if (observer_root == nullptr || !observer_root->has_value()) {
        return {};
    }

    const auto observer_path = ts_path_to_time_path(vd.meta, vd.path.indices);
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
    const bool signal_descendant_bind = used_ancestor_meta && current != nullptr && current->kind == TSKind::SIGNAL;
    const bool current_is_ref_value =
        current != nullptr &&
        current->kind == TSKind::TSValue &&
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
        unregister_link_target_observer(*link_target);
        link_target->owner_time_ptr = resolve_owner_time_ptr(vd);
        link_target->parent_link = resolve_parent_link_target(vd);
        // Non-REF scalar consumers bound to REF wrappers are driven by
        // resolved-target writes/rebind ticks. REF consumers must still observe
        // REF wrapper writes so empty->bound transitions propagate (for example
        // nested tsd_get_items REF->REF chains). Static container consumers
        // (TSB/fixed TSL) also need wrapper writes because unbound REF static
        // payloads are surfaced through wrapper-local state.
        const bool target_is_ref_wrapper = target_meta != nullptr && target_meta->kind == TSKind::REF;
        const bool observer_is_ref_wrapper = current != nullptr && current->kind == TSKind::REF;
        const bool observer_is_static_container =
            current != nullptr &&
            (current->kind == TSKind::TSB ||
             (current->kind == TSKind::TSL && current->fixed_size() > 0));
        link_target->notify_on_ref_wrapper_write =
            !target_is_ref_wrapper || observer_is_ref_wrapper || observer_is_static_container;
        if (debug_op_bind) {
            std::fprintf(stderr,
                         "[op_bind]  lt=%p notify_on_ref_wrapper_write=%d\n",
                         static_cast<void*>(link_target),
                         link_target->notify_on_ref_wrapper_write ? 1 : 0);
        }

        if (signal_descendant_bind) {
            if (!link_target->is_linked) {
                link_target->bind(target, current_time);
            } else {
                const ViewData normalized_target = [&target]() {
                    ViewData out = target;
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

        link_target->bind(target, current_time);
        refresh_dynamic_ref_binding(vd, current_time);
        register_link_target_observer(*link_target);

        // TSB root bind is peered (container slot). Field binds are un-peered.
        link_target->peered = (current != nullptr && current->kind == TSKind::TSB);

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
            const bool non_peer_parent = parent_meta != nullptr &&
                                         (parent_meta->kind == TSKind::TSB ||
                                          (parent_meta->kind == TSKind::TSL && parent_meta->fixed_size() > 0));
            if (non_peer_parent) {
                if (LinkTarget* parent_link = resolve_link_target(vd, parent_path); parent_link != nullptr) {
                    if (parent_meta->kind == TSKind::TSB) {
                        // Any direct child bind transitions TSB from peered to un-peered.
                        unregister_link_target_observer(*parent_link);
                        parent_link->unbind();
                        parent_link->peered = false;
                    } else {
                        bool keep_parent_bound = false;

                        const bool static_container_parent =
                            parent_link->is_linked &&
                            parent_meta->kind == TSKind::TSL &&
                            parent_meta->fixed_size() > 0;

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
                                !(parent_target_meta != nullptr && parent_target_meta->kind == TSKind::REF);
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

        if (current != nullptr && current->kind == TSKind::REF) {
            if (target_meta != nullptr && target_meta->kind != TSKind::REF) {
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
        unregister_ref_link_observer(*ref_link);
        store_to_ref_link(*ref_link, target);
        if (current_time != MIN_DT) {
            ref_link->last_rebind_time = current_time;
        }
        register_ref_link_observer(*ref_link);

        if (current != nullptr && current->kind == TSKind::REF) {
            const TSMeta* target_meta = meta_at_path(target.meta, target.path.indices);
            if (target_meta != nullptr && target_meta->kind != TSKind::REF) {
                assign_ref_value_from_target(vd, target);
            } else {
                clear_ref_value(vd);
            }
        }
    }

    if (current != nullptr && current->kind != TSKind::REF) {
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
        unregister_link_target_observer(*lt);
        lt->unbind(current_time);
        lt->peered = false;
    } else {
        auto* ref_link = resolve_ref_link(vd, vd.path.indices);
        if (ref_link == nullptr) {
            return;
        }
        unregister_ref_link_observer(*ref_link);
        ref_link->unbind();
        if (current_time != MIN_DT) {
            ref_link->last_rebind_time = current_time;
        }
    }

    if (current != nullptr && current->kind == TSKind::REF) {
        clear_ref_value(vd);
    } else {
        clear_ref_container_ancestor_cache(vd);
    }
}

bool op_is_bound(const ViewData& vd) {
    if (vd.uses_link_target) {
        ViewData bound{};
        if (resolve_bound_target_view_data(vd, bound)) {
            return true;
        }
        if (LinkTarget* payload = resolve_link_target(vd, vd.path.indices); payload != nullptr) {
            return payload->is_linked;
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
                                   current != nullptr &&
                                   current->kind == TSKind::REF &&
                                   has_local_ref_wrapper_value(vd);
    const bool ref_static_children = active &&
                                     current != nullptr &&
                                     current->kind == TSKind::REF &&
                                     has_bound_ref_static_children(vd);
    if (vd.uses_link_target) {
        if (LinkTarget* payload = resolve_link_target(vd, vd.path.indices); payload != nullptr) {
            payload->active_notifier = active ? input : nullptr;
            if (ref_local_wrapper && input != nullptr) {
                const engine_time_t current_time = resolve_input_current_time(input);
                if (current_time != MIN_DT) {
                    input->notify(current_time);
                }
            }

            if (ref_static_children) {
                const TSMeta* element_meta = current != nullptr ? current->element_ts() : nullptr;
                size_t child_count = 0;
                if (element_meta != nullptr) {
                    if (element_meta->kind == TSKind::TSB && element_meta->fields() != nullptr) {
                        child_count = element_meta->field_count();
                    } else if (element_meta->kind == TSKind::TSL && element_meta->fixed_size() > 0) {
                        child_count = element_meta->fixed_size();
                    }
                }

                for (size_t i = 0; i < child_count; ++i) {
                    std::vector<size_t> child_path = vd.path.indices;
                    child_path.push_back(i);
                    if (LinkTarget* child_link = resolve_link_target(vd, child_path); child_link != nullptr) {
                        child_link->active_notifier = active ? input : nullptr;
                        if (active) {
                            notify_activation_if_modified(child_link, input);
                        }
                    }
                }

                if (input != nullptr) {
                    const engine_time_t current_time = resolve_input_current_time(input);
                    if (current_time != MIN_DT) {
                        input->notify(current_time);
                    }
                }
                payload->active_notifier = nullptr;
                return;
            }

            if (!payload->is_linked) {
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
            payload->active_notifier = active ? input : nullptr;
            if (active) {
                notify_activation_if_modified(payload, input);
            }
        }
    }
}

const engine_time_t* op_window_value_times(const ViewData& vd) {
    (void)vd;
    return nullptr;
}

size_t op_window_value_times_count(const ViewData& vd) {
    (void)vd;
    return 0;
}

engine_time_t op_window_first_modified_time(const ViewData& vd) {
    (void)vd;
    return MIN_DT;
}

bool op_window_has_removed_value(const ViewData& vd) {
    (void)vd;
    return false;
}

View op_window_removed_value(const ViewData& vd) {
    (void)vd;
    return {};
}

size_t op_window_removed_value_count(const ViewData& vd) {
    (void)vd;
    return 0;
}

size_t op_window_size(const ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || current->kind != TSKind::TSW || current->is_duration_based()) {
        return 0;
    }
    return current->period();
}

size_t op_window_min_size(const ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || current->kind != TSKind::TSW || current->is_duration_based()) {
        return 0;
    }
    return current->min_period();
}

size_t op_window_length(const ViewData& vd) {
    View v = op_value(vd);
    if (!v.valid()) {
        return 0;
    }
    if (v.is_cyclic_buffer()) {
        return v.as_cyclic_buffer().size();
    }
    if (v.is_queue()) {
        return v.as_queue().size();
    }
    return 0;
}

bool op_set_add(ViewData& vd, const View& elem, engine_time_t current_time) {
    if (!elem.valid()) {
        return false;
    }
    auto maybe_set = resolve_value_slot_mut(vd);
    if (!maybe_set.has_value() || !maybe_set->valid() || !maybe_set->is_set()) {
        return false;
    }

    auto set = maybe_set->as_set();
    const bool added = set.add(elem);
    if (added) {
        stamp_time_paths(vd, current_time);
    }
    return added;
}

bool op_set_remove(ViewData& vd, const View& elem, engine_time_t current_time) {
    if (!elem.valid()) {
        return false;
    }
    auto maybe_set = resolve_value_slot_mut(vd);
    if (!maybe_set.has_value() || !maybe_set->valid() || !maybe_set->is_set()) {
        return false;
    }

    auto set = maybe_set->as_set();
    const bool removed = set.remove(elem);
    if (removed) {
        stamp_time_paths(vd, current_time);
    }
    return removed;
}

void op_set_clear(ViewData& vd, engine_time_t current_time) {
    auto maybe_set = resolve_value_slot_mut(vd);
    if (!maybe_set.has_value() || !maybe_set->valid() || !maybe_set->is_set()) {
        return;
    }

    auto set = maybe_set->as_set();
    if (set.size() == 0) {
        return;
    }
    set.clear();
    stamp_time_paths(vd, current_time);
}

bool op_dict_remove(ViewData& vd, const View& key, engine_time_t current_time) {
    if (!key.valid()) {
        return false;
    }
    auto maybe_map = resolve_value_slot_mut(vd);
    if (!maybe_map.has_value() || !maybe_map->valid() || !maybe_map->is_map()) {
        return false;
    }

    auto map = maybe_map->as_map();
    const auto removed_slot = map_slot_for_key(map, key);
    if (!removed_slot.has_value()) {
        return false;
    }
    ViewData child_vd = vd;
    child_vd.path.indices.push_back(*removed_slot);
    const bool removed_was_valid = op_valid(child_vd);

    auto slots = resolve_tsd_delta_slots(vd);
    clear_tsd_delta_if_new_tick(vd, current_time, slots);

    bool added_this_cycle = false;
    if (slots.added_set.valid() && slots.added_set.is_set()) {
        auto added = slots.added_set.as_set();
        added_this_cycle = added.contains(key);
        if (added_this_cycle) {
            added.remove(key);
        }
    }

    if (!map.remove(key)) {
        return false;
    }

    compact_tsd_child_time_slot(vd, *removed_slot);
    compact_tsd_child_delta_slot(vd, *removed_slot);
    compact_tsd_child_link_slot(vd, *removed_slot);

    if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
        slots.changed_values_map.as_map().remove(key);
    }

    if (!added_this_cycle &&
        slots.removed_set.valid() && slots.removed_set.is_set()) {
        slots.removed_set.as_set().add(key);
        if (!removed_was_valid &&
            slots.added_set.valid() && slots.added_set.is_set()) {
            // Marker: removed from key-space without a visible value removal.
            slots.added_set.as_set().add(key);
        }
    }

    stamp_time_paths(vd, current_time);
    notify_link_target_observers(vd, current_time);
    return true;
}

TSView op_dict_create(ViewData& vd, const View& key, engine_time_t current_time) {
    const bool debug_create = std::getenv("HGRAPH_DEBUG_TSD_CREATE_CORE") != nullptr;
    if (!key.valid()) {
        if (debug_create) {
            std::fprintf(stderr, "[op_dict_create] invalid key\n");
        }
        return {};
    }
    auto maybe_map = resolve_value_slot_mut(vd);
    if (!maybe_map.has_value() || !maybe_map->valid() || !maybe_map->is_map()) {
        if (debug_create) {
            std::fprintf(stderr,
                         "[op_dict_create] map slot missing valid=%d is_map=%d\n",
                         (maybe_map.has_value() && maybe_map->valid()) ? 1 : 0,
                         (maybe_map.has_value() && maybe_map->valid() && maybe_map->is_map()) ? 1 : 0);
        }
        return {};
    }

    auto map = maybe_map->as_map();
    if (debug_create) {
        std::fprintf(stderr,
                     "[op_dict_create] begin size=%zu key_schema=%p map_key=%p value_type=%p contains_before=%d\n",
                     map.size(),
                     static_cast<const void*>(key.schema()),
                     static_cast<const void*>(map.key_type()),
                     static_cast<const void*>(map.value_type()),
                     map.contains(key) ? 1 : 0);
    }
    auto slots = resolve_tsd_delta_slots(vd);
    clear_tsd_delta_if_new_tick(vd, current_time, slots);

    if (!map.contains(key)) {
        const value::TypeMeta* value_type = map.value_type();
        if (value_type == nullptr) {
            if (debug_create) {
                std::fprintf(stderr, "[op_dict_create] value_type null\n");
            }
            return {};
        }
        value::Value default_value(value_type);
        default_value.emplace();
        map.set(key, default_value.view());
        if (debug_create) {
            std::fprintf(stderr,
                         "[op_dict_create] after set size=%zu contains=%d\n",
                         map.size(),
                         map.contains(key) ? 1 : 0);
        }
        const auto slot = map_slot_for_key(map, key);
        if (slot.has_value()) {
            ensure_tsd_child_time_slot(vd, *slot);
            ensure_tsd_child_delta_slot(vd, *slot);
            ensure_tsd_child_link_slot(vd, *slot);
        } else if (debug_create) {
            std::fprintf(stderr, "[op_dict_create] slot lookup failed after set\n");
        }

        if (slots.added_set.valid() && slots.added_set.is_set()) {
            slots.added_set.as_set().add(key);
        }
        if (slots.removed_set.valid() && slots.removed_set.is_set()) {
            slots.removed_set.as_set().remove(key);
        }
        if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
            slots.changed_values_map.as_map().remove(key);
        }

        stamp_time_paths(vd, current_time);
        notify_link_target_observers(vd, current_time);
    }
    if (debug_create) {
        std::fprintf(stderr,
                     "[op_dict_create] end size=%zu contains=%d\n",
                     map.size(),
                     map.contains(key) ? 1 : 0);
    }
    return op_child_by_key(vd, key, current_time);
}

TSView op_dict_set(ViewData& vd, const View& key, const View& value, engine_time_t current_time) {
    if (!key.valid() || !value.valid()) {
        return {};
    }
    auto maybe_map = resolve_value_slot_mut(vd);
    if (!maybe_map.has_value() || !maybe_map->valid() || !maybe_map->is_map()) {
        return {};
    }

    auto map = maybe_map->as_map();
    auto slots = resolve_tsd_delta_slots(vd);
    clear_tsd_delta_if_new_tick(vd, current_time, slots);

    const bool existed = map.contains(key);
    if (!existed) {
        const value::TypeMeta* value_type = map.value_type();
        if (value_type == nullptr) {
            return {};
        }
        value::Value default_value(value_type);
        default_value.emplace();
        map.set(key, default_value.view());
    }

    const auto slot = map_slot_for_key(map, key);
    if (!slot.has_value()) {
        return {};
    }

    ensure_tsd_child_time_slot(vd, *slot);
    ensure_tsd_child_delta_slot(vd, *slot);
    ensure_tsd_child_link_slot(vd, *slot);

    ViewData child_vd = vd;
    child_vd.path.indices.push_back(*slot);
    op_set_value(child_vd, value, current_time);

    if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
        View child_value = op_value(child_vd);
        if (child_value.valid()) {
            slots.changed_values_map.as_map().set(key, child_value);
        } else {
            slots.changed_values_map.as_map().remove(key);
        }
    }
    if (!existed && slots.added_set.valid() && slots.added_set.is_set()) {
        slots.added_set.as_set().add(key);
    }
    if (slots.removed_set.valid() && slots.removed_set.is_set()) {
        slots.removed_set.as_set().remove(key);
    }

    return op_child_at(vd, *slot, current_time);
}

const ts_window_ops k_window_ops{
    &op_window_value_times,
    &op_window_value_times_count,
    &op_window_first_modified_time,
    &op_window_has_removed_value,
    &op_window_removed_value,
    &op_window_removed_value_count,
    &op_window_size,
    &op_window_min_size,
    &op_window_length,
};

const ts_set_ops k_set_ops{
    &op_set_add,
    &op_set_remove,
    &op_set_clear,
};

const ts_dict_ops k_dict_ops{
    &op_dict_remove,
    &op_dict_create,
    &op_dict_set,
};

const ts_list_ops k_list_ops{
    &op_child_at,
    &op_list_size,
};

const ts_bundle_ops k_bundle_ops{
    &op_child_at,
    &op_child_by_name,
    &op_bundle_size,
};

ts_ops make_common_ops(TSKind kind) {
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
        kind,
        {},
    };
    out.specific.none = ts_ops::ts_none_ops{0};
    return out;
}

ts_ops make_tsw_ops() {
    ts_ops out = make_common_ops(TSKind::TSW);
    out.specific.window = k_window_ops;
    return out;
}

ts_ops make_tss_ops() {
    ts_ops out = make_common_ops(TSKind::TSS);
    out.specific.set = k_set_ops;
    return out;
}

ts_ops make_tsd_ops() {
    ts_ops out = make_common_ops(TSKind::TSD);
    out.specific.dict = k_dict_ops;
    return out;
}

ts_ops make_tsl_ops() {
    ts_ops out = make_common_ops(TSKind::TSL);
    out.specific.list = k_list_ops;
    return out;
}

ts_ops make_tsb_ops() {
    ts_ops out = make_common_ops(TSKind::TSB);
    out.specific.bundle = k_bundle_ops;
    return out;
}

void copy_view_data_value_impl(ViewData dst, const ViewData& src, engine_time_t current_time);

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
    for (View key : dst_map.keys()) {
        bool keep = false;
        for (const auto& source_key : source_keys) {
            const View source_view = source_key.view();
            if (source_view.schema() == key.schema() && source_view.equals(key)) {
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

    for (const auto& key : to_remove) {
        const auto removed_slot = map_slot_for_key(dst_map, key.view());
        bool removed_was_valid = false;
        if (removed_slot.has_value()) {
            ViewData child_vd = dst;
            child_vd.path.indices.push_back(*removed_slot);
            removed_was_valid = op_valid(child_vd);
        }
        dst_map.remove(key.view());
        changed = true;
        if (removed_slot.has_value()) {
            compact_tsd_child_time_slot(dst, *removed_slot);
            compact_tsd_child_delta_slot(dst, *removed_slot);
            compact_tsd_child_link_slot(dst, *removed_slot);
        }
        if (removed_was_valid && slots.removed_set.valid() && slots.removed_set.is_set()) {
            slots.removed_set.as_set().add(key.view());
        }
        if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
            slots.changed_values_map.as_map().remove(key.view());
        }
        stamp_time_paths(dst, current_time);
    }

    const value::TypeMeta* value_type = dst_map.value_type();
    for (const auto& key : source_keys) {
        const bool existed = dst_map.contains(key.view());
        if (!existed) {
            if (value_type == nullptr) {
                continue;
            }
            value::Value default_value(value_type);
            default_value.emplace();
            dst_map.set(key.view(), default_value.view());
            if (slots.added_set.valid() && slots.added_set.is_set()) {
                slots.added_set.as_set().add(key.view());
            }
            stamp_time_paths(dst, current_time);
            changed = true;
        }

        auto slot = map_slot_for_key(dst_map, key.view());
        if (!slot.has_value()) {
            continue;
        }

        ensure_tsd_child_time_slot(dst, *slot);
        ensure_tsd_child_delta_slot(dst, *slot);
        ensure_tsd_child_link_slot(dst, *slot);

        TSView src_child = op_child_by_key(src, key.view(), current_time);
        TSView dst_child = op_child_at(dst, *slot, current_time);
        if (!src_child || !dst_child) {
            continue;
        }

        const bool source_child_modified = op_modified(src_child.view_data(), current_time);
        if (!source_child_modified && existed) {
            if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                slots.changed_values_map.as_map().remove(key.view());
            }
            if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                slots.removed_set.as_set().remove(key.view());
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
                    slots.changed_values_map.as_map().set(key.view(), child_value);
                }
            } else {
                slots.changed_values_map.as_map().remove(key.view());
            }
        }
        if (slots.removed_set.valid() && slots.removed_set.is_set()) {
            slots.removed_set.as_set().remove(key.view());
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

    if (dst_meta->kind != src_meta->kind) {
        throw std::runtime_error("copy_view_data_value: source/destination schema kinds differ");
    }

    switch (dst_meta->kind) {
        case TSKind::TSValue:
        case TSKind::REF:
        case TSKind::SIGNAL:
        case TSKind::TSW:
            op_set_value(dst, op_value(src), current_time);
            return;

        case TSKind::TSS:
            copy_tss(dst, src, current_time);
            return;

        case TSKind::TSL: {
            const size_t n = std::min(op_list_size(dst), op_list_size(src));
            for (size_t i = 0; i < n; ++i) {
                TSView src_child = op_child_at(src, i, current_time);
                TSView dst_child = op_child_at(dst, i, current_time);
                if (!src_child || !dst_child) {
                    continue;
                }
                copy_view_data_value_impl(dst_child.view_data(), src_child.view_data(), current_time);
            }
            return;
        }

        case TSKind::TSB:
            for (size_t i = 0; i < dst_meta->field_count(); ++i) {
                TSView src_child = op_child_at(src, i, current_time);
                TSView dst_child = op_child_at(dst, i, current_time);
                if (!src_child || !dst_child) {
                    continue;
                }
                copy_view_data_value_impl(dst_child.view_data(), src_child.view_data(), current_time);
            }
            return;

        case TSKind::TSD:
            copy_tsd(dst, src, current_time);
            return;
    }
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

}  // namespace

const ts_ops* get_ts_ops(TSKind kind) {
    switch (kind) {
        case TSKind::TSValue:
            return &k_ts_value_ops;
        case TSKind::TSS:
            return &k_tss_ops;
        case TSKind::TSD:
            return &k_tsd_ops;
        case TSKind::TSL:
            return &k_tsl_ops;
        case TSKind::TSB:
            return &k_tsb_ops;
        case TSKind::REF:
            return &k_ref_ops;
        case TSKind::SIGNAL:
            return &k_signal_ops;
        case TSKind::TSW:
            return &k_tsw_tick_ops;
    }
    return &k_ts_value_ops;
}

const ts_ops* get_ts_ops(const TSMeta* meta) {
    if (meta == nullptr) {
        return &k_ts_value_ops;
    }
    if (meta->kind == TSKind::TSW) {
        return meta->is_duration_based() ? &k_tsw_duration_ops : &k_tsw_tick_ops;
    }
    return get_ts_ops(meta->kind);
}

const ts_ops* default_ts_ops() {
    return get_ts_ops(TSKind::TSValue);
}

void store_to_link_target(LinkTarget& target, const ViewData& source) {
    target.bind(source);
}

void store_to_ref_link(REFLink& target, const ViewData& source) {
    const TSMeta* source_meta = meta_at_path(source.meta, source.path.indices);
    if (source_meta != nullptr && source_meta->kind == TSKind::REF) {
        target.bind_to_ref(source);
    } else {
        target.bind(source);
    }
}

bool resolve_direct_bound_view_data(const ViewData& source, ViewData& out) {
    if (auto rebound = resolve_bound_view_data(source); rebound.has_value()) {
        out = std::move(*rebound);
        return true;
    }
    out = source;
    return false;
}

bool resolve_bound_target_view_data(const ViewData& source, ViewData& out) {
    out = source;
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
        followed = true;
    }

    return false;
}

bool resolve_previous_bound_target_view_data(const ViewData& source, ViewData& out) {
    out = source;
    if (!source.uses_link_target) {
        return false;
    }

    if (LinkTarget* target = resolve_link_target(source, source.path.indices);
        target != nullptr && target->has_previous_target) {
        out = target->previous_view_data(source.sampled);
        out.projection = merge_projection(source.projection, out.projection);
        return true;
    }

    return false;
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

void reset_ts_link_observers() {
    // Registries are endpoint-owned and do not require process-global cleanup.
}

}  // namespace hgraph
