#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/link_observer_registry.h>
#include <hgraph/types/time_series/ts_ops.h>

#include <unordered_set>

namespace hgraph {
namespace {

std::unordered_set<const REFLink*>& live_ref_links() {
    static std::unordered_set<const REFLink*> pointers;
    return pointers;
}

void track_live_ref_link(const REFLink* ref_link, bool live) {
    if (live) {
        live_ref_links().insert(ref_link);
    } else {
        live_ref_links().erase(ref_link);
    }
}

void redirect_ref_link_registrations(TSLinkObserverRegistry* registry,
                                     const REFLink* from,
                                     REFLink* to) {
    if (registry == nullptr || from == nullptr || to == nullptr || from == to) {
        return;
    }

    for (auto& [_, registrations] : registry->ref_entries) {
        for (auto& registration : registrations) {
            if (registration.ref_link == from) {
                registration.ref_link = to;
            }
        }
    }
}

void redirect_ref_link_registries(const REFLink& payload,
                                  const REFLink* from,
                                  REFLink* to) {
    std::unordered_set<TSLinkObserverRegistry*> registries;
    if (payload.source.link_observer_registry != nullptr) {
        registries.insert(payload.source.link_observer_registry);
    }
    if (payload.target.link_observer_registry != nullptr) {
        registries.insert(payload.target.link_observer_registry);
    }

    for (TSLinkObserverRegistry* registry : registries) {
        redirect_ref_link_registrations(registry, from, to);
    }
}

}  // namespace

bool is_live_ref_link(const REFLink* ref_link) noexcept {
    if (ref_link == nullptr) {
        return false;
    }
    return live_ref_links().contains(ref_link);
}

REFLink::REFLink() {
    track_live_ref_link(this, true);
}

REFLink::~REFLink() {
    track_live_ref_link(this, false);
    unregister_ts_ref_link_observer(*this);
}

REFLink::REFLink(const REFLink& other)
    : is_linked(other.is_linked),
      source(other.source),
      target(other.target),
      last_rebind_time(other.last_rebind_time) {
    track_live_ref_link(this, true);
    if (is_linked) {
        register_ts_ref_link_observer(*this);
    }
}

REFLink& REFLink::operator=(const REFLink& other) {
    if (this != &other) {
        unregister_ts_ref_link_observer(*this);
        is_linked = other.is_linked;
        source = other.source;
        target = other.target;
        // Keep active_notifier owner-local.
        last_rebind_time = other.last_rebind_time;
        if (is_linked) {
            register_ts_ref_link_observer(*this);
        }
    }
    return *this;
}

REFLink::REFLink(REFLink&& other) noexcept
    : is_linked(other.is_linked),
      source(std::move(other.source)),
      target(std::move(other.target)),
      last_rebind_time(other.last_rebind_time) {
    active_notifier.set_target(other.active_notifier.target());
    track_live_ref_link(this, true);
    if (is_linked) {
        redirect_ref_link_registries(*this, &other, this);
        register_ts_ref_link_observer(*this);
    }
    other.active_notifier.set_target(nullptr);
    other.is_linked = false;
    other.source = {};
    other.target = {};
    other.last_rebind_time = MIN_DT;
}

REFLink& REFLink::operator=(REFLink&& other) noexcept {
    if (this != &other) {
        unregister_ts_ref_link_observer(*this);
        is_linked = other.is_linked;
        source = std::move(other.source);
        target = std::move(other.target);
        active_notifier.set_target(other.active_notifier.target());
        last_rebind_time = other.last_rebind_time;
        if (is_linked) {
            redirect_ref_link_registries(*this, &other, this);
            register_ts_ref_link_observer(*this);
        }

        other.active_notifier.set_target(nullptr);
        other.is_linked = false;
        other.source = {};
        other.target = {};
        other.last_rebind_time = MIN_DT;
    }
    return *this;
}

void REFLink::bind(const ViewData& source_view) {
    source = source_view;
    is_linked = true;
}

void REFLink::bind_to_ref(const ViewData& source_view) {
    bind(source_view);
}

void REFLink::bind_target(const ViewData& target_view) {
    target = target_view;
    is_linked = true;
}

void REFLink::unbind() {
    is_linked = false;
    source = {};
    target = {};
    last_rebind_time = MIN_DT;
}

bool REFLink::has_target() const {
    return target.meta != nullptr;
}

bool REFLink::valid() const {
    return is_linked && (source.meta != nullptr || target.meta != nullptr);
}

bool REFLink::modified(engine_time_t current_time) const {
    if (!is_linked) {
        return false;
    }
    if (last_rebind_time == current_time) {
        return true;
    }

    const ViewData& vd = has_target() ? target : source;
    if (vd.ops != nullptr && vd.time_data != nullptr) {
        return vd.ops->modified(vd, current_time);
    }
    return false;
}

ViewData REFLink::resolved_view_data() const {
    return has_target() ? target : source;
}

void REFLink::notify(engine_time_t et) {
    last_rebind_time = et;
    if (active_notifier.active()) {
        active_notifier.notify(et);
    }
}

}  // namespace hgraph
