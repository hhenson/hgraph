#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/link_observer_registry.h>
#include <hgraph/types/time_series/ts_ops.h>

namespace hgraph {
namespace {

void redirect_link_target_registrations(TSLinkObserverRegistry* registry,
                                        const LinkTarget* from,
                                        LinkTarget* to) {
    if (registry == nullptr || from == nullptr || to == nullptr || from == to) {
        return;
    }

    for (auto& [_, registrations] : registry->entries) {
        for (auto& registration : registrations) {
            if (registration.link_target == from) {
                registration.link_target = to;
            }
        }
    }
}

}  // namespace

LinkTarget::~LinkTarget() {
    unregister_ts_link_observer(*this);
}

LinkTarget::LinkTarget(const LinkTarget& other) {
    copy_target_data_from(other);
    // Structural fields remain owner-local by contract.
    last_rebind_time = other.last_rebind_time;
    has_previous_target = other.has_previous_target;
    previous_target = other.previous_target;
    has_resolved_target = other.has_resolved_target;
    resolved_target = other.resolved_target;
    if (is_linked) {
        register_ts_link_observer(*this);
    }
}

LinkTarget& LinkTarget::operator=(const LinkTarget& other) {
    if (this != &other) {
        unregister_ts_link_observer(*this);
        copy_target_data_from(other);
        // Preserve owner-local structural fields.
        last_rebind_time = other.last_rebind_time;
        has_previous_target = other.has_previous_target;
        previous_target = other.previous_target;
        has_resolved_target = other.has_resolved_target;
        resolved_target = other.resolved_target;
        if (is_linked) {
            register_ts_link_observer(*this);
        }
    }
    return *this;
}

LinkTarget::LinkTarget(LinkTarget&& other) noexcept {
    move_target_data_from(std::move(other));
    last_rebind_time = other.last_rebind_time;
    has_previous_target = other.has_previous_target;
    previous_target = other.previous_target;
    has_resolved_target = other.has_resolved_target;
    resolved_target = other.resolved_target;
    other.last_rebind_time = MIN_DT;
    other.has_previous_target = false;
    other.previous_target = {};
    other.has_resolved_target = false;
    other.resolved_target = {};
}

LinkTarget& LinkTarget::operator=(LinkTarget&& other) noexcept {
    if (this != &other) {
        unregister_ts_link_observer(*this);
        move_target_data_from(std::move(other));
        last_rebind_time = other.last_rebind_time;
        has_previous_target = other.has_previous_target;
        previous_target = other.previous_target;
        has_resolved_target = other.has_resolved_target;
        resolved_target = other.resolved_target;
        other.last_rebind_time = MIN_DT;
        other.has_previous_target = false;
        other.previous_target = {};
        other.has_resolved_target = false;
        other.resolved_target = {};
    }
    return *this;
}

void LinkTarget::copy_target_data_from(const LinkTarget& other) {
    is_linked = other.is_linked;
    target_path = other.target_path;
    value_data = other.value_data;
    time_data = other.time_data;
    observer_data = other.observer_data;
    delta_data = other.delta_data;
    link_data = other.link_data;
    link_observer_registry = other.link_observer_registry;
    projection = other.projection;
    ops = other.ops;
    meta = other.meta;
    fan_in_targets = other.fan_in_targets;
    notify_on_ref_wrapper_write = other.notify_on_ref_wrapper_write;
    observer_is_signal = other.observer_is_signal;
    observer_ref_to_nonref_target = other.observer_ref_to_nonref_target;
}

void LinkTarget::move_target_data_from(LinkTarget&& other) noexcept {
    TSLinkObserverRegistry* direct_registry = other.link_observer_registry;
    TSLinkObserverRegistry* resolved_registry =
        other.has_resolved_target ? other.resolved_target.link_observer_registry : nullptr;

    copy_target_data_from(other);

    redirect_link_target_registrations(direct_registry, &other, this);
    if (resolved_registry != direct_registry) {
        redirect_link_target_registrations(resolved_registry, &other, this);
    }

    if (is_linked) {
        register_ts_link_observer(*this);
    }

    other.clear_target_data();
}

void LinkTarget::clear_target_data() {
    is_linked = false;
    target_path = {};
    value_data = nullptr;
    time_data = nullptr;
    observer_data = nullptr;
    delta_data = nullptr;
    link_data = nullptr;
    link_observer_registry = nullptr;
    projection = ViewProjection::NONE;
    ops = nullptr;
    meta = nullptr;
    fan_in_targets.clear();
    notify_on_ref_wrapper_write = true;
    observer_is_signal = false;
    observer_ref_to_nonref_target = false;
}

void LinkTarget::bind(const ViewData& target, engine_time_t current_time) {
    const bool was_linked = is_linked;
    const bool same_binding =
        was_linked &&
        target_path.indices == target.path.indices &&
        value_data == target.value_data &&
        time_data == target.time_data &&
        observer_data == target.observer_data &&
        delta_data == target.delta_data &&
        link_data == target.link_data &&
        link_observer_registry == target.link_observer_registry &&
        projection == target.projection &&
        ops == target.ops &&
        meta == target.meta;

    // Python parity: rebinding to the exact same endpoint is a no-op and
    // must not tick wrapper modified/rebind state.
    if (same_binding) {
        return;
    }

    const bool preserve_resolved_target = same_binding && has_resolved_target;
    const ViewData preserved_resolved_target = preserve_resolved_target ? resolved_target : ViewData{};

    if (is_linked) {
        has_previous_target = true;
        previous_target = as_view_data(false);
    } else if (!has_previous_target) {
        has_previous_target = false;
        previous_target = {};
    }

    is_linked = true;
    target_path = target.path;
    value_data = target.value_data;
    time_data = target.time_data;
    observer_data = target.observer_data;
    delta_data = target.delta_data;
    link_data = target.link_data;
    link_observer_registry = target.link_observer_registry;
    projection = target.projection;
    ops = target.ops;
    meta = target.meta;
    fan_in_targets.clear();

    if (current_time != MIN_DT) {
        last_rebind_time = current_time;
        if (owner_time_ptr != nullptr && *owner_time_ptr < current_time) {
            *owner_time_ptr = current_time;
        }
    }

    if (preserve_resolved_target) {
        has_resolved_target = true;
        resolved_target = preserved_resolved_target;
    } else {
        has_resolved_target = false;
        resolved_target = {};
    }
}

void LinkTarget::unbind(engine_time_t current_time) {
    if (is_linked) {
        has_previous_target = true;
        previous_target = as_view_data(false);
    } else if (!has_previous_target) {
        has_previous_target = false;
        previous_target = {};
    }

    clear_target_data();

    if (current_time != MIN_DT) {
        last_rebind_time = current_time;
        if (owner_time_ptr != nullptr && *owner_time_ptr < current_time) {
            *owner_time_ptr = current_time;
        }
    } else {
        last_rebind_time = MIN_DT;
    }

    has_resolved_target = false;
    resolved_target = {};
}

bool LinkTarget::modified(engine_time_t current_time) const {
    if (last_rebind_time == current_time) {
        return true;
    }
    if (!is_linked || owner_time_ptr == nullptr) {
        return false;
    }
    return *owner_time_ptr == current_time;
}

ViewData LinkTarget::as_view_data(bool sampled) const {
    ViewData vd;
    vd.path = target_path;
    vd.value_data = value_data;
    vd.time_data = time_data;
    vd.observer_data = observer_data;
    vd.delta_data = delta_data;
    vd.link_data = link_data;
    vd.link_observer_registry = link_observer_registry;
    vd.sampled = sampled;
    vd.uses_link_target = false;
    vd.projection = projection;
    vd.ops = ops;
    vd.meta = meta;
    return vd;
}

ViewData LinkTarget::previous_view_data(bool sampled) const {
    ViewData vd = previous_target;
    vd.sampled = sampled;
    return vd;
}

void LinkTarget::notify(engine_time_t et) {
    if (owner_time_ptr != nullptr && *owner_time_ptr < et) {
        *owner_time_ptr = et;
    }
    if (parent_link != nullptr) {
        parent_link->notify(et);
    }
    if (active_notifier != nullptr) {
        active_notifier->notify(et);
    }
}

}  // namespace hgraph
