#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/ts_ops.h>

namespace hgraph {

REFLink::REFLink(const REFLink& other)
    : is_linked(other.is_linked),
      source(other.source),
      target(other.target),
      // active_notifier is intentionally owner-local.
      active_notifier(nullptr),
      last_rebind_time(other.last_rebind_time) {}

REFLink& REFLink::operator=(const REFLink& other) {
    if (this != &other) {
        is_linked = other.is_linked;
        source = other.source;
        target = other.target;
        // Keep active_notifier owner-local.
        last_rebind_time = other.last_rebind_time;
    }
    return *this;
}

REFLink::REFLink(REFLink&& other) noexcept
    : is_linked(other.is_linked),
      source(std::move(other.source)),
      target(std::move(other.target)),
      active_notifier(other.active_notifier),
      last_rebind_time(other.last_rebind_time) {
    other.active_notifier = nullptr;
    other.is_linked = false;
    other.source = {};
    other.target = {};
    other.last_rebind_time = MIN_DT;
}

REFLink& REFLink::operator=(REFLink&& other) noexcept {
    if (this != &other) {
        is_linked = other.is_linked;
        source = std::move(other.source);
        target = std::move(other.target);
        active_notifier = other.active_notifier;
        last_rebind_time = other.last_rebind_time;

        other.active_notifier = nullptr;
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
    if (active_notifier != nullptr) {
        active_notifier->notify(et);
    }
}

}  // namespace hgraph
