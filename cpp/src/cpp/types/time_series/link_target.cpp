#include <hgraph/types/time_series/link_target.h>

namespace hgraph {

LinkTarget::LinkTarget(const LinkTarget& other) {
    copy_target_data_from(other);
    // Structural fields remain owner-local by contract.
}

LinkTarget& LinkTarget::operator=(const LinkTarget& other) {
    if (this != &other) {
        copy_target_data_from(other);
        // Preserve owner-local structural fields.
    }
    return *this;
}

LinkTarget::LinkTarget(LinkTarget&& other) noexcept {
    move_target_data_from(std::move(other));
}

LinkTarget& LinkTarget::operator=(LinkTarget&& other) noexcept {
    if (this != &other) {
        move_target_data_from(std::move(other));
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
    ops = other.ops;
    meta = other.meta;
}

void LinkTarget::move_target_data_from(LinkTarget&& other) noexcept {
    copy_target_data_from(other);
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
    ops = nullptr;
    meta = nullptr;
}

void LinkTarget::bind(const ViewData& target) {
    is_linked = true;
    target_path = target.path;
    value_data = target.value_data;
    time_data = target.time_data;
    observer_data = target.observer_data;
    delta_data = target.delta_data;
    link_data = target.link_data;
    ops = target.ops;
    meta = target.meta;
}

void LinkTarget::unbind() {
    clear_target_data();
}

bool LinkTarget::modified(engine_time_t current_time) const {
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
    vd.sampled = sampled;
    vd.uses_link_target = true;
    vd.ops = ops;
    vd.meta = meta;
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

