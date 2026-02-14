#include <hgraph/types/time_series/ts_ops.h>

#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/value/type_registry.h>

#include <optional>
#include <stdexcept>

namespace hgraph {
namespace {

using value::View;
using value::Value;
using value::ValueView;

const TSMeta* meta_at_path(const TSMeta* root, const std::vector<size_t>& indices) {
    const TSMeta* meta = root;
    for (size_t index : indices) {
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
            case TSKind::REF:
                meta = meta->element_ts();
                break;
            default:
                return nullptr;
        }
    }
    return meta;
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
            if (index >= map.size()) {
                return std::nullopt;
            }
            size_t slot = 0;
            std::optional<View> key;
            for (View k : map.keys()) {
                if (slot++ == index) {
                    key = k;
                    break;
                }
            }
            if (!key.has_value()) {
                return std::nullopt;
            }
            current = map.at(*key);
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
            if (index >= map.size()) {
                return std::nullopt;
            }
            size_t slot = 0;
            std::optional<View> key;
            for (View k : map.keys()) {
                if (slot++ == index) {
                    key = k;
                    break;
                }
            }
            if (!key.has_value()) {
                return std::nullopt;
            }
            current = map.at(*key);
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

std::vector<size_t> ts_path_to_link_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    std::vector<size_t> out;
    const TSMeta* meta = root_meta;

    for (size_t index : ts_path) {
        if (meta == nullptr) {
            break;
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
                    out.push_back(index);
                }
                meta = meta->element_ts();
                break;
            case TSKind::TSD:
                // Collection-level link only.
                meta = meta->element_ts();
                break;
            case TSKind::REF:
                meta = meta->element_ts();
                break;
            default:
                return out;
        }
    }

    // TSB nodes always have a container link at slot 0.
    if (meta != nullptr && meta->kind == TSKind::TSB) {
        out.push_back(0);
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

            case TSKind::REF:
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

            case TSKind::REF:
                meta = meta->element_ts();
                break;

            default:
                return out;
        }
    }

    return out;
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
    static const value::TypeMeta* meta = value::TypeRegistry::instance().get_by_name("LinkTarget");
    return meta;
}

const value::TypeMeta* ref_link_meta() {
    static const value::TypeMeta* meta = value::TypeRegistry::instance().get_by_name("REFLink");
    return meta;
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

LinkTarget* resolve_parent_link_target(const ViewData& vd) {
    if (vd.path.indices.empty()) {
        return nullptr;
    }

    std::vector<size_t> parent_path = vd.path.indices;
    parent_path.pop_back();
    return resolve_link_target(vd, parent_path);
}

void stamp_time_paths(ViewData& vd, engine_time_t current_time) {
    auto* time_root = static_cast<Value*>(vd.time_data);
    if (time_root == nullptr || time_root->schema() == nullptr) {
        return;
    }

    if (!time_root->has_value()) {
        time_root->emplace();
    }

    for (const auto& path : time_stamp_paths_for_ts_path(vd.meta, vd.path.indices)) {
        std::optional<ValueView> slot;
        if (path.empty()) {
            slot = time_root->view();
        } else {
            slot = navigate_mut(time_root->view(), path);
        }
        if (!slot.has_value()) {
            continue;
        }
        if (engine_time_t* et_ptr = extract_time_ptr(*slot); et_ptr != nullptr) {
            *et_ptr = current_time;
        }
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

const TSMeta* op_ts_meta(const ViewData& vd) {
    return meta_at_path(vd.meta, vd.path.indices);
}

engine_time_t op_last_modified_time(const ViewData& vd) {
    auto* time_root = static_cast<const Value*>(vd.time_data);
    if (time_root == nullptr || !time_root->has_value()) {
        return MIN_DT;
    }

    auto time_path = ts_path_to_time_path(vd.meta, vd.path.indices);
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

bool op_modified(const ViewData& vd, engine_time_t current_time) {
    return vd.sampled || op_last_modified_time(vd) == current_time;
}

bool op_valid(const ViewData& vd) {
    auto* value_root = static_cast<const Value*>(vd.value_data);
    if (value_root == nullptr || !value_root->has_value()) {
        return false;
    }
    auto maybe = navigate_const(value_root->view(), vd.path.indices);
    return maybe.has_value() && maybe->valid();
}

bool op_all_valid(const ViewData& vd) {
    return op_valid(vd);
}

bool op_sampled(const ViewData& vd) {
    return vd.sampled;
}

View op_value(const ViewData& vd) {
    auto* value_root = static_cast<const Value*>(vd.value_data);
    if (value_root == nullptr || !value_root->has_value()) {
        return {};
    }
    auto maybe = navigate_const(value_root->view(), vd.path.indices);
    return maybe.has_value() ? *maybe : View{};
}

View op_delta_value(const ViewData& vd) {
    auto* delta_root = static_cast<const Value*>(vd.delta_data);
    if (delta_root == nullptr || !delta_root->has_value()) {
        return {};
    }
    auto maybe = navigate_const(delta_root->view(), vd.path.indices);
    return maybe.has_value() ? *maybe : View{};
}

bool op_has_delta(const ViewData& vd) {
    auto* delta_root = static_cast<const Value*>(vd.delta_data);
    return delta_root != nullptr && delta_root->has_value();
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
    stamp_time_paths(vd, current_time);
}

void op_apply_delta(ViewData& vd, const View& delta, engine_time_t current_time) {
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
}

void op_invalidate(ViewData& vd) {
    auto* value_root = static_cast<Value*>(vd.value_data);
    if (value_root == nullptr) {
        return;
    }
    if (vd.path.indices.empty()) {
        value_root->reset();
    }
}

nb::object op_to_python(const ViewData& vd) {
    View v = op_value(vd);
    return v.valid() ? v.to_python() : nb::none();
}

nb::object op_delta_to_python(const ViewData& vd) {
    View v = op_delta_value(vd);
    return v.valid() ? v.to_python() : nb::none();
}

void op_from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value()) {
        return;
    }

    if (vd.path.indices.empty() && src.is_none()) {
        auto* value_root = static_cast<Value*>(vd.value_data);
        if (value_root != nullptr) {
            value_root->reset();
            stamp_time_paths(vd, current_time);
        }
        return;
    }

    maybe_dst->from_python(src);
    stamp_time_paths(vd, current_time);
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
    size_t slot = 0;
    for (View k : map.keys()) {
        if (k.schema() == key.schema() && k.equals(key)) {
            return op_child_at(vd, slot, current_time);
        }
        ++slot;
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
    (void)vd;
    (void)current_time;
}

void op_bind(ViewData& vd, const ViewData& target) {
    if (vd.uses_link_target) {
        auto* link_target = resolve_link_target(vd, vd.path.indices);
        if (link_target == nullptr) {
            return;
        }
        store_to_link_target(*link_target, target);
        link_target->owner_time_ptr = resolve_owner_time_ptr(vd);
        link_target->parent_link = resolve_parent_link_target(vd);

        // TSB root bind is peered (container slot). Field binds are un-peered.
        const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
        link_target->peered = (current != nullptr && current->kind == TSKind::TSB);

        if (link_target->peered && current != nullptr && current->fields() != nullptr) {
            for (size_t i = 0; i < current->field_count(); ++i) {
                std::vector<size_t> child_path = vd.path.indices;
                child_path.push_back(i);
                if (LinkTarget* child_link = resolve_link_target(vd, child_path); child_link != nullptr) {
                    child_link->unbind();
                    child_link->peered = false;
                }
            }
        }

        // When binding a child of a TSB, un-peer the parent container link.
        if (!vd.path.indices.empty()) {
            std::vector<size_t> parent_path = vd.path.indices;
            parent_path.pop_back();
            const TSMeta* parent_meta = meta_at_path(vd.meta, parent_path);
            if (parent_meta != nullptr && parent_meta->kind == TSKind::TSB) {
                if (LinkTarget* parent_link = resolve_link_target(vd, parent_path); parent_link != nullptr) {
                    parent_link->unbind();
                    parent_link->peered = false;
                }
            }
        }
    } else {
        auto* ref_link = resolve_ref_link(vd, vd.path.indices);
        if (ref_link == nullptr) {
            return;
        }
        store_to_ref_link(*ref_link, target);
    }
}

void op_unbind(ViewData& vd) {
    if (vd.uses_link_target) {
        auto* lt = resolve_link_target(vd, vd.path.indices);
        if (lt == nullptr) {
            return;
        }
        lt->unbind();
        lt->peered = false;
    } else {
        auto* ref_link = resolve_ref_link(vd, vd.path.indices);
        if (ref_link == nullptr) {
            return;
        }
        ref_link->unbind();
    }
}

bool op_is_bound(const ViewData& vd) {
    if (vd.uses_link_target) {
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

void op_set_active(ViewData& vd, ValueView active_view, bool active, TSInput* input) {
    set_active_flag(active_view, active);

    if (vd.uses_link_target) {
        if (LinkTarget* payload = resolve_link_target(vd, vd.path.indices); payload != nullptr) {
            payload->active_notifier = active ? input : nullptr;
        }
    } else {
        if (REFLink* payload = resolve_ref_link(vd, vd.path.indices); payload != nullptr) {
            payload->active_notifier = active ? input : nullptr;
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
    const bool removed = map.remove(key);
    if (removed) {
        stamp_time_paths(vd, current_time);
    }
    return removed;
}

TSView op_dict_create(ViewData& vd, const View& key, engine_time_t current_time) {
    if (!key.valid()) {
        return {};
    }
    auto maybe_map = resolve_value_slot_mut(vd);
    if (!maybe_map.has_value() || !maybe_map->valid() || !maybe_map->is_map()) {
        return {};
    }

    auto map = maybe_map->as_map();
    if (!map.contains(key)) {
        const value::TypeMeta* value_type = map.value_type();
        if (value_type == nullptr) {
            return {};
        }
        value::Value default_value(value_type);
        default_value.emplace();
        map.set(key, default_value.view());
        stamp_time_paths(vd, current_time);
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
    map.set(key, value);
    stamp_time_paths(vd, current_time);
    return op_child_by_key(vd, key, current_time);
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
    if (source.meta != nullptr && source.meta->kind == TSKind::REF) {
        target.bind_to_ref(source);
    } else {
        target.bind(source);
    }
}

}  // namespace hgraph
