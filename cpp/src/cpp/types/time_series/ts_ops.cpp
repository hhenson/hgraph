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
#include <hgraph/types/value/type_registry.h>

#include <algorithm>
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

bool is_prefix_path(const std::vector<size_t>& lhs, const std::vector<size_t>& rhs) {
    if (lhs.size() > rhs.size()) {
        return false;
    }
    return std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

bool paths_related(const std::vector<size_t>& lhs, const std::vector<size_t>& rhs) {
    return is_prefix_path(lhs, rhs) || is_prefix_path(rhs, lhs);
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
        return *et;
    }
    return MIN_DT;
}

void notify_activation_if_modified(LinkTarget* payload, TSInput* input) {
    if (payload == nullptr || input == nullptr || !payload->is_linked || payload->ops == nullptr) {
        return;
    }

    const engine_time_t current_time = resolve_input_current_time(input);
    if (current_time == MIN_DT) {
        return;
    }

    ViewData target_vd = payload->as_view_data(false);
    if (!payload->ops->valid(target_vd)) {
        return;
    }
    if (!payload->ops->modified(target_vd, current_time)) {
        return;
    }
    payload->notify(payload->ops->last_modified_time(target_vd));
}

void notify_activation_if_modified(REFLink* payload, TSInput* input) {
    if (payload == nullptr || input == nullptr || !payload->valid()) {
        return;
    }

    const engine_time_t current_time = resolve_input_current_time(input);
    if (current_time == MIN_DT) {
        return;
    }
    // REF inputs mirror legacy semantics: activating a valid reference should
    // schedule evaluation even when the underlying target did not tick this cycle.
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

void unregister_link_target_observer(const LinkTarget& link_target) {
    TSLinkObserverRegistry* direct_registry = link_target.link_observer_registry;
    TSLinkObserverRegistry* resolved_registry = link_target.has_resolved_target ? link_target.resolved_target.link_observer_registry : nullptr;

    unregister_link_target_observer_from_registry(link_target, direct_registry);
    if (resolved_registry != direct_registry) {
        unregister_link_target_observer_from_registry(link_target, resolved_registry);
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

void register_link_target_observer(const LinkTarget& link_target) {
    if (!link_target.is_linked) {
        return;
    }

    auto* mutable_target = const_cast<LinkTarget*>(&link_target);
    register_link_target_observer_entry(
        link_target.link_observer_registry,
        link_target.value_data,
        link_target.target_path.indices,
        mutable_target);

    if (link_target.has_resolved_target && link_target.resolved_target.value_data != nullptr) {
        register_link_target_observer_entry(
            link_target.resolved_target.link_observer_registry,
            link_target.resolved_target.value_data,
            link_target.resolved_target.path.indices,
            mutable_target);
    }
}

void notify_link_target_observers(const ViewData& target_view, engine_time_t current_time) {
    if (target_view.value_data == nullptr || target_view.link_observer_registry == nullptr) {
        return;
    }

    std::vector<LinkTarget*> observers;
    auto it = target_view.link_observer_registry->entries.find(target_view.value_data);
    if (it == target_view.link_observer_registry->entries.end()) {
        return;
    }

    std::unordered_set<LinkTarget*> dedupe;
    for (const LinkObserverRegistration& registration : it->second) {
        if (registration.link_target == nullptr) {
            continue;
        }
        if (!paths_related(registration.path, target_view.path.indices)) {
            continue;
        }
        if (dedupe.insert(registration.link_target).second) {
            observers.push_back(registration.link_target);
        }
    }

    for (LinkTarget* observer : observers) {
        if (observer != nullptr && observer->is_linked) {
            observer->notify(current_time);
        }
    }
}

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

std::optional<ValueView> resolve_delta_slot_mut(ViewData& vd) {
    auto* delta_root = static_cast<Value*>(vd.delta_data);
    if (delta_root == nullptr || delta_root->schema() == nullptr) {
        return std::nullopt;
    }
    if (!delta_root->has_value()) {
        delta_root->emplace();
    }
    if (vd.path.indices.empty()) {
        return delta_root->view();
    }
    return navigate_mut(delta_root->view(), vd.path.indices);
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

std::optional<size_t> map_slot_for_key(const value::MapView& map, const View& key) {
    size_t slot = 0;
    for (View k : map.keys()) {
        if (k.schema() == key.schema() && k.equals(key)) {
            return slot;
        }
        ++slot;
    }
    return std::nullopt;
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

bool tss_delta_empty(const TSSDeltaSlots& slots) {
    return set_view_empty(slots.added_set) && set_view_empty(slots.removed_set);
}

bool tsd_delta_empty(const TSDDeltaSlots& slots) {
    return map_view_empty(slots.changed_values_map) && set_view_empty(slots.added_set) && set_view_empty(slots.removed_set);
}

engine_time_t op_last_modified_time(const ViewData& vd);
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
}

void clear_tss_delta_if_new_tick(ViewData& vd, engine_time_t current_time, TSSDeltaSlots slots) {
    if (!slots.slot.valid()) {
        return;
    }
    if (op_last_modified_time(vd) < current_time) {
        clear_tss_delta_slots(slots);
    }
}

void clear_tsd_delta_if_new_tick(ViewData& vd, engine_time_t current_time, TSDDeltaSlots slots) {
    if (!slots.slot.valid()) {
        return;
    }
    if (op_last_modified_time(vd) < current_time) {
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

    for (size_t index : ts_path) {
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
                case TSKind::REF:
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
                // Collection-level link only.
                crossed_dynamic_boundary = true;
                meta = meta->element_ts();
                break;
            case TSKind::REF:
                meta = meta->element_ts();
                break;
            default:
                return out;
        }
    }

    // TSB and fixed-size TSL nodes have a container link at slot 0.
    if (!crossed_dynamic_boundary && meta != nullptr && meta->kind == TSKind::TSB) {
        out.push_back(0);
    } else if (!crossed_dynamic_boundary && meta != nullptr && meta->kind == TSKind::TSL && meta->fixed_size() > 0) {
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

bool is_unpeered_static_container_view(const ViewData& vd, const TSMeta* current) {
    if (!vd.uses_link_target || current == nullptr) {
        return false;
    }

    if (current->kind != TSKind::TSB && (current->kind != TSKind::TSL || current->fixed_size() == 0)) {
        return false;
    }

    if (LinkTarget* payload = resolve_link_target(vd, vd.path.indices); payload != nullptr) {
        return !payload->is_linked;
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
        parent_time_path.size() == 1 && parent_time_path[0] == 0) {
        // Root TSD time schema is tuple[container_time, child_times...], and
        // ts_path_to_time_path returns [0] for container time. For child-slot
        // materialization we need the tuple itself.
        parent_time_path.clear();
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
    if (child_slot >= list.size()) {
        list.resize(child_slot + 1);
    }
}

LinkTarget* resolve_parent_link_target(const ViewData& vd) {
    if (vd.path.indices.empty()) {
        return nullptr;
    }

    std::vector<size_t> parent_path = vd.path.indices;
    parent_path.pop_back();
    return resolve_link_target(vd, parent_path);
}

std::vector<size_t> link_residual_ts_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    std::vector<size_t> residual;
    const TSMeta* current = root_meta;
    bool collecting_residual = false;

    for (size_t index : ts_path) {
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

std::optional<ViewData> resolve_bound_view_data(const ViewData& vd) {
    if (vd.uses_link_target) {
        if (LinkTarget* target = resolve_link_target(vd, vd.path.indices);
            target != nullptr && target->is_linked) {
            ViewData resolved = target->as_view_data(vd.sampled);
            const auto residual = link_residual_ts_path(vd.meta, vd.path.indices);
            if (!residual.empty()) {
                resolved.path.indices.insert(resolved.path.indices.end(), residual.begin(), residual.end());
            }
            return resolved;
        }
    } else {
        if (REFLink* ref_link = resolve_ref_link(vd, vd.path.indices);
            ref_link != nullptr && ref_link->is_linked) {
            ViewData resolved = ref_link->resolved_view_data();
            resolved.sampled = resolved.sampled || vd.sampled;
            return resolved;
        }
    }

    return std::nullopt;
}

std::optional<View> resolve_value_slot_const(const ViewData& vd);
bool resolve_read_view_data(const ViewData& vd, const TSMeta* self_meta, ViewData& out);

engine_time_t rebind_time_for_view(const ViewData& vd) {
    if (vd.uses_link_target) {
        if (LinkTarget* target = resolve_link_target(vd, vd.path.indices); target != nullptr) {
            return target->last_rebind_time;
        }
        return MIN_DT;
    }

    if (REFLink* ref_link = resolve_ref_link(vd, vd.path.indices); ref_link != nullptr) {
        return ref_link->last_rebind_time;
    }
    return MIN_DT;
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

bool resolve_ref_bound_target_view_data(const ViewData& ref_view, ViewData& out) {
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
    return true;
}

void refresh_dynamic_ref_binding(const ViewData& vd, engine_time_t current_time) {
    if (!vd.uses_link_target) {
        return;
    }

    LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
    if (link_target == nullptr || !link_target->is_linked) {
        return;
    }

    ViewData source_view = link_target->as_view_data(vd.sampled);
    ViewData resolved_target{};
    const bool has_resolved_target =
        resolve_read_view_data(source_view, nullptr, resolved_target) &&
        !same_view_identity(resolved_target, source_view);

    if (!has_resolved_target) {
        const bool had_resolved = link_target->has_resolved_target;
        link_target->has_resolved_target = false;
        link_target->resolved_target = {};
        link_target->has_previous_target = false;
        link_target->previous_target = {};
        if (had_resolved) {
            unregister_link_target_observer(*link_target);
            register_link_target_observer(*link_target);
        }
        return;
    }

    if (!link_target->has_resolved_target) {
        link_target->has_resolved_target = has_resolved_target;
        link_target->resolved_target = has_resolved_target ? resolved_target : ViewData{};
        if (has_resolved_target) {
            unregister_link_target_observer(*link_target);
            register_link_target_observer(*link_target);
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

    if (!resolve_read_view_data(previous_view, self_meta, previous_resolved)) {
        return false;
    }
    if (!resolve_read_view_data(current_view, self_meta, current_resolved)) {
        return false;
    }
    return true;
}

nb::object tsd_bridge_delta_to_python(const ViewData& previous_data, const ViewData& current_data) {
    nb::dict delta_out;

    auto previous_value = resolve_value_slot_const(previous_data);
    auto current_value = resolve_value_slot_const(current_data);

    const bool has_previous = previous_value.has_value() && previous_value->valid() && previous_value->is_map();
    const bool has_current = current_value.has_value() && current_value->valid() && current_value->is_map();

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
                    delta_out[key.to_python()] = current_entry.to_python();
                }
            }
        } else {
            for (View key : current_map.keys()) {
                delta_out[key.to_python()] = current_map.at(key).to_python();
            }
        }
    }

    if (has_previous) {
        const auto previous_map = previous_value->as_map();
        nb::object remove = get_remove();
        if (has_current) {
            const auto current_map = current_value->as_map();
            for (View key : previous_map.keys()) {
                if (!current_map.contains(key)) {
                    delta_out[key.to_python()] = remove;
                }
            }
        } else {
            for (View key : previous_map.keys()) {
                delta_out[key.to_python()] = remove;
            }
        }
    }

    return get_frozendict()(delta_out);
}

nb::object tss_bridge_delta_to_python(const ViewData& previous_data, const ViewData& current_data) {
    nb::set added_set;
    nb::set removed_set;

    auto previous_value = resolve_value_slot_const(previous_data);
    auto current_value = resolve_value_slot_const(current_data);

    const bool has_previous = previous_value.has_value() && previous_value->valid() && previous_value->is_set();
    const bool has_current = current_value.has_value() && current_value->valid() && current_value->is_set();

    if (has_current) {
        auto current_set = current_value->as_set();
        if (has_previous) {
            auto previous_set = previous_value->as_set();
            for (View elem : current_set) {
                if (!previous_set.contains(elem)) {
                    added_set.add(elem.to_python());
                }
            }
        } else {
            for (View elem : current_set) {
                added_set.add(elem.to_python());
            }
        }
    }

    if (has_previous) {
        auto previous_set = previous_value->as_set();
        if (has_current) {
            auto current_set = current_value->as_set();
            for (View elem : previous_set) {
                if (!current_set.contains(elem)) {
                    removed_set.add(elem.to_python());
                }
            }
        } else {
            for (View elem : previous_set) {
                removed_set.add(elem.to_python());
            }
        }
    }

    return python_set_delta(nb::frozenset(added_set), nb::frozenset(removed_set));
}

bool is_tsd_key_set_projection(const ViewData& vd) {
    return vd.projection == ViewProjection::TSD_KEY_SET;
}

bool resolve_tsd_key_set_projection_view(const ViewData& vd, ViewData& out) {
    if (is_tsd_key_set_projection(vd)) {
        out = vd;
        return true;
    }

    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return false;
    }

    ViewData resolved{};
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
        return false;
    }

    if (!is_tsd_key_set_projection(resolved)) {
        return false;
    }

    out = std::move(resolved);
    return true;
}

bool resolve_tsd_key_set_source(const ViewData& vd, ViewData& out) {
    ViewData projection_view{};
    if (!resolve_tsd_key_set_projection_view(vd, projection_view)) {
        return false;
    }

    ViewData source = projection_view;
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

bool tsd_key_set_has_added_or_removed(const ViewData& source) {
    auto* delta_root = static_cast<const Value*>(source.delta_data);
    if (delta_root == nullptr || !delta_root->has_value()) {
        return false;
    }

    std::optional<View> maybe_delta;
    if (source.path.indices.empty()) {
        maybe_delta = delta_root->view();
    } else {
        maybe_delta = navigate_const(delta_root->view(), source.path.indices);
    }
    if (!maybe_delta.has_value() || !maybe_delta->valid() || !maybe_delta->is_tuple()) {
        return false;
    }

    auto tuple = maybe_delta->as_tuple();
    const bool has_added = tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set() && tuple.at(1).as_set().size() > 0;
    const bool has_removed = tuple.size() > 2 && tuple.at(2).valid() && tuple.at(2).is_set() && tuple.at(2).as_set().size() > 0;
    return has_added || has_removed;
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
            if (tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set()) {
                for (View elem : tuple.at(1).as_set()) {
                    added_out.add(elem.to_python());
                }
            }
            if (tuple.size() > 2 && tuple.at(2).valid() && tuple.at(2).is_set()) {
                for (View elem : tuple.at(2).as_set()) {
                    removed_out.add(elem.to_python());
                }
            }
        }
    }

    return python_set_delta(nb::frozenset(added_out), nb::frozenset(removed_out));
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
            return true;
        }

        if (auto ref_value = resolve_value_slot_const(out);
            ref_value.has_value() && ref_value->valid() && ref_value->schema() == ts_reference_meta()) {
            TimeSeriesReference ref = nb::cast<TimeSeriesReference>(ref_value->to_python());
            if (const ViewData* target = ref.bound_view(); target != nullptr) {
                out = *target;
                out.sampled = out.sampled || vd.sampled;
                continue;
            }

            // Empty local REF values are placeholders used by REF->REF bind.
            // Fall through to binding resolution if a link exists.
            if (!ref.is_empty()) {
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

bool assign_ref_value_from_target(ViewData& vd, const ViewData& target) {
    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || maybe_dst->schema() != ts_reference_meta()) {
        return false;
    }
    maybe_dst->from_python(nb::cast(TimeSeriesReference::make(target)));
    return true;
}

void clear_ref_value(ViewData& vd) {
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
    const engine_time_t rebind_time = rebind_time_for_view(vd);

    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
        auto* time_root = static_cast<const Value*>(vd.time_data);
        if (time_root == nullptr || !time_root->has_value()) {
            return rebind_time;
        }

        auto time_path = ts_path_to_time_path(vd.meta, vd.path.indices);
        std::optional<View> maybe_time;
        if (time_path.empty()) {
            maybe_time = time_root->view();
        } else {
            maybe_time = navigate_const(time_root->view(), time_path);
        }

        if (!maybe_time.has_value()) {
            return rebind_time;
        }
        return std::max(extract_time_value(*maybe_time), rebind_time);
    }

    if (is_unpeered_static_container_view(vd, self_meta)) {
        engine_time_t out = rebind_time;
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
        return rebind_time;
    }
    const ViewData* data = &resolved;

    auto* time_root = static_cast<const Value*>(data->time_data);
    if (time_root == nullptr || !time_root->has_value()) {
        return rebind_time;
    }

    auto time_path = ts_path_to_time_path(data->meta, data->path.indices);
    std::optional<View> maybe_time;
    if (time_path.empty()) {
        maybe_time = time_root->view();
    } else {
        maybe_time = navigate_const(time_root->view(), time_path);
    }

    if (!maybe_time.has_value()) {
        return rebind_time;
    }
    return std::max(extract_time_value(*maybe_time), rebind_time);
}

bool op_modified(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);

    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
        // REF views tick on local reference updates/rebinds (or when sampled),
        // not on bound-target value changes.
        if (vd.sampled || rebind_time_for_view(vd) == current_time) {
            return true;
        }
        return op_last_modified_time(vd) == current_time;
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        if (vd.sampled) {
            return true;
        }
        if (rebind_time_for_view(vd) == current_time) {
            return true;
        }
        return tsd_key_set_has_added_or_removed(key_set_source);
    }
    return vd.sampled || op_last_modified_time(vd) == current_time;
}

bool op_valid(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
        if (auto local = resolve_value_slot_const(vd);
            local.has_value() && local->valid() && local->schema() == ts_reference_meta()) {
            TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
            if (!ref.is_empty()) {
                return true;
            }
        }
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        auto value = resolve_value_slot_const(key_set_source);
        return value.has_value() && value->valid() && value->is_map();
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

    // A default-constructed value slot is not valid until the node has been stamped.
    return op_last_modified_time(vd) > MIN_DT;
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
        std::optional<View> local_empty_ref;
        if (auto local = resolve_value_slot_const(vd); local.has_value() && local->valid()) {
            if (local->schema() == ts_reference_meta()) {
                TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
                if (!ref.is_empty()) {
                    return *local;
                }
                local_empty_ref = *local;
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

        if (local_empty_ref.has_value()) {
            return *local_empty_ref;
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
    if (vd.path.indices.empty()) {
        value_root->reset();
    }
}

nb::object op_to_python(const ViewData& vd) {
    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_to_python(key_set_source);
    }

    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
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

    View v = op_value(vd);
    return v.valid() ? v.to_python() : nb::none();
}

nb::object op_delta_to_python(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        ViewData previous_bridge{};
        ViewData current_bridge{};
        const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
        if (self_meta != nullptr &&
            resolve_rebind_bridge_views(vd, self_meta, current_time, previous_bridge, current_bridge)) {
            ViewData previous_source{};
            ViewData current_source{};
            if (resolve_tsd_key_set_source(previous_bridge, previous_source) &&
                resolve_tsd_key_set_source(current_bridge, current_source)) {
                return tsd_key_set_bridge_delta_to_python(previous_source, current_source);
            }
        }

        if (!op_modified(vd, current_time)) {
            return nb::none();
        }
        return tsd_key_set_delta_to_python(key_set_source);
    }

    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
        if (!op_modified(vd, current_time)) {
            return nb::none();
        }
        View v = op_delta_value(vd);
        return v.valid() ? v.to_python() : nb::none();
    }

    const auto delta_view_to_python = [](const View& view) -> nb::object {
        if (!view.valid()) {
            return nb::none();
        }
        if (view.schema() == ts_reference_meta()) {
            TimeSeriesReference ref = nb::cast<TimeSeriesReference>(view.to_python());
            if (const ViewData* target = ref.bound_view(); target != nullptr) {
                return op_to_python(*target);
            }
            return nb::none();
        }
        return view.to_python();
    };

    ViewData resolved{};
    if (!resolve_read_view_data(vd, self_meta, resolved)) {
        return nb::none();
    }
    const ViewData* data = &resolved;

    const TSMeta* current = meta_at_path(data->meta, data->path.indices);
    if (current != nullptr && (current->kind == TSKind::TSS || current->kind == TSKind::TSD)) {
        ViewData previous_bridge{};
        ViewData current_bridge{};
        if (resolve_rebind_bridge_views(vd, self_meta, current_time, previous_bridge, current_bridge)) {
            if (current->kind == TSKind::TSS) {
                return tss_bridge_delta_to_python(previous_bridge, current_bridge);
            }
            return tsd_bridge_delta_to_python(previous_bridge, current_bridge);
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
        if (!op_modified(vd, current_time)) {
            return nb::none();
        }

        nb::set added_set;
        nb::set removed_set;

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
                    }
                }
                if (tuple.size() > 1) {
                    View removed = tuple.at(1);
                    if (removed.valid() && removed.is_set()) {
                        for (View elem : removed.as_set()) {
                            removed_set.add(elem.to_python());
                        }
                    }
                }
            }
        }

        return python_set_delta(nb::frozenset(added_set), nb::frozenset(removed_set));
    }

    if (current != nullptr && current->kind == TSKind::TSD) {
        auto* delta_root = static_cast<const Value*>(data->delta_data);
        if (delta_root == nullptr || !delta_root->has_value()) {
            return get_frozendict()(nb::dict{});
        }

        std::optional<View> maybe_delta;
        if (data->path.indices.empty()) {
            maybe_delta = delta_root->view();
        } else {
            maybe_delta = navigate_const(delta_root->view(), data->path.indices);
        }

        if (!maybe_delta.has_value() || !maybe_delta->valid() || !maybe_delta->is_tuple()) {
            return get_frozendict()(nb::dict{});
        }

        nb::dict delta_out;
        auto tuple = maybe_delta->as_tuple();

        if (tuple.size() > 0) {
            View changed_values = tuple.at(0);
            if (changed_values.valid() && changed_values.is_map()) {
                const auto changed_map = changed_values.as_map();
                const TSMeta* element_meta = current->element_ts();
                const bool nested_element = element_meta != nullptr && !is_scalar_like_ts_kind(element_meta->kind);

                if (!nested_element) {
                    for (View key : changed_map.keys()) {
                        delta_out[key.to_python()] = delta_view_to_python(changed_map.at(key));
                    }
                } else {
                    auto current_value = resolve_value_slot_const(*data);
                    if (current_value.has_value() && current_value->valid() && current_value->is_map()) {
                        const auto value_map = current_value->as_map();
                        for (View key : changed_map.keys()) {
                            auto slot = map_slot_for_key(value_map, key);
                            if (!slot.has_value()) {
                                continue;
                            }
                            ViewData child = *data;
                            child.path.indices.push_back(*slot);
                            delta_out[key.to_python()] = op_delta_to_python(child, current_time);
                        }
                    }
                }
            }
        }

        if (tuple.size() > 2) {
            View removed_keys = tuple.at(2);
            if (removed_keys.valid() && removed_keys.is_set()) {
                auto set = removed_keys.as_set();
                nb::object remove = get_remove();
                for (View key : set) {
                    delta_out[key.to_python()] = remove;
                }
            }
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
            return get_frozendict()(delta_out);
        }
    }

    if (current != nullptr && current->kind == TSKind::TSL) {
        nb::dict delta_out;
        const size_t n = op_list_size(*data);
        for (size_t i = 0; i < n; ++i) {
            ViewData child = *data;
            child.path.indices.push_back(i);
            if (op_last_modified_time(child) != current_time || !op_valid(child)) {
                continue;
            }
            View child_delta = op_delta_value(child);
            if (child_delta.valid()) {
                delta_out[nb::int_(i)] = delta_view_to_python(child_delta);
            }
        }
        return get_frozendict()(delta_out);
    }

    if (current != nullptr && current->kind == TSKind::TSB) {
        nb::dict delta_out;
        if (current->fields() == nullptr) {
            return get_frozendict()(delta_out);
        }

        for (size_t i = 0; i < current->field_count(); ++i) {
            ViewData child = *data;
            child.path.indices.push_back(i);
            if (op_last_modified_time(child) != current_time || !op_valid(child)) {
                continue;
            }

            const char* field_name = current->fields()[i].name;
            if (field_name == nullptr) {
                continue;
            }

            View child_delta = op_delta_value(child);
            if (child_delta.valid()) {
                delta_out[nb::str(field_name)] = delta_view_to_python(child_delta);
            }
        }
        return get_frozendict()(delta_out);
    }

    View v = op_delta_value(vd);
    return v.valid() ? v.to_python() : nb::none();
}

void op_from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);

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
        clear_tss_delta_slots(slots);

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
        clear_tsd_delta_slots(slots);

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

                if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                    slots.changed_values_map.as_map().remove(key);
                }

                if (!added_this_cycle && slots.removed_set.valid() && slots.removed_set.is_set()) {
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

    maybe_dst->from_python(src);
    stamp_time_paths(vd, current_time);
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
    notify_link_target_observers(vd, current_time);
}

void op_bind(ViewData& vd, const ViewData& target, engine_time_t current_time) {
    if (!vd.path.indices.empty()) {
        std::vector<size_t> parent_path = vd.path.indices;
        parent_path.pop_back();
        const TSMeta* parent_meta = meta_at_path(vd.meta, parent_path);
        if (parent_meta != nullptr && parent_meta->kind == TSKind::REF) {
            ViewData promoted_input = vd;
            promoted_input.path.indices = std::move(parent_path);

            ViewData promoted_target = target;
            if (!promoted_target.path.indices.empty()) {
                promoted_target.path.indices.pop_back();
            }

            op_bind(promoted_input, promoted_target, current_time);
            return;
        }
    }

    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);

    if (vd.uses_link_target) {
        auto* link_target = resolve_link_target(vd, vd.path.indices);
        if (link_target == nullptr) {
            return;
        }
        unregister_link_target_observer(*link_target);
        link_target->owner_time_ptr = resolve_owner_time_ptr(vd);
        link_target->parent_link = resolve_parent_link_target(vd);

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
                    bool keep_parent_bound = false;

                    const bool static_container_parent =
                        parent_link->is_linked &&
                        (parent_meta->kind == TSKind::TSB ||
                         (parent_meta->kind == TSKind::TSL && parent_meta->fixed_size() > 0));

                    if (static_container_parent) {
                        const size_t child_index = vd.path.indices.back();
                        const auto& parent_target_path = parent_link->target_path.indices;
                        const auto& child_target_path = target.path.indices;

                        // Peered static-container child binds map one-to-one from parent target path.
                        keep_parent_bound =
                            child_target_path.size() == parent_target_path.size() + 1 &&
                            std::equal(parent_target_path.begin(), parent_target_path.end(), child_target_path.begin()) &&
                            child_target_path.back() == child_index &&
                            parent_link->value_data == target.value_data &&
                            parent_link->time_data == target.time_data &&
                            parent_link->observer_data == target.observer_data &&
                            parent_link->delta_data == target.delta_data &&
                            parent_link->link_data == target.link_data;
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

        if (current != nullptr && current->kind == TSKind::REF) {
            if (target.meta != nullptr && target.meta->kind != TSKind::REF) {
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
        store_to_ref_link(*ref_link, target);
        if (current_time != MIN_DT) {
            ref_link->last_rebind_time = current_time;
        }

        if (current != nullptr && current->kind == TSKind::REF) {
            if (target.meta != nullptr && target.meta->kind != TSKind::REF) {
                assign_ref_value_from_target(vd, target);
            } else {
                clear_ref_value(vd);
            }
        }
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
        ref_link->unbind();
        if (current_time != MIN_DT) {
            ref_link->last_rebind_time = current_time;
        }
    }

    if (current != nullptr && current->kind == TSKind::REF) {
        clear_ref_value(vd);
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
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    const bool ref_local_wrapper = active &&
                                   current != nullptr &&
                                   current->kind == TSKind::REF &&
                                   has_local_ref_wrapper_value(vd);
    if (vd.uses_link_target) {
        if (LinkTarget* payload = resolve_link_target(vd, vd.path.indices); payload != nullptr) {
            if (!payload->is_linked) {
                payload->active_notifier = nullptr;
                return;
            }

            if (ref_local_wrapper) {
                if (input != nullptr) {
                    const engine_time_t current_time = resolve_input_current_time(input);
                    if (current_time != MIN_DT) {
                        input->notify(current_time);
                    }
                }
                payload->active_notifier = nullptr;
                return;
            }

            payload->active_notifier = active ? input : nullptr;
            if (active) {
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
