#include "ts_ops_internal.h"

namespace hgraph {

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
        } else if (meta->kind == TSKind::TSB) {
            out.push_back(0);  // container link slot.
        } else if (meta->kind == TSKind::TSL && meta->fixed_size() > 0) {
            out.push_back(0);  // fixed-size TSL container link slot.
        } else if (meta->kind == TSKind::TSD) {
            out.push_back(0);  // dynamic TSD container link slot.
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
                out.push_back(1);
                out.push_back(index);
                meta = meta->element_ts();
                break;
            default:
                return out;
        }
    }

    // Container/REF nodes have a root link slot at 0.
    if (!crossed_dynamic_boundary && meta != nullptr) {
        if (meta->kind == TSKind::REF) {
            out.push_back(0);
        } else if (meta->kind == TSKind::TSB) {
            out.push_back(0);
        } else if (meta->kind == TSKind::TSL && meta->fixed_size() > 0) {
            out.push_back(0);
        } else if (meta->kind == TSKind::TSD) {
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
        if (meta->kind == TSKind::TSB ||
            meta->kind == TSKind::TSL ||
            meta->kind == TSKind::TSD ||
            meta->kind == TSKind::TSW) {
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

std::vector<size_t> ts_path_to_observer_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    std::vector<size_t> out;
    const TSMeta* meta = root_meta;

    if (meta == nullptr) {
        return out;
    }

    if (ts_path.empty()) {
        if (meta->kind == TSKind::TSB ||
            meta->kind == TSKind::TSL ||
            meta->kind == TSKind::TSD) {
            out.push_back(0);  // container observer slot
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

    if (root_meta->kind == TSKind::TSB ||
        root_meta->kind == TSKind::TSL ||
        root_meta->kind == TSKind::TSD ||
        root_meta->kind == TSKind::TSW) {
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
            bind_view_data_ops(resolved);
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
                bind_view_data_ops(resolved);
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
            ViewData resolved = std::move(*resolved_ref_ancestor);
            bind_view_data_ops(resolved);
            log_resolve("ref-value-ancestor", resolved);
            return resolved;
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
            bind_view_data_ops(resolved);
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
                bind_view_data_ops(resolved);
                log_resolve("ref-ancestor", resolved);
                return resolved;
            }
        }
    }

    return std::nullopt;
}

engine_time_t rebind_time_for_view(const ViewData& vd) {
    if (vd.uses_link_target) {
        engine_time_t out = MIN_DT;
        if (LinkTarget* target = resolve_link_target(vd, vd.path.indices); target != nullptr) {
            // First empty->resolved binds must surface as modified time so
            // non-REF consumers (for example lag over switch stubs) sample the
            // newly bound value on the bind tick.
            if (target->has_previous_target || target->has_resolved_target) {
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

bool ref_child_payload_valid(const ViewData& ref_child_vd) {
    if (!op_valid(ref_child_vd)) {
        return false;
    }
    View ref_value = op_value(ref_child_vd);
    if (!(ref_value.valid() && ref_value.schema() == ts_reference_meta())) {
        return false;
    }
    try {
        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(ref_value.to_python());
        return ref.is_valid();
    } catch (...) {
        return false;
    }
}

bool container_child_valid_for_aggregation(const ViewData& child_vd) {
    // Python all_valid semantics treat REF children as valid based on wrapper
    // validity, not referent payload validity.
    return op_valid(child_vd);
}

bool ref_child_rebound_this_tick(const ViewData& ref_child) {
    ViewData previous{};
    if (!resolve_previous_bound_target_view_data(ref_child, previous)) {
        return false;
    }

    ViewData current{};
    if (!resolve_bound_target_view_data(ref_child, current)) {
        return false;
    }

    return !same_view_identity(previous, current);
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

    // If the traversed leaf itself is REF (for example TSD slot values in
    // TSD[K, REF[...]]), treat that leaf depth as the first REF ancestor.
    if (current != nullptr && current->kind == TSKind::REF) {
        ref_depth_out = ts_path.size();
        return true;
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



}  // namespace hgraph
