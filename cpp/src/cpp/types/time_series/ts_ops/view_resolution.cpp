#include "ts_ops_internal.h"

namespace hgraph {

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
    if (dispatch_meta_is_tsd(self_meta) &&
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
    if (!dispatch_meta_is_tsd(self_meta)) {
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
    if (dispatch_meta_is_tsd(self_meta) &&
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


}  // namespace hgraph
