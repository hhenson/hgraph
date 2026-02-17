#include <hgraph/types/time_series/view_data.h>

#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/value/map_storage.h>
#include <hgraph/types/value/value.h>

#include <algorithm>
#include <optional>
#include <sstream>
#include <stdexcept>

namespace hgraph {
namespace {

using value::MapStorage;
using value::View;

const TSMeta* meta_child(const TSMeta* meta, size_t index) {
    if (meta == nullptr) {
        return nullptr;
    }

    switch (meta->kind) {
        case TSKind::TSB:
            if (meta->fields() == nullptr || index >= meta->field_count()) {
                return nullptr;
            }
            return meta->fields()[index].ts_type;

        case TSKind::TSL:
        case TSKind::TSD:
            return meta->element_ts();

        case TSKind::REF:
            return meta_child(meta->element_ts(), index);

        default:
            return nullptr;
    }
}

std::optional<View> map_key_for_index(const View& map_view, size_t index) {
    if (!map_view.valid() || !map_view.is_map()) {
        return std::nullopt;
    }

    auto map = map_view.as_map();
    if (const auto* storage = static_cast<const MapStorage*>(map.data()); storage != nullptr) {
        const auto& key_set = storage->key_set();
        if (key_set.is_alive(index)) {
            return View(storage->key_at_slot(index), map.key_type());
        }
    }

    size_t ordinal = 0;
    for (View key : map.keys()) {
        if (ordinal++ == index) {
            return key;
        }
    }
    return std::nullopt;
}

std::optional<View> child_value_by_index(const View& view, size_t index) {
    if (!view.valid()) {
        return std::nullopt;
    }

    if (view.is_bundle()) {
        auto bundle = view.as_bundle();
        if (index < bundle.size()) {
            return bundle.at(index);
        }
        return std::nullopt;
    }

    if (view.is_tuple()) {
        auto tuple = view.as_tuple();
        if (index < tuple.size()) {
            return tuple.at(index);
        }
        return std::nullopt;
    }

    if (view.is_list()) {
        auto list = view.as_list();
        if (index < list.size()) {
            return list.at(index);
        }
        return std::nullopt;
    }

    if (view.is_map()) {
        auto maybe_key = map_key_for_index(view, index);
        if (!maybe_key.has_value()) {
            return std::nullopt;
        }
        const value::MapView map_view = view.as_map();
        return map_view.at(*maybe_key);
    }

    return std::nullopt;
}

std::string key_to_path_string(const View& key) {
    if (!key.valid()) {
        return {};
    }
    if (key.is_scalar_type<std::string>()) {
        return key.as<std::string>();
    }
    return key.to_string();
}

}  // namespace

std::string FQPathElement::to_string() const {
    if (std::holds_alternative<std::string>(element)) {
        return std::get<std::string>(element);
    }
    return std::to_string(std::get<size_t>(element));
}

std::string FQPath::to_string() const {
    std::ostringstream oss;
    oss << "node=" << node_id << (port_type == PortType::INPUT ? ":in" : ":out");
    for (const auto& elem : path) {
        oss << "/" << elem.to_string();
    }
    return oss.str();
}

ShortPath ShortPath::child(size_t index) const {
    ShortPath next = *this;
    next.indices.push_back(index);
    return next;
}

FQPath ShortPath::to_fq() const {
    FQPath out;
    out.port_type = port_type;
    out.node_id = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(node));
    out.path.reserve(indices.size());
    for (size_t index : indices) {
        out.path.emplace_back(FQPathElement::index(index));
    }
    return out;
}

FQPath ShortPath::to_fq(const ViewData& root) const {
    FQPath out;
    out.port_type = port_type;
    const node_ptr resolved_node = node != nullptr ? node : root.path.node;
    out.node_id = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(resolved_node));

    const auto& root_indices = root.path.indices;
    if (indices.size() < root_indices.size() ||
        !std::equal(root_indices.begin(), root_indices.end(), indices.begin())) {
        out.path.reserve(indices.size());
        for (size_t index : indices) {
            out.path.emplace_back(FQPathElement::index(index));
        }
        return out;
    }

    out.path.reserve(indices.size());
    for (size_t i = 0; i < root_indices.size(); ++i) {
        out.path.emplace_back(FQPathElement::index(indices[i]));
    }

    const TSMeta* meta = root.meta;
    std::optional<View> current_value;
    if (const auto* root_value = static_cast<const value::Value*>(root.value_data);
        root_value != nullptr && root_value->has_value()) {
        current_value = root_value->view();
    }

    for (size_t i = root_indices.size(); i < indices.size(); ++i) {
        const size_t index = indices[i];

        bool converted = false;
        std::optional<View> map_key;
        if (meta != nullptr) {
            if (meta->kind == TSKind::TSB && meta->fields() != nullptr && index < meta->field_count() &&
                meta->fields()[index].name != nullptr) {
                out.path.emplace_back(FQPathElement::field(meta->fields()[index].name));
                converted = true;
            } else if (meta->kind == TSKind::TSD && current_value.has_value()) {
                map_key = map_key_for_index(*current_value, index);
                if (map_key.has_value()) {
                    out.path.emplace_back(FQPathElement::field(key_to_path_string(*map_key)));
                    converted = true;
                }
            }
        }

        if (!converted) {
            out.path.emplace_back(FQPathElement::index(index));
        }

        if (current_value.has_value()) {
            if (meta != nullptr && meta->kind == TSKind::TSD && map_key.has_value()) {
                const value::MapView map_view = current_value->as_map();
                current_value = map_view.at(*map_key);
            } else {
                current_value = child_value_by_index(*current_value, index);
            }
        }

        meta = meta_child(meta, index);
    }

    return out;
}

std::string ShortPath::to_string() const {
    return to_fq().to_string();
}

LinkTarget* ViewData::as_link_target() const {
#ifndef NDEBUG
    if (!uses_link_target) {
        throw std::runtime_error("ViewData::as_link_target called when uses_link_target=false");
    }
#endif
    return static_cast<LinkTarget*>(link_data);
}

REFLink* ViewData::as_ref_link() const {
#ifndef NDEBUG
    if (uses_link_target) {
        throw std::runtime_error("ViewData::as_ref_link called when uses_link_target=true");
    }
#endif
    return static_cast<REFLink*>(link_data);
}

void debug_assert_view_data_consistency(const ViewData& vd) {
#ifndef NDEBUG
    // Contract checks required by TSI-02.
    if (vd.meta == nullptr) {
        // Allow null meta only for null/empty placeholder views.
        if (!vd.is_null()) {
            throw std::runtime_error("ViewData has non-null pointers with null meta");
        }
        return;
    }

    if (vd.ops == nullptr) {
        throw std::runtime_error("ViewData has meta but null ts_ops pointer");
    }

    if (vd.link_data != nullptr) {
        if (vd.uses_link_target) {
            (void)vd.as_link_target();
        } else {
            (void)vd.as_ref_link();
        }
    }
#endif
}

}  // namespace hgraph
