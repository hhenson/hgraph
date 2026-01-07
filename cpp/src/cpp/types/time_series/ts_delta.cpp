//
// ts_delta.cpp - Delta views and values implementation
//

#include <hgraph/types/time_series/ts_delta.h>

namespace hgraph {

// ============================================================================
// SetDeltaView Implementation
// ============================================================================

SetDeltaView::SetDeltaView(SetTSOverlay* overlay,
                           value::ConstSetView set_view,
                           const value::TypeMeta* element_schema) noexcept
    : _overlay(overlay)
    , _set_view(set_view)
    , _element_schema(element_schema)
{}

bool SetDeltaView::has_added() const noexcept {
    return _overlay && _overlay->has_added();
}

bool SetDeltaView::has_removed() const noexcept {
    return _overlay && _overlay->has_removed();
}

std::vector<value::ConstValueView> SetDeltaView::added_values() const {
    if (!_overlay || !_set_view.valid()) return {};

    const auto& indices = _overlay->added_indices();
    std::vector<value::ConstValueView> result;
    result.reserve(indices.size());

    for (size_t idx : indices) {
        result.push_back(_set_view.at(idx));
    }
    return result;
}

std::vector<value::ConstValueView> SetDeltaView::removed_values() const {
    if (!_overlay) return {};

    const auto& removed = _overlay->removed_values();
    std::vector<value::ConstValueView> result;
    result.reserve(removed.size());

    for (const auto& val : removed) {
        result.push_back(val.view());
    }
    return result;
}

SetDeltaValue SetDeltaView::to_value() const {
    return SetDeltaValue(*this);
}

// ============================================================================
// SetDeltaValue Implementation
// ============================================================================

SetDeltaValue::Builder::Builder(const value::TypeMeta* element_schema) noexcept
    : _element_schema(element_schema)
{}

SetDeltaValue::Builder& SetDeltaValue::Builder::add(value::PlainValue value) {
    _added.push_back(std::move(value));
    return *this;
}

SetDeltaValue::Builder& SetDeltaValue::Builder::remove(value::PlainValue value) {
    _removed.push_back(std::move(value));
    return *this;
}

SetDeltaValue SetDeltaValue::Builder::build() {
    return SetDeltaValue(_element_schema, std::move(_added), std::move(_removed));
}

SetDeltaValue::SetDeltaValue(const SetDeltaView& view)
    : _element_schema(view.element_schema())
{
    if (!view.valid()) return;

    // Copy added values
    for (const auto& added_view : view.added_values()) {
        value::PlainValue val(_element_schema);
        _element_schema->ops->copy_assign(val.data(), added_view.data(), _element_schema);
        _added.push_back(std::move(val));
    }

    // Copy removed values
    for (const auto& removed_view : view.removed_values()) {
        value::PlainValue val(_element_schema);
        _element_schema->ops->copy_assign(val.data(), removed_view.data(), _element_schema);
        _removed.push_back(std::move(val));
    }
}

SetDeltaValue::SetDeltaValue(const value::TypeMeta* element_schema,
                             std::vector<value::PlainValue> added,
                             std::vector<value::PlainValue> removed) noexcept
    : _element_schema(element_schema)
    , _added(std::move(added))
    , _removed(std::move(removed))
{}

std::vector<value::ConstValueView> SetDeltaValue::added_values() const {
    std::vector<value::ConstValueView> result;
    result.reserve(_added.size());
    for (const auto& val : _added) {
        result.push_back(val.view());
    }
    return result;
}

std::vector<value::ConstValueView> SetDeltaValue::removed_values() const {
    std::vector<value::ConstValueView> result;
    result.reserve(_removed.size());
    for (const auto& val : _removed) {
        result.push_back(val.view());
    }
    return result;
}

// ============================================================================
// MapDeltaView Implementation
// ============================================================================

MapDeltaView::MapDeltaView(MapTSOverlay* overlay,
                           value::ConstMapView map_view,
                           const value::TypeMeta* key_schema,
                           const value::TypeMeta* value_schema,
                           engine_time_t time) noexcept
    : _overlay(overlay)
    , _map_view(map_view)
    , _key_schema(key_schema)
    , _value_schema(value_schema)
    , _time(time)
{}

bool MapDeltaView::has_added() const noexcept {
    return _overlay && _overlay->has_added_keys();
}

bool MapDeltaView::has_removed() const noexcept {
    return _overlay && _overlay->has_removed_keys();
}

bool MapDeltaView::has_modified() const noexcept {
    return _overlay && _overlay->has_modified_keys(_time);
}

std::vector<value::ConstValueView> MapDeltaView::added_keys() const {
    if (!_overlay || !_map_view.valid()) return {};

    const auto& indices = _overlay->added_key_indices();
    std::vector<value::ConstValueView> result;
    result.reserve(indices.size());

    for (size_t idx : indices) {
        result.push_back(_map_view.key_at(idx));
    }
    return result;
}

std::vector<value::ConstValueView> MapDeltaView::added_values() const {
    if (!_overlay || !_map_view.valid()) return {};

    const auto& indices = _overlay->added_key_indices();
    std::vector<value::ConstValueView> result;
    result.reserve(indices.size());

    for (size_t idx : indices) {
        result.push_back(_map_view.value_at(idx));
    }
    return result;
}

std::vector<value::ConstValueView> MapDeltaView::removed_keys() const {
    if (!_overlay) return {};

    const auto& removed = _overlay->removed_key_values();
    std::vector<value::ConstValueView> result;
    result.reserve(removed.size());

    for (const auto& val : removed) {
        result.push_back(val.view());
    }
    return result;
}

std::vector<value::ConstValueView> MapDeltaView::modified_keys() const {
    if (!_overlay || !_map_view.valid()) return {};

    auto indices = _overlay->modified_key_indices(_time);
    std::vector<value::ConstValueView> result;
    result.reserve(indices.size());

    for (size_t idx : indices) {
        result.push_back(_map_view.key_at(idx));
    }
    return result;
}

std::vector<value::ConstValueView> MapDeltaView::modified_values() const {
    if (!_overlay || !_map_view.valid()) return {};

    auto indices = _overlay->modified_key_indices(_time);
    std::vector<value::ConstValueView> result;
    result.reserve(indices.size());

    for (size_t idx : indices) {
        result.push_back(_map_view.value_at(idx));
    }
    return result;
}

const std::vector<std::unique_ptr<TSOverlayStorage>>& MapDeltaView::removed_value_overlays() const {
    static const std::vector<std::unique_ptr<TSOverlayStorage>> empty;
    if (!_overlay) return empty;
    return _overlay->removed_value_overlays();
}

SetDeltaView MapDeltaView::key_delta_view() const {
    if (!_overlay) return SetDeltaView();
    // Create a SetDeltaView that wraps the map's key tracking via KeySetOverlayView
    // This is a simplified version - for full functionality we'd need KeySetOverlay to be a SetTSOverlay
    // For now, return invalid view since KeySetOverlayView isn't a SetTSOverlay
    return SetDeltaView();
}

MapDeltaValue MapDeltaView::to_value() const {
    return MapDeltaValue(*this);
}

// ============================================================================
// MapDeltaValue Implementation
// ============================================================================

MapDeltaValue::Builder::Builder(const value::TypeMeta* key_schema,
                                const value::TypeMeta* value_schema) noexcept
    : _key_schema(key_schema)
    , _value_schema(value_schema)
{}

MapDeltaValue::Builder& MapDeltaValue::Builder::add(value::PlainValue key, value::PlainValue value) {
    _added.push_back(Entry{std::move(key), std::move(value)});
    return *this;
}

MapDeltaValue::Builder& MapDeltaValue::Builder::remove(value::PlainValue key) {
    _removed_keys.push_back(std::move(key));
    return *this;
}

MapDeltaValue::Builder& MapDeltaValue::Builder::modify(value::PlainValue key, value::PlainValue value) {
    _modified.push_back(Entry{std::move(key), std::move(value)});
    return *this;
}

MapDeltaValue MapDeltaValue::Builder::build() {
    return MapDeltaValue(_key_schema, _value_schema, std::move(_added), std::move(_removed_keys), std::move(_modified));
}

MapDeltaValue::MapDeltaValue(const MapDeltaView& view)
    : _key_schema(view.key_schema())
    , _value_schema(view.value_schema())
{
    if (!view.valid()) return;

    // Copy added key-value pairs
    auto added_keys = view.added_keys();
    auto added_vals = view.added_values();
    _added.reserve(added_keys.size());

    for (size_t i = 0; i < added_keys.size(); ++i) {
        value::PlainValue key(_key_schema);
        _key_schema->ops->copy_assign(key.data(), added_keys[i].data(), _key_schema);

        value::PlainValue val(_value_schema);
        _value_schema->ops->copy_assign(val.data(), added_vals[i].data(), _value_schema);

        _added.push_back(Entry{std::move(key), std::move(val)});
    }

    // Copy removed keys
    for (const auto& removed_view : view.removed_keys()) {
        value::PlainValue key(_key_schema);
        _key_schema->ops->copy_assign(key.data(), removed_view.data(), _key_schema);
        _removed_keys.push_back(std::move(key));
    }

    // Copy modified key-value pairs
    auto modified_keys = view.modified_keys();
    auto modified_vals = view.modified_values();
    _modified.reserve(modified_keys.size());

    for (size_t i = 0; i < modified_keys.size(); ++i) {
        value::PlainValue key(_key_schema);
        _key_schema->ops->copy_assign(key.data(), modified_keys[i].data(), _key_schema);

        value::PlainValue val(_value_schema);
        _value_schema->ops->copy_assign(val.data(), modified_vals[i].data(), _value_schema);

        _modified.push_back(Entry{std::move(key), std::move(val)});
    }
}

MapDeltaValue::MapDeltaValue(const value::TypeMeta* key_schema,
                             const value::TypeMeta* value_schema,
                             std::vector<Entry> added,
                             std::vector<value::PlainValue> removed_keys,
                             std::vector<Entry> modified) noexcept
    : _key_schema(key_schema)
    , _value_schema(value_schema)
    , _added(std::move(added))
    , _removed_keys(std::move(removed_keys))
    , _modified(std::move(modified))
{}

std::vector<value::ConstValueView> MapDeltaValue::added_key_views() const {
    std::vector<value::ConstValueView> result;
    result.reserve(_added.size());
    for (const auto& entry : _added) {
        result.push_back(entry.key.view());
    }
    return result;
}

std::vector<value::ConstValueView> MapDeltaValue::added_value_views() const {
    std::vector<value::ConstValueView> result;
    result.reserve(_added.size());
    for (const auto& entry : _added) {
        result.push_back(entry.value.view());
    }
    return result;
}

std::vector<value::ConstValueView> MapDeltaValue::removed_key_views() const {
    std::vector<value::ConstValueView> result;
    result.reserve(_removed_keys.size());
    for (const auto& key : _removed_keys) {
        result.push_back(key.view());
    }
    return result;
}

std::vector<value::ConstValueView> MapDeltaValue::modified_key_views() const {
    std::vector<value::ConstValueView> result;
    result.reserve(_modified.size());
    for (const auto& entry : _modified) {
        result.push_back(entry.key.view());
    }
    return result;
}

std::vector<value::ConstValueView> MapDeltaValue::modified_value_views() const {
    std::vector<value::ConstValueView> result;
    result.reserve(_modified.size());
    for (const auto& entry : _modified) {
        result.push_back(entry.value.view());
    }
    return result;
}

}  // namespace hgraph
