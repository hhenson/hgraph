//
// ts_view.cpp - TSView, TSMutableView, TSBView implementation
//

#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <stdexcept>

namespace hgraph {

// ============================================================================
// TSView Implementation
// ============================================================================

TSView::TSView(const void* data, const TSMeta* ts_meta) noexcept
    : _view(data, ts_meta ? ts_meta->value_schema() : nullptr)
    , _ts_meta(ts_meta)
    , _container(nullptr)
    , _overlay(nullptr)
    , _root(nullptr)
    , _path()
{}

TSView::TSView(const void* data, const TSMeta* ts_meta, const TSValue* container) noexcept
    : _view(data, ts_meta ? ts_meta->value_schema() : nullptr)
    , _ts_meta(ts_meta)
    , _container(container)
    , _overlay(container ? const_cast<TSOverlayStorage*>(container->overlay()) : nullptr)
    , _root(container)  // Container is the root
    , _path()           // Empty path for root
{}


TSView::TSView(const void* data, const TSMeta* ts_meta, TSOverlayStorage* overlay) noexcept
    : _view(data, ts_meta ? ts_meta->value_schema() : nullptr)
    , _ts_meta(ts_meta)
    , _container(nullptr)
    , _overlay(overlay)
    , _root(nullptr)
    , _path()
{}

TSView::TSView(const void* data, const TSMeta* ts_meta, TSOverlayStorage* overlay,
               const TSValue* root, LightweightPath path) noexcept
    : _view(data, ts_meta ? ts_meta->value_schema() : nullptr)
    , _ts_meta(ts_meta)
    , _container(nullptr)
    , _overlay(overlay)
    , _root(root)
    , _path(std::move(path))
{}

TSView::TSView(const TSValue& ts_value)
    : _view(ts_value.value().data(), ts_value.value_schema())
    , _ts_meta(ts_value.ts_meta())
    , _container(&ts_value)
    , _overlay(const_cast<TSOverlayStorage*>(ts_value.overlay()))
    , _root(&ts_value)  // Container is the root
    , _path()           // Empty path for root
{}

bool TSView::valid() const noexcept {
    return _ts_meta != nullptr && _view.valid();
}

TSBView TSView::as_bundle() const {
    if (!valid()) {
        throw std::runtime_error("TSView::as_bundle() called on invalid view");
    }
    if (_ts_meta->kind() != TSTypeKind::TSB) {
        throw std::runtime_error("TSView::as_bundle() called on non-bundle type");
    }
    // Propagate overlay (schema guarantees type match)
    auto* composite = _overlay ? static_cast<CompositeTSOverlay*>(_overlay) : nullptr;
    TSBView result(_view.data(), static_cast<const TSBTypeMeta*>(_ts_meta), composite);
    // Propagate path tracking
    result._root = _root;
    result._path = _path;
    // Propagate link source for transparent link navigation
    result._link_source = _link_source;
    return result;
}

TSLView TSView::as_list() const {
    if (!valid()) {
        throw std::runtime_error("TSView::as_list() called on invalid view");
    }
    if (_ts_meta->kind() != TSTypeKind::TSL) {
        throw std::runtime_error("TSView::as_list() called on non-list type");
    }
    // Propagate overlay (schema guarantees type match)
    auto* list_ov = _overlay ? static_cast<ListTSOverlay*>(_overlay) : nullptr;
    TSLView result(_view.data(), static_cast<const TSLTypeMeta*>(_ts_meta), list_ov);
    // Propagate path tracking
    result._root = _root;
    result._path = _path;
    // Propagate link source for transparent link navigation
    result._link_source = _link_source;
    return result;
}

TSDView TSView::as_dict() const {
    if (!valid()) {
        throw std::runtime_error("TSView::as_dict() called on invalid view");
    }
    if (_ts_meta->kind() != TSTypeKind::TSD) {
        throw std::runtime_error("TSView::as_dict() called on non-dict type");
    }
    // Propagate overlay (schema guarantees type match)
    auto* map_ov = _overlay ? static_cast<MapTSOverlay*>(_overlay) : nullptr;
    TSDView result(_view.data(), static_cast<const TSDTypeMeta*>(_ts_meta), map_ov);
    // Propagate path tracking
    result._root = _root;
    result._path = _path;
    // Propagate link source for transparent link navigation
    result._link_source = _link_source;
    return result;
}

TSSView TSView::as_set() const {
    if (!valid()) {
        throw std::runtime_error("TSView::as_set() called on invalid view");
    }
    if (_ts_meta->kind() != TSTypeKind::TSS) {
        throw std::runtime_error("TSView::as_set() called on non-set type");
    }
    // Propagate overlay (schema guarantees type match)
    auto* set_ov = _overlay ? static_cast<SetTSOverlay*>(_overlay) : nullptr;
    TSSView result(_view.data(), static_cast<const TSSTypeMeta*>(_ts_meta), set_ov);
    // Propagate path tracking
    result._root = _root;
    result._path = _path;
    // Propagate link source for transparent link navigation
    result._link_source = _link_source;
    return result;
}

nb::object TSView::to_python() const {
    if (!valid()) {
        return nb::none();
    }

    // If we have container access, use its policy-aware to_python (uses cache)
    if (_container) {
        return _container->to_python();
    }

    // No container - use direct conversion (no cache)
    const value::TypeMeta* schema = _ts_meta->value_schema();
    return schema->ops->to_python(_view.data(), schema);
}

bool TSView::ts_valid() const {
    if (_overlay) {
        return _overlay->last_modified_time() != MIN_DT;
    }
    // No overlay means no time-series tracking
    return valid();
}

bool TSView::modified_at(engine_time_t time) const {
    if (_overlay) {
        return _overlay->last_modified_time() == time;
    }
    // No overlay means no modification tracking
    return false;
}

bool TSView::modified() const {
    // Get the owning node to access the evaluation time
    Node* node = owning_node();
    if (!node) {
        return false;  // No owning node, can't determine current time
    }

    graph_ptr graph = node->graph();
    if (!graph) {
        return false;  // No graph, can't determine current time
    }

    return modified_at(graph->evaluation_time());
}

engine_time_t TSView::last_modified_time() const {
    if (_overlay) {
        return _overlay->last_modified_time();
    }
    // No overlay means no modification tracking
    return MIN_DT;
}

Node* TSView::owning_node() const {
    if (!_container) {
        return nullptr;  // No container means no node ownership
    }
    return _container->owning_node();
}

std::optional<StoredPath> TSView::stored_path() const {
    if (!_root) {
        return std::nullopt;  // No root means no path tracking
    }

    Node* node = _root->owning_node();
    if (!node) {
        return std::nullopt;  // Root has no owning node
    }

    // Create base stored path from root's node info, including output schema
    StoredPath result(
        node->owning_graph_id(),
        static_cast<size_t>(node->node_ndx()),
        _root->output_id(),
        _root->ts_meta()  // Store the output schema for type checking during expansion
    );

    // Convert lightweight path elements to stored path elements
    // This requires walking the schema AND data to know element types and lookup keys
    const TSMeta* current_meta = _root->ts_meta();
    const void* current_data = _root->value().data();

    for (size_t ordinal : _path.elements) {
        if (!current_meta) {
            throw std::runtime_error("stored_path: lost schema during navigation");
        }

        switch (current_meta->kind()) {
            case TSTypeKind::TSB: {
                // Bundle: convert ordinal to field name
                auto* bundle_meta = static_cast<const TSBTypeMeta*>(current_meta);
                if (ordinal >= bundle_meta->field_count()) {
                    throw std::runtime_error("stored_path: field index out of range");
                }
                const TSBFieldInfo& field_info = bundle_meta->field(ordinal);
                result.elements.push_back(StoredValue::from_string(field_info.name));

                // Navigate data to the field
                value::ConstBundleView bundle_view(current_data, bundle_meta->value_schema());
                current_data = bundle_view.at(field_info.name).data();
                current_meta = field_info.type;
                break;
            }
            case TSTypeKind::TSL: {
                // List: ordinal is the index
                auto* list_meta = static_cast<const TSLTypeMeta*>(current_meta);
                result.elements.push_back(StoredValue::from_index(ordinal));

                // Navigate data to the element
                value::ConstListView list_view(current_data, list_meta->value_schema());
                if (ordinal >= list_view.size()) {
                    throw std::runtime_error("stored_path: list index out of range");
                }
                current_data = list_view.at(ordinal).data();
                current_meta = list_meta->element_type();
                break;
            }
            case TSTypeKind::TSD: {
                // Dict: ordinal is the slot index - look up the actual key
                auto* dict_meta = static_cast<const TSDTypeMeta*>(current_meta);
                const value::TypeMeta* value_schema = dict_meta->value_schema();

                // Build a map view from current position
                value::ConstMapView map_view(current_data, value_schema);
                if (ordinal >= map_view.size()) {
                    throw std::runtime_error("stored_path: TSD slot index out of range");
                }

                // Get the key at this slot and store it as a Value
                value::ConstValueView key_view = map_view.key_at(ordinal);
                result.elements.push_back(StoredValue::from_view(key_view));

                // Navigate to the value for next iteration
                current_data = map_view.value_at(ordinal).data();
                current_meta = dict_meta->value_ts_type();
                break;
            }
            case TSTypeKind::TSS: {
                // Set: elements don't have child time-series, this shouldn't happen
                throw std::runtime_error(
                    "stored_path: TSS elements do not have child time-series");
            }
            default: {
                // Scalar types should not have path elements
                throw std::runtime_error(
                    "stored_path: unexpected path element for scalar type");
            }
        }
    }

    return result;
}

// ============================================================================
// TSMutableView Implementation
// ============================================================================

TSMutableView::TSMutableView(void* data, const TSMeta* ts_meta) noexcept
    : TSView(data, ts_meta)
    , _mutable_view(data, ts_meta ? ts_meta->value_schema() : nullptr)
    , _mutable_container(nullptr)
{}

TSMutableView::TSMutableView(void* data, const TSMeta* ts_meta, TSValue* container) noexcept
    : TSView(data, ts_meta, container)
    , _mutable_view(data, ts_meta ? ts_meta->value_schema() : nullptr)
    , _mutable_container(container)
{}

TSMutableView::TSMutableView(void* data, const TSMeta* ts_meta, TSOverlayStorage* overlay) noexcept
    : TSView(data, ts_meta, overlay)
    , _mutable_view(data, ts_meta ? ts_meta->value_schema() : nullptr)
    , _mutable_container(nullptr)
{}

TSMutableView::TSMutableView(TSValue& ts_value)
    : TSView(ts_value)
    , _mutable_view(ts_value.value().data(), ts_value.value_schema())
    , _mutable_container(&ts_value)
{}

void TSMutableView::copy_from(const TSView& source) {
    if (!valid() || !source.valid()) {
        throw std::runtime_error("TSMutableView::copy_from() called with invalid view");
    }
    // Use the underlying value schema to perform the copy
    const value::TypeMeta* schema = _ts_meta->value_schema();
    if (schema && schema->ops) {
        schema->ops->copy_assign(_mutable_view.data(), source.value_view().data(), schema);
    }
}

void TSMutableView::from_python(const nb::object& src) {
    if (!valid()) {
        throw std::runtime_error("TSMutableView::from_python() called on invalid view");
    }

    // If we have mutable container access, use its policy-aware from_python
    // This properly invalidates Python cache and handles all policies
    if (_mutable_container) {
        _mutable_container->from_python(src);
        return;
    }

    // No container - use direct conversion (no cache invalidation)
    const value::TypeMeta* schema = _ts_meta->value_schema();
    if (schema && schema->ops) {
        schema->ops->from_python(_mutable_view.data(), src, schema);
    }
}

void TSMutableView::notify_modified(engine_time_t time) {
    // Update overlay-based tracking (canonical source)
    if (_overlay) {
        _overlay->mark_modified(time);
    }
    // Notify container for root-level tracking
    if (_mutable_container) {
        _mutable_container->notify_modified(time);
    }
}

void TSMutableView::invalidate_ts() {
    // Update overlay-based tracking (canonical source)
    if (_overlay) {
        _overlay->mark_invalid();
    }
    // Invalidate container for root-level tracking
    if (_mutable_container) {
        _mutable_container->invalidate_ts();
    }
}

// ============================================================================
// TSBView Implementation
// ============================================================================

TSBView::TSBView(const void* data, const TSBTypeMeta* ts_meta) noexcept
    : TSView(data, ts_meta)
{}

TSBView::TSBView(const void* data, const TSBTypeMeta* ts_meta, CompositeTSOverlay* overlay) noexcept
    : TSView(data, ts_meta, static_cast<TSOverlayStorage*>(overlay))
{}

TSView TSBView::field(const std::string& name) const {
    if (!valid()) {
        throw std::runtime_error("TSBView::field() called on invalid view");
    }

    const TSBTypeMeta* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);
    const TSBFieldInfo* field_info = bundle_meta->field(name);

    if (!field_info) {
        throw std::runtime_error("TSBView::field(): field '" + name + "' not found");
    }

    size_t index = field_info->index;

    // ===== Link Support: Check if this child is linked =====
    if (_link_source && _link_source->is_linked(index)) {
        // Follow the link - return view into the linked output
        TSLink* link = const_cast<TSValue*>(_link_source)->link_at(index);
        return link->view();
    }

    // Navigate to the field data using the bundle view
    value::ConstBundleView bundle_view = _view.as_bundle();
    value::ConstValueView field_value = bundle_view.at(name);

    // Extend path with field ordinal
    LightweightPath child_path = _path.with(index);

    // ===== Link Support: Check for nested TSValue with links =====
    const TSValue* child_link_source = nullptr;
    if (_link_source) {
        // Check if there's a nested non-peered child with link support
        child_link_source = const_cast<TSValue*>(_link_source)->child_value(index);
    }

    // Use overlay-based child navigation with path
    if (auto* composite = composite_overlay()) {
        TSOverlayStorage* child_overlay = composite->child(index);
        TSView result(field_value.data(), field_info->type, child_overlay, _root, std::move(child_path));
        result.set_link_source(child_link_source);
        return result;
    }

    // No overlay - return view with path but without tracking
    TSView result(field_value.data(), field_info->type, nullptr, _root, std::move(child_path));
    result.set_link_source(child_link_source);
    return result;
}

TSView TSBView::field(size_t index) const {
    if (!valid()) {
        throw std::runtime_error("TSBView::field() called on invalid view");
    }

    const TSBTypeMeta* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);

    if (index >= bundle_meta->field_count()) {
        throw std::out_of_range("TSBView::field(): index " + std::to_string(index) +
                                " out of range (size=" + std::to_string(bundle_meta->field_count()) + ")");
    }

    // ===== Link Support: Check if this child is linked =====
    if (_link_source && _link_source->is_linked(index)) {
        // Follow the link - return view into the linked output
        TSLink* link = const_cast<TSValue*>(_link_source)->link_at(index);
        return link->view();
    }

    const TSBFieldInfo& field_info = bundle_meta->field(index);

    // Navigate to the field data using the bundle view by name
    value::ConstBundleView bundle_view = _view.as_bundle();
    value::ConstValueView field_value = bundle_view.at(field_info.name);

    // Extend path with field ordinal
    LightweightPath child_path = _path.with(index);

    // ===== Link Support: Check for nested TSValue with links =====
    const TSValue* child_link_source = nullptr;
    if (_link_source) {
        // Check if there's a nested non-peered child with link support
        child_link_source = const_cast<TSValue*>(_link_source)->child_value(index);
    }

    // Use overlay-based child navigation with path
    if (auto* composite = composite_overlay()) {
        TSOverlayStorage* child_overlay = composite->child(index);
        TSView result(field_value.data(), field_info.type, child_overlay, _root, std::move(child_path));
        result.set_link_source(child_link_source);
        return result;
    }

    // No overlay - return view with path but without tracking
    TSView result(field_value.data(), field_info.type, nullptr, _root, std::move(child_path));
    result.set_link_source(child_link_source);
    return result;
}

size_t TSBView::field_count() const noexcept {
    if (!valid()) return 0;
    return static_cast<const TSBTypeMeta*>(_ts_meta)->field_count();
}

bool TSBView::has_field(const std::string& name) const noexcept {
    if (!valid()) return false;
    return static_cast<const TSBTypeMeta*>(_ts_meta)->field(name) != nullptr;
}

BundleDeltaView TSBView::delta_view(engine_time_t time) {
    auto* overlay = composite_overlay();
    if (!overlay || !valid()) {
        return BundleDeltaView();  // Invalid view
    }

    // Get schemas from TSMeta
    const auto* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);
    const value::TypeMeta* bundle_schema = bundle_meta->value_schema();

    return BundleDeltaView(overlay, _view.as_bundle(), bundle_schema, time);
}

CompositeTSOverlay* TSBView::composite_overlay() const noexcept {
    if (!_overlay) return nullptr;
    return static_cast<CompositeTSOverlay*>(_overlay);
}

// ============================================================================
// TSLView Implementation
// ============================================================================

TSLView::TSLView(const void* data, const TSLTypeMeta* ts_meta) noexcept
    : TSView(data, ts_meta)
{}

TSLView::TSLView(const void* data, const TSLTypeMeta* ts_meta, ListTSOverlay* overlay) noexcept
    : TSView(data, ts_meta, static_cast<TSOverlayStorage*>(overlay))
{}

TSView TSLView::element(size_t index) const {
    if (!valid()) {
        throw std::runtime_error("TSLView::element() called on invalid view");
    }

    // ===== Link Support: Check if this element is linked =====
    if (_link_source && _link_source->is_linked(index)) {
        // Follow the link - return view into the linked output
        TSLink* link = const_cast<TSValue*>(_link_source)->link_at(index);
        return link->view();
    }

    const TSLTypeMeta* list_meta_ptr = static_cast<const TSLTypeMeta*>(_ts_meta);
    const TSMeta* element_type = list_meta_ptr->element_type();

    // Navigate to the element data using the list view
    value::ConstListView list_view = _view.as_list();

    if (index >= list_view.size()) {
        throw std::out_of_range("TSLView::element(): index " + std::to_string(index) +
                                " out of range (size=" + std::to_string(list_view.size()) + ")");
    }

    value::ConstValueView element_value = list_view.at(index);

    // Extend path with element index
    LightweightPath child_path = _path.with(index);

    // ===== Link Support: Check for nested TSValue with links =====
    const TSValue* child_link_source = nullptr;
    if (_link_source) {
        // Check if there's a nested non-peered child with link support
        child_link_source = const_cast<TSValue*>(_link_source)->child_value(index);
    }

    // Use overlay-based child navigation with path
    if (auto* list_ov = list_overlay()) {
        TSOverlayStorage* child_overlay = list_ov->child(index);
        TSView result(element_value.data(), element_type, child_overlay, _root, std::move(child_path));
        result.set_link_source(child_link_source);
        return result;
    }

    // No overlay - return view with path but without tracking
    TSView result(element_value.data(), element_type, nullptr, _root, std::move(child_path));
    result.set_link_source(child_link_source);
    return result;
}

size_t TSLView::size() const noexcept {
    if (!valid()) return 0;
    return _view.as_list().size();
}

bool TSLView::is_fixed_size() const noexcept {
    if (!valid()) return false;
    return static_cast<const TSLTypeMeta*>(_ts_meta)->is_fixed_size();
}

size_t TSLView::fixed_size() const noexcept {
    if (!valid()) return 0;
    return static_cast<const TSLTypeMeta*>(_ts_meta)->fixed_size();
}

ListDeltaView TSLView::delta_view(engine_time_t time) {
    auto* overlay = list_overlay();
    if (!overlay || !valid()) {
        return ListDeltaView();  // Invalid view
    }

    // Get element schema from TSMeta
    const auto* list_meta = static_cast<const TSLTypeMeta*>(_ts_meta);
    const value::TypeMeta* element_schema = list_meta->element_type()->value_schema();

    return ListDeltaView(overlay, _view.as_list(), element_schema, time);
}

ListTSOverlay* TSLView::list_overlay() const noexcept {
    if (!_overlay) return nullptr;
    return static_cast<ListTSOverlay*>(_overlay);
}

// ============================================================================
// TSDView Implementation
// ============================================================================

TSDView::TSDView(const void* data, const TSDTypeMeta* ts_meta) noexcept
    : TSView(data, ts_meta)
{}

TSDView::TSDView(const void* data, const TSDTypeMeta* ts_meta, MapTSOverlay* overlay) noexcept
    : TSView(data, ts_meta, static_cast<TSOverlayStorage*>(overlay))
{}

size_t TSDView::size() const noexcept {
    if (!valid()) return 0;
    return _view.as_map().size();
}

MapDeltaView TSDView::delta_view(engine_time_t time) {
    auto* overlay = map_overlay();
    if (!overlay || !valid()) {
        return MapDeltaView();  // Invalid view
    }

    // Trigger lazy cleanup if time has changed
    (void)overlay->has_delta_at(time);

    // Get schemas from TSMeta
    const auto* dict_meta = static_cast<const TSDTypeMeta*>(_ts_meta);
    const value::TypeMeta* key_schema = dict_meta->key_type();
    const value::TypeMeta* value_schema = dict_meta->value_ts_type()->value_schema();

    return MapDeltaView(overlay, _view.as_map(), key_schema, value_schema, time);
}

KeySetOverlayView TSDView::key_set_view() const {
    auto* overlay = map_overlay();
    if (!overlay) {
        return KeySetOverlayView(nullptr);
    }
    return overlay->key_set_view();
}

MapTSOverlay* TSDView::map_overlay() const noexcept {
    if (!_overlay) return nullptr;
    return static_cast<MapTSOverlay*>(_overlay);
}

// ============================================================================
// TSSView Implementation
// ============================================================================

TSSView::TSSView(const void* data, const TSSTypeMeta* ts_meta) noexcept
    : TSView(data, ts_meta)
{}

TSSView::TSSView(const void* data, const TSSTypeMeta* ts_meta, SetTSOverlay* overlay) noexcept
    : TSView(data, ts_meta, static_cast<TSOverlayStorage*>(overlay))
{}

size_t TSSView::size() const noexcept {
    if (!valid()) return 0;
    return _view.as_set().size();
}

SetDeltaView TSSView::delta_view(engine_time_t time) {
    auto* overlay = set_overlay();
    if (!overlay || !valid()) {
        return SetDeltaView();  // Invalid view
    }

    // Trigger lazy cleanup if time has changed
    (void)overlay->has_delta_at(time);

    // Get element schema from TSMeta
    const auto* set_meta = static_cast<const TSSTypeMeta*>(_ts_meta);
    const value::TypeMeta* element_schema = set_meta->element_type();

    return SetDeltaView(overlay, _view.as_set(), element_schema);
}

SetTSOverlay* TSSView::set_overlay() const noexcept {
    if (!_overlay) return nullptr;
    return static_cast<SetTSOverlay*>(_overlay);
}

}  // namespace hgraph
