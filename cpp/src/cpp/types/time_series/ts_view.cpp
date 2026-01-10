//
// ts_view.cpp - TSView, TSMutableView, TSBView implementation
//

#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_ref_target_link.h>
#include <hgraph/types/value/window_storage_ops.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ref.h>
#include <fmt/format.h>
#include <stdexcept>
#include <unordered_map>

namespace hgraph {

// External cache for REF output values (defined in py_ref.cpp)
// Key is the TSValue pointer, value is the stored TimeSeriesReference
extern std::unordered_map<const TSValue*, nb::object> g_ref_output_cache;

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

    // Dispatch based on type kind for collection types
    switch (_ts_meta->kind()) {
        case TSTypeKind::TSB:
            return as_bundle().to_python();
        case TSTypeKind::TSL:
            return as_list().to_python();
        case TSTypeKind::TSD:
            return as_dict().to_python();
        case TSTypeKind::TSS:
            return as_set().to_python();
        case TSTypeKind::REF: {
            // For REF inputs with link support, navigate the link to find the bound output
            if (_link_source && _link_source->has_link_support() && !_path.is_root()) {
                // Navigate to find the link at our path position
                const TSValue* current = _link_source;
                for (size_t i = 0; i + 1 < _path.elements.size(); ++i) {
                    current = current->child_value(_path.elements[i]);
                    if (!current) {
                        return nb::cast(TimeSeriesReference::make());
                    }
                }

                size_t final_idx = _path.elements.back();
                const LinkStorage* storage = current->link_storage_at(final_idx);
                if (storage) {
                    // Extract the bound output from the link
                    const TSValue* bound_output = std::visit([](const auto& link) -> const TSValue* {
                        using T = std::decay_t<decltype(link)>;
                        if constexpr (std::is_same_v<T, std::monostate>) {
                            return nullptr;
                        } else if constexpr (std::is_same_v<T, std::unique_ptr<TSLink>>) {
                            return link ? link->output() : nullptr;
                        } else if constexpr (std::is_same_v<T, std::unique_ptr<TSRefTargetLink>>) {
                            return link ? link->ref_output() : nullptr;
                        } else {
                            return nullptr;
                        }
                    }, *storage);

                    if (bound_output) {
                        // Check cache for this bound output - it contains a TimeSeriesReference
                        auto it = g_ref_output_cache.find(bound_output);
                        if (it != g_ref_output_cache.end()) {
                            // AUTOMATIC DEREFERENCING: if the cached value is a TimeSeriesReference,
                            // follow it to get the actual value (mimics Python's rebinding behavior)
                            nb::object cached = it->second;
                            if (nb::isinstance<TimeSeriesReference>(cached)) {
                                TimeSeriesReference ref = nb::cast<TimeSeriesReference>(cached);
                                // Check VIEW_BOUND first since is_bound() returns true for both
                                if (ref.is_view_bound()) {
                                    // VIEW_BOUND reference: follow to the TSValue
                                    const TSValue* target = ref.view_output();
                                    if (target && target->ts_valid()) {
                                        // Create a view of the target and get its value
                                        TSView target_view(*target);
                                        return target_view.to_python();
                                    }
                                } else if (ref.kind() == TimeSeriesReference::Kind::BOUND) {
                                    // BOUND reference: follow to the legacy output
                                    const auto& output = ref.output();
                                    if (output && output->valid()) {
                                        return output->py_delta_value();
                                    }
                                }
                            }
                            // If dereferencing failed or not applicable, return as-is
                            return cached;
                        }
                        // Create view-bound reference to the bound output
                        return nb::cast(TimeSeriesReference::make_view_bound(bound_output));
                    }
                }
            }

            // Fallback: For REF outputs (not linked inputs), check the cache using root
            if (_root) {
                auto it = g_ref_output_cache.find(_root);
                if (it != g_ref_output_cache.end()) {
                    // Found in cache - need to dereference if it's a TimeSeriesReference
                    nb::object cached = it->second;
                    if (nb::isinstance<TimeSeriesReference>(cached)) {
                        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(cached);
                        // Check VIEW_BOUND first since is_bound() returns true for both BOUND and VIEW_BOUND
                        if (ref.is_view_bound()) {
                            const TSValue* target = ref.view_output();
                            if (target && target->ts_valid()) {
                                TSView target_view(*target);
                                return target_view.to_python();
                            }
                        } else if (ref.kind() == TimeSeriesReference::Kind::BOUND) {
                            const auto& output = ref.output();
                            if (output && output->valid()) {
                                return output->py_delta_value();
                            }
                        }
                    }
                    // Not a TimeSeriesReference or dereferencing failed, return as-is
                    return cached;
                }
                // Not in cache - create a view-bound reference
                return nb::cast(TimeSeriesReference::make_view_bound(_root));
            }
            // No root - return empty reference
            return nb::cast(TimeSeriesReference::make());
        }
        default:
            break;
    }

    // For scalar types (TS, SIGNAL, TSW), use direct conversion
    if (_container) {
        return _container->to_python();
    }
    const value::TypeMeta* schema = _ts_meta->value_schema();
    return schema->ops->to_python(_view.data(), schema);
}

nb::object TSView::to_python_delta() const {
    if (!valid()) {
        return nb::none();
    }

    // Dispatch based on type kind for collection types
    switch (_ts_meta->kind()) {
        case TSTypeKind::TSB:
            return as_bundle().to_python_delta();
        case TSTypeKind::TSL:
            return as_list().to_python_delta();
        case TSTypeKind::TSD:
            return as_dict().to_python_delta();
        case TSTypeKind::TSS:
            return as_set().to_python_delta();
        case TSTypeKind::TSW: {
            // For Window types, return just the newest value (most recent addition)
            const value::TypeMeta* schema = _ts_meta->value_schema();
            if (schema->kind == value::TypeKind::Window) {
                return value::WindowStorageOps::get_newest_value_python(_view.data(), schema);
            }
            break;
        }
        case TSTypeKind::REF:
            // For REF types, delta_value is the same as value
            return to_python();
        default:
            break;
    }

    // For other scalar types, delta_value == value
    return to_python();
}

bool TSView::ts_valid() const {
    if (_overlay) {
        return _overlay->last_modified_time() != MIN_DT;
    }
    // No overlay means no time-series tracking
    return valid();
}

bool TSView::all_valid() const {
    if (!ts_valid()) {
        return false;
    }
    // Dispatch based on type kind
    switch (_ts_meta->kind()) {
        case TSTypeKind::TSB:
            return as_bundle().all_valid();
        case TSTypeKind::TSL:
            return as_list().all_valid();
        case TSTypeKind::TSD:
            return as_dict().all_valid();
        case TSTypeKind::TSW: {
            // For TSW, all_valid means size >= min_size
            const value::TypeMeta* schema = _ts_meta->value_schema();
            if (schema->kind == value::TypeKind::Window) {
                auto* storage = static_cast<const value::WindowStorage*>(_view.data());
                return storage->size >= storage->min_size;
            }
            return true;  // Time-delta windows (fallback)
        }
        default:
            // For scalar types (TS, TSS, SIGNAL, REF), all_valid == ts_valid
            return true;
    }
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
    // First check container (direct owner)
    if (_container) {
        return _container->owning_node();
    }
    // Fall back to root (for child views navigated via path)
    if (_root) {
        return _root->owning_node();
    }
    return nullptr;  // No container or root means no node ownership
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

    // Special handling for REF types: store TimeSeriesReference in cache
    if (_ts_meta->kind() == TSTypeKind::REF) {
        // REF outputs store TimeSeriesReference objects, not raw values
        // We can't store Python objects in the C++ value storage (wrong schema),
        // so we use a global cache keyed by the root TSValue pointer

        if (src.is_none()) {
            // Clear/invalidate - remove from cache
            if (_root) {
                g_ref_output_cache.erase(_root);
            }
            return;
        }

        // Check that we got a TimeSeriesReference (by checking type name)
        std::string type_name = nb::cast<std::string>(src.type().attr("__name__"));
        if (type_name.find("TimeSeriesReference") == std::string::npos &&
            type_name.find("BoundTimeSeriesReference") == std::string::npos &&
            type_name.find("EmptyTimeSeriesReference") == std::string::npos &&
            type_name.find("UnBoundTimeSeriesReference") == std::string::npos) {
            throw std::runtime_error(
                "TSMutableView::from_python: REF output value must be a TimeSeriesReference, got " +
                type_name);
        }

        // Store in global cache
        if (_root) {
            g_ref_output_cache[_root] = src;
        } else if (_mutable_container) {
            g_ref_output_cache[_mutable_container] = src;
        }

        // Note: modification notification is handled by caller (notify_modified)
        return;
    }

    // Special handling for TSW types: use push_back semantics
    if (_ts_meta->kind() == TSTypeKind::TSW) {
        // Get the current engine time from the owning node
        engine_time_t current_time = MIN_DT;
        Node* node = owning_node();
        if (node) {
            graph_ptr g = node->graph();
            if (g) {
                current_time = g->evaluation_time();
            }
        }

        // Use WindowStorageOps::push_back_python to append (value, time)
        const value::TypeMeta* schema = _ts_meta->value_schema();
        if (schema && schema->kind == value::TypeKind::Window) {
            value::WindowStorageOps::push_back_python(
                _mutable_view.data(), src, current_time, schema);
            return;
        }
        // Fall through to default handling if schema kind doesn't match
    }

    // Special handling for TSB with dict input: need to mark child overlays
    if (_ts_meta->kind() == TSTypeKind::TSB && nb::isinstance<nb::dict>(src)) {
        // Get the CompositeTSOverlay to mark children
        CompositeTSOverlay* composite_overlay = nullptr;
        if (_overlay) {
            composite_overlay = dynamic_cast<CompositeTSOverlay*>(_overlay);
        }

        // If we have mutable container access, use its policy-aware from_python
        if (_mutable_container) {
            _mutable_container->from_python(src);
        } else {
            // No container - use direct conversion
            const value::TypeMeta* schema = _ts_meta->value_schema();
            if (schema && schema->ops) {
                schema->ops->from_python(_mutable_view.data(), src, schema);
            }
        }

        // Now mark the child overlays that were set
        if (composite_overlay) {
            // Get current time for marking
            engine_time_t current_time = MIN_DT;
            Node* node = owning_node();
            if (node) {
                graph_ptr g = node->graph();
                if (g) {
                    current_time = g->evaluation_time();
                }
            }

            // Mark each child overlay from the dict keys
            nb::dict d = nb::cast<nb::dict>(src);
            const TSBTypeMeta* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);
            for (auto item : d) {
                // Convert field name to index
                std::string field_name = nb::cast<std::string>(item.first);
                const TSBFieldInfo* field_info = bundle_meta->field(field_name);
                if (field_info) {
                    TSOverlayStorage* child_ov = composite_overlay->child(field_info->index);
                    if (child_ov) {
                        child_ov->mark_modified(current_time);
                    }
                }
            }
        }
        return;
    }

    // Special handling for TSL with dict input: need to mark child overlays
    if (_ts_meta->kind() == TSTypeKind::TSL && nb::isinstance<nb::dict>(src)) {
        // Get the ListTSOverlay to mark children
        ListTSOverlay* list_overlay = nullptr;
        if (_overlay) {
            list_overlay = dynamic_cast<ListTSOverlay*>(_overlay);
        }

        // If we have mutable container access, use its policy-aware from_python
        if (_mutable_container) {
            _mutable_container->from_python(src);
        } else {
            // No container - use direct conversion
            const value::TypeMeta* schema = _ts_meta->value_schema();
            if (schema && schema->ops) {
                schema->ops->from_python(_mutable_view.data(), src, schema);
            }
        }

        // Now mark the child overlays that were set
        if (list_overlay) {
            // Get current time for marking
            engine_time_t current_time = MIN_DT;
            Node* node = owning_node();
            if (node) {
                graph_ptr g = node->graph();
                if (g) {
                    current_time = g->evaluation_time();
                }
            }

            // Mark each child overlay from the dict keys
            nb::dict d = nb::cast<nb::dict>(src);
            for (auto item : d) {
                size_t idx = nb::cast<size_t>(item.first);
                TSOverlayStorage* child_ov = list_overlay->child(idx);
                if (child_ov) {
                    child_ov->mark_modified(current_time);
                }
            }
        }
        return;
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

TSBView::TSBView(const TSValue& ts_value) noexcept
    : TSView(ts_value)
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
    // For REF types, don't follow the link - the REF wrapper handles link navigation
    // to return a TimeSeriesReference. For non-REF types, follow the link to get the value.
    if (_link_source && _link_source->is_linked(index) && !field_info->type->is_reference()) {
        // Follow the link - return view into the linked output
        // Check for both TSLink and TSRefTargetLink
        LinkStorage* storage = const_cast<TSValue*>(_link_source)->link_storage_at(index);
        if (storage) {
            return link_storage_view(*storage);
        }
    }

    // Navigate to the field data using the bundle view
    value::ConstBundleView bundle_view = _view.as_bundle();
    value::ConstValueView field_value = bundle_view.at(name);

    // Extend path with field ordinal
    LightweightPath child_path = _path.with(index);

    // ===== Link Support: Check for nested TSValue with links =====
    const TSValue* child_link_source = nullptr;
    if (_link_source) {
        if (field_info->type->is_reference()) {
            // For REF types, keep the parent as link source so the REF wrapper
            // can access the link at this field's index via the path
            child_link_source = _link_source;
        } else {
            // For non-REF types, check if there's a nested non-peered child with link support
            child_link_source = const_cast<TSValue*>(_link_source)->child_value(index);
        }
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

    const TSBFieldInfo& field_info = bundle_meta->field(index);

    // ===== Link Support: Check if this child is linked =====
    // For REF types, don't follow the link - the REF wrapper handles link navigation
    // to return a TimeSeriesReference. For non-REF types, follow the link to get the value.
    if (_link_source && _link_source->is_linked(index) && !field_info.type->is_reference()) {
        // Follow the link - return view into the linked output
        // Check for both TSLink and TSRefTargetLink
        LinkStorage* storage = const_cast<TSValue*>(_link_source)->link_storage_at(index);
        if (storage) {
            return link_storage_view(*storage);
        }
    }

    // Navigate to the field data using the bundle view by name
    value::ConstBundleView bundle_view = _view.as_bundle();
    value::ConstValueView field_value = bundle_view.at(field_info.name);

    // Extend path with field ordinal
    LightweightPath child_path = _path.with(index);

    // ===== Link Support: Check for nested TSValue with links =====
    const TSValue* child_link_source = nullptr;
    if (_link_source) {
        if (field_info.type->is_reference()) {
            // For REF types, keep the parent as link source so the REF wrapper
            // can access the link at this field's index via the path
            child_link_source = _link_source;
        } else {
            // For non-REF types, check if there's a nested non-peered child with link support
            child_link_source = const_cast<TSValue*>(_link_source)->child_value(index);
        }
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

value::ConstValueView TSSView::const_iterator::operator*() const {
    if (!_view || !_view->valid()) {
        throw std::runtime_error("TSSView::const_iterator::operator*(): invalid view");
    }
    value::ConstSetView set_view = _view->value_view().as_set();
    return set_view.at(_index);
}

std::vector<value::ConstValueView> TSSView::values() const {
    std::vector<value::ConstValueView> result;
    if (!valid()) return result;

    result.reserve(size());
    for (auto it = begin(); it != end(); ++it) {
        result.push_back(*it);
    }
    return result;
}

// ============================================================================
// TSBView Iteration Implementation
// ============================================================================

std::vector<std::string_view> TSBView::keys() const {
    std::vector<std::string_view> result;
    if (!valid()) return result;

    const auto* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);
    result.reserve(bundle_meta->field_count());

    for (size_t i = 0; i < bundle_meta->field_count(); ++i) {
        result.push_back(bundle_meta->field(i).name);
    }
    return result;
}

std::vector<TSView> TSBView::ts_values() const {
    std::vector<TSView> result;
    if (!valid()) return result;

    const auto* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);
    result.reserve(bundle_meta->field_count());

    for (size_t i = 0; i < bundle_meta->field_count(); ++i) {
        result.push_back(field(i));
    }
    return result;
}

std::vector<std::pair<std::string_view, TSView>> TSBView::items() const {
    std::vector<std::pair<std::string_view, TSView>> result;
    if (!valid()) return result;

    const auto* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);
    result.reserve(bundle_meta->field_count());

    for (size_t i = 0; i < bundle_meta->field_count(); ++i) {
        result.emplace_back(bundle_meta->field(i).name, field(i));
    }
    return result;
}

std::vector<std::string_view> TSBView::valid_keys() const {
    std::vector<std::string_view> result;
    if (!valid()) return result;

    const auto* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);

    for (size_t i = 0; i < bundle_meta->field_count(); ++i) {
        TSView field_view = field(i);
        if (field_view.ts_valid()) {
            result.push_back(bundle_meta->field(i).name);
        }
    }
    return result;
}

std::vector<TSView> TSBView::valid_values() const {
    std::vector<TSView> result;
    if (!valid()) return result;

    const auto* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);

    for (size_t i = 0; i < bundle_meta->field_count(); ++i) {
        TSView field_view = field(i);
        if (field_view.ts_valid()) {
            result.push_back(field_view);
        }
    }
    return result;
}

std::vector<std::pair<std::string_view, TSView>> TSBView::valid_items() const {
    std::vector<std::pair<std::string_view, TSView>> result;
    if (!valid()) return result;

    const auto* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);

    for (size_t i = 0; i < bundle_meta->field_count(); ++i) {
        TSView field_view = field(i);
        if (field_view.ts_valid()) {
            result.emplace_back(bundle_meta->field(i).name, field_view);
        }
    }
    return result;
}

// ============================================================================
// TSLView Iteration Implementation
// ============================================================================

std::vector<TSView> TSLView::ts_values() const {
    std::vector<TSView> result;
    if (!valid()) return result;

    size_t count = size();
    result.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        result.push_back(element(i));
    }
    return result;
}

std::vector<TSView> TSLView::valid_values() const {
    std::vector<TSView> result;
    if (!valid()) return result;

    size_t count = size();

    for (size_t i = 0; i < count; ++i) {
        TSView elem_view = element(i);
        if (elem_view.ts_valid()) {
            result.push_back(elem_view);
        }
    }
    return result;
}

std::vector<size_t> TSLView::valid_indices() const {
    std::vector<size_t> result;
    if (!valid()) return result;

    size_t count = size();

    for (size_t i = 0; i < count; ++i) {
        TSView elem_view = element(i);
        if (elem_view.ts_valid()) {
            result.push_back(i);
        }
    }
    return result;
}

std::vector<std::pair<size_t, TSView>> TSLView::valid_items() const {
    std::vector<std::pair<size_t, TSView>> result;
    if (!valid()) return result;

    size_t count = size();

    for (size_t i = 0; i < count; ++i) {
        TSView elem_view = element(i);
        if (elem_view.ts_valid()) {
            result.emplace_back(i, elem_view);
        }
    }
    return result;
}

// ============================================================================
// TSDView Iteration Implementation
// ============================================================================

std::vector<value::ConstValueView> TSDView::keys() const {
    std::vector<value::ConstValueView> result;
    if (!valid()) return result;

    value::ConstMapView map_view = _view.as_map();
    size_t count = map_view.size();
    result.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        result.push_back(map_view.key_at(i));
    }
    return result;
}

std::vector<TSView> TSDView::ts_values() const {
    std::vector<TSView> result;
    if (!valid()) return result;

    value::ConstMapView map_view = _view.as_map();
    size_t count = map_view.size();
    result.reserve(count);

    const auto* dict_meta = static_cast<const TSDTypeMeta*>(_ts_meta);
    const TSMeta* value_ts_type = dict_meta->value_ts_type();

    for (size_t i = 0; i < count; ++i) {
        value::ConstValueView value_view = map_view.value_at(i);
        LightweightPath child_path = _path.with(i);

        if (auto* map_ov = map_overlay()) {
            TSOverlayStorage* value_ov = map_ov->value_overlay(i);
            result.emplace_back(value_view.data(), value_ts_type, value_ov, _root, std::move(child_path));
        } else {
            result.emplace_back(value_view.data(), value_ts_type, nullptr, _root, std::move(child_path));
        }
    }
    return result;
}

std::vector<std::pair<value::ConstValueView, TSView>> TSDView::items() const {
    std::vector<std::pair<value::ConstValueView, TSView>> result;
    if (!valid()) return result;

    value::ConstMapView map_view = _view.as_map();
    size_t count = map_view.size();
    result.reserve(count);

    const auto* dict_meta = static_cast<const TSDTypeMeta*>(_ts_meta);
    const TSMeta* value_ts_type = dict_meta->value_ts_type();

    for (size_t i = 0; i < count; ++i) {
        value::ConstValueView key_view = map_view.key_at(i);
        value::ConstValueView value_view = map_view.value_at(i);
        LightweightPath child_path = _path.with(i);

        TSView ts_val;
        if (auto* map_ov = map_overlay()) {
            TSOverlayStorage* value_ov = map_ov->value_overlay(i);
            ts_val = TSView(value_view.data(), value_ts_type, value_ov, _root, std::move(child_path));
        } else {
            ts_val = TSView(value_view.data(), value_ts_type, nullptr, _root, std::move(child_path));
        }
        result.emplace_back(key_view, ts_val);
    }
    return result;
}

std::vector<value::ConstValueView> TSDView::valid_keys() const {
    std::vector<value::ConstValueView> result;
    if (!valid()) return result;

    value::ConstMapView map_view = _view.as_map();
    size_t count = map_view.size();

    const auto* dict_meta = static_cast<const TSDTypeMeta*>(_ts_meta);
    const TSMeta* value_ts_type = dict_meta->value_ts_type();

    for (size_t i = 0; i < count; ++i) {
        value::ConstValueView value_view = map_view.value_at(i);

        // Create TSView to check validity
        TSView ts_val;
        if (auto* map_ov = map_overlay()) {
            TSOverlayStorage* value_ov = map_ov->value_overlay(i);
            ts_val = TSView(value_view.data(), value_ts_type, value_ov);
        } else {
            ts_val = TSView(value_view.data(), value_ts_type);
        }

        if (ts_val.ts_valid()) {
            result.push_back(map_view.key_at(i));
        }
    }
    return result;
}

std::vector<TSView> TSDView::valid_values() const {
    std::vector<TSView> result;
    if (!valid()) return result;

    value::ConstMapView map_view = _view.as_map();
    size_t count = map_view.size();

    const auto* dict_meta = static_cast<const TSDTypeMeta*>(_ts_meta);
    const TSMeta* value_ts_type = dict_meta->value_ts_type();

    for (size_t i = 0; i < count; ++i) {
        value::ConstValueView value_view = map_view.value_at(i);
        LightweightPath child_path = _path.with(i);

        TSView ts_val;
        if (auto* map_ov = map_overlay()) {
            TSOverlayStorage* value_ov = map_ov->value_overlay(i);
            ts_val = TSView(value_view.data(), value_ts_type, value_ov, _root, std::move(child_path));
        } else {
            ts_val = TSView(value_view.data(), value_ts_type, nullptr, _root, std::move(child_path));
        }

        if (ts_val.ts_valid()) {
            result.push_back(ts_val);
        }
    }
    return result;
}

std::vector<std::pair<value::ConstValueView, TSView>> TSDView::valid_items() const {
    std::vector<std::pair<value::ConstValueView, TSView>> result;
    if (!valid()) return result;

    value::ConstMapView map_view = _view.as_map();
    size_t count = map_view.size();

    const auto* dict_meta = static_cast<const TSDTypeMeta*>(_ts_meta);
    const TSMeta* value_ts_type = dict_meta->value_ts_type();

    for (size_t i = 0; i < count; ++i) {
        value::ConstValueView key_view = map_view.key_at(i);
        value::ConstValueView value_view = map_view.value_at(i);
        LightweightPath child_path = _path.with(i);

        TSView ts_val;
        if (auto* map_ov = map_overlay()) {
            TSOverlayStorage* value_ov = map_ov->value_overlay(i);
            ts_val = TSView(value_view.data(), value_ts_type, value_ov, _root, std::move(child_path));
        } else {
            ts_val = TSView(value_view.data(), value_ts_type, nullptr, _root, std::move(child_path));
        }

        if (ts_val.ts_valid()) {
            result.emplace_back(key_view, ts_val);
        }
    }
    return result;
}

// ============================================================================
// all_valid() Implementations
// ============================================================================

bool TSBView::all_valid() const {
    if (!valid()) return false;

    const auto* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);
    for (size_t i = 0; i < bundle_meta->field_count(); ++i) {
        TSView field_view = field(i);
        if (!field_view.ts_valid()) {
            return false;
        }
    }
    return true;
}

bool TSLView::all_valid() const {
    if (!valid()) return false;

    size_t count = size();
    for (size_t i = 0; i < count; ++i) {
        TSView elem_view = element(i);
        if (!elem_view.ts_valid()) {
            return false;
        }
    }
    return true;
}

bool TSDView::all_valid() const {
    if (!valid()) return false;

    value::ConstMapView map_view = _view.as_map();
    size_t count = map_view.size();

    const auto* dict_meta = static_cast<const TSDTypeMeta*>(_ts_meta);
    const TSMeta* value_ts_type = dict_meta->value_ts_type();

    for (size_t i = 0; i < count; ++i) {
        value::ConstValueView value_view = map_view.value_at(i);

        TSView ts_val;
        if (auto* map_ov = map_overlay()) {
            TSOverlayStorage* value_ov = map_ov->value_overlay(i);
            ts_val = TSView(value_view.data(), value_ts_type, value_ov);
        } else {
            ts_val = TSView(value_view.data(), value_ts_type);
        }

        if (!ts_val.ts_valid()) {
            return false;
        }
    }
    return true;
}

// ============================================================================
// Python Conversion Methods for Specialized Views
// ============================================================================

nb::object TSBView::to_python() const {
    if (!valid()) return nb::none();

    // Python behavior: {name: field.value if field.valid else None for each field}
    nb::dict result;
    for (auto& key : keys()) {
        TSView field_view = field(std::string(key));
        if (field_view.ts_valid()) {
            // Recursive call - let the field's view handle its own type
            result[nb::cast(std::string(key))] = field_view.to_python();
        } else {
            result[nb::cast(std::string(key))] = nb::none();
        }
    }
    return result;
}

nb::object TSBView::to_python_delta() const {
    if (!valid()) return nb::none();

    // Get current evaluation time
    Node* n = owning_node();
    if (!n || !n->cached_evaluation_time_ptr()) {
        // No time context - return empty dict
        return nb::dict();
    }
    engine_time_t eval_time = *n->cached_evaluation_time_ptr();

    // Python behavior: {name: field.delta_value for modified fields}
    nb::dict result;
    for (auto& key : keys()) {
        TSView field_view = field(std::string(key));
        if (field_view.modified_at(eval_time)) {
            result[nb::cast(std::string(key))] = field_view.to_python_delta();
        }
    }
    return result;
}

nb::object TSLView::to_python() const {
    if (!valid()) return nb::none();

    // Python behavior: tuple(elem.value if elem.valid else None for each element)
    nb::list result;
    size_t count = size();
    for (size_t i = 0; i < count; ++i) {
        TSView elem = element(i);
        if (elem.ts_valid()) {
            // Recursive call - let the element's view handle its own type
            result.append(elem.to_python());
        } else {
            result.append(nb::none());
        }
    }
    return nb::tuple(result);
}

nb::object TSLView::to_python_delta() const {
    if (!valid()) return nb::none();

    // Get current evaluation time
    Node* n = owning_node();
    if (!n || !n->cached_evaluation_time_ptr()) {
        // No time context - return empty dict
        return nb::dict();
    }
    engine_time_t eval_time = *n->cached_evaluation_time_ptr();

    // Python behavior: {i: elem.delta_value for i, elem in enumerate(elements) if elem.modified}
    nb::dict result;
    size_t count = size();
    for (size_t i = 0; i < count; ++i) {
        TSView elem = element(i);
        if (elem.modified_at(eval_time)) {
            result[nb::int_(i)] = elem.to_python_delta();
        }
    }
    return result;
}

nb::object TSDView::to_python() const {
    if (!valid()) return nb::none();

    // Python behavior: {key: value.value if value.valid else None for each entry}
    nb::dict result;
    value::ConstMapView map_view = _view.as_map();
    size_t count = map_view.size();
    const auto* dict_meta = static_cast<const TSDTypeMeta*>(_ts_meta);

    for (size_t i = 0; i < count; ++i) {
        value::ConstValueView key_view = map_view.key_at(i);
        value::ConstValueView val_view = map_view.value_at(i);

        const TSMeta* value_ts_type = dict_meta->value_ts_type();
        TSView ts_val;
        if (auto* map_ov = map_overlay()) {
            TSOverlayStorage* value_ov = map_ov->value_overlay(i);
            ts_val = TSView(val_view.data(), value_ts_type, value_ov);
        } else {
            ts_val = TSView(val_view.data(), value_ts_type);
        }

        nb::object py_key = key_view.to_python();
        if (ts_val.ts_valid()) {
            result[py_key] = ts_val.to_python();
        } else {
            result[py_key] = nb::none();
        }
    }
    return result;
}

nb::object TSDView::to_python_delta() const {
    if (!valid()) return nb::none();

    // For TSD, delta shows added/removed keys and modified values
    // This is more complex - for now return the full dict as delta
    // TODO: Implement proper delta with added_keys, removed_keys, modified_values
    return to_python();
}

nb::object TSSView::to_python() const {
    if (!valid()) return nb::none();

    // Python behavior: frozenset of element values
    nb::set result;
    value::ConstSetView set_view = _view.as_set();
    size_t count = set_view.size();
    const auto* set_meta = static_cast<const TSSTypeMeta*>(_ts_meta);

    for (size_t i = 0; i < count; ++i) {
        value::ConstValueView elem_view = set_view.at(i);
        nb::object py_elem = elem_view.to_python();
        result.add(py_elem);
    }
    return nb::frozenset(result);
}

nb::object TSSView::to_python_delta() const {
    if (!valid()) return nb::none();

    // For TSS, delta shows added/removed elements
    // TODO: Implement proper delta tracking
    // For now, return the full set as delta
    return to_python();
}

}  // namespace hgraph
