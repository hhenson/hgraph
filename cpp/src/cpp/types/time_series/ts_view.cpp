//
// ts_view.cpp - TSView, TSMutableView, TSBView implementation
//

#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_ref_target_link.h>
#include <hgraph/types/value/window_storage_ops.h>
#include <hgraph/types/value/composite_ops.h>
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
// Helper function to navigate REF links and find bound output
// ============================================================================

/**
 * @brief Navigate a REF type's link to find the bound output.
 *
 * For REF inputs that are linked to a non-REF output, this function navigates
 * the link structure to find the bound TSValue. This is used to check the
 * bound output's validity and modification state instead of the REF overlay.
 *
 * @param view The TSView to navigate from
 * @return Pointer to the bound TSValue, or nullptr if not found
 */
static const TSValue* navigate_ref_link(const TSView& view) {
    if (!view.ts_meta() || view.ts_meta()->kind() != TSTypeKind::REF) {
        return nullptr;
    }

    // Only navigate if we have link support and a valid path
    if (!view.link_source() || !view.link_source()->has_link_support()) {
        return nullptr;
    }

    const auto& path = view.path();
    if (path.is_root()) {
        return nullptr;
    }

    // Navigate to find the link at our path position
    const TSValue* current = view.link_source();
    for (size_t i = 0; i + 1 < path.elements.size(); ++i) {
        current = current->child_value(path.elements[i]);
        if (!current) {
            return nullptr;
        }
    }

    size_t final_idx = path.elements.back();
    const LinkStorage* storage = current->link_storage_at(final_idx);
    if (!storage) {
        return nullptr;
    }

    // Extract the bound output from the link
    return std::visit([](const auto& link) -> const TSValue* {
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
}

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

            // Fallback: For REF outputs (not linked inputs), check the cache
            if (_root) {
                // Determine the correct cache key:
                // - For root-level REF: use _root
                // - For bundle field REF: use the child TSValue (from path)
                const TSValue* cache_key = _root;
                if (!_path.is_root() && _root->has_link_support() && !_path.elements.empty()) {
                    // Navigate to the child TSValue for bundle field REFs
                    cache_key = _root->child_value(_path.elements.back());
                }

                if (cache_key) {
                    auto it = g_ref_output_cache.find(cache_key);
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
                }
                // Not in cache - create a view-bound reference using the cache key
                return nb::cast(TimeSeriesReference::make_view_bound(cache_key ? cache_key : _root));
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
    // For REF types, check validity based on input links or output cache
    if (_ts_meta && _ts_meta->kind() == TSTypeKind::REF) {
        // For inputs with link support, check the bound output's validity
        const TSValue* bound_output = navigate_ref_link(*this);
        if (bound_output) {
            return bound_output->ts_valid();
        }

        // For outputs (no link), check if the cached TimeSeriesReference is valid
        // Use the same cache key logic as to_python
        if (_root) {
            const TSValue* cache_key = _root;
            if (!_path.is_root() && _root->has_link_support() && !_path.elements.empty()) {
                cache_key = _root->child_value(_path.elements.back());
            }
            if (cache_key) {
                auto it = g_ref_output_cache.find(cache_key);
                if (it != g_ref_output_cache.end()) {
                    // Check if the cached value is a valid (non-empty) TimeSeriesReference
                    nb::object cached = it->second;
                    if (nb::isinstance<TimeSeriesReference>(cached)) {
                        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(cached);
                        // REF is valid if it has output (not empty)
                        return ref.has_output();
                    }
                    // If it's not a TimeSeriesReference, consider it valid
                    return true;
                }
            }
        }
        // No cache entry - not valid
        return false;
    }

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
    // For REF types with link support, check the bound output's modification
    if (_ts_meta && _ts_meta->kind() == TSTypeKind::REF) {
        const TSValue* bound_output = navigate_ref_link(*this);
        if (bound_output) {
            return bound_output->modified_at(time);
        }
    }

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
    // For REF types with link support, get the bound output's last modified time
    if (_ts_meta && _ts_meta->kind() == TSTypeKind::REF) {
        const TSValue* bound_output = navigate_ref_link(*this);
        if (bound_output) {
            return bound_output->last_modified_time();
        }
    }

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
    // and handle REF fields specially
    if (_ts_meta->kind() == TSTypeKind::TSB && nb::isinstance<nb::dict>(src)) {
        const TSBTypeMeta* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);
        nb::dict d = nb::cast<nb::dict>(src);

        // Get the CompositeTSOverlay to mark children
        CompositeTSOverlay* composite_overlay = nullptr;
        if (_overlay) {
            composite_overlay = dynamic_cast<CompositeTSOverlay*>(_overlay);
        }

        // Get current time for marking
        engine_time_t current_time = MIN_DT;
        Node* node = owning_node();
        if (node) {
            graph_ptr g = node->graph();
            if (g) {
                current_time = g->evaluation_time();
            }
        }

        // Check if any field is a REF type - if so, we need to handle each field individually
        bool has_ref_fields = false;
        for (size_t i = 0; i < bundle_meta->field_count(); ++i) {
            if (bundle_meta->field_meta(i)->kind() == TSTypeKind::REF) {
                has_ref_fields = true;
                break;
            }
        }

        if (has_ref_fields) {
            // Handle each field individually to properly support REF fields
            // Ensure link support is enabled on the container for child value access
            if (_mutable_container && !_mutable_container->has_link_support()) {
                _mutable_container->enable_link_support();
            }
            for (auto item : d) {
                std::string field_name = nb::cast<std::string>(item.first);
                const TSBFieldInfo* field_info = bundle_meta->field(field_name);
                if (!field_info) continue;

                const TSMeta* field_ts_meta = bundle_meta->field_meta(field_info->index);
                nb::object field_value = nb::cast<nb::object>(item.second);

                if (field_ts_meta->kind() == TSTypeKind::REF) {
                    // REF field - store in cache using the field's child TSValue
                    if (_mutable_container) {
                        TSValue* field_ts_value = _mutable_container->get_or_create_child_value(field_info->index);
                        if (field_ts_value && !field_value.is_none()) {
                            g_ref_output_cache[field_ts_value] = field_value;
                        } else if (field_ts_value && field_value.is_none()) {
                            g_ref_output_cache.erase(field_ts_value);
                        }
                    }
                } else {
                    // Non-REF field - use value layer via child TSValue
                    if (_mutable_container) {
                        // Get the child TSValue and set its value
                        TSValue* field_ts_value = _mutable_container->get_or_create_child_value(field_info->index);
                        if (field_ts_value) {
                            field_ts_value->from_python(field_value);
                        }
                    }
                    // Note: if no mutable container and has REF fields, we can't handle this case
                    // properly without container access. This shouldn't happen in normal usage.
                }

                // Mark overlay if available
                if (composite_overlay) {
                    TSOverlayStorage* child_ov = composite_overlay->child(field_info->index);
                    if (child_ov) {
                        child_ov->mark_modified(current_time);
                    }
                }
            }
        } else {
            // No REF fields - use existing path through value layer
            if (_mutable_container) {
                _mutable_container->from_python(src);
            } else {
                const value::TypeMeta* schema = _ts_meta->value_schema();
                if (schema && schema->ops) {
                    schema->ops->from_python(_mutable_view.data(), src, schema);
                }
            }

            // Mark child overlays
            if (composite_overlay) {
                for (auto item : d) {
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

    // Special handling for TSD with dict input: track key additions in overlay
    // and set child time-series values properly.
    if (_ts_meta->kind() == TSTypeKind::TSD && nb::isinstance<nb::dict>(src)) {
        MapTSOverlay* map_overlay = nullptr;
        if (_overlay) {
            map_overlay = dynamic_cast<MapTSOverlay*>(_overlay);
        }

        // Get current time for tracking
        engine_time_t current_time = MIN_DT;
        Node* node = owning_node();
        if (node) {
            graph_ptr g = node->graph();
            if (g) {
                current_time = g->evaluation_time();
            }
        }

        const auto* dict_meta = static_cast<const TSDTypeMeta*>(_ts_meta);
        const value::TypeMeta* key_schema = dict_meta->key_type();
        const TSMeta* value_ts_type = dict_meta->value_ts_type();
        const value::TypeMeta* val_schema = value_ts_type ? value_ts_type->value_schema() : nullptr;
        const value::TypeMeta* map_schema = _ts_meta->value_schema();

        nb::dict input_dict = nb::cast<nb::dict>(src);
        value::MapView mut_map(_mutable_view.data(), map_schema);

        for (auto item : input_dict) {
            nb::object py_key = nb::borrow<nb::object>(item.first);
            nb::object py_value = nb::borrow<nb::object>(item.second);

            // Check if value is a REMOVE or REMOVE_IF_EXISTS sentinel
            std::string type_name = nb::cast<std::string>(py_value.type().attr("__name__"));
            bool is_remove = (type_name == "Sentinel");

            // Create key value for operations
            value::PlainValue temp_key(key_schema);
            if (key_schema->ops->from_python) {
                key_schema->ops->from_python(temp_key.data(), py_key, key_schema);
            }

            if (is_remove) {
                // Handle REMOVE marker
                std::string sentinel_name = nb::cast<std::string>(py_value.attr("name"));
                bool is_remove_if_exists = (sentinel_name == "REMOVE_IF_EXISTS");

                auto key_index_opt = mut_map.find_index(temp_key.const_view());

                if (!key_index_opt.has_value() && is_remove_if_exists) {
                    continue;  // REMOVE_IF_EXISTS but key doesn't exist - skip
                }

                if (key_index_opt.has_value()) {
                    size_t key_index = key_index_opt.value();

                    // Record removal in overlay before modifying backing store
                    if (map_overlay) {
                        value::PlainValue removed_key(key_schema);
                        key_schema->ops->copy_assign(removed_key.data(), temp_key.data(), key_schema);
                        map_overlay->record_key_removed(key_index, current_time, std::move(removed_key));
                    }

                    // Erase from backing store
                    if (map_schema->ops->erase) {
                        map_schema->ops->erase(_mutable_view.data(), temp_key.data(), map_schema);
                    }
                }
            } else {
                // Regular value - add/update key and set child time-series value

                // Create a temporary value for set_with_index (will be overwritten by child from_python)
                value::PlainValue temp_val(val_schema);
                if (val_schema->ops && val_schema->ops->construct) {
                    val_schema->ops->construct(temp_val.data(), val_schema);
                }

                // Insert or update the key
                value::MapSetResult result = mut_map.set_with_index(temp_key.const_view(), temp_val.const_view());

                // If newly inserted, record key addition in overlay
                if (result.inserted && map_overlay) {
                    map_overlay->record_key_added(result.index, current_time);
                }

                // Get the value slot at this index and create a child TSMutableView
                auto* storage = static_cast<value::MapStorage*>(_mutable_view.data());
                void* val_ptr = storage->get_value_ptr(result.index);

                // Create/get child overlay for this index (ensure it exists for tracking)
                TSOverlayStorage* child_overlay = nullptr;
                if (map_overlay) {
                    child_overlay = map_overlay->ensure_value_overlay(result.index);
                }

                // Create a TSMutableView for the child time-series
                TSMutableView child_view(val_ptr, value_ts_type, child_overlay);

                // Set the value via the child's from_python
                child_view.from_python(py_value);

                // Mark the child as modified if we have an overlay
                if (child_overlay) {
                    child_overlay->mark_modified(current_time);
                }
            }
        }

        return;
    }

    // Special handling for TSS: compute delta (added/removed) and update overlay
    if (_ts_meta->kind() == TSTypeKind::TSS) {
        SetTSOverlay* set_overlay = nullptr;
        if (_overlay) {
            set_overlay = dynamic_cast<SetTSOverlay*>(_overlay);
        }

        // Get current time for tracking
        engine_time_t current_time = MIN_DT;
        Node* node = owning_node();
        if (node) {
            graph_ptr g = node->graph();
            if (g) {
                current_time = g->evaluation_time();
            }
        }

        // Get the current set view to compare with new value
        value::ConstSetView old_set = _view.as_set();
        size_t old_size = old_set.size();

        // Collect old set elements as Python objects for comparison
        std::vector<nb::object> old_elements;
        old_elements.reserve(old_size);
        for (size_t i = 0; i < old_size; ++i) {
            old_elements.push_back(old_set.at(i).to_python());
        }

        // Convert new value - accept set, frozenset, or PythonSetDelta
        std::string type_name = nb::cast<std::string>(src.type().attr("__name__"));
        bool is_set_delta = (type_name.find("SetDelta") != std::string::npos);

        if (is_set_delta) {
            // Handle PythonSetDelta: apply additions and removals
            nb::object added = src.attr("added");
            nb::object removed = src.attr("removed");

            // Process removals first
            if (set_overlay && !removed.is_none()) {
                for (auto item : removed) {
                    nb::object py_item = nb::borrow<nb::object>(item);
                    // Find index of this item in old set
                    for (size_t i = 0; i < old_elements.size(); ++i) {
                        if (old_elements[i].equal(py_item)) {
                            // Record removal with the value
                            const value::TypeMeta* elem_type = static_cast<const TSSTypeMeta*>(_ts_meta)->element_type();
                            value::PlainValue removed_val(elem_type);
                            if (elem_type && elem_type->ops && elem_type->ops->from_python) {
                                elem_type->ops->from_python(removed_val.data(), py_item, elem_type);
                            }
                            set_overlay->record_removed(i, current_time, std::move(removed_val));
                            break;
                        }
                    }
                }
            }

            // Remove elements from the backing store
            if (!removed.is_none()) {
                const value::TypeMeta* schema = _ts_meta->value_schema();
                for (auto item : removed) {
                    nb::object py_item = nb::borrow<nb::object>(item);
                    // Create temp value for removal
                    const value::TypeMeta* elem_type = schema->element_type;
                    if (elem_type && elem_type->ops) {
                        std::vector<char> temp_storage(elem_type->size);
                        void* temp = temp_storage.data();
                        if (elem_type->ops->construct) {
                            elem_type->ops->construct(temp, elem_type);
                        }
                        if (elem_type->ops->from_python) {
                            elem_type->ops->from_python(temp, py_item, elem_type);
                        }
                        // Erase from set
                        if (schema->ops->erase) {
                            schema->ops->erase(_mutable_view.data(), temp, schema);
                        }
                        if (elem_type->ops->destruct) {
                            elem_type->ops->destruct(temp, elem_type);
                        }
                    }
                }
            }

            // Add elements to the backing store (only if not already present)
            if (!added.is_none()) {
                const value::TypeMeta* schema = _ts_meta->value_schema();
                for (auto item : added) {
                    nb::object py_item = nb::borrow<nb::object>(item);

                    // Check if already in old set - skip if so
                    bool already_exists = false;
                    for (const auto& old_elem : old_elements) {
                        if (old_elem.equal(py_item)) {
                            already_exists = true;
                            break;
                        }
                    }
                    if (already_exists) {
                        continue;  // Skip duplicate - no delta for existing element
                    }

                    // Create temp value for insertion
                    const value::TypeMeta* elem_type = schema->element_type;
                    if (elem_type && elem_type->ops) {
                        std::vector<char> temp_storage(elem_type->size);
                        void* temp = temp_storage.data();
                        if (elem_type->ops->construct) {
                            elem_type->ops->construct(temp, elem_type);
                        }
                        if (elem_type->ops->from_python) {
                            elem_type->ops->from_python(temp, py_item, elem_type);
                        }
                        // Insert into set
                        if (schema->ops->insert) {
                            schema->ops->insert(_mutable_view.data(), temp, schema);
                        }
                        if (set_overlay) {
                            // Get the actual index where it was inserted
                            value::ConstSetView new_set = _view.as_set();
                            for (size_t i = 0; i < new_set.size(); ++i) {
                                if (new_set.at(i).to_python().equal(py_item)) {
                                    // Create PlainValue copy for O(1) lookup tracking
                                    value::PlainValue added_value(elem_type);
                                    if (elem_type->ops->copy_assign) {
                                        elem_type->ops->copy_assign(added_value.data(), temp, elem_type);
                                    }
                                    set_overlay->record_added(i, current_time, std::move(added_value));
                                    break;
                                }
                            }
                        }
                        if (elem_type->ops->destruct) {
                            elem_type->ops->destruct(temp, elem_type);
                        }
                    }
                }
            }
        } else {
            // Handle set/frozenset: compute delta and replace
            std::vector<nb::object> new_elements;
            if (nb::isinstance<nb::set>(src) || nb::isinstance<nb::frozenset>(src)) {
                for (auto item : src) {
                    new_elements.push_back(nb::borrow<nb::object>(item));
                }
            } else if (nb::isinstance<nb::list>(src) || nb::isinstance<nb::tuple>(src)) {
                nb::sequence seq = nb::cast<nb::sequence>(src);
                for (size_t i = 0; i < nb::len(seq); ++i) {
                    new_elements.push_back(nb::borrow<nb::object>(seq[i]));
                }
            } else {
                throw std::runtime_error("TSMutableView::from_python: TSS expects set, frozenset, or SetDelta");
            }

            // Compute delta: removed = old - new, added = new - old
            std::vector<nb::object> removed_items;
            std::vector<nb::object> added_items;

            // Find removed items (in old but not in new)
            for (const auto& old_elem : old_elements) {
                bool found = false;
                for (const auto& new_elem : new_elements) {
                    if (old_elem.equal(new_elem)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    removed_items.push_back(old_elem);
                }
            }

            // Find added items (in new but not in old)
            for (const auto& new_elem : new_elements) {
                bool found = false;
                for (const auto& old_elem : old_elements) {
                    if (new_elem.equal(old_elem)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    added_items.push_back(new_elem);
                }
            }

            // Record removals in overlay (before modifying backing store)
            if (set_overlay) {
                const value::TypeMeta* elem_type = static_cast<const TSSTypeMeta*>(_ts_meta)->element_type();
                for (const auto& py_item : removed_items) {
                    // Find index of this item in old set
                    for (size_t i = 0; i < old_elements.size(); ++i) {
                        if (old_elements[i].equal(py_item)) {
                            value::PlainValue removed_val(elem_type);
                            if (elem_type && elem_type->ops && elem_type->ops->from_python) {
                                elem_type->ops->from_python(removed_val.data(), py_item, elem_type);
                            }
                            set_overlay->record_removed(i, current_time, std::move(removed_val));
                            break;
                        }
                    }
                }
            }

            // Update the backing store with new value
            const value::TypeMeta* schema = _ts_meta->value_schema();
            if (schema && schema->ops && schema->ops->from_python) {
                schema->ops->from_python(_mutable_view.data(), src, schema);
            }

            // Record additions in overlay (after modifying backing store to get correct indices)
            if (set_overlay) {
                value::ConstSetView new_set = _view.as_set();
                const auto* set_meta = static_cast<const TSSTypeMeta*>(_ts_meta);
                const value::TypeMeta* elem_type = set_meta->element_type();
                for (const auto& py_item : added_items) {
                    // Find index of this item in new set
                    for (size_t i = 0; i < new_set.size(); ++i) {
                        if (new_set.at(i).to_python().equal(py_item)) {
                            // Create PlainValue for O(1) lookup tracking
                            value::PlainValue added_value(elem_type);
                            if (elem_type && elem_type->ops && elem_type->ops->from_python) {
                                elem_type->ops->from_python(added_value.data(), py_item, elem_type);
                            }
                            set_overlay->record_added(i, current_time, std::move(added_value));
                            break;
                        }
                    }
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

bool TSDView::contains_python(const nb::object& key) const {
    if (!valid()) return false;

    // Get key schema and use TypeOps::contains for O(1) lookup
    const value::TypeMeta* key_schema = dict_meta()->key_type();
    const value::TypeMeta* map_schema = _ts_meta->value_schema();

    if (!key_schema || !key_schema->ops || !map_schema || !map_schema->ops || !map_schema->ops->contains) {
        return false;
    }

    // Create temp key storage and convert from Python
    std::vector<char> temp_storage(key_schema->size);
    void* temp = temp_storage.data();

    if (key_schema->ops->construct) {
        key_schema->ops->construct(temp, key_schema);
    }
    if (key_schema->ops->from_python) {
        try {
            key_schema->ops->from_python(temp, key, key_schema);
        } catch (...) {
            if (key_schema->ops->destruct) {
                key_schema->ops->destruct(temp, key_schema);
            }
            return false;  // Conversion failed - key can't be in map
        }
    }

    // Use O(1) contains lookup
    bool result = map_schema->ops->contains(_view.data(), temp, map_schema);

    if (key_schema->ops->destruct) {
        key_schema->ops->destruct(temp, key_schema);
    }

    return result;
}

bool TSDView::was_added(const value::ConstValueView& key, engine_time_t time) {
    auto* overlay = map_overlay();
    if (!overlay || !valid() || !overlay->has_delta_at(time)) {
        return false;
    }

    value::ConstMapView map_view = _view.as_map();
    for (size_t idx : overlay->added_key_indices()) {
        if (key == map_view.key_at(idx)) {
            return true;
        }
    }
    return false;
}

bool TSDView::was_added_python(const nb::object& key, engine_time_t time) {
    return was_added(make_value(dict_meta()->key_type(), key).const_view(), time);
}

bool TSDView::was_removed(const value::ConstValueView& key, engine_time_t time) {
    auto* overlay = map_overlay();
    if (!overlay || !valid() || !overlay->has_delta_at(time)) {
        return false;
    }

    for (const auto& k : overlay->removed_key_values()) {
        if (key == k.const_view()) {
            return true;
        }
    }
    return false;
}

bool TSDView::was_removed_python(const nb::object& key, engine_time_t time) {
    return was_removed(make_value(dict_meta()->key_type(), key).const_view(), time);
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

bool TSSView::contains_python(const nb::object& element) const {
    if (!valid()) return false;

    // Get element schema and use TypeOps::contains for O(1) lookup
    const value::TypeMeta* elem_schema = set_meta()->element_type();
    const value::TypeMeta* set_schema = _ts_meta->value_schema();

    if (!elem_schema || !elem_schema->ops || !set_schema || !set_schema->ops || !set_schema->ops->contains) {
        return false;
    }

    // Create temp element storage and convert from Python
    std::vector<char> temp_storage(elem_schema->size);
    void* temp = temp_storage.data();

    if (elem_schema->ops->construct) {
        elem_schema->ops->construct(temp, elem_schema);
    }
    if (elem_schema->ops->from_python) {
        try {
            elem_schema->ops->from_python(temp, element, elem_schema);
        } catch (...) {
            if (elem_schema->ops->destruct) {
                elem_schema->ops->destruct(temp, elem_schema);
            }
            return false;  // Conversion failed - element can't be in set
        }
    }

    // Use O(1) contains lookup
    bool result = set_schema->ops->contains(_view.data(), temp, set_schema);

    if (elem_schema->ops->destruct) {
        elem_schema->ops->destruct(temp, elem_schema);
    }

    return result;
}

bool TSSView::was_added(const value::ConstValueView& element, engine_time_t time) {
    auto* overlay = set_overlay();
    if (!overlay || !valid() || !overlay->has_delta_at(time)) {
        return false;
    }

    // Use O(1) hash set lookup
    return overlay->was_added_element(element);
}

bool TSSView::was_added_python(const nb::object& element, engine_time_t time) {
    return was_added(make_value(set_meta()->element_type(), element).const_view(), time);
}

bool TSSView::was_removed(const value::ConstValueView& element, engine_time_t time) {
    auto* overlay = set_overlay();
    if (!overlay || !valid() || !overlay->has_delta_at(time)) {
        return false;
    }

    // Use O(1) hash set lookup
    return overlay->was_removed_element(element);
}

bool TSSView::was_removed_python(const nb::object& element, engine_time_t time) {
    return was_removed(make_value(set_meta()->element_type(), element).const_view(), time);
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

    // Python behavior: {name: field.delta_value for modified and valid fields}
    nb::dict result;
    for (auto& key : keys()) {
        TSView field_view = field(std::string(key));
        if (field_view.modified_at(eval_time) && field_view.ts_valid()) {
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

    // Get the current evaluation time from the owning node
    Node* node = owning_node();
    engine_time_t current_time = MIN_DT;
    if (node) {
        graph_ptr graph = node->graph();
        if (graph) {
            current_time = graph->evaluation_time();
        }
    }

    // Import REMOVE sentinel from Python
    auto REMOVE_marker = nb::module_::import_("hgraph._types._tsd_type").attr("REMOVE");

    nb::dict result;
    const auto* dict_meta_ptr = static_cast<const TSDTypeMeta*>(_ts_meta);

    // Get overlay for delta info
    auto* overlay = map_overlay();
    if (!overlay) {
        // No overlay - return empty dict
        return nb::module_::import_("frozendict").attr("frozendict")(result);
    }

    // Check for any delta: structural changes (adds/removes) or value modifications
    bool has_structural_delta = overlay->has_delta_at(current_time);
    bool has_modified_values = overlay->has_modified_keys(current_time);

    if (!has_structural_delta && !has_modified_values) {
        // No delta this tick - return empty dict
        return nb::module_::import_("frozendict").attr("frozendict")(result);
    }

    value::ConstMapView map_view = _view.as_map();
    const TSMeta* value_ts_type = dict_meta_ptr->value_ts_type();

    // Helper to create child TSView at a given index
    auto make_child_view = [&](size_t idx) -> TSView {
        value::ConstValueView val_view = map_view.value_at(idx);
        if (auto* map_ov = map_overlay()) {
            TSOverlayStorage* value_ov = map_ov->value_overlay(idx);
            return TSView(val_view.data(), value_ts_type, value_ov);
        }
        return TSView(val_view.data(), value_ts_type);
    };

    // Include added keys with their delta values
    for (size_t idx : overlay->added_key_indices()) {
        nb::object py_key = map_view.key_at(idx).to_python();
        // Get the child time-series view and its delta_value
        TSView child_view = make_child_view(idx);
        nb::object delta_val = child_view.to_python_delta();
        if (!delta_val.is_none()) {
            result[py_key] = delta_val;
        }
    }

    // Include modified keys (not in added) with their delta values
    for (size_t idx : overlay->modified_key_indices(current_time)) {
        // Skip if already in added (added keys are also "modified")
        bool is_added = false;
        for (size_t added_idx : overlay->added_key_indices()) {
            if (added_idx == idx) {
                is_added = true;
                break;
            }
        }
        if (is_added) continue;

        nb::object py_key = map_view.key_at(idx).to_python();
        TSView child_view = make_child_view(idx);
        nb::object delta_val = child_view.to_python_delta();
        if (!delta_val.is_none()) {
            result[py_key] = delta_val;
        }
    }

    // Include removed keys with REMOVE marker
    for (const auto& removed_key : overlay->removed_key_values()) {
        nb::object py_key = removed_key.const_view().to_python();
        result[py_key] = REMOVE_marker;
    }

    return nb::module_::import_("frozendict").attr("frozendict")(result);
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

    // Get the current evaluation time from the owning node
    Node* node = owning_node();
    engine_time_t current_time = MIN_DT;
    if (node) {
        graph_ptr graph = node->graph();
        if (graph) {
            current_time = graph->evaluation_time();
        }
    }

    // Import Python SetDelta class
    auto PythonSetDelta = nb::module_::import_("hgraph._impl._types._tss").attr("PythonSetDelta");

    // Get the set overlay for delta tracking
    TSSView* self_mut = const_cast<TSSView*>(this);  // delta_view is non-const due to lazy cleanup
    SetDeltaView delta = self_mut->delta_view(current_time);

    // Build frozensets for added and removed
    nb::set added_set;
    nb::set removed_set;

    if (delta.valid()) {
        // Get added values from the current set
        for (const auto& val : delta.added_values()) {
            added_set.add(val.to_python());
        }
        // Get removed values from the overlay buffer
        for (const auto& val : delta.removed_values()) {
            removed_set.add(val.to_python());
        }
    }

    return PythonSetDelta(nb::frozenset(added_set), nb::frozenset(removed_set));
}

}  // namespace hgraph
