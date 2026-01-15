//
// ts_view.cpp - TSView, TSMutableView, TSBView implementation
//

#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_ref_target_link.h>
#include <hgraph/types/time_series/ts_link.h>
#include <hgraph/types/value/window_storage_ops.h>
#include <hgraph/types/value/composite_ops.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/constants.h>
#include <fmt/format.h>
#include <stdexcept>
#include <unordered_map>
#include <iostream>

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
    const TSValue* result = std::visit([](const auto& link) -> const TSValue* {
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

    // If no direct link found, check for "virtualized combine" case:
    // For REF[TSB[...]] inputs where individual inner fields are bound directly,
    // the links are created on child_value(final_idx) instead of at final_idx.
    // In this case, we consider the REF "bound" if its child_value exists and has links.
    if (!result) {
        const TSValue* child = current->child_value(final_idx);
        if (child && child->has_link_support()) {
            // Check if any links exist on the child
            for (size_t i = 0; i < child->child_count(); ++i) {
                if (child->is_linked(i)) {
                    // Return the child_value as the "bound output" for validity checking
                    // This allows the REF to be considered valid when inner fields are linked
                    return child;
                }
            }
        }
    }

    return result;
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
{
    // Handle key_set view case: if this is a TSS backed by a TSD (via _cast_source),
    // we need to sync the keys from the source TSD before creating the view.
    // This handles the case where the TSD has been modified since the key_set was created.
    if (_ts_meta && _ts_meta->kind() == TSTypeKind::TSS && ts_value.cast_source() != nullptr) {
        const TSValue* source = ts_value.cast_source();
        if (source->ts_meta() && source->ts_meta()->kind() == TSTypeKind::TSD &&
            source->value().valid()) {
            // Sync keys from TSD to key_set
            auto tsd_view = source->value().view().as_map();
            auto& key_set_value = const_cast<TSValue&>(ts_value).value();
            auto key_set_view = key_set_value.view().as_set();
            key_set_view.clear();
            for (size_t i = 0; i < tsd_view.size(); ++i) {
                auto key = tsd_view.key_at(i);
                key_set_view.insert(key);
            }
            // Also sync the overlay modification time from the source TSD
            if (_overlay && source->overlay()) {
                _overlay->mark_modified(source->overlay()->last_modified_time());
            }
        }
    }
}

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
    // Propagate container for cast TSValue handling
    result._container = _container;
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
    // Propagate expected element REF meta for TS→REF conversion
    result._expected_element_ref_meta = _expected_element_ref_meta;
    // Propagate bound output for TS→REF TimeSeriesReference creation
    result._bound_output = _bound_output;
    // Propagate container for cast TSValue handling
    result._container = _container;
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
    // Propagate expected element REF meta for TS→REF conversion
    result._expected_element_ref_meta = _expected_element_ref_meta;
    // Propagate bound output for TS→REF TimeSeriesReference creation
    result._bound_output = _bound_output;
    // Propagate container for cast TSValue handling
    result._container = _container;
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
    // Propagate container for cast TSValue handling (key_set sync)
    result._container = _container;
    return result;
}

nb::object TSView::to_python() const {
    if (!valid()) {
        return nb::none();
    }

    // ===== TS→REF Conversion: Create TimeSeriesReference when wrapping as REF =====
    // This happens when input expects TSL[REF[TS[T]]] but is bound to TSL[TS[T]] output.
    // The _bound_output points to the output element's TSValue.
    if (should_wrap_elements_as_ref() && _bound_output) {
        // Create a VIEW_BOUND TimeSeriesReference pointing to the output TSValue
        TimeSeriesReference ref = TimeSeriesReference::make_view_bound(_bound_output);
        return nb::cast(std::move(ref));
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
            // Cast TSValue REF: When this is a cast REF element (from TSL[TS] → TSL[REF[TS]]),
            // create a TimeSeriesReference pointing to the source element
            if (_container && _container->is_cast()) {
                // Check for source element (TS→REF conversion from list/bundle)
                if (_container->source_element()) {
                    return nb::cast(TimeSeriesReference::make_view_bound(_container->source_element()));
                }
                // Fallback: direct TS→REF cast (source_element is cast_source)
                if (_container->cast_source()) {
                    return nb::cast(TimeSeriesReference::make_view_bound(_container->cast_source()));
                }
            }

            // Direct bound output: When TSLView::element() extracted the bound output element
            // and set it via set_bound_output(), use it directly to create a TimeSeriesReference.
            if (_bound_output) {
                auto bound_kind = _bound_output->ts_meta() ? _bound_output->ts_meta()->kind() : TSTypeKind::TS;

                // Check if bound_output is a container and we have an element index
                if (_bound_output_elem_index >= 0 && bound_kind == TSTypeKind::TSL) {
                    // Check what the ELEMENT type is - if the element is also a TSL, we should NOT use elem_index
                    // because the element is a whole container, not a scalar
                    const TSLTypeMeta* list_meta = static_cast<const TSLTypeMeta*>(_bound_output->ts_meta());
                    const TSMeta* elem_meta = list_meta->element_type();

                    // Only use elem_index for scalar elements (like TS), not for container elements (like TSL)
                    // For container elements, the element itself should be returned whole
                    if (elem_meta->kind() == TSTypeKind::TSL || elem_meta->kind() == TSTypeKind::TSB ||
                        elem_meta->kind() == TSTypeKind::TSD || elem_meta->kind() == TSTypeKind::TSS) {
                        // Container element - don't use elem_index, navigate to get the container
                        // and return a direct reference to it
                        // TODO: Navigate to the element and return TimeSeriesReference to it directly
                        // For now, fallthrough to use elem_index (which may be wrong)
                    }

                    // Create a view-bound reference with the element path
                    return nb::cast(TimeSeriesReference::make_view_bound(
                        _bound_output, static_cast<size_t>(_bound_output_elem_index)));
                }
                return nb::cast(TimeSeriesReference::make_view_bound(_bound_output));
            }

            // Auto-dereference: When a non-REF input (e.g., TS[int]) binds to a REF output,
            // Python auto-dereferences. We must return the target's value, not TimeSeriesReference.
            if (_auto_deref_ref) {
                // Navigate to find the target output and return its value
                if (_link_source && _link_source->has_link_support() && !_path.is_root()) {
                    const TSValue* current = _link_source;
                    for (size_t i = 0; i + 1 < _path.elements.size(); ++i) {
                        current = current->child_value(_path.elements[i]);
                        if (!current) {
                            return nb::none();  // No valid target
                        }
                    }

                    size_t final_idx = _path.elements.back();
                    const LinkStorage* storage = current->link_storage_at(final_idx);
                    if (storage) {
                        // For auto-deref, get the actual TARGET, not the REF output
                        const TSValue* target_output = std::visit([](const auto& link) -> const TSValue* {
                            using T = std::decay_t<decltype(link)>;
                            if constexpr (std::is_same_v<T, std::monostate>) {
                                return nullptr;
                            } else if constexpr (std::is_same_v<T, std::unique_ptr<TSLink>>) {
                                return link ? link->output() : nullptr;
                            } else if constexpr (std::is_same_v<T, std::unique_ptr<TSRefTargetLink>>) {
                                // For REF bindings, get target_output (the actual data), not ref_output
                                return link ? link->target_output() : nullptr;
                            } else {
                                return nullptr;
                            }
                        }, *storage);

                        if (target_output) {
                            // DEREFERENCE: Return the target's value, not the TimeSeriesReference
                            TSView target_view = target_output->view();
                            return target_view.to_python();
                        }
                    }
                }
                // Fallback for auto-deref: get target from TSValue's ref_cache
                // The _root is the REF output's TSValue. Look up its TimeSeriesReference
                // in the cache and get the target it points to.
                if (_root && _root->has_ref_cache()) {
                    try {
                        nb::object cached = std::any_cast<nb::object>(_root->ref_cache());
                        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(cached);
                        if (ref.is_view_bound()) {
                            const TSValue* target = ref.view_output();
                            if (target) {
                                // DEREFERENCE: Return the target's value
                                TSView target_view = target->view();
                                nb::object result = target_view.to_python();
                                return result;
                            }
                        }
                    } catch (const std::bad_any_cast&) {
                    } catch (const std::exception& e) {
                    } catch (...) {
                    }
                }
                return nb::none();
            }

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
                        // Check TSValue's ref_cache for this bound output - it contains a TimeSeriesReference
                        if (bound_output->has_ref_cache()) {
                            try {
                                // Return the cached TimeSeriesReference directly (NO automatic dereferencing)
                                // Python's REF.value returns the TimeSeriesReference, not the dereferenced value.
                                // Users who want the underlying data should use .output.value
                                return std::any_cast<nb::object>(bound_output->ref_cache());
                            } catch (const std::bad_any_cast&) {
                                // Fall through to create reference
                            }
                        }
                        // Create view-bound reference to the bound output
                        return nb::cast(TimeSeriesReference::make_view_bound(bound_output));
                    }

                    // "Virtualized combine" case: No direct link, but child_value has nested links.
                    // This happens for REF[TSL[...]] where inner elements are bound to separate outputs.
                    const TSValue* child = current->child_value(final_idx);
                    if (child && child->has_link_support()) {
                        bool has_nested_links = false;
                        for (size_t i = 0; i < child->child_count(); ++i) {
                            if (child->is_linked(i)) {
                                has_nested_links = true;
                                break;
                            }
                        }
                        if (has_nested_links) {
                            // Create view-bound reference to the child_value (the inner TSL with links)
                            return nb::cast(TimeSeriesReference::make_view_bound(child));
                        }
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

                if (cache_key && cache_key->has_ref_cache()) {
                    try {
                        // Return the cached value directly (NO automatic dereferencing)
                        // Python's REF.value returns the TimeSeriesReference, not the dereferenced value.
                        return std::any_cast<nb::object>(cache_key->ref_cache());
                    } catch (const std::bad_any_cast&) {
                        // Fall through to create reference
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
    // NOTE: We always use the view's data pointer, NOT _container->to_python(),
    // because for element views (cast TSValues), the _view.data() points to the
    // actual element data, while _container->to_python() would read from _value
    // which doesn't hold the element data for cast/source_element TSValues.
    const value::TypeMeta* schema = _ts_meta->value_schema();
    return schema->ops->to_python(_view.data(), schema);
}

nb::object TSView::to_python_delta() const {
    // ===== KEY_SET support: Handle TSD source views as TSS =====
    // When _tsd_source is set, this is a key_set view of a TSD.
    // Build delta directly from TSD's MapTSOverlay.
    if (_tsd_source) {
        const TSMeta* source_meta = _tsd_source->ts_meta();
        if (source_meta && source_meta->kind() == TSTypeKind::TSD) {
            // Get delta directly from TSD's MapTSOverlay
            auto* tsd_overlay = dynamic_cast<const MapTSOverlay*>(_tsd_source->overlay());

            if (tsd_overlay) {
                nb::set added_set;
                nb::set removed_set;

                // Get added keys from TSD
                const auto& added_key_indices = tsd_overlay->added_key_indices();
                if (!added_key_indices.empty()) {
                    // Access the TSD's data to get the actual key values
                    value::ConstMapView tsd_map = _tsd_source->value().view().as_map();
                    for (size_t idx : added_key_indices) {
                        if (idx < tsd_map.size()) {
                            auto added_key = tsd_map.key_at(idx);
                            added_set.add(added_key.to_python());
                        }
                    }
                }

                // Get removed keys from TSD (stored in overlay)
                const auto& removed_keys = tsd_overlay->removed_key_values();
                for (const auto& removed_key : removed_keys) {
                    removed_set.add(removed_key.view().to_python());
                }

                // Import Python SetDelta class and return
                auto PythonSetDelta = nb::module_::import_("hgraph._impl._types._tss").attr("PythonSetDelta");
                return PythonSetDelta(nb::frozenset(added_set), nb::frozenset(removed_set));
            }
        }
        // If we get here, something is wrong with the key_set setup
        return nb::none();
    }

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
        case TSTypeKind::REF: {
            // Auto-dereference: Return the target's delta_value, not value
            if (_auto_deref_ref) {
                // Navigate to find the target output
                if (_link_source && _link_source->has_link_support() && !_path.is_root()) {
                    const TSValue* current = _link_source;
                    for (size_t i = 0; i + 1 < _path.elements.size(); ++i) {
                        current = current->child_value(_path.elements[i]);
                        if (!current) {
                            return nb::none();
                        }
                    }

                    size_t final_idx = _path.elements.back();
                    const LinkStorage* storage = current->link_storage_at(final_idx);
                    if (storage) {
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
                            // DEREFERENCE: Return the target's delta_value
                            TSView target_view = bound_output->view();
                            return target_view.to_python_delta();
                        }
                    }
                }
                // Fallback for auto-deref: get target from TSValue's ref_cache
                // Note: Proper delta computation for REF target switches is handled by
                // TSRefTargetLink in PyTimeSeriesInput::delta_value(). This fallback
                // just delegates to the target's delta_value.
                if (_root && _root->has_ref_cache()) {
                    try {
                        nb::object cached = std::any_cast<nb::object>(_root->ref_cache());
                        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(cached);
                        if (ref.is_view_bound()) {
                            const TSValue* target = ref.view_output();
                            if (target) {
                                TSView target_view = target->view();
                                return target_view.to_python_delta();
                            }
                        }
                    } catch (const std::bad_any_cast&) {
                    } catch (...) {}
                }
                return nb::none();
            }

            // For REF inputs: Python rebinds the input to the underlying target (TSS, TSL, etc).
            // In C++, we emulate this by returning the target's delta_value instead of TimeSeriesReference.
            // Two cases to handle:
            // 1. TS→REF binding (non-REF output → REF input): return bound output's delta directly
            // 2. REF→REF binding (REF output → REF input): get TimeSeriesReference, then target's delta
            //
            // Helper lambda to get target's delta from a bound output
            auto get_target_delta = [](const TSValue* bound_output) -> std::optional<nb::object> {
                if (!bound_output) {
                    return std::nullopt;
                }

                const TSMeta* bound_meta = bound_output->ts_meta();
                if (!bound_meta) {
                    return std::nullopt;
                }

                // Case 1: TS→REF binding - bound output is non-REF (TSS, TSL, TS, etc.)
                // Return the non-REF output's delta_value directly
                if (bound_meta->kind() != TSTypeKind::REF) {
                    TSView target_view = bound_output->view();
                    return target_view.to_python_delta();
                }

                // Case 2: REF→REF binding - bound output is REF
                // Get the TimeSeriesReference from ref_cache and return target's delta
                if (bound_output->has_ref_cache()) {
                    try {
                        nb::object cached = std::any_cast<nb::object>(bound_output->ref_cache());
                        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(cached);
                        if (ref.is_view_bound()) {
                            const TSValue* target = ref.view_output();
                            if (target) {
                                const TSMeta* target_meta = target->ts_meta();
                                if (target_meta && target_meta->kind() != TSTypeKind::REF) {
                                    TSView target_view = target->view();
                                    return target_view.to_python_delta();
                                }
                            }
                        }
                    } catch (const std::bad_any_cast&) {
                    } catch (const std::exception&) {
                    }
                }
                return std::nullopt;
            };

            // Check for nested REF input (bundle field, etc.)
            if (_link_source && _link_source->has_link_support() && !_path.is_root()) {
                const TSValue* current = _link_source;
                for (size_t i = 0; i + 1 < _path.elements.size(); ++i) {
                    current = current->child_value(_path.elements[i]);
                    if (!current) {
                        break;
                    }
                }

                if (current) {
                    size_t final_idx = _path.elements.back();
                    const LinkStorage* storage = current->link_storage_at(final_idx);
                    if (storage) {
                        // For TSRefTargetLink, use view() which returns the dereferenced target view
                        // For TSLink, get the output directly
                        auto [bound_output, is_ref_target_link] = std::visit([](const auto& link) -> std::pair<const TSValue*, bool> {
                            using T = std::decay_t<decltype(link)>;
                            if constexpr (std::is_same_v<T, std::monostate>) {
                                return {nullptr, false};
                            } else if constexpr (std::is_same_v<T, std::unique_ptr<TSLink>>) {
                                return {link ? link->output() : nullptr, false};
                            } else if constexpr (std::is_same_v<T, std::unique_ptr<TSRefTargetLink>>) {
                                // Return target_output for direct delta access
                                return {link ? link->target_output() : nullptr, true};
                            } else {
                                return {nullptr, false};
                            }
                        }, *storage);

                        // For TSRefTargetLink, the bound_output is already the target - get its delta directly
                        if (is_ref_target_link && bound_output) {
                            const TSMeta* target_meta = bound_output->ts_meta();
                            if (target_meta && target_meta->kind() != TSTypeKind::REF) {
                                TSView target_view = bound_output->view();
                                return target_view.to_python_delta();
                            }
                        }

                        auto result = get_target_delta(bound_output);
                        if (result) return *result;
                    }
                }
            }

            // Check for root-level REF input
            if (_root && _root->has_link_support() && _path.is_root()) {
                // Root-level input - check for link at index 0 (the whole input is linked)
                const LinkStorage* storage = _root->link_storage_at(0);
                if (storage) {
                    // For TSRefTargetLink, use target_output which returns the dereferenced target
                    // For TSLink, get the output directly
                    auto [bound_output, is_ref_target_link] = std::visit([](const auto& link) -> std::pair<const TSValue*, bool> {
                        using T = std::decay_t<decltype(link)>;
                        if constexpr (std::is_same_v<T, std::monostate>) {
                            return {nullptr, false};
                        } else if constexpr (std::is_same_v<T, std::unique_ptr<TSLink>>) {
                            return {link ? link->output() : nullptr, false};
                        } else if constexpr (std::is_same_v<T, std::unique_ptr<TSRefTargetLink>>) {
                            // Return target_output for direct delta access
                            return {link ? link->target_output() : nullptr, true};
                        } else {
                            return {nullptr, false};
                        }
                    }, *storage);

                    // For TSRefTargetLink, the bound_output is already the target - get its delta directly
                    if (is_ref_target_link && bound_output) {
                        const TSMeta* target_meta = bound_output->ts_meta();
                        if (target_meta && target_meta->kind() != TSTypeKind::REF) {
                            TSView target_view = bound_output->view();
                            return target_view.to_python_delta();
                        }
                    }

                    auto result = get_target_delta(bound_output);
                    if (result) return *result;
                }
            }

            // Note: REF target switch delta computation is handled by
            // TSRefTargetLink in PyTimeSeriesInput::delta_value()
            // For TSS, delta_value is the same as value when accessed directly
            return to_python();
        }
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
            // For "virtualized combine" case: bound_output is a synthetic child_value with links
            // Check if it has link support with active links - if so, check ALL linked outputs
            if (bound_output->has_link_support()) {
                bool has_any_link = false;
                bool all_valid = true;
                for (size_t i = 0; i < bound_output->child_count(); ++i) {
                    if (bound_output->is_linked(i)) {
                        has_any_link = true;
                        // Get the linked output and check its validity
                        LinkStorage* storage = const_cast<TSValue*>(bound_output)->link_storage_at(i);
                        if (storage) {
                            const TSValue* linked_output = std::visit([](const auto& link) -> const TSValue* {
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
                            if (!linked_output || !linked_output->ts_valid()) {
                                all_valid = false;
                                break;
                            }
                        }
                    }
                }
                if (has_any_link) {
                    return all_valid;
                }
            }
            // Fall back to regular ts_valid check on bound_output
            return bound_output->ts_valid();
        }

        // For outputs (no link), check if the cached TimeSeriesReference is valid
        // Use the same cache key logic as to_python
        if (_root) {
            const TSValue* cache_key = _root;
            if (!_path.is_root() && _root->has_link_support() && !_path.elements.empty()) {
                cache_key = _root->child_value(_path.elements.back());
            }
            if (cache_key && cache_key->has_ref_cache()) {
                try {
                    // Check if the cached value is a valid (non-empty) TimeSeriesReference
                    nb::object cached = std::any_cast<nb::object>(cache_key->ref_cache());
                    if (nb::isinstance<TimeSeriesReference>(cached)) {
                        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(cached);
                        // REF is valid if it has output (not empty)
                        return ref.has_output();
                    }
                    // If it's not a TimeSeriesReference, consider it valid
                    return true;
                } catch (const std::bad_any_cast&) {
                    // Fall through to return false
                }
            }
        }
        // No cache entry - not valid
        return false;
    }

    // Note: For TSSView key_set views, the constructor sets _overlay to the TSD's overlay,
    // so the regular overlay check below works correctly.
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
    // For REF types with link support, check if the REF binding was "sampled" at this time
    // REF inputs should only report modified when the binding changes (sample_time),
    // not when underlying values change. This matches Python's _sampled semantics.
    if (_ts_meta && _ts_meta->kind() == TSTypeKind::REF) {
        const TSValue* bound_output = navigate_ref_link(*this);
        if (bound_output) {
            // For "virtualized combine" case: bound_output is a synthetic child_value with links
            // Check if any link was sampled at this time (binding took effect)
            if (bound_output->has_link_support()) {
                for (size_t i = 0; i < bound_output->child_count(); ++i) {
                    if (bound_output->is_linked(i)) {
                        const LinkStorage* storage = bound_output->link_storage_at(i);
                        if (storage) {
                            bool sampled = std::visit([time](const auto& link) -> bool {
                                using T = std::decay_t<decltype(link)>;
                                if constexpr (std::is_same_v<T, std::monostate>) {
                                    return false;
                                } else if constexpr (std::is_same_v<T, std::unique_ptr<TSLink>>) {
                                    return link && link->sampled_at(time);
                                } else if constexpr (std::is_same_v<T, std::unique_ptr<TSRefTargetLink>>) {
                                    // TSRefTargetLink might have different semantics
                                    return false;
                                } else {
                                    return false;
                                }
                            }, *storage);
                            if (sampled) {
                                return true;  // At least one link was sampled at this time
                            }
                        }
                    }
                }
                // No links sampled at this time - REF not modified
                return false;
            }
            // Fall back to checking output modification for direct bindings
            return bound_output->modified_at(time);
        }
    }

    // Note: For TSSView key_set views, the constructor sets _overlay to the TSD's overlay,
    // so the regular overlay check below works correctly.
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

    // Note: For TSSView key_set views, the constructor sets _overlay to the TSD's overlay,
    // so the regular overlay check below works correctly.
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

bool TSMutableView::from_python(const nb::object& src) {
    if (!valid()) {
        throw std::runtime_error("TSMutableView::from_python() called on invalid view");
    }

    // Special handling for REF types: store TimeSeriesReference in cache
    if (_ts_meta->kind() == TSTypeKind::REF) {
        // REF outputs store TimeSeriesReference objects, not raw values
        // We can't store Python objects in the C++ value storage (wrong schema),
        // so we use the TSValue's ref_cache member

        if (src.is_none()) {
            // Clear/invalidate - remove from cache
            if (_root) {
                _root->clear_ref_cache();
            }
            return true;  // Invalidating is a change
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

        // Determine cache key
        const TSValue* cache_key = _root ? _root : _mutable_container;

        // Note: Delta computation for REF target switches is now handled by
        // TSRefTargetLink's prev_target_output() in PyTimeSeriesInput::delta_value()

        // Store in TSValue's ref_cache and mark overlay as modified
        if (cache_key) {
            cache_key->set_ref_cache(src);

            // Get current engine time and mark overlay as modified
            // This triggers bottom-up notification to subscribers (TSRefTargetLink, TSLink)
            engine_time_t current_time = MIN_DT;
            Node* node = owning_node();
            if (node) {
                graph_ptr g = node->graph();
                if (g) {
                    current_time = g->evaluation_time();
                }
            }
            if (_overlay) {
                _overlay->mark_modified(current_time);
            }
        }

        return true;
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
            return true;  // Push is always a change
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
                    // REF field - store in TSValue's ref_cache using the field's child TSValue
                    if (_mutable_container) {
                        TSValue* field_ts_value = _mutable_container->get_or_create_child_value(field_info->index);
                        if (field_ts_value && !field_value.is_none()) {
                            field_ts_value->set_ref_cache(field_value);
                            // Note: REF observer notification is handled bottom-up via overlay subscription.
                            // TSRefTargetLink subscribes to the REF field's overlay and resolves the
                            // TimeSeriesReference in its notify() when the overlay is modified.
                        } else if (field_ts_value && field_value.is_none()) {
                            field_ts_value->clear_ref_cache();
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
        return true;
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
        return true;
    }

    // Special handling for TSL with tuple/list input: need to mark child overlays
    // This happens when values are written as (val0, val1, ...) instead of {0: val0, 1: val1, ...}
    if (_ts_meta->kind() == TSTypeKind::TSL && (nb::isinstance<nb::tuple>(src) || nb::isinstance<nb::list>(src))) {
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

        // Now mark the child overlays for non-None elements
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

            // Mark each child overlay for elements that are not None
            nb::sequence seq = nb::cast<nb::sequence>(src);
            size_t count = nb::len(seq);
            for (size_t idx = 0; idx < count; ++idx) {
                nb::object elem = nb::borrow<nb::object>(seq[idx]);
                if (!elem.is_none()) {
                    TSOverlayStorage* child_ov = list_overlay->child(idx);
                    if (child_ov) {
                        child_ov->mark_modified(current_time);
                    }
                }
            }
        }
        return true;
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

        return true;
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

        // Track whether any actual changes were made
        bool tss_changed = false;

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
                            tss_changed = true;  // Removal happened
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
                            tss_changed = true;  // Addition happened
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
            // Handle set/frozenset
            // Python TSS semantics:
            // - frozenset: replacement semantics (replaces entire set, computing added and removed)
            // - set/list/tuple: incremental semantics (add items not in set, no automatic removal)
            // - Removed() wrappers: explicit removal markers
            bool is_frozenset = nb::isinstance<nb::frozenset>(src);
            std::vector<nb::object> new_elements;
            std::vector<nb::object> explicit_removals;  // From Removed() wrappers

            // Get the Removed class for isinstance checks
            nb::object removed_class = get_removed();

            if (nb::isinstance<nb::set>(src) || is_frozenset) {
                for (auto item : src) {
                    nb::object elem = nb::borrow<nb::object>(item);
                    // Check for Removed() wrapper
                    if (nb::isinstance(elem, removed_class)) {
                        // Extract the inner item and mark for removal
                        nb::object inner_item = elem.attr("item");
                        explicit_removals.push_back(inner_item);
                    } else {
                        new_elements.push_back(elem);
                    }
                }
            } else if (nb::isinstance<nb::list>(src) || nb::isinstance<nb::tuple>(src)) {
                nb::sequence seq = nb::cast<nb::sequence>(src);
                for (size_t i = 0; i < nb::len(seq); ++i) {
                    nb::object elem = nb::borrow<nb::object>(seq[i]);
                    // Check for Removed() wrapper
                    if (nb::isinstance(elem, removed_class)) {
                        // Extract the inner item and mark for removal
                        nb::object inner_item = elem.attr("item");
                        explicit_removals.push_back(inner_item);
                    } else {
                        new_elements.push_back(elem);
                    }
                }
            } else {
                throw std::runtime_error("TSMutableView::from_python: TSS expects set, frozenset, or SetDelta");
            }
            std::vector<nb::object> removed_items;
            std::vector<nb::object> added_items;

            // Process explicit removals (from Removed() wrappers)
            // Only remove if the item is actually in the current set
            for (const auto& removal : explicit_removals) {
                for (const auto& old_elem : old_elements) {
                    if (removal.equal(old_elem)) {
                        removed_items.push_back(removal);
                        break;
                    }
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

            // Find removed items (only for frozenset - replacement semantics)
            // Combine with explicit removals
            if (is_frozenset) {
                for (const auto& old_elem : old_elements) {
                    // Skip if already marked for explicit removal
                    bool already_removed = false;
                    for (const auto& removal : explicit_removals) {
                        if (old_elem.equal(removal)) {
                            already_removed = true;
                            break;
                        }
                    }
                    if (already_removed) continue;

                    // Check if in new elements
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
            }

            // Track if we have actual changes
            tss_changed = !added_items.empty() || !removed_items.empty();

            // Record removals in overlay BEFORE modifying backing store (need old indices)
            if (set_overlay && !removed_items.empty()) {
                value::ConstSetView old_set = _view.as_set();
                const auto* set_meta = static_cast<const TSSTypeMeta*>(_ts_meta);
                const value::TypeMeta* elem_type = set_meta->element_type();
                for (const auto& py_item : removed_items) {
                    // Find index of this item in old set
                    for (size_t i = 0; i < old_set.size(); ++i) {
                        if (old_set.at(i).to_python().equal(py_item)) {
                            // Create PlainValue for tracking
                            value::PlainValue removed_value(elem_type);
                            if (elem_type && elem_type->ops && elem_type->ops->from_python) {
                                elem_type->ops->from_python(removed_value.data(), py_item, elem_type);
                            }
                            set_overlay->record_removed(i, current_time, std::move(removed_value));
                            break;
                        }
                    }
                }
            }

            // Build the new set value
            nb::set new_set_py;
            if (is_frozenset) {
                // Replacement semantics: only new elements
                for (const auto& elem : new_elements) {
                    new_set_py.add(elem);
                }
            } else {
                // Incremental semantics: merge old elements with added items, excluding explicit removals
                for (const auto& elem : old_elements) {
                    // Skip if this element was explicitly removed
                    bool was_removed = false;
                    for (const auto& removal : removed_items) {
                        if (elem.equal(removal)) {
                            was_removed = true;
                            break;
                        }
                    }
                    if (!was_removed) {
                        new_set_py.add(elem);
                    }
                }
                for (const auto& elem : added_items) {
                    new_set_py.add(elem);
                }
            }

            // Update the backing store with new value
            const value::TypeMeta* schema = _ts_meta->value_schema();
            if (schema && schema->ops && schema->ops->from_python) {
                schema->ops->from_python(_mutable_view.data(), new_set_py, schema);
            }

            // Record additions in overlay (after modifying backing store to get correct indices)
            if (set_overlay && !added_items.empty()) {
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
        return tss_changed;
    }

    // If we have mutable container access, use its policy-aware from_python
    // This properly invalidates Python cache and handles all policies
    if (_mutable_container) {
        _mutable_container->from_python(src);
        return true;
    }

    // No container - use direct conversion (no cache invalidation)
    const value::TypeMeta* schema = _ts_meta->value_schema();
    if (schema && schema->ops) {
        schema->ops->from_python(_mutable_view.data(), src, schema);
    }
    return true;
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
            TSView linked_view = link_storage_view(*storage);

            // Check for KEY_SET binding (TSD viewed as TSS)
            // For KEY_SET, linked_view is a TSSView from TSLink::view().
            // We need to preserve link_source and path so PyTimeSeriesInput::delta_value()
            // can find the TSLink and call its view() method dynamically.
            bool is_key_set = std::visit([](const auto& link) -> bool {
                using T = std::decay_t<decltype(link)>;
                if constexpr (std::is_same_v<T, std::unique_ptr<TSLink>>) {
                    std::cerr << "[DEBUG] TSLink element_index=" << (link ? link->element_index() : -999) << std::endl;
                    return link && link->is_key_set_binding();
                }
                return false;
            }, *storage);

            std::cerr << "[DEBUG] TSBView::field(name) index=" << index
                      << " is_key_set=" << (is_key_set ? "yes" : "no")
                      << " linked_view.valid=" << (linked_view.valid() ? "yes" : "no") << std::endl;

            if (is_key_set && linked_view.valid()) {
                // Create a new view that preserves link_source for KEY_SET resolution
                LightweightPath child_path = _path.with(index);
                std::cerr << "[DEBUG] TSBView::field creating KEY_SET view" << std::endl;
                TSView result(linked_view.value_view().data(), linked_view.ts_meta(),
                              linked_view.overlay(), _root, std::move(child_path));
                result.set_link_source(_link_source);
                return result;
            }

            // Check if the linked output is a REF but input expects non-REF
            // This happens when a non-REF input (e.g., TIME_SERIES_TYPE resolved to TS[int])
            // is wired to a REF output. We need to dereference to get the target's view.
            if (linked_view.valid() && linked_view.ts_meta() && linked_view.ts_meta()->is_reference()) {
                // The linked output is REF - need to dereference to get target's view
                // Get the REF output's TSValue to look up its value in the cache
                const TSValue* ref_output = std::visit([](const auto& link) -> const TSValue* {
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

                if (ref_output && ref_output->has_ref_cache()) {
                    // Look up the TimeSeriesReference value in the TSValue's ref_cache
                    try {
                        nb::object cached = std::any_cast<nb::object>(ref_output->ref_cache());
                        // Cast to TimeSeriesReference and check if it points to a target
                        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(cached);
                        if (ref.is_view_bound()) {
                            // Get the target TSValue and return a view into it
                            const TSValue* target = ref.view_output();
                            if (target) {
                                return target->view();
                            }
                        }
                    } catch (const std::bad_any_cast&) {
                        // Fall through
                    }
                }

                // For REF outputs where auto-deref is needed but cache not populated yet,
                // preserve link_source so delta_value() can dynamically resolve via TSRefTargetLink
                // Also set auto_deref so to_python() knows to dereference the REF
                LightweightPath child_path = _path.with(index);
                TSView result(linked_view.value_view().data(), linked_view.ts_meta(),
                              linked_view.overlay(), _root, std::move(child_path));
                result.set_link_source(_link_source);
                result.set_auto_deref(true);  // Enable auto-dereference for REF->non-REF binding
                return result;
            }

            return linked_view;
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
            TSView linked_view = link_storage_view(*storage);

            // Check for KEY_SET binding (TSD viewed as TSS)
            // For KEY_SET, linked_view is a TSSView from TSLink::view().
            // We need to preserve link_source and path so PyTimeSeriesInput::delta_value()
            // can find the TSLink and call its view() method dynamically.
            bool is_key_set = std::visit([](const auto& link) -> bool {
                using T = std::decay_t<decltype(link)>;
                if constexpr (std::is_same_v<T, std::unique_ptr<TSLink>>) {
                    return link && link->is_key_set_binding();
                }
                return false;
            }, *storage);

            if (is_key_set) {
                // For KEY_SET bindings, create a view that preserves link_source for KEY_SET resolution.
                // We don't require linked_view.valid() because the TSSView reads directly from
                // the TSD source, which may not have been modified yet. The view is still usable
                // for accessing the TSD's keys through the _tsd_source pointer.
                LightweightPath child_path = _path.with(index);
                // Use the linked_view's metadata (TSSTypeMeta) and overlay
                // The data pointer is null for KEY_SET views since they read from _tsd_source
                TSView result(linked_view.value_view().data(), linked_view.ts_meta(),
                              linked_view.overlay(), _root, std::move(child_path));
                result.set_link_source(_link_source);
                return result;
            }

            // Check if the linked output is a REF but input expects non-REF
            // This happens when a non-REF input (e.g., TIME_SERIES_TYPE resolved to TS[int])
            // is wired to a REF output. We need to dereference to get the target's view.
            if (linked_view.valid() && linked_view.ts_meta() && linked_view.ts_meta()->is_reference()) {
                // The linked output is REF - need to dereference to get target's view
                // First, check if we have a TSRefTargetLink with a bound target - this is the most reliable
                const TSValue* direct_target = std::visit([](const auto& link) -> const TSValue* {
                    using T = std::decay_t<decltype(link)>;
                    if constexpr (std::is_same_v<T, std::unique_ptr<TSRefTargetLink>>) {
                        return link ? link->target_output() : nullptr;
                    }
                    return nullptr;
                }, *storage);

                if (direct_target) {
                    return direct_target->view();
                }

                // Fallback: Get the REF output's TSValue to look up its value in the cache
                const TSValue* ref_output = std::visit([](const auto& link) -> const TSValue* {
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


                if (ref_output && ref_output->has_ref_cache()) {
                    // Look up the TimeSeriesReference value in the TSValue's cache
                    try {
                        nb::object cached = std::any_cast<nb::object>(ref_output->ref_cache());
                        // Cast to TimeSeriesReference and check if it points to a target
                        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(cached);
                        if (ref.is_view_bound()) {
                            // Get the target TSValue and return a view into it
                            const TSValue* target = ref.view_output();
                            if (target) {
                                return target->view();
                            }
                        }
                    } catch (const std::bad_any_cast&) {
                        // Cache contains wrong type, ignore
                    }
                }
            }

            // For REF outputs where auto-deref is needed but cache not populated yet,
            // preserve link_source so delta_value() can dynamically resolve via TSRefTargetLink
            // Also set auto_deref so to_python() knows to dereference the REF
            if (linked_view.valid() && linked_view.ts_meta() && linked_view.ts_meta()->is_reference()) {
                // Create a new view with link_source and path so delta_value can resolve.
                LightweightPath child_path = _path.with(index);
                TSView result(linked_view.value_view().data(), linked_view.ts_meta(),
                              linked_view.overlay(), _root, std::move(child_path));
                result.set_link_source(_link_source);
                result.set_auto_deref(true);  // Enable auto-dereference for REF->non-REF binding
                return result;
            }

            // Handle TSRefTargetLink during initialization when target is not bound yet.
            // In this case linked_view.valid() is false but we still need to set up
            // link_source and path so the wrapper can dynamically resolve the TSRefTargetLink.
            bool is_ref_target_link = std::visit([](const auto& link) -> bool {
                using T = std::decay_t<decltype(link)>;
                if constexpr (std::is_same_v<T, std::monostate>) {
                    return false;
                } else if constexpr (std::is_same_v<T, std::unique_ptr<TSRefTargetLink>>) {
                    return link != nullptr;
                } else {
                    return false;
                }
            }, *storage);

            if (is_ref_target_link) {
                // Get the expected element type from TSRefTargetLink (or linked_view if available)
                const TSMeta* expected_meta = linked_view.ts_meta();
                TSRefTargetLink* ref_link = std::get<std::unique_ptr<TSRefTargetLink>>(*storage).get();
                if (!expected_meta && ref_link) {
                    expected_meta = ref_link->expected_element_meta();
                }

                if (expected_meta) {
                    // Create a view with link_source and path for dynamic resolution
                    LightweightPath child_path = _path.with(index);
                    TSView result(nullptr, expected_meta, static_cast<TSOverlayStorage*>(nullptr),
                                  _root, std::move(child_path));
                    result.set_link_source(_link_source);
                    result.set_auto_deref(true);
                    return result;
                }
            }

            // Check if input and output have different element types that require conversion.
            // E.g., input is TSL[REF[TS[int]]] but output is TSL[TS[int]].
            // In this case, we preserve link info so element() can create REF wrappers.
            // We keep OUTPUT's schema (since data layout matches) but set _link_source
            // so child accesses can detect the type conversion need.
            if (linked_view.valid() && linked_view.ts_meta() &&
                field_info.type->kind() == linked_view.ts_meta()->kind()) {
                // Same container kind - check for element type mismatch
                bool needs_element_conversion = false;

                if (field_info.type->kind() == TSTypeKind::TSL) {
                    const auto* input_tsl = static_cast<const TSLTypeMeta*>(field_info.type);
                    const auto* output_tsl = static_cast<const TSLTypeMeta*>(linked_view.ts_meta());
                    // Input expects REF elements but output has non-REF
                    needs_element_conversion = input_tsl->element_type()->is_reference() &&
                                              !output_tsl->element_type()->is_reference();
                } else if (field_info.type->kind() == TSTypeKind::TSD) {
                    const auto* input_tsd = static_cast<const TSDTypeMeta*>(field_info.type);
                    const auto* output_tsd = static_cast<const TSDTypeMeta*>(linked_view.ts_meta());
                    needs_element_conversion = input_tsd->value_ts_type()->is_reference() &&
                                              !output_tsd->value_ts_type()->is_reference();
                }

                if (needs_element_conversion) {
                    // Use the TSValue cast mechanism for TS→REF conversion.
                    // This creates a cast TSValue with the target schema (e.g., TSL[REF[TS[int]]])
                    // that has child_values for each element containing REF TSValues.
                    TSValue* output_ts_value = std::visit([](const auto& link) -> TSValue* {
                        using T = std::decay_t<decltype(link)>;
                        if constexpr (std::is_same_v<T, std::monostate>) {
                            return nullptr;
                        } else if constexpr (std::is_same_v<T, std::unique_ptr<TSLink>>) {
                            return link ? const_cast<TSValue*>(link->output()) : nullptr;
                        } else if constexpr (std::is_same_v<T, std::unique_ptr<TSRefTargetLink>>) {
                            return link ? const_cast<TSValue*>(link->ref_output()) : nullptr;
                        } else {
                            return nullptr;
                        }
                    }, *storage);

                    if (output_ts_value) {
                        // Get or create the cast TSValue with the input's expected schema
                        TSValue* cast_ts_value = output_ts_value->cast_to(field_info.type);
                        if (cast_ts_value) {
                            // Return a view of the cast TSValue
                            return cast_ts_value->view();
                        }
                    }

                    // Fallback to original behavior if cast fails
                    return linked_view;
                }
            }

            return linked_view;
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

    const TSLTypeMeta* list_meta_ptr = static_cast<const TSLTypeMeta*>(_ts_meta);
    const TSMeta* element_type = list_meta_ptr->element_type();

    // ===== Cast TSValue: For TS→REF conversions, elements are in child_values =====
    // When this view was created from a cast TSValue (via TSBView::field() cast_to()),
    // the element TSValues are stored in the cast's link_support->child_values.
    if (_container && _container->is_cast()) {
        const TSValue* elem_ts_value = _container->child_value(index);
        if (elem_ts_value) {
            // Return a view of the element TSValue
            // The element is a REF TSValue with _cast_source pointing to the source list
            return elem_ts_value->view();
        }
    }

    // ===== Link Support: Check if this element is linked =====
    if (_link_source && _link_source->is_linked(index)) {
        // Follow the link - return view into the linked output
        // Handle both TSLink (non-REF bindings) and TSRefTargetLink (REF bindings)
        TSLink* ts_link = const_cast<TSValue*>(_link_source)->link_at(index);
        TSRefTargetLink* ref_link = const_cast<TSValue*>(_link_source)->ref_link_at(index);

        const TSValue* bound_output = nullptr;
        TSView linked_view;

        if (ts_link) {
            bound_output = ts_link->output();
            if (bound_output) {
                linked_view = bound_output->view();
            } else {
                // No bound output yet - return invalid view
                LightweightPath child_path = _path.with(index);
                return TSView(nullptr, element_type, nullptr, _root, std::move(child_path));
            }
        } else if (ref_link) {
            // For TSRefTargetLink, check if we have a valid target or element-based binding
            if (ref_link->is_element_binding() || ref_link->target_output()) {
                // Use view() which properly handles element-based bindings
                bound_output = ref_link->target_output();
                linked_view = ref_link->view();  // This navigates to element if element-based binding
            } else {
                // No target yet - use ref_output (the REF TSValue itself) as before
                // This handles initialization before the REF output has been written to
                bound_output = ref_link->ref_output();
                if (bound_output) {
                    linked_view = bound_output->view();
                } else {
                    // No bound output yet - return invalid view
                    LightweightPath child_path = _path.with(index);
                    return TSView(nullptr, element_type, nullptr, _root, std::move(child_path));
                }
            }
        }

        if (!linked_view.valid()) {
            // Link exists but no output bound yet - return invalid view
            LightweightPath child_path = _path.with(index);
            return TSView(nullptr, element_type, nullptr, _root, std::move(child_path));
        }

        // Handle TSL[REF[T]] → TSL[T] binding:
        // When bound_output is a container (e.g., TSL) but input element is REF,
        // we need to navigate to the element at `index` within bound_output.
        // This happens when ref_link was set to the whole TSL output, not individual elements.
        if (element_type->kind() == TSTypeKind::REF &&
            linked_view.ts_meta() && linked_view.ts_meta()->kind() == TSTypeKind::TSL) {
            // Check if the REF's target type is itself a container (TSL, TSB, TSD, TSS)
            // If so, we should return a REF to the whole inner element, not use elem_index to navigate further
            const REFTypeMeta* ref_meta = static_cast<const REFTypeMeta*>(element_type);
            const TSMeta* target_type = ref_meta->referenced_type();
            TSTypeKind target_kind = target_type ? target_type->kind() : TSTypeKind::TS;


            // bound_output is the whole TSL - extract element view at index
            const TSLTypeMeta* list_meta = static_cast<const TSLTypeMeta*>(linked_view.ts_meta());
            TSLView as_list(linked_view.value_view().data(), list_meta,
                           static_cast<ListTSOverlay*>(linked_view.overlay()));
            TSView elem_view = as_list.element(index);

            // For container elements (TSL, TSB, TSD, TSS), return a REF to the element
            // WITHOUT using elem_index - the REF points to the whole container, not an element within it
            // The bound_output (ref_output) IS the target container itself
            if (target_kind == TSTypeKind::TSL || target_kind == TSTypeKind::TSB ||
                target_kind == TSTypeKind::TSD || target_kind == TSTypeKind::TSS) {
                // Create REF view pointing to the whole container (no element navigation)
                LightweightPath child_path;
                child_path.elements.push_back(index);

                // Use bound_output as the root for REF cache key uniqueness
                // Each TSL[REF[T]] element needs a unique cache key - bound_output is unique per element
                TSView result(elem_view.value_view().data(), element_type, elem_view.overlay(),
                             bound_output, std::move(child_path));
                result.set_link_source(_link_source);
                // bound_output IS the target container - use it directly without elem_index
                result.set_bound_output(bound_output);
                result.set_bound_output_elem_index(-1);  // -1 means no element navigation
                return result;
            }

            // Create REF view pointing to the scalar element
            // Store the path [index] so to_python can navigate to the element
            LightweightPath child_path;
            child_path.elements.push_back(index);

            TSView result(elem_view.value_view().data(), element_type, elem_view.overlay(), _root, std::move(child_path));
            result.set_link_source(_link_source);
            // Store the whole TSL as bound_output and the element index in _bound_output_elem_index
            result.set_bound_output(bound_output);
            result.set_bound_output_elem_index(static_cast<int>(index));
            return result;
        }

        // Handle TS→REF conversion: If input type is REF but output is non-REF scalar,
        // return a view that will create a TimeSeriesReference when .value is called.
        if (element_type->kind() == TSTypeKind::REF &&
            linked_view.ts_meta() && linked_view.ts_meta()->kind() != TSTypeKind::REF) {
            // Return a REF-typed view that tracks the linked output
            // The view's data points to the output, but the type is REF
            // When to_python() is called, it will create a TimeSeriesReference

            // IMPORTANT: The path should be relative to _link_source.
            // Since _link_source is the TSL (not the bundle), the path should just be [index],
            // not accumulated from parent levels (which would include the bundle field index).
            LightweightPath child_path;
            child_path.elements.push_back(index);

            TSView result(linked_view.value_view().data(), element_type, nullptr, _root, std::move(child_path));
            // Set the link source to allow proper REF value retrieval
            result.set_link_source(_link_source);
            // Store bound_output for REF to use
            result.set_bound_output(bound_output);
            return result;
        }

        return linked_view;
    }

    // ===== REF elements with nested links (no direct link) =====
    // When element is REF type but not directly linked, check for child_value with nested links.
    // This handles the "virtualized combine" case: REF[TSL[TS[T]]] where inner TS elements
    // are bound to separate outputs (from TSL.from_ts(ts1, ts2) etc).
    if (element_type->kind() == TSTypeKind::REF && _link_source) {
        const TSValue* child_value = const_cast<TSValue*>(_link_source)->child_value(index);
        if (child_value && child_value->has_link_support()) {
            // Check if any nested link exists
            bool has_nested_links = false;
            for (size_t i = 0; i < child_value->child_count(); ++i) {
                if (child_value->is_linked(i)) {
                    has_nested_links = true;
                    break;
                }
            }
            if (has_nested_links) {
                // Return a REF view with proper path and link_source setup
                // Path is just [index] relative to _link_source (the parent TSL)
                LightweightPath child_path;
                child_path.elements.push_back(index);

                // Use the child_value's data for the view, but type is REF
                TSView result(child_value->value().data(), element_type, nullptr, _root, std::move(child_path));
                result.set_link_source(_link_source);  // Parent's link_source for navigation
                return result;
            }
        }
    }

    // element_type already extracted above

    // Navigate to the element data using the list view
    value::ConstListView list_view = _view.as_list();

    if (index >= list_view.size()) {
        throw std::out_of_range("TSLView::element(): index " + std::to_string(index) +
                                " out of range (size=" + std::to_string(list_view.size()) + ")");
    }

    value::ConstValueView element_value = list_view.at(index);

    // Extend path with element index
    LightweightPath child_path = _path.with(index);

    // ===== TS→REF Conversion: Check if elements should be wrapped as REF =====
    // This happens when an input expects TSL[REF[TS[T]]] but is bound to TSL[TS[T]] output.
    // The _expected_element_ref_meta was set by TSBView::field() when detecting this case.
    // We keep the ACTUAL element type (for data layout) but set the bound_output so
    // to_python() can create a TimeSeriesReference.
    if (should_wrap_elements_as_ref()) {
        // Return a view with the actual element type (for correct data access)
        TSView result(element_value.data(), element_type, list_overlay() ? list_overlay()->child(index) : nullptr,
                      _root, std::move(child_path));
        // Store the expected REF type for the wrapper layer to use
        result.set_expected_element_ref_meta(_expected_element_ref_meta);

        // Set the bound output to the element's TSValue from the output container
        // This allows to_python() to create a TimeSeriesReference pointing to the correct output element
        if (_bound_output) {
            const TSValue* element_output = _bound_output->child_value(index);
            result.set_bound_output(element_output);
        }
        return result;
    }

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

TSSView::TSSView(const TSValue* tsd_source_ptr, const TSSTypeMeta* tss_meta) noexcept
    : TSView(nullptr, tss_meta)  // No direct data - we read from _tsd_source
{
    // Set the base class's _tsd_source member (moved from TSSView to TSView to prevent slicing)
    _tsd_source = tsd_source_ptr;

    // For key_set views, the overlay is the TSD's MapTSOverlay
    // This enables ts_valid(), modified_at(), etc. to work correctly
    if (tsd_source_ptr) {
        _overlay = const_cast<TSOverlayStorage*>(tsd_source_ptr->overlay());
    }
}

size_t TSSView::size() const noexcept {
    // For key_set views, get size from TSD's map
    if (_tsd_source) {
        return _tsd_source->value().view().as_map().size();
    }
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
    // IMPORTANT: Python auto-dereferences REF outputs at binding time, so when iterating
    // over bundle fields for delta_value, REF fields that have TimeSeriesReferences
    // pointing to targets should return the target's delta_value, not TimeSeriesReference.
    //
    // For REF OUTPUT fields, we need to check TWO things:
    // 1. If the REF value itself changed (new TimeSeriesReference binding)
    // 2. If the REF's TARGET value changed (underlying ts modified)
    // This mirrors Python's observe_reference mechanism where sink inputs are bound
    // directly to targets and detect target changes.
    nb::dict result;
    const TSBTypeMeta* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);

    for (auto& key : keys()) {
        TSView field_view = field(std::string(key));
        const TSBFieldInfo* field_info = bundle_meta->field(std::string(key));

        // Check if this is a REF field - needs special handling for target modification
        if (field_info && field_info->type->is_reference() && field_view.ts_valid()) {
            // REF field - check both REF modification AND target modification
            nb::object ref_value = field_view.to_python();
            if (!ref_value.is_none()) {
                try {
                    TimeSeriesReference ref = nb::cast<TimeSeriesReference>(ref_value);
                    if (ref.is_view_bound()) {
                        const TSValue* target = ref.view_output();
                        if (target) {
                            // Check if REF itself changed OR target is modified
                            bool ref_modified = field_view.modified_at(eval_time);
                            bool target_modified = target->modified_at(eval_time);
                            if (ref_modified || target_modified) {
                                result[nb::cast(std::string(key))] = target->view().to_python_delta();
                            }
                            continue;
                        }
                    }
                } catch (...) {
                    // Cast failed - use normal path
                }
            }
            // REF field but couldn't get target - check if REF itself modified
            if (field_view.modified_at(eval_time)) {
                result[nb::cast(std::string(key))] = field_view.to_python_delta();
            }
        } else if (field_view.modified_at(eval_time) && field_view.ts_valid()) {
            // Normal case (non-REF field)
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
        bool modified = elem.modified_at(eval_time);
        if (modified) {
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
    // ===== Key_set view: Read keys directly from TSD source =====
    // This is the correct approach - view the TSD as a TSS without creating a separate TSValue
    if (_tsd_source) {
        const TSMeta* source_meta = _tsd_source->ts_meta();
        if (source_meta && source_meta->kind() == TSTypeKind::TSD) {
            // Return keys from TSD directly as a frozenset
            value::ConstMapView tsd_map = _tsd_source->value().view().as_map();
            nb::set result;
            for (size_t i = 0; i < tsd_map.size(); ++i) {
                result.add(tsd_map.key_at(i).to_python());
            }
            return nb::frozenset(result);
        }
    }

    if (!valid()) return nb::none();

    // Python behavior: frozenset of element values
    nb::set result;
    value::ConstSetView set_view = _view.as_set();
    size_t count = set_view.size();

    for (size_t i = 0; i < count; ++i) {
        value::ConstValueView elem_view = set_view.at(i);
        nb::object py_elem = elem_view.to_python();
        result.add(py_elem);
    }
    return nb::frozenset(result);
}

nb::object TSSView::to_python_delta() const {
    // Get the current evaluation time from the owning node
    Node* node = owning_node();
    engine_time_t current_time = MIN_DT;
    if (node) {
        graph_ptr graph = node->graph();
        if (graph) {
            current_time = graph->evaluation_time();
        }
    }

    // ===== Key_set view: Build delta directly from TSD's overlay =====
    // This is the correct approach - view the TSD as a TSS without creating a separate TSValue
    if (_tsd_source) {
        const TSMeta* source_meta = _tsd_source->ts_meta();
        if (source_meta && source_meta->kind() == TSTypeKind::TSD) {
            // Get delta directly from TSD's MapTSOverlay
            auto* tsd_overlay = dynamic_cast<const MapTSOverlay*>(_tsd_source->overlay());

            if (tsd_overlay) {
                nb::set added_set;
                nb::set removed_set;

                // Get added keys from TSD
                const auto& added_key_indices = tsd_overlay->added_key_indices();
                if (!added_key_indices.empty()) {
                    // Access the TSD's data to get the actual key values
                    value::ConstMapView tsd_map = _tsd_source->value().view().as_map();
                    for (size_t idx : added_key_indices) {
                        if (idx < tsd_map.size()) {
                            auto added_key = tsd_map.key_at(idx);
                            added_set.add(added_key.to_python());
                        }
                    }
                }

                // Get removed keys from TSD (stored in overlay)
                const auto& removed_keys = tsd_overlay->removed_key_values();
                for (const auto& removed_key : removed_keys) {
                    removed_set.add(removed_key.view().to_python());
                }

                // Import Python SetDelta class and return
                auto PythonSetDelta = nb::module_::import_("hgraph._impl._types._tss").attr("PythonSetDelta");
                return PythonSetDelta(nb::frozenset(added_set), nb::frozenset(removed_set));
            }
        }
    }

    if (!valid()) return nb::none();

    // ===== Normal TSS case: Use SetDeltaView =====
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
