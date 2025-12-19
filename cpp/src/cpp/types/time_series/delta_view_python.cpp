//
// Created by Claude on 19/12/2025.
//
// Python conversion implementation for DeltaView
//

#include <hgraph/types/time_series/delta_view_python.h>
#include <hgraph/types/value/python_conversion.h>
#include <hgraph/types/value/set_type.h>
#include <hgraph/types/value/dict_type.h>

namespace hgraph::ts {

// Cached Python type references
static nb::object g_python_set_delta_class;
static nb::object g_remove_sentinel;
static nb::object g_frozendict_class;
static bool g_types_initialized = false;

void init_delta_python_types() {
    if (g_types_initialized) return;

    try {
        // Import PythonSetDelta from hgraph._impl._types._tss
        auto tss_module = nb::module_::import_("hgraph._impl._types._tss");
        g_python_set_delta_class = tss_module.attr("PythonSetDelta");

        // Import REMOVE from hgraph._types._tsd_type
        auto tsd_module = nb::module_::import_("hgraph._types._tsd_type");
        g_remove_sentinel = tsd_module.attr("REMOVE");

        // Import frozendict
        auto frozendict_module = nb::module_::import_("frozendict");
        g_frozendict_class = frozendict_module.attr("frozendict");

        g_types_initialized = true;
    } catch (const nb::python_error& e) {
        // Types not available - will fall back to basic conversion
        g_types_initialized = false;
    }
}

bool delta_python_types_initialized() {
    return g_types_initialized;
}

// Forward declaration for recursive calls
static nb::object delta_to_python_impl(const DeltaView& view);

// Scalar delta - just return the value
static nb::object scalar_delta_to_python(const DeltaView& view) {
    auto value_view = view.scalar_delta();
    if (!value_view.valid()) return nb::none();

    return value::value_to_python(value_view.data(), value_view.schema());
}

// Bundle delta - dict of {field_name: delta_value} for modified fields
static nb::object bundle_delta_to_python(const DeltaView& view) {
    nb::dict result;

    size_t count = view.bundle_field_count();
    for (size_t i = 0; i < count; ++i) {
        if (view.bundle_field_modified(i)) {
            auto field_view = view.bundle_field(i);
            if (field_view.valid()) {
                auto name = view.bundle_field_name(i);
                nb::object field_delta = delta_to_python_impl(field_view);
                result[nb::cast(std::string(name))] = field_delta;
            }
        }
    }

    return result;
}

// List delta - dict of {index: delta_value} for modified elements
static nb::object list_delta_to_python(const DeltaView& view) {
    nb::dict result;

    size_t count = view.list_element_count();
    for (size_t i = 0; i < count; ++i) {
        if (view.list_element_modified(i)) {
            auto elem_view = view.list_element(i);
            if (elem_view.valid()) {
                nb::object elem_delta = delta_to_python_impl(elem_view);
                result[nb::cast(static_cast<int64_t>(i))] = elem_delta;
            }
        }
    }

    return result;
}

// Set delta - PythonSetDelta with added/removed frozensets
static nb::object set_delta_to_python(const DeltaView& view) {
    if (!g_types_initialized) {
        init_delta_python_types();
    }

    auto* storage = view.set_storage();
    if (!storage) return nb::none();

    // Get the set meta for element type
    auto* meta = view.meta();
    if (!meta || meta->ts_kind != TimeSeriesKind::TSS) return nb::none();
    auto* tss_meta = static_cast<const TSSTypeMeta*>(meta);
    auto* element_type = tss_meta->element_type;

    engine_time_t time = view.time();
    auto& tracker = const_cast<value::ModificationTracker&>(view.tracker());

    // Build added set - elements added at current time
    nb::set added_set;
    for (auto elem : *storage) {
        // Look up the element's index in the storage
        auto opt_index = storage->find_index(elem.ptr);
        if (opt_index && tracker.set_element_added_at(*opt_index, time)) {
            nb::object py_elem = value::value_to_python(elem.ptr, element_type);
            added_set.add(py_elem);
        }
    }

    // Build removed set - from tracker's removed elements
    nb::set removed_set;
    size_t removed_count = tracker.set_removed_count();
    for (size_t i = 0; i < removed_count; ++i) {
        auto removed = tracker.set_removed_element(i);
        if (removed.ptr) {
            nb::object py_elem = value::value_to_python(removed.ptr, element_type);
            removed_set.add(py_elem);
        }
    }

    // Convert to frozensets
    nb::frozenset added_frozen(added_set);
    nb::frozenset removed_frozen(removed_set);

    // Create PythonSetDelta
    if (g_types_initialized && g_python_set_delta_class.is_valid()) {
        return g_python_set_delta_class(added_frozen, removed_frozen);
    }

    // Fallback: return tuple of (added, removed)
    return nb::make_tuple(added_frozen, removed_frozen);
}

// Dict delta - frozendict with modified entries, removed keys → REMOVE
static nb::object dict_delta_to_python(const DeltaView& view) {
    if (!g_types_initialized) {
        init_delta_python_types();
    }

    auto* storage = view.dict_storage();
    if (!storage) return nb::none();

    // Get the dict meta for key/value types
    auto* meta = view.meta();
    if (!meta || meta->ts_kind != TimeSeriesKind::TSD) return nb::none();
    auto* tsd_meta = static_cast<const TSDTypeMeta*>(meta);
    auto* key_type = tsd_meta->key_type;
    auto* value_ts_meta = tsd_meta->value_ts_type;

    // Get the value schema from the value TS meta
    // For TSD[K, TS[V]], value_ts_meta is TSTypeMeta with scalar_type pointing to V
    auto* value_schema = value_ts_meta->value_schema();
    if (!value_schema) return nb::none();

    // Build dict with modified entries
    nb::dict result;

    // Iterate over current entries, adding modified ones
    // ConstIterator returns ConstKeyValuePair with index included
    for (auto kv : *storage) {
        if (view.dict_entry_modified(kv.index)) {
            nb::object py_key = value::value_to_python(kv.key.ptr, key_type);
            // For the value, convert using the value schema
            nb::object py_value = value::value_to_python(kv.value.ptr, value_schema);
            result[py_key] = py_value;
        }
    }

    // Add removed keys with REMOVE sentinel
    size_t removed_count = view.dict_removed_count();
    for (size_t i = 0; i < removed_count; ++i) {
        auto removed_key = view.tracker().dict_removed_key(i);
        if (removed_key.ptr) {
            nb::object py_key = value::value_to_python(removed_key.ptr, key_type);
            if (g_types_initialized && g_remove_sentinel.is_valid()) {
                result[py_key] = g_remove_sentinel;
            } else {
                // Fallback: use None to indicate removal
                result[py_key] = nb::none();
            }
        }
    }

    // Convert to frozendict if available
    if (g_types_initialized && g_frozendict_class.is_valid()) {
        return g_frozendict_class(result);
    }

    return result;
}

// Ref delta - just return the value (same as scalar)
static nb::object ref_delta_to_python(const DeltaView& view) {
    auto value_view = view.ref_delta();
    if (!value_view.valid()) return nb::none();

    return value::value_to_python(value_view.data(), value_view.schema());
}

// Main dispatch function
static nb::object delta_to_python_impl(const DeltaView& view) {
    if (!view.valid()) return nb::none();

    switch (view.ts_kind()) {
        case TimeSeriesKind::TS:
            return scalar_delta_to_python(view);
        case TimeSeriesKind::TSB:
            return bundle_delta_to_python(view);
        case TimeSeriesKind::TSL:
            return list_delta_to_python(view);
        case TimeSeriesKind::TSS:
            return set_delta_to_python(view);
        case TimeSeriesKind::TSD:
            return dict_delta_to_python(view);
        case TimeSeriesKind::REF:
            return ref_delta_to_python(view);
        default:
            // For signal and other types, return the value
            return scalar_delta_to_python(view);
    }
}

nb::object delta_to_python(const DeltaView& view) {
    return delta_to_python_impl(view);
}

} // namespace hgraph::ts
