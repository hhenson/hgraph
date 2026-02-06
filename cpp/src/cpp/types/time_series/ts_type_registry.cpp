/**
 * @file ts_type_registry.cpp
 * @brief Implementation of the TSTypeRegistry singleton.
 *
 * This file implements the factory methods for creating cached TSMeta instances.
 */

#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/value/type_registry.h>

#include <cstring>

namespace hgraph {

// ============================================================================
// Singleton Instance
// ============================================================================

TSTypeRegistry& TSTypeRegistry::instance() {
    static TSTypeRegistry instance;
    return instance;
}

// ============================================================================
// Helper Methods
// ============================================================================

const char* TSTypeRegistry::intern_string(const std::string& s) {
    auto buf = std::make_unique<char[]>(s.size() + 1);
    std::memcpy(buf.get(), s.c_str(), s.size() + 1);
    const char* ptr = buf.get();
    field_names_.push_back(std::move(buf));
    return ptr;
}

TSMeta* TSTypeRegistry::create_schema() {
    auto meta = std::make_unique<TSMeta>();
    TSMeta* ptr = meta.get();
    schemas_.push_back(std::move(meta));
    return ptr;
}

// ============================================================================
// TS[T] - Scalar Time-Series
// ============================================================================

const TSMeta* TSTypeRegistry::ts(const value::TypeMeta* value_type) {
    // Check cache
    auto it = ts_cache_.find(value_type);
    if (it != ts_cache_.end()) {
        return it->second;
    }

    // Create new schema
    auto* meta = create_schema();
    meta->kind = TSKind::TSValue;
    meta->value_type = value_type;

    // Cache and return
    ts_cache_[value_type] = meta;
    return meta;
}

// ============================================================================
// TSS[T] - Time-Series Set
// ============================================================================

const TSMeta* TSTypeRegistry::tss(const value::TypeMeta* element_type) {
    // Check cache
    auto it = tss_cache_.find(element_type);
    if (it != tss_cache_.end()) {
        return it->second;
    }

    // Build the set value schema from element type
    // This is needed for TSValue to allocate SetStorage and for ts_ops::make_value_view
    const value::TypeMeta* value_schema = nullptr;
    if (element_type) {
        value_schema = value::TypeRegistry::instance()
            .set(element_type)
            .build();
    }

    // Create new schema
    auto* meta = create_schema();
    meta->kind = TSKind::TSS;
    meta->value_type = value_schema;  // Store set value schema for ts_ops (like TSD stores map schema)

    // Cache and return
    tss_cache_[element_type] = meta;
    return meta;
}

// ============================================================================
// TSD[K, V] - Time-Series Dict
// ============================================================================

const TSMeta* TSTypeRegistry::tsd(const value::TypeMeta* key_type, const TSMeta* value_ts) {
    // Check cache
    TSDKey cache_key{key_type, value_ts};
    auto it = tsd_cache_.find(cache_key);
    if (it != tsd_cache_.end()) {
        return it->second;
    }

    // Build the map value schema from key and value's value type
    // This is needed for ts_ops::make_value_view to work correctly
    const value::TypeMeta* value_schema = nullptr;
    if (key_type && value_ts && value_ts->value_type) {
        value_schema = value::TypeRegistry::instance()
            .map(key_type, value_ts->value_type)
            .build();
    }

    // Create new schema
    auto* meta = create_schema();
    meta->kind = TSKind::TSD;
    meta->key_type = key_type;
    meta->element_ts = value_ts;
    meta->value_type = value_schema;  // Store map value schema for ts_ops

    // Cache and return
    tsd_cache_[cache_key] = meta;
    return meta;
}

// ============================================================================
// TSL[TS, Size] - Time-Series List
// ============================================================================

const TSMeta* TSTypeRegistry::tsl(const TSMeta* element_ts, size_t fixed_size) {
    // Check cache
    TSLKey cache_key{element_ts, fixed_size};
    auto it = tsl_cache_.find(cache_key);
    if (it != tsl_cache_.end()) {
        return it->second;
    }

    // Build the list value schema from element's value type
    // This is needed for ts_ops::make_value_view to work correctly
    const value::TypeMeta* value_schema = nullptr;
    if (element_ts && element_ts->value_type) {
        value_schema = value::TypeRegistry::instance()
            .fixed_list(element_ts->value_type, fixed_size)
            .build();
    }

    // Create new schema
    auto* meta = create_schema();
    meta->kind = TSKind::TSL;
    meta->element_ts = element_ts;
    meta->fixed_size = fixed_size;
    meta->value_type = value_schema;  // Store list value schema for ts_ops

    // Cache and return
    tsl_cache_[cache_key] = meta;
    return meta;
}

// ============================================================================
// TSW[T, period, min_period] - Tick-Based Window
// ============================================================================

const TSMeta* TSTypeRegistry::tsw(const value::TypeMeta* value_type,
                                   size_t period, size_t min_period) {
    // Check cache
    TSWKey cache_key{
        value_type,
        false,  // is_duration = false
        static_cast<int64_t>(period),
        static_cast<int64_t>(min_period)
    };
    auto it = tsw_cache_.find(cache_key);
    if (it != tsw_cache_.end()) {
        return it->second;
    }

    // Create new schema
    auto* meta = create_schema();
    meta->kind = TSKind::TSW;
    meta->value_type = value_type;
    meta->is_duration_based = false;
    meta->window.tick.period = period;
    meta->window.tick.min_period = min_period;

    // Cache and return
    tsw_cache_[cache_key] = meta;
    return meta;
}

// ============================================================================
// TSW[T, time_range, min_time_range] - Duration-Based Window
// ============================================================================

const TSMeta* TSTypeRegistry::tsw_duration(const value::TypeMeta* value_type,
                                            engine_time_delta_t time_range,
                                            engine_time_delta_t min_time_range) {
    // Check cache
    TSWKey cache_key{
        value_type,
        true,  // is_duration = true
        time_range.count(),
        min_time_range.count()
    };
    auto it = tsw_cache_.find(cache_key);
    if (it != tsw_cache_.end()) {
        return it->second;
    }

    // Create new schema
    auto* meta = create_schema();
    meta->kind = TSKind::TSW;
    meta->value_type = value_type;
    meta->is_duration_based = true;
    meta->window.duration.time_range = time_range;
    meta->window.duration.min_time_range = min_time_range;

    // Cache and return
    tsw_cache_[cache_key] = meta;
    return meta;
}

// ============================================================================
// TSB[Schema] - Time-Series Bundle
// ============================================================================

const TSMeta* TSTypeRegistry::tsb(
    const std::vector<std::pair<std::string, const TSMeta*>>& fields,
    const std::string& name,
    nb::object python_type)
{
    // Check cache by structural identity
    TSBKey cache_key{fields, name};
    auto it = tsb_cache_.find(cache_key);
    if (it != tsb_cache_.end()) {
        return it->second;
    }

    // Allocate field info array
    const size_t field_count = fields.size();
    auto field_array = std::make_unique<TSBFieldInfo[]>(field_count);

    for (size_t i = 0; i < field_count; ++i) {
        field_array[i].name = intern_string(fields[i].first);
        field_array[i].index = i;
        field_array[i].ts_type = fields[i].second;
    }

    // Build the bundle value schema from fields
    // This is needed for ts_ops::make_value_view to work correctly
    const value::TypeMeta* value_schema = nullptr;
    if (field_count > 0) {
        auto builder = value::TypeRegistry::instance().bundle(name);
        for (size_t i = 0; i < field_count; ++i) {
            const auto& f = fields[i];
            if (f.second && f.second->value_type) {
                builder.field(f.first.c_str(), f.second->value_type);
            }
        }
        value_schema = builder.build();
    }

    // Create new schema
    auto* meta = create_schema();
    meta->kind = TSKind::TSB;
    meta->fields = field_array.get();
    meta->field_count = field_count;
    meta->bundle_name = intern_string(name);
    meta->python_type = std::move(python_type);
    meta->value_type = value_schema;  // Store bundle value schema for ts_ops

    // Store field array for ownership
    field_arrays_.push_back(std::move(field_array));

    // Cache and return
    tsb_cache_[cache_key] = meta;
    return meta;
}

// ============================================================================
// REF[TS] - Time-Series Reference
// ============================================================================

const TSMeta* TSTypeRegistry::ref(const TSMeta* referenced_ts) {
    // Check cache
    auto it = ref_cache_.find(referenced_ts);
    if (it != ref_cache_.end()) {
        return it->second;
    }

    // Create new schema
    auto* meta = create_schema();
    meta->kind = TSKind::REF;
    meta->element_ts = referenced_ts;
    // REF stores TSReference as its value type
    meta->value_type = TSMetaSchemaCache::instance().ts_reference_meta();

    // Cache and return
    ref_cache_[referenced_ts] = meta;
    return meta;
}

// ============================================================================
// SIGNAL - Singleton
// ============================================================================

const TSMeta* TSTypeRegistry::signal() {
    if (signal_singleton_) {
        return signal_singleton_;
    }

    auto* meta = create_schema();
    meta->kind = TSKind::SIGNAL;
    // SIGNAL needs a value_type so it can be included in bundle schemas
    // The value is a bool - True when signaled (matching Python's behavior)
    meta->value_type = value::scalar_type_meta<bool>();
    signal_singleton_ = meta;
    return meta;
}

// ============================================================================
// Schema Dereferencing
// ============================================================================

bool TSTypeRegistry::contains_ref(const TSMeta* meta) {
    if (!meta) {
        return false;
    }

    switch (meta->kind) {
        case TSKind::REF:
            return true;

        case TSKind::TSB:
            // Check all fields for REF
            for (size_t i = 0; i < meta->field_count; ++i) {
                if (contains_ref(meta->fields[i].ts_type)) {
                    return true;
                }
            }
            return false;

        case TSKind::TSL:
            // Check element type
            return contains_ref(meta->element_ts);

        case TSKind::TSD:
            // Check value type (keys are scalars, never TS)
            return contains_ref(meta->element_ts);

        case TSKind::TSValue:
        case TSKind::TSS:
        case TSKind::TSW:
        case TSKind::SIGNAL:
            // These don't contain nested TS types
            return false;

        default:
            return false;
    }
}

const TSMeta* TSTypeRegistry::dereference(const TSMeta* source) {
    if (!source) {
        return nullptr;
    }

    // Check cache first
    auto it = deref_cache_.find(source);
    if (it != deref_cache_.end()) {
        return it->second;
    }

    const TSMeta* result = nullptr;

    switch (source->kind) {
        case TSKind::REF: {
            // REF[T] → dereference(T)
            // The target might also contain REFs, so recurse
            result = dereference(source->element_ts);
            break;
        }

        case TSKind::TSB: {
            // Check if any field contains REF
            bool has_ref = false;
            for (size_t i = 0; i < source->field_count; ++i) {
                if (contains_ref(source->fields[i].ts_type)) {
                    has_ref = true;
                    break;
                }
            }

            if (!has_ref) {
                // No REFs in fields, return original
                result = source;
            } else {
                // Build dereferenced fields
                std::vector<std::pair<std::string, const TSMeta*>> deref_fields;
                deref_fields.reserve(source->field_count);

                for (size_t i = 0; i < source->field_count; ++i) {
                    const auto& field = source->fields[i];
                    deref_fields.emplace_back(
                        field.name,
                        dereference(field.ts_type)
                    );
                }

                // Create new TSB with dereferenced fields
                // Append "_deref" to name to distinguish from original
                std::string deref_name = source->bundle_name ? source->bundle_name : "";
                if (!deref_name.empty()) {
                    deref_name += "_deref";
                }

                result = tsb(deref_fields, deref_name, source->python_type);
            }
            break;
        }

        case TSKind::TSL: {
            // Dereference element type
            const TSMeta* deref_element = dereference(source->element_ts);
            if (deref_element == source->element_ts) {
                // No change
                result = source;
            } else {
                result = tsl(deref_element, source->fixed_size);
            }
            break;
        }

        case TSKind::TSD: {
            // Dereference value type (key type stays same - it's a scalar)
            const TSMeta* deref_value = dereference(source->element_ts);
            if (deref_value == source->element_ts) {
                // No change
                result = source;
            } else {
                result = tsd(source->key_type, deref_value);
            }
            break;
        }

        case TSKind::TSValue:
        case TSKind::TSS:
        case TSKind::TSW:
        case TSKind::SIGNAL:
            // These don't contain nested TS types with potential REFs
            result = source;
            break;

        default:
            result = source;
            break;
    }

    // Cache the result (even if unchanged, to avoid re-checking)
    deref_cache_[source] = result;
    return result;
}

} // namespace hgraph
