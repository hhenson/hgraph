/**
 * @file ts_type_registry.cpp
 * @brief Implementation of the TSTypeRegistry singleton.
 *
 * This file implements the factory methods for creating cached TSMeta instances.
 */

#include <hgraph/types/time_series/ts_type_registry.h>

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

    // Create new schema
    auto* meta = create_schema();
    meta->kind = TSKind::TSS;
    meta->value_type = element_type;

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

    // Create new schema
    auto* meta = create_schema();
    meta->kind = TSKind::TSD;
    meta->key_type = key_type;
    meta->element_ts = value_ts;

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

    // Create new schema
    auto* meta = create_schema();
    meta->kind = TSKind::TSL;
    meta->element_ts = element_ts;
    meta->fixed_size = fixed_size;

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

    // Create new schema
    auto* meta = create_schema();
    meta->kind = TSKind::TSB;
    meta->fields = field_array.get();
    meta->field_count = field_count;
    meta->bundle_name = intern_string(name);
    meta->python_type = std::move(python_type);

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
    signal_singleton_ = meta;
    return meta;
}

} // namespace hgraph
