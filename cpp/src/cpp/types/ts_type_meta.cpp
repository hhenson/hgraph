//
// Created by Claude on 15/12/2025.
//
// TimeSeriesTypeMeta implementations
//

#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_type_registry.h>

namespace hgraph {

// ============================================================================
// TSTypeMeta - TS[T]
// ============================================================================

std::string TSTypeMeta::type_name_str() const {
    if (name) return name;
    return "TS[" + scalar_type->type_name_str() + "]";
}

// ============================================================================
// TSSTypeMeta - TSS[T]
// ============================================================================

std::string TSSTypeMeta::type_name_str() const {
    if (name) return name;
    return "TSS[" + element_type->type_name_str() + "]";
}

// ============================================================================
// TSDTypeMeta - TSD[K, V]
// ============================================================================

std::string TSDTypeMeta::type_name_str() const {
    if (name) return name;
    return "TSD[" + key_type->type_name_str() + ", " +
           value_ts_type->type_name_str() + "]";
}

// ============================================================================
// TSLTypeMeta - TSL[V, Size]
// ============================================================================

std::string TSLTypeMeta::type_name_str() const {
    if (name) return name;
    std::string result = "TSL[" + element_ts_type->type_name_str();
    if (size >= 0) {
        result += ", Size[" + std::to_string(size) + "]";
    }
    result += "]";
    return result;
}

// ============================================================================
// TSBTypeMeta - TSB[Schema]
// ============================================================================

std::string TSBTypeMeta::type_name_str() const {
    if (name) return name;
    std::string result = "TSB[";
    bool first = true;
    for (const auto& field : fields) {
        if (!first) result += ", ";
        first = false;
        result += field.name + ": " + field.type->type_name_str();
    }
    result += "]";
    return result;
}

// ============================================================================
// TSWTypeMeta - TSW[T, Size]
// ============================================================================

std::string TSWTypeMeta::type_name_str() const {
    if (name) return name;
    std::string result = "TSW[" + scalar_type->type_name_str();
    if (size >= 0) {
        result += ", " + std::to_string(size);
    }
    result += "]";
    return result;
}

// ============================================================================
// REFTypeMeta - REF[TS_TYPE]
// ============================================================================

std::string REFTypeMeta::type_name_str() const {
    if (name) return name;
    return "REF[" + value_ts_type->type_name_str() + "]";
}

// ============================================================================
// TimeSeriesTypeRegistry
// ============================================================================

TimeSeriesTypeRegistry& TimeSeriesTypeRegistry::global() {
    static TimeSeriesTypeRegistry instance;
    return instance;
}

const TimeSeriesTypeMeta* TimeSeriesTypeRegistry::register_by_key(
    size_t key, std::unique_ptr<TimeSeriesTypeMeta> meta) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _types.find(key);
    if (it != _types.end()) {
        return it->second.get();
    }
    auto* ptr = meta.get();
    _types[key] = std::move(meta);
    return ptr;
}

const TimeSeriesTypeMeta* TimeSeriesTypeRegistry::lookup_by_key(size_t key) const {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _types.find(key);
    return it != _types.end() ? it->second.get() : nullptr;
}

bool TimeSeriesTypeRegistry::contains_key(size_t key) const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _types.contains(key);
}

size_t TimeSeriesTypeRegistry::cache_size() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _types.size();
}

} // namespace hgraph
