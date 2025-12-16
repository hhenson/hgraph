//
// Created by Claude on 15/12/2025.
//
// TimeSeriesTypeMeta implementations
//

#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_v2_types.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {

// ============================================================================
// TimeSeriesTypeMeta - Base class default implementations
// ============================================================================

time_series_output_s_ptr TimeSeriesTypeMeta::make_output(time_series_output_ptr owning_output) const {
    // Default: create using the parent's owning node
    // This creates time-series with node parentage rather than time-series parentage
    // Concrete types may override this if they need different behavior
    return make_output(owning_output->owning_node());
}

time_series_input_s_ptr TimeSeriesTypeMeta::make_input(time_series_input_ptr owning_input) const {
    // Default: create using the parent's owning node
    return make_input(owning_input->owning_node());
}

// ============================================================================
// TSTypeMeta - TS[T]
// ============================================================================

std::string TSTypeMeta::type_name_str() const {
    if (name) return name;
    return "TS[" + scalar_type->type_name_str() + "]";
}

time_series_output_s_ptr TSTypeMeta::make_output(node_ptr owning_node) const {
    return arena_make_shared_as<TsOutput, TimeSeriesOutput>(owning_node, this);
}

time_series_input_s_ptr TSTypeMeta::make_input(node_ptr owning_node) const {
    return arena_make_shared_as<TsInput, TimeSeriesInput>(owning_node, this);
}

size_t TSTypeMeta::output_memory_size() const {
    return sizeof(TsOutput);
}

size_t TSTypeMeta::input_memory_size() const {
    return sizeof(TsInput);
}

// ============================================================================
// TSSTypeMeta - TSS[T]
// ============================================================================

std::string TSSTypeMeta::type_name_str() const {
    if (name) return name;
    return "TSS[" + element_type->type_name_str() + "]";
}

time_series_output_s_ptr TSSTypeMeta::make_output(node_ptr owning_node) const {
    return arena_make_shared_as<TssOutput, TimeSeriesOutput>(owning_node, this);
}

time_series_input_s_ptr TSSTypeMeta::make_input(node_ptr owning_node) const {
    return arena_make_shared_as<TssInput, TimeSeriesInput>(owning_node, this);
}

size_t TSSTypeMeta::output_memory_size() const {
    return sizeof(TssOutput);
}

size_t TSSTypeMeta::input_memory_size() const {
    return sizeof(TssInput);
}

// ============================================================================
// TSDTypeMeta - TSD[K, V]
// ============================================================================

std::string TSDTypeMeta::type_name_str() const {
    if (name) return name;
    return "TSD[" + key_type->type_name_str() + ", " +
           value_ts_type->type_name_str() + "]";
}

time_series_output_s_ptr TSDTypeMeta::make_output(node_ptr owning_node) const {
    // TSD V2 not yet implemented - falls back to V1 via cpp_type_meta returning None
    return {};
}

time_series_input_s_ptr TSDTypeMeta::make_input(node_ptr owning_node) const {
    // TSD V2 not yet implemented - falls back to V1 via cpp_type_meta returning None
    return {};
}

size_t TSDTypeMeta::output_memory_size() const {
    // TSD V2 not yet implemented
    return 0;
}

size_t TSDTypeMeta::input_memory_size() const {
    // TSD V2 not yet implemented
    return 0;
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

time_series_output_s_ptr TSLTypeMeta::make_output(node_ptr owning_node) const {
    return arena_make_shared_as<TslOutput, TimeSeriesOutput>(owning_node, this);
}

time_series_input_s_ptr TSLTypeMeta::make_input(node_ptr owning_node) const {
    return arena_make_shared_as<TslInput, TimeSeriesInput>(owning_node, this);
}

size_t TSLTypeMeta::output_memory_size() const {
    // Base size plus vector overhead (elements allocated separately)
    return sizeof(TslOutput);
}

size_t TSLTypeMeta::input_memory_size() const {
    return sizeof(TslInput);
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

time_series_output_s_ptr TSBTypeMeta::make_output(node_ptr owning_node) const {
    return arena_make_shared_as<TsbOutput, TimeSeriesOutput>(owning_node, this);
}

time_series_input_s_ptr TSBTypeMeta::make_input(node_ptr owning_node) const {
    return arena_make_shared_as<TsbInput, TimeSeriesInput>(owning_node, this);
}

size_t TSBTypeMeta::output_memory_size() const {
    return sizeof(TsbOutput);
}

size_t TSBTypeMeta::input_memory_size() const {
    return sizeof(TsbInput);
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

time_series_output_s_ptr TSWTypeMeta::make_output(node_ptr owning_node) const {
    // TSW V2 not yet implemented - falls back to V1
    return {};
}

time_series_input_s_ptr TSWTypeMeta::make_input(node_ptr owning_node) const {
    // TSW V2 not yet implemented - falls back to V1
    return {};
}

size_t TSWTypeMeta::output_memory_size() const {
    // TSW V2 not yet implemented
    return 0;
}

size_t TSWTypeMeta::input_memory_size() const {
    // TSW V2 not yet implemented
    return 0;
}

// ============================================================================
// REFTypeMeta - REF[TS_TYPE]
// ============================================================================

std::string REFTypeMeta::type_name_str() const {
    if (name) return name;
    return "REF[" + value_ts_type->type_name_str() + "]";
}

time_series_output_s_ptr REFTypeMeta::make_output(node_ptr owning_node) const {
    // REF V2 not yet implemented - falls back to V1
    return {};
}

time_series_input_s_ptr REFTypeMeta::make_input(node_ptr owning_node) const {
    // REF V2 not yet implemented - falls back to V1
    return {};
}

size_t REFTypeMeta::output_memory_size() const {
    // REF V2 not yet implemented
    return 0;
}

size_t REFTypeMeta::input_memory_size() const {
    // REF V2 not yet implemented
    return 0;
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
