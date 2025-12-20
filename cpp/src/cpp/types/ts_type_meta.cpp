//
// Created by Claude on 15/12/2025.
//
// TSMeta implementations
//

#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {

// ============================================================================
// TSMeta - Base class default implementations
// ============================================================================

time_series_output_s_ptr TSMeta::make_output(time_series_output_ptr owning_output) const {
    // Default: create using the parent's owning node
    // This creates time-series with node parentage rather than time-series parentage
    // Concrete types may override this if they need different behavior
    return make_output(owning_output->owning_node());
}

time_series_input_s_ptr TSMeta::make_input(time_series_input_ptr owning_input) const {
    // Default: create using the parent's owning node
    return make_input(owning_input->owning_node());
}

// ============================================================================
// TSValueMeta - TS[T]
// ============================================================================

std::string TSValueMeta::type_name_str() const {
    if (name) return name;
    return "TS[" + scalar_type->type_name_str() + "]";
}

time_series_output_s_ptr TSValueMeta::make_output(node_ptr owning_node) const {
    // Not yet implemented - falls back to Python implementation
    return {};
}

time_series_input_s_ptr TSValueMeta::make_input(node_ptr owning_node) const {
    // Not yet implemented - falls back to Python implementation
    return {};
}

size_t TSValueMeta::output_memory_size() const {
    return 0;  // Not yet implemented
}

size_t TSValueMeta::input_memory_size() const {
    return 0;  // Not yet implemented
}

// ============================================================================
// TSSTypeMeta - TSS[T]
// ============================================================================

std::string TSSTypeMeta::type_name_str() const {
    if (name) return name;
    return "TSS[" + element_type->type_name_str() + "]";
}

time_series_output_s_ptr TSSTypeMeta::make_output(node_ptr owning_node) const {
    // Not yet implemented
    return {};
}

time_series_input_s_ptr TSSTypeMeta::make_input(node_ptr owning_node) const {
    // Not yet implemented
    return {};
}

size_t TSSTypeMeta::output_memory_size() const {
    return 0;  // Not yet implemented
}

size_t TSSTypeMeta::input_memory_size() const {
    return 0;  // Not yet implemented
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
    // Not yet implemented
    return {};
}

time_series_input_s_ptr TSDTypeMeta::make_input(node_ptr owning_node) const {
    // Not yet implemented
    return {};
}

size_t TSDTypeMeta::output_memory_size() const {
    return 0;  // Not yet implemented
}

size_t TSDTypeMeta::input_memory_size() const {
    return 0;  // Not yet implemented
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
    // Not yet implemented
    return {};
}

time_series_input_s_ptr TSLTypeMeta::make_input(node_ptr owning_node) const {
    // Not yet implemented
    return {};
}

size_t TSLTypeMeta::output_memory_size() const {
    return 0;  // Not yet implemented
}

size_t TSLTypeMeta::input_memory_size() const {
    return 0;  // Not yet implemented
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
    // Not yet implemented
    return {};
}

time_series_input_s_ptr TSBTypeMeta::make_input(node_ptr owning_node) const {
    // Not yet implemented
    return {};
}

size_t TSBTypeMeta::output_memory_size() const {
    return 0;  // Not yet implemented
}

size_t TSBTypeMeta::input_memory_size() const {
    return 0;  // Not yet implemented
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
    // Not yet implemented
    return {};
}

time_series_input_s_ptr TSWTypeMeta::make_input(node_ptr owning_node) const {
    // Not yet implemented
    return {};
}

size_t TSWTypeMeta::output_memory_size() const {
    return 0;  // Not yet implemented
}

size_t TSWTypeMeta::input_memory_size() const {
    return 0;  // Not yet implemented
}

// ============================================================================
// REFTypeMeta - REF[TS_TYPE]
// ============================================================================

std::string REFTypeMeta::type_name_str() const {
    if (name) return name;
    return "REF[" + value_ts_type->type_name_str() + "]";
}

time_series_output_s_ptr REFTypeMeta::make_output(node_ptr owning_node) const {
    // Create a TSOutput for REF type with RefStorage value schema
    return std::make_shared<ts::TSOutput>(this, owning_node);
}

time_series_input_s_ptr REFTypeMeta::make_input(node_ptr owning_node) const {
    // Create a TSInput for REF type
    return std::make_shared<ts::TSInput>(this, owning_node);
}

size_t REFTypeMeta::output_memory_size() const {
    return 0;  // Not yet implemented
}

size_t REFTypeMeta::input_memory_size() const {
    return 0;  // Not yet implemented
}

// ============================================================================
// TSTypeRegistry
// ============================================================================

TSTypeRegistry& TSTypeRegistry::global() {
    static TSTypeRegistry instance;
    return instance;
}

const TSMeta* TSTypeRegistry::register_by_key(
    size_t key, std::unique_ptr<TSMeta> meta) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _types.find(key);
    if (it != _types.end()) {
        return it->second.get();
    }
    auto* ptr = meta.get();
    _types[key] = std::move(meta);
    return ptr;
}

const TSMeta* TSTypeRegistry::lookup_by_key(size_t key) const {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _types.find(key);
    return it != _types.end() ? it->second.get() : nullptr;
}

bool TSTypeRegistry::contains_key(size_t key) const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _types.contains(key);
}

size_t TSTypeRegistry::cache_size() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _types.size();
}

} // namespace hgraph
