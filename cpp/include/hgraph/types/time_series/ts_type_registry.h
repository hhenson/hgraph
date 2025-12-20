//
// Created by Claude on 15/12/2025.
//
// TSTypeRegistry - Caches TSMeta instances
//

#ifndef HGRAPH_TS_TYPE_REGISTRY_H
#define HGRAPH_TS_TYPE_REGISTRY_H

#include <hgraph/types/time_series/ts_type_meta.h>
#include <unordered_map>
#include <memory>
#include <mutex>

namespace hgraph {

/**
 * TSTypeRegistry - Central registry for time-series type metadata
 *
 * Provides:
 * - Registration of types by hash key
 * - Lookup of types by hash key
 * - Ownership of dynamically created type metadata
 * - Thread-safe registration
 *
 * Usage:
 *   auto& registry = TSTypeRegistry::global();
 *   size_t key = ts_hash_combine(TS_SEED, ptr_hash);
 *   if (auto* existing = registry.lookup_by_key(key)) {
 *       return existing;
 *   }
 *   return registry.register_by_key(key, std::move(meta));
 */
class TSTypeRegistry {
public:
    /**
     * Get the global singleton instance
     */
    static TSTypeRegistry& global();

    /**
     * Register a type by hash key (for caching)
     * Thread-safe, returns existing if key already registered
     */
    const TSMeta* register_by_key(size_t key,
                                               std::unique_ptr<TSMeta> meta);

    /**
     * Lookup by hash key (returns nullptr if not found)
     */
    [[nodiscard]] const TSMeta* lookup_by_key(size_t key) const;

    /**
     * Check if hash key exists
     */
    [[nodiscard]] bool contains_key(size_t key) const;

    /**
     * Get number of cached types
     */
    [[nodiscard]] size_t cache_size() const;

private:
    TSTypeRegistry() = default;
    mutable std::mutex _mutex;
    std::unordered_map<size_t, std::unique_ptr<TSMeta>> _types;
};

/**
 * Hash combining utility for building composite type keys
 * Uses Boost-style hash combine
 */
inline size_t ts_hash_combine(size_t h1, size_t h2) {
    return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
}

} // namespace hgraph

#endif // HGRAPH_TS_TYPE_REGISTRY_H
