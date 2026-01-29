#pragma once

/**
 * @file ts_type_registry.h
 * @brief Central registry for time-series type metadata.
 *
 * The TSTypeRegistry is the single source of truth for TSMeta pointers.
 * All time-series types must be created through this registry. Creation
 * provides a cached TSMeta pointer used for type identity comparisons.
 *
 * Thread Safety:
 * - TSTypeRegistry is initialized as a function-local static, which is
 *   thread-safe per C++11.
 * - All factory methods are thread-safe for concurrent reads.
 * - Writes (new schema creation) are NOT thread-safe but are expected to
 *   occur during wiring (single-threaded phase under Python's GIL).
 * - For other use cases, ensure all type creation completes before
 *   concurrent access begins.
 */

#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/util/date_time.h>

#include <nanobind/nanobind.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace nb = nanobind;

namespace hgraph {

/**
 * @brief Central registry for all time-series type metadata.
 *
 * The TSTypeRegistry maintains ownership of all TSMeta instances and provides
 * factory methods for creating them. Types are created once and their metadata
 * pointers remain stable for the lifetime of the registry (process lifetime).
 *
 * Usage:
 * @code
 * auto& registry = TSTypeRegistry::instance();
 *
 * // Create simple time-series schemas
 * const TSMeta* ts_int = registry.ts(int_type_meta);
 * const TSMeta* tss_str = registry.tss(str_type_meta);
 *
 * // Create nested time-series schemas
 * const TSMeta* tsd_int_float = registry.tsd(int_type_meta, registry.ts(float_type_meta));
 * const TSMeta* tsl_ts = registry.tsl(ts_int, 5);  // Fixed size list
 *
 * // Create window schemas
 * const TSMeta* tsw_tick = registry.tsw(float_type_meta, 10, 5);  // tick-based
 * const TSMeta* tsw_dur = registry.tsw_duration(float_type_meta,
 *     std::chrono::minutes(5), std::chrono::minutes(1));  // duration-based
 *
 * // Create bundle schemas
 * std::vector<std::pair<std::string, const TSMeta*>> fields = {
 *     {"price", ts_float},
 *     {"volume", ts_int}
 * };
 * const TSMeta* tsb = registry.tsb(fields, "Quote");
 * @endcode
 */
class TSTypeRegistry {
public:
    /// Get the singleton instance
    static TSTypeRegistry& instance();

    // Deleted copy/move
    TSTypeRegistry(const TSTypeRegistry&) = delete;
    TSTypeRegistry& operator=(const TSTypeRegistry&) = delete;
    TSTypeRegistry(TSTypeRegistry&&) = delete;
    TSTypeRegistry& operator=(TSTypeRegistry&&) = delete;

    // ========== Factory Methods ==========

    /**
     * @brief Create a TS[T] schema for scalar time-series.
     *
     * @param value_type The TypeMeta for the scalar value type
     * @return Cached TSMeta pointer (same input = same pointer)
     */
    const TSMeta* ts(const value::TypeMeta* value_type);

    /**
     * @brief Create a TSS[T] schema for time-series set.
     *
     * @param element_type The TypeMeta for the set element type
     * @return Cached TSMeta pointer
     */
    const TSMeta* tss(const value::TypeMeta* element_type);

    /**
     * @brief Create a TSD[K, V] schema for time-series dict.
     *
     * @param key_type The TypeMeta for the dict key type
     * @param value_ts The TSMeta for the dict value time-series
     * @return Cached TSMeta pointer
     */
    const TSMeta* tsd(const value::TypeMeta* key_type, const TSMeta* value_ts);

    /**
     * @brief Create a TSL[TS, Size] schema for time-series list.
     *
     * @param element_ts The TSMeta for the list element time-series
     * @param fixed_size Fixed size (0 = dynamic SIZE)
     * @return Cached TSMeta pointer
     */
    const TSMeta* tsl(const TSMeta* element_ts, size_t fixed_size = 0);

    /**
     * @brief Create a TSW[T, period, min_period] schema for tick-based window.
     *
     * @param value_type The TypeMeta for the window value type
     * @param period Number of ticks in the window
     * @param min_period Minimum number of ticks required (default: 0)
     * @return Cached TSMeta pointer
     */
    const TSMeta* tsw(const value::TypeMeta* value_type,
                      size_t period, size_t min_period = 0);

    /**
     * @brief Create a TSW[T, time_range, min_time_range] schema for duration-based window.
     *
     * @param value_type The TypeMeta for the window value type
     * @param time_range Duration of the time window
     * @param min_time_range Minimum duration required (default: 0)
     * @return Cached TSMeta pointer
     */
    const TSMeta* tsw_duration(const value::TypeMeta* value_type,
                               engine_time_delta_t time_range,
                               engine_time_delta_t min_time_range = engine_time_delta_t{0});

    /**
     * @brief Create a TSB[Schema] schema for time-series bundle.
     *
     * TSB schemas are cached by structural identity (fields), not just name.
     * Two TSBs with the same fields return the same TSMeta pointer.
     *
     * @param fields Vector of (field_name, field_ts_meta) pairs
     * @param name Bundle schema name for display/debugging
     * @param python_type Optional Python class for to_python reconstruction
     * @return Cached TSMeta pointer
     */
    const TSMeta* tsb(const std::vector<std::pair<std::string, const TSMeta*>>& fields,
                      const std::string& name,
                      nb::object python_type = nb::none());

    /**
     * @brief Create a REF[TS] schema for time-series reference.
     *
     * @param referenced_ts The TSMeta for the referenced time-series
     * @return Cached TSMeta pointer
     */
    const TSMeta* ref(const TSMeta* referenced_ts);

    /**
     * @brief Get the SIGNAL schema singleton.
     *
     * SIGNAL is a marker time-series with no value type.
     *
     * @return The singleton SIGNAL TSMeta pointer
     */
    const TSMeta* signal();

    // ========== Schema Dereferencing ==========

    /**
     * @brief Get or create the dereferenced version of a schema.
     *
     * Recursively transforms REF[T] → T throughout the schema tree.
     * This is used by SIGNAL to bind to the actual data sources rather
     * than reference wrappers.
     *
     * If the schema contains no REF types, returns the original schema.
     * Results are cached for efficiency.
     *
     * @param source The source schema (may contain REF types)
     * @return The dereferenced schema (no REF types), or source if unchanged
     *
     * @code
     * // Example transformations:
     * // REF[TS[float]]                    → TS[float]
     * // TSB[a: REF[TS[int]], b: TS[str]]  → TSB[a: TS[int], b: TS[str]]
     * // TSD[str, REF[TS[float]]]          → TSD[str, TS[float]]
     * // TS[int]                           → TS[int] (unchanged)
     * @endcode
     */
    const TSMeta* dereference(const TSMeta* source);

    /**
     * @brief Check if a schema contains any REF types.
     *
     * Recursively checks the schema tree for REF nodes.
     *
     * @param meta The schema to check
     * @return true if the schema or any nested schema is REF
     */
    static bool contains_ref(const TSMeta* meta);

private:
    TSTypeRegistry() = default;
    ~TSTypeRegistry() = default;

    // ========== Storage ==========

    /// Storage for TSMeta instances (owns all created schemas)
    std::vector<std::unique_ptr<TSMeta>> schemas_;

    // ========== Caches ==========

    /// TS cache: value_type -> TSMeta*
    std::unordered_map<const value::TypeMeta*, const TSMeta*> ts_cache_;

    /// TSS cache: element_type -> TSMeta*
    std::unordered_map<const value::TypeMeta*, const TSMeta*> tss_cache_;

    /// TSD cache key: (key_type, value_ts)
    struct TSDKey {
        const value::TypeMeta* key_type;
        const TSMeta* value_ts;
        bool operator==(const TSDKey& other) const {
            return key_type == other.key_type && value_ts == other.value_ts;
        }
    };
    struct TSDKeyHash {
        size_t operator()(const TSDKey& k) const {
            return std::hash<const void*>{}(k.key_type) ^
                   (std::hash<const void*>{}(k.value_ts) << 1);
        }
    };
    std::unordered_map<TSDKey, const TSMeta*, TSDKeyHash> tsd_cache_;

    /// TSL cache key: (element_ts, fixed_size)
    struct TSLKey {
        const TSMeta* element_ts;
        size_t fixed_size;
        bool operator==(const TSLKey& other) const {
            return element_ts == other.element_ts && fixed_size == other.fixed_size;
        }
    };
    struct TSLKeyHash {
        size_t operator()(const TSLKey& k) const {
            return std::hash<const void*>{}(k.element_ts) ^
                   (std::hash<size_t>{}(k.fixed_size) << 1);
        }
    };
    std::unordered_map<TSLKey, const TSMeta*, TSLKeyHash> tsl_cache_;

    /// TSW cache key: (value_type, is_duration, range, min_range)
    struct TSWKey {
        const value::TypeMeta* value_type;
        bool is_duration;
        int64_t range;      // period or time_range.count()
        int64_t min_range;  // min_period or min_time_range.count()
        bool operator==(const TSWKey& other) const {
            return value_type == other.value_type &&
                   is_duration == other.is_duration &&
                   range == other.range &&
                   min_range == other.min_range;
        }
    };
    struct TSWKeyHash {
        size_t operator()(const TSWKey& k) const {
            size_t h = std::hash<const void*>{}(k.value_type);
            h ^= std::hash<bool>{}(k.is_duration) << 1;
            h ^= std::hash<int64_t>{}(k.range) << 2;
            h ^= std::hash<int64_t>{}(k.min_range) << 3;
            return h;
        }
    };
    std::unordered_map<TSWKey, const TSMeta*, TSWKeyHash> tsw_cache_;

    /// TSB cache key: structural identity (fields)
    struct TSBKey {
        std::vector<std::pair<std::string, const TSMeta*>> fields;
        std::string name;
        bool operator==(const TSBKey& other) const {
            if (name != other.name) return false;
            if (fields.size() != other.fields.size()) return false;
            for (size_t i = 0; i < fields.size(); ++i) {
                if (fields[i].first != other.fields[i].first) return false;
                if (fields[i].second != other.fields[i].second) return false;
            }
            return true;
        }
    };
    struct TSBKeyHash {
        size_t operator()(const TSBKey& k) const {
            size_t h = std::hash<std::string>{}(k.name);
            for (const auto& field : k.fields) {
                h ^= std::hash<std::string>{}(field.first) << 1;
                h ^= std::hash<const void*>{}(field.second) << 2;
            }
            return h;
        }
    };
    std::unordered_map<TSBKey, const TSMeta*, TSBKeyHash> tsb_cache_;

    /// REF cache: referenced_ts -> TSMeta*
    std::unordered_map<const TSMeta*, const TSMeta*> ref_cache_;

    /// SIGNAL singleton
    const TSMeta* signal_singleton_ = nullptr;

    /// Dereference cache: source_ts -> dereferenced_ts
    /// Note: If dereferenced == source, we still cache to avoid re-checking
    std::unordered_map<const TSMeta*, const TSMeta*> deref_cache_;

    // ========== String Storage ==========

    /// Field name string storage (owns field name strings)
    std::vector<std::unique_ptr<char[]>> field_names_;

    /// TSBFieldInfo array storage (owns field arrays)
    std::vector<std::unique_ptr<TSBFieldInfo[]>> field_arrays_;

    // ========== Helper Methods ==========

    /**
     * @brief Intern a string (store and return stable pointer).
     *
     * @param s The string to intern
     * @return Pointer to stable copy of the string
     */
    const char* intern_string(const std::string& s);

    /**
     * @brief Create a new TSMeta and store it.
     *
     * @return Pointer to the new TSMeta (owned by registry)
     */
    TSMeta* create_schema();
};

} // namespace hgraph
