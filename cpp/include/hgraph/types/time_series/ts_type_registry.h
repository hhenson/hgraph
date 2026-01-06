//
// Created by Claude on 05/01/2025.
//

#ifndef HGRAPH_TS_TYPE_REGISTRY_H
#define HGRAPH_TS_TYPE_REGISTRY_H

#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/value/type_registry.h>
#include <nanobind/nanobind.h>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <vector>
#include <string>

namespace nb = nanobind;

namespace hgraph {

    /**
     * @brief Registry for interning TSMeta instances.
     *
     * Similar to TypeRegistry for TypeMeta, this ensures that structurally
     * identical time-series types share the same TSMeta pointer.
     *
     * Thread-safe for concurrent reads during execution.
     * Registration happens during graph construction under Python's GIL.
     */
    class TSTypeRegistry {
    public:
        /**
         * @brief Get the singleton instance.
         */
        static TSTypeRegistry& instance();

        // Delete copy/move
        TSTypeRegistry(const TSTypeRegistry&) = delete;
        TSTypeRegistry& operator=(const TSTypeRegistry&) = delete;

        // ========================================================================
        // TS[T] - Scalar time-series
        // ========================================================================

        /**
         * @brief Get or create a TSValueMeta for the given scalar type.
         */
        const TSValueMeta* ts(const value::TypeMeta* scalar_schema);

        // ========================================================================
        // TSB - Bundle
        // ========================================================================

        /**
         * @brief Get or create a TSBTypeMeta for the given fields.
         *
         * The value schema is constructed internally from the fields' value schemas.
         *
         * @param fields Vector of (name, ts_type) pairs
         * @param name Optional bundle name
         * @param python_type Optional Python type (e.g., CompoundScalar) for to_python
         */
        const TSBTypeMeta* tsb(const std::vector<std::pair<std::string, const TSMeta*>>& fields,
                               const std::string& name = "",
                               nb::object python_type = nb::object());

        // ========================================================================
        // TSL - List
        // ========================================================================

        /**
         * @brief Get or create a TSLTypeMeta for the given element type.
         *
         * The value schema is constructed internally from element_type->value_schema().
         *
         * @param element_type The time-series type of elements
         * @param fixed_size Fixed size (0 = dynamic)
         */
        const TSLTypeMeta* tsl(const TSMeta* element_type, size_t fixed_size);

        // ========================================================================
        // TSD - Dictionary
        // ========================================================================

        /**
         * @brief Get or create a TSDTypeMeta for the given key and value types.
         *
         * The value schema is constructed internally from key_type and value_type->value_schema().
         *
         * @param key_type The scalar type of keys
         * @param value_type The time-series type of values
         */
        const TSDTypeMeta* tsd(const value::TypeMeta* key_type,
                               const TSMeta* value_type);

        // ========================================================================
        // TSS - Set
        // ========================================================================

        /**
         * @brief Get or create a TSSTypeMeta for the given element type.
         *
         * The value schema is constructed internally as Set[element_type].
         *
         * @param element_type The scalar type of elements
         */
        const TSSTypeMeta* tss(const value::TypeMeta* element_type);

        // ========================================================================
        // TSW - Window
        // ========================================================================

        /**
         * @brief Get or create a size-based TSWTypeMeta (tick count window).
         *
         * The value schema is constructed internally as List[value_type].
         *
         * @param value_type The scalar type of values in the window
         * @param size Window size (number of ticks)
         * @param min_size Minimum size before window is valid (number of ticks)
         */
        const TSWTypeMeta* tsw(const value::TypeMeta* value_type,
                               size_t size, size_t min_size);

        /**
         * @brief Get or create a duration-based TSWTypeMeta (time window).
         *
         * The value schema is constructed internally as List[value_type].
         * Accepts Python timedelta directly via nanobind chrono conversion.
         *
         * @param value_type The scalar type of values in the window
         * @param time_range Time range as engine_time_delta_t (microseconds)
         * @param min_time_range Minimum time range before window is valid
         */
        const TSWTypeMeta* tsw_duration(const value::TypeMeta* value_type,
                                        engine_time_delta_t time_range,
                                        engine_time_delta_t min_time_range);

        // ========================================================================
        // REF - Reference
        // ========================================================================

        /**
         * @brief Get or create a REFTypeMeta for the given referenced type.
         */
        const REFTypeMeta* ref(const TSMeta* referenced_type);

        // ========================================================================
        // SIGNAL
        // ========================================================================

        /**
         * @brief Get the singleton SignalTypeMeta.
         */
        const SignalTypeMeta* signal();

    private:
        TSTypeRegistry() = default;

        // Storage for owned TSMeta instances
        std::vector<std::unique_ptr<TSMeta>> _owned;

        // Cache keys for structural identity
        // Key format varies by type kind

        // TS[T] cache: keyed by scalar schema pointer
        std::unordered_map<const value::TypeMeta*, const TSValueMeta*> _ts_cache;

        // TSB cache: keyed by sorted field names + types (complex key)
        struct TSBKey {
            std::vector<std::pair<std::string, const TSMeta*>> fields;
            std::string name;

            bool operator==(const TSBKey& other) const;
        };
        struct TSBKeyHash {
            size_t operator()(const TSBKey& key) const;
        };
        std::unordered_map<TSBKey, const TSBTypeMeta*, TSBKeyHash> _tsb_cache;

        // TSL cache: keyed by (element_type, fixed_size)
        struct TSLKey {
            const TSMeta* element_type;
            size_t fixed_size;

            bool operator==(const TSLKey& other) const {
                return element_type == other.element_type && fixed_size == other.fixed_size;
            }
        };
        struct TSLKeyHash {
            size_t operator()(const TSLKey& key) const {
                return std::hash<const void*>{}(key.element_type) ^
                       (std::hash<size_t>{}(key.fixed_size) << 1);
            }
        };
        std::unordered_map<TSLKey, const TSLTypeMeta*, TSLKeyHash> _tsl_cache;

        // TSD cache: keyed by (key_type, value_type)
        struct TSDKey {
            const value::TypeMeta* key_type;
            const TSMeta* value_type;

            bool operator==(const TSDKey& other) const {
                return key_type == other.key_type && value_type == other.value_type;
            }
        };
        struct TSDKeyHash {
            size_t operator()(const TSDKey& key) const {
                return std::hash<const void*>{}(key.key_type) ^
                       (std::hash<const void*>{}(key.value_type) << 1);
            }
        };
        std::unordered_map<TSDKey, const TSDTypeMeta*, TSDKeyHash> _tsd_cache;

        // TSS cache: keyed by element_type pointer
        std::unordered_map<const value::TypeMeta*, const TSSTypeMeta*> _tss_cache;

        // TSW cache: keyed by (value_type, is_time_based, size/time_range, min_size/min_time_range)
        struct TSWKey {
            const value::TypeMeta* value_type;
            bool is_time_based;
            int64_t size_or_time;      // size (ticks) or time_range.count()
            int64_t min_size_or_time;  // min_size (ticks) or min_time_range.count()

            bool operator==(const TSWKey& other) const {
                return value_type == other.value_type &&
                       is_time_based == other.is_time_based &&
                       size_or_time == other.size_or_time &&
                       min_size_or_time == other.min_size_or_time;
            }
        };
        struct TSWKeyHash {
            size_t operator()(const TSWKey& key) const {
                return std::hash<const void*>{}(key.value_type) ^
                       (std::hash<bool>{}(key.is_time_based) << 1) ^
                       (std::hash<int64_t>{}(key.size_or_time) << 2) ^
                       (std::hash<int64_t>{}(key.min_size_or_time) << 3);
            }
        };
        std::unordered_map<TSWKey, const TSWTypeMeta*, TSWKeyHash> _tsw_cache;

        // REF cache: keyed by referenced_type pointer
        std::unordered_map<const TSMeta*, const REFTypeMeta*> _ref_cache;

        // Singleton signal instance
        std::unique_ptr<SignalTypeMeta> _signal;

        // Mutex for thread-safe registration
        mutable std::mutex _mutex;
    };

    /**
     * @brief Register TSTypeRegistry bindings with nanobind.
     */
    void register_ts_type_registry_with_nanobind(nb::module_& m);

} // namespace hgraph

#endif // HGRAPH_TS_TYPE_REGISTRY_H
