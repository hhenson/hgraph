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

    const TSMeta* ts(const value::TypeMeta* value_type);
    const TSMeta* tss(const value::TypeMeta* element_type);
    const TSMeta* tsd(const value::TypeMeta* key_type, const TSMeta* value_ts);
    const TSMeta* tsl(const TSMeta* element_ts, size_t fixed_size = 0);
    const TSMeta* tsw(const value::TypeMeta* value_type,
                      size_t period, size_t min_period = 0);
    const TSMeta* tsw_duration(const value::TypeMeta* value_type,
                               engine_time_delta_t time_range,
                               engine_time_delta_t min_time_range = engine_time_delta_t{0});
    const TSMeta* tsb(const std::vector<std::pair<std::string, const TSMeta*>>& fields,
                      const std::string& name,
                      nb::object python_type = nb::none());
    const TSMeta* ref(const TSMeta* referenced_ts);
    const TSMeta* signal();

    // ========== Schema Dereferencing ==========

    const TSMeta* dereference(const TSMeta* source);
    static bool contains_ref(const TSMeta* meta);

private:
    TSTypeRegistry() = default;
    ~TSTypeRegistry() = default;

    // ========== Storage ==========

    std::vector<std::unique_ptr<TSMeta>> schemas_;

    // ========== Caches ==========

    std::unordered_map<const value::TypeMeta*, const TSMeta*> ts_cache_;
    std::unordered_map<const value::TypeMeta*, const TSMeta*> tss_cache_;

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

    struct TSWKey {
        const value::TypeMeta* value_type;
        bool is_duration;
        int64_t range;
        int64_t min_range;
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

    std::unordered_map<const TSMeta*, const TSMeta*> ref_cache_;
    const TSMeta* signal_singleton_ = nullptr;
    std::unordered_map<const TSMeta*, const TSMeta*> deref_cache_;

    // ========== String Storage ==========

    std::vector<std::unique_ptr<char[]>> field_names_;
    std::vector<std::unique_ptr<TSBFieldInfo[]>> field_arrays_;

    // ========== Helper Methods ==========

    const char* intern_string(const std::string& s);
    TSMeta* create_schema();
};

// ============================================================================
// Time-Series Builders (header-only, inline)
// ============================================================================

/**
 * @brief Builder for TS[T] schemas (scalar time-series).
 */
class TSBuilder {
public:
    TSBuilder() = default;

    TSBuilder& set_value_type(const value::TypeMeta* type) {
        _value_type = type;
        return *this;
    }

    const TSMeta* build() {
        return TSTypeRegistry::instance().ts(_value_type);
    }

private:
    const value::TypeMeta* _value_type{nullptr};
};

/**
 * @brief Builder for TSB[Schema] schemas (time-series bundle).
 */
class TSBBuilder {
public:
    TSBBuilder() = default;

    TSBBuilder& set_name(const std::string& name) {
        _name = name;
        return *this;
    }

    TSBBuilder& add_field(const std::string& name, const TSMeta* ts) {
        _fields.emplace_back(name, ts);
        return *this;
    }

    TSBBuilder& set_python_type(nb::object py_type) {
        _python_type = std::move(py_type);
        return *this;
    }

    const TSMeta* build() {
        return TSTypeRegistry::instance().tsb(_fields, _name, _python_type);
    }

private:
    std::string _name;
    std::vector<std::pair<std::string, const TSMeta*>> _fields;
    nb::object _python_type = nb::none();
};

/**
 * @brief Builder for TSL[TS, Size] schemas (time-series list).
 */
class TSLBuilder {
public:
    TSLBuilder() = default;

    TSLBuilder& set_element_ts(const TSMeta* ts) {
        _element_ts = ts;
        return *this;
    }

    TSLBuilder& set_size(size_t size) {
        _fixed_size = size;
        return *this;
    }

    const TSMeta* build() {
        return TSTypeRegistry::instance().tsl(_element_ts, _fixed_size);
    }

private:
    const TSMeta* _element_ts{nullptr};
    size_t _fixed_size{0};
};

/**
 * @brief Builder for TSD[K, V] schemas (time-series dict).
 */
class TSDBuilder {
public:
    TSDBuilder() = default;

    TSDBuilder& set_key_type(const value::TypeMeta* type) {
        _key_type = type;
        return *this;
    }

    TSDBuilder& set_value_ts(const TSMeta* ts) {
        _value_ts = ts;
        return *this;
    }

    const TSMeta* build() {
        return TSTypeRegistry::instance().tsd(_key_type, _value_ts);
    }

private:
    const value::TypeMeta* _key_type{nullptr};
    const TSMeta* _value_ts{nullptr};
};

/**
 * @brief Builder for TSS[T] schemas (time-series set).
 */
class TSSBuilder {
public:
    TSSBuilder() = default;

    TSSBuilder& set_element_type(const value::TypeMeta* type) {
        _element_type = type;
        return *this;
    }

    const TSMeta* build() {
        return TSTypeRegistry::instance().tss(_element_type);
    }

private:
    const value::TypeMeta* _element_type{nullptr};
};

/**
 * @brief Builder for TSW[T] schemas (time-series window).
 *
 * Supports both tick-based and duration-based windows.
 */
class TSWBuilder {
public:
    TSWBuilder() = default;

    TSWBuilder& set_element_type(const value::TypeMeta* type) {
        _value_type = type;
        return *this;
    }

    TSWBuilder& set_period(size_t period) {
        _period = period;
        _is_duration = false;
        return *this;
    }

    TSWBuilder& set_min_window_period(engine_time_delta_t min_period) {
        _min_duration = min_period;
        _is_duration = true;
        return *this;
    }

    TSWBuilder& set_time_range(engine_time_delta_t time_range) {
        _duration = time_range;
        _is_duration = true;
        return *this;
    }

    TSWBuilder& set_min_period(size_t min_period) {
        _min_period = min_period;
        return *this;
    }

    const TSMeta* build() {
        if (_is_duration) {
            return TSTypeRegistry::instance().tsw_duration(_value_type, _duration, _min_duration);
        }
        return TSTypeRegistry::instance().tsw(_value_type, _period, _min_period);
    }

private:
    const value::TypeMeta* _value_type{nullptr};
    size_t _period{0};
    size_t _min_period{0};
    engine_time_delta_t _duration{0};
    engine_time_delta_t _min_duration{0};
    bool _is_duration{false};
};

/**
 * @brief Builder for REF[TS] schemas (time-series reference).
 */
class REFBuilder {
public:
    REFBuilder() = default;

    REFBuilder& set_target_ts(const TSMeta* ts) {
        _target_ts = ts;
        return *this;
    }

    const TSMeta* build() {
        return TSTypeRegistry::instance().ref(_target_ts);
    }

private:
    const TSMeta* _target_ts{nullptr};
};

} // namespace hgraph
