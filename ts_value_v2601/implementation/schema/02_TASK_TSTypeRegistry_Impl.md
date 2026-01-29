# Task 02: TSTypeRegistry Implementation

## Objective

Create the singleton registry that creates, caches, and manages TSMeta instances.

## Files to Create

1. `cpp/include/hgraph/types/time_series/ts_type_registry.h` - Declaration
2. `cpp/src/cpp/types/time_series/ts_type_registry.cpp` - Implementation

## Header Design

```cpp
#pragma once

#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/value/type_meta.h>
#include <nanobind/nanobind.h>

#include <unordered_map>
#include <vector>
#include <string>
#include <memory>

namespace hgraph {

namespace nb = nanobind;

class TSTypeRegistry {
public:
    // Singleton access
    static TSTypeRegistry& instance();

    // Factory methods - return cached TSMeta*

    // TS[T] - scalar time-series
    const TSMeta* ts(const value::TypeMeta* value_type);

    // TSS[T] - time-series set
    const TSMeta* tss(const value::TypeMeta* element_type);

    // TSD[K, V] - time-series dict
    const TSMeta* tsd(const value::TypeMeta* key_type, const TSMeta* value_ts);

    // TSL[TS, Size] - time-series list
    const TSMeta* tsl(const TSMeta* element_ts, size_t fixed_size = 0);

    // TSW[T, period, min_period] - tick-based window
    const TSMeta* tsw(const value::TypeMeta* value_type,
                      size_t period, size_t min_period = 0);

    // TSW[T, time_range, min_time_range] - duration-based window
    const TSMeta* tsw_duration(const value::TypeMeta* value_type,
                               engine_time_delta_t time_range,
                               engine_time_delta_t min_time_range = engine_time_delta_t{0});

    // TSB[Schema] - time-series bundle
    const TSMeta* tsb(const std::vector<std::pair<std::string, const TSMeta*>>& fields,
                      const std::string& name,
                      nb::object python_type = nb::none());

    // REF[TS] - reference to time-series
    const TSMeta* ref(const TSMeta* referenced_ts);

    // SIGNAL - singleton
    const TSMeta* signal();

private:
    TSTypeRegistry() = default;
    ~TSTypeRegistry() = default;
    TSTypeRegistry(const TSTypeRegistry&) = delete;
    TSTypeRegistry& operator=(const TSTypeRegistry&) = delete;

    // Storage for TSMeta instances (owns all created schemas)
    std::vector<std::unique_ptr<TSMeta>> schemas_;

    // Caching maps for deduplication
    std::unordered_map<const value::TypeMeta*, const TSMeta*> ts_cache_;
    std::unordered_map<const value::TypeMeta*, const TSMeta*> tss_cache_;

    // TSD cache key: (key_type, value_ts)
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

    // TSL cache key: (element_ts, fixed_size)
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

    // TSW cache key: (value_type, is_duration, period/time_range, min_period/min_time_range)
    struct TSWKey {
        const value::TypeMeta* value_type;
        bool is_duration;
        int64_t range;      // period or time_range.count()
        int64_t min_range;  // min_period or min_time_range.count()
        bool operator==(const TSWKey& other) const;
    };
    struct TSWKeyHash { size_t operator()(const TSWKey& k) const; };
    std::unordered_map<TSWKey, const TSMeta*, TSWKeyHash> tsw_cache_;

    // TSB cache by name (schemas with same name should be same structure)
    std::unordered_map<std::string, const TSMeta*> tsb_cache_;

    // REF cache
    std::unordered_map<const TSMeta*, const TSMeta*> ref_cache_;

    // SIGNAL singleton
    const TSMeta* signal_singleton_ = nullptr;

    // Field string storage (owns field name strings)
    std::vector<std::unique_ptr<char[]>> field_names_;

    // TSBFieldInfo array storage
    std::vector<std::unique_ptr<TSBFieldInfo[]>> field_arrays_;

    // Helper to allocate and store a string
    const char* intern_string(const std::string& s);

    // Helper to create a TSMeta and store it
    TSMeta* create_schema();
};

} // namespace hgraph
```

## Implementation Details

### Singleton Pattern

```cpp
TSTypeRegistry& TSTypeRegistry::instance() {
    static TSTypeRegistry instance;
    return instance;
}
```

### ts() Implementation

```cpp
const TSMeta* TSTypeRegistry::ts(const value::TypeMeta* value_type) {
    auto it = ts_cache_.find(value_type);
    if (it != ts_cache_.end()) {
        return it->second;
    }

    auto* meta = create_schema();
    meta->kind = TSKind::TSValue;
    meta->value_type = value_type;

    ts_cache_[value_type] = meta;
    return meta;
}
```

### tsb() Implementation (Most Complex)

```cpp
const TSMeta* TSTypeRegistry::tsb(
    const std::vector<std::pair<std::string, const TSMeta*>>& fields,
    const std::string& name,
    nb::object python_type)
{
    // Check cache by name
    auto it = tsb_cache_.find(name);
    if (it != tsb_cache_.end()) {
        return it->second;
    }

    // Allocate field array
    auto field_array = std::make_unique<TSBFieldInfo[]>(fields.size());
    for (size_t i = 0; i < fields.size(); ++i) {
        field_array[i].name = intern_string(fields[i].first);
        field_array[i].index = i;
        field_array[i].ts_type = fields[i].second;
    }

    auto* meta = create_schema();
    meta->kind = TSKind::TSB;
    meta->fields = field_array.get();
    meta->field_count = fields.size();
    meta->bundle_name = intern_string(name);
    meta->python_type = std::move(python_type);

    field_arrays_.push_back(std::move(field_array));
    tsb_cache_[name] = meta;
    return meta;
}
```

### signal() Implementation

```cpp
const TSMeta* TSTypeRegistry::signal() {
    if (signal_singleton_) {
        return signal_singleton_;
    }

    auto* meta = create_schema();
    meta->kind = TSKind::SIGNAL;
    signal_singleton_ = meta;
    return meta;
}
```

## CMake Integration

Add to `cpp/src/cpp/CMakeLists.txt` in the HGRAPH_SOURCES list:

```cmake
set(HGRAPH_SOURCES
        # ... existing sources ...

        # TSTypeRegistry (add after types/value sources)
        types/time_series/ts_type_registry.cpp

        # Python bindings for TSTypeRegistry (add after api/python/py_value.cpp)
        api/python/py_ts_type_registry.cpp

        # ... rest of sources ...
)
```

## Thread Safety Documentation

Add to header:
```cpp
// Thread Safety: TSTypeRegistry is initialized as a function-local static,
// which is thread-safe per C++11. All factory methods are thread-safe for
// concurrent reads. Writes (new schema creation) are NOT thread-safe but
// are expected to occur during wiring (single-threaded phase).
```

## TSB Caching Strategy

TSB schemas are cached by **structural identity** (not just name):
- Cache key: sorted list of (field_name, TSMeta*) pairs
- This matches TypeRegistry pattern for compound types
- Same-named schemas with different fields are distinct

```cpp
struct TSBKey {
    std::vector<std::pair<std::string, const TSMeta*>> fields;
    std::string name;
    bool operator==(const TSBKey& other) const;
};
struct TSBKeyHash { size_t operator()(const TSBKey& k) const; };
std::unordered_map<TSBKey, const TSMeta*, TSBKeyHash> tsb_cache_;
```

## Testing Approach

1. **Basic Creation**: Each factory method creates valid TSMeta
2. **Deduplication**: Same inputs return same pointer
3. **Field Access**: TSB fields accessible by index
4. **SIGNAL Singleton**: Always returns same pointer
