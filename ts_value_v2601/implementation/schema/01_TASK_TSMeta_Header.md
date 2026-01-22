# Task 01: TSMeta Header Design

## Objective

Create the TSMeta schema structures that describe time-series type metadata.

## File to Create

`cpp/include/hgraph/types/time_series/ts_meta.h`

## Namespace

All types in `namespace hgraph` (consistent with TypeMeta in value namespace).

## Thread Safety

```cpp
// Thread Safety: TSMeta structures are immutable after creation.
// TSTypeRegistry handles thread-safe creation and caching.
```

## Design

### Forward Declaration

```cpp
namespace hgraph {

// Forward declarations for self-referential types
struct TSMeta;
struct TSBFieldInfo;

} // namespace hgraph
```

### TSKind Enum

```cpp
enum class TSKind : uint8_t {
    TSValue,     // TS[T] - scalar time-series
    TSS,         // TSS[T] - time-series set
    TSD,         // TSD[K, V] - time-series dict
    TSL,         // TSL[TS, Size] - time-series list
    TSW,         // TSW[T, size, min_size] - time-series window
    TSB,         // TSB[Schema] - time-series bundle
    REF,         // REF[TS] - reference to time-series
    SIGNAL       // SIGNAL - presence/absence marker
};
```

### TSBFieldInfo Structure

```cpp
struct TSBFieldInfo {
    const char* name;      // Field name (owned by registry)
    size_t index;          // 0-based field index
    const TSMeta* ts_type; // Field's time-series schema
};
```

### TSMeta Structure

TSMeta uses a tagged union approach - the `kind` field determines which members are valid:

```cpp
struct TSMeta {
    TSKind kind;

    // Value type - valid for: TSValue, TSS, TSW
    const TypeMeta* value_type = nullptr;

    // Key type - valid for: TSD
    const TypeMeta* key_type = nullptr;

    // Element TS - valid for: TSD (value), TSL (element), REF (referenced)
    const TSMeta* element_ts = nullptr;

    // Fixed size - valid for: TSL (0 = dynamic SIZE)
    size_t fixed_size = 0;

    // Window parameters - valid for: TSW
    bool is_duration_based = false;  // true = time-based, false = tick-based
    union {
        struct {
            size_t period;
            size_t min_period;
        } tick;
        struct {
            engine_time_delta_t time_range;
            engine_time_delta_t min_time_range;
        } duration;
    } window = {};

    // Bundle fields - valid for: TSB
    const TSBFieldInfo* fields = nullptr;
    size_t field_count = 0;
    const char* bundle_name = nullptr;  // Schema class name
    nb::object python_type;             // For reconstruction (optional)

    // Helper methods
    bool is_collection() const {
        return kind == TSKind::TSS || kind == TSKind::TSD ||
               kind == TSKind::TSL || kind == TSKind::TSB;
    }

    bool is_scalar_ts() const {
        return kind == TSKind::TSValue || kind == TSKind::TSW ||
               kind == TSKind::SIGNAL;
    }
};
```

## Includes Required

```cpp
#include <hgraph/types/value/type_meta.h>  // TypeMeta
#include <hgraph/types/time.h>              // engine_time_delta_t
#include <nanobind/nanobind.h>              // nb::object
#include <cstdint>
#include <cstddef>
```

## Implementation Notes

1. **Pointer Stability**: All pointers in TSMeta (TypeMeta*, TSMeta*, char*) must be stable for the process lifetime. The TSTypeRegistry owns these.

2. **Python Type**: The `python_type` field is for TSB schemas to enable proper `to_python()` conversion. When set, conversion returns an instance of that class. When unset (None), returns a dict.

3. **Window Union**: Using a union for window parameters saves space since only one representation is used per TSW schema.

4. **Forward Declaration**: TSMeta is self-referential (element_ts points to TSMeta). Use forward declaration or put definition before struct body.

## Testing Approach

- Compile-time: Ensure header compiles standalone
- Unit tests in ts_type_registry tests will exercise construction
