# Value and View Design

## Overview

The Value/View system provides type-erased access to data:
- **Value**: Owns memory, manages lifecycle
- **View**: Borrows memory, provides access

## Value

### Purpose
Type-erased owning container for any value conforming to a TypeMeta.

### Structure

```cpp
class Value {
    ValueStorage _storage;          // Owns payload memory (SBO + heap fallback)
    const TypeMeta* _schema{nullptr}; // Schema identity (independent of payload presence)

public:
    Value() = default;                      // invalid (no schema)
    explicit Value(const TypeMeta* schema); // typed-null by default

    // Explicit copy model (copy ctor/assign disabled)
    static Value copy(const Value& other);

    // Top-level nullability
    bool has_value() const;
    void reset();   // typed-null, schema preserved
    void emplace(); // construct payload for current schema

    // Access
    const TypeMeta* schema() const;
    void* data();             // throws std::bad_optional_access when top-level null
    const void* data() const; // throws std::bad_optional_access when top-level null
};
```

### Memory Management

**Design Principle**: value semantics for users, RAII internally, explicit null state.

1. Payload memory is owned by `ValueStorage`.
2. `ValueStorage` uses SBO (`24` bytes, align `8`) and heap fallback.
3. Type lifecycle is delegated through `type_ops` (`construct/destroy/copy/move`).
4. Top-level nullability is represented by payload presence (`_storage.has_value()`), not by altering schema identity.
5. Nested nullability is represented by per-node validity masks:
   - Bundle/Tuple: per-field bitmap
   - List: per-element bitmap
   - Map values: per-slot bitmap

This design keeps the in-process layout compact and efficient while preserving Arrow-compatible validity semantics.

## View

### Purpose
Type-erased non-owning reference to data. Provides read/write access to a value and tracks its position within the owning Value's structure.

### Structure

```cpp
class View {
    void* data_;              // Borrowed pointer to data
    const TypeMeta* schema_;  // Type schema

public:
    // Schema access
    const TypeMeta& schema() const;

    // Size (for composites)
    size_t size() const;

    // Type-erased access
    template<typename T> T as() const;          // Scalar extraction
    template<typename T> T* data();             // Direct pointer access
    View at(size_t index);                      // List/tuple element access
    View at(std::string_view name);             // Bundle field access

    // Operations (forwarded through schema's ops)
    bool equals(const View& other) const;
    size_t hash() const;
    std::string to_string() const;
    nb::object to_python() const;
};
```

### View Owner and Path

Owner/path metadata exists for some internal traversal utilities, but it is **not** currently a locked user-facing contract for the Value/View surface. The stable public contract is data/schema access through `View` and kind-specific wrappers.

### Kind-Specific Wrappers

Views have kind-specific wrappers for convenient access:

| Kind | Wrapper | Additional Interface |
|------|---------|---------------------|
| Atomic | View | as<T>(), set<T>() |
| Bundle | BundleView | at(name), at(index), field_name(index), items() |
| List | ListView | at(index), append(), clear(), values(), items() |
| Map | MapView | at(key), contains(key), set_item(key, val), keys(), items() |
| Set | SetView | contains(elem), add(elem), remove(elem), values() |

### Example: BundleView

```cpp
class BundleView : public View {
public:
    // Field access by name
    View at(std::string_view name);

    // Field access by index
    View at(size_t index);

    // Field count and name
    size_t field_count() const;
    std::string_view field_name(size_t index) const;

    // Iteration (ViewPairRange: field_name -> value)
    ViewPairRange items() const;
};
```

## ViewData

### Purpose
Compact representation for paths through type-erased data.

### Structure

```cpp
struct ViewData {
    ShortPath path;     // Node* + port_type + indices
    void* data;         // Pointer to data
    ts_ops* ops;        // Operations vtable
};
```

### Relationship to Link and TSView

```
Link = ViewData (no current_time)
TSView = ViewData + engine_time_t current_time
```

## Type Casting

**Note**: Type casting is a **TSOutput-only** concern, not a Value-layer operation. Values are stored with their native schema; casting happens when an output provides alternative representations to consumers expecting different schemas.

See [TSOutput and TSInput Design](05_TSOUTPUT_TSINPUT.md) for cast storage and management details.

## Design Decisions

### Nullability and Layout Decisions (Implemented)

This design now follows a two-level nullability model:

1. **Top-level Value nullability** (typed null):
   - `Value(schema)` starts null by default (schema present, payload absent).
   - `has_value()`, `reset()`, `emplace()` control top-level presence.
   - `view()` / `data()` throw `std::bad_optional_access` when top-level null.
2. **Nested nullability** for composite/container children:
   - Bundle/Tuple: per-field validity bitmap
   - List (fixed/dynamic): per-element validity bitmap
   - Map: per-value-slot validity bitmap
   - Set elements: non-null by design

`None` is therefore a **state**, not a schema/type.

#### Top-Level Value Storage

- Schema and payload presence are tracked independently:
  - schema pointer: always available once constructed from schema
  - payload presence: `ValueStorage.has_value()`
- Payload uses SBO + heap fallback (implementation detail):
  - small payloads inline (`SBO_BUFFER_SIZE = 24`, `SBO_ALIGNMENT = 8`)
  - larger payloads heap-allocated with schema alignment

#### Nested Validity Layout

For fixed-size composite layouts, validity bits are stored as a compact tail region:

- Bundle/Tuple: `[field payload region][validity bits][padding]`
- Fixed List: `[element payload region][validity bits][padding]`

For dynamic containers, validity is stored in parallel dynamic buffers:

- Dynamic List: `data[]` + `validity[]`
- Map values: `values[]` + `value_validity[]` (parallel to key slots)

All validity masks are 1 bit per logical child/slot, with `1 = valid`, `0 = null`.

#### Nullability Rules Locked In

1. `Value(schema)` is typed-null by default.
2. Map keys are non-null.
3. Map values are nullable.
4. Set elements are non-null.
5. No null schema/type exists; null is represented via validity state only.

#### Python Interop

1. Top-level null Value converts to Python `None`.
2. `from_python(None)` maps to top-level typed-null (`reset()`).
3. Nested `None` in bundle/tuple/list/map values maps to nested validity bits.

#### Arrow Compatibility Strategy

The implementation prioritizes compact internal layout and efficient in-house algorithms, while keeping Arrow-compatible validity semantics:

1. Validity bit operations use `nanoarrow` bitmap primitives.
2. Validity/data are represented as independent logical buffers.
3. Full zero-copy is practical for some structures and not guaranteed for all (notably map/set may still require compaction when exported).

This is an explicit compatibility target, not a requirement that internal storage be Arrow-native.

### Const Correctness
The View system uses normal C++ constness and guarded mutation methods. There is no separate public `const_view()` API or parallel `Const*View` family in the surface design; read-only behavior comes from const-qualified access.

### Error Handling
Invalid operations raise exceptions. We use the appropriate C++ exception where available (e.g., `std::out_of_range`, `std::invalid_argument`), otherwise we use `std::runtime_error`.

## References

- User Guide: `02_VALUE.md`
- Research: `11_VALUE_VIEW_REIMPLEMENTATION.md`
