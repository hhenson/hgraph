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
    // TODO: Define fields

    // TypeMeta* meta_;    // Schema describing the value
    // void* data_;        // Owned memory

public:
    // Construction
    // Value(TypeMeta* meta);                    // Default construct
    // Value(TypeMeta* meta, const void* src);   // Copy construct

    // Access
    // void* data();
    // const void* data() const;
    // TypeMeta* meta() const;

    // View creation
    // View view();
    // View view() const;
};
```

### Memory Management

**Design Principle**: Value semantics for users, RAII internally.

**Allocation**:
- Schema (TypeMeta) provides `size()` and `alignment()` for the value's memory requirements
- Value allocates a contiguous memory block of the required size on construction
- Memory is allocated once at construction, not resized

**Initialization**:
- Vtable method `type_ops::default_construct(data_, meta_)` initializes the allocated memory
- For compound types, this recursively initializes all child values
- For containers (List, Dict, Set), this initializes the container structure (not elements)

**Destruction**:
- Vtable method `type_ops::destruct(data_, meta_)` cleans up the value
- For compound types, recursively destructs children
- For containers, destructs all elements then the container structure
- Value then deallocates the memory block

```cpp
class Value {
    TypeMeta* meta_;
    void* data_;

public:
    explicit Value(TypeMeta* meta) : meta_(meta) {
        data_ = allocate(meta_->size(), meta_->alignment());
        meta_->ops()->default_construct(data_, meta_);
    }

    Value(TypeMeta* meta, const void* src) : meta_(meta) {
        data_ = allocate(meta_->size(), meta_->alignment());
        meta_->ops()->copy_construct(data_, src, meta_);
    }

    ~Value() {
        meta_->ops()->destruct(data_, meta_);
        deallocate(data_);
    }

    // Move semantics transfer ownership
    Value(Value&& other) noexcept
        : meta_(other.meta_), data_(other.data_) {
        other.data_ = nullptr;
    }

    // Copy uses vtable
    Value(const Value& other) : Value(other.meta_, other.data_) {}
};
```

**Lightweight SBO (Small Buffer Optimization)**:

For primitive types that fit within a pointer (8 bytes on 64-bit), store the value directly in the `data_` field itself - no heap allocation needed.

| Type | Storage | Notes |
|------|---------|-------|
| bool | inline | 1 byte, stored in data_ |
| int8_t, uint8_t | inline | 1 byte |
| int16_t, uint16_t | inline | 2 bytes |
| int32_t, uint32_t | inline | 4 bytes |
| int64_t, uint64_t | inline | 8 bytes |
| float | inline | 4 bytes |
| double | inline | 8 bytes |
| nb::object | inline | Python object (pointer-sized) |
| Everything else | heap | Compounds, containers, strings, etc. |

```cpp
class Value {
    TypeMeta* meta_;
    union {
        void* heap_data_;      // For heap-allocated values
        uint64_t inline_data_; // For primitives (reinterpret as needed)
    };

    bool is_inline() const {
        return meta_->size() <= sizeof(void*) && meta_->is_primitive();
    }

public:
    void* data() {
        return is_inline() ? reinterpret_cast<void*>(&inline_data_) : heap_data_;
    }

    explicit Value(TypeMeta* meta) : meta_(meta) {
        if (is_inline()) {
            inline_data_ = 0;  // Zero-initialize
            meta_->ops()->default_construct(&inline_data_, meta_);
        } else {
            heap_data_ = allocate(meta_->size(), meta_->alignment());
            meta_->ops()->default_construct(heap_data_, meta_);
        }
    }

    ~Value() {
        if (is_inline()) {
            meta_->ops()->destruct(&inline_data_, meta_);
        } else {
            meta_->ops()->destruct(heap_data_, meta_);
            deallocate(heap_data_);
        }
    }
};
```

This keeps allocation overhead zero for the most common scalar types while maintaining uniform access through `data()`.

## View

### Purpose
Type-erased non-owning reference to data. Provides read/write access to a value and tracks its position within the owning Value's structure.

### Structure

```cpp
class View {
    void* data_;              // Borrowed pointer to data
    const TypeMeta* schema_;  // Type schema
    Value* owner_;            // Owning Value (for lifetime tracking)
    Path path_;               // Path from owner root to this position

public:
    // Schema access
    const TypeMeta& schema() const;

    // Owner and path access
    Value& owner();
    const Path& path() const;

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

Every View maintains a reference to its **owning Value** and the **path** traversed to reach it:

```cpp
Value point(point_schema);
point.at("x").set<double>(1.0);

// Get a nested view
View x_view = point.view().at("x");

// Access owner and path
Value& owner = x_view.owner();        // Reference to 'point'
const Path& path = x_view.path();     // Path: ["x"]
std::string path_str = path.to_string();  // "x"

// Deeper nesting
Value nested(nested_schema);
View deep = nested.view().at("a").at(0).at("b");
// deep.path() → ["a", 0, "b"]
// deep.owner() → reference to 'nested'
```

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
2. `from_python(None)` maps to top-level typed-null (`reset()`) where validation policy allows.
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
