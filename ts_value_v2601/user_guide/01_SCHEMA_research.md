# Schema Research: type_ops Design Analysis

**Related**: [Schema](01_SCHEMA.md)

---

## Problem Statement

The `type_ops` structure provides the operations vtable for type-erased values. Different type kinds (atomic, bundle, list, set, map) require different operations. The design challenge is how to represent this efficiently in terms of:

- Memory overhead per type
- Call dispatch performance (indirection levels)
- Extensibility (adding new kinds or operations)
- Implementation complexity

---

## Design Options Analyzed

### Option 1: Flat Table (All Functions in One Struct)

```cpp
struct type_ops {
    // Common operations (all types)
    void (*construct)(void* dst);
    void (*destroy)(void* ptr);
    void (*copy)(void* dst, const void* src);
    void (*move)(void* dst, void* src);
    bool (*equals)(const void* a, const void* b);
    size_t (*hash)(const void* ptr);
    std::string (*to_string)(const void* ptr);
    nb::object (*to_python)(const void* ptr);
    void (*from_python)(void* ptr, nb::object obj);

    // Bundle-specific (nullptr for non-bundles)
    View (*field_at_index)(void* ptr, size_t idx);
    View (*field_at_name)(void* ptr, std::string_view name);

    // List-specific (nullptr for non-lists)
    View (*list_at)(void* ptr, size_t idx);
    void (*list_append)(void* ptr, const void* elem);

    // Set-specific (nullptr for non-sets)
    bool (*set_contains)(const void* ptr, const void* elem);
    void (*set_add)(void* ptr, const void* elem);
    bool (*set_remove)(void* ptr, const void* elem);

    // Map-specific (nullptr for non-maps)
    View (*map_at)(void* ptr, const void* key);
    void (*map_set)(void* ptr, const void* key, const void* val);
    // ... etc
};
```

**Pros:**
- Simple, single indirection to call any operation
- Cache-friendly when operations are called in sequence
- Easy to understand and debug
- No heap allocation for the ops table itself

**Cons:**
- Wastes memory: every type carries slots for all operations (many nullptr)
- Table size grows with every new operation added
- Poor extensibility: adding a new kind requires modifying the struct

---

### Option 2: Base + Trait Extensions

```cpp
struct base_ops {
    // Universal operations
    void (*construct)(void* dst);
    void (*destroy)(void* ptr);
    void (*copy)(void* dst, const void* src);
    void (*move)(void* dst, void* src);
    bool (*equals)(const void* a, const void* b);
    size_t (*hash)(const void* ptr);
    std::string (*to_string)(const void* ptr);
    nb::object (*to_python)(const void* ptr);
    void (*from_python)(void* ptr, nb::object obj);

    // Extension point
    std::unique_ptr<trait_ops> traits;
};

struct bundle_trait_ops : trait_ops {
    View (*field_at_index)(void* ptr, size_t idx);
    View (*field_at_name)(void* ptr, std::string_view name);
    size_t (*field_count)(const void* ptr);
};

struct list_trait_ops : trait_ops {
    View (*at)(void* ptr, size_t idx);
    void (*append)(void* ptr, const void* elem);
    size_t (*size)(const void* ptr);
};
// ... etc
```

**Pros:**
- Smaller base table size
- Only allocates trait-specific ops when needed
- Better separation of concerns
- More extensible: new traits don't affect existing types

**Cons:**
- Extra indirection (base -> traits -> function)
- Heap allocation for trait_ops
- More complex to set up and reason about
- Runtime type check needed before casting trait_ops

---

### Option 3: Indirect Shared VTable (Classic C++ Style)

```cpp
// One static instance per concrete type
struct type_ops {
    // All operations inline
    void (*construct)(void* dst);
    // ... all functions
};

// TypeMeta stores only a pointer
class TypeMeta {
    const type_ops* ops_;  // Points to static instance
};
```

**Pros:**
- Minimal per-TypeMeta overhead (single pointer)
- VTable is shared across all instances of same type
- Same pattern as C++ virtual functions
- Used by libunifex as "Indirect VTable"

**Cons:**
- Extra indirection on every call
- Still has the "wasted slots" problem if using flat table
- Static initialization order issues possible

---

### Option 4: Inline VTable (No Indirection)

```cpp
class TypeMeta {
    // Ops stored directly in TypeMeta
    void (*construct_)(void* dst);
    void (*destroy_)(void* ptr);
    // ... all function pointers inline
};
```

**Pros:**
- No indirection - fastest call dispatch
- The woid library shows this can be 3x faster than virtual functions
- Good cache locality when calling multiple ops

**Cons:**
- Larger TypeMeta size
- Every TypeMeta copy duplicates all function pointers
- Not practical if TypeMeta is frequently copied

---

### Option 5: Hybrid - Tag + Function Table Union (Selected)

```cpp
enum class TypeKind : uint8_t { Atomic, Bundle, List, Set, Map };

// Separate tables per kind
struct atomic_ops { /* atomic-specific */ };
struct bundle_ops { /* bundle-specific */ };
struct list_ops { /* list-specific */ };
// ...

struct type_ops {
    // Common ops (always present)
    void (*construct)(void* dst);
    void (*destroy)(void* ptr);
    void (*copy)(void* dst, const void* src);
    bool (*equals)(const void* a, const void* b);
    size_t (*hash)(const void* ptr);
    nb::object (*to_python)(const void* ptr);
    void (*from_python)(void* ptr, nb::object obj);

    // Kind-specific ops via union (no heap allocation)
    TypeKind kind;
    union {
        atomic_ops atomic;
        bundle_ops bundle;
        list_ops list;
        set_ops set;
        map_ops map;
    } specific;
};
```

**Pros:**
- No wasted space (union reuses memory)
- No heap allocation
- Single indirection
- Type-safe: kind tag indicates which union member is active
- Extensible: add new kinds without growing existing tables

**Cons:**
- Union size is max of all specific_ops
- Requires kind check before accessing specific ops
- Slightly more complex access pattern

---

### Option 6: Sean Parent Style (Concept/Model)

```cpp
struct type_concept {
    virtual ~type_concept() = default;
    virtual void copy_to(void* dst) const = 0;
    virtual bool equals(const type_concept& other) const = 0;
    virtual nb::object to_python() const = 0;
    // ...
};

template<typename T>
struct type_model : type_concept {
    T data_;
    void copy_to(void* dst) const override { new(dst) T(data_); }
    bool equals(const type_concept& other) const override { /* ... */ }
    // ...
};
```

**Pros:**
- Elegant, well-documented pattern
- Natural C++ inheritance for dispatch
- Easy to add Small Buffer Optimization

**Cons:**
- One virtual call per operation
- Requires storing the model object (not just function pointers)
- Less control over memory layout

---

## Comparison Summary

| Option | Memory | Indirection | Extensibility | Complexity |
|--------|--------|-------------|---------------|------------|
| 1. Flat Table | Wasteful | Single | Poor | Low |
| 2. Base + unique_ptr Traits | Moderate | Double | Good | Medium |
| 3. Indirect Shared VTable | Minimal | Double | Poor | Low |
| 4. Inline VTable | Large | None | Poor | Low |
| 5. Tag + Union | Optimal | Single | Good | Medium |
| 6. Sean Parent | Moderate | Virtual | Medium | Medium |

---

## Decision: Option 5 (Tag + Union)

Option 5 was selected for this project because it provides:

1. **Optimal memory usage**: Union reuses memory across kind-specific ops
2. **Single indirection for common ops**: No performance penalty for frequent operations
3. **Good extensibility**: New kinds can be added without affecting existing types
4. **No heap allocation**: All ops stored inline or in static tables
5. **Type safety**: Kind tag ensures correct union member access

---

## References

- libunifex Type Erasure Documentation - VTable Storage Strategies
- woid Library - High-performance Type Erasure (benchmarks showing 3x improvement over virtual functions)
- Boost.TypeErasure - Runtime polymorphism patterns
- boost-ext/te - C++17 Type Erasure library
- Sean Parent - "Inheritance Is The Base Class of Evil" (GoingNative 2013)
- Klaus Iglberger - "Breaking Dependencies: Type Erasure" (CppCon 2021)
- Arthur O'Dwyer - "What is Type Erasure?"
- Andrzej Krzemienski - "Common Optimizations" (Small Buffer Optimization)
