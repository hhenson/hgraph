# Type Model Cleanup Proposal

**Date**: 2026-01-14
**Status**: Draft
**Related**:
- `Value_DESIGN.md`
- `TSValue_DESIGN.md`
- `Value_TSValue_MIGRATION_PLAN.md`

---

## 1. Problem Statement

The current type model has **inconsistent dispatch mechanisms** that lead to:
- 11+ switch statements on TypeKind/TSTypeKind scattered across the codebase
- Adding a new type requires updates in N places
- Incomplete type erasure (type knowledge leaks through switches)
- Different extension patterns for Value (policies) vs TSValue (inheritance)

### 1.1 Current Dispatch Mechanisms

| Layer | Mechanism | Extension Point |
|-------|-----------|-----------------|
| Value TypeOps | Function pointer vtable | Add functions to `TypeOps` struct |
| TSMeta | Virtual methods (inheritance) | Create new subclass |
| TSOverlayStorage | Virtual methods (inheritance) | Create new subclass |
| Visitor/Navigation | Switch statements | Add case to each switch |

### 1.2 Switch Statement Inventory

```
cpp/include/hgraph/types/value/visitor.h:90      - TypeKind (8 cases)
cpp/include/hgraph/types/value/visitor.h:119    - TypeKind (8 cases)
cpp/include/hgraph/types/value/traversal.h:134  - TypeKind (8 cases)
cpp/src/cpp/types/time_series/ts_overlay_storage.cpp:822 - TSTypeKind (8 cases)
cpp/src/cpp/types/time_series/ts_value.cpp:144  - TSTypeKind (2 cases, incomplete!)
cpp/src/cpp/types/time_series/ts_value.cpp:200  - TSTypeKind (2 cases, incomplete!)
cpp/src/cpp/types/time_series/ts_value.cpp:288  - TSTypeKind (2 cases, incomplete!)
cpp/src/cpp/types/time_series/ts_view.cpp:213   - TSTypeKind (4 cases, incomplete!)
cpp/src/cpp/api/python/wrapper_factory.cpp:295  - TSTypeKind (1/8 implemented!)
cpp/src/cpp/api/python/wrapper_factory.cpp:324  - TSTypeKind (1/8 implemented!)
```

The **incomplete switches** are the most problematic - they silently do the wrong thing for types not explicitly handled.

---

## 2. Design Goals

1. **Single point of extension**: Adding a new type should require updates in ONE place
2. **Complete type erasure**: Code should operate on types without knowing their concrete kind
3. **Consistent dispatch**: One mechanism for all type-based dispatch
4. **Zero-cost abstraction**: No overhead when features aren't used
5. **Compositional extension**: New capabilities (TS overlay, link support) added via composition, not inheritance

---

## 3. Proposed Solution: Type-Carried Operations

### 3.1 Core Principle

**Store operations with the schema, not in external switches.**

Instead of:
```cpp
// External switch dispatches based on kind
switch (schema->kind) {
    case TypeKind::List: do_list_thing(data, schema); break;
    case TypeKind::Map:  do_map_thing(data, schema); break;
    // ... must update for every new type
}
```

Use:
```cpp
// Type carries its own operations
schema->ops->do_thing(data, schema);  // Uniform dispatch
```

This is already done for `TypeOps` - we need to extend this pattern.

### 3.2 Extended TypeOps

Add navigation and child-access operations to `TypeOps`:

```cpp
struct TypeOps {
    // === Existing core operations ===
    void (*construct)(void* dst, const TypeMeta* schema);
    void (*destruct)(void* obj, const TypeMeta* schema);
    // ... existing ops ...

    // === NEW: Structural navigation (replaces switches in visitor.h) ===

    /// Get number of children (0 for scalars, field_count for bundles, etc.)
    size_t (*child_count)(const void* obj, const TypeMeta* schema);

    /// Get child schema by index (nullptr if no children or out of range)
    const TypeMeta* (*child_schema)(const TypeMeta* schema, size_t index);

    /// Get child data pointer by index (nullptr if no children)
    const void* (*child_data)(const void* obj, const TypeMeta* schema, size_t index);
    void* (*child_data_mut)(void* obj, const TypeMeta* schema, size_t index);

    /// Get child schema by name (for bundles, nullptr otherwise)
    const TypeMeta* (*child_schema_by_name)(const TypeMeta* schema, std::string_view name);

    /// Get child data pointer by name (for bundles)
    const void* (*child_data_by_name)(const void* obj, const TypeMeta* schema, std::string_view name);
    void* (*child_data_mut_by_name)(void* obj, const TypeMeta* schema, std::string_view name);

    // === NEW: Iteration support ===

    /// Create iterator state (opaque, type-specific)
    void* (*iter_begin)(const void* obj, const TypeMeta* schema);

    /// Advance iterator, returns false when done
    bool (*iter_next)(void* iter_state, const TypeMeta* schema);

    /// Get current element from iterator
    const void* (*iter_current)(void* iter_state, const TypeMeta* schema);

    /// Destroy iterator state
    void (*iter_end)(void* iter_state, const TypeMeta* schema);
};
```

### 3.3 TSMeta Operations Table

Instead of virtual methods + factory switches, add an operations table:

```cpp
struct TSTypeOps {
    // === Overlay factory (replaces make_ts_overlay switch) ===
    std::unique_ptr<TSOverlayStorage> (*make_overlay)(const TSMeta* ts_meta);

    // === Child navigation (replaces ts_view.cpp switches) ===
    size_t (*child_count)(const TSMeta* ts_meta);
    const TSMeta* (*child_meta)(const TSMeta* ts_meta, size_t index);
    const TSMeta* (*child_meta_by_name)(const TSMeta* ts_meta, std::string_view name);

    // === Link support queries (replaces ts_value.cpp switches) ===
    bool (*supports_child_links)(const TSMeta* ts_meta);
    size_t (*link_child_count)(const TSMeta* ts_meta);

    // === Python wrapping (replaces wrapper_factory.cpp switches) ===
    nb::object (*wrap_input_view)(const TSView& view, void* owner);
    nb::object (*wrap_output_view)(const TSMutableView& view, void* owner);
};

struct TSMeta {
    TSTypeKind kind;                    // Keep for debugging/logging only
    const value::TypeMeta* value_schema;
    const TSTypeOps* ops;               // <-- NEW: Operations vtable

    // Type-specific fields stored inline (not via inheritance)
    union {
        struct { /* TSValueMeta fields */ } scalar;
        struct { const TSBFieldInfo* fields; size_t field_count; } bundle;
        struct { const TSMeta* element_type; size_t fixed_size; } list;
        struct { const value::TypeMeta* key_type; const TSMeta* value_type; } dict;
        struct { const value::TypeMeta* element_type; } set;
        struct { const value::TypeMeta* value_type; size_t size; size_t min_size; } window;
        struct { const TSMeta* referenced_type; } ref;
    } data;
};
```

### 3.4 Benefits

**Before (adding new type TSQ - "time-series queue"):**
1. Add `TSQ` to `TSTypeKind` enum
2. Create `TSQTypeMeta : TSMeta` class with virtual method overrides
3. Create `QueueTSOverlay : TSOverlayStorage` class
4. Add `case TSTypeKind::TSQ:` to `make_ts_overlay()`
5. Add `case TSTypeKind::TSQ:` to `ts_value.cpp` enable_link_support
6. Add `case TSTypeKind::TSQ:` to `ts_value.cpp` create_link
7. Add `case TSTypeKind::TSQ:` to `ts_value.cpp` get_or_create_child_value
8. Add `case TSTypeKind::TSQ:` to `ts_view.cpp` navigate_path
9. Add `case TSTypeKind::TSQ:` to `wrapper_factory.cpp` wrap_input_view
10. Add `case TSTypeKind::TSQ:` to `wrapper_factory.cpp` wrap_output_view
11. Hope you didn't miss any switches

**After (with proposed design):**
1. Add `TSQ` to `TSTypeKind` enum (for debug logging only)
2. Create `QueueTSOverlay` class
3. Define `TSQ_TYPE_OPS` constant with all operations
4. Register TSQ schema with ops pointer
5. Done - all dispatch uses ops table automatically

---

## 4. Implementation Plan

### Phase 1: Extend TypeOps with Navigation

Add child navigation operations to `TypeOps`. This eliminates switches in `visitor.h` and `traversal.h`.

```cpp
// For scalars: all child ops return nullptr/0
// For bundles: child_count = field_count, child_schema = field types
// For lists: child_count = size, child_schema = element_type
// For maps: child_count = size, child_schema returns key_type for even indices, element_type for odd
// etc.
```

**Files to modify:**
- `cpp/include/hgraph/types/value/type_meta.h` - Add ops
- `cpp/src/cpp/types/value/` - Implement for each type
- `cpp/include/hgraph/types/value/visitor.h` - Use ops instead of switch
- `cpp/include/hgraph/types/value/traversal.h` - Use ops instead of switch

### Phase 2: Add TSTypeOps

Create the `TSTypeOps` structure and migrate TSMeta from inheritance to composition.

**Key changes:**
- TSMeta becomes a single struct with ops pointer + union for type-specific data
- Each TS type has a static `TSTypeOps` instance
- Remove virtual methods from TSMeta base

**Files to modify:**
- `cpp/include/hgraph/types/time_series/ts_type_meta.h` - New structure
- `cpp/src/cpp/types/time_series/ts_type_meta.cpp` - Implement ops for each type
- Remove inheritance hierarchy (TSValueMeta, TSBTypeMeta, etc. become factory functions)

### Phase 3: Migrate Overlay Factory

Move `make_ts_overlay()` switch into `TSTypeOps::make_overlay`.

**Files to modify:**
- `cpp/src/cpp/types/time_series/ts_overlay_storage.cpp` - Remove factory switch
- Each overlay type defines its factory function

### Phase 4: Migrate TSValue/TSView Switches

Replace remaining switches with ops dispatch.

**Files to modify:**
- `cpp/src/cpp/types/time_series/ts_value.cpp` - Use `ops->supports_child_links()` etc.
- `cpp/src/cpp/types/time_series/ts_view.cpp` - Use `ops->child_meta()` for navigation

### Phase 5: Migrate Python Wrapper Factory

Replace wrapper_factory switches with ops.

**Files to modify:**
- `cpp/src/cpp/api/python/wrapper_factory.cpp` - Use `ops->wrap_input_view()` etc.

---

## 5. Detailed Design: TSMeta Restructure

### 5.1 Current Structure (Inheritance-Based)

```cpp
struct TSMeta {
    virtual TSTypeKind kind() const = 0;
    virtual const TypeMeta* value_schema() const = 0;
    virtual std::string to_string() const = 0;
};

struct TSValueMeta : TSMeta { ... };
struct TSBTypeMeta : TSMeta { ... };
struct TSLTypeMeta : TSMeta { ... };
// ... 8 subclasses
```

### 5.2 Proposed Structure (Composition-Based)

```cpp
// Operations table - one per TS type kind
struct TSTypeOps {
    // Core operations
    std::string (*to_string)(const TSMeta* meta);

    // Overlay factory
    std::unique_ptr<TSOverlayStorage> (*make_overlay)(const TSMeta* meta);

    // Child navigation
    size_t (*child_count)(const TSMeta* meta);
    const TSMeta* (*child_meta)(const TSMeta* meta, size_t index);
    const TSMeta* (*child_meta_by_name)(const TSMeta* meta, std::string_view name);

    // Link support
    bool (*can_have_child_links)();  // true for TSB, TSL; false for others

    // Python wrapping
    nb::object (*wrap_view)(/* ... */);
};

// Single unified structure (no inheritance)
struct TSMeta {
    TSTypeKind kind;                     // For debugging/logging only
    const value::TypeMeta* value_schema; // Underlying data schema
    const TSTypeOps* ops;                // Operations vtable

    // Type-specific payload (tagged union avoids separate allocations)
    union Payload {
        // TS[T] - scalar time-series
        struct { } scalar;  // No extra data needed

        // TSB[fields] - bundle
        struct {
            const TSBFieldInfo* fields;
            size_t field_count;
            const char* name;  // Optional bundle name
        } bundle;

        // TSL[TS[T], Size] - list
        struct {
            const TSMeta* element_type;
            size_t fixed_size;  // 0 = dynamic
        } list;

        // TSD[K, TS[V]] - dict
        struct {
            const value::TypeMeta* key_type;
            const TSMeta* value_type;
        } dict;

        // TSS[T] - set
        struct {
            const value::TypeMeta* element_type;
        } set;

        // TSW[T, Size] - window
        struct {
            const value::TypeMeta* value_type;
            size_t size;
            size_t min_size;
            engine_time_delta_t time_range;
            engine_time_delta_t min_time_range;
            bool is_time_based;
        } window;

        // REF[TS[T]] - reference
        struct {
            const TSMeta* referenced_type;
        } ref;

        // SIGNAL - no payload
        struct { } signal;
    } payload;

    // Convenience accessors (inline, type-checked)
    [[nodiscard]] const TSBFieldInfo* bundle_fields() const {
        assert(kind == TSTypeKind::TSB);
        return payload.bundle.fields;
    }

    [[nodiscard]] size_t bundle_field_count() const {
        assert(kind == TSTypeKind::TSB);
        return payload.bundle.field_count;
    }

    [[nodiscard]] const TSMeta* list_element_type() const {
        assert(kind == TSTypeKind::TSL);
        return payload.list.element_type;
    }

    // ... etc for each type
};
```

### 5.3 Static Operations Tables

Define one ops table per type kind:

```cpp
// In ts_type_ops.cpp

namespace {

// === Scalar TS operations ===
std::string ts_scalar_to_string(const TSMeta* meta) {
    return "TS[" + meta->value_schema->ops->to_string(nullptr, meta->value_schema) + "]";
}

std::unique_ptr<TSOverlayStorage> ts_scalar_make_overlay(const TSMeta*) {
    return std::make_unique<ScalarTSOverlay>();
}

size_t ts_scalar_child_count(const TSMeta*) { return 0; }
const TSMeta* ts_scalar_child_meta(const TSMeta*, size_t) { return nullptr; }
const TSMeta* ts_scalar_child_meta_by_name(const TSMeta*, std::string_view) { return nullptr; }
bool ts_scalar_can_have_child_links() { return false; }

constexpr TSTypeOps TS_SCALAR_OPS = {
    .to_string = ts_scalar_to_string,
    .make_overlay = ts_scalar_make_overlay,
    .child_count = ts_scalar_child_count,
    .child_meta = ts_scalar_child_meta,
    .child_meta_by_name = ts_scalar_child_meta_by_name,
    .can_have_child_links = ts_scalar_can_have_child_links,
    .wrap_view = nullptr,  // TODO: implement
};

// === Bundle TSB operations ===
std::string tsb_to_string(const TSMeta* meta) {
    std::string result = "TSB[";
    for (size_t i = 0; i < meta->payload.bundle.field_count; ++i) {
        if (i > 0) result += ", ";
        result += meta->payload.bundle.fields[i].name;
        result += ": ";
        result += meta->payload.bundle.fields[i].type->ops->to_string(meta->payload.bundle.fields[i].type);
    }
    return result + "]";
}

std::unique_ptr<TSOverlayStorage> tsb_make_overlay(const TSMeta* meta) {
    return std::make_unique<CompositeTSOverlay>(meta);
}

size_t tsb_child_count(const TSMeta* meta) {
    return meta->payload.bundle.field_count;
}

const TSMeta* tsb_child_meta(const TSMeta* meta, size_t index) {
    if (index >= meta->payload.bundle.field_count) return nullptr;
    return meta->payload.bundle.fields[index].type;
}

const TSMeta* tsb_child_meta_by_name(const TSMeta* meta, std::string_view name) {
    for (size_t i = 0; i < meta->payload.bundle.field_count; ++i) {
        if (name == meta->payload.bundle.fields[i].name) {
            return meta->payload.bundle.fields[i].type;
        }
    }
    return nullptr;
}

bool tsb_can_have_child_links() { return true; }

constexpr TSTypeOps TSB_OPS = {
    .to_string = tsb_to_string,
    .make_overlay = tsb_make_overlay,
    .child_count = tsb_child_count,
    .child_meta = tsb_child_meta,
    .child_meta_by_name = tsb_child_meta_by_name,
    .can_have_child_links = tsb_can_have_child_links,
    .wrap_view = nullptr,
};

// ... similar for TSL, TSD, TSS, TSW, REF, SIGNAL

} // anonymous namespace

// Public function to get ops for a kind (used during TSMeta construction)
const TSTypeOps* get_ts_type_ops(TSTypeKind kind) {
    switch (kind) {  // This is the ONE switch for ops lookup
        case TSTypeKind::TS:     return &TS_SCALAR_OPS;
        case TSTypeKind::TSB:    return &TSB_OPS;
        case TSTypeKind::TSL:    return &TSL_OPS;
        case TSTypeKind::TSD:    return &TSD_OPS;
        case TSTypeKind::TSS:    return &TSS_OPS;
        case TSTypeKind::TSW:    return &TSW_OPS;
        case TSTypeKind::REF:    return &REF_OPS;
        case TSTypeKind::SIGNAL: return &SIGNAL_OPS;
    }
    return nullptr;
}
```

### 5.4 Factory Functions Replace Subclass Constructors

```cpp
// Instead of: new TSBTypeMeta(fields, value_schema, name)
// Use: make_tsb_meta(fields, value_schema, name)

TSMeta* make_ts_meta(const value::TypeMeta* scalar_schema) {
    auto* meta = new TSMeta{};
    meta->kind = TSTypeKind::TS;
    meta->value_schema = scalar_schema;
    meta->ops = get_ts_type_ops(TSTypeKind::TS);
    return meta;
}

TSMeta* make_tsb_meta(std::span<TSBFieldInfo> fields,
                       const value::TypeMeta* bundle_schema,
                       std::string_view name = "") {
    auto* meta = new TSMeta{};
    meta->kind = TSTypeKind::TSB;
    meta->value_schema = bundle_schema;
    meta->ops = get_ts_type_ops(TSTypeKind::TSB);

    // Copy fields to owned storage (or reference if externally managed)
    auto* owned_fields = new TSBFieldInfo[fields.size()];
    std::copy(fields.begin(), fields.end(), owned_fields);
    meta->payload.bundle.fields = owned_fields;
    meta->payload.bundle.field_count = fields.size();
    meta->payload.bundle.name = /* copy name */;

    return meta;
}

// ... etc for each type
```

---

## 6. Usage Examples

### 6.1 Before: Switch-Based Navigation

```cpp
// ts_view.cpp - current implementation
TSView TSView::navigate_path(std::span<PathElement> path) {
    const void* current_data = _data;
    const TSMeta* current_meta = _ts_meta;
    TSOverlayStorage* current_overlay = _overlay;

    for (const auto& elem : path) {
        switch (current_meta->kind()) {  // <-- SWITCH!
            case TSTypeKind::TSB: {
                auto* bundle_meta = static_cast<const TSBTypeMeta*>(current_meta);
                // ... navigate bundle
                break;
            }
            case TSTypeKind::TSL: {
                auto* list_meta = static_cast<const TSLTypeMeta*>(current_meta);
                // ... navigate list
                break;
            }
            // ... 4 more cases, some types not handled!
        }
    }
    return TSView(current_data, current_meta, current_overlay);
}
```

### 6.2 After: Ops-Based Navigation

```cpp
// ts_view.cpp - proposed implementation
TSView TSView::navigate_path(std::span<PathElement> path) {
    const void* current_data = _data;
    const TSMeta* current_meta = _ts_meta;
    TSOverlayStorage* current_overlay = _overlay;

    for (const auto& elem : path) {
        // Uniform dispatch through ops - no switch!
        if (elem.is_index()) {
            current_meta = current_meta->ops->child_meta(current_meta, elem.index());
            current_data = current_meta->value_schema->ops->child_data(
                current_data, current_meta->value_schema, elem.index());
            // Navigate overlay too...
        } else {
            current_meta = current_meta->ops->child_meta_by_name(current_meta, elem.name());
            current_data = current_meta->value_schema->ops->child_data_by_name(
                current_data, current_meta->value_schema, elem.name());
        }

        if (!current_meta || !current_data) {
            throw std::runtime_error("Invalid path");
        }
    }
    return TSView(current_data, current_meta, current_overlay);
}
```

### 6.3 Before: Factory Switch

```cpp
// ts_overlay_storage.cpp - current implementation
std::unique_ptr<TSOverlayStorage> make_ts_overlay(const TSMeta* ts_meta) {
    switch (ts_meta->kind()) {  // <-- SWITCH!
        case TSTypeKind::TS:     return std::make_unique<ScalarTSOverlay>();
        case TSTypeKind::TSB:    return std::make_unique<CompositeTSOverlay>(ts_meta);
        case TSTypeKind::TSL:    return std::make_unique<ListTSOverlay>(ts_meta);
        case TSTypeKind::TSS:    return std::make_unique<SetTSOverlay>(ts_meta);
        case TSTypeKind::TSD:    return std::make_unique<MapTSOverlay>(ts_meta);
        case TSTypeKind::TSW:    return std::make_unique<ListTSOverlay>(ts_meta);
        case TSTypeKind::REF:    return std::make_unique<ScalarTSOverlay>();
        case TSTypeKind::SIGNAL: return std::make_unique<ScalarTSOverlay>();
    }
    return nullptr;
}
```

### 6.4 After: Ops-Based Factory

```cpp
// ts_overlay_storage.cpp - proposed implementation
std::unique_ptr<TSOverlayStorage> make_ts_overlay(const TSMeta* ts_meta) {
    if (!ts_meta || !ts_meta->ops || !ts_meta->ops->make_overlay) {
        return nullptr;
    }
    return ts_meta->ops->make_overlay(ts_meta);  // No switch!
}
```

---

## 7. Migration Strategy

### 7.1 Backwards Compatibility

During migration, support both patterns:
1. Keep existing TSMeta subclasses working
2. Add ops pointer to base TSMeta (can be nullptr initially)
3. When ops is non-null, use it; otherwise fall back to virtual methods
4. Gradually migrate each switch to use ops
5. Once all switches migrated, remove virtual methods

### 7.2 Step-by-Step Migration

**Week 1: Foundation**
- Add `TSTypeOps` struct definition
- Add `ops` pointer to `TSMeta` base class (default nullptr)
- Create ops tables for each type but don't wire them up yet

**Week 2: Factory Migration**
- Implement `make_overlay` in each ops table
- Modify `make_ts_overlay()` to check ops first, fall back to switch
- Test that overlay creation works through both paths

**Week 3: Navigation Migration**
- Implement `child_count`, `child_meta`, `child_meta_by_name` in ops tables
- Migrate `ts_view.cpp` navigation to use ops
- Migrate `ts_value.cpp` child access to use ops

**Week 4: Complete Migration**
- Implement remaining ops (wrap_view, etc.)
- Remove switch statements one by one
- Remove virtual methods from TSMeta once all code uses ops

---

## 8. Open Questions

1. **Memory ownership**: Should `TSMeta` own its field info, or reference externally managed data?
   - Current: Subclasses own their vectors
   - Proposed: Union payload could reference external data (smaller footprint)

2. **Python type caching**: Should we cache Python type objects in `TSTypeOps`?
   - Would avoid repeated lookups during wrapping

3. **Iteration protocol**: Is the iterator state approach too complex?
   - Alternative: Expose typed iteration only through views (current approach)

4. **Compile-time optimization**: Can we use `if constexpr` more aggressively?
   - When schema is known at compile time, could skip vtable indirection

---

## 9. Expected Benefits

| Metric | Before | After |
|--------|--------|-------|
| Switch statements | 11+ | 1 (ops lookup at construction) |
| Files to modify for new type | 6+ | 2 (ops table + overlay class) |
| Risk of incomplete switch | High | None (ops always complete) |
| Runtime dispatch cost | Same | Same (vtable either way) |
| Code locality | Scattered | Centralized in ops tables |

---

## 10. Summary

This proposal eliminates the proliferation of switch statements by:
1. Storing type-specific operations **with** the type metadata
2. Using a single consistent dispatch mechanism (function pointer vtable)
3. Making extension a matter of defining an ops table, not hunting for switches

The migration can be done incrementally with backwards compatibility throughout.
