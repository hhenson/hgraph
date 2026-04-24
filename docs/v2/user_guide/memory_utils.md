# Memory Utils User Guide

`hgraph::v2::MemoryUtils` is the storage-planning layer for the v2 type-erased value system.

It is built around three concepts:

1. `StoragePlan`: a static description of layout and lifecycle
2. `AllocatorOps`: an explicit allocation policy over `StorageLayout`
3. `StorageHandle`: an owning-or-borrowed runtime handle for live storage

This layer is intended to sit underneath the eventual erased `Value` and `ValueBuilder` APIs.

## Header

```cpp
#include <hgraph/v2/types/utils/memory_utils.h>
```

## `StoragePlan`

`StoragePlan` describes a payload without owning memory.

It answers:

1. how many bytes the payload needs
2. what alignment it requires
3. whether it can live inline in a handle
4. how to default-construct, copy-construct, move-construct, and destroy the payload

The key design point is that plans are process-lifetime static objects. Typed plans are cached per C++ type, and composite plans are interned for the lifetime of the process.

### Typed plans

Start with:

```cpp
using MemoryUtils = hgraph::v2::MemoryUtils;

const auto& plan = MemoryUtils::plan_for<uint32_t>();
```

`plan_for<T>()` returns a cached static plan for `T`.

Useful queries:

```cpp
plan.valid();
plan.layout.size;
plan.layout.alignment;
plan.can_default_construct();
plan.can_copy_construct();
plan.can_move_construct();
plan.requires_destroy();
plan.stores_inline<>();
plan.requires_deallocate<>();
```

### Plan lifetime and identity

Because plans are cached, pointer identity is meaningful:

```cpp
const auto& lhs = MemoryUtils::plan_for<uint32_t>();
const auto& rhs = MemoryUtils::plan_for<uint32_t>();

bool same_plan = &lhs == &rhs;
```

That same rule applies to composite plans built from the same structure.

## Allocation Is Separate: `AllocatorOps`

Allocation is not part of `StoragePlan`.

Instead, allocation is handled by `AllocatorOps`, which works only from `StorageLayout`:

```cpp
MemoryUtils::AllocatorOps allocator;
void* memory = allocator.allocate_storage(plan.layout);
allocator.deallocate_storage(memory, plan.layout);
```

The default allocator uses aligned `operator new` / `operator delete`.

You can provide custom allocation hooks:

```cpp
void* my_allocate(MemoryUtils::StorageLayout layout);
void my_deallocate(void* memory, MemoryUtils::StorageLayout layout) noexcept;

MemoryUtils::AllocatorOps allocator{
    .allocate = &my_allocate,
    .deallocate = &my_deallocate,
};
```

This keeps plan description and memory sourcing separate.

## Inline vs Heap Placement

Inline eligibility is controlled by `InlineStoragePolicy`.

The default policy is intentionally small:

```cpp
MemoryUtils::InlineStoragePolicy<>
```

That means:

1. inline budget is `sizeof(void*)`
2. inline alignment is `alignof(void*)`
3. a payload only stays inline if it is trivially copyable and trivially destructible

Example:

```cpp
const auto& plan = MemoryUtils::plan_for<uint32_t>();
bool is_inline = plan.stores_inline<>();
```

You can use a larger handle policy when needed:

```cpp
using WidePolicy = MemoryUtils::InlineStoragePolicy<16, 8>;

const auto& plan = MemoryUtils::plan_for<MyPod>();
bool is_inline = plan.stores_inline<WidePolicy>();
```

## `StorageHandle`

`StorageHandle` holds a plan and live storage.

It supports two modes:

1. owning
2. borrowed

### Owning handles

Constructing a handle from a plan creates an owning handle and default-constructs the payload:

```cpp
MemoryUtils::StorageHandle<> handle(MemoryUtils::plan_for<uint32_t>());
*handle.as<uint32_t>() = 42;
```

Equivalent factory:

```cpp
auto handle = MemoryUtils::StorageHandle<>::owning(MemoryUtils::plan_for<uint32_t>());
```

If you want a custom allocator:

```cpp
MemoryUtils::StorageHandle<> handle(MemoryUtils::plan_for<MyType>(), allocator);
```

### Borrowed handles

Borrowed handles refer to existing storage without taking ownership:

```cpp
TrackedValue external;
auto ref = MemoryUtils::StorageHandle<>::reference(MemoryUtils::plan_for<TrackedValue>(), &external);
```

Borrowed handles:

1. do not allocate
2. do not destroy the referenced storage
3. still carry the plan

### Handle queries

```cpp
handle.has_value();
handle.is_owning();
handle.is_reference();
handle.stores_inline();
handle.stores_heap();
handle.plan();
handle.allocator();
handle.data();
```

`handle.plan()` returns the cached plan pointer for the active payload.

### Typed access

```cpp
auto* value = handle.as<uint32_t>();
```

### Copy and move semantics

`StorageHandle` is intentionally value-like on copy:

1. copying any populated handle creates a new owning handle
2. copying a borrowed handle deep-copies the referenced payload
3. copying an owning handle deep-copies the owned payload
4. moving transfers the current ownership/reference state

Example:

```cpp
TrackedValue external;
auto ref = MemoryUtils::StorageHandle<>::reference(MemoryUtils::plan_for<TrackedValue>(), &external);

auto owned_copy = ref;  // deep copy, now owning
```

### Destruction

Owning handles are RAII objects:

1. they destroy the payload when they die
2. they deallocate heap storage through their allocator when needed
3. borrowed handles do not touch the referenced storage

You can release a handle early with:

```cpp
handle.reset();
```

## Composite Plans

Composite plans describe structured payloads built from child plans.

Two composite kinds are supported:

1. `tuple()`
2. `named_tuple()`

Both support nesting through `add_plan(...)` or `add_field(..., plan)`.

### Tuple builder

Use `tuple()` for positional composition:

```cpp
const auto& tuple_plan = MemoryUtils::tuple()
    .add_type<uint8_t>()
    .add_plan(MemoryUtils::plan_for<uint32_t>())
    .build();
```

Supported tuple-builder calls:

1. `add_type<T>()`
2. `add_plan(plan)`

### Named tuple builder

Use `named_tuple()` when fields need stable names:

```cpp
const auto& point_plan = MemoryUtils::named_tuple()
    .add_field<uint32_t>("x")
    .add_field<uint32_t>("y")
    .build();
```

Supported named-tuple-builder calls:

1. `add_field<T>(name)`
2. `add_field(name, plan)`

Field names must be unique and non-empty.

### Shorthand helpers

Tuple shorthand:

```cpp
const auto& tuple_plan = MemoryUtils::tuple_plan({
    &MemoryUtils::plan_for<uint8_t>(),
    &MemoryUtils::plan_for<uint32_t>(),
});
```

Named tuple shorthand:

```cpp
const auto& named_plan = MemoryUtils::named_tuple_plan({
    {"x", &MemoryUtils::plan_for<uint32_t>()},
    {"y", &MemoryUtils::plan_for<uint32_t>()},
});
```

`composite()` and `composite_plan(...)` remain tuple aliases for now.

## Nested Structures

Nested structure is expressed directly through child plans.

Example:

```cpp
const auto& point = MemoryUtils::named_tuple()
    .add_field<uint16_t>("x")
    .add_field<uint16_t>("y")
    .build();

const auto& payload = MemoryUtils::tuple()
    .add_type<uint8_t>()
    .add_plan(point)
    .build();
```

That is the intended model for nested tuple and named-tuple storage.

## Inspecting Composite Metadata

Every composite plan exposes its child metadata:

```cpp
plan.is_composite();
plan.is_tuple();
plan.is_named_tuple();
plan.component_count();
plan.components();
plan.component(0);
```

Named tuples also support lookup by field name:

```cpp
plan.component("x");
plan.find_component("x");
```

Each component reports:

1. `index`
2. `offset`
3. `name` for named tuples
4. `plan` as a pointer to the child plan

Example:

```cpp
const auto& field = point.component("x");
size_t offset = field.offset;
const auto* child_plan = field.plan;
```

## Composite Lifecycle Semantics

Composite plans compose child lifecycle automatically:

1. children default-construct in index order
2. children destroy in reverse order
3. copy construction proceeds child-by-child in index order
4. move construction proceeds child-by-child in index order
5. if construction fails partway through, already-constructed children are destroyed before the exception is rethrown

Because composites are built from plans, nested composites inherit the same behavior recursively.

## Addressing Child Storage

When you need to reach into a composite block manually, use:

```cpp
MemoryUtils::advance(memory, offset);
MemoryUtils::cast<T>(memory);
```

Example:

```cpp
MemoryUtils::StorageHandle<> handle(point);

auto* x = MemoryUtils::cast<uint32_t>(
    MemoryUtils::advance(handle.data(), point.component("x").offset));
```

## Recommended Usage Pattern

For new erased-storage code, the intended pattern is:

1. derive or cache a `StoragePlan` from a schema
2. use `tuple()` or `named_tuple()` to build structured plans
3. use `add_plan(...)` and `add_field(...)` for nested composition
4. use `StorageHandle` for live values
5. use `AllocatorOps` when storage sourcing needs to be customized
6. use borrowed handles only as references, not as a shared ownership mechanism

## Current Limitations

This is still a low-level utility layer. In particular:

1. it is not yet wired into the v2 `ValueBuilder`
2. it does not yet derive plans directly from the v2 type registry
3. it does not yet solve allocator strategy for dynamic container internals
4. the final erased `Value` handle may still choose a tighter physical representation than `StorageHandle`

## Related Code

1. [memory_utils.h](/Users/hhenson/CLionProjects/hgraph_1/cpp/include/hgraph/v2/types/utils/memory_utils.h:1)
2. [test_v2_memory_utils.cpp](/Users/hhenson/CLionProjects/hgraph_1/cpp/tests/test_v2_memory_utils.cpp:1)
