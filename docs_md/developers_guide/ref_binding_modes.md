# REF[TSL] Binding Modes Explained

**Date:** 2025-11-10  
**Purpose:** Document the two binding modes for `REF[TSL]` and how they're detected

## Overview

`REF[TSL]` inputs support **two distinct binding modes**, detected by the presence/absence of `_items`:

1. **Peered Mode** - `_items = None` (bound to `REF[TSL]` output)
2. **Non-Peered Mode** - `_items != None` (bound to individual outputs via `TSL`)

## Binding Mode 1: Peered

### When It Happens
```python
@graph
def producer() -> REF[TSL[TS[int], Size[3]]]:
    # Returns a reference output
    ...

@graph  
def consumer(ref: REF[TSL[TS[int], Size[3]]]):
    # ref is bound DIRECTLY to producer's REF output
    ...

result = consumer(producer())  # <-- Peered binding
```

### What Happens
1. During wiring, `consumer`'s `ref` input is bound to `producer`'s output
2. Both are `REF[TSL]` types → **peered connection**
3. `ref._output` is set to the output
4. `ref._items` stays `None` (never created)
5. `__getitem__` is **never called** during wiring

### Detection
```python
if self._output is not None and self._items is None:
    # Peered mode!
```

### Behavior
- `ref.value` → calls `self._output.value` (peer's value)
- `ref.modified` → calls `self._output.modified`
- `ref.valid` → calls `self._output.valid`
- No children created, no iteration needed

---

## Binding Mode 2: Non-Peered

### When It Happens
```python
@graph
def producer() -> TSL[TS[int], Size[3]]:
    # Returns a regular TSL
    ...

@graph
def consumer(ref: REF[TSL[TS[int], Size[3]]]):
    # ref will be bound to ELEMENTS of the TSL
    ...

result = consumer(producer())  # <-- Non-peered binding
```

### What Happens
1. During wiring, graph builder needs to connect `producer`'s TSL to `consumer`'s REF input
2. Types don't match: `TSL` → `REF[TSL]` (not peers!)
3. Graph builder calls `ref[0]`, `ref[1]`, `ref[2]` to get child inputs
4. **First `__getitem__` call triggers creation of ALL items at once**
5. `ref._items = [child0, child1, child2]` created
6. Each child is then bound: `child0.bind_output(tsl[0])`, etc.
7. `ref._output` stays `None`

### Detection
```python
if self._items is not None:
    # Non-peered mode!
```

### Behavior
- `ref.value` → constructs `UnBoundTimeSeriesReference(from_items=[i.value for i in self._items])`
- `ref.modified` → `any(i.modified for i in self._items)`
- `ref.valid` → `any(i.valid for i in self._items)`
- Children handle their own bindings

---

## Implementation Details

### PythonTimeSeriesListReferenceInput

```python
@dataclass
class PythonTimeSeriesListReferenceInput(...):
    _items: list[TimeSeriesReferenceInput] | None = None  # None initially
    _size: int = 0  # Known from builder
    _value_builder: Optional["TSInputBuilder"] = None  # Knows how to create children
```

### __getitem__ - Efficient Batch Creation

```python
def __getitem__(self, item):
    # On FIRST access, create ALL items at once
    if self._items is None:
        # Create all children in one go (more efficient than one-by-one)
        self._items = [
            self._value_builder.make_instance(owning_input=self)
            for _ in range(self._size)
        ]
    return self._items[item]
```

**Why batch creation?**
- During wiring, `ref[0]`, `ref[1]`, `ref[2]` will be called in sequence
- Creating all at once is more efficient than lazy one-by-one
- Still maintains the detection pattern (`_items` presence)

### do_bind_output - Handles Both Modes

```python
def do_bind_output(self, output: TimeSeriesOutput) -> bool:
    if isinstance(output, TimeSeriesReferenceOutput):
        # PEERED MODE: output is also a REF
        self._value = None
        return super().do_bind_output(output)  # Sets self._output

    # NON-PEERED MODE: output is concrete TSL
    
    # Validate size
    if len(output) != self._size:
        raise TypeError(f"Size mismatch: {self._size} != {len(output)}")
    
    # Create items if not already created (fallback if bind_output called directly)
    if self._items is None:
        self._items = [
            self._value_builder.make_instance(owning_input=self)
            for _ in range(self._size)
        ]
    
    # Bind each child
    for i, child in enumerate(self._items):
        child.bind_output(output[i])
    
    return False  # Not a peer
```

### State Properties - Adapt to Mode

```python
@property
def modified(self) -> bool:
    if self._sampled:
        return True
    elif self._output is not None:
        # Peered mode: ask output
        return self.output.modified
    elif self._items:
        # Non-peered mode: ask children
        return any(i.modified for i in self._items)
    else:
        return False
```

---

## Comparison: Before vs After

### Before (Generic Class)

```python
class PythonTimeSeriesReferenceInput:
    _items: list | None = None  # Could be anything
    
    def __getitem__(self, item):
        if self._items is None:
            self._items = []
        # Create one-by-one as needed
        while item > len(self._items) - 1:
            new_item = PythonTimeSeriesReferenceInput(...)
            self._items.append(new_item)
        return self._items[item]
```

**Issues:**
- ❌ Lazy one-by-one creation
- ❌ No size validation
- ❌ Unbounded growth
- ❌ Type information lost

### After (Specialized Class)

```python
class PythonTimeSeriesListReferenceInput:
    _items: list | None = None  # None = peered, list = non-peered
    _size: int = 0  # Known size
    _value_builder: TSInputBuilder  # Knows child type
    
    def __getitem__(self, item):
        if self._items is None:
            # Create ALL items at once
            self._items = [
                self._value_builder.make_instance(...)
                for _ in range(self._size)
            ]
        return self._items[item]
```

**Benefits:**
- ✅ Batch creation (more efficient)
- ✅ Size validation at binding
- ✅ Fixed size (can't grow beyond _size)
- ✅ Type information preserved

---

## Benefits of This Pattern

### 1. Efficient Detection

```python
# Fast check - no method calls needed
if self._items is None:
    # Peered mode
else:
    # Non-peered mode
```

### 2. Memory Efficiency

**Peered mode:**
- `_items = None` → Zero memory overhead
- Direct delegation to output

**Non-peered mode:**
- `_items` created only when needed
- But created all at once (no fragmentation)

### 3. Type Safety

```python
# Size known at construction
ref._size = 3

# Validated at binding
if len(output) != self._size:
    raise TypeError(f"Size mismatch")
```

### 4. Clear Semantics

```python
# Peered: Simple delegation
if self._output is not None:
    return self._output.modified

# Non-peered: Aggregate from children  
elif self._items:
    return any(i.modified for i in self._items)
```

---

## Example Scenarios

### Scenario 1: Peered Binding

```python
@compute_node
def create_ref() -> REF[TSL[TS[int], Size[3]]]:
    return REF.make(...)  # Creates and returns reference

@compute_node
def use_ref(ref: REF[TSL[TS[int], Size[3]]]) -> TS[bool]:
    return ref.valid

# Wiring
result = use_ref(create_ref())
```

**Flow:**
1. `use_ref`'s `ref` parameter is wired to `create_ref`'s output
2. Both are `REF[TSL]` → peered connection
3. `ref._output` = output from `create_ref`
4. `ref._items` = `None` (never created)
5. `__getitem__` never called

### Scenario 2: Non-Peered Binding

```python
@graph
def create_tsl() -> TSL[TS[int], Size[3]]:
    return [const(1), const(2), const(3)]

@compute_node
def use_ref(ref: REF[TSL[TS[int], Size[3]]]) -> TS[bool]:
    return ref.valid

# Wiring
result = use_ref(create_tsl())
```

**Flow:**
1. `use_ref`'s `ref` parameter needs to bind to `TSL` output
2. Types don't match: `TSL` ≠ `REF[TSL]` → non-peered
3. Graph builder extracts child inputs: calls `ref[0]`, `ref[1]`, `ref[2]`
4. **First call to `ref[0]` creates all items:**
   ```python
   ref._items = [
       REF[TS[int]] child0,
       REF[TS[int]] child1,
       REF[TS[int]] child2,
   ]
   ```
5. Graph builder binds each child:
   - `ref._items[0].bind_output(tsl[0])`
   - `ref._items[1].bind_output(tsl[1])`
   - `ref._items[2].bind_output(tsl[2])`
6. `ref._output` stays `None`

### Scenario 3: Direct Programmatic Binding

```python
# Create ref input manually
ref = PythonTimeSeriesListReferenceInput(_size=3, _value_builder=...)

# Bind directly to TSL output (no wiring, no __getitem__ calls)
ref.bind_output(tsl_output)
```

**Flow:**
1. `bind_output()` called directly
2. Detects non-peered (not a `TimeSeriesReferenceOutput`)
3. `_items` is still `None` (no __getitem__ called)
4. **Fallback: Creates items in `do_bind_output`**:
   ```python
   if self._items is None:
       self._items = [
           self._value_builder.make_instance(...)
           for _ in range(self._size)
       ]
   ```
5. Binds each child

---

## Key Insights

### 1. _items as Mode Indicator

`_items` serves dual purpose:
- **Stores children** (when in non-peered mode)
- **Indicates mode** (`None` = peered, `not None` = non-peered)

### 2. Batch Creation

Creating all items at once is more efficient than one-by-one:
- **Old:** `ref[0]` creates 1, `ref[1]` creates 1, `ref[2]` creates 1
- **New:** `ref[0]` creates ALL, `ref[1]` accesses, `ref[2]` accesses

### 3. Size Validation

Size mismatch detected early:
```python
# At binding time, not runtime!
if len(output) != self._size:
    raise TypeError(f"Size mismatch...")
```

### 4. Lazy But Efficient

- Peered mode: No items created (pure delegation)
- Non-peered mode: Items created on first access (but all at once)

---

## Testing

Both binding modes are tested in `hgraph_unit_tests/_wiring/test_ref.py`:

**Peered tests:**
- `test_ref` - Basic reference
- `test_route_ref` - Routing references
- `test_merge_ref` - Merging references

**Non-peered tests:**
- `test_merge_ref_non_peer` - Non-peered simple
- `test_merge_ref_non_peer_complex_inner_ts` - **REF[TSL] non-peered**
- `test_merge_ref_inner_non_peer_ts` - **REF[TSL] non-peered**
- `test_free_bundle_ref` - **REF[TSB] non-peered**

All tests pass in both Python and C++ runtimes! ✅

---

## Summary

The specialized `PythonTimeSeriesListReferenceInput` class:

✅ **Supports both binding modes** (peered and non-peered)  
✅ **Uses `_items` presence as mode detector**  
✅ **Creates items efficiently** (all at once, not one-by-one)  
✅ **Validates size** at binding time  
✅ **Preserves type information** (knows size, has builder for children)  
✅ **Backward compatible** (all existing tests pass)  

The implementation correctly handles the dual-mode nature of reference inputs while adding type safety and validation!

