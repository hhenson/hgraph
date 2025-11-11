# REF[TSL]/REF[TSB] Item Creation Policy

**Date:** 2025-11-10  
**Purpose:** Document when and where `_items` should be created

---

## Core Principle

**Items should ONLY be created in two places:**
1. `__getitem__()` - When accessed during wiring
2. `do_bind_output()` - When binding to non-peered output

**Items should NEVER be force-created in:**
- `__iter__()` - Only iterate if already created
- Property getters (`value`, `modified`, `valid`, etc.) - Check if items exist
- `make_active()`/`make_passive()` - Only propagate if items exist
- Any other methods

---

## Why This Matters

### 1. Peered Mode Must Stay Lightweight

**Peered binding:** `REF[TSL]` → `REF[TSL]` output
- `_items` must stay `None` (indicates peered mode)
- Creating items would waste memory and break mode detection
- Zero overhead is the goal

```python
# Peered mode
ref._output = peer_output
ref._items = None  # Must stay None!

# If __iter__ forced creation:
for item in ref:  # ❌ Would create items unnecessarily!
    pass
```

### 2. Non-Peered Mode is On-Demand

**Non-peered binding:** `REF[TSL]` → `TSL` output
- Items created only when needed
- Triggered by wiring accessing children: `ref[0]`, `ref[1]`, etc.
- Or by binding logic if accessed before `__getitem__`

```python
# Non-peered mode
ref[0]  # Creates ALL items
ref._items = [child0, child1, child2]  # Now exists
```

---

## Creation Locations

### ✅ Location 1: `__getitem__()` - Wiring Access

**When:** During graph wiring, when children are accessed

```python
def __getitem__(self, item):
    # First access creates ALL items at once
    if self._items is None:
        if self._value_builder is None:
            raise RuntimeError("Builder not set")
        # Create all items in one batch
        self._items = [
            self._value_builder.make_instance(owning_input=self)
            for _ in range(self._size)  # TSL: homogeneous
            # or
            for builder in self._field_builders  # TSB: heterogeneous
        ]
    return self._items[item]
```

**Example:**
```python
@graph
def g(ref: REF[TSL[TS[int], Size[3]]]):
    # Wiring accesses children
    a = ref[0]  # ← Creates ALL items here
    b = ref[1]  # Access existing
    c = ref[2]  # Access existing
```

### ✅ Location 2: `do_bind_output()` - Non-Peered Binding

**When:** Binding to non-peered output, if items not created yet

```python
def do_bind_output(self, output: TimeSeriesOutput):
    if output.is_reference():
        # Peered - don't create items
        return super().do_bind_output(output)
    
    # Non-peered - create items if not already done
    if self._items is None:
        # Fallback: create if bind_output called before __getitem__
        self._items = [
            self._value_builder.make_instance(owning_input=self)
            for _ in range(self._size)
        ]
    
    # Bind each child
    for i, child in enumerate(self._items):
        child.bind_output(output[i])
```

**Example:**
```python
# Direct programmatic binding (not via wiring)
ref = PythonTimeSeriesListReferenceInput(...)
ref.bind_output(tsl_output)  # ← Creates items here if needed
```

### ✅ Location 3: `clone_binding()` - Cloning Non-Peered Bindings

**When:** Cloning from another ref that has items

```python
def clone_binding(self, other: TimeSeriesReferenceInput):
    self.un_bind_output()
    if other.output:
        # Peered - just bind to output, no items
        self.bind_output(other.output)
    elif hasattr(other, '_items') and other._items:
        # Non-peered - ensure our items exist before cloning
        if self._items is None:
            # Create items to match other
            self._items = [
                self._value_builder.make_instance(owning_input=self)
                for _ in range(self._size)
            ]
        # Clone bindings
        for o, s in zip(other._items, self._items):
            s.clone_binding(o)
```

**Example:**
```python
# In map/reduce nodes
inner_ref.clone_binding(outer_ref)  # ← May create items if outer is non-peered
```

---

## Anti-Patterns (What NOT To Do)

### ❌ DON'T Force Creation in `__iter__()`

**Wrong:**
```python
def __iter__(self):
    if self._items is None:
        # ❌ BAD: Forces creation even in peered mode!
        if self._size > 0:
            _ = self[0]
    return iter(self._items) if self._items else iter([])
```

**Right:**
```python
def __iter__(self):
    # ✅ GOOD: Only iterate if already created
    return iter(self._items) if self._items else iter([])
```

**Why:** Iteration might happen in peered mode (debugging, logging, etc.). Forcing creation breaks the mode detection and wastes memory.

### ❌ DON'T Force Creation in Property Getters

**Wrong:**
```python
@property
def modified(self) -> bool:
    if self._items is None:
        # ❌ BAD: Creates items just to check modified state!
        _ = self[0]
    return any(i.modified for i in self._items)
```

**Right:**
```python
@property
def modified(self) -> bool:
    if self._output is not None:
        return self._output.modified  # Peered
    elif self._items:
        return any(i.modified for i in self._items)  # Non-peered
    else:
        return False  # Neither mode active yet
```

**Why:** Properties are called frequently. Creating items in getters would have severe performance impact and break peered mode.

### ❌ DON'T Force Creation in `make_active()`/`make_passive()`

**Wrong:**
```python
def make_active(self):
    # ❌ BAD: Creates items to make them active!
    if self._items is None:
        _ = self[0]
    for item in self._items:
        item.make_active()
```

**Right:**
```python
def make_active(self):
    if self._output is not None:
        super().make_active()  # Peered
    else:
        self._active = True
    
    # Only propagate if items were created (non-peered)
    if self._items:
        for item in self._items:
            item.make_active()
```

**Why:** Making ref active doesn't mean children need to exist. In peered mode, activation is handled by the output.

---

## Mode Detection Pattern

The presence of `_items` is the **authoritative mode indicator**:

```python
# Check binding mode
if self._items is None:
    if self._output is not None:
        # Peered mode
        return self._output.modified
    else:
        # Not bound yet
        return False
else:
    # Non-peered mode
    return any(i.modified for i in self._items)
```

---

## Summary

**Creation triggers:**
1. ✅ `__getitem__(n)` - Wiring accesses `ref[n]`
2. ✅ `do_bind_output(non_ref)` - Binding to concrete TSL/TSB
3. ✅ `clone_binding(other)` - Cloning non-peered binding

**Never create in:**
- ❌ `__iter__()` - Only iterate existing
- ❌ Property getters - Only check if exists
- ❌ `make_active()`/`make_passive()` - Only propagate if exists
- ❌ Any other methods

**Key principle:** Peered mode must have zero overhead. Creating items breaks this guarantee.

