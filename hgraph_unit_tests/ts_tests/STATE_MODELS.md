# Time-Series State Models

This document describes the state models for hgraph's time-series types. Each time-series type
maintains internal state that governs its behavior within the reactive graph execution model.

## Overview

hgraph implements a functional reactive programming model where data flows through a directed
acyclic graph. Time-series types are the fundamental data containers that:

1. **Hold values** that change over time (ticks)
2. **Track modification state** to enable efficient propagation
3. **Support binding** between outputs (producers) and inputs (consumers)
4. **Manage subscriptions** for notification of changes

### State Properties

All time-series types share a common set of state properties:

| Property | Type | Description |
|----------|------|-------------|
| `valid` | bool | Whether the time-series has a value (has been set at least once) |
| `modified` | bool/None | Whether the value changed in the current tick. `None` when unbound |
| `all_valid` | bool | Whether this and all nested time-series are valid |
| `bound` | bool | Whether an input is connected to an output |
| `has_peer` | bool | Whether there is a peer connection (output for input, input for output) |
| `active` | bool | Whether the input is actively subscribing to changes |

### Input vs Output

The time-series architecture distinguishes between:

- **Outputs**: Own data, can be written to, notify subscribers of changes
- **Inputs**: Delegate to bound outputs, can subscribe to changes, read-only

```
┌─────────────┐     bind_output      ┌─────────────┐
│   Output    │ ←───────────────────→ │    Input    │
│  (owns data)│                       │  (delegates)│
└─────────────┘     un_bind_output   └─────────────┘
```

---

## TS (Time-Series Scalar)

The fundamental time-series type representing a single scalar value that changes over time.

### Purpose
Holds a single typed value (int, float, str, etc.) that can be updated on each graph tick.

### State Diagram

```
                    ┌────────────────────────────────────────┐
                    │            UNINITIALIZED               │
                    │  valid=False, modified=False           │
                    └────────────────────────────────────────┘
                                       │
                                       │ value.setter / mark_modified
                                       ▼
                    ┌────────────────────────────────────────┐
                    │              VALID                     │
                    │  valid=True, modified=True             │
                    └────────────────────────────────────────┘
                                       │
                                       │ (tick boundary - cleared by runtime)
                                       ▼
                    ┌────────────────────────────────────────┐
                    │          VALID (not modified)          │
                    │  valid=True, modified=False            │
                    └────────────────────────────────────────┘
```

### Key Transitions

| Transition | Trigger | Description |
|------------|---------|-------------|
| `valid: False → True` | `value.setter`, `mark_modified` | First value assigned |
| `modified: False → True` | `value.setter`, `mark_modified` | Value changed this tick |
| `bound: False → True` | `bind_output` | Input connected to output |
| `active: False → True` | `make_active` | Input starts receiving notifications |

### Lifecycle Example

```python
# 1. Output created: valid=False, modified=False
output = PythonTimeSeriesValueOutput(...)

# 2. Value set: valid=True, modified=True
output.value = 42

# 3. Input binds: bound=True, has_peer=True
input.bind_output(output)

# 4. Input made active: active=True
input.make_active()

# 5. Value changes: modified=True, subscribers notified
output.value = 43
```

---

## TSS (Time-Series Set)

A time-series representing a set that tracks additions and removals as delta changes.

### Purpose
Holds a set of values with efficient delta tracking. Each tick can add or remove elements,
and consumers can observe either the full set or just the delta (added/removed).

### State Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                      SET STATES                             │
├─────────────────────────────────────────────────────────────┤
│  valid=False, modified=False  →  Empty, uninitialized      │
│  valid=True, modified=True    →  Has elements, changed     │
│  valid=True, modified=False   →  Has elements, unchanged   │
└─────────────────────────────────────────────────────────────┘

Delta Properties:
  - added: Set of elements added this tick
  - removed: Set of elements removed this tick
  - value: Current full set
  - delta_value: The delta (added, removed) tuple
```

### Key Operations

| Operation | State Change | Description |
|-----------|--------------|-------------|
| `add(element)` | modified=True, added contains element | Add element to set |
| `remove(element)` | modified=True, removed contains element | Remove element from set |
| `clear()` | modified=True, all in removed | Remove all elements |

### Lifecycle Example

```python
# 1. Empty set: valid=False, length=0
tss = TimeSeriesSet[int]()

# 2. Add elements: valid=True, modified=True, added={1,2}
tss.value = {1, 2}

# 3. Next tick - add more: modified=True, added={3}
tss.add(3)

# 4. Remove element: modified=True, removed={1}
tss.remove(1)
```

---

## TSL (Time-Series List)

A time-series representing a fixed-size list of nested time-series.

### Purpose
Holds a collection of time-series indexed by position. Each element is itself a time-series,
allowing nested reactive updates.

### State Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                      LIST STATES                            │
├─────────────────────────────────────────────────────────────┤
│  all_valid=False  →  Some elements not yet valid           │
│  all_valid=True   →  All elements have values              │
│  modified=True    →  At least one element changed          │
└─────────────────────────────────────────────────────────────┘

Propagation:
  - Child modified → Parent marked modified
  - Parent notified → Can check which children changed
```

### Key Behaviors

| Behavior | Description |
|----------|-------------|
| Size | Fixed at construction time (SIZE parameter in type) |
| Element Access | `tsl[i]` returns the time-series at index i |
| Modification | Setting `tsl[i].value` marks both element and container modified |
| Validity | `all_valid` only True when all elements are valid |

### Lifecycle Example

```python
# 1. Create list of 3 TS[int]: all_valid=False
tsl = TimeSeriesList[TS[int], SIZE[3]]()

# 2. Set first element: modified=True, all_valid=False
tsl[0].value = 10

# 3. Set all elements: all_valid=True
tsl[1].value = 20
tsl[2].value = 30
```

---

## TSB (Time-Series Bundle)

A time-series representing a named collection of heterogeneous time-series (like a struct).

### Purpose
Groups related time-series by name, similar to a dataclass or struct. Each field has its
own type and can be accessed by name.

### State Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     BUNDLE STATES                           │
├─────────────────────────────────────────────────────────────┤
│  Schema defines fields: {name: TS[type], ...}              │
│  all_valid=True when all fields are valid                  │
│  modified=True when any field modified                     │
└─────────────────────────────────────────────────────────────┘

Example Schema:
  class MyBundle(TimeSeriesSchema):
      x: TS[int]
      y: TS[float]
      name: TS[str]
```

### Key Behaviors

| Behavior | Description |
|----------|-------------|
| Field Access | `bundle.field_name` returns the time-series for that field |
| Modification | Setting any field marks the bundle modified |
| Validity | `all_valid` only True when all fields are valid |
| Nested Bundles | Fields can themselves be bundles for hierarchical data |

### Lifecycle Example

```python
# 1. Create bundle: all fields invalid
@ts_schema
class Point(TimeSeriesSchema):
    x: TS[float]
    y: TS[float]

bundle: TSB[Point] = ...

# 2. Set x: bundle.modified=True, all_valid=False
bundle.x.value = 1.0

# 3. Set y: all_valid=True
bundle.y.value = 2.0
```

---

## TSD (Time-Series Dictionary)

A time-series representing a dynamic dictionary with time-series values.

### Purpose
Maps keys to time-series values, with the key set being able to grow/shrink over time.
Tracks which keys were added, removed, or had their values modified.

### State Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                   DICTIONARY STATES                         │
├─────────────────────────────────────────────────────────────┤
│  length: Number of keys in dictionary                      │
│  added_keys: Keys added this tick                          │
│  removed_keys: Keys removed this tick                      │
│  modified_keys: Keys with modified values                  │
└─────────────────────────────────────────────────────────────┘

Delta Tracking:
  - New key → added_keys
  - Deleted key → removed_keys
  - Value change → key in modified_keys
```

### Key Operations

| Operation | State Change | Description |
|-----------|--------------|-------------|
| `tsd[key] = value` | modified=True, key added if new | Set/update key |
| `del tsd[key]` | modified=True, key in removed | Remove key |
| `tsd[key].value = x` | modified=True, key in modified | Update existing value |

### Lifecycle Example

```python
# 1. Empty dict: length=0
tsd = TimeSeriesDict[str, TS[int]]()

# 2. Add key: length=1, added_keys={'a'}
tsd['a'].value = 1

# 3. Add another: length=2, added_keys={'b'}
tsd['b'].value = 2

# 4. Update value: modified_keys={'a'}
tsd['a'].value = 10
```

---

## TSW (Time-Series Window)

A time-series representing a sliding window of historical values.

### Purpose
Maintains a bounded history of values for a nested time-series. Supports both count-based
windows (last N values) and time-based windows (values from last T duration).

### State Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    WINDOW STATES                            │
├─────────────────────────────────────────────────────────────┤
│  length: Current number of values in window                │
│  min_size_met: Whether minimum required values present     │
│  modified: New value pushed this tick                      │
└─────────────────────────────────────────────────────────────┘

Window Types:
  - Count-based: SIZE[N] - keeps last N values
  - Time-based: PERIOD[duration] - keeps values from window
```

### Key Behaviors

| Behavior | Description |
|----------|-------------|
| Push | New values added to front, old values may be evicted |
| Eviction | Values removed when window size/time exceeded |
| Access | Can iterate over all values in window |
| Min Size | Can require minimum values before window is "ready" |

### Lifecycle Example

```python
# 1. Window of last 3 values: length=0
window: TSW[TS[int], SIZE[3]] = ...

# 2. Push first value: length=1
window.value = 10

# 3. Push more: length=3
window.value = 20
window.value = 30

# 4. Push beyond size - oldest evicted: length=3
window.value = 40  # 10 is evicted
```

---

## REF (Time-Series Reference)

A reference type that allows indirect binding between time-series.

### Purpose
Provides a level of indirection, allowing dynamic rebinding of time-series connections
at runtime. Used for implementing feedback loops and dynamic routing.

### State Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                   REFERENCE STATES                          │
├─────────────────────────────────────────────────────────────┤
│  active: Whether actively forwarding values                │
│  output: The target time-series being referenced           │
└─────────────────────────────────────────────────────────────┘

Reference Flow:
  REF[TS[int]] → points to → TS[int]
  Reading REF returns value from target
  Changes to target propagate through REF
```

### Key Behaviors

| Behavior | Description |
|----------|-------------|
| Binding | `ref.output = target` sets the reference target |
| Reading | `ref.value` returns `ref.output.value` |
| Notification | Changes to target notify ref subscribers |

---

## SIGNAL

A special time-series type that carries no value, just indicates an event occurred.

### Purpose
Used for synchronization and triggering without data transfer. The "tick" itself is
the information - no value is carried.

### State Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    SIGNAL STATES                            │
├─────────────────────────────────────────────────────────────┤
│  modified=True: Signal fired this tick                     │
│  modified=False: No signal this tick                       │
└─────────────────────────────────────────────────────────────┘

Note: valid is always True for signals (they don't carry values)
```

---

## Common State Transitions

### Binding Lifecycle

All input types follow this binding lifecycle:

```
┌────────────┐    bind_output    ┌────────────┐    make_active    ┌────────────┐
│  UNBOUND   │ ─────────────────→ │   BOUND    │ ────────────────→ │   ACTIVE   │
│ bound=False│                    │ bound=True │                   │ active=True│
│ has_peer=F │                    │ has_peer=T │                   │            │
└────────────┘                    └────────────┘                   └────────────┘
       ▲                                │
       │                                │ un_bind_output
       │                                ▼
       │         ┌────────────────────────────────────────┐
       └─────────│ modified becomes None when unbound     │
                 └────────────────────────────────────────┘
```

### Modification Propagation

When a nested container element is modified:

```
Child modified → mark_modified() called on child
                      │
                      ▼
              Parent notified via subscription
                      │
                      ▼
              Parent marks itself modified
                      │
                      ▼
              Continues up to graph root
```

---

## Test Coverage Summary

All state transitions documented above are exercised by the test suite:

| Type | Unique Transitions | Test Coverage |
|------|-------------------|---------------|
| TS | 15 | 100% |
| TSS | 13 | 100% |
| TSL | 14 | 100% |
| TSB | 9 | 100% |
| TSD | 11 | 100% |
| TSW | 9 | 100% |
| REF | 1 | 100% |
| SIGNAL | 7 | 100% |

The behavior tests in this directory verify each state transition through isolated
test cases that exercise specific state changes and verify the expected outcomes.

---

## Generating This Documentation

This documentation was generated by instrumenting the Python time-series implementation
and tracing all state transitions during test execution:

```bash
# Run tests with tracing enabled
HGRAPH_TRACE_STATES=1 uv run pytest hgraph_unit_tests/ts_tests/ -v

# Analyze the generated trace
python hgraph_unit_tests/ts_tests/analyze_state_flows.py state_trace.json
```

The raw trace data and auto-generated analysis are in:
- `state_trace.json` - Raw transition data
- `state_trace_analysis.md` - Auto-generated tables and diagrams
