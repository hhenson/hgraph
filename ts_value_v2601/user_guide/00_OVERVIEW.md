# HGraph Time-Series System: User Guide

**Version**: v2601
**Audience**: Graph implementers and node authors

---

## What Is This?

The HGraph time-series system provides **type-erased, schema-driven data structures** for building reactive computation graphs. Users define nodes that consume and produce time-series values, and the runtime handles the plumbing.

This guide describes what you interact with as a user. It does not cover runtime internals.

### C++ API

This guide focuses on the **C++ API** - the high-performance implementation used for building nodes and accessing time-series data. The API provides type-erased access to data via Views, with schema-driven operations.

---

## Core Mental Model

### Values and Time-Series

There are two related but distinct concepts:

| Concept | What It Is | Example |
|---------|------------|---------|
| **Value** | A piece of data at a point in time | `42`, `"hello"`, `{x: 1.0, y: 2.0}` |
| **Time-Series** | A value that changes over time, with change tracking | `TS[int]`, `TSB[Point]`, `TSL[TS[float], 10]` |

A time-series **contains** a value. When you read `ts.value`, you get the current value. When you write to a time-series output, you update its value and mark it as modified.

### Inputs and Outputs

Nodes have:
- **Inputs**: Read-only views of time-series data, potentially linked to another node's output
- **Outputs**: Writable time-series data owned by this node

```
┌─────────────┐          ┌─────────────┐
│   Node A    │          │   Node B    │
│             │          │             │
│  output ●───┼──link────┼──▶ input    │
└─────────────┘          └─────────────┘
```

When Node A writes to its output, Node B sees the change through its input.

### Links: The Binding Mechanism

**Links** are central to the system. An input doesn't copy data - it **links** to an output and observes it.

- A **bound link** points to a specific output
- An **unbound link** is a placeholder waiting to be connected
- Links can be **active** (notifying on changes) or **passive** (data available but not triggering)

Links enable:
- Zero-copy data sharing between nodes
- Fine-grained change notification
- Dynamic rewiring at runtime (for REF types)

---

## Type System Overview

### Scalar Types

All Value types are scalar (scalar in time). This includes atomic values (`int`, `float`, `bool`, `str`, `datetime`, `timedelta`, etc.) as well as composite values (bundles, lists, sets, maps).

### Composite Types

| Type | Description | Value Access |
|------|-------------|--------------|
| **Bundle** | Named fields (like a struct) | `bundle.field_name` or `bundle[index]` |
| **Tuple** | Positional fields (unnamed) | `tuple[index]` |
| **List** | Homogeneous sequence | `list[index]`, `len(list)` |
| **Set** | Unique unordered elements | `element in set`, iteration |
| **Map/Dict** | Key-value pairs | `map[key]`, `key in map` |

### Time-Series Types

| Type | Contains | Use Case |
|------|----------|----------|
| **TS[T]** | Single scalar value | Most common - a changing value |
| **TSB[Schema]** | Bundle of time-series fields | Structured data where fields change independently |
| **TSL[TS[T], Size]** | List of time-series | Fixed or dynamic collection of independent series |
| **TSD[K, TS[V]]** | Dict mapping keys to time-series | Dynamic keyed collection |
| **TSS[T]** | Set that changes over time | Tracking membership |
| **REF[TS[T]]** | Reference to another time-series | Dynamic routing |
| **SIGNAL** | Tick with no value | Pure event notification |

---

## Document Index

1. **[Schema](01_SCHEMA.md)** - Defining types (start here)
2. **[Value](02_VALUE.md)** - Constructing and operating on data values
3. **[Time-Series](03_TIME_SERIES.md)** - Time-series inputs and outputs
4. **[Links and Binding](04_LINKS_AND_BINDING.md)** - How data flows between nodes
5. **[Access Patterns](05_ACCESS_PATTERNS.md)** - Reading, writing, iteration
6. **[Delta and Change Tracking](06_DELTA.md)** - Working with incremental changes

---

## Quick Example

```python
@compute_node
def moving_average(
    ts: TS[float],          # Input: linked to some output
    window_size: int = 10       # Scalar parameter (not time-series)
) -> TS[float]:                 # Output: owned by this node

    # Check if input changed this tick
    if ts.modified:
        current = ts.value   # Read current value
        # ... compute average ...
        result = ...
        return result            # Write to output (marks it modified)
```

This is loosely the C++ equivalent:

```cpp
void moving_average(
    const TSView& ts,             // Input: linked to some output (inputs are marked const)
    int window_size,              // Scalar parameter (not time-series)
    TSView& output                // Output: owned by this node (outputs are non-const i.e. mutable)
) {
    // Check if input changed this tick
    if (ts.modified()) {
        double current = ts.value().as<double>();   // Read current value (type-erased)
        // ... compute average ...
        double result = ...
        output.set_value(value_from(result));       // Write to output (marks it modified)
    }
}
```

The user sees:
- `ts` - an input that tracks changes
- `ts.modified()` - did it change this tick?
- `ts.value()` - the current data
- `output.set_value()` - write to output

Everything else (links, notifications, schema) is handled by the runtime.
