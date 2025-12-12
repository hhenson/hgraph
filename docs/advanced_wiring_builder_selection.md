# Advanced Wiring: Builder Selection Based on Binding Mode

**Last Updated:** 2025-12-12
**Status:** Experimental (stub infrastructure complete, optimizations pending)

## Overview

This document describes the architecture for selecting optimized runtime builders based on binding mode information captured during wiring. The goal is to enable the runtime to use specialized, optimized classes when the binding pattern is known at wiring time, rather than detecting it at runtime.

## Problem Statement

When a `REF[X]` input binds to an output, there are two distinct scenarios:

1. **Peered Binding** (`REF[X]` → `REF[X]`): The input binds directly to a REF output. The value is already a `TimeSeriesReference` and can be used directly.

2. **Non-Peered Binding** (`REF[X]` → `X`): The input binds to a non-REF output. The runtime must wrap the output in a `BoundTimeSeriesReference` to satisfy the REF type.

Currently, the runtime detects which case applies by checking the output type at bind time. This works but has drawbacks:
- Runtime overhead for type checking
- Complex code paths that handle both cases
- Missed optimization opportunities (e.g., the peered case could skip reference wrapping entirely)

## Solution Architecture

### Key Concept: Binding Mode

The `BindingMode` flag (defined in `hgraph/_builder/_graph_builder.py`) captures binding patterns:

```python
class BindingMode(Flag):
    PEER = 0           # Direct type match (REF→REF, TS→TS)
    REF_NON_PEER = auto()  # Reference wrapping needed (REF[X]→X)
    NON_PEER = auto()      # Structural binding (subscript into TSL/TSB)
```

These flags can be combined with bitwise OR for complex cases.

### Data Flow

```
Wiring Time                          Build Time                      Runtime
-----------                          ----------                      -------

WiringPort.get_binding_mode()   →   WiringNodeSignature
         ↓                              .input_binding_modes   →   Builder.make_instance()
WiringPort.get_binding_modes_by_path()       ↓                           ↓
         ↓                          Factory._make_ref_input_builder()    ↓
WiringNodeInstance.input_binding_modes       ↓                      Peered/NonPeered
                                    Type-specific builder with       stub class
                                    binding_mode parameter
```

### Path-Based Binding Modes

Binding modes are tracked by path tuple, not just input name. This handles nested structures:

```python
input_binding_modes: frozendict[tuple, BindingMode]

# Examples:
# (0,) → BindingMode.PEER           # First input, peered
# (1,) → BindingMode.REF_NON_PEER   # Second input, non-peered REF
# (2, 0) → BindingMode.PEER         # Third input, first element (for TSL/TSB)
# (2, 1) → BindingMode.NON_PEER     # Third input, second element
```

For peered inputs, only the top-level path is stored: `(ndx,) → mode`

For non-peered inputs (TSB/TSL), paths extend to leaf elements: `(ndx, sub_path...) → mode`

## Key Files and Their Roles

### Wiring Layer

**`hgraph/_wiring/_wiring_port.py`**
- `WiringPort.get_binding_mode(input_type)` - Determines binding mode for a single port
- `WiringPort.get_binding_modes_by_path(base_path, input_type)` - Collects modes for all leaf paths
- `TSBWiringPort.get_binding_modes_by_path()` - Recursive collection for bundles
- `TSLWiringPort.get_binding_modes_by_path()` - Recursive collection for lists

**`hgraph/_wiring/_wiring_node_signature.py`**
- `WiringNodeSignature.input_binding_modes` - Stores path→mode mapping
- `copy_with()` - Creates signature copy with binding modes attached

**`hgraph/_wiring/_wiring_node_instance.py`**
- `WiringNodeInstance.input_binding_modes` - Computed property that collects all binding modes
- `create_node_builder_and_edges()` - Passes binding modes to builder creation

### Builder Layer

**`hgraph/_builder/_ts_builder.py`** (abstract interfaces)
- Base builder classes define the interface
- `TimeSeriesBuilderFactory.make_input_builder(value_tp, input_binding_modes)`

**`hgraph/_impl/_builder/_ts_builder.py`** (Python implementation)
- `PythonTimeSeriesBuilderFactory` - Factory that creates type-specific builders
- `_make_ref_input_builder(ref_tp, binding_mode)` - Creates REF builders with binding mode
- `_make_input_builder_with_binding_mode()` - Internal helper for TSB field builders

Type-specific REF input builders (each accepts `binding_mode` parameter):
- `PythonTSREFInputBuilder` - For `REF[TS[...]]`
- `PythonTSDREFInputBuilder` - For `REF[TSD[...]]`
- `PythonTSSREFInputBuilder` - For `REF[TSS[...]]`
- `PythonTSWREFInputBuilder` - For `REF[TSW[...]]`
- `PythonTSLREFInputBuilder` - For `REF[TSL[...]]`
- `PythonTSBREFInputBuilder` - For `REF[TSB[...]]`

### Runtime Layer

**`hgraph/_impl/_types/_ref.py`**
- Base reference input classes (handle both binding modes at runtime)
- Stub classes for future optimization:

```python
# Peered stubs (REF→REF bindings)
PythonPeeredTSReferenceInput
PythonPeeredTSDReferenceInput
PythonPeeredTSSReferenceInput
PythonPeeredTSWReferenceInput
PythonPeeredTSLReferenceInput
PythonPeeredTSBReferenceInput

# Non-peered stubs (REF→X bindings)
PythonNonPeeredTSReferenceInput
PythonNonPeeredTSDReferenceInput
PythonNonPeeredTSSReferenceInput
PythonNonPeeredTSWReferenceInput
PythonNonPeeredTSLReferenceInput
PythonNonPeeredTSBReferenceInput
```

Currently, these stubs inherit from their parent classes and delegate all behavior. The infrastructure exists for implementing optimized behavior.

## Builder Selection Logic

Each type-specific builder uses the binding mode to select the appropriate runtime class:

```python
@dataclass(frozen=True)
class PythonTSREFInputBuilder(PythonInputBuilder, REFInputBuilder):
    value_tp: "HgTimeSeriesTypeMetaData"
    binding_mode: "BindingMode" = None

    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph._builder._graph_builder import BindingMode
        from hgraph._impl._types._ref import (
            PythonPeeredTSReferenceInput,
            PythonNonPeeredTSReferenceInput,
        )

        # Select class based on binding mode
        if self.binding_mode is not None and self.binding_mode & BindingMode.REF_NON_PEER:
            cls = PythonNonPeeredTSReferenceInput
        else:
            cls = PythonPeeredTSReferenceInput

        return cls[self.value_tp.py_type](
            _parent_or_node=owning_input if owning_input is not None else owning_node
        )
```

## TSB Field Builder Selection

For `TSB` (bundle) inputs, binding modes are passed down to field builders:

```python
@dataclass(frozen=True)
class PythonTSBInputBuilder(PythonInputBuilder, TSBInputBuilder):
    schema: "TimeSeriesSchema"
    input_binding_modes: Mapping[tuple, "BindingMode"] | None = None

    def __post_init__(self):
        factory = TimeSeriesBuilderFactory.instance()
        binding_modes = self.input_binding_modes or {}

        def make_field_builder(field_ndx: int, field_name: str, field_type):
            # Look up by path tuple (field_ndx,) for top-level fields
            binding_mode = binding_modes.get((field_ndx,), BindingMode.PEER)
            return factory._make_input_builder_with_binding_mode(field_type, binding_mode)

        # Create field builders with appropriate binding modes
        ...
```

## Implementing Optimizations

### Python Optimization (Future)

To optimize the peered case in Python:

```python
@dataclass
class PythonPeeredTSReferenceInput(PythonTimeSeriesValueReferenceInput):
    """Optimized for REF→REF bindings - no wrapping needed."""

    def bind_output(self, output: TimeSeriesOutput):
        # Skip type checking - we know it's a REF output
        # Skip BoundTimeSeriesReference wrapping
        self._reference = output.value  # Direct reference access
        ...
```

### C++ Implementation

The same pattern applies to C++:

1. **Define binding mode enum** in C++ headers
2. **Store binding modes** in node signature or builder
3. **Create specialized C++ classes** for peered/non-peered cases
4. **Builder selects class** based on binding mode

Example C++ structure:
```cpp
// Peered variant - optimized for REF→REF
class PeeredTSReferenceInput : public TimeSeriesReferenceInput {
    void bind_output(TimeSeriesOutput* output) override {
        // Direct binding, no wrapping
        _reference = static_cast<ReferenceOutput*>(output)->value();
    }
};

// Non-peered variant - handles REF→X with wrapping
class NonPeeredTSReferenceInput : public TimeSeriesReferenceInput {
    void bind_output(TimeSeriesOutput* output) override {
        // Wrap in BoundTimeSeriesReference
        _reference = make_bound_reference(output);
    }
};
```

## Testing

The stub infrastructure is tested by the existing test suite:

```bash
# Python runtime
HGRAPH_USE_CPP=0 uv run pytest hgraph_unit_tests/_wiring/test_ref.py -v

# C++ runtime
HGRAPH_USE_CPP=1 uv run pytest hgraph_unit_tests/_wiring/test_ref.py -v
```

Key test cases:
- `test_merge_ref` - Peered REF binding
- `test_merge_ref_non_peer` - Non-peered REF binding
- `test_merge_ref_non_peer_complex_inner_ts` - Nested non-peered
- `test_free_bundle_ref` - TSB with REF fields

## Complexity Assessment

This experiment demonstrated that the architecture is manageable:

1. **Wiring changes**: Minimal - mostly adding `get_binding_modes_by_path()` methods
2. **Signature changes**: One new field (`input_binding_modes`)
3. **Builder changes**: Each type-specific builder gains one parameter and selection logic
4. **Runtime changes**: Stub classes (12 total) that can be optimized incrementally

The binding mode information flows naturally through existing abstractions without requiring major refactoring.

## Limitations and Considerations

1. **Binding mode must be 100% accurate**: Once optimized builders are used, there's no runtime fallback. The wiring-time detection must be correct.

2. **Generic REF builder still exists**: For unknown/fallback types, `PythonREFInputBuilder` handles both modes at runtime.

3. **C++ factory compatibility**: The C++ factory's `make_input_builder` accepts `input_binding_modes=None` for API compatibility but doesn't use it yet.

4. **Incremental optimization**: Stubs can be optimized one type at a time. Start with the most common case (`REF[TS[...]]`) and expand.

## Summary

| Component | File | Key Changes |
|-----------|------|-------------|
| Binding mode detection | `_wiring_port.py` | `get_binding_mode()`, `get_binding_modes_by_path()` |
| Mode storage | `_wiring_node_signature.py` | `input_binding_modes` field |
| Mode collection | `_wiring_node_instance.py` | `input_binding_modes` property |
| Builder factory | `_ts_builder.py` | `_make_ref_input_builder(ref_tp, binding_mode)` |
| Type-specific builders | `_ts_builder.py` | `binding_mode` parameter on 6 builders |
| Runtime stubs | `_ref.py` | 12 stub classes (peered/non-peered x 6 types) |
