# TSB Specialized Class - Trace Analysis Findings

## Root Cause Identified

The specialized `PythonTimeSeriesBundleReferenceInput` creates a fundamentally different binding pattern than the generic class, which causes cleanup issues.

### Generic TSB REF Behavior

```
[GEN bind] output_type=PythonTimeSeriesBundleOutput, is_ref=False
[GEN bind] → Non-peered binding, creating value wrapper
```

- Wraps the ENTIRE bundle output as a reference: `self._value = TimeSeriesReference.make(output)`
- Sets `self._output = None` (non-peered)
- Does NOT create children
- Does NOT create subscriptions to individual elements
- When accessed via `__getitem__`, creates children lazily but they're not bound to anything

### Specialized TSB REF Behavior (ORIGINAL)

```
[SPEC bind] → Non-peered binding to PythonTimeSeriesBundleOutput with 4 fields
[SPEC bind] → Creating 4 children
[SPEC bind] → Binding 4 children to output elements
[SPEC bind]   Binding child[0] to output[0] type=PythonTimeSeriesValueOutput
[GEN bind]  → Non-peered binding, creating value wrapper
```

- Creates children eagerly in `do_bind_output`
- Binds each child DIRECTLY to bundle output elements: `child.bind_output(output[i])`
- This creates subscriptions from children (in parent graph) to outputs (in nested graph)

### The Problem

When a switch node's nested graph is destroyed:
1. It tries to release the bundle outputs
2. Those outputs still have subscribers (the specialized TSB's children from outside the graph)
3. The cleanup fails with: `AssertionError: Output instance still has subscribers when released`

In the generic case, there are NO direct subscriptions to the outputs because the entire bundle is wrapped as a single reference.

### Why Specialized TSB Created Children

The specialized TSB was trying to be "smart" by:
1. Pre-creating children during binding
2. Binding them to output elements for direct access
3. Providing type-specific access patterns

But this creates **cross-graph subscriptions** that violate the graph lifecycle assumptions.

## Attempted Fix #1: Don't Bind Children

Changed `do_bind_output` to just wrap the entire output like the generic version:

```python
self._value = TimeSeriesReference.make(output)
self._output = None
# Don't create or bind children
```

**Result:** 4 test failures (worse than before)

The fix broke functionality because:
- Some code expects children to exist and be accessible
- Children need to provide typed access to bundle elements
- Just wrapping the bundle doesn't preserve the type-specific behavior

## The Fundamental Challenge

The specialized TSB needs to:
1. ✅ Provide type-safe, heterogeneous access to bundle elements
2. ✅ Work with both peered and non-peered binding modes
3. ❌ **NOT create cross-graph subscriptions** (this is the blocker)

But requirement #3 conflicts with how the current implementation achieves #1 and #2.

## Possible Solutions

###  Option 1: Lazy Children with Value Access (Not Binding)

Children should:
- Be created lazily on first `__getitem__` access
- NOT be bound to outputs
- Instead, access values through the parent's `_value` property
- Act as "views" into the wrapped bundle, not as separate bindings

This requires changing how children get their values - they would need to delegate to `parent._value[i]` instead of having their own bindings.

### Option 2: Only Use Specialized for Peered Binding

Only use `PythonTimeSeriesBundleReferenceInput` when binding to another REF output (peered mode). For non-peered binding to actual bundles, fall back to generic.

This is simpler but loses the type benefits for non-peered cases.

### Option 3: Different Binding Strategy

Instead of binding children to outputs during `do_bind_output`, bind them lazily when accessed, but to a COPY or WRAPPER of the output that doesn't create subscriptions.

This is complex and might not align with the framework's design.

## Recommendation

**Option 1** is the most promising but requires significant refactoring of how children access values. The children would need to be "virtual" - providing typed access without actual bindings.

**Option 2** is the safest short-term solution - use specialized only for peered mode, which is what most reduce operations use anyway.

## Test Status

- **Generic TSB (current fallback):** ✅ 1309 tests pass
- **Specialized TSB (original):** ❌ 1 test fails (cross-graph subscription issue)
- **Specialized TSB (fix attempt #1):** ❌ 4 tests fail (broke functionality)

## Files with Tracing

Comprehensive tracing was added to:
- Generic `PythonTimeSeriesReferenceInput`: bind/unbind/getitem
- Specialized `PythonTimeSeriesBundleReferenceInput`: bind/unbind/getitem

Traces saved to:
- `/tmp/gen_trace_full.txt` - Generic version (passing)
- `/tmp/spec_trace_full.txt` - Specialized version (failing)

Enable with: `TSB_TRACE=1` environment variable

