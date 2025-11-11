# TSB Specialized Class Debugging - Session Complete

## Goal
Enable and debug the specialized `PythonTimeSeriesBundleReferenceInput` class to understand why it fails while the generic version works.

## What Was Done

### 1. Added Comprehensive Tracing

Added detailed tracing to both generic and specialized implementations:

**Files Modified:**
- `hgraph/_impl/_types/_ref.py`:
  - Generic `PythonTimeSeriesReferenceInput`: `do_bind_output`, `do_un_bind_output`, `__getitem__`
  - Specialized `PythonTimeSeriesBundleReferenceInput`: `do_bind_output`, `do_un_bind_output`, `__getitem__`

**Trace Control:**
- Enable with: `TSB_TRACE=1` environment variable
- Traces show: bind/unbind operations, child creation, output types, IDs

**Trace Files Generated:**
- `/tmp/gen_trace_full.txt` - Generic version (passing)  
- `/tmp/spec_trace_full.txt` - Specialized version (failing)

### 2. Ran Comparative Analysis

Executed the failing test (`test_race_tsd_of_bundles_switch_bundle_types`) with both implementations and captured full traces.

### 3. Root Cause Identified

**The Problem:** Cross-graph subscriptions

**Generic Version:**
```
[GEN bind] output_type=PythonTimeSeriesBundleOutput
[GEN bind] → Non-peered binding, creating value wrapper
```
- Wraps entire bundle: `self._value = TimeSeriesReference.make(output)`
- No children created during binding
- No subscriptions to individual elements

**Specialized Version:**
```
[SPEC bind] → Creating 4 children
[SPEC bind] → Binding 4 children to output elements
[SPEC bind]   Binding child[0] to output[0] type=PythonTimeSeriesValueOutput
```
- Creates children eagerly in `do_bind_output`
- Binds each child to bundle output elements: `child.bind_output(output[i])`
- **Creates subscriptions from parent graph to nested graph outputs**

**Why This Fails:**

When a switch node destroys a nested graph:
1. The nested graph tries to release its outputs
2. Those outputs have subscribers from OUTSIDE the graph (the specialized TSB's children)
3. Cleanup assertion fails: `Output instance still has subscribers when released`

The generic version doesn't have this problem because it never creates direct subscriptions to the output elements - it just wraps the whole bundle as a single reference.

### 4. Attempted Fix

Tried making specialized TSB behave like generic by only wrapping the output without binding children:

```python
# In do_bind_output for non-peered binding:
self._value = TimeSeriesReference.make(output)
self._output = None
# Don't create or bind children
```

**Result:** Broke 4 tests (worse than before)

The fix broke functionality because some code expects children to exist and provide typed access to bundle elements.

## The Fundamental Problem

The specialized TSB has conflicting requirements:

1. ✅ Provide type-safe, heterogeneous access to bundle elements
2. ✅ Work with both peered and non-peered binding modes  
3. ❌ **NOT create cross-graph subscriptions**

Requirement #3 conflicts with the current approach to achieving #1 and #2.

## Current Solution

**Status:** Specialized TSB remains **DISABLED**, using generic fallback.

**Test Results:** ✅ All 1309 tests pass

**Code Location:**
- `hgraph/_impl/_builder/_ts_builder.py` lines 434-443
- Comment added: `# Use generic - specialized TSB creates cross-graph subscriptions`

## Future Solutions

Three possible approaches documented in `TSB_TRACE_FINDINGS.md`:

### Option 1: Lazy Children with Value Access (Best)
Children act as "views" into the wrapped bundle, delegating to `parent._value[i]` instead of having their own bindings.

**Pros:** Preserves type safety, avoids cross-graph subscriptions
**Cons:** Requires significant refactoring of how children access values

### Option 2: Specialized Only for Peered Mode (Safest)
Use specialized class only when binding REF → REF (peered). Use generic for REF → TSB (non-peered).

**Pros:** Simple, safe
**Cons:** Loses type benefits for non-peered cases

### Option 3: Different Binding Strategy (Complex)
Bind children lazily to copies/wrappers that don't create subscriptions.

**Pros:** Might work
**Cons:** Complex, may not align with framework design

## Documentation

Created comprehensive documentation:
1. **`TSB_TRACE_FINDINGS.md`** - Detailed analysis with traces and solutions
2. **`TSB_DEBUG_SESSION_COMPLETE.md`** (this file) - Session summary
3. Inline code comments explaining the issue

## Tracing Code

The tracing code is still in place and can be used for future debugging:
- Set `TSB_TRACE=1` to enable
- Traces show bind/unbind patterns, child creation, subscriptions
- Format: `[GEN/SPEC] [operation] [details]`

## Conclusion

**Key Insight:** The specialized TSB's eager child creation and binding strategy creates cross-graph subscriptions that violate the framework's graph lifecycle assumptions. The generic version avoids this by treating the entire bundle as an opaque reference.

**Current State:** Using generic TSB fallback. All tests pass. TSL specialized class works perfectly.

**Next Steps:** Implement Option 1 (lazy children with value access) to enable specialized TSB without cross-graph subscriptions.

## Test Status Summary

- **Generic TSB:** ✅ 1309 tests pass
- **Specialized TSB (original):** ❌ 1 test fails (cross-graph subscription)
- **Specialized TSB (fix attempt):** ❌ 4 tests fail (broke functionality)
- **Current (generic fallback):** ✅ 1309 tests pass
- **TSL specialized:** ✅ Working perfectly (no similar issues)

