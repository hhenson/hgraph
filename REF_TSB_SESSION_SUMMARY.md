# REF[TSB] Debugging Session Summary

## Goal
Enable the specialized `PythonTimeSeriesBundleReferenceInput` class and resolve any issues.

## What Was Done

### 1. Enabled TSB Specialized Class
- Modified `PythonTSBREFInputBuilder.make_instance()` to create `PythonTimeSeriesBundleReferenceInput`
- Set `_size` and `_field_builders` during instantiation

### 2. Fixed Builder Release Logic
Initial attempts tried to manually unbind and release children in the builder's `release_instance` method. After investigation, discovered that the generic `PythonREFInputBuilder` does NOT release children:

```python
def release_instance(self, item):
    super().release_instance(item)  # That's all!
```

Updated both `PythonTSLREFInputBuilder` and `PythonTSBREFInputBuilder` to match this pattern:
- **Don't** call `un_bind_output` on children
- **Don't** call `release_instance` on children
- Just call `super().release_instance(item)`

This fixed the TSL case completely.

### 3. Discovered TSB-Specific Issue
After matching the generic builder pattern, TSL works perfectly but TSB still fails with:
```
AssertionError: Output instance still has subscribers when released
```

The error occurs during switch node graph cleanup in `test_race_tsd_of_bundles_switch_bundle_types`.

### 4. Isolated the Problem
- ✅ TSL specialized + TSB generic: All 1309 tests pass
- ❌ TSL specialized + TSB specialized: Cleanup assertion fails
- Conclusion: Issue is specific to `PythonTimeSeriesBundleReferenceInput`, not the general approach

### 5. Cleanup
- Removed all debug/tracing code
- Deleted temporary test files
- Documented the issue in `TSB_ISSUE_ANALYSIS.md`

## Current State

### Working ✅
- **TSL specialized class** (`PythonTimeSeriesListReferenceInput`):
  - Handles both peered and non-peered binding
  - Batch creates children on first `__getitem__` access
  - Properly unbinds children in `do_un_bind_output`
  - All tests pass including complex switch/TSD scenarios

### Disabled ⚠️
- **TSB specialized class** (`PythonTimeSeriesBundleReferenceInput`):
  - Code is complete and follows same pattern as TSL
  - Fails one complex test involving switch+TSD+nested bundles
  - Currently disabled (using generic fallback) at:
    - `hgraph/_impl/_builder/_ts_builder.py` lines 434-442

### Files Modified
1. `hgraph/_impl/_types/_ref.py`:
   - `PythonTimeSeriesListReferenceInput`: Fully working
   - `PythonTimeSeriesBundleReferenceInput`: Complete but disabled

2. `hgraph/_impl/_builder/_ts_builder.py`:
   - `PythonTSLREFInputBuilder`: Enabled, working
   - `PythonTSBREFInputBuilder`: Creates generic instead of specialized

## Test Results
```bash
uv run pytest hgraph_unit_tests --tb=no -q
# 1309 passed, 3 skipped, 4 xfailed, 8 xpassed, 15 warnings in 14.94s
```

## Key Learnings

1. **Builder Release Pattern**: REF input builders should NOT release children. This differs from TSL/TSB (non-ref) builders which do release children. The children of references have different lifecycle management.

2. **Peered vs Non-Peered**: The `_items` presence indicates binding mode:
   - `_items is None`: Peered binding (bound to another reference output)
   - `_items is not None`: Non-peered binding (bound to concrete structured output)

3. **TSL vs TSB Complexity**: TSL (homogeneous children) works fine with specialized class. TSB (heterogeneous children) has additional complexity that needs investigation.

## Next Steps (For Future Work)

To fix the TSB issue:
1. Add detailed tracing comparing generic vs specialized behavior
2. Focus on the switch node graph release sequence
3. Check if binding/unbinding order matters for heterogeneous children
4. Verify that all children are properly detached during cleanup
5. Consider if nested bundles (TSB of TSB) need special handling

## Recommendation

**Keep current state**: TSL specialized enabled, TSB using generic fallback. This gives us:
- Performance improvements for `REF[TSL[...]]` (most common case)
- Correct type-specific behavior for lists
- All tests passing
- Clear documentation of the remaining issue

The TSB issue can be addressed in a future debugging session with fresh context.

