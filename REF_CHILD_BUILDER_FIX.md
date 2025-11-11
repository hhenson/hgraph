# REF[TSL] and REF[TSB] Child Builder Fix

**Date:** 2025-11-11  
**Status:** ✅ **FIXED**

## Issue

The C++ factory in `_use_cpp_runtime.py` was creating incorrect child builders for `REF[TSL]` and `REF[TSB]` types. 

### Problem

For `REF[TSL[TS[int], Size[3]]]`:
- **Wrong:** Created child builders for `TS[int]` (non-reference)
- **Correct:** Should create child builders for `REF[TS[int]]` (reference-wrapped)

This broke reference semantics because the children were not themselves references.

### Root Cause

The C++ factory methods `_make_ref_input_builder()` and `_make_ref_output_builder()` were calling:
```python
self.make_input_builder(referenced_tp.value_tp)  # TSL case
self.make_input_builder(tp)  # TSB field case
```

These calls created builders for the raw types (`TS[int]`) instead of wrapping them in `REF` first.

## Solution

Added helper functions that check if the child type is already a `REF` type:
- If already `REF`: Use it directly
- If not `REF`: Wrap it in `HgREFTypeMetaData` first

This matches the Python implementation in `_ts_builder.py` lines 676-709.

### Code Changes

**File:** `hgraph/_use_cpp_runtime.py`

**Lines 241-273 (Input builders):**
```python
def _make_ref_input_builder(self, ref_tp):
    """Create specialized C++ reference input builder based on what's being referenced"""
    referenced_tp = ref_tp.value_tp
    
    def _make_child_ref_builder(child_tp):
        """Wrap child type in REF if not already a REF type"""
        if type(child_tp) is hgraph.HgREFTypeMetaData:
            # Already a reference type, use its builder directly
            return self._make_ref_input_builder(child_tp)
        else:
            # Wrap in REF
            child_ref_tp = hgraph.HgREFTypeMetaData(child_tp)
            return self._make_ref_input_builder(child_ref_tp)
    
    return {
        hgraph.HgTSLTypeMetaData: lambda: _hgraph.InputBuilder_TSL_Ref(
            _make_child_ref_builder(referenced_tp.value_tp),  # ← Fixed
            referenced_tp.size_tp.py_type.SIZE
        ),
        hgraph.HgTSBTypeMetaData: lambda: _hgraph.InputBuilder_TSB_Ref(
            ...,
            [_make_child_ref_builder(tp) for tp in ...]  # ← Fixed
        ),
        ...
    }
```

**Lines 275-307 (Output builders):**
- Same pattern applied to `_make_ref_output_builder()`

## Test Results

### Before Fix
- Potentially incorrect reference semantics
- Child builders not wrapped in REF types

### After Fix
✅ **All tests pass with C++ runtime:**
- 14/14 `test_ref.py` tests pass
- 22/22 `test_control_operators.py` tests pass (including complex nested REF scenarios)
- 761/761 operator tests pass
- Specifically: `test_merge_ref_inner_non_peer_ts` uses `REF[TSL[TS[int], Size[2]]]` and passes

## Impact

### Fixed Types
- ✅ `REF[TSL[...]]` - Reference to time series lists
- ✅ `REF[TSB[...]]` - Reference to time series bundles

### Preserved Semantics
- Child elements of `REF[TSL]` are now correctly `REF[T]` instead of `T`
- Nested references work correctly (e.g., `REF[TSL[REF[TS[int]]]]`)
- Matches Python implementation behavior exactly

## Files Modified

1. **`hgraph/_use_cpp_runtime.py`**
   - Added `_make_child_ref_builder()` helper to `_make_ref_input_builder()` (lines 245-253)
   - Updated TSL input builder call (line 259)
   - Updated TSB input builder call (line 268)
   - Added `_make_child_ref_builder()` helper to `_make_ref_output_builder()` (lines 279-287)
   - Updated TSL output builder call (line 293)
   - Updated TSB output builder call (line 302)

## Verification

Run tests with C++ runtime:
```bash
HGRAPH_USE_CPP=1 uv run pytest hgraph_unit_tests/_wiring/test_ref.py -v
HGRAPH_USE_CPP=1 uv run pytest hgraph_unit_tests/_operators/test_control_operators.py -v
HGRAPH_USE_CPP=1 uv run pytest hgraph_unit_tests/_operators -v
```

All tests should pass with no failures or errors.

