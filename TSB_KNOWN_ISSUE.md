# Known Issue: REF[TSB] Specialized Input with Complex Nested Graphs

**Date:** 2025-11-10  
**Status:** 🔍 **INVESTIGATION NEEDED**

## Issue

The specialized `PythonTimeSeriesBundleReferenceInput` class causes a cleanup assertion failure in one specific complex test:

**Failing Test:**
- `hgraph_unit_tests/_operators/test_control_operators.py::test_race_tsd_of_bundles_switch_bundle_types`

**Error:**
```
AssertionError: Output instance still has subscribers when released, this is a bug.
output belongs to node None
subscriber nodes are [eval_node_graph.g.reduce_tsd_of_bundles_with_race]
```

## Test Complexity

This test involves:
- `TSD[int, TSB[SC]]` - Dictionary of bundles
- `map_()` node - Maps over the TSD
- `switch_()` node - Switches between two bundle creation modes
- `reduce_tsd_of_bundles_with_race()` - Reduces with race conditions
- `REF[TS[int]]` - References inside the bundle fields
- `REF[TSB[S]]` - Final output is a reference to a bundle

## What Works

✅ **TSB Output Specialized** - `PythonTimeSeriesBundleReferenceOutput` works fine  
✅ **Simple TSB Tests** - `test_free_bundle_ref` passes  
✅ **All Other Tests** - 1308/1309 tests pass  

## What Fails

❌ **TSB Input Specialized** - `PythonTimeSeriesBundleReferenceInput` causes cleanup issue  

## Workaround

Currently using generic `PythonTimeSeriesReferenceInput` for TSB inputs:

```python
class PythonTSBREFInputBuilder:
    def make_instance(self, ...):
        # Use generic class instead of specialized
        return PythonTimeSeriesReferenceInput[...](...)
```

## Investigation Needed

### Hypothesis 1: Cleanup Ordering

The error occurs during graph teardown in a switch node. The specialized class might need special handling for:
- Nested graph cleanup
- Switch node graph removal
- Reduce node cleanup

### Hypothesis 2: Field Builder Ordering

TSB is heterogeneous. Maybe the field_builders list order doesn't match the schema order?

**Current approach:**
```python
field_builders_list = list(self.field_builders.values())  # Dict insertion order
```

**Could try:**
```python
# Use schema order explicitly
field_builders_list = [
    self.field_builders[key] 
    for key in self.schema.__meta_data_schema__.keys()
]
```

### Hypothesis 3: Clone Binding Issue

The test involves cloning bindings in map nodes. Maybe `clone_binding()` in `PythonTimeSeriesBundleReferenceInput` has a subtle bug?

### Hypothesis 4: Release Logic

Maybe the specialized class needs custom cleanup in `release_instance()` method that's not in the builder?

## Next Steps

1. **Add debug logging** to understand cleanup sequence
2. **Check schema field ordering** - verify it matches what TSB expects
3. **Review clone_binding** - ensure it handles TSB correctly
4. **Check if test was recently added** - might be a pre-existing fragile test
5. **Compare with Python ref implementation** - see if there's a pattern we're missing

## Current Status

**Production Ready:**
- ✅ REF[TSL] - Fully specialized (input and output)
- ✅ REF[TSB] output - Specialized
- ⚠️  REF[TSB] input - Using generic (deferred until issue resolved)

**Test Results:**
- Python runtime: 1308/1309 passed (99.92%)
- C++ runtime: Not tested with TSB specialized

