# TSB Specialized Reference Input Issue

## Summary

The specialized `PythonTimeSeriesBundleReferenceInput` class causes a cleanup issue in the `test_race_tsd_of_bundles_switch_bundle_types` test, while the `PythonTimeSeriesListReferenceInput` class works correctly.

## Test Results

- **TSL specialized + TSB generic**: ✅ All tests pass (1309 passed)
- **TSL specialized + TSB specialized**: ❌ Cleanup assertion fails

## Error Details

When using the specialized TSB class, the following error occurs during graph cleanup:

```
AssertionError: Output instance still has subscribers when released, this is a bug.
output belongs to node None
subscriber nodes are [eval_node_graph.g.reduce_tsd_of_bundles_with_race]
```

### Context

The error occurs in:
1. A switch node releases a nested graph (graph for key being removed from TSD)
2. That nested graph contains a node with a TSB output
3. That TSB output contains a TS[int] field
4. That field still has a subscriber from the reduce node in the parent graph
5. The subscriber should have been unbound before the output is released

### Subscriber Chain

```
TS[int] Output (in nested graph)
    ↓ still subscribed by
TS[int] Input
    ↓ parent
TSB[S] Input
    ↓ parent  
TSD[int, REF[TSB[S]]] Input
    ↓ parent
REF[TSB[S]] Input (this is the specialized class)
    ↓ parent
reduce_tsd_of_bundles_with_race node (in parent graph)
```

## Differences Between TSL and TSB Classes

Both classes follow the same pattern:
- Dual binding modes (peered vs non-peered)
- On-demand batch creation of `_items` via `__getitem__`
- Unbinding children in `do_un_bind_output`
- NOT releasing children in builder's `release_instance` (matching generic behavior)

### Key Difference

- **TSL**: Homogeneous children (all same type)
  - `_items: list[TimeSeriesReferenceInput]`
  - Single `_value_builder: TSInputBuilder`
  
- **TSB**: Heterogeneous children (different types per field)
  - `_items: list[TimeSeriesReferenceInput]`
  - List of `_field_builders: list[TSInputBuilder]`

## Hypothesis

The issue may be related to:
1. **Binding order**: TSB binds children by index (`output[i]`), which might create inappropriate direct bindings
2. **Nested structures**: The test involves nested bundles (TSB[SC] → TSB[S]), which might expose an issue with how TSB handles nested children
3. **Switch graph lifecycle**: The switch's nested graph lifecycle might interact differently with heterogeneous children

## Investigation Needed

1. Compare how generic TSB creates and binds children vs specialized TSB
2. Check if the generic class has special handling for nested bundles
3. Verify that children are properly unbound when the parent TSB ref is unbound
4. Check if there's a difference in how `_items` ownership works for heterogeneous vs homogeneous children

## Current Status

- TSB specialized class is **disabled** (using generic fallback)
- File: `hgraph/_impl/_builder/_ts_builder.py` line 434-442
- TSL specialized class is **enabled** and working correctly
- All tests pass with this configuration

## Next Steps

To debug this issue:
1. Add comprehensive tracing to both generic and specialized TSB do_bind_output/do_un_bind_output
2. Run the failing test with tracing to compare execution paths
3. Look for differences in when/how children are unbound
4. Check if the issue is specific to switch node graph cleanup or more general

