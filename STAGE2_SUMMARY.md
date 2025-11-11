# Stage 2 Complete: Specialized Builders ✅

**Date:** 2025-11-10  
**Status:** ✅ **COMPLETE** (Python implementation)

## What Was Accomplished

Successfully created specialized builder classes that:
1. **Store structure information** (size for TSL, schema for TSB)
2. **Store child builders** (to create properly typed children)
3. **Pre-allocate children for TSL** (list-based structure)
4. **Prepare for TSB** (schema and field builders stored, but pre-allocation deferred to Stage 3)

## Changes Made

### New Builder Classes (`hgraph/_impl/_builder/_ts_builder.py`)

#### 1. PythonTSLREFInputBuilder
```python
@dataclass(frozen=True)
class PythonTSLREFInputBuilder(PythonInputBuilder, REFInputBuilder):
    value_tp: "HgTimeSeriesTypeMetaData"
    value_builder: TSInputBuilder  # Builder for child references
    size_tp: "HgScalarTypeMetaData"
    
    def make_instance(self, owning_node=None, owning_input=None):
        # Creates PythonTimeSeriesReferenceInput with pre-allocated _items list
        ref._items = [
            self.value_builder.make_instance(owning_input=ref)
            for _ in range(size)
        ]
```

**Benefits:**
- ✅ Pre-allocates all children at construction
- ✅ Children have correct types (recursively created reference builders)
- ✅ No more lazy creation complexity
- ✅ Proper cleanup in `release_instance()`

#### 2. PythonTSBREFInputBuilder
```python
@dataclass(frozen=True)
class PythonTSBREFInputBuilder(PythonInputBuilder, REFInputBuilder):
    value_tp: "HgTimeSeriesTypeMetaData"
    schema: "TimeSeriesSchema"
    field_builders: Mapping[str, TSInputBuilder]
    
    def make_instance(self, owning_node=None, owning_input=None):
        # Stores schema and field_builders for future use
        # Pre-allocation deferred to Stage 3 (needs specialized class)
```

**Benefits:**
- ✅ Stores schema information
- ✅ Stores field builders with correct types
- ✅ Ready for Stage 3 specialized classes

#### 3. Output Builders
- `PythonTSLREFOutputBuilder` - stores size information
- `PythonTSBREFOutputBuilder` - stores schema information

### Updated Factory Methods

#### `_make_ref_input_builder()`
```python
if isinstance(referenced_tp, HgTSLTypeMetaData):
    # Create specialized builder with child references
    child_ref_tp = HgREFTypeMetaData(referenced_tp.value_tp)
    child_builder = self._make_ref_input_builder(child_ref_tp)  # Recursion!
    
    return PythonTSLREFInputBuilder(
        value_tp=referenced_tp,
        value_builder=child_builder,
        size_tp=referenced_tp.size_tp
    )
```

**Key Feature:** Recursive builder creation for nested references like `REF[TSL[REF[TS[int]], Size[2]]]`

## Testing

All 14 tests pass:

```bash
cd /Users/hhenson/cursor/hgraph
uv run pytest hgraph_unit_tests/_wiring/test_ref.py -v
```

**Results:**
```
test_ref PASSED
test_route_ref PASSED
test_merge_ref PASSED
test_merge_ref_non_peer PASSED
test_merge_ref_non_peer_complex_inner_ts PASSED  ← Uses TSL refs
test_merge_ref_inner_non_peer_ts PASSED          ← Uses TSL refs
test_merge_ref_set PASSED
test_merge_ref_set1 PASSED
test_merge_ref_set2 PASSED
test_merge_ref_set3 PASSED
test_tss_ref_contains PASSED
test_merge_with_tsd PASSED
test_merge_tsd PASSED
test_free_bundle_ref PASSED                       ← Uses TSB refs
```

## What's Working

### ✅ REF[TSL[...]] - Fully Implemented
- Pre-allocated children
- Recursive builder creation
- Type-safe child references
- Proper cleanup

### ✅ REF[TSB[...]] - Partially Implemented
- Schema and field builders stored
- Still uses generic `PythonTimeSeriesReferenceInput`
- Pre-allocation deferred to Stage 3

### ✅ REF[TS], REF[TSD], etc. - Unchanged
- Still use generic `PythonREFInputBuilder`
- Working as before

## Benefits Already Achieved

### For REF[TSL]

**Before Stage 2:**
```python
ref_input._items = None  # Lazy creation on-demand
```

**After Stage 2:**
```python
# Pre-allocated at construction with correct types
ref_input._items = [
    REF[TS[int]] instance,  # Child 0
    REF[TS[int]] instance,  # Child 1
    REF[TS[int]] instance,  # Child 2
]
```

**Benefits:**
1. ✅ **No lazy creation** - All children exist at construction
2. ✅ **Type safety** - Each child is a proper reference input
3. ✅ **Known size** - `len(ref_input._items)` always equals declared size
4. ✅ **Proper cleanup** - `release_instance()` releases all children
5. ✅ **Foundation for validation** - Can check size matches at binding time

### For REF[TSB]

**Benefits:**
1. ✅ **Schema preserved** - Know field names at construction
2. ✅ **Type information** - Have builders for each field
3. ✅ **Ready for Stage 3** - When we create specialized classes

## Known Limitations

### REF[TSB] Structure

The generic `PythonTimeSeriesReferenceInput` uses `_items` as a list for lazy creation:

```python
def __getitem__(self, item):
    if self._items is None:
        self._items = []
    while item > len(self._items) - 1:  # Expects list with integer index
        new_item = PythonTimeSeriesReferenceInput(_parent_or_node=self)
        self._items.append(new_item)
    return self._items[item]
```

But TSB needs `_items` as a dict:
```python
ref_input._items = {
    'a': REF[TS[int]] instance,
    'b': REF[TS[str]] instance,
}
```

**Solution:** Stage 3 will create `PythonTimeSeriesBundleReferenceInput` with dict-based structure.

## Next Steps: Stage 3

Create specialized input/output classes:

### 1. PythonTimeSeriesListReferenceInput
```python
@dataclass
class PythonTimeSeriesListReferenceInput(TimeSeriesReferenceInput):
    _size: int  # Known at construction
    _items: list[TimeSeriesReferenceInput]  # Pre-allocated, NOT optional
    
    def bind_output(self, output):
        if len(output) != self._size:
            raise TypeError(f"Size mismatch: expected {self._size}, got {len(output)}")
        # Bind each child...
```

### 2. PythonTimeSeriesBundleReferenceInput
```python
@dataclass  
class PythonTimeSeriesBundleReferenceInput(TimeSeriesReferenceInput):
    _schema: TimeSeriesSchema
    _items: dict[str, TimeSeriesReferenceInput]  # Pre-allocated
    
    def __getattr__(self, item):
        return self._items[item]  # Access by field name
```

### 3. Update Builders
```python
class PythonTSBREFInputBuilder:
    def make_instance(self, ...):
        # Create PythonTimeSeriesBundleReferenceInput instead
        ref = PythonTimeSeriesBundleReferenceInput(...)
        ref._items = {
            key: builder.make_instance(owning_input=ref)
            for key, builder in self.field_builders.items()
        }
```

## Code Location

**Modified file:**
- `/Users/hhenson/cursor/hgraph/hgraph/_impl/_builder/_ts_builder.py`

**Lines added:**
- Lines 396-498: New specialized builder classes
- Lines 547-599: Updated factory methods

## Summary

Stage 2 is **complete for TSL references** and provides the foundation for TSB references. The specialized builders successfully:

1. ✅ Store structure information (size, schema)
2. ✅ Store typed child builders
3. ✅ Pre-allocate children for TSL (working in tests)
4. ✅ Prepare for TSB pre-allocation (deferred to Stage 3)
5. ✅ Maintain backward compatibility (all tests pass)

The architecture is now in place for Stage 3, where we'll create specialized classes that fully leverage these builders.

---

**Status Matrix:**

| Feature | Stage 1 | Stage 2 | Stage 3 |
|---------|---------|---------|---------|
| Detect REF[TSL] | ✅ | ✅ | ✅ |
| Detect REF[TSB] | ✅ | ✅ | ✅ |
| Store size info | ❌ | ✅ | ✅ |
| Store schema info | ❌ | ✅ | ✅ |
| Pre-allocate TSL children | ❌ | ✅ | ✅ |
| Pre-allocate TSB fields | ❌ | ⚠️ Partial | ✅ |
| Specialized TSL class | ❌ | ❌ | 🔲 |
| Specialized TSB class | ❌ | ❌ | 🔲 |
| Type-safe binding | ❌ | ❌ | 🔲 |
| Size/schema validation | ❌ | ❌ | 🔲 |

**Ready for Stage 3 when you are!**

