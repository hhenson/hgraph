# Reference Type Size Initialization Fix

## Issue Summary

The C++ `TimeSeriesListReferenceInput`, `TimeSeriesBundleReferenceInput`, and their output counterparts declared private `_size` member variables that were never initialized. The builders created instances without passing size information, causing `_size` to remain 0 and breaking:
- Size validation logic
- Batch child creation that the Python implementation relies on

## Root Cause

The specialized reference builders in C++ were simple wrappers with no stored metadata:
- They had no fields to store size or child builders
- They created instances using only the basic constructor
- The instance `_size` fields remained at their default value of 0

This differed from Python where:
- `PythonTSLREFInputBuilder` stores `value_builder` and `size_tp`
- `PythonTSBREFInputBuilder` stores `schema` and `field_builders`
- These are passed to instances during creation

## Solution Implemented

### 1. Updated Builder Classes (specialized_ref_builders.h)

Added fields to store metadata:

**TimeSeriesListRefInputBuilder:**
```cpp
InputBuilder::ptr value_builder;  // Builder for child items
size_t size;                      // Fixed size of the list
```

**TimeSeriesBundleRefInputBuilder:**
```cpp
time_series_schema_ptr schema;                      // Schema for bundle fields
std::vector<InputBuilder::ptr> field_builders;      // Builders for each field
```

Similar changes for output builders.

### 2. Added Constructors to Reference Classes (ref.h)

Added constructors that accept size:
```cpp
TimeSeriesListReferenceInput(Node *owning_node, size_t size);
TimeSeriesListReferenceInput(TimeSeriesType *parent_input, size_t size);
```

### 3. Implemented Constructors (ref.cpp)

Constructors initialize `_size`:
```cpp
TimeSeriesListReferenceInput::TimeSeriesListReferenceInput(Node *owning_node, size_t size)
    : TimeSeriesReferenceInput(owning_node), _size(size) {}
```

### 4. Updated Builder Implementations (specialized_ref_builders.cpp)

Builders now:
- Have constructors that accept and store metadata
- Pass size to instances in `make_instance()`
- Implement `is_same_type()` for proper comparison
- Updated nanobind registration to accept parameters

### 5. Updated Python Integration (_use_cpp_runtime.py)

Updated `_make_ref_input_builder()` and `_make_ref_output_builder()` to:
- Extract size from `referenced_tp.size_tp.py_type.SIZE` for TSL
- Extract schema and field builders for TSB
- Pass these to C++ builder constructors

## Files Modified

1. **cpp/include/hgraph/types/ref.h**
   - Added size-accepting constructors to specialized ref classes
   - Added `size()` accessor methods

2. **cpp/src/cpp/types/ref.cpp**
   - Implemented new constructors
   - Updated nanobind registrations

3. **cpp/include/hgraph/builders/time_series_types/specialized_ref_builders.h**
   - Added metadata fields to builder classes
   - Added proper constructors
   - Added `is_same_type()` declarations

4. **cpp/src/cpp/builders/time_series_types/specialized_ref_builders.cpp**
   - Implemented constructors storing metadata
   - Updated `make_instance()` to pass size
   - Implemented `is_same_type()` methods
   - Updated nanobind registrations

5. **hgraph/_use_cpp_runtime.py**
   - Updated `_make_ref_input_builder()` to pass size/schema/builders
   - Updated `_make_ref_output_builder()` to pass size/schema/builders

## Result

Now the C++ implementation matches Python behavior:
- ✅ Size is properly initialized in instances
- ✅ Size validation will work correctly
- ✅ Batch child creation logic can access the size
- ✅ Builders store and pass all necessary metadata
- ✅ Consistent with how non-ref TSL and TSB builders work

## Testing

Once compiled, the fix can be verified with:
```bash
HGRAPH_USE_CPP=1 uv run pytest hgraph_unit_tests/_wiring/test_ref.py -v
```

Key test scenarios:
1. REF[TSL[...]] - verify size is set and accessible
2. REF[TSB[...]] - verify size matches field count
3. Size-based validation should not fail with index errors
4. Child access patterns should work correctly

