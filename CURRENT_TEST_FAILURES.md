# Current Test Failures Baseline

**Date**: 2025-11-24  
**Environment**: C++ Runtime Enabled (`HGRAPH_USE_CPP=1`)  
**Total Tests**: 1336  
**Passed**: 1306  
**Failed**: 10  
**Skipped**: 8  
**XFailed**: 4  
**XPassed**: 8  

## Summary

Before starting the shared_ptr migration, we need to track the current test failures to ensure we don't make things worse.

## Failing Tests

### 1. Component Tests (2 failures)

#### `test_record_replay`
- **File**: `hgraph_unit_tests/_wiring/test_component.py::test_record_replay`
- **Error**: `TypeError: Unable to convert function return value to a Python type!`
- **Signature**: `make_instance(self, owning_node: object | None = None, owning_output: object | None = None) -> hgraph::TimeSeriesOutput`
- **Location**: `hgraph/_impl/_operators/_record_replay_in_memory.py:126`
- **Issue**: Output builder `make_instance` returning C++ TimeSeriesOutput that can't be converted to Python type

#### `test_record_recovery`
- **File**: `hgraph_unit_tests/_wiring/test_component.py::test_record_recovery`
- **Error**: Similar to `test_record_replay` (likely same root cause)

### 2. Mesh Tests (6 failures)

#### `test_mesh`
- **File**: `hgraph_unit_tests/_wiring/test_mesh.py::test_mesh`
- **Error**: `hgraph._types._error_type.NodeException`
- **Issue**: Mesh-related functionality failing

#### `test_mesh_2`
- **File**: `hgraph_unit_tests/_wiring/test_mesh.py::test_mesh_2`
- **Error**: `hgraph._types._error_type.NodeException`
- **Issue**: Mesh-related functionality failing

#### `test_mesh_named`
- **File**: `hgraph_unit_tests/_wiring/test_mesh.py::test_mesh_named`
- **Error**: `hgraph._types._error_type.NodeException`
- **Issue**: Mesh-related functionality failing

#### `test_mesh_cycle`
- **File**: `hgraph_unit_tests/_wiring/test_mesh.py::test_mesh_cycle`
- **Error**: `AssertionError`
- **Issue**: Mesh cycle detection/handling issue

#### `test_mesh_removal`
- **File**: `hgraph_unit_tests/_wiring/test_mesh.py::test_mesh_removal`
- **Error**: `hgraph._types._error_type.NodeException`
- **Issue**: Mesh removal functionality failing

#### `test_mesh_object_keys`
- **File**: `hgraph_unit_tests/_wiring/test_mesh.py::test_mesh_object_keys`
- **Error**: `hgraph._types._error_type.NodeException`
- **Issue**: Mesh with object keys failing

### 3. Debug/Inspector Tests (1 failure)

#### `test_inspector_sort_key`
- **File**: `hgraph_unit_tests/debug/test_inspector.py::test_inspector_sort_key`
- **Error**: `hgraph._types._error_type.NodeException` (likely)
- **Issue**: Inspector functionality failing

### 4. Example Tests (1 failure)

#### `test_examples`
- **File**: `hgraph_unit_tests/test_examples.py::test_examples`
- **Error**: `TypeError: Unable to convert function return value to a Python type!`
- **Issue**: Similar to component test failures - output builder conversion issue

## Common Error Patterns

### Pattern 1: Output Builder Conversion (3 tests)
- `test_record_replay`
- `test_record_recovery`
- `test_examples`

**Error**: `TypeError: Unable to convert function return value to a Python type!`  
**Location**: Output builder `make_instance` methods  
**Root Cause**: C++ TimeSeriesOutput objects not properly wrapped for Python

### Pattern 2: Mesh Node Errors (6 tests)
- All mesh-related tests

**Error**: `hgraph._types._error_type.NodeException`  
**Location**: Mesh node evaluation  
**Root Cause**: Likely related to mesh node evaluation or clock hierarchy (from previous work)

### Pattern 3: Inspector Errors (1 test)
- `test_inspector_sort_key`

**Error**: `hgraph._types._error_type.NodeException`  
**Location**: Inspector functionality  
**Root Cause**: Unknown, may be related to mesh or component issues

## Notes

1. **Mesh tests**: All 6 mesh tests are failing, suggesting a systematic issue with mesh functionality
2. **Output builder**: 3 tests failing due to output builder conversion issues
3. **Total failure rate**: ~0.75% (10/1336) - relatively low, but these need to be tracked

## Action Items

- [ ] Document these failures before starting migration
- [ ] Ensure migration doesn't introduce new failures in these areas
- [ ] After migration, verify these tests still fail with same errors (or are fixed)
- [ ] Track any new failures introduced by migration






