# HGraph C++ Unit Tests

## Visitor Pattern Tests

This directory contains standalone C++ unit tests for the visitor pattern implementation in hgraph.

### Test File: `test_visitor_standalone.cpp`

This file contains comprehensive tests for both CRTP and Acyclic visitor patterns. It uses mock time series types to test the visitor infrastructure without requiring the full hgraph runtime dependencies (Python, nanobind, etc.).

#### Test Coverage

The test suite validates:

1. **CRTP Visitor Pattern**
   - Basic type visitation (TS, TSB, TSL, TSD, TSS)
   - Template type instantiation
   - Counting visitors
   - Const visitors

2. **Acyclic Visitor Pattern**
   - Specific type handling
   - Type-selective visitation (ignoring unsupported types)
   - Collection types (list, dict, set)
   - Polymorphic visitation via base pointers

3. **Const Visitation**
   - Read-only CRTP visitors
   - Read-only Acyclic visitors

4. **Pattern Separation**
   - CRTP for compile-time generic operations
   - Acyclic for runtime-specific type handling
   - Demonstrates that the two patterns are mutually exclusive due to `requires` clause

5. **Edge Cases**
   - Selective visitors that only handle specific types
   - Unsupported types are silently ignored in Acyclic pattern

6. **Template Instantiation**
   - Multiple TSD (dict) instantiations with different key types
   - Multiple TSS (set) instantiations with different value types

7. **Dispatch Mechanism**
   - Verifies CRTP dispatch when visitor doesn't derive from TimeSeriesVisitor
   - Verifies Acyclic dispatch when visitor derives from TimeSeriesVisitor

#### Test Results

All 24 test cases pass with 50 assertions:
- ✅ 7 CRTP visitor tests
- ✅ 5 Acyclic visitor tests
- ✅ 3 Const visitor tests
- ✅ 2 Polymorphic visitation tests
- ✅ 2 Mixed pattern tests
- ✅ 1 Edge case test
- ✅ 2 Template instantiation tests
- ✅ 2 Dispatch mechanism tests

### Building the Tests

The tests are built using a standalone CMake configuration that doesn't depend on the main hgraph Python module.

```bash
cd /Users/hhenson/CLionProjects/hgraph/cpp/tests
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j 8
```

### Running the Tests

```bash
./build/hgraph_visitor_tests
```

Or run specific tests using Catch2 filters:

```bash
# Run only CRTP tests
./build/hgraph_visitor_tests "[crtp]"

# Run only Acyclic tests
./build/hgraph_visitor_tests "[acyclic]"

# Run only const visitor tests
./build/hgraph_visitor_tests "[const]"

# Run template instantiation tests
./build/hgraph_visitor_tests "[templates]"

# Run a specific test case
./build/hgraph_visitor_tests "CRTP Visitor - Basic TS int"
```

### Dependencies

- **Catch2 v3.5.0**: Testing framework (automatically fetched via FetchContent)
- **fmt 10.1.0**: Formatting library (automatically fetched via FetchContent)
- **C++23**: Required for `requires` clauses and other modern features

### Architecture Notes

#### Mock Types

The test file defines mock time series types that mirror the real hgraph types:

- `MockTimeSeriesOutput`: Base class with virtual `accept()` methods
- `MockTS<T>`: Template for value time series
- `MockTSB`: Bundle time series
- `MockTSL`: List time series
- `MockTSD<K>`: Template for dict time series with key type K
- `MockTSS<T>`: Template for set time series with value type T

Each mock type implements:
1. Virtual `accept()` methods for Acyclic visitor support (runtime dispatch)
2. Template `accept()` methods for CRTP visitor support (compile-time dispatch)

The `requires` clause ensures the correct dispatch path is chosen:
- CRTP path: `requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)`
- Acyclic path: Virtual methods called when visitor derives from `TimeSeriesVisitor`

#### Why Standalone Tests?

The tests are standalone (not using actual hgraph types) because:

1. **Independence**: Tests can run without Python, nanobind, or other runtime dependencies
2. **Speed**: Much faster to compile and run
3. **Isolation**: Tests focus purely on visitor pattern mechanics
4. **Portability**: Can be run in any C++23 environment

The mock types faithfully reproduce the visitor pattern structure used in the real hgraph implementation, so these tests validate the actual pattern used in production code.

### Integration with Main Build

The tests can optionally be built as part of the main hgraph build:

```bash
cd /Users/hhenson/CLionProjects/hgraph/cpp
cmake -B build -DHGRAPH_BUILD_TESTS=ON
cmake --build build
```

However, this is not required since the tests have their own standalone build configuration.
