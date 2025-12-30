# Testing Patterns Workflow

**Purpose:** Standard patterns for writing tests and debugging C++ vs Python behavior mismatches.

---

## Test File Organization

### Naming Conventions

- Test files mirror source structure
- Prefix test files with `test_`
- Example: Source at `hgraph/_types/_scalar_type.py` → Test at `hgraph_unit_tests/_types/test_scalar_type.py`

### Directory Mapping

| Source Location | Test Location |
|-----------------|---------------|
| `cpp/include/hgraph/types/value/` | `hgraph_unit_tests/_types/_value/` |
| `cpp/include/hgraph/types/time_series/` | `hgraph_unit_tests/_types/_time_series/` |
| `hgraph/_operators/` | `hgraph_unit_tests/_operators/` |

---

## Python Test Style (pytest)

Use function-based tests, not class-based:

```python
import pytest

def test_basic_functionality():
    """Clear description of what is being tested."""
    result = function_under_test()
    assert result == expected_value


def test_edge_case_empty_input():
    """Test behavior with empty input."""
    result = function_under_test([])
    assert result is None


@pytest.mark.parametrize("input_val,expected", [
    (1, 2),
    (2, 4),
    (0, 0),
    (-1, -2),
])
def test_parameterized_cases(input_val, expected):
    """Test multiple inputs systematically."""
    assert double(input_val) == expected
```

---

## C++ Test Style (Catch2)

```cpp
#include <catch2/catch_test_macros.hpp>
#include "hgraph/types/value/your_type.h"

TEST_CASE("YourType basic functionality", "[value][your_type]") {
    SECTION("construction") {
        YourType t;
        REQUIRE(t.is_valid());
    }
    
    SECTION("operation") {
        YourType t;
        t.do_something();
        REQUIRE(t.state() == expected_state);
    }
}
```

---

## Debugging C++ vs Python Mismatches

When C++ behavior doesn't match Python, follow this systematic approach:

### Step 1: Confirm the Mismatch

```bash
# Run with C++ - observe failure
uv run pytest hgraph_unit_tests/<path>/test_<name>.py -v

# Run with Python - confirm it passes
HGRAPH_USE_CPP=0 uv run pytest hgraph_unit_tests/<path>/test_<name>.py -v
```

### Step 2: Add Tracing to Both Implementations

Add trace statements to compare execution flow:

**Python tracing:**
```python
def some_method(self, arg):
    print(f"[PY] some_method called: arg={arg}")
    result = self._compute(arg)
    print(f"[PY] some_method result: {result}")
    return result
```

**C++ tracing:**
```cpp
void SomeClass::some_method(Arg arg) {
    std::cout << "[CPP] some_method called: arg=" << arg << std::endl;
    auto result = compute(arg);
    std::cout << "[CPP] some_method result: " << result << std::endl;
    return result;
}
```

### Step 3: Compare Execution Paths

Run both implementations and compare the trace output:

```bash
# Capture C++ trace
uv run pytest <test> -v -s 2>&1 | tee cpp_trace.log

# Capture Python trace  
HGRAPH_USE_CPP=0 uv run pytest <test> -v -s 2>&1 | tee py_trace.log

# Compare
diff cpp_trace.log py_trace.log
```

### Step 4: Narrow Down the Deviation

Look for the first point where traces diverge:
- Different values at the same step → investigate that computation
- Different number of calls → investigate control flow
- Missing calls → check conditional logic

### Step 5: Create Minimal Reproduction (if needed)

If the test is too complex, create a minimal test case:

```python
def test_minimal_reproduction():
    """Minimal case isolating the specific issue."""
    # Setup only what's needed
    obj = MinimalSetup()
    
    # Single operation that fails
    result = obj.problematic_operation()
    
    # Single assertion
    assert result == expected
```

---

## Validation Checklist

Before marking any feature complete:

```bash
# 1. Rebuild C++ (ensure latest code)
cmake --build cmake-build-debug

# 2. Run full test suite
uv run pytest hgraph_unit_tests -v

# 3. Verify all tests pass (no failures, no errors)
# Skipped and xfail tests are acceptable

# 4. Optional: Confirm Python still works
HGRAPH_USE_CPP=0 uv run pytest hgraph_unit_tests -v
```

---

## Quick Debugging Commands

```bash
# Run single test with output visible
uv run pytest hgraph_unit_tests/<path>/test_<name>.py::<test_function> -v -s

# Run test and stop on first failure
uv run pytest <tests> -v -x

# Run tests matching a keyword
uv run pytest hgraph_unit_tests -v -k "keyword"

# Show local variables on failure
uv run pytest <tests> -v -l

# Enter debugger on failure
uv run pytest <tests> -v --pdb
```

---

## Common Issues and Solutions

| Symptom | Likely Cause | Solution |
|---------|--------------|----------|
| Tests pass in Python, fail in C++ | Logic mismatch | Add tracing, compare flows |
| Tests pass sometimes | State leakage or timing | Check static/global state |
| Segfault in C++ | Memory issue | Check ownership, lifetimes |
| Import error | Build/symlink issue | Rebuild, check symlink |
| Old behavior persists | Stale .so | Rebuild C++, verify symlink |
