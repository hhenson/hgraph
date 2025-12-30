# C++ Feature Development Workflow

**Purpose:** Standard workflow for implementing new or improved C++ features in hgraph.

---

## Core Principles

1. **Python is Authoritative** - The Python implementation defines correct behavior. C++ must match it exactly.
2. **No Python Changes** - Never modify existing Python logic unless explicitly requested. The Python code is the reference standard.
3. **Breaking Changes Forbidden** - Existing behavior must be preserved unless explicitly requested otherwise.
4. **Full Validation Required** - All tests must pass before a feature is considered complete.

---

## Pre-Development Checklist

Before starting any C++ feature work:

```bash
# 1. Verify feature flag is enabled
cat hgraph_features.yaml
# Should show: use_cpp: true

# 2. Verify symlink is in place
ls -la .venv/lib/python3.12/site-packages/hgraph/_hgraph*.so
# Should be symlinked to cmake-build-debug/cpp/src/cpp/_hgraph*.so

# 3. Run a quick sanity test
uv run pytest hgraph_unit_tests/_operators/test_const.py -v
```

---

## Development Workflow

### Step 1: Understand the Python Reference

Before writing any C++ code:

1. **Locate the Python implementation** - Find the authoritative Python code for the feature
2. **Read and understand the behavior** - Trace through the Python code to understand:
   - Input/output contracts
   - Edge cases handled
   - State management
3. **Identify existing tests** - Find tests that exercise this functionality
4. **Run tests with Python** to establish baseline:
   ```bash
   HGRAPH_USE_CPP=0 uv run pytest <relevant_tests> -v
   ```

### Step 2: Design the C++ Implementation

1. **Map Python concepts to C++** - Identify how Python structures translate to C++
2. **Follow existing C++ patterns** - Look at similar implementations in the codebase
3. **Plan the binding layer** - How will Python interact with the C++ code?

### Step 3: Implement Incrementally

1. **Start with core logic** - Implement the fundamental behavior first
2. **Build frequently**:
   ```bash
   cmake --build cmake-build-debug
   ```
3. **Test frequently** - Run relevant tests after each significant change:
   ```bash
   uv run pytest <relevant_tests> -v
   ```
4. **Compare with Python** - If behavior differs, investigate immediately

### Step 4: Validate Completeness

1. **Run the full test suite**:
   ```bash
   uv run pytest hgraph_unit_tests -v
   ```
2. **Verify no regressions** - All previously passing tests must still pass
3. **Compare C++ and Python behavior** (optional but recommended for complex features):
   ```bash
   # Run with C++
   uv run pytest hgraph_unit_tests -v
   
   # Run with Python
   HGRAPH_USE_CPP=0 uv run pytest hgraph_unit_tests -v
   ```

---

## File Organization

### C++ Source Files

| Type | Location |
|------|----------|
| Headers | `cpp/include/hgraph/<category>/` |
| Implementations | `cpp/src/cpp/<category>/` |
| Python bindings | `cpp/src/cpp/api/python/` |

### Test Files

Test files mirror the source structure:

| Source | Test |
|--------|------|
| `cpp/include/hgraph/types/value/` | `hgraph_unit_tests/_types/_value/` |
| `cpp/include/hgraph/types/time_series/` | `hgraph_unit_tests/_types/_time_series/` |

---

## Common Pitfalls

1. **Stale build artifacts** - Always rebuild after C++ changes
2. **Wrong feature flag** - Verify `use_cpp: true` in `hgraph_features.yaml`
3. **Broken symlink** - Check the .so symlink if tests seem to use old code
4. **Assuming Python behavior** - Always verify by reading the Python source

---

## Quick Reference Commands

```bash
# Build C++
cmake --build cmake-build-debug

# Run specific tests
uv run pytest hgraph_unit_tests/<path>/test_<name>.py -v

# Run full suite
uv run pytest hgraph_unit_tests -v

# Compare C++ vs Python
uv run pytest <tests> -v                    # C++
HGRAPH_USE_CPP=0 uv run pytest <tests> -v   # Python
```
