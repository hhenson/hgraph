# C++ Build Status

## Current Situation

C++ specialized reference classes have been created and committed, but cannot be compiled locally due to cmake configuration issues with Python/nanobind detection.

## Local Build Issue

```
CMake Error: Could not find nanobind
- Cmake finding wrong Python (3.13 instead of 3.12 from venv)
- nanobind module not found in that Python
```

## CI/CD Status

The code has been pushed to `origin/cpp_v2_1` (commit 0ccc6e97).
If CI/CD is failing, likely compilation errors need to be fixed.

## What to Check

### 1. Compilation Errors

If CI/CD shows compilation errors, likely issues:

**Common C++ Compilation Issues:**
- Missing header includes
- Typos in class/function names
- Namespace issues
- Signature mismatches

### 2. Linking Errors

If compilation succeeds but linking fails:
- Missing symbol definitions
- Duplicate symbols
- Library dependencies

### 3. Python Import Errors

If build succeeds but Python import fails:
- Nanobind registration issues
- Symbol name mismatches
- Module initialization problems

## How to Fix

### For Compilation Errors:

1. Check CI/CD logs for specific error messages
2. Fix issues in:
   - `cpp/include/hgraph/types/ref.h`
   - `cpp/src/cpp/types/ref.cpp`
   - `cpp/include/hgraph/builders/time_series_types/specialized_ref_builders.h`
   - `cpp/src/cpp/builders/time_series_types/specialized_ref_builders.cpp`

### For Import Errors:

3. Check builder names match what Python expects:
   - `_hgraph.InputBuilder_TS_Value_Ref`
   - `_hgraph.InputBuilder_TSL_Ref`
   - etc.

## Files to Review

1. **ref.h** - Class declarations with proper inheritance
2. **ref.cpp** - Nanobind registrations  
3. **specialized_ref_builders.h** - Builder declarations
4. **specialized_ref_builders.cpp** - Builder implementations
5. **_use_cpp_runtime.py** - Python factory dispatch

## Status

- ✅ C++ code structure created
- ✅ Files committed and pushed
- ❌ Local compilation blocked (cmake/Python issue)
- ⏳ CI/CD results pending

Once you see the CI/CD error logs, those will indicate what needs to be fixed.
