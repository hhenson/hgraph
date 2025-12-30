# Value Schema Integration Plan

**Status**: Complete
**Author**: Claude
**Date**: 2025-12-30

---

## Overview

This document outlines the plan to integrate Python's `HgTypeMetaData` system with the C++ Value type schema (`TypeMeta`). The goal is to add a `cpp_type` property to `HgScalarTypeMetaData` and its subclasses that returns the corresponding C++ `TypeMeta*` schema.

---

## Design Principles (from type-improvements branch)

### 1. Object Fallback Pattern

The C++ side uses `nb::object` (nanobind Python object wrapper) as a fallback for any Python type that doesn't have a native C++ representation. This means:

- **No type is truly unsupported** - everything can be stored
- Types like `str`, `bytes`, `Enum`, `datetime.time` use `nb::object`
- Operations on `nb::object` delegate to Python runtime (equality, hashing, arithmetic, etc.)

### 2. Centralized Factory Functions in C++

Instead of mapping types in Python, the C++ side provides factory functions:

```python
# Python calls these C++ functions:
_hgraph.get_scalar_type_meta(py_type)  # Maps Python type -> TypeMeta*
_hgraph.get_dict_type_meta(key_meta, value_meta)
_hgraph.get_set_type_meta(element_meta)
_hgraph.get_dynamic_list_type_meta(element_meta)  # For tuple[T, ...]
_hgraph.get_bundle_type_meta(fields, type_name)   # For fixed tuples, CompoundScalar
```

### 3. C++ Type Mapping

| Python Type | C++ Storage Type | Notes |
|-------------|------------------|-------|
| `bool` | `bool` | Native |
| `int` | `int64_t` | Native |
| `float` | `double` | Native |
| `datetime.date` | `engine_date_t` | Native |
| `datetime.datetime` | `engine_time_t` | Native |
| `datetime.timedelta` | `engine_time_delta_t` | Native |
| `str` | `nb::object` | **Fallback to Python object** |
| `bytes` | `nb::object` | Fallback |
| `datetime.time` | `nb::object` | Fallback |
| `Enum` subclasses | `nb::object` | Fallback |
| Any other type | `nb::object` | Fallback |

### 4. Recursive Type Building

Composite types recursively call `cpp_type` on their element types:

```python
# HgDictScalarType.cpp_type:
key_meta = self.key_type.cpp_type
value_meta = self.value_type.cpp_type
return _hgraph.get_dict_type_meta(key_meta, value_meta)
```

---

## Files to Modify

### C++ Side

| File | Changes |
|------|---------|
| `cpp/include/hgraph/types/value/python_conversion.h` | Add `ScalarTypeOpsWithPython<nb::object>` specialization for Python object storage |
| `cpp/src/cpp/types/type_meta_bindings.cpp` | **New file**: Register `get_scalar_type_meta()`, `get_dict_type_meta()`, etc. |
| `cpp/include/hgraph/types/value/type_meta_bindings.h` | **New file**: Header for bindings |
| `cpp/src/cpp/api/python/py_value.cpp` | Include type_meta_bindings registration |

### Python Side

| File | Changes |
|------|---------|
| `hgraph/_types/_scalar_type_meta_data.py` | Add `cpp_type` property to HgAtomicType and collection types |

---

## Implementation Plan

### Phase 1: C++ Infrastructure

1. **Add `nb::object` scalar type support** in `python_conversion.h`:
   - `ScalarTypeOpsWithPython<nb::object>` - stores arbitrary Python objects
   - Delegates operations (equals, hash, arithmetic) to Python runtime

2. **Create `type_meta_bindings.cpp`**:
   - `get_scalar_type_meta(py_type)` - maps Python type to TypeMeta
   - Caching via TypeRegistry with hash-based keys

3. **Update `py_value.cpp`** to register the new bindings

### Phase 2: Python Integration

1. **Add `cpp_type` property to `HgAtomicType`**:
   ```python
   @property
   def cpp_type(self):
       from hgraph._feature_switch import is_feature_enabled
       if not is_feature_enabled("use_cpp"):
           return None
       try:
           import hgraph._hgraph as _hgraph
           return _hgraph.get_scalar_type_meta(self.py_type)
       except (ImportError, AttributeError):
           return None
   ```

2. **Add `cpp_type` to collection types**:
   - `HgTupleCollectionScalarType` -> `get_dynamic_list_type_meta(element_meta)`
   - `HgTupleFixedScalarType` -> `get_bundle_type_meta(fields, None)` with synthetic field names `$0`, `$1`, etc.
   - `HgSetScalarType` -> `get_set_type_meta(element_meta)`
   - `HgDictScalarType` -> `get_dict_type_meta(key_meta, value_meta)`
   - `HgCompoundScalarType` -> `get_bundle_type_meta(fields, type_name)`

### Phase 3: Testing

1. Add tests for all supported scalar types
2. Add tests for composite types
3. Verify caching behavior
4. Test Python object fallback types (`str`, `bytes`, `Enum`, etc.)

---

## Key Design Decisions

### Decision 1: Property Name

Use `cpp_type` to:
- Mirror `py_type` property (symmetry between Python and C++ type access)
- Be concise and intuitive
- The return type (`TypeMeta`) is self-documenting

### Decision 2: Fallback to `nb::object`

All Python types have a valid C++ representation:
- Native types: `bool`, `int64_t`, `double`, datetime types
- Fallback: `nb::object` for everything else

This ensures `cpp_type` **always returns a valid TypeMeta** (never `None` for type resolution failures).

### Decision 3: Caching in C++

Type caching happens in C++ using a key-based registry:
- Scalar types: keyed by `std::type_index`
- Composite types: keyed by hash of component TypeMeta pointers

This avoids duplicate type registrations and ensures pointer stability.

### Decision 4: Bundle Type Equivalence

**Bundles are type-equivalent based on field definitions only**, not their names:
- Cache key = hash of (field names + field types + field order)
- The `type_name` parameter is optional metadata for display/lookup
- Two bundles with identical fields **in the same order** return the **same TypeMeta*** regardless of name
- **Field ordering is critical** - the memory layout depends on it

Example:
```python
# These return the SAME TypeMeta* (structurally equivalent)
bundle1 = get_bundle_type_meta([("x", int_meta), ("y", float_meta)], "Point2D")
bundle2 = get_bundle_type_meta([("x", int_meta), ("y", float_meta)], "Coordinate")
bundle3 = get_bundle_type_meta([("x", int_meta), ("y", float_meta)], None)

assert bundle1 is bundle2 is bundle3  # Same TypeMeta*

# But different order = DIFFERENT TypeMeta*
bundle4 = get_bundle_type_meta([("y", float_meta), ("x", int_meta)], "Point2D")
assert bundle1 is not bundle4  # Different due to field order
```

The name can be registered as a lookup alias via `register_named_bundle()` but doesn't affect type identity.

### Decision 5: Resolution Requirement

`cpp_type` raises an exception if the type is unresolved (contains TypeVars):
```python
if not self.is_resolved:
    raise TypeError(f"Cannot get cpp_type for unresolved type: {self}")
```

This is intentional:
- Unresolved types cannot be mapped to concrete C++ types
- Raising an exception prevents accidental bugs from silently propagating `None`
- Callers must ensure types are resolved before accessing `cpp_type`

---

## Example Usage

```python
from hgraph._types._scalar_type_meta_data import HgAtomicType
from hgraph._types._type_meta_data import HgTypeMetaData

# Scalar types
int_meta = HgTypeMetaData.parse_type(int)
cpp_int = int_meta.cpp_type  # TypeMeta for int64_t

str_meta = HgTypeMetaData.parse_type(str)
cpp_str = str_meta.cpp_type  # TypeMeta for nb::object

# Composite types
from typing import Dict
dict_meta = HgTypeMetaData.parse_type(Dict[str, int])
cpp_dict = dict_meta.cpp_type  # Dict TypeMeta with object keys, int64 values

# Using the TypeMeta to create Values
from hgraph._hgraph import value
v = value.PlainValue(cpp_int)
v.view().from_python(42)
print(v.const_view().to_python())  # 42
```

---

## Open Questions

(None currently - all major design decisions resolved)

---

## Design Notes

### Numpy Array Handling

`HgArrayScalarTypeMetaData` (numpy arrays) will use a List TypeMeta with an additional flag:

- Add a flag to List TypeMeta indicating "return as numpy array"
- Underlying storage is the same (contiguous list of elements)
- Python conversion logic checks the flag:
  - If flag set: `to_python()` returns `numpy.ndarray`, `from_python()` accepts array/buffer
  - If flag not set: returns Python `list` or `tuple`
- **This is a distinct type schema** - a List with the numpy flag is not type-equivalent to a List without it
- The flag is included in the cache key for type deduplication

This allows efficient buffer-compatible storage while preserving numpy semantics at the Python boundary.

### Size and WindowSize Handling

`Size` and `WindowSize` types will map to `int64_t` TypeMeta with an additional flag:

- Underlying C++ storage is `int64_t` (just a number)
- Add a flag to the schema indicating the Python type (`SIZE`, `WINDOW_SIZE`, etc.)
- Python conversion logic checks the flag:
  - If `SIZE` flag: `to_python()` returns `Size[n]`, `from_python()` accepts `Size`
  - If `WINDOW_SIZE` flag: `to_python()` returns `WindowSize[n]`, `from_python()` accepts `WindowSize`
  - If no flag: standard `int` conversion
- **This is a distinct type schema** - an int64 with the SIZE flag is not type-equivalent to a plain int64
- The flag is included in the cache key for type deduplication

---

## Success Criteria

- [x] All Python scalar types can be mapped to C++ TypeMeta
- [x] Composite types recursively build correct TypeMeta
- [x] `nb::object` fallback works correctly for non-native types
- [x] Type caching prevents duplicate TypeMeta creation
- [x] All existing tests continue to pass
- [x] New integration tests pass

---

## References

- Branch: `origin/type-improvements`
- Key files:
  - `cpp/src/cpp/types/type_meta_bindings.cpp`
  - `cpp/include/hgraph/types/value/python_conversion.h`
  - `hgraph/_types/_scalar_type_meta_data.py` (diff shows pattern)
