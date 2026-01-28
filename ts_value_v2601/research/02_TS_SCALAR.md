# TS (Scalar) Behavior

Wrapper API surface:
- Uses only the base `TimeSeriesInput`/`TimeSeriesOutput` methods.

---

## Python behavior (current)

### Output (`PythonTimeSeriesValueOutput`)
State:
- `_value`: stored scalar or `None`.

Properties:
- `value` returns `_value`.
- `delta_value` returns `_value` (scalar delta equals full value).

Methods:
- `value = v`:
  - If `v is None`: calls `invalidate()` (clears value and marks invalid).
  - If type checking is enabled (`HG_TYPE_CHECKING`):
    - Validates `isinstance(v, tp)` (supports generic origins).
    - Raises `TypeError` on mismatch.
  - Sets `_value = v` and `mark_modified()`.
- `invalidate()`:
  - Clears `_value` and sets `last_modified_time = MIN_DT` via `mark_invalid()`.
- `clear()`:
  - No-op for scalar output.
- `can_apply_result(result)`:
  - Returns `not self.modified` (disallows overwrite in same tick).
- `apply_result(result)`:
  - Ignores `None`.
  - Otherwise assigns `self.value = result` and raises `TypeError` on mismatch.
- `copy_from_output(other)` / `copy_from_input(other)`:
  - Copies the scalar value and marks modified.

### Input (`PythonTimeSeriesValueInput`)
- Inherits all behavior from `PythonBoundTimeSeriesInput`.
- `value`, `delta_value`, `modified`, `valid`, `last_modified_time` forward to the bound output.

---

## C++ proxy behavior (current)

- `PyTimeSeriesValueInput/Output` are thin wrappers around base input/output wrappers.
- All semantics are provided by `TSView` and `TSMutableView` conversions.

---

## Behavioral examples

### Example: setting and invalidating
- t0: output invalid (`value is None`, `valid == False`).
- t1: `output.value = 5`:
  - `value == 5`, `delta_value == 5`, `valid == True`, `modified == True` at t1.
- t2: `output.value = None`:
  - `value == None`, `valid == False`, `modified == True` at t2.


---

## Tick-by-tick traces (per method)

### value (get/set)
| Tick / Action | value | delta_value | modified | valid |
| --- | --- | --- | --- | --- |
| t0: initial (unset) | `None` | `None` | `False` | `False` |
| t1: set `value = 5` | `5` | `5` | `True` | `True` |
| t2: no change | `5` | `None` | `False` | `True` |

### invalidate
| Tick / Action | value | valid | modified |
| --- | --- | --- | --- |
| t1: `invalidate()` | `None` | `False` | `True` |

### can_apply_result / apply_result
| Tick / Action | can_apply_result | value | modified |
| --- | --- | --- | --- |
| t1: already modified | `False` | `5` | `True` |
| t2: not modified, `apply_result(7)` | `True` | `7` | `True` |

### copy_from_output / copy_from_input
| Tick / Action | value | modified |
| --- | --- | --- |
| t1: copy same value | `5` | `False` |
| t2: copy different value | `7` | `True` |
