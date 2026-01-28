# TSW (Window) Behavior

Wrapper API surface:
- Fixed window output: `value_times`, `first_modified_time`, `size`, `min_size`, `has_removed_value`, `removed_value`,
  `__len__`, plus base output methods.
- Time window output: `value_times`, `first_modified_time`, `size`, `min_size`, `has_removed_value`, `removed_value`,
  `__len__`, plus base output methods (some are not implemented in the current proxy).
- Window input: `value_times`, `first_modified_time`, `has_removed_value`, `removed_value`, `__len__`, plus base input methods.

---

## Python behavior (current)

### Fixed-size window output (`PythonTimeSeriesFixedWindowOutput`)
State:
- `_value`: numpy array of size `_size`.
- `_times`: numpy array of timestamps.
- `_start`, `_length`: ring buffer indices.
- `_removed_value`: last evicted value (cleared after evaluation).

Properties:
- `value`:
  - Returns `None` if `_length < _min_size`.
  - If `_length < _size`, returns a copy of the prefix.
  - If full, returns a rolled array so index 0 is the oldest element.
- `delta_value`:
  - Returns most recent element only if its timestamp equals the current evaluation time.
  - Otherwise returns `None`.
- `value_times`:
  - Returns timestamps aligned with `value` (rolled as needed).
- `all_valid`:
  - `True` only if `valid` and `_length >= _min_size`.
- `has_removed_value`:
  - `True` if `_removed_value` is set (eviction occurred this tick).

Methods:
- `apply_result(result)`:
  - Appends to the ring buffer with current evaluation time.
  - If full, evicts the oldest element and sets `_removed_value` until post-eval reset.
  - Marks modified.
- `value = array`:
  - Sets `_value` directly (length must be <= size), updates `_length` and `_start`, marks modified.
- `invalidate()`:
  - Calls `mark_invalid()`.
- `mark_invalid()`:
  - Resets `_value`, `_times`, `_start`, `_length` to empty defaults.
- `copy_from_output` / `copy_from_input`:
  - Copies buffer and times from source, marks modified.

### Time-based window output (`PythonTimeSeriesTimeWindowOutput`)
State:
- `_value`: deque of values.
- `_times`: deque of timestamps.
- `_size`: timedelta window size.
- `_min_size`: timedelta minimum window size.
- `_removed_values`: tuple of evicted values (cleared after evaluation).

Properties:
- `ready`:
  - `True` once evaluation time has advanced by `_min_size` since graph start.
- `value`:
  - If `ready`, rolls out-of-window elements and returns a numpy array of values.
  - If not `ready`, returns `None`.
- `delta_value`:
  - Returns last element only if last timestamp equals evaluation time and `ready`.
  - Else `None`.
- `value_times`:
  - Returns deque of timestamps after rolling.
- `first_modified_time`:
  - Returns first timestamp after rolling, or `MIN_TD` if empty.

Methods:
- `apply_result(result)`:
  - Appends value and current time; marks modified.
- `_roll()`:
  - Evicts elements older than `evaluation_time - _size`.
  - Captures removed values and schedules reset after evaluation.
- `invalidate()`:
  - Calls `mark_invalid()`.
- `mark_invalid()`:
  - Clears deques and marks invalid.

Known issues in current Python implementation:
- `removed_value` property recursively returns itself (likely a bug).
- `copy_from_output`/`copy_from_input` for time window use `PythonTimeSeriesFixedWindowOutput` in assertions.

### Window input (`PythonTimeSeriesWindowInput`)
- `value_times`, `first_modified_time`, `has_removed_value`, `removed_value`, `size`, `min_size`, `__len__` delegate
  to bound output.

---

## C++ proxy behavior (current)

### Fixed-size window wrappers
- All accessors are implemented via `WindowStorageOps` and `TSWTypeMeta`.

### Time-based window wrappers
- `size` and `min_size` return time ranges; other accessors throw `runtime_error`.

---

## Behavioral examples

### Example: fixed window eviction
- Window size = 3.
- Values set at t1, t2, t3 => full.
- t4: apply_result(v4):
  - Oldest value becomes `removed_value` for t4.
  - `has_removed_value == True` at t4, then clears after evaluation.


---

## Tick-by-tick traces (per method)

### Fixed window: value / value_times
| Tick / Action | value | delta_value | value_times |
| --- | --- | --- | --- |
| t0: length < min_size | `None` | `None` | `None` |
| t1..tN: length >= min_size | window array | last element if appended this tick, else `None` | aligned times |

### Fixed window: has_removed_value / removed_value
| Tick / Action | has_removed_value | removed_value |
| --- | --- | --- |
| tK: overflow | `True` | evicted element |
| tK+1: after eval | `False` | `None` |

### Time window: value / value_times
| Tick / Action | value | delta_value | value_times |
| --- | --- | --- | --- |
| t0..tM: not ready | `None` | `None` | `None` |
| tM+1: ready | window array | last element if last timestamp == eval time, else `None` | aligned times |

### Window input accessors
| Tick / Action | accessor behavior |
| --- | --- |
| any tick | delegates to bound output |
