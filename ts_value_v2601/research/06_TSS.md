# TSS (Set) Behavior

Wrapper API surface:
- Output: `__contains__`, `__len__`, `empty`, `values`, `added`, `removed`, `was_added`, `was_removed`, `add`,
  `remove`, `get_contains_output`, `release_contains_output`, `is_empty_output`, `__str__`, `__repr__`,
  plus base output methods.
- Input: `__contains__`, `__len__`, `empty`, `values`, `added`, `removed`, `was_added`, `was_removed`,
  `__str__`, `__repr__`, plus base input methods.

---

## Python behavior (current)

### Output (`PythonTimeSeriesSetOutput`)
State:
- `_value`: current set.
- `_added`: elements added in current tick (or `None`).
- `_removed`: elements removed in current tick (or `None`).
- `_contains_ref_outputs`: feature extension producing TS[bool] per element.
- `_is_empty_ref_output`: TS[bool] output tracking empty state.

Properties:
- `value` returns `_value`.
- `delta_value` returns `SetDelta(added, removed)` using the global set delta factory.

Methods:
- `value = v`:
  - `None` => `invalidate()` (clears value and marks invalid).
  - `SetDelta`:
    - Computes added/removed vs `_value`, updates `_value`.
  - `frozenset`:
    - Computes added/removed vs old value, sets `_value`.
  - Any iterable:
    - Treats `Removed(x)` entries as removals.
    - Adds others not present.
    - Raises if an element appears in both added and removed.
  - Calls `_post_modify()` which:
    - Calls `mark_modified()` if any changes (or output invalid).
    - Updates `_is_empty_ref_output` and `_contains_ref_outputs`.
- `add(element)`:
  - Adds element if absent, updates `_added`/`_removed`, marks modified.
- `remove(element)`:
  - Removes if present, updates `_removed` (unless added in same tick), marks modified.
- `clear()`:
  - Marks all current values as removed, clears `_value`, marks modified.
- `invalidate()`:
  - Calls `clear()` then sets `last_modified_time = MIN_DT`.
- `mark_modified()`:
  - Schedules `_reset()` after evaluation to clear `_added` and `_removed`.
- `get_contains_output(item, requester)`:
  - Returns a TS[bool] output that tracks membership for `item`.
- `release_contains_output(item, requester)`:
  - Releases a previously created contains output.
- `is_empty_output()`:
  - Returns TS[bool] output indicating emptiness; initializes if invalid.

### Input (`PythonTimeSeriesSetInput`)
State:
- `_prev_output`: previous output after rebinding, used for delta computation.

Properties:
- `modified` is `True` if bound output modified or sampled.
- `delta_value` is `PythonSetDelta(self.added(), self.removed())`.

Methods:
- `added()`:
  - If `_prev_output` exists, computes new values minus previous values (accounting for added/removed in prev).
  - If sampled: returns all current values.
  - Else returns `output.added()`.
- `removed()`:
  - If `_prev_output` exists, computes previous values minus current values.
  - If sampled: returns empty set.
  - Else returns `output.removed()`.
- `was_added(item)`:
  - For `_prev_output`, intended to check previous values; current implementation is missing a `return`.
  - If sampled: returns `item in output.value()`.
  - Else returns `output.was_added(item)`.
- `was_removed(item)`:
  - For `_prev_output`, checks item in previous and not in current.
  - If sampled: `False`.
  - Else returns `output.was_removed(item)`.

---

## C++ proxy behavior (current)

- Read-only set behavior uses `TSSView` and `SetDeltaView`.
- `add`, `remove`, `get_contains_output`, `release_contains_output`, `is_empty_output` throw.

---

## Behavioral examples

### Example: add/remove within same tick
- Start with `{1}`.
- `add(2)` then `remove(2)` in same tick:
  - `_added` contains 2, then removal discards it.
  - Net effect: 2 is not added, but removal may still mark modified.

### Example: sampling after rebind
- Input rebinds at t5 to a new output with values `{A, B}`.
- `added()` returns `{A, B}` (sampled semantics).
- `removed()` returns empty set.


---

## Tick-by-tick traces (per method)

### value (output)
| Tick / Action | value | delta_value |
| --- | --- | --- |
| t0: empty set, invalid | `{}` | `SetDelta(added={}, removed={})` |
| t1: set to `{1,2}` | `{1,2}` | `SetDelta(added={1,2}, removed={})` |

### added / removed
| Tick / Action | added | removed |
| --- | --- | --- |
| t1: add `{1,2}` | `{1,2}` | `{}` |
| t2: remove `{1}` | `{}` | `{1}` |

### was_added / was_removed
| Tick / Action | was_added(1) | was_removed(1) |
| --- | --- | --- |
| t1: add `{1}` | `True` | `False` |
| t2: remove `{1}` | `False` | `True` |

### add/remove
| Tick / Action | added | removed |
| --- | --- | --- |
| t1: add existing | `{}` | `{}` |
| t2: add new | `{new}` | `{}` |
| t3: remove missing | `{}` | `{}` |
| t4: add then remove same tick | `{}` | `{}` |

### clear
| Tick / Action | removed | value |
| --- | --- | --- |
| t1: `clear()` | all existing | `{}` |

### get_contains_output / is_empty_output
| Tick / Action | contains output | empty output |
| --- | --- | --- |
| t1: add `{1}` | `contains(1)=True` | `False` |
| t2: clear | `contains(1)=False` | `True` |

### Input added/removed during rebind
| Tick / Action | added() | removed() | was_added(1) |
| --- | --- | --- | --- |
| t5: rebind to `{A, B}` | `{A, B}` | `{}` | `None` (missing return path) |
