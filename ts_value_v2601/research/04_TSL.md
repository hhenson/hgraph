# TSL (List) Behavior

Wrapper API surface:
- Output: `__getitem__`, `__iter__`, `__len__`, `empty`, `keys`, `values`, `items`, `valid_keys`, `valid_values`,
  `valid_items`, `modified_keys`, `modified_values`, `modified_items`, `__str__`, `__repr__`, plus base output methods.
- Input: `__getitem__`, `__iter__`, `__len__`, `empty`, `keys`, `values`, `items`, `valid_keys`, `valid_values`,
  `valid_items`, `modified_keys`, `modified_values`, `modified_items`, `__str__`, `__repr__`, plus base input methods.

---

## Python behavior (current)

### Output (`PythonTimeSeriesListOutput`)
State:
- Fixed-size list of child outputs.

Properties:
- `value`:
  - Returns a tuple of element values (invalid elements yield `None`).
- `delta_value`:
  - Returns `{index: delta_value}` for modified elements (validity not checked).
- `all_valid`:
  - `True` only if all elements are valid.

Methods:
- `value = v`:
  - `None` => `invalidate()` (invalidates all elements).
  - Mapping (`dict`/`frozendict`): assigns by index for non-`None` values.
  - Sequence (`list`/`tuple`):
    - Length must match list size, otherwise `ValueError`.
    - Assigns each non-`None` element.
- `clear()`:
  - Calls `clear()` on each element.
- `invalidate()`:
  - Invalidates all elements.
- `mark_invalid()`:
  - If valid, marks each element invalid and sets parent invalid.
- `can_apply_result(result)`:
  - Rejects if any child cannot apply its element.
- `apply_result(result)`:
  - Delegates to `value = result`.
- `copy_from_output` / `copy_from_input`:
  - Copies each element value.

### Input (`PythonTimeSeriesListInput`)
Lists can be **peered** (parent output bound) or **un-peered** (per-element binding).

Binding:
- `do_bind_output(output)`:
  - Binds each element input to corresponding output element.
  - If all bindings succeed, sets parent `_output` to be peered.
- `do_un_bind_output(unbind_refs=False)`:
  - Unbinds each element; clears parent binding if peered.

Property semantics:
- `bound` is true if parent bound or any child bound.
- When **peered**:
  - `value`, `delta_value`, `modified`, `valid`, `last_modified_time` delegate to base input.
  - `delta_value` uses base unless sampled; if sampled, returns dict of modified items.
  - `modified` is `base.modified or sampled`.
- When **un-peered**:
  - `value` is tuple of element values.
  - `delta_value` is dict of modified items.
  - `modified`/`valid` are any-child semantics.
  - `last_modified_time` is max child last_modified_time.
- `active`:
  - Peered: base active.
  - Un-peered: `any(child.active)`.
  - `make_active`/`make_passive` apply to all children if un-peered.

---

## C++ proxy behavior (current)

Wrapper behavior uses `TSLView` directly:
- `__getitem__` accepts index and returns **input views**.
- `keys/values/items` return Python lists of indices and input view wrappers.
- `valid_*` and `modified_*` filter by element validity and `modified_at(eval_time)`.
- `modified_*` returns empty if no cached eval time on owning node.

Notably missing in proxy:
- No per-element mutation (`__setitem__`, `append`, `resize`). Output updates are via `output.value = ...`.

---

## Behavioral examples

### Example: setting by dict
- Output size = 3.
- `output.value = {0: 1.0, 2: 3.0}`:
  - Element 0 and 2 are updated, element 1 is untouched.
  - `delta_value` includes only indices 0 and 2 if those elements are modified.

### Example: un-peered input
- Two elements bound independently.
- Only element 1 ticks at t5.
- `input.modified == True`, `delta_value == {1: elem1.delta_value}`.


---

## Tick-by-tick traces (per method)

### value (output)
| Tick / Action | value | delta_value |
| --- | --- | --- |
| t0: elements invalid | tuple of `None` values | `{}` |
| t1: set index `0` | tuple with value at `0` and `None` elsewhere | `{0: delta}` |

### keys/values/items
| Tick / Action | keys | values/items |
| --- | --- | --- |
| t0: any tick | `[0..n-1]` | values are child inputs; items are `(index, child)` |

### modified_keys/values/items
| Tick / Action | modified_keys/values/items |
| --- | --- |
| t1: element `1` modified | only index `1` |
| t2: no modifications | empty |

### valid_keys/values/items
| Tick / Action | valid_keys/values/items |
| --- | --- |
| t1: mixed validity | only valid indices |

### __getitem__
| Tick / Action | result |
| --- | --- |
| t1: `tsl[2]` | child input view at index `2` |

### clear / invalidate
| Tick / Action | element state | list valid |
| --- | --- | --- |
| t1: `clear()` | elements cleared | unchanged unless children change |
| t2: `invalidate()` | elements invalidated | `False` |

### all_valid
| Tick / Action | all_valid |
| --- | --- |
| t1: all elements valid | `True` |
| t2: any element invalid | `False` |
