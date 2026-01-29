# TSB (Bundle) Behavior

Wrapper API surface:
- Output: `__getitem__`, `__iter__`, `__len__`, `__contains__`, `keys`, `values`, `items`, `valid_keys`, `valid_values`,
  `valid_items`, `modified_keys`, `modified_values`, `modified_items`, `key_from_value`, `empty`, `__str__`, `__repr__`,
  plus base output methods.
- Input: `__getitem__`, `__getattr__`, `__iter__`, `__len__`, `__contains__`, `keys`, `values`, `items`, `valid_keys`,
  `valid_values`, `valid_items`, `modified_keys`, `modified_values`, `modified_items`, `key_from_value`, `empty`,
  `__str__`, `__repr__`, plus base input methods.

---

## Python behavior (current)

### Output (`PythonTimeSeriesBundleOutput`)
State:
- Holds child outputs per field (schema-defined order and names).

Properties:
- `value`:
  - If schema defines `scalar_type()` (compound scalar), returns an instance built from **all fields**.
  - Otherwise returns `{field: value}` for fields that are `valid`.
- `delta_value`:
  - Returns `{field: delta_value}` for fields that are both `modified` and `valid`.
- `all_valid`:
  - `True` only if **all** child fields are valid.

Methods:
- `value = mapping_or_scalar`:
  - If `None`: `invalidate()`.
  - If a compound scalar instance:
    - For each schema field, if attribute is not `None`, assigns to the corresponding child output.
  - If mapping/dict:
    - For each provided key/value, assigns non-`None` values to child outputs.
- `clear()`:
  - Calls `clear()` on each child output (no parent-level state change).
- `invalidate()`:
  - Invalidates all children, then `mark_invalid()` on the bundle itself.
- `can_apply_result(result)`:
  - If `None`: `True`.
  - If a compound scalar instance: returns `self.modified` (note: counter-intuitive; preserves current behavior).
  - Else iterates fields and returns `False` if any child cannot apply the value.
- `apply_result(result)`:
  - If not `None`, delegates to `value = result`.
- `copy_from_output(other)` / `copy_from_input(other)`:
  - Copies each child field independently.

### Input (`PythonTimeSeriesBundleInput`)
Bundles can be **peered** (bound to a bundle output) or **un-peered** (bound per-field).

Binding:
- `do_bind_output(output)`:
  - Binds each child input to the corresponding child output.
  - If all children are bound, sets `_output = output` (peered), otherwise `_output = None` (un-peered).
- `do_un_bind_output(unbind_refs=False)`:
  - Unbinds all child inputs; if peered, also unbinds the parent.

Property semantics:
- `bound` is true if the parent is bound **or** any child is bound.
- When **peered** (`has_peer`):
  - `value`, `delta_value`, `modified`, `valid`, `last_modified_time` delegate to base input.
- When **un-peered**:
  - `value`:
    - Compound scalar: builds from fields that are valid or optional in the scalar type.
    - Else dict of valid fields.
  - `delta_value`: dict of modified+valid fields.
  - `modified`: `any(child.modified)`.
  - `valid`: `any(child.valid)`.
  - `last_modified_time`: max of child last_modified_time.
- `active`:
  - Peered: base active.
  - Un-peered: `any(child.active)`.
  - `make_active`/`make_passive` apply to all children if un-peered.

---

## C++ proxy behavior (current)

Wrapper behavior uses `TSBView` directly:
- Field access: `__getitem__` accepts `str` (field name) or `int` (index) and returns **input views**.
- `__getattr__` maps to field lookup.
- `keys/values/items` return Python lists of names and input view wrappers.
- `valid_*` and `modified_*` filter by field validity and `modified_at(eval_time)`.
- `modified_*` returns empty if no cached eval time on owning node.
- `key_from_value` is a stub (always `None`).

Notably missing in proxy:
- No field setters (`__setitem__`); mutation is via `output.value = ...` only.

---

## Behavioral examples

### Example: un-peered bundle input
- Input has two fields: `a`, `b`.
- Only `a` is bound and modified at t1.
- `input.modified == True` (any child modified).
- `input.value` includes `a` only (unless compound scalar rules include defaults).
- `input.delta_value` is `{ "a": a.delta_value }`.

### Example: scalar bundle output
- Schema has `scalar_type()`.
- `output.value = Point(x=1, y=None)`:
  - Sets `x` child output only; `y` untouched.


---

## Tick-by-tick traces (per method)

### keys/values/items
| Tick / Action | keys | values/items |
| --- | --- | --- |
| t0: initial | schema field names | child inputs for each field |
| t1: no change | unchanged | unchanged |

### value (output)
| Tick / Action | value | delta_value |
| --- | --- | --- |
| t0: children invalid | `{}` (or scalar instance if defined) | `{}` |
| t1: set field `a` | `{a: value}` (or scalar with `a` set) | `{a: delta}` |

### modified_keys/values/items
| Tick / Action | modified_keys/values/items |
| --- | --- |
| t1: field `a` modified | only `a` |
| t2: no modified fields | empty |

### valid_keys/values/items
| Tick / Action | valid_keys/values/items |
| --- | --- |
| t1: mixed validity | only valid child fields |

### __getitem__/__getattr__
| Tick / Action | result |
| --- | --- |
| t1: `tsb["a"]` / `tsb.a` | child input view |

### clear / invalidate
| Tick / Action | child state | bundle valid |
| --- | --- | --- |
| t1: `clear()` | children cleared | unchanged unless children change |
| t2: `invalidate()` | children invalidated | `False` |

### all_valid
| Tick / Action | all_valid |
| --- | --- |
| t1: all children valid | `True` |
| t2: any child invalid | `False` |
