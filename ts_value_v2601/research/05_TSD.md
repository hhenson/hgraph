# TSD (Dict) Behavior

Wrapper API surface:
- Output: `__contains__`, `__getitem__`, `__setitem__`, `__delitem__`, `__len__`, `pop`, `get`, `get_or_create`,
  `create`, `clear`, `__iter__`, `keys`, `values`, `items`, `valid_keys`, `valid_values`, `valid_items`,
  `added_keys`, `added_values`, `added_items`, `was_added`, `has_added`, `modified_keys`, `modified_values`,
  `modified_items`, `was_modified`, `removed_keys`, `removed_values`, `removed_items`, `was_removed`,
  `key_from_value`, `has_removed`, `get_ref`, `release_ref`, `key_set`, `__str__`, `__repr__`, plus base output methods.
- Input: `__contains__`, `__getitem__`, `__len__`, `__iter__`, `get`, `get_or_create`, `create`, `keys`, `values`,
  `items`, `valid_keys`, `valid_values`, `valid_items`, `added_keys`, `added_values`, `added_items`, `was_added`,
  `has_added`, `modified_keys`, `modified_values`, `modified_items`, `was_modified`, `removed_keys`, `removed_values`,
  `removed_items`, `was_removed`, `on_key_added`, `on_key_removed`, `key_from_value`, `has_removed`, `key_set`,
  `__str__`, `__repr__`, plus base input methods.

---

## Python behavior (current)

### Output (`PythonTimeSeriesDictOutput`)
State:
- `_ts_values`: map of key -> child output.
- `_key_set`: TSS output tracking current keys.
- `_added_keys`: set used for "added" delta (currently **never populated** in this class).
- `_removed_items`: map of removed key -> child output for this tick.
- `_modified_items`: map of key -> child output modified in this tick.
- `_ts_values_to_keys`: reverse map of child output id -> key.
- `_ref_ts_feature`: feature output extension for key->REF outputs.

Properties:
- `value`:
  - Returns `frozendict({k: v.value for k, v in items() if v.valid})`.
- `delta_value`:
  - Returns `frozendict` of:
    - `{k: v.delta_value}` for items where `v.modified and v.valid`.
    - `{k: REMOVE}` for keys in `removed_keys()`.

Methods:
- `value = mapping_or_iterable`:
  - `None` => `invalidate()`.
  - If output is invalid and input is empty: marks `key_set` modified anyway (ensures a tick).
  - For each entry:
    - If value is `REMOVE`/`REMOVE_IF_EXISTS`, deletes key (skips if not present and REMOVE_IF_EXISTS).
    - Else `get_or_create(key).value = value`.
- `__delitem__(key)`:
  - Raises `KeyError` if key not present.
  - Removes key from `key_set`, notifies key observers.
  - Clears the child output and tracks it in `_removed_items` unless it was added this tick.
  - Schedules cleanup `_clear_key_changes()` after evaluation time.
- `create(key)`:
  - Adds key to `key_set` and constructs child output.
  - Updates reverse map, reference feature, and notifies key observers.
  - Schedules cleanup after evaluation time.
- `clear()`:
  - Clears `key_set` and all child outputs.
  - Moves all items to `_removed_items`, clears `_ts_values`.
- `invalidate()`:
  - Invalidates all child outputs, then `mark_invalid()`.
- `can_apply_result(result)`:
  - Rejects if:
    - Trying to remove a key whose value is modified in this tick.
    - Trying to re-add a key removed earlier in the same tick.
    - Any child cannot apply its value.
- `mark_child_modified(child, modified_time)`:
  - Resets `_modified_items` on first modification in the tick.
  - Adds `(key -> child)` for modified non-key_set children.
- `added_keys/values/items`:
  - Returns `_added_keys` (note: `_added_keys` is never updated here; current behavior is empty unless set elsewhere).
- `removed_keys/values/items`:
  - Returns `_removed_items` from current tick.
- `modified_keys/values/items`:
  - Returns `_modified_items` if output is modified, else empty.
- `get_ref(key, requester)`:
  - Returns a TS output that yields a reference to the key's value (via feature extension).
- `release_ref(key, requester)`:
  - Releases a reference output created with `get_ref`.

### Input (`PythonTimeSeriesDictInput`)
State:
- `_ts_values`: map of key -> child input.
- `_key_set`: TSS input tracking current keys.
- `_removed_items`: map of removed key -> (input, was_valid).
- `_modified_items`: map of key -> child input modified in this tick.
- `_has_peer`: whether input is peered to output as a whole.
- `_prev_output`: prior output after rebinding, used for delta semantics.

Binding:
- `do_bind_output(output)`:
  - Determines peering: if key/value types mismatch and references are involved, `peer=False`.
  - Binds `key_set` to output's key_set.
  - If already bound, records `_prev_output` and schedules reset.
  - Binds each key via `on_key_added`/`on_key_removed` and registers as key observer.
- `do_un_bind_output(unbind_refs=False)`:
  - Unbinds `key_set` and all children.
  - Tracks removed items and cleans them up after evaluation.
  - Preserves "transplanted" children (not parented by this input).

Properties:
- `has_peer == _has_peer`.
- `modified`:
  - Peered: base modified.
  - Un-peered & active: `last_notified_time == eval_time` or `key_set.modified` or `sampled`.
  - Un-peered & passive: `key_set.modified` or any child modified.
- `last_modified_time`:
  - Peered: base last_modified_time.
  - Un-peered & active: max of `last_notified_time`, `key_set.last_modified_time`, `_sample_time`.
  - Un-peered & passive: max of key_set and child times.
- `value`:
  - `frozendict({k: v.value for k, v in items()})` (no validity filtering).
- `delta_value`:
  - `frozendict` combining modified items plus `{k: REMOVE}` for removed items that were valid.

Delta helpers:
- `added_keys/values/items`:
  - Derived from `key_set.added()` or from previous output when rebinding.
- `removed_keys/values/items`:
  - Derived from key_set removals or previous output state.
- `modified_keys/values/items`:
  - Uses `_modified_items` collected in `notify_parent`.

---

## C++ proxy behavior (current)

Read-only support is mostly implemented via `TSDView` and `MapDeltaView`:
- `contains`, `keys`, `values`, `items`, `valid_*`, `added_*`, `removed_*`, `was_added` work.
- `modified_*`, `was_modified`, `get_item`, `get`, `get_or_create`, `create`, `key_set` throw in view wrappers.
- Input `on_key_added` / `on_key_removed` throw.

---

## Behavioral examples

### Example: remove and delta
- t1: keys = {A, B}; both valid.
- t2: `del output['A']`:
  - `removed_keys == {'A'}` for t2.
  - `delta_value` includes `{ 'A': REMOVE }` (if A was valid).

### Example: rebind input
- t3: input rebinds to a new output.
- `added_keys`/`removed_keys` may be computed against `_prev_output` for that tick.


---

## Tick-by-tick traces (per method)

### value (output)
| Tick / Action | value | delta_value |
| --- | --- | --- |
| t0: empty | `{}` | `{}` |
| t1: set key `a` | `{a: value}` (valid entries only) | `{a: value}` |

### get_or_create / create
| Tick / Action | key_set | child output |
| --- | --- | --- |
| t1: `get_or_create('a')` | adds `a` | created |
| t1: `get_or_create('a')` again | unchanged | existing |

### __delitem__
| Tick / Action | removed_keys | cleanup |
| --- | --- | --- |
| t1: `del output['a']` | includes `a` | scheduled after eval |

### added_keys/values/items
| Tick / Action | added_keys/values/items |
| --- | --- |
| t1: key added | empty set (current behavior) |

### removed_keys/values/items
| Tick / Action | removed_keys/values/items |
| --- | --- |
| t1: key removed | includes removed keys |
| t2: after evaluation | cleared |

### modified_keys/values/items
| Tick / Action | modified_keys/values/items |
| --- | --- |
| t1: child modified | includes modified keys |

### valid_keys/values/items
| Tick / Action | valid_keys/values/items |
| --- | --- |
| t1: mixed validity | only valid keys |

### key_set
| Tick / Action | key_set.value |
| --- | --- |
| t1: keys `{a, b}` | `{a, b}` |

### key_from_value
| Tick / Action | result |
| --- | --- |
| t1: `key_from_value(child)` | key for that child |
