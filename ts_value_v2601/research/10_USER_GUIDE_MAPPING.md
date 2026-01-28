# User Guide -> Current Python Runtime Mapping

This maps the user guide concepts (`ts_value_v2601/user_guide`) to how the **current Python time-series** behavior is
implemented and re-created. Each table is per type, with method mapping and behavioral notes.

---

## TS (Scalar)

| User guide concept | Current Python API | Behavior re-created in Python |
| --- | --- | --- |
| `value()` | `PythonTimeSeriesValueInput/Output.value` | Output stores `_value`; input forwards to output if bound |
| `delta_value()` | `PythonTimeSeriesValueInput/Output.delta_value` | For scalars, equals `value` on output; input mirrors |
| `modified()` | `PythonTimeSeriesValueInput.modified` | True if output modified or input sampled this tick |
| `valid()` | `PythonTimeSeriesValueInput.valid` | True if bound output valid; output valid once set |
| `set_value(x)` | `PythonTimeSeriesValueOutput.value = x` | Type-checked (if enabled), sets `_value`, `mark_modified()` |
| link/active | `bind_output`, `make_active` | Active input subscribes to output notifications |

---

## TSB (Bundle)

| User guide concept | Current Python API | Behavior re-created in Python |
| --- | --- | --- |
| field access | `PythonTimeSeriesBundleInput.__getattr__/__getitem__` | Returns child time-series input for field |
| `modified()` | `PythonTimeSeriesBundleInput.modified` | True if any child modified |
| `all_valid()` | `PythonTimeSeriesBundleInput.all_valid` | True if all children valid |
| `modified_items()` | `PythonTimeSeriesBundleInput.modified_items()` | Yields (field, child) for modified fields |
| `delta_value()` | `PythonTimeSeriesBundleOutput.delta_value` | Dict of modified fields only |

---

## TSL (List)

| User guide concept | Current Python API | Behavior re-created in Python |
| --- | --- | --- |
| element access | `PythonTimeSeriesListInput.__getitem__` | Returns child input at index |
| `modified()` | `PythonTimeSeriesListInput.modified` | True if any element modified |
| `modified_items()` | `PythonTimeSeriesListInput.modified_items()` | Yields (index, child) for modified elements |
| `delta_value()` | `PythonTimeSeriesListOutput.delta_value` | Dict of modified indices to deltas |
| size/iteration | `__len__`, `__iter__` | Fixed-size list semantics (size known at build) |

---

## TSD (Dict)

| User guide concept | Current Python API | Behavior re-created in Python |
| --- | --- | --- |
| key access | `PythonTimeSeriesDictInput.__getitem__` | Returns child input for key |
| `contains(k)` | `__contains__` | Non-peered uses `_ts_values`; peered uses `key_set` |
| key_set | `TSD.key_set` | Backed by `TSS` output/input; key add/remove is tracked |
| `modified_items()` | `PythonTimeSeriesDictInput.modified_items()` | Keys whose child outputs modified |
| `delta_value()` | `PythonTimeSeriesDictOutput.delta_value` | Dict of modified child deltas + `REMOVE` markers |
| `added_keys()` | `PythonTimeSeriesDictOutput.added_keys()` | **Currently empty** (never populated) |
| non-peered case | `PythonTimeSeriesDictInput.has_peer=False` | Occurs when value type mismatch with REF; key add/remove still works |

---

## TSS (Set)

| User guide concept | Current Python API | Behavior re-created in Python |
| --- | --- | --- |
| membership | `PythonTimeSeriesSetInput.__contains__` | Delegates to output when bound |
| `added()` / `removed()` | `PythonTimeSeriesSetInput.added/removed` | Uses output deltas or sampled diff on rebind |
| `was_added(x)` | `PythonTimeSeriesSetInput.was_added` | Missing `return` in `_prev_output` path (returns `None`) |
| `delta_value()` | `PythonTimeSeriesSetOutput.delta_value` | `SetDelta(added, removed)` |
| `set_value()` | `PythonTimeSeriesSetOutput.value = Set/SetDelta` | Computes added/removed and updates value |

---

## TSW (Window)

| User guide concept | Current Python API | Behavior re-created in Python |
| --- | --- | --- |
| `value()` | `PythonTimeSeriesWindowOutput.value` | Fixed: buffer roll once `min_size` met; Time: deque window if `ready` |
| `delta_value()` | `PythonTimeSeriesWindowOutput.delta_value` | Fixed: last element if appended at eval time; Time: last element if timestamp == eval time |
| removed element | `removed_value` / `has_removed_value` | Fixed: valid on overflow; Time: `removed_value` is recursive bug |

---

## REF (Reference)

| User guide concept | Current Python API | Behavior re-created in Python |
| --- | --- | --- |
| `REF -> REF` | `PythonTimeSeriesReferenceOutput/Input` | Peered; behaves like `TS[TimeSeriesReference]` |
| `TS -> REF` | `PythonTimeSeriesReferenceInput.do_bind_output` | Caches `TimeSeriesReference.make(output)`; not peered |
| `REF -> TS` | `TimeSeriesReference.bind_input` | Binds input to referenced output; rebinds on ref change |
| `value()` | `PythonTimeSeriesReferenceInput.value` | `output.value` if peered, else cached `_value` or composed from `_items` |
| `delta_value()` | `PythonTimeSeriesReferenceInput.delta_value` | Always equals `value` |

---

## SIGNAL

| User guide concept | Current Python API | Behavior re-created in Python |
| --- | --- | --- |
| tick/no data | `PythonTimeSeriesSignal.value/delta_value` | Always `True` |
| modified/valid | `PythonTimeSeriesSignal.modified/valid` | Aggregates child signals if present; else base input behavior |
| indexing | `PythonTimeSeriesSignal.__getitem__` | Creates child signals for free bundle/TSL bindings |

---

## TimeSeriesReference (Token)

| User guide concept | Current Python API | Behavior re-created in Python |
| --- | --- | --- |
| empty ref | `EmptyTimeSeriesReference` | `is_valid=False`, `is_empty=True`, `bind_input` unbinds |
| bound ref | `BoundTimeSeriesReference(output)` | `bind_input` binds input to output; `is_valid` mirrors output |
| unbound ref | `UnBoundTimeSeriesReference(items)` | Binds each item (if any) to child inputs; `is_valid` if any item valid |
| construction | `TimeSeriesReference.make(...)` | Builds Empty/Bound/UnBound based on input type |

