# Base Time-Series Behavior (Input/Output)

This section is the deep behavioral spec for methods exposed by the C++ wrappers. Each method is described in
terms of the **current Python runtime behavior**, so the new API can re-implement it faithfully.

Wrapper API surface (authoritative list):
- Output (`TimeSeriesOutput` wrapper): `owning_node`, `owning_graph`, `value` (get/set), `delta_value`, `modified`,
  `valid`, `all_valid`, `last_modified_time`, `is_reference()`, `can_apply_result()`, `apply_result()`, `clear()`,
  `invalidate()`, `copy_from_output()`, `copy_from_input()`.
- Input (`TimeSeriesInput` wrapper): `owning_node`, `owning_graph`, `value`, `delta_value`, `modified`, `valid`,
  `all_valid`, `last_modified_time`, `is_reference()`, `bound`, `active`, `make_active()`, `make_passive()`,
  `bind_output()`, `un_bind_output()`, `has_peer`.

---

## Python behavior (current)

### Shared concepts
- **Evaluation time**: `evaluation_clock.evaluation_time` is the current tick timestamp.
- **Validity**: A series is valid if it has ever been set and not invalidated in its current state.
- **Modified**: A series is modified if it changed **during the current evaluation time**.
- **Sampling**: When bindings change mid-evaluation, inputs track `_sample_time` so they still count as modified.

### Output state and transitions (`PythonTimeSeriesOutput`)
Internal state:
- `_last_modified_time`: initialized to `MIN_DT` (invalid sentinel).
- `_subscribers`: list of inputs subscribed for notifications.

Derived properties:
- `valid == (_last_modified_time > MIN_DT)`.
- `modified == (evaluation_time == _last_modified_time)`.
- `all_valid == valid` (composites override).
- `last_modified_time == _last_modified_time`.

Methods:
- `mark_modified(modified_time=None)`:
  - If `modified_time` is `None`:
    - Uses `evaluation_time` if `owning_node` exists.
    - Uses `MAX_ET` if no owning node.
  - If `modified_time` is newer than `_last_modified_time`, updates it.
  - If output has a parent output, calls `parent.mark_child_modified(self, modified_time)`.
  - Notifies all subscribers by calling `subscriber.notify(modified_time)`.
- `mark_invalid()`:
  - If previously valid, sets `_last_modified_time = MIN_DT` and notifies subscribers at current `evaluation_time`.
- `subscribe(node)` / `unsubscribe(node)`:
  - Adds/removes the input in `_subscribers`.
- `re_parent(parent)`:
  - Changes owning parent (node or parent output) without any notification side effects.

### Input state and transitions (`PythonBoundTimeSeriesInput`)
Internal state:
- `_output`: bound output or `None`.
- `_reference_output`: if bound through a `TimeSeriesReferenceOutput`.
- `_active`: whether input is subscribed to `_output` notifications.
- `_sample_time`: used to mark the input as modified when binding changes mid-tick.
- `_notify_time`: last time this input notified its parent (dedupe).

Derived properties:
- `bound == (_output is not None)`.
- `has_peer == (_output is not None)` (base assumption, overridden by composites).
- `value == _output.value` if bound, else `None`.
- `delta_value == _output.delta_value` if bound, else `None`.
- `modified == (_output.modified or _sampled)` if bound, else `False`.
- `_sampled == (_sample_time != MIN_DT and _sample_time == evaluation_time)`.
- `valid == (bound and _output.valid)`.
- `all_valid == (bound and _output.all_valid)`.
- `last_modified_time == max(_output.last_modified_time, _sample_time)` if bound, else `MIN_DT`.

Methods:
- `make_active()`:
  - If already active, no-op.
  - Sets `_active = True`.
  - If `_output` exists:
    - Subscribes to `_output`.
    - If `_output.valid and _output.modified`, immediately `notify(_output.last_modified_time)`.
    - Otherwise, if `_sampled`, notifies at `_sample_time`.
- `make_passive()`:
  - If active, unsubscribes from `_output` and sets `_active = False`.
- `notify(modified_time)`:
  - Dedupes by `_notify_time`.
  - If a parent input exists, calls `parent.notify_parent(...)`.
  - Otherwise, calls `owning_node.notify(modified_time)`.
- `bind_output(output)`:
  - If `output` is a `TimeSeriesReferenceOutput`:
    - If it has a value, it binds this input via that reference.
    - Registers as an observer via `output.observe_reference(self)`.
    - Sets `_reference_output` and returns `peer=False` (reference binding, not a direct peer).
  - Otherwise:
    - If already bound to the same output, returns `has_peer`.
    - Uses `do_bind_output(output)` (default sets `_output` and returns `True`).
  - If owning node is started/starting and output is valid (or was previously bound):
    - Sets `_sample_time = evaluation_time`.
    - If active, calls `notify(_sample_time)`.
- `un_bind_output(unbind_refs=False)`:
  - If `unbind_refs` and `_reference_output` exists, stops observing and clears reference.
  - If bound, calls `do_un_bind_output(...)` (default clears `_output` and unsubscribes).
  - If node is started and input was valid:
    - Sets `_sample_time = evaluation_time`.
    - If active, calls `owning_node.notify(_sample_time)`.
- `do_bind_output(output)` (default behavior):
  - Temporarily `make_passive()` (unsubscribe from old output).
  - Sets `_output = output`.
  - If previously active, `make_active()` (resubscribe to new output).
  - Returns `True` (peer binding).
- `do_un_bind_output(unbind_refs=False)` (default behavior):
  - Unsubscribes if active, sets `_output = None`.

### Notification chain
- Output triggers `input.notify(modified_time)`.
- Input dedupes by timestamp, then forwards to parent input or owning node.
- Parent inputs implement `notify_parent(child, modified_time)` to aggregate modifications.

---

## C++ proxy behavior (current)

The proxy is **view-based** (no subscription state). Differences to Python behavior:

- `active` is always `True`; `make_active`/`make_passive` are no-ops.
- `bound` is an explicit flag used only for REF passthrough (`_explicit_bound`).
- `bind_output`/`un_bind_output` just flip `_explicit_bound` and clear `_bound_output`.
- `has_peer` is always `False`.
- `modified`, `valid`, `delta_value` may be **REF-target aware** (see REF section).

---

## Behavioral timelines (Python)

### Example: bind, active, modified
- t0: Input unbound, inactive.
- t1: `bind_output(out)` on started node:
  - `_sample_time = t1` (if output valid or was bound).
  - If input active, notifies at t1.
- t1: `make_active()`:
  - Subscribes to output.
  - If output modified at t1, notifies immediately.
- t2: Output `mark_modified()`:
  - Input receives notify(t2), owning node scheduled.

### Example: unbind on active input
- t3: `un_bind_output()` on started node:
  - `_sample_time = t3` and `notify(t3)` if active.
  - `modified == True` for this tick even though output is now `None`.


---

## Tick-by-tick traces (per method)

### Output.value (get/set)
| Tick / Action | value | delta_value | modified | valid |
| --- | --- | --- | --- | --- |
| t0: initial (unset) | `None` | `None` | `False` | `False` |
| t1: set `value = V` | `V` | `V` | `True` | `True` |
| t2: no change | `V` | `None` | `False` | `True` |

### Output.modified
| Tick / Action | modified |
| --- | --- |
| t0: initial (unset) | `False` |
| t1: `mark_modified()` | `True` |
| t2: no change | `False` |

### Output.valid
| Tick / Action | valid |
| --- | --- |
| t0: initial (unset) | `False` |
| t1: set `value = V` | `True` |
| t2: `invalidate()` | `False` |

### Output.all_valid
| Tick / Action | all_valid |
| --- | --- |
| t0: scalar invalid | `False` |
| t1: scalar valid | `True` |
| t2: composite with one invalid child | `False` |

### Output.last_modified_time
| Tick / Action | last_modified_time |
| --- | --- |
| t1: modified | `evaluation_time` |
| t2: no change | unchanged |
| t3: `invalidate()` | `MIN_DT` |

### Output.apply_result
| Tick / Action | can_apply_result | value | modified |
| --- | --- | --- | --- |
| t1: `apply_result(V)` | `True` | `V` | `True` |
| t2: `apply_result(None)` | `True` | unchanged | `False` |
| t3: already modified, `apply_result(V2)` | `False` | unchanged | `True` |

### Output.can_apply_result
| Tick / Action | can_apply_result |
| --- | --- |
| t1: scalar already modified | `False` |
| t2: scalar not modified | `True` |
| t3: composite with conflicting child changes | `False` (type-specific) |

### Output.clear
| Tick / Action | value | modified |
| --- | --- | --- |
| t1: scalar `clear()` | unchanged | `False` |
| t1: composite `clear()` | emptied | `True` |

### Output.invalidate
| Tick / Action | valid | modified |
| --- | --- | --- |
| t1: `invalidate()` | `False` | `True` |

### Output.copy_from_output / copy_from_input
| Tick / Action | value | modified |
| --- | --- | --- |
| t1: copy same value | unchanged | `False` |
| t2: copy different value | updated | `True` |

### Input.value
| Tick / Action | value |
| --- | --- |
| t0: unbound | `None` |
| t1: bound to output | output.value |
| t2: unbound | `None` |

### Input.delta_value
| Tick / Action | delta_value |
| --- | --- |
| t1: bound, output modified | output.delta_value |
| t2: bound, no change | `None` |
| t3: unbound | `None` |

### Input.modified
| Tick / Action | modified |
| --- | --- |
| t1: output modified | `True` |
| t2: sampled (bind/unbind) | `True` |
| t3: no change | `False` |

### Input.valid / all_valid
| Tick / Action | valid | all_valid |
| --- | --- | --- |
| t1: bound, output valid | `True` | `True` |
| t2: output invalidated | `False` | `False` |
| t3: unbound | `False` | `False` |

### Input.last_modified_time
| Tick / Action | last_modified_time |
| --- | --- |
| t1: bound | `max(output.last_modified_time, sample_time)` |
| t2: unbound | `MIN_DT` |

### Input.bound / has_peer
| Tick / Action | bound | has_peer |
| --- | --- | --- |
| t0: initial (unbound) | `False` | `False` |
| t1: `bind_output(out)` | `True` | `True` |
| t2: `un_bind_output()` | `False` | `False` |

### Input.active / make_active / make_passive
- t0: active False.
- t1: make_active -> True and subscribes to output.
- t2: make_passive -> False and unsubscribes.

### Input.bind_output
- t1: binds to output; if output valid or previously bound, sets sample_time and notifies if active.

### Input.un_bind_output
- t1: clears binding; if previously valid and node started, samples and notifies if active.
