# REF (Reference) Behavior

Wrapper API surface:
- `TimeSeriesReference`: `__str__`, `__repr__`, `__eq__`, `bind_input()`, `has_output`, `is_empty`, `is_bound`,
  `is_unbound`, `is_valid`, `output`, `items`, `__getitem__`, `make(...)`.
- REF input/output wrappers: base `TimeSeriesInput`/`TimeSeriesOutput` methods plus type markers.

---

## Python behavior (current)

### Reference value types

**EmptyTimeSeriesReference**
- `bind_input(input)` unbinds the input.
- `is_valid == False`, `has_output == False`, `is_empty == True`.

**BoundTimeSeriesReference**
- Holds a direct output.
- `bind_input(input)` binds input to output; reactivates if needed.
- `is_valid` mirrors output validity.

**UnBoundTimeSeriesReference**
- Holds a list of references (used for collection references).
- `bind_input(input_iterable)` zips through child inputs and binds each item.
- `is_valid` if any child reference is valid.

### Reference output (`PythonTimeSeriesReferenceOutput`)
State:
- `_value`: a `TimeSeriesReference` or `None`.
- `_reference_observers`: inputs observing this reference (for rebinding).

Methods:
- `value = ref`:
  - `None` => `invalidate()`.
  - Validates type `TimeSeriesReference`.
  - Sets `_value`, marks modified, and rebinds all observers via `ref.bind_input`.
- `observe_reference(input)` / `stop_observing_reference(input)`:
  - Adds/removes inputs from observer list.
- `clear()`:
  - Sets `value` to `EmptyTimeSeriesReference()` (twice in code).
- `delta_value` equals `value`.

### Reference input (`PythonTimeSeriesReferenceInput`)
Reference input can bind to:
1. A `TimeSeriesReferenceOutput` (peer reference output).
2. A non-reference output (wraps it as a reference).
3. A list of child reference inputs (for collection refs).

State:
- `_value`: cached reference value (if not directly bound to a REF output).
- `_items`: list of child reference inputs (for unbound collection references).

Binding behavior:
- If bound to a `TimeSeriesReferenceOutput`:
  - Delegates to base binding and uses `output.value` as reference value.
- If bound to a non-reference output:
  - Sets `_value = TimeSeriesReference.make(output)`.
  - Leaves `_output = None` (not peered).
  - If node started, samples immediately.
- `un_bind_output()` clears `_value` and unbinds all child items.

Property semantics:
- `value` resolves in order:
  1. `_output.value` if bound to REF output.
  2. `_value` if cached.
  3. If `_items` exist, creates `TimeSeriesReference.make(from_items=...)` and caches.
- `delta_value` equals `value`.
- `modified`:
  - `True` if sampled, or output modified, or any child modified.
- `valid`:
  - `True` if `_value` exists, any child valid, or output valid.
- `all_valid`:
  - `True` if all child refs valid or `_value` exists or base all_valid.

Specialized reference inputs:
- `PythonTimeSeriesListReferenceInput`:
  - Enforces max size; creates **all items at once** on first access.
- `PythonTimeSeriesBundleReferenceInput`:
  - Supports int index and string key access; uses generic lazy item creation.
- Other specialized reference inputs/outputs are markers only.

---

## C++ proxy behavior (current)

### TimeSeriesReference wrapper
- `bind_input(input)` only supports `EMPTY` and `VIEW_BOUND` references.
  - `VIEW_BOUND` binds a passthrough input to a view-backed output via `set_bound_output`.
- `make(ts, from_items)`:
  - Accepts view-based input/output wrappers, legacy outputs, or references.
  - For view-based wrappers, uses view-root or bound output pointer to create a view-bound reference.
- `output` returns:
  - For view-bound: an **input view wrapper** for the referenced output (or element for list refs).
  - For legacy bound: returns wrapped output object.

### REF rebinding on inputs
`PyTimeSeriesInput` includes REF-aware handling:
- `value()` and `delta_value()` resolve from `TSRefTargetLink` if bound to REF output.
- On rebind at current tick:
  - For TSL: `delta_value` returns dict of all elements.
  - For TSD: returns full dict plus REMOVE entries for keys no longer present.
  - For TSS: returns a `PythonSetDelta` with full added/removed sets.

Limitations:
- BOUND/UNBOUND legacy reference modes are not supported in view-based wrappers beyond `make(...)`.

---

## Behavioral examples

### Example: REF input bound to non-REF output
- Input binds to output `O` (non-reference).
- `input.value` becomes `TimeSeriesReference.make(O)` (not peered).
- `modified` ticks on binding due to sampling.

### Example: REF rebinding
- REF output changes target from A to B at t10.
- Input `delta_value` at t10 returns full value of B (not just delta), per special handling.

---

## Usage modes and state machines (Python)

The REF type behaves differently depending on *what is bound to what*. There are three distinct modes:

1) **TimeSeries -> REF** (non-reference output bound to REF input)  
2) **REF -> REF** (reference output bound to reference input)  
3) **REF -> TimeSeries** (TimeSeriesReference binds a TimeSeries input to a concrete output)

### Mode 1: TimeSeries -> REF (non-reference output to REF input)
This path is *not peered*. The REF input caches a `TimeSeriesReference` that wraps the time-series output.

| Tick / Action | input.value | input.delta_value | input.modified | input.valid | notes |
| --- | --- | --- | --- | --- | --- |
| t0: unbound | `None` | `None` | `False` | `False` | no cached reference |
| t1: bind to non-REF output `O` | `TimeSeriesReference.make(O)` | same as value | `True` | `True` | `_value` cached; `_output` stays `None` |
| t2: underlying `O` modified | `TimeSeriesReference.make(O)` | same as value | `True` | `True` | `modified` becomes True because input sampled or child modified |
| t3: unbind | `None` | `None` | `True` | `False` | `un_bind_output()` clears cached value, samples |

#### Binding timeline (fresh)
```
ref_in (REF input)       time-series output O       TimeSeriesReference
t0: unbound               valid? no                 none
t1: bind to O   ------->  O                          make(O) cached
                           (no peer)                 input.value = ref(O)
                           sample_time = t1          modified=True at t1
```

#### Rebinding timeline (non-ref output changes)
```
t2: O ticks/modified
ref_in.modified -> True (from sampling/child activity), value remains ref(O)
delta_value == value (ref(O)) for this tick
```

### Mode 2: REF -> REF (reference output to reference input)
This is equivalent to `TS[TimeSeriesReference] -> TS[TimeSeriesReference]` (peered). The only special cases are adaptor
paths where input/output types do not match.

| Tick / Action | ref_out.value | input.value | input.delta_value | input.modified | input.valid | notes |
| --- | --- | --- | --- | --- | --- | --- |
| t0: ref_out empty | EmptyRef | EmptyRef | EmptyRef | `False` | `False` | input bound, but empty reference |
| t1: ref_out -> BoundRef(A) | BoundRef(A) | BoundRef(A) | BoundRef(A) | `True` | `True` | observers rebind to A |
| t2: A modified | BoundRef(A) | BoundRef(A) | BoundRef(A) | `True` | `True` | modified reflects upstream change |
| t3: ref_out -> BoundRef(B) | BoundRef(B) | BoundRef(B) | BoundRef(B) | `True` | `True` | rebinding occurs in same tick |
| t4: ref_out -> EmptyRef | EmptyRef | EmptyRef | EmptyRef | `True` | `False` | unbind from target |

#### Binding timeline (fresh)
```
ref_out (REF out)         ref_in (REF in)            TimeSeriesReference
t0: EmptyRef  --------->  bound to ref_out           ref_in.value = EmptyRef
t1: BoundRef(A) update -> ref_in observes, rebinds   ref_in.value = ref(A)
                           sample_time = t1          modified=True at t1
```

#### Rebinding timeline (ref target changes)
```
t2: ref_out.value = BoundRef(B)
ref_in rebinding -> unbind A, bind B
sample_time = t2, modified=True at t2
delta_value == value (BoundRef(B)) for this tick
```

### Mode 3: REF -> TimeSeries (TimeSeriesReference binds TimeSeries input)
This path is driven by `TimeSeriesReference.bind_input(...)`, which connects concrete outputs to time-series inputs.

| Tick / Action | reference kind | time-series input binding | input.value | input.modified | input.valid |
| --- | --- | --- | --- | --- | --- |
| t0: EmptyRef | EmptyRef | unbound | `None` | `False` | `False` |
| t1: BoundRef(A).bind_input(ts_in) | BoundRef(A) | bound to `A` | `A.value` | `True` | `A.valid` |
| t2: UnBoundRef([A,B]).bind_input(ts_list) | UnBoundRef | each child bound | child values | any child modified | any child valid |
| t3: switch to EmptyRef | EmptyRef | unbound | `None` | `True` | `False` |

#### Binding timeline (TimeSeriesReference -> input)
```
TimeSeriesReference      time-series input          effect
EmptyRef.bind_input      ts_in                      unbinds
BoundRef(A).bind_input   ts_in                      binds ts_in to A
UnBoundRef([A,B]).bind_input  ts_list               binds each child
```

---

## Tick-by-tick traces (per method)

### TimeSeriesReference.bind_input
| Tick / Action | input binding |
| --- | --- |
| t1: empty ref | unbound |
| t1: bound ref | bound to output; may re-activate |
| t1: unbound ref (composite) | binds each child input to its ref |

### Reference output value
| Tick / Action | value | delta_value | observer effect |
| --- | --- | --- | --- |
| t1: set to BoundRef | BoundRef | BoundRef | observers rebind |
| t2: set to EmptyRef | EmptyRef | EmptyRef | observers unbind |

### Reference input value
| Tick / Action | value | delta_value |
| --- | --- | --- |
| t1: bound to REF output | output.value | output.value |
| t2: bound to non-REF output | `TimeSeriesReference.make(output)` | `TimeSeriesReference.make(output)` |
| t3: bound to items | `TimeSeriesReference.make(from_items=...)` | `TimeSeriesReference.make(from_items=...)` |

### REF rebind delta handling (proxy)
| Tick / Action | delta_value |
| --- | --- |
| t1: REF target switches | full value/delta for TSL/TSD/TSS |

---

## TimeSeriesReference construction and propagation

`TimeSeriesReference` is the **token** that represents "what the REF points to." It is constructed and propagated in
three main ways:

| Source | Construction | Notes |
| --- | --- | --- |
| REF output assigns | `ref_out.value = TimeSeriesReference.make(output)` | Observers rebind immediately |
| REF input bound to non-REF output | `TimeSeriesReference.make(output)` cached in input | Not peered, uses sample_time |
| REF input bound to item refs | `TimeSeriesReference.make(from_items=refs)` | Used for TSL/TSD/TSS/TSB references |

When a `TimeSeriesReference` is updated:
- **REF output** immediately calls `bind_input` on observers, which triggers rebind.
- **REF input** may produce a *new* `TimeSeriesReference` (cached or composed from items) and expose it through `value`.

---

## Delta and sample-time notes (rebind-focused)

Rebinding is treated as a *sampling event*:
- `bind_output(...)` or reference update sets `sample_time = evaluation_time` for the REF input.
- That sampling causes `modified == True` even if underlying value did not tick in the same cycle.
- `delta_value` equals `value` for REF inputs/outputs, so rebind produces a **full reference tick**.

Special deltas on rebind (proxy-aware):
- **TSL**: delta includes *all* elements (full list).
- **TSD**: delta includes full dict plus `REMOVE` for keys no longer present.
- **TSS**: delta is `SetDelta(added=..., removed=...)` against the previous bound output.

Implication: rebinds are **not incremental**; they snapshot the new target and emit a full delta to align downstream
state with the new reference target.

---

## Trace results (Python, HGRAPH_USE_CPP=0)

These traces use `eval_node` with explicit REF wiring. They expose behaviors that are easy to miss in design docs.

### Trace A: TimeSeries -> REF (non-ref output bound to REF input)
Inputs: `ts=[1,2,None,3]`, wiring `ref_in` bound to `ts` output.

| Tick | ref_in.value | ref_in.modified | ref_in.valid | ref_in.bound | ref_in.has_peer | ref_in.sampled | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| t1 | BoundRef(ts) | `True` | `True` | `False` | `False` | `True` | bind triggers sample_time |
| t2 | BoundRef(ts) | `False` | `True` | `False` | `False` | `False` | **ts changed but ref_in.modified is False** |
| t3 | (no tick) | — | — | — | — | — | no upstream tick |
| t4 | BoundRef(ts) | `False` | `True` | `False` | `False` | `False` | ts changed, ref_in still not modified |

Key takeaway: **non-ref REF inputs do not tick on underlying TS changes**; only binding/sampling changes `modified`.

### Trace B: REF -> REF (REF output bound to REF input)
Inputs: `mode=[0,1,1,0]`, `ts=[10,20,None,30]`, `ref_out = EmptyRef if mode==0 else ref_in.value`.

| Tick | ref_out.value | ref_in.value | ref_in.modified | ref_in.valid | ref_in.bound | ref_in.has_peer | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| t1 | EmptyRef | EmptyRef | `True` | `True` | `True` | `True` | **valid True even for EmptyRef** |
| t2 | BoundRef(ts) | BoundRef(ts) | `True` | `True` | `True` | `True` | rebind triggers modified |
| t3 | BoundRef(ts) | BoundRef(ts) | `True` | `True` | `True` | `True` | ref_out tick repeats -> modified |
| t4 | EmptyRef | EmptyRef | `True` | `True` | `True` | `True` | unbind still reports valid |

Key takeaway: **REF inputs bound to REF outputs stay `valid=True` even when reference is EmptyRef**; validity here is about
the presence of a ref output, not about the underlying target being valid.

### Trace C: REF -> TimeSeries via UnBoundTimeSeriesReference (TSL)
Inputs: `mode=[0,1,1,1,0]`, `ts1=[1,None,2,None,None]`, `ts2=[None,10,None,20,None]`.  
`ref_list = UnBoundRef([ref_a.value, ref_b.value])`, `ref_a/ref_b` are REF inputs bound to `ts1/ts2`.

| Tick | ref_list.value | item0.bound | item1.bound | item0.valid | item1.valid | notes |
| --- | --- | --- | --- | --- | --- | --- |
| t1 | EmptyRef | `False` | `False` | `False` | `False` | items created after bind |
| t2 | UnBoundRef([A,B]) | `False` | `False` | `False` | `False` | **UnBoundRef does not bind items** |
| t3 | UnBoundRef([A,B]) | `False` | `False` | `False` | `False` | rebind still no item binding |
| t4 | UnBoundRef([A,B]) | `False` | `False` | `False` | `False` | unchanged |
| t5 | EmptyRef | `False` | `False` | `False` | `False` | unbound |

Key takeaway: **UnBoundTimeSeriesReference does not bind child items when the REF input is peered** (has_peer=True).
`bind_input()` unbinds the peered REF input first (clearing items), so the subsequent zip over items is empty.

This is a critical edge case for REF[TSL]/REF[TSB]/REF[TSD]/REF[TSS] behavior: if the REF input is created as a peered
reference, *child bindings never materialize* unless the binding model changes.

### Trace D: Non-peered `TSD[str, REF[TS]]` (nested graph style)
Inputs: `tsd=[{"a":1}, {"a":2}, {"b":3}, {"a": REMOVE}]`, wired into a graph that expects `TSD[str, REF[TS[int]]]`.
This forces **non-peered** binding because the output value type is `TS[int]` and the input value type is `REF[TS[int]]`.

| Tick | tsd.has_peer | keys | item(a).has_peer | item(a).modified | item(b).modified | notes |
| --- | --- | --- | --- | --- | --- | --- |
| t1 | `False` | `{a}` | `False` | `True` | — | key add creates ref wrapper |
| t2 | — | — | — | — | — | **no tick when `a` updates** |
| t3 | `False` | `{a,b}` | `False` | `False` | `True` | `b` added, `a` still not modified |
| t4 | `False` | `{b}` | — | — | `False` | `a` removed; `b` does not tick |

Key takeaway: **`TSD[str, REF[TS]]` can be non-peered**, and child REF inputs are **non-peered** too. Updates to the
underlying `TS` values do not mark the REF items as modified; only key-level add/remove changes tick the TSD.
