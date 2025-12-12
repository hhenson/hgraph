# Time-Series Type System Analysis

**Last Updated:** 2025-12-12
**Purpose:** Comprehensive analysis of the hgraph time-series type system for refactoring preparation

---

## Table of Contents

1. [Overview](#overview)
2. [Type Hierarchy](#type-hierarchy)
3. [State Management](#state-management)
4. [Input/Output Binding](#inputoutput-binding)
5. [Collection Types](#collection-types)
6. [Reference Types (REF)](#reference-types-ref)
7. [Node Lifecycle Integration](#node-lifecycle-integration)
8. [State Transitions](#state-transitions)
9. [Key Invariants](#key-invariants)
10. [Testing Requirements](#testing-requirements)

---

## Overview

The hgraph time-series type system implements a reactive dataflow model where:

- **Outputs** hold values and notify subscribers when modified
- **Inputs** wrap outputs and provide access to values
- **Nodes** are scheduled for evaluation when their active inputs are notified
- **State** is tracked via timestamps (`last_modified_time`) relative to `MIN_DT`

### Core Types

| Type | Description | Value Semantics |
|------|-------------|-----------------|
| `TS[T]` | Scalar time-series | Single value of type T |
| `TSL[T, Size]` | Time-series list | Fixed-size list of time-series |
| `TSB[Schema]` | Time-series bundle | Named heterogeneous fields |
| `TSD[K, V]` | Time-series dict | Dynamic keyed collection |
| `TSS[T]` | Time-series set | Set with add/remove delta |
| `TSW[T, Size]` | Time-series window | Circular buffer of values |
| `REF[T]` | Time-series reference | Pointer to another time-series |
| `SIGNAL` | Signal input | Only ticked state, no value |

---

## Type Hierarchy

### Abstract Base Classes

```
TimeSeries (ABC) - /hgraph/_types/_time_series_types.py
├── Properties: owning_node, owning_graph, value, delta_value, modified, valid, all_valid, last_modified_time
├── Methods: re_parent(), is_reference()
│
├── TimeSeriesOutput
│   ├── Properties: parent_output, has_parent_output, value (setter)
│   ├── Methods: can_apply_result(), apply_result(), subscribe(), unsubscribe()
│   ├── Methods: copy_from_output(), copy_from_input(), clear(), invalidate()
│   └── Methods: mark_invalid(), mark_modified()
│
├── TimeSeriesInput
│   ├── Properties: parent_input, has_parent_input, bound, has_peer, output, active
│   ├── Methods: bind_output(), un_bind_output(), do_bind_output(), do_un_bind_output()
│   └── Methods: make_active(), make_passive()
│
└── TimeSeriesSignalInput (extends TimeSeriesInput)
    └── Properties: value (always True)
```

### Python Implementation Classes

```
/hgraph/_impl/_types/

_output.py:
└── PythonTimeSeriesOutput (ABC)
    ├── Fields: _parent_or_node, _subscribers[], _last_modified_time
    └── Implements: all output state management

_input.py:
├── PythonTimeSeriesInput (ABC)
│   └── Fields: _parent_or_node
│
└── PythonBoundTimeSeriesInput (ABC)
    ├── Fields: _output, _reference_output, _active, _sample_time, _notify_time
    └── Implements: binding, activation, notification

_ts.py:
├── PythonTimeSeriesValueOutput
│   └── Fields: _tp, _value
└── PythonTimeSeriesValueInput
    └── (inherits from PythonBoundTimeSeriesInput)

_tsl.py:
├── PythonTimeSeriesListOutput
└── PythonTimeSeriesListInput

_tsb.py:
├── PythonTimeSeriesBundleOutput
└── PythonTimeSeriesBundleInput

_tsd.py:
├── PythonTimeSeriesDictOutput
│   └── Fields: _key_observers[], _ts_builder, _removed_items{}, _added_keys{}, _modified_items{}
└── PythonTimeSeriesDictInput
    └── Fields: _prev_output, _has_peer, _ts_builder, _removed_items{}, _modified_items{}

_tss.py:
├── PythonTimeSeriesSetOutput
│   └── Fields: _tp, _value (set), _added, _removed
└── PythonTimeSeriesSetInput
    └── Fields: _prev_output

_tsw.py:
├── PythonTimeSeriesFixedWindowOutput
│   └── Fields: _tp, _value (array), _times (array), _size, _min_size, _start, _length, _removed_value
├── PythonTimeSeriesTimeWindowOutput
│   └── Fields: _tp, _value (deque), _times (deque), _size, _min_size, _ready, _removed_values
└── PythonTimeSeriesWindowInput

_ref.py:
├── PythonTimeSeriesReferenceOutput
│   └── Fields: _tp, _value (TimeSeriesReference), _reference_observers{}
├── PythonTimeSeriesReferenceInput
│   └── Fields: _value, _items[]
└── Specialized: PythonTimeSeriesValueReferenceInput, PythonTimeSeriesListReferenceInput, etc.

_signal.py:
└── PythonTimeSeriesSignal
    └── Fields: _ts_values[]
```

---

## State Management

### Core State Properties

#### Output State

| Property | Definition | Type |
|----------|------------|------|
| `_last_modified_time` | Timestamp of last modification | `datetime` |
| `valid` | `last_modified_time > MIN_DT` | `bool` |
| `modified` | `evaluation_time == last_modified_time` | `bool` |
| `all_valid` | All elements valid (collection-specific) | `bool` |
| `_subscribers` | List of inputs to notify | `list[TimeSeriesInput]` |

#### Input State

| Property | Definition | Type |
|----------|------------|------|
| `_output` | Bound output (if peered) | `TimeSeriesOutput | None` |
| `_active` | Subscribed to output changes | `bool` |
| `_sample_time` | Time when sampled (for binding/unbinding notifications) | `datetime` |
| `_notify_time` | Last notification time (for dedup) | `datetime` |
| `bound` | `_output is not None` (simple) or `any(child.bound)` (collection) | `bool` |
| `has_peer` | Bound to single matching output | `bool` |
| `_sampled` | `_sample_time == evaluation_time` | `bool` |

### State Derivation Formulas

```python
# Output
output.valid = output._last_modified_time > MIN_DT
output.modified = output.owning_graph.evaluation_clock.evaluation_time == output._last_modified_time

# Input (simple)
input.valid = input.bound and input._output.valid
input.modified = (input._output is not None and input._output.modified) or input._sampled
input.last_modified_time = max(input._output.last_modified_time, input._sample_time) if input.bound else MIN_DT

# Input (collection, non-peered)
input.valid = any(child.valid for child in input.values())
input.modified = any(child.modified for child in input.values())
input.last_modified_time = max(child.last_modified_time for child in input.values())
```

---

## Input/Output Binding

### Binding Flow

```
1. input.bind_output(output)
   │
   ├── If output is TimeSeriesReferenceOutput:
   │   ├── output.observe_reference(input)    # Register for reference changes
   │   ├── output.value.bind_input(input)     # Bind to referenced output
   │   └── input._reference_output = output
   │
   └── Else (normal binding):
       ├── input.do_bind_output(output)
       │   ├── input.make_passive()           # Unsubscribe from old output
       │   ├── input._output = output
       │   └── if was_active: input.make_active()  # Subscribe to new output
       │
       └── If node started and output valid:
           ├── input._sample_time = evaluation_time
           └── if input.active: input.notify()
```

### Subscription Flow

```
input.make_active():
│
├── Set _active = True
├── If _output is not None:
│   ├── _output.subscribe(self)               # Add to subscribers list
│   └── If _output.valid and _output.modified:
│       └── self.notify()                      # Immediate notification
│
└── If _sampled:
    └── self.notify()                          # Sampled notification

input.make_passive():
│
├── Set _active = False
└── If _output is not None:
    └── _output.unsubscribe(self)             # Remove from subscribers
```

### Notification Flow

```
output.mark_modified(modified_time):
│
├── Update _last_modified_time
├── If has_parent_output:
│   └── parent.mark_child_modified(self, modified_time)
│
└── _notify(modified_time):
    └── For each subscriber in _subscribers:
        └── subscriber.notify(modified_time)

input.notify(modified_time):
│
├── If _notify_time == modified_time: return   # Dedup
├── Update _notify_time
│
└── If has_parent_input:
│   └── parent.notify_parent(self, modified_time)
└── Else:
    └── owning_node.notify(modified_time)
```

### Peered vs Non-Peered Binding

**Peered Binding** (`has_peer = True`):
- Input wraps a single output of matching type
- State delegates directly to output: `input.valid = output.valid`
- Single subscription: input subscribes to output

**Non-Peered Binding** (`has_peer = False`):
- Input's children wrap multiple independent outputs
- State computed from children: `input.valid = any(child.valid)`
- Multiple subscriptions: each child subscribes independently
- Example: `TSL[TS[int], Size[2]]` bound to two separate `TS[int]` outputs

---

## Collection Types

### TSL (Time-Series List)

**Output Structure:**
```python
class PythonTimeSeriesListOutput:
    _ts_values: list[TimeSeriesOutput]  # Fixed-size list from __init__

    @property
    def value(self) -> tuple:
        return tuple(ts.value if ts.valid else None for ts in self._ts_values)

    @property
    def delta_value(self) -> dict[int, Any]:
        return {i: ts.delta_value for i, ts in enumerate(self._ts_values) if ts.modified}

    @property
    def all_valid(self) -> bool:
        return all(ts.valid for ts in self.values())
```

**Input Peering Logic:**
```python
def do_bind_output(self, output):
    peer = True
    for ts_input, ts_output in zip(self.values(), output.values()):
        peer &= ts_input.bind_output(ts_output)  # All must be peered
    super().do_bind_output(output if peer else None)
    return peer
```

### TSB (Time-Series Bundle)

**Output Structure:**
```python
class PythonTimeSeriesBundleOutput:
    _ts_value: dict[str, TimeSeriesOutput]  # Named fields from schema
    __schema__: TimeSeriesSchema

    @property
    def value(self):
        if s := self.__schema__.scalar_type():
            return s(**{k: ts.value for k, ts in self.items()})
        else:
            return {k: ts.value for k, ts in self.items() if ts.valid}

    @property
    def delta_value(self):
        return {k: ts.delta_value for k, ts in self.items() if ts.modified and ts.valid}
```

**Key Difference from TSL:**
- Heterogeneous field types (each field can be different type)
- String keys from schema vs integer indices
- Optional scalar type mapping for structured return

### TSD (Time-Series Dict)

**Output Structure:**
```python
class PythonTimeSeriesDictOutput:
    _ts_values: dict[K, V]                # Dynamic keyed collection
    _key_set: TimeSeriesSetOutput         # Tracks keys as TSS
    _removed_items: dict[K, V]            # Items removed this cycle
    _added_keys: set[K]                   # Keys added this cycle
    _modified_items: dict[K, V]           # Items modified this cycle
    _key_observers: list[TSDKeyObserver]  # Observe key changes

    @property
    def delta_value(self):
        return frozendict(
            chain(
                ((k, v.delta_value) for k, v in self.items() if v.modified and v.valid),
                ((k, REMOVE) for k in self.removed_keys()),
            )
        )
```

**Key Observer Pattern:**
```python
class TSDKeyObserver:
    def on_key_added(self, key: K): ...
    def on_key_removed(self, key: K): ...

# TSD Input implements TSDKeyObserver
class PythonTimeSeriesDictInput(TSDKeyObserver):
    def on_key_added(self, key):
        v = self.get_or_create(key)
        v.bind_output(self.output[key])

    def on_key_removed(self, key):
        value = self._ts_values.pop(key, None)
        if value.parent_input is self:
            self._removed_items[key] = (value, value.valid)
```

**Cleanup Mechanism:**
- After-evaluation callbacks clean up removed items
- `_clear_key_changes()` releases builders and unbinds

### TSS (Time-Series Set)

**Output Structure:**
```python
class PythonTimeSeriesSetOutput:
    _value: set[SCALAR]           # Current set
    _added: set[SCALAR] | None    # Added this cycle
    _removed: set[SCALAR] | None  # Removed this cycle
    _contains_ref_outputs: FeatureOutputExtension  # For contains() queries
    _is_empty_ref_output: TimeSeriesOutput         # For is_empty() queries

    @property
    def delta_value(self) -> SetDelta:
        return set_delta(self._added, self._removed, self._tp)
```

**Delta Semantics:**
```python
class SetDelta:
    added: frozenset[SCALAR]
    removed: frozenset[SCALAR]

    def __add__(self, other: SetDelta) -> SetDelta:
        # Combine deltas: re-add cancels remove, new remove cancels old add
        added = (self.added - other.removed) | other.added
        removed = (other.removed - self.added) | (self.removed - other.added)
        return SetDelta(added=added, removed=removed)
```

**Rebinding Delta Computation:**
When input rebinds to different output:
```python
def added(self):
    if self._prev_output is not None:
        # Old values + what was removed - what was only added = original state
        # New values - original state = what was added
        original = (self._prev_output.values() | self._prev_output.removed()) - self._prev_output.added()
        return self.values() - original
    elif self._sampled:
        return self.values()  # Everything is "added" on sample
    else:
        return self.output.added()
```

### TSW (Time-Series Window)

**Fixed Window (Count-Based):**
```python
class PythonTimeSeriesFixedWindowOutput:
    _value: Array[SCALAR]         # Circular buffer
    _times: Array[datetime]       # Timestamps for each value
    _size: int                    # Buffer capacity
    _min_size: int                # Minimum for valid
    _start: int                   # Circular buffer start index
    _length: int                  # Current length
    _removed_value: SCALAR | None # Value dropped on overflow

    @property
    def all_valid(self) -> bool:
        return self.valid and self._length >= self._min_size

    def apply_result(self, result):
        # Add to circular buffer
        if self._length >= self._size:
            self._removed_value = self._value[self._start]
            self._start = (self._start + 1) % self._size
        pos = (self._start + self._length) % self._size
        self._value[pos] = result
        self._times[pos] = evaluation_time
        self.mark_modified()
```

**Time Window (Duration-Based):**
```python
class PythonTimeSeriesTimeWindowOutput:
    _value: deque[SCALAR]         # Dynamic buffer
    _times: deque[datetime]       # Timestamps
    _size: timedelta              # Window duration
    _min_size: timedelta          # Minimum duration for valid
    _ready: bool                  # Has min_size time passed
    _removed_values: tuple | None # Values dropped by rolling

    def _roll(self):
        # Remove values outside time window
        tm = evaluation_time - self._size
        removed = []
        while self._times and self._times[0] < tm:
            self._times.popleft()
            removed.append(self._value.popleft())
        if removed:
            self._removed_values = tuple(removed)
```

---

## Reference Types (REF)

### Reference Abstractions

```python
class TimeSeriesReference(ABC):
    @abstractmethod
    def bind_input(self, input_: TimeSeriesInput): ...

    @property
    @abstractmethod
    def is_valid(self) -> bool: ...

    @property
    @abstractmethod
    def has_output(self) -> bool: ...

    @property
    @abstractmethod
    def is_empty(self) -> bool: ...
```

### Reference Types

**BoundTimeSeriesReference:**
```python
class BoundTimeSeriesReference(TimeSeriesReference):
    output: TimeSeriesOutput

    def bind_input(self, input_):
        if input_.bound and not input_.has_peer:
            input_.un_bind_output()
        input_.bind_output(self.output)

    @property
    def is_valid(self) -> bool:
        return self.output.valid
```

**UnBoundTimeSeriesReference:**
```python
class UnBoundTimeSeriesReference(TimeSeriesReference):
    items: list[TimeSeriesReference]  # For non-peered collections

    def bind_input(self, input_):
        for item, r in zip(input_, self.items):
            if r:
                r.bind_input(item)
            elif item.bound:
                item.un_bind_output()
```

**EmptyTimeSeriesReference:**
```python
class EmptyTimeSeriesReference(TimeSeriesReference):
    def bind_input(self, input_):
        input_.un_bind_output()  # Unbind since no output

    @property
    def is_empty(self) -> bool:
        return True
```

### REF Input Binding Modes

**Peered Mode (REF[X] → REF[X]):**
```python
def do_bind_output(self, output):
    if isinstance(output, TimeSeriesReferenceOutput):
        self._value = None  # Clear cached value
        return super().do_bind_output(output)  # Normal binding
```

**Non-Peered Mode (REF[X] → X):**
```python
def do_bind_output(self, output):
    # output is NOT a TimeSeriesReferenceOutput
    self._value = TimeSeriesReference.make(output)  # Wrap in reference
    self._output = None  # No direct output binding
    if self.owning_node.is_started:
        self._sample_time = evaluation_time
        self.notify(self._sample_time)
    else:
        self.owning_node.start_inputs.append(self)  # Schedule for start
    return False  # Not peered
```

### Reference Observer Pattern

```python
class PythonTimeSeriesReferenceOutput:
    _reference_observers: dict[int, TimeSeriesInput]

    @value.setter
    def value(self, v: TimeSeriesReference):
        self._value = v
        self.mark_modified()
        # Rebind all observers to new reference
        for observer in self._reference_observers.values():
            self._value.bind_input(observer)

    def observe_reference(self, input_):
        self._reference_observers[id(input_)] = input_

    def stop_observing_reference(self, input_):
        self._reference_observers.pop(id(input_), None)
```

---

## Node Lifecycle Integration

### Node State Machine

```
           ┌─────────┐
           │  INIT   │  (constructor)
           └────┬────┘
                │ initialise()
                ▼
           ┌─────────┐
           │ READY   │  (inputs/outputs set)
           └────┬────┘
                │ start()
                ▼
  ┌────────►┌─────────┐◄────────┐
  │SCHEDULE │STARTING │ NOTIFY  │
  │         └────┬────┘         │
  │              │              │
  │              ▼              │
  │         ┌─────────┐         │
  │         │ STARTED │◄────────┘
  │         └────┬────┘
  │              │ stop()
  │              ▼
  │         ┌─────────┐
  │         │ STOPPING│
  │         └────┬────┘
  │              │
  │              ▼
  │         ┌─────────┐
  └─────────│ STOPPED │
            └─────────┘
```

### Node.start() Sequence

```python
def start(self):
    self._initialise_kwargs()      # Build kwargs dict
    self._initialise_inputs()      # Activate inputs, call start() on scheduled inputs
    self._initialise_state()       # Restore recordable state if recovering
    self.do_start()                # User start function

    # Handle scheduler
    if self._scheduler.pop_tag("start", None) is not None:
        self.notify()              # Schedule for evaluation
```

### Input Initialization

```python
def _initialise_inputs(self):
    # 1. Call start() on inputs that were scheduled (REF non-peered)
    for i in self._start_inputs:
        i.start()  # Triggers sample notification

    # 2. Make active inputs active
    for k, ts in self.input.items():
        if k in self.signature.active_inputs:
            ts.make_active()  # Subscribes and may notify
```

### Node.eval() Validity Checks

```python
def eval(self):
    # Check valid_inputs constraint
    if self.signature.valid_inputs:
        if not all(self.input[k].valid for k in self.signature.valid_inputs):
            return  # Skip evaluation

    # Check all_valid_inputs constraint
    if self.signature.all_valid_inputs:
        if not all(self.input[k].all_valid for k in self.signature.all_valid_inputs):
            return  # Skip evaluation

    # Check scheduler - must be scheduled or have modified inputs
    if self.signature.uses_scheduler:
        if not scheduled and not any(self.input[k].modified for k in inputs):
            return  # Skip evaluation

    # Execute
    self.do_eval()
```

### Node.notify() Scheduling

```python
def notify(self, modified_time=None):
    if self.is_started or self.is_starting:
        self.graph.schedule_node(self.node_ndx, modified_time or evaluation_time)
    else:
        # Before start - schedule for start
        self.scheduler.schedule(when=MIN_ST, tag="start")
```

---

## State Transitions

### Output State Transitions

```
                    ┌──────────────┐
                    │   INVALID    │  (_last_modified_time = MIN_DT)
                    │   valid=F    │
                    │   _value=None│
                    └──────┬───────┘
                           │ value = x
                           │ mark_modified()
                           ▼
              ┌────────────────────────┐
              │         VALID          │  (_last_modified_time > MIN_DT)
              │   valid=T              │
              │   _value=x             │
              └────────────────────────┘
                    │           │
     mark_modified()│           │invalidate()
     value = y      │           │mark_invalid()
                    ▼           ▼
           ┌────────────┐  ┌──────────────┐
           │  MODIFIED  │  │   INVALID    │
           │  modified=T│  │   valid=F    │
           │  _value=y  │  │   _value=None│
           └────────────┘  └──────────────┘
                    │
                    │ (next evaluation cycle)
                    ▼
           ┌────────────────┐
           │  NOT_MODIFIED  │  (evaluation_time > _last_modified_time)
           │  modified=F    │
           │  valid=T       │
           └────────────────┘
```

### Input State Transitions

```
                    ┌──────────────┐
                    │   UNBOUND    │  (_output = None)
                    │   bound=F    │
                    │   valid=F    │
                    └──────┬───────┘
                           │ bind_output(output)
                           ▼
              ┌────────────────────────┐
              │        BOUND           │  (_output = output)
              │   bound=T              │
              │   valid=output.valid   │
              └────────────────────────┘
                    │           │
      make_active() │           │ make_passive()
                    ▼           ▼
           ┌────────────┐  ┌────────────┐
           │   ACTIVE   │  │  PASSIVE   │
           │  active=T  │  │  active=F  │
           │ subscribed │  │ not subscr │
           └────────────┘  └────────────┘
                    │
                    │ un_bind_output()
                    ▼
           ┌────────────────┐
           │    UNBOUND     │
           │   bound=F      │
           │   _sampled=T   │  (if node started & was valid)
           └────────────────┘
```

### Collection Input (Non-Peered) Transitions

```
                    ┌──────────────────────┐
                    │     NON-PEERED       │
                    │   has_peer=F         │
                    │   _output=None       │
                    │   children[].bound=T │
                    └──────────┬───────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
          ▼                    ▼                    ▼
    ┌───────────┐        ┌───────────┐        ┌───────────┐
    │ valid=any │        │ modified= │        │ active=   │
    │ child.valid│       │ any child │        │ any child │
    └───────────┘        │ .modified │        │ .active   │
                         └───────────┘        └───────────┘
```

### Reference Input Transitions

```
                    ┌──────────────────────┐
                    │     UNBOUND REF      │
                    │   _output=None       │
                    │   _value=None        │
                    │   _items=None        │
                    └──────────┬───────────┘
                               │
           ┌───────────────────┼───────────────────┐
           │                   │                   │
           ▼                   ▼                   ▼
    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
    │ PEERED REF  │     │ NON-PEERED  │     │ NON-PEERED  │
    │ (REF→REF)   │     │ (REF→X)     │     │ (REF→items) │
    │ _output=ref │     │ _value=wrap │     │ _items=[]   │
    └─────────────┘     └─────────────┘     └─────────────┘
           │                   │                   │
           │                   │                   │
           ▼                   ▼                   ▼
    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
    │ value=      │     │ value=      │     │ value=make  │
    │ output.value│     │ _value      │     │ (from_items)│
    └─────────────┘     └─────────────┘     └─────────────┘
```

---

## Key Invariants

### 1. Modification Invariant
```python
# Output can only be modified once per evaluation cycle for same path
assert not output.modified or output.can_apply_result(new_value)

# Exception: Collections allow multiple child modifications
```

### 2. Validity Invariant
```python
# Valid implies has been modified at some point
assert output.valid == (output.last_modified_time > MIN_DT)

# Input validity depends on binding
assert input.valid == (input.bound and input._output.valid)  # peered
assert input.valid == any(child.valid for child in input.values())  # non-peered
```

### 3. Subscription Invariant
```python
# Active input is in output's subscriber list
assert (input in output._subscribers) == (input.active and input.bound)
```

### 4. Notification Deduplication
```python
# Input only notifies once per evaluation time
input.notify(time1)
input.notify(time1)  # No-op, _notify_time == time1
```

### 5. Sample Time Invariant
```python
# Sample time set on bind/unbind during started node
assert input._sampled implies (input._sample_time == evaluation_time)
```

### 6. Collection Peering Invariant
```python
# Collection is peered iff ALL children are peered
tsl.has_peer == all(child.has_peer for child in tsl.values())
```

### 7. Reference Observer Invariant
```python
# REF input observing REF output is registered
assert (id(input) in output._reference_observers) == (input._reference_output is output)
```

### 8. TSD Key Observer Invariant
```python
# TSD input observes TSD output keys when bound
assert (input in output._key_observers) == (input.output is output)
```

---

## Testing Requirements

### State Transition Tests Required

#### Output Tests

1. **TS Output State**
   - Initial state (invalid, value=None)
   - Set value → valid, modified
   - Next cycle → valid, not modified
   - Invalidate → invalid, value=None
   - Set value to None → invalidate

2. **Collection Output State**
   - Child modification propagates to parent
   - all_valid computation
   - delta_value accumulation
   - clear() vs invalidate() semantics

3. **TSS Output State**
   - Add/remove delta tracking
   - Same-cycle add+remove handling
   - Empty set transitions

4. **TSD Output State**
   - Key creation/deletion
   - Key observer notifications
   - Removed items cleanup

5. **TSW Output State**
   - Circular buffer overflow
   - min_size validity
   - Removed value tracking

#### Input Tests

1. **Binding Tests**
   - bind_output when unbound
   - bind_output when already bound (rebind)
   - un_bind_output preserving sample

2. **Activation Tests**
   - make_active subscribes
   - make_passive unsubscribes
   - Active + output modified → notify
   - Active + sampled → notify

3. **Peering Tests**
   - TSL all-peered binding
   - TSL partial-peered binding → non-peered
   - TSB peering with schema

4. **Notification Tests**
   - Notification deduplication
   - Parent notification propagation
   - Node scheduling on notify

#### Reference Tests

1. **REF Binding Modes**
   - REF → REF (peered)
   - REF → X (non-peered, wrapped)
   - REF → items (non-peered, unbound)

2. **Reference Observer**
   - Output value change rebinds observers
   - Stop observing cleans up

3. **Clone Binding**
   - Clone from peered ref
   - Clone from non-peered ref

#### Node Integration Tests

1. **Start Sequence**
   - Inputs activated in order
   - start_inputs called
   - Initial notify if scheduled

2. **Stop Sequence**
   - Inputs unbound
   - Scheduler reset

3. **Eval Validity Checks**
   - valid_inputs constraint
   - all_valid_inputs constraint
   - scheduler constraint

### Isolated Unit Tests (by Type)

Each type should have tests independent of other types where possible:

```
test_ts_output_state.py
test_ts_input_binding.py
test_tsl_output_state.py
test_tsl_input_peering.py
test_tsb_output_state.py
test_tsb_input_schema.py
test_tsd_output_keys.py
test_tsd_input_observer.py
test_tss_output_delta.py
test_tss_input_rebind.py
test_tsw_output_buffer.py
test_tsw_output_time.py
test_ref_binding_modes.py
test_ref_observer.py
test_signal_input.py
```

---

## File Reference

### Python Implementation Files

| File | Classes |
|------|---------|
| `_output.py` | `PythonTimeSeriesOutput` |
| `_input.py` | `PythonTimeSeriesInput`, `PythonBoundTimeSeriesInput` |
| `_ts.py` | `PythonTimeSeriesValueOutput`, `PythonTimeSeriesValueInput` |
| `_tsl.py` | `PythonTimeSeriesListOutput`, `PythonTimeSeriesListInput` |
| `_tsb.py` | `PythonTimeSeriesBundleOutput`, `PythonTimeSeriesBundleInput` |
| `_tsd.py` | `PythonTimeSeriesDictOutput`, `PythonTimeSeriesDictInput` |
| `_tss.py` | `PythonTimeSeriesSetOutput`, `PythonTimeSeriesSetInput` |
| `_tsw.py` | `PythonTimeSeriesFixedWindowOutput`, `PythonTimeSeriesTimeWindowOutput`, `PythonTimeSeriesWindowInput` |
| `_ref.py` | `PythonTimeSeriesReferenceOutput`, `PythonTimeSeriesReferenceInput`, specialized variants |
| `_signal.py` | `PythonTimeSeriesSignal` |

### Abstract Interface Files

| File | Classes |
|------|---------|
| `_time_series_types.py` | `TimeSeries`, `TimeSeriesOutput`, `TimeSeriesInput`, `TimeSeriesSignalInput`, `TimeSeriesIterable` |
| `_ts_type.py` | `TimeSeriesValueOutput`, `TimeSeriesValueInput` |
| `_tsl_type.py` | `TimeSeriesListOutput`, `TimeSeriesListInput` |
| `_tsb_type.py` | `TimeSeriesBundleOutput`, `TimeSeriesBundleInput` |
| `_tsd_type.py` | `TimeSeriesDictOutput`, `TimeSeriesDictInput` |
| `_tss_type.py` | `TimeSeriesSetOutput`, `TimeSeriesSetInput`, `SetDelta` |
| `_tsw_type.py` | `TimeSeriesWindowOutput`, `TimeSeriesWindowInput` |
| `_ref_type.py` | `TimeSeriesReferenceOutput`, `TimeSeriesReferenceInput`, `TimeSeriesReference` |
