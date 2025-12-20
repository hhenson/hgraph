# HGraph TimeSeries Type Migration Plan

**Last Updated:** 2025-12-20
**Status:** Planning Phase
**Related Documents:**
- `time_series/TS_DESIGN.md` - Core type-erased design
- `value/VALUE_DESIGN.md` - Value system design
- `../../KNOWN_ISSUES.md` - Current defects and gaps

---

## Executive Summary

This document outlines the migration strategy for replacing the legacy Python-based TimeSeries implementation with a type-erased C++ approach. The goal is to achieve feature parity with Python behavioral contracts while leveraging C++ performance benefits.

**Current State:**
- Core type-erased infrastructure (Value, TypeMeta, TypeOps) is complete
- AccessStrategy hierarchy for inputs is implemented
- Basic TSOutput/TSInput scaffolding exists
- Node implementations are stubs (throw "not yet implemented")
- Python wrappers have incomplete functionality

**Target State:**
- Full behavioral parity with Python reference implementation
- Type-erased operations eliminate template bloat
- Schema-driven builder infrastructure
- Comprehensive test coverage

---

## Part 1: Python Behavioral Requirements

### 1.1 Core Behavioral Contracts

These contracts are derived from the Python reference implementation in `hgraph/_impl/_types/`.

#### Contract 1: Modification Tracking
**Source:** `_output.py:PythonTimeSeriesOutput`

```python
@property
def modified(self) -> bool:
    context = self.owning_graph.evaluation_clock
    return context.evaluation_time == self._last_modified_time
```

**Requirements:**
- Output tracks `_last_modified_time` as a datetime
- `modified` property compares against current evaluation time
- Modification is transient per-tick (resets each evaluation cycle)
- `mark_modified()` updates timestamp and propagates to parent

**C++ Mapping:**
- `ModificationTrackerStorage` in TSOutput already provides this
- Need to verify evaluation clock integration

#### Contract 2: Active/Passive Subscription
**Source:** `_input.py:PythonBoundTimeSeriesInput`

```python
def make_active(self):
    if not self._active:
        self._active = True
        if self._output is not None:
            self._output.subscribe(self)
            if self._output.valid and self._output.modified:
                self.notify(self._output.last_modified_time)
                return
        if self._sampled:
            self.notify(self._sample_time)

def make_passive(self):
    if self._active:
        self._active = False
        if self._output is not None:
            self._output.unsubscribe(self)
```

**Requirements:**
- Inputs have active/passive state
- Active inputs subscribe to their bound output
- On becoming active: check if output is valid+modified, notify immediately
- Passive inputs do not receive notifications
- Subscription changes trigger appropriate subscribe/unsubscribe calls

**C++ Mapping:**
- `ObserverStorage` provides subscription list
- Need to implement `subscribe()/unsubscribe()` on TSOutput
- TSInput needs `make_active()/make_passive()` methods

#### Contract 3: Notification Idempotency
**Source:** `_input.py:PythonBoundTimeSeriesInput.notify()`

```python
def notify(self, modified_time: datetime):
    if self._notify_time != modified_time:
        self._notify_time = modified_time
        (self.parent_input.notify_parent(self, modified_time)
         if self.parent_input
         else self.owning_node.notify(modified_time))
```

**Requirements:**
- Notifications are deduplicated per evaluation time
- `_notify_time` tracks last notification time
- Prevents redundant node scheduling within same tick
- Only propagates if time differs from last notification

**C++ Mapping:**
- TSInput needs `_notify_time` field
- Guard check before propagation

#### Contract 4: Parent Chain Propagation
**Source:** `_output.py:mark_modified()`, `_input.py:notify()`

```python
# Output propagation
def mark_modified(self, modified_time: datetime = None):
    if self._last_modified_time < modified_time:
        self._last_modified_time = modified_time
        if self.has_parent_output:
            self._parent_or_node.mark_child_modified(self, modified_time)
        self._notify(modified_time)

# Input propagation
def notify(self, modified_time: datetime):
    if self._notify_time != modified_time:
        self._notify_time = modified_time
        (self.parent_input.notify_parent(self, modified_time)
         if self.parent_input
         else self.owning_node.notify(modified_time))
```

**Requirements:**
- Parent outputs receive `mark_child_modified()` calls from children
- Parent inputs receive `notify_parent()` calls from children
- Chain continues until reaching owning node
- Node receives final `notify()` to trigger scheduling

**C++ Mapping:**
- `_parent_or_node` union pattern works for both
- Need `has_parent_input()/has_parent_output()` checks
- Terminal case: call `node->notify()`

#### Contract 5: Delta Value Semantics
**Source:** Various collection implementations

**Requirements:**
- `delta_value` represents change since last tick
- For scalars: same as `value` (or None if not modified)
- For collections:
  - TSD: dict of only modified keys
  - TSL: list of only modified indices
  - TSS: set of added/removed elements
- Reset each tick (computed lazily or tracked explicitly)

**C++ Mapping:**
- Current implementation returns `value()` for delta (DEFECT)
- Need per-type delta tracking
- AccessStrategy can provide type-specific delta logic

#### Contract 6: Sampling Behavior
**Source:** `_input.py:PythonBoundTimeSeriesInput`

```python
@property
def _sampled(self) -> bool:
    return (self._sample_time != MIN_DT and
            self._sample_time == self.owning_graph.evaluation_clock.evaluation_time)

def bind_output(self, output: TimeSeriesOutput) -> bool:
    # ... binding logic ...
    if ((self.owning_node.is_started or self.owning_node.is_starting)
        and self._output is not None
        and (was_bound or self._output.valid)):
        self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
        if self.active:
            self.notify(self._sample_time)
```

**Requirements:**
- "Sampling" occurs when binding changes during execution
- `_sample_time` tracks when binding last changed
- `modified` includes sampled state: `output.modified or self._sampled`
- Allows nodes to react to binding changes even if value unchanged

**C++ Mapping:**
- TSInput needs `_sample_time` field
- `modified` property must check both output modification and sample state

#### Contract 7: Bound vs Unbound State
**Source:** `_input.py:PythonBoundTimeSeriesInput`

```python
@property
def bound(self) -> bool:
    return self._output is not None

@property
def has_peer(self) -> bool:
    return self._output is not None

@property
def valid(self) -> bool:
    return self.bound and self._output.valid
```

**Requirements:**
- `bound`: input has an output reference
- `has_peer`: specifically has a peer output (not just any binding)
- `valid`: bound AND output itself is valid
- `all_valid`: for collections, all children are valid

**C++ Mapping:**
- Already have `bound_output_` pointer in TSInput
- Need clear semantics for peer vs reference binding

#### Contract 8: Reference (REF) Semantics
**Source:** `_ref_type.py`, `_ref_meta_data.py`, `_input.py`

```python
def bind_output(self, output: TimeSeriesOutput) -> bool:
    if isinstance(output, TimeSeriesReferenceOutput):
        if output.value:
            output.value.bind_input(self)
        output.observe_reference(self)
        self._reference_output = output
        peer = False
    else:
        peer = self.do_bind_output(output)
```

**Requirements:**
- REF types hold pointers to other time-series
- `TimeSeriesReferenceOutput.value` is the referenced TS
- Reference observers get notified when reference changes
- `observe_reference()`/`stop_observing_reference()` for lifecycle
- Referenced TS may be None (unbound reference)

**C++ Mapping:**
- RefObserver and RefWrapper AccessStrategies exist
- Need `_reference_output` field for observer tracking
- Python wrapper issues documented in KNOWN_ISSUES.md

#### Contract 9: Collection Key Tracking (TSD/TSS)
**Source:** Collection implementations

**Requirements:**
- TSD tracks: `keys()`, `added()`, `removed()`, `get_or_create()`
- TSS tracks: `added()`, `removed()` per tick
- Changes are delta-based per evaluation cycle
- `get_or_create()` lazily creates per-key inputs

**C++ Mapping:**
- Need modification flags per key
- Delta sets for added/removed tracking
- Builder-based lazy creation

#### Contract 10: Valid Propagation
**Source:** Various implementations

**Requirements:**
- `valid`: has a value that can be read
- `all_valid`: for collections, all children valid
- Validity can change during execution (binding changes)
- First valid state triggers initial notification

**C++ Mapping:**
- `valid_` flag in Value storage
- Collection types need aggregate validity check

---

## Part 2: C++ Type-Erased Architecture Mapping

### 2.1 Current Infrastructure Status

| Component | Status | Notes |
|-----------|--------|-------|
| `Value` | ✅ Complete | Type-erased value storage |
| `TypeMeta` | ✅ Complete | Runtime type information |
| `TypeOps` | ✅ Complete | Function pointer operations |
| `TSOutput` | ⚠️ Partial | Storage complete, operations incomplete |
| `TSInput` | ⚠️ Partial | AccessStrategy complete, binding incomplete |
| `AccessStrategy` | ✅ Complete | DirectAccess, CollectionAccess, RefObserver, RefWrapper |
| `Builders` | ⚠️ Partial | Schema exists, instantiation incomplete |
| `Node implementations` | ❌ Stubs | All nested nodes throw "not yet implemented" |
| `Python wrappers` | ⚠️ Partial | Many methods return stubs |

### 2.2 Key Type Mappings

| Python Type | C++ Type | AccessStrategy |
|-------------|----------|----------------|
| `TS[T]` | `TSOutput<DirectAccess>` + `TSInput<DirectAccess>` | DirectAccess |
| `TSB[schema]` | `TSOutput` + `TSInput` with field schema | DirectAccess (per-field) |
| `TSL[T, Size]` | `TSOutput<CollectionAccess>` + `TSInput<CollectionAccess>` | CollectionAccess |
| `TSD[K, V]` | `TSOutput<CollectionAccess>` + `TSInput<CollectionAccess>` | CollectionAccess |
| `TSS[T]` | `TSOutput<CollectionAccess>` + `TSInput<CollectionAccess>` | CollectionAccess |
| `REF[T]` | `TSOutput` + `TSInput<RefWrapper/RefObserver>` | RefWrapper/RefObserver |
| `TSW[T, Size]` | `TSOutput<CollectionAccess>` + `TSInput<CollectionAccess>` | CollectionAccess |

### 2.3 Storage Classes

```
TSOutput = Value + ModificationTrackerStorage + ObserverStorage
TSInput  = AccessStrategy + BindingState + NotificationState
```

**Value Storage:**
- `storage_`: Raw bytes for value
- `valid_`: Validity flag
- `type_meta_`: Runtime type information

**ModificationTrackerStorage:**
- `last_modified_time_`: Engine time of last modification
- For collections: per-key modification flags

**ObserverStorage:**
- `subscribers_`: List of subscribed TSInputs
- `notify()`: Iterate and call `input->notify(time)`

**TSInput State:**
- `bound_output_`: Pointer to bound TSOutput
- `active_`: Subscription state
- `sample_time_`: Last binding change time
- `notify_time_`: Last notification time (for idempotency)
- `parent_or_node_`: Parent input or owning node

---

## Part 3: Gap Analysis

### 3.1 Critical Gaps

#### Gap 1: Node Implementations
**Impact:** HIGH - Blocks nested graph functionality
**Affected:** SwitchNode, TsdMapNode, ReduceNode, ComponentNode, MeshNode

All these nodes have stub implementations that throw "not yet implemented". They require:
- Dynamic graph creation/destruction
- Output binding between parent and nested graphs
- Key-based graph management (TsdMapNode)
- Tree-based reduction structures (ReduceNode)

**Migration Strategy:**
1. Implement `bind_parent_output()` mechanism (see KNOWN_ISSUES.md design notes)
2. Port SwitchNode first (simplest nested case)
3. Port TsdMapNode (key-indexed nested graphs)
4. Port ReduceNode (tree structure)
5. Port ComponentNode and MeshNode (complex dependencies)

#### Gap 2: Delta Value Implementation
**Impact:** MEDIUM - Incorrect behavior for collection types
**Affected:** All collection types (TSD, TSL, TSS)

Current implementation returns `value()` for `delta_value()`.

**Migration Strategy:**
1. Add delta tracking storage to collection types
2. Track added/removed/modified keys per tick
3. Clear delta state at tick boundary
4. Implement per-type delta accessors

#### Gap 3: Parent Tracking
**Impact:** MEDIUM - Views don't work correctly
**Affected:** Python wrappers for nested structures

`parent_output()` and `parent_input()` always return None.

**Migration Strategy:**
1. Ensure `_parent_or_node` is set correctly during construction
2. Implement `has_parent_output()/has_parent_input()` checks
3. Wire up parent references in builders

#### Gap 4: Reference Type Support
**Impact:** MEDIUM - REF types partially broken
**Affected:** REF[T] inputs

Several methods are stubs:
- `has_peer()` - always returns False
- `output()` - always returns None
- `bind_output()` - always returns False

**Migration Strategy:**
1. Implement RefObserver subscription mechanism
2. Track `_reference_output` separately from `_bound_output`
3. Implement reference change notifications

### 3.2 Minor Gaps

| Gap | Impact | Notes |
|-----|--------|-------|
| `all_valid()` | LOW | Returns `valid()` instead of aggregate check |
| BackTrace capture | LOW | Error diagnostics incomplete |
| OutputBuilder.make_instance | LOW | Throws; time-series now owned by Node |

---

## Part 4: Migration Roadmap

### Phase 1: Core Binding Infrastructure (Priority: CRITICAL)

**Objective:** Complete TSInput/TSOutput binding lifecycle

**Tasks:**
1. [ ] Implement `TSOutput::subscribe(TSInput*)`
2. [ ] Implement `TSOutput::unsubscribe(TSInput*)`
3. [ ] Implement `TSInput::make_active()/make_passive()`
4. [ ] Add notification idempotency (`_notify_time` check)
5. [ ] Implement sampling behavior (`_sample_time` tracking)
6. [ ] Implement `bind_output()`/`un_bind_output()` with proper state transitions

**Validation:**
- Test: Input subscribes when becoming active
- Test: Input unsubscribes when becoming passive
- Test: Notifications are idempotent per-tick
- Test: Binding changes trigger sampling notifications

### Phase 2: Parent Chain Propagation (Priority: HIGH)

**Objective:** Complete modification/notification propagation

**Tasks:**
1. [ ] Implement `TSOutput::mark_child_modified()`
2. [ ] Implement `TSInput::notify_parent()`
3. [ ] Ensure parent references set in builders
4. [ ] Wire up to Node::notify() as terminal case

**Validation:**
- Test: Child modification propagates to parent output
- Test: Child notification propagates to parent input
- Test: Terminal notification reaches node scheduler

### Phase 3: Delta Value Tracking (Priority: MEDIUM)

**Objective:** Implement per-tick delta semantics

**Tasks:**
1. [ ] Add delta storage to collection types
2. [ ] Track added/removed keys for TSD/TSS
3. [ ] Track modified indices for TSL
4. [ ] Implement tick-boundary delta reset
5. [ ] Expose delta through Python wrappers

**Validation:**
- Test: TSD delta contains only modified keys
- Test: TSS delta contains added/removed sets
- Test: Delta resets each tick

### Phase 4: Reference Types (Priority: MEDIUM)

**Objective:** Complete REF[T] implementation

**Tasks:**
1. [ ] Implement reference observer mechanism
2. [ ] Track `_reference_output` in TSInput
3. [ ] Implement `observe_reference()`/`stop_observing_reference()`
4. [ ] Handle reference rebinding notifications
5. [ ] Fix Python wrapper has_peer/output/bind_output

**Validation:**
- Test: REF input tracks bound reference
- Test: Reference change notifies observers
- Test: has_peer correctly distinguishes peer vs ref binding

### Phase 5: Nested Node Implementation (Priority: HIGH)

**Objective:** Implement nested graph nodes

**Tasks:**
1. [ ] Implement `Node::bind_parent_output()` mechanism
2. [ ] Port SwitchNode implementation
3. [ ] Port TsdMapNode implementation
4. [ ] Port ReduceNode implementation
5. [ ] Port ComponentNode implementation
6. [ ] Port MeshNode implementation

**Validation:**
- Test: SwitchNode switches between graphs based on key
- Test: TsdMapNode creates/destroys per-key graphs
- Test: ReduceNode performs tree-based reduction
- Test: ComponentNode manages component lifecycle
- Test: MeshNode handles rank-based scheduling

### Phase 6: Python Wrapper Completion (Priority: LOW)

**Objective:** Complete Python API surface

**Tasks:**
1. [ ] Implement `all_valid()` for collections
2. [ ] Implement proper `parent_input()`/`parent_output()`
3. [ ] Complete BackTrace error capture
4. [ ] Review and fix remaining stubs

**Validation:**
- Test: all_valid returns correct aggregate state
- Test: Parent accessors return correct references
- Test: Error backtraces include input values

---

## Part 5: Testing Strategy

### 5.1 Existing Test Coverage

From `TS_DESIGN.md`:
- 34 tests, 115 assertions
- Located in `hgraph_unit_tests/_types/_time_series/`

### 5.2 Required Test Additions

| Category | Tests Needed |
|----------|-------------|
| Binding lifecycle | subscribe/unsubscribe, active/passive transitions |
| Notification | idempotency, parent propagation, sampling |
| Delta values | per-type delta tracking, tick reset |
| References | observer mechanism, rebinding |
| Nested nodes | SwitchNode, TsdMapNode, ReduceNode, etc. |

### 5.3 Comparison Testing

Run tests with both implementations to verify behavioral parity:

```bash
# Python implementation
HGRAPH_USE_CPP=0 uv run pytest hgraph_unit_tests/_types/_time_series/ -v

# C++ implementation
HGRAPH_USE_CPP=1 uv run pytest hgraph_unit_tests/_types/_time_series/ -v
```

---

## Appendix A: Key File Locations

### Python Reference Implementation
- `hgraph/_impl/_types/_output.py` - Output base class
- `hgraph/_impl/_types/_input.py` - Input base class
- `hgraph/_impl/_types/_ts_type_impl.py` - TS[T] implementation
- `hgraph/_impl/_types/_tsb_impl.py` - TSB implementation
- `hgraph/_impl/_types/_tsd_impl.py` - TSD implementation
- `hgraph/_impl/_types/_tsl_impl.py` - TSL implementation
- `hgraph/_impl/_types/_tss_impl.py` - TSS implementation
- `hgraph/_impl/_types/_ref_impl.py` - REF implementation

### C++ Implementation
- `cpp/include/hgraph/types/time_series/ts_output.h` - TSOutput class
- `cpp/include/hgraph/types/time_series/ts_input.h` - TSInput class
- `cpp/include/hgraph/types/time_series/access_strategy.h` - AccessStrategy hierarchy
- `cpp/include/hgraph/types/value/value.h` - Type-erased Value
- `cpp/include/hgraph/types/type_meta.h` - Runtime type info
- `cpp/src/cpp/nodes/` - Node implementations (stubs)

### Design Documents
- `cpp/include/hgraph/types/time_series/TS_DESIGN.md` - Core design
- `cpp/include/hgraph/types/value/VALUE_DESIGN.md` - Value design
- `cpp/KNOWN_ISSUES.md` - Defects and gaps

---

## Appendix B: Python Behavioral Contract Tests

These test patterns should be implemented to verify C++ behavioral parity:

```python
# Contract 1: Modification Tracking
def test_output_modified_same_tick():
    output.apply_result(value)
    assert output.modified == True

def test_output_not_modified_different_tick():
    output.apply_result(value)
    advance_tick()
    assert output.modified == False

# Contract 2: Active/Passive Subscription
def test_input_subscribes_when_active():
    input.bind_output(output)
    input.make_active()
    assert input in output.subscribers

def test_input_unsubscribes_when_passive():
    input.make_active()
    input.make_passive()
    assert input not in output.subscribers

# Contract 3: Notification Idempotency
def test_notification_idempotent_same_tick():
    input.notify(current_time)
    input.notify(current_time)  # Should not propagate again
    assert node.notify_count == 1

# Contract 6: Sampling
def test_binding_change_triggers_sample():
    input.bind_output(output1)
    start_node()
    input.bind_output(output2)
    assert input.modified == True  # Due to sampling
```

---

*Document Version: 1.0*
*Last Updated: 2025-12-20*
