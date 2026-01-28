# Test Specifications: TSValue Infrastructure

**Version:** 1.0
**Date:** 2026-01-23
**Reference:** `ts_value_v2601/implementation/ts_value/01_IMPLEMENTATION_PLAN_v2.md`

---

## Section 1: Overview

### 1.1 Testing Strategy

All tests use pytest function-based style as per project guidelines. Tests exercise both Python API (via pybind11 bindings) and verify C++ implementation correctness. Test files are organized to mirror the source structure under `hgraph_unit_tests/_types/_time_series/`.

### 1.2 Test File Organization

| Test File | Phase | Components Tested |
|-----------|-------|-------------------|
| `test_observer_list.py` | 1 | ObserverList |
| `test_time_array.py` | 1 | TimeArray |
| `test_observer_array.py` | 1 | ObserverArray |
| `test_set_delta.py` | 2 | SetDelta |
| `test_map_delta.py` | 2 | MapDelta |
| `test_delta_nav.py` | 2 | BundleDeltaNav, ListDeltaNav |
| `test_ts_meta_schema.py` | 3 | Schema generation functions |
| `test_ts_value.py` | 4 | TSValue class |
| `test_ts_view.py` | 5 | TSView and kind-specific views |
| `test_ts_value_integration.py` | 8 | End-to-end scenarios |
| `test_ts_value_conformance.py` | 8 | Python reference parity |

### 1.3 Common Test Fixtures

```python
import pytest
from hgraph import engine_time_t, MIN_ST, MAX_ST

@pytest.fixture
def current_time():
    """Standard test time."""
    return engine_time_t(1000)

@pytest.fixture
def next_tick():
    """Time representing next engine tick."""
    return engine_time_t(1001)

@pytest.fixture
def mock_notifiable():
    """Mock Notifiable for observer testing."""
    class MockNotifiable:
        def __init__(self):
            self.modified_count = 0
            self.removed_count = 0
            self.last_time = None

        def notify_modified(self, t: engine_time_t):
            self.modified_count += 1
            self.last_time = t

        def notify_removed(self):
            self.removed_count += 1

    return MockNotifiable()
```

---

## Section 2: Phase 1 - Foundation Types

### 2.1 test_observer_list.py

**Purpose:** Test ObserverList management and notification.

```python
# File: hgraph_unit_tests/_types/_time_series/test_observer_list.py

def test_observer_list_default_construction():
    """ObserverList starts empty."""
    obs_list = ObserverList()
    assert obs_list.empty()
    assert obs_list.size() == 0

def test_observer_list_add_observer(mock_notifiable):
    """Can add observer to list."""
    obs_list = ObserverList()
    obs_list.add_observer(mock_notifiable)
    assert not obs_list.empty()
    assert obs_list.size() == 1

def test_observer_list_remove_observer(mock_notifiable):
    """Can remove observer from list."""
    obs_list = ObserverList()
    obs_list.add_observer(mock_notifiable)
    obs_list.remove_observer(mock_notifiable)
    assert obs_list.empty()

def test_observer_list_notify_modified(mock_notifiable, current_time):
    """notify_modified calls observer with time."""
    obs_list = ObserverList()
    obs_list.add_observer(mock_notifiable)
    obs_list.notify_modified(current_time)
    assert mock_notifiable.modified_count == 1
    assert mock_notifiable.last_time == current_time

def test_observer_list_notify_removed(mock_notifiable):
    """notify_removed calls observer."""
    obs_list = ObserverList()
    obs_list.add_observer(mock_notifiable)
    obs_list.notify_removed()
    assert mock_notifiable.removed_count == 1

def test_observer_list_multiple_observers():
    """Multiple observers all receive notifications."""
    obs_list = ObserverList()
    observers = [MockNotifiable() for _ in range(5)]
    for obs in observers:
        obs_list.add_observer(obs)

    obs_list.notify_modified(engine_time_t(1000))

    for obs in observers:
        assert obs.modified_count == 1

def test_observer_list_clear():
    """clear() removes all observers."""
    obs_list = ObserverList()
    observers = [MockNotifiable() for _ in range(5)]
    for obs in observers:
        obs_list.add_observer(obs)

    obs_list.clear()
    assert obs_list.empty()

def test_observer_list_duplicate_add(mock_notifiable):
    """Adding same observer twice does not duplicate notifications."""
    obs_list = ObserverList()
    obs_list.add_observer(mock_notifiable)
    obs_list.add_observer(mock_notifiable)

    obs_list.notify_modified(engine_time_t(1000))
    # Implementation-dependent: either prevent duplicate add or allow multiple notifications
    # Document expected behavior
```

### 2.2 test_time_array.py

**Purpose:** Test TimeArray parallel timestamp storage.

```python
# File: hgraph_unit_tests/_types/_time_series/test_time_array.py

def test_time_array_default_construction():
    """TimeArray starts with zero size."""
    ta = TimeArray()
    assert ta.size() == 0

def test_time_array_on_capacity():
    """on_capacity resizes internal storage."""
    ta = TimeArray()
    ta.on_capacity(0, 10)
    assert ta.size() >= 10  # Or test via other means

def test_time_array_on_insert(current_time):
    """on_insert initializes slot to MIN_ST."""
    ta = TimeArray()
    ta.on_capacity(0, 10)
    ta.on_insert(0)
    assert ta.at(0) == MIN_ST
    assert not ta.valid(0)  # MIN_ST means not valid

def test_time_array_set_and_at(current_time):
    """set() and at() work correctly."""
    ta = TimeArray()
    ta.on_capacity(0, 10)
    ta.on_insert(0)
    ta.set(0, current_time)
    assert ta.at(0) == current_time

def test_time_array_modified(current_time, next_tick):
    """modified() returns True when slot time >= current_time."""
    ta = TimeArray()
    ta.on_capacity(0, 10)
    ta.on_insert(0)
    ta.set(0, current_time)

    # Modified at same time
    assert ta.modified(0, current_time)

    # Not modified in future tick
    assert not ta.modified(0, next_tick)

def test_time_array_valid():
    """valid() returns False for MIN_ST, True otherwise."""
    ta = TimeArray()
    ta.on_capacity(0, 10)
    ta.on_insert(0)

    assert not ta.valid(0)  # Initialized to MIN_ST
    ta.set(0, engine_time_t(1000))
    assert ta.valid(0)

def test_time_array_on_erase():
    """on_erase handles slot removal correctly."""
    ta = TimeArray()
    ta.on_capacity(0, 10)
    ta.on_insert(0)
    ta.set(0, engine_time_t(1000))
    ta.on_erase(0)
    # Behavior after erase depends on implementation
    # Document expected behavior

def test_time_array_on_clear():
    """on_clear resets all slots."""
    ta = TimeArray()
    ta.on_capacity(0, 10)
    for i in range(10):
        ta.on_insert(i)
        ta.set(i, engine_time_t(1000 + i))

    ta.on_clear()
    # All slots should be invalidated or removed

def test_time_array_data_pointer():
    """data() returns pointer to underlying storage."""
    ta = TimeArray()
    ta.on_capacity(0, 10)
    ta.on_insert(0)
    ta.set(0, engine_time_t(1000))

    ptr = ta.data()
    assert ptr is not None
```

### 2.3 test_observer_array.py

**Purpose:** Test ObserverArray parallel observer lists.

```python
# File: hgraph_unit_tests/_types/_time_series/test_observer_array.py

def test_observer_array_default_construction():
    """ObserverArray starts with zero size."""
    oa = ObserverArray()
    assert oa.size() == 0

def test_observer_array_on_capacity():
    """on_capacity resizes internal storage."""
    oa = ObserverArray()
    oa.on_capacity(0, 10)
    assert oa.size() >= 10

def test_observer_array_on_insert():
    """on_insert initializes slot with empty ObserverList."""
    oa = ObserverArray()
    oa.on_capacity(0, 10)
    oa.on_insert(0)
    assert oa.at(0).empty()

def test_observer_array_at_returns_observer_list(mock_notifiable, current_time):
    """at() returns modifiable ObserverList."""
    oa = ObserverArray()
    oa.on_capacity(0, 10)
    oa.on_insert(0)

    obs_list = oa.at(0)
    obs_list.add_observer(mock_notifiable)
    obs_list.notify_modified(current_time)

    assert mock_notifiable.modified_count == 1

def test_observer_array_on_erase():
    """on_erase handles slot removal."""
    oa = ObserverArray()
    oa.on_capacity(0, 10)
    oa.on_insert(0)
    oa.at(0).add_observer(MockNotifiable())
    oa.on_erase(0)
    # Document expected behavior

def test_observer_array_on_clear():
    """on_clear clears all observer lists."""
    oa = ObserverArray()
    oa.on_capacity(0, 10)
    for i in range(10):
        oa.on_insert(i)
        oa.at(i).add_observer(MockNotifiable())

    oa.on_clear()
    # All observer lists should be empty or removed

def test_observer_array_independent_slots(current_time):
    """Each slot has independent ObserverList."""
    oa = ObserverArray()
    oa.on_capacity(0, 3)
    for i in range(3):
        oa.on_insert(i)

    obs1 = MockNotifiable()
    obs2 = MockNotifiable()

    oa.at(0).add_observer(obs1)
    oa.at(1).add_observer(obs2)

    oa.at(0).notify_modified(current_time)

    assert obs1.modified_count == 1
    assert obs2.modified_count == 0  # Not notified
```

---

## Section 3: Phase 2 - Delta Structures

### 3.1 test_set_delta.py

**Purpose:** Test slot-based SetDelta tracking.

```python
# File: hgraph_unit_tests/_types/_time_series/test_set_delta.py

def test_set_delta_default_construction():
    """SetDelta starts empty."""
    sd = SetDelta()
    assert sd.empty()
    assert len(sd.added()) == 0
    assert len(sd.removed()) == 0
    assert not sd.was_cleared()

def test_set_delta_on_insert():
    """on_insert adds slot to added list."""
    sd = SetDelta()
    sd.on_insert(5)
    assert 5 in sd.added()
    assert not sd.empty()

def test_set_delta_on_erase():
    """on_erase adds slot to removed list."""
    sd = SetDelta()
    sd.on_erase(5)
    assert 5 in sd.removed()
    assert not sd.empty()

def test_set_delta_add_then_remove_cancellation():
    """Add then remove in same tick cancels out."""
    sd = SetDelta()
    sd.on_insert(5)
    sd.on_erase(5)
    # Should cancel out - slot not in added or removed
    assert 5 not in sd.added()
    assert 5 not in sd.removed()

def test_set_delta_remove_then_add():
    """Remove then add in same tick results in update (both lists)."""
    sd = SetDelta()
    sd.on_erase(5)  # Pre-existing element removed
    sd.on_insert(5)  # Same slot reused for new element
    # Both removed and added have the slot
    assert 5 in sd.removed()
    assert 5 in sd.added()

def test_set_delta_on_clear():
    """on_clear sets was_cleared flag."""
    sd = SetDelta()
    sd.on_insert(1)
    sd.on_insert(2)
    sd.on_clear()
    assert sd.was_cleared()

def test_set_delta_clear():
    """clear() resets delta state."""
    sd = SetDelta()
    sd.on_insert(1)
    sd.on_erase(2)
    sd.on_clear()
    sd.clear()

    assert sd.empty()
    assert len(sd.added()) == 0
    assert len(sd.removed()) == 0
    assert not sd.was_cleared()

def test_set_delta_multiple_inserts():
    """Multiple inserts tracked correctly."""
    sd = SetDelta()
    for i in range(10):
        sd.on_insert(i)

    assert len(sd.added()) == 10
    for i in range(10):
        assert i in sd.added()

def test_set_delta_mixed_operations():
    """Complex sequence of operations."""
    sd = SetDelta()

    # Add slots 0-4
    for i in range(5):
        sd.on_insert(i)

    # Remove slots 2, 3
    sd.on_erase(2)
    sd.on_erase(3)

    # Add slots 5, 6
    sd.on_insert(5)
    sd.on_insert(6)

    # Slots 0, 1, 4, 5, 6 in added (2, 3 cancelled out)
    assert set(sd.added()) == {0, 1, 4, 5, 6}
    # Slots 2, 3 removed but cancelled by earlier add
    # (depends on implementation - document expected behavior)
```

### 3.2 test_map_delta.py

**Purpose:** Test slot-based MapDelta tracking with child deltas.

```python
# File: hgraph_unit_tests/_types/_time_series/test_map_delta.py

def test_map_delta_default_construction():
    """MapDelta starts empty."""
    md = MapDelta()
    assert md.empty()
    assert len(md.added()) == 0
    assert len(md.removed()) == 0
    assert len(md.updated()) == 0
    assert not md.was_cleared()

def test_map_delta_on_insert():
    """on_insert adds slot to added list."""
    md = MapDelta()
    md.on_insert(5)
    assert 5 in md.added()

def test_map_delta_on_erase():
    """on_erase adds slot to removed list."""
    md = MapDelta()
    md.on_erase(5)
    assert 5 in md.removed()

def test_map_delta_on_update():
    """on_update adds slot to updated list."""
    md = MapDelta()
    md.on_update(5)
    assert 5 in md.updated()

def test_map_delta_add_remove_cancellation():
    """Add then remove cancels out."""
    md = MapDelta()
    md.on_insert(5)
    md.on_erase(5)
    assert 5 not in md.added()
    assert 5 not in md.removed()

def test_map_delta_children_access():
    """children() returns modifiable vector."""
    md = MapDelta()
    md.on_capacity(0, 10)

    children = md.children()
    assert len(children) == 10

    # Can set child delta
    child_delta = SetDelta()
    children[0] = child_delta

def test_map_delta_on_clear():
    """on_clear sets was_cleared flag."""
    md = MapDelta()
    md.on_insert(1)
    md.on_clear()
    assert md.was_cleared()

def test_map_delta_clear():
    """clear() resets all state."""
    md = MapDelta()
    md.on_insert(1)
    md.on_erase(2)
    md.on_update(3)
    md.on_clear()
    md.clear()

    assert md.empty()
    assert not md.was_cleared()

def test_map_delta_update_after_insert():
    """Update after insert in same tick - slot in added only."""
    md = MapDelta()
    md.on_insert(5)
    md.on_update(5)
    # New slot doesn't need separate update tracking
    assert 5 in md.added()
    # Implementation may or may not include in updated
```

### 3.3 test_delta_nav.py

**Purpose:** Test navigation delta structures.

```python
# File: hgraph_unit_tests/_types/_time_series/test_delta_nav.py

def test_bundle_delta_nav_default_construction():
    """BundleDeltaNav starts with MIN_ST clear time."""
    nav = BundleDeltaNav()
    assert nav.last_cleared_time == MIN_ST
    assert len(nav.children) == 0

def test_bundle_delta_nav_children():
    """children vector can be populated."""
    nav = BundleDeltaNav()
    nav.children.resize(3)

    # Can assign child deltas
    child = SetDelta()
    nav.children[0] = child

def test_bundle_delta_nav_clear(current_time):
    """clear() resets children and updates clear time."""
    nav = BundleDeltaNav()
    nav.children.resize(3)
    nav.last_cleared_time = current_time

    child = SetDelta()
    child.on_insert(0)
    nav.children[0] = child

    nav.clear()
    # Children should be cleared
    # last_cleared_time may be updated (implementation-dependent)

def test_list_delta_nav_default_construction():
    """ListDeltaNav starts with MIN_ST clear time."""
    nav = ListDeltaNav()
    assert nav.last_cleared_time == MIN_ST
    assert len(nav.children) == 0

def test_list_delta_nav_children():
    """children vector can be populated."""
    nav = ListDeltaNav()
    nav.children.resize(5)

    for i in range(5):
        child = SetDelta()
        child.on_insert(i)
        nav.children[i] = child

def test_list_delta_nav_clear():
    """clear() resets all children."""
    nav = ListDeltaNav()
    nav.children.resize(5)

    for i in range(5):
        child = SetDelta()
        child.on_insert(i)
        nav.children[i] = child

    nav.clear()
    # All child deltas should be cleared

def test_delta_variant_type_safety():
    """DeltaVariant holds correct types."""
    variants = [
        DeltaVariant(),  # monostate
        DeltaVariant(SetDelta()),
        DeltaVariant(MapDelta()),
        DeltaVariant(BundleDeltaNav()),
        DeltaVariant(ListDeltaNav()),
    ]

    # Can check which type is held
    assert isinstance(variants[1].get(), SetDelta)
    assert isinstance(variants[2].get(), MapDelta)
```

---

## Section 4: Phase 3 - Schema Generation

### 4.1 test_ts_meta_schema.py

**Purpose:** Test schema generation functions.

```python
# File: hgraph_unit_tests/_types/_time_series/test_ts_meta_schema.py

# --- has_delta() tests ---

def test_has_delta_ts():
    """TS[T] has no delta."""
    meta = create_ts_meta(TSKind.TS, int)
    assert not has_delta(meta)

def test_has_delta_tss():
    """TSS[T] has delta."""
    meta = create_ts_meta(TSKind.TSS, int)
    assert has_delta(meta)

def test_has_delta_tsd():
    """TSD[K,V] has delta."""
    meta = create_ts_meta(TSKind.TSD, str, int)
    assert has_delta(meta)

def test_has_delta_tsb_no_delta_fields():
    """TSB with only TS fields has no delta."""
    meta = create_tsb_meta([
        ("a", TSKind.TS, int),
        ("b", TSKind.TS, float),
    ])
    assert not has_delta(meta)

def test_has_delta_tsb_with_tss_field():
    """TSB with TSS field has delta."""
    meta = create_tsb_meta([
        ("a", TSKind.TS, int),
        ("b", TSKind.TSS, int),
    ])
    assert has_delta(meta)

def test_has_delta_tsl_no_delta_element():
    """TSL with TS element has no delta."""
    meta = create_tsl_meta(TSKind.TS, int, 5)
    assert not has_delta(meta)

def test_has_delta_tsl_with_tss_element():
    """TSL with TSS element has delta."""
    meta = create_tsl_meta(TSKind.TSS, int, 5)
    assert has_delta(meta)

def test_has_delta_signal():
    """SIGNAL has no delta."""
    meta = create_ts_meta(TSKind.SIGNAL)
    assert not has_delta(meta)

# --- generate_time_schema() tests ---

def test_time_schema_ts():
    """TS[T] time schema is engine_time_t."""
    meta = create_ts_meta(TSKind.TS, int)
    time_meta = generate_time_schema(meta)
    assert time_meta.type_id == type_id_of(engine_time_t)

def test_time_schema_tss():
    """TSS[T] time schema is engine_time_t."""
    meta = create_ts_meta(TSKind.TSS, int)
    time_meta = generate_time_schema(meta)
    assert time_meta.type_id == type_id_of(engine_time_t)

def test_time_schema_tsd():
    """TSD[K,V] time schema is tuple[engine_time_t, var_list[...]]."""
    meta = create_ts_meta(TSKind.TSD, str, int)
    time_meta = generate_time_schema(meta)

    assert time_meta.is_tuple()
    fields = time_meta.fields()
    assert len(fields) == 2
    assert fields[0].type_id == type_id_of(engine_time_t)  # Container time
    assert fields[1].is_var_list()  # Per-slot times

def test_time_schema_tsb():
    """TSB time schema is tuple[engine_time_t, fixed_list[...]]."""
    meta = create_tsb_meta([
        ("a", TSKind.TS, int),
        ("b", TSKind.TS, float),
    ])
    time_meta = generate_time_schema(meta)

    assert time_meta.is_tuple()
    fields = time_meta.fields()
    assert len(fields) == 2
    assert fields[0].type_id == type_id_of(engine_time_t)
    assert fields[1].is_fixed_list()
    assert len(fields[1].elements()) == 2  # Two field times

def test_time_schema_tsl():
    """TSL time schema is tuple[engine_time_t, fixed_list[...]]."""
    meta = create_tsl_meta(TSKind.TS, int, 3)
    time_meta = generate_time_schema(meta)

    assert time_meta.is_tuple()
    fields = time_meta.fields()
    assert len(fields) == 2
    assert fields[0].type_id == type_id_of(engine_time_t)
    assert fields[1].is_fixed_list()
    assert len(fields[1].elements()) == 3

# --- generate_observer_schema() tests ---

def test_observer_schema_ts():
    """TS[T] observer schema is ObserverList."""
    meta = create_ts_meta(TSKind.TS, int)
    obs_meta = generate_observer_schema(meta)
    assert obs_meta.type_id == type_id_of(ObserverList)

def test_observer_schema_tsd():
    """TSD observer schema is tuple[ObserverList, var_list[...]]."""
    meta = create_ts_meta(TSKind.TSD, str, int)
    obs_meta = generate_observer_schema(meta)

    assert obs_meta.is_tuple()
    fields = obs_meta.fields()
    assert len(fields) == 2
    assert fields[0].type_id == type_id_of(ObserverList)
    assert fields[1].is_var_list()

def test_observer_schema_tsb():
    """TSB observer schema is tuple[ObserverList, fixed_list[...]]."""
    meta = create_tsb_meta([
        ("a", TSKind.TS, int),
        ("b", TSKind.TSS, float),
    ])
    obs_meta = generate_observer_schema(meta)

    assert obs_meta.is_tuple()
    fields = obs_meta.fields()
    assert len(fields) == 2
    assert fields[0].type_id == type_id_of(ObserverList)
    assert fields[1].is_fixed_list()

# --- generate_delta_value_schema() tests ---

def test_delta_schema_ts():
    """TS[T] has no delta schema (void)."""
    meta = create_ts_meta(TSKind.TS, int)
    delta_meta = generate_delta_value_schema(meta)
    assert delta_meta is None or delta_meta.type_id == type_id_of(void)

def test_delta_schema_tss():
    """TSS[T] delta schema is SetDelta."""
    meta = create_ts_meta(TSKind.TSS, int)
    delta_meta = generate_delta_value_schema(meta)
    assert delta_meta.type_id == type_id_of(SetDelta)

def test_delta_schema_tsd():
    """TSD[K,V] delta schema is MapDelta."""
    meta = create_ts_meta(TSKind.TSD, str, int)
    delta_meta = generate_delta_value_schema(meta)
    assert delta_meta.type_id == type_id_of(MapDelta)

def test_delta_schema_tsb_no_delta():
    """TSB with no delta fields has no delta schema."""
    meta = create_tsb_meta([
        ("a", TSKind.TS, int),
        ("b", TSKind.TS, float),
    ])
    delta_meta = generate_delta_value_schema(meta)
    assert delta_meta is None or delta_meta.type_id == type_id_of(void)

def test_delta_schema_tsb_with_delta():
    """TSB with delta field has BundleDeltaNav schema."""
    meta = create_tsb_meta([
        ("a", TSKind.TS, int),
        ("b", TSKind.TSS, float),
    ])
    delta_meta = generate_delta_value_schema(meta)
    assert delta_meta.type_id == type_id_of(BundleDeltaNav)

def test_delta_schema_tsl_no_delta():
    """TSL with TS element has no delta schema."""
    meta = create_tsl_meta(TSKind.TS, int, 5)
    delta_meta = generate_delta_value_schema(meta)
    assert delta_meta is None or delta_meta.type_id == type_id_of(void)

def test_delta_schema_tsl_with_delta():
    """TSL with TSS element has ListDeltaNav schema."""
    meta = create_tsl_meta(TSKind.TSS, int, 5)
    delta_meta = generate_delta_value_schema(meta)
    assert delta_meta.type_id == type_id_of(ListDeltaNav)

def test_delta_schema_signal():
    """SIGNAL has no delta schema."""
    meta = create_ts_meta(TSKind.SIGNAL)
    delta_meta = generate_delta_value_schema(meta)
    assert delta_meta is None or delta_meta.type_id == type_id_of(void)
```

---

## Section 5: Phase 4 - TSValue

### 5.1 test_ts_value.py

**Purpose:** Test TSValue owning container.

```python
# File: hgraph_unit_tests/_types/_time_series/test_ts_value.py

# --- Construction tests ---

def test_ts_value_construction_ts():
    """TSValue can be constructed for TS[int]."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)
    assert ts_val.meta() == meta

def test_ts_value_construction_tss():
    """TSValue can be constructed for TSS[int]."""
    meta = create_ts_meta(TSKind.TSS, int)
    ts_val = TSValue(meta)
    assert ts_val.has_delta()

def test_ts_value_construction_tsd():
    """TSValue can be constructed for TSD[str, int]."""
    meta = create_ts_meta(TSKind.TSD, str, int)
    ts_val = TSValue(meta)
    assert ts_val.has_delta()

def test_ts_value_construction_tsb():
    """TSValue can be constructed for TSB."""
    meta = create_tsb_meta([
        ("a", TSKind.TS, int),
        ("b", TSKind.TSS, float),
    ])
    ts_val = TSValue(meta)
    assert ts_val.has_delta()

# --- View access tests ---

def test_ts_value_value_view():
    """value_view() returns valid View."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)
    view = ts_val.value_view()
    assert view is not None

def test_ts_value_time_view():
    """time_view() returns valid View."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)
    view = ts_val.time_view()
    assert view is not None

def test_ts_value_observer_view():
    """observer_view() returns valid View."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)
    view = ts_val.observer_view()
    assert view is not None

def test_ts_value_delta_view_no_delta():
    """delta_value_view() for TS[T] returns empty/null view."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)
    view = ts_val.delta_value_view(engine_time_t(1000))
    # View may be null or represent void type

def test_ts_value_delta_view_with_delta(current_time):
    """delta_value_view() for TSS[T] returns SetDelta view."""
    meta = create_ts_meta(TSKind.TSS, int)
    ts_val = TSValue(meta)
    view = ts_val.delta_value_view(current_time)
    assert view is not None

# --- Time semantics tests ---

def test_ts_value_initial_not_valid():
    """New TSValue is not valid (not yet set)."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)
    assert not ts_val.valid()

def test_ts_value_initial_not_modified():
    """New TSValue is not modified."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)
    assert not ts_val.modified(engine_time_t(1000))

def test_ts_value_last_modified_time_initial():
    """Initial last_modified_time is MIN_ST."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)
    assert ts_val.last_modified_time() == MIN_ST

# --- Lazy delta clearing tests ---

def test_ts_value_delta_clear_on_tick_advance(current_time, next_tick):
    """Delta is cleared when current_time > last_delta_clear_time."""
    meta = create_ts_meta(TSKind.TSS, int)
    ts_val = TSValue(meta)

    # Get delta view at current_time
    delta1 = ts_val.delta_value_view(current_time)
    # Make some changes (via the view)
    # ...

    # Get delta view at next_tick - should be cleared
    delta2 = ts_val.delta_value_view(next_tick)
    # Delta should be empty (cleared)

def test_ts_value_delta_not_cleared_same_tick(current_time):
    """Delta is not cleared within same tick."""
    meta = create_ts_meta(TSKind.TSS, int)
    ts_val = TSValue(meta)

    # Multiple delta accesses in same tick don't clear
    delta1 = ts_val.delta_value_view(current_time)
    delta2 = ts_val.delta_value_view(current_time)
    # Delta should persist

# --- ts_view() tests ---

def test_ts_value_ts_view(current_time):
    """ts_view() returns coordinated TSView."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)
    view = ts_val.ts_view(current_time)
    assert view.meta() == meta
    assert view.current_time() == current_time
```

---

## Section 6: Phase 5 - TSView

### 6.1 test_ts_view.py

**Purpose:** Test TSView and kind-specific wrappers.

```python
# File: hgraph_unit_tests/_types/_time_series/test_ts_view.py

# --- TSView base tests ---

def test_ts_view_construction(current_time):
    """TSView can be constructed from TSValue."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)
    view = TSView(ts_val, current_time)

    assert view.meta() == meta
    assert view.current_time() == current_time

def test_ts_view_modified_check(current_time, next_tick):
    """modified() uses >= comparison."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)

    # Set value at current_time
    # ... (depends on how value is set)

    view_same = TSView(ts_val, current_time)
    view_later = TSView(ts_val, next_tick)

    # Modified at same time
    # assert view_same.modified()
    # Not modified at later time
    # assert not view_later.modified()

def test_ts_view_valid_check():
    """valid() returns True after value is set."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)
    view = TSView(ts_val, engine_time_t(1000))

    assert not view.valid()  # Initially not valid

def test_ts_view_value_access(current_time):
    """value() returns correct View."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)
    view = TSView(ts_val, current_time)

    val_view = view.value()
    assert val_view is not None

def test_ts_view_delta_access(current_time):
    """delta_value() returns delta View."""
    meta = create_ts_meta(TSKind.TSS, int)
    ts_val = TSValue(meta)
    view = TSView(ts_val, current_time)

    delta_view = view.delta_value()
    assert delta_view is not None

def test_ts_view_has_delta():
    """has_delta() matches meta."""
    meta_no_delta = create_ts_meta(TSKind.TS, int)
    meta_with_delta = create_ts_meta(TSKind.TSS, int)

    ts_val1 = TSValue(meta_no_delta)
    ts_val2 = TSValue(meta_with_delta)

    view1 = TSView(ts_val1, engine_time_t(1000))
    view2 = TSView(ts_val2, engine_time_t(1000))

    assert not view1.has_delta()
    assert view2.has_delta()

# --- TSScalarView tests ---

def test_ts_scalar_view_value_as(current_time):
    """TSScalarView provides typed value access."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)

    # Set value...
    view = TSScalarView(ts_val, current_time)
    # value = view.value_as[int]()

# --- TSBView tests ---

def test_tsb_view_field_by_name(current_time):
    """TSBView provides field access by name."""
    meta = create_tsb_meta([
        ("a", TSKind.TS, int),
        ("b", TSKind.TS, float),
    ])
    ts_val = TSValue(meta)
    view = TSBView(ts_val, current_time)

    field_a = view.field("a")
    field_b = view.field("b")

    assert field_a is not None
    assert field_b is not None

def test_tsb_view_field_by_index(current_time):
    """TSBView provides field access by index."""
    meta = create_tsb_meta([
        ("a", TSKind.TS, int),
        ("b", TSKind.TS, float),
    ])
    ts_val = TSValue(meta)
    view = TSBView(ts_val, current_time)

    field_0 = view.field(0)
    field_1 = view.field(1)

    assert field_0 is not None
    assert field_1 is not None

def test_tsb_view_modified_per_field(current_time):
    """TSBView tracks per-field modification."""
    meta = create_tsb_meta([
        ("a", TSKind.TS, int),
        ("b", TSKind.TS, float),
    ])
    ts_val = TSValue(meta)
    view = TSBView(ts_val, current_time)

    # Initially neither modified
    assert not view.modified("a")
    assert not view.modified("b")

# --- TSLView tests ---

def test_tsl_view_at(current_time):
    """TSLView provides element access by index."""
    meta = create_tsl_meta(TSKind.TS, int, 3)
    ts_val = TSValue(meta)
    view = TSLView(ts_val, current_time)

    elem_0 = view.at(0)
    elem_1 = view.at(1)
    elem_2 = view.at(2)

    assert elem_0 is not None
    assert elem_1 is not None
    assert elem_2 is not None

def test_tsl_view_size(current_time):
    """TSLView reports correct size."""
    meta = create_tsl_meta(TSKind.TS, int, 5)
    ts_val = TSValue(meta)
    view = TSLView(ts_val, current_time)

    assert view.size() == 5

def test_tsl_view_element_modified(current_time):
    """TSLView tracks per-element modification."""
    meta = create_tsl_meta(TSKind.TS, int, 3)
    ts_val = TSValue(meta)
    view = TSLView(ts_val, current_time)

    for i in range(3):
        assert not view.element_modified(i)

# --- TSDView tests ---

def test_tsd_view_at(current_time):
    """TSDView provides element access by key."""
    meta = create_ts_meta(TSKind.TSD, str, int)
    ts_val = TSValue(meta)
    view = TSDView(ts_val, current_time)

    # Add element...
    # elem = view.at("key")

def test_tsd_view_contains(current_time):
    """TSDView checks key existence."""
    meta = create_ts_meta(TSKind.TSD, str, int)
    ts_val = TSValue(meta)
    view = TSDView(ts_val, current_time)

    assert not view.contains("key")  # Initially empty

def test_tsd_view_delta_slots(current_time):
    """TSDView provides added/removed/updated slots."""
    meta = create_ts_meta(TSKind.TSD, str, int)
    ts_val = TSValue(meta)
    view = TSDView(ts_val, current_time)

    added = view.added_slots()
    removed = view.removed_slots()
    updated = view.updated_slots()

    assert len(added) == 0
    assert len(removed) == 0
    assert len(updated) == 0

# --- TSSView tests ---

def test_tss_view_contains(current_time):
    """TSSView checks element membership."""
    meta = create_ts_meta(TSKind.TSS, int)
    ts_val = TSValue(meta)
    view = TSSView(ts_val, current_time)

    assert not view.contains(42)

def test_tss_view_size(current_time):
    """TSSView reports correct size."""
    meta = create_ts_meta(TSKind.TSS, int)
    ts_val = TSValue(meta)
    view = TSSView(ts_val, current_time)

    assert view.size() == 0

def test_tss_view_delta_slots(current_time):
    """TSSView provides added/removed slots."""
    meta = create_ts_meta(TSKind.TSS, int)
    ts_val = TSValue(meta)
    view = TSSView(ts_val, current_time)

    added = view.added_slots()
    removed = view.removed_slots()

    assert len(added) == 0
    assert len(removed) == 0

def test_tss_view_was_cleared(current_time):
    """TSSView reports was_cleared status."""
    meta = create_ts_meta(TSKind.TSS, int)
    ts_val = TSValue(meta)
    view = TSSView(ts_val, current_time)

    assert not view.was_cleared()
```

---

## Section 7: Phase 8 - Integration Tests

### 7.1 test_ts_value_integration.py

**Purpose:** End-to-end scenarios testing complete workflows.

```python
# File: hgraph_unit_tests/_types/_time_series/test_ts_value_integration.py

def test_ts_scalar_full_lifecycle():
    """Complete lifecycle: create, set, read, modify."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)

    # Initial state
    view_t1 = ts_val.ts_view(engine_time_t(1))
    assert not view_t1.valid()
    assert not view_t1.modified()

    # Set value at t=2
    # ... set value to 42

    view_t2 = ts_val.ts_view(engine_time_t(2))
    assert view_t2.valid()
    assert view_t2.modified()
    # assert view_t2.value_as[int]() == 42

    # Read at t=3 (same value, not modified this tick)
    view_t3 = ts_val.ts_view(engine_time_t(3))
    assert view_t3.valid()
    assert not view_t3.modified()

def test_tss_delta_tracking_lifecycle():
    """TSS delta tracking across ticks."""
    meta = create_ts_meta(TSKind.TSS, int)
    ts_val = TSValue(meta)

    # Tick 1: Add elements 1, 2, 3
    view_t1 = ts_val.ts_view(engine_time_t(1))
    # ... add elements

    # Check delta at t=1
    delta_t1 = view_t1.delta_value()
    # Added should contain slots for 1, 2, 3

    # Tick 2: Remove element 2, add element 4
    view_t2 = ts_val.ts_view(engine_time_t(2))
    # ... remove 2, add 4

    # Delta should be fresh (old delta cleared)
    delta_t2 = view_t2.delta_value()
    # Removed should contain slot for 2
    # Added should contain slot for 4

def test_tsd_nested_delta_tracking():
    """TSD with TSS values tracks nested deltas."""
    meta = create_tsd_meta(str, TSKind.TSS, int)
    ts_val = TSValue(meta)

    # Add key "a" with TSS value
    # ... add key

    # Get view for "a"
    view = ts_val.ts_view(engine_time_t(1))
    tsd_view = TSDView(view)
    elem_view = tsd_view.at("a")

    # Modify TSS at "a"
    # ... add elements to the TSS

    # MapDelta should track "a" as updated
    # Child delta for "a" should track TSS additions

def test_tsb_field_modification_tracking():
    """TSB tracks modification per field."""
    meta = create_tsb_meta([
        ("x", TSKind.TS, int),
        ("y", TSKind.TS, float),
        ("z", TSKind.TSS, int),
    ])
    ts_val = TSValue(meta)

    # Modify only field "x" at t=1
    # ... set x

    view_t1 = ts_val.ts_view(engine_time_t(1))
    tsb_view = TSBView(view_t1)

    assert tsb_view.modified("x")
    assert not tsb_view.modified("y")
    assert not tsb_view.modified("z")

    # Modify field "z" TSS at t=2
    # ... add to z

    view_t2 = ts_val.ts_view(engine_time_t(2))
    tsb_view2 = TSBView(view_t2)

    assert not tsb_view2.modified("x")  # Not modified this tick
    assert tsb_view2.modified("z")

def test_observer_notification_chain():
    """Observers notified on modification."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)

    observer = MockNotifiable()

    # Get observer view and add observer
    obs_view = ts_val.observer_view()
    obs_list = obs_view.as_type[ObserverList]()
    obs_list.add_observer(observer)

    # Modify value
    # ... set value

    # Observer should be notified
    assert observer.modified_count == 1

def test_tsd_slot_observer_wiring():
    """TSD slots automatically wire observers."""
    meta = create_tsd_meta(str, TSKind.TS, int)
    ts_val = TSValue(meta)

    # Add key "a"
    # The time_ and observer_ arrays should be auto-wired

    view = ts_val.ts_view(engine_time_t(1))
    # Check that per-slot time tracking works
```

### 7.2 test_ts_value_conformance.py

**Purpose:** Verify C++ matches Python reference implementation.

```python
# File: hgraph_unit_tests/_types/_time_series/test_ts_value_conformance.py

import os
import pytest

@pytest.fixture
def use_python_impl():
    """Run with Python implementation for comparison."""
    old_val = os.environ.get('HGRAPH_USE_CPP', '1')
    os.environ['HGRAPH_USE_CPP'] = '0'
    yield
    os.environ['HGRAPH_USE_CPP'] = old_val

@pytest.fixture
def use_cpp_impl():
    """Run with C++ implementation."""
    old_val = os.environ.get('HGRAPH_USE_CPP', '1')
    os.environ['HGRAPH_USE_CPP'] = '1'
    yield
    os.environ['HGRAPH_USE_CPP'] = old_val

def test_modified_semantics_match(use_python_impl):
    """Python modified() semantics."""
    # ... test
    pass

def test_modified_semantics_match_cpp(use_cpp_impl):
    """C++ modified() semantics match Python."""
    # ... same test, must produce same results
    pass

def test_delta_clear_semantics_match():
    """Delta clearing behaves identically."""
    pass

def test_observer_notification_order():
    """Observer notification order is consistent."""
    pass

def test_nested_tsd_tss_behavior():
    """Nested TSD[K, TSS[T]] behaves identically."""
    pass

@pytest.mark.parametrize("ts_kind,value_type", [
    (TSKind.TS, int),
    (TSKind.TS, float),
    (TSKind.TS, str),
    (TSKind.TSS, int),
    (TSKind.TSS, str),
])
def test_basic_ts_conformance(ts_kind, value_type):
    """Basic TS operations conform across implementations."""
    pass
```

---

## Section 8: Test Data Generators

### 8.1 Common Fixtures

```python
# File: hgraph_unit_tests/_types/_time_series/conftest.py

import pytest
from hgraph import (
    TSKind, TSMeta, TSValue, TSView,
    create_ts_meta, create_tsb_meta, create_tsl_meta, create_tsd_meta,
    engine_time_t, MIN_ST, MAX_ST,
    ObserverList, SetDelta, MapDelta, BundleDeltaNav, ListDeltaNav,
)

@pytest.fixture
def current_time():
    """Standard test time."""
    return engine_time_t(1000)

@pytest.fixture
def next_tick():
    """Next engine tick."""
    return engine_time_t(1001)

@pytest.fixture
def ts_meta_int():
    """TS[int] meta."""
    return create_ts_meta(TSKind.TS, int)

@pytest.fixture
def tss_meta_int():
    """TSS[int] meta."""
    return create_ts_meta(TSKind.TSS, int)

@pytest.fixture
def tsd_meta_str_int():
    """TSD[str, int] meta."""
    return create_ts_meta(TSKind.TSD, str, int)

@pytest.fixture
def tsb_meta_mixed():
    """TSB with mixed field types."""
    return create_tsb_meta([
        ("scalar", TSKind.TS, int),
        ("set", TSKind.TSS, int),
        ("nested_dict", TSKind.TSD, str, int),
    ])

@pytest.fixture
def tsl_meta_ts():
    """TSL[TS[int], 5] meta."""
    return create_tsl_meta(TSKind.TS, int, 5)

@pytest.fixture
def tsl_meta_tss():
    """TSL[TSS[int], 3] meta."""
    return create_tsl_meta(TSKind.TSS, int, 3)

class MockNotifiable:
    """Mock observer for testing notifications."""
    def __init__(self):
        self.modified_times = []
        self.removed_count = 0

    def notify_modified(self, t: engine_time_t):
        self.modified_times.append(t)

    def notify_removed(self):
        self.removed_count += 1

    @property
    def modified_count(self):
        return len(self.modified_times)

    @property
    def last_time(self):
        return self.modified_times[-1] if self.modified_times else None

@pytest.fixture
def mock_notifiable():
    return MockNotifiable()

def make_notifiable_list(count: int):
    """Create list of mock notifiables."""
    return [MockNotifiable() for _ in range(count)]
```

---

## Section 9: Test Categories

### 9.1 Smoke Tests (Quick Validation)

```python
# pytest markers for test categorization

@pytest.mark.smoke
def test_ts_value_construction_smoke():
    """Quick sanity check that TSValue can be created."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)
    assert ts_val is not None

@pytest.mark.smoke
def test_ts_view_construction_smoke():
    """Quick sanity check that TSView can be created."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)
    view = TSView(ts_val, engine_time_t(1000))
    assert view is not None
```

### 9.2 Edge Cases

```python
@pytest.mark.edge_case
def test_min_time_handling():
    """MIN_ST is handled correctly."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)

    view = ts_val.ts_view(MIN_ST)
    assert not view.valid()
    assert not view.modified()

@pytest.mark.edge_case
def test_max_time_handling():
    """MAX_ST is handled correctly."""
    meta = create_ts_meta(TSKind.TS, int)
    ts_val = TSValue(meta)

    view = ts_val.ts_view(MAX_ST)
    # Should not crash

@pytest.mark.edge_case
def test_empty_tsb():
    """TSB with zero fields."""
    meta = create_tsb_meta([])
    ts_val = TSValue(meta)
    # Should handle gracefully

@pytest.mark.edge_case
def test_tsl_zero_size():
    """TSL with zero elements."""
    meta = create_tsl_meta(TSKind.TS, int, 0)
    ts_val = TSValue(meta)
    view = TSLView(ts_val, engine_time_t(1000))
    assert view.size() == 0
```

### 9.3 Performance Tests

```python
@pytest.mark.performance
@pytest.mark.slow
def test_tsd_many_elements():
    """TSD with many elements performs reasonably."""
    meta = create_ts_meta(TSKind.TSD, int, int)
    ts_val = TSValue(meta)

    # Add 10000 elements
    for i in range(10000):
        # ... add element
        pass

    # Access should be fast

@pytest.mark.performance
@pytest.mark.slow
def test_observer_many_subscribers():
    """Many observers don't cause issues."""
    obs_list = ObserverList()
    observers = [MockNotifiable() for _ in range(1000)]

    for obs in observers:
        obs_list.add_observer(obs)

    # Notify should work
    obs_list.notify_modified(engine_time_t(1000))
```

---

## Section 10: Running Tests

### 10.1 Test Commands

```bash
# Run all TSValue tests
uv run pytest hgraph_unit_tests/_types/_time_series/ -v

# Run specific phase
uv run pytest hgraph_unit_tests/_types/_time_series/test_observer_list.py -v

# Run smoke tests only
uv run pytest hgraph_unit_tests/_types/_time_series/ -v -m smoke

# Run with Python implementation for comparison
HGRAPH_USE_CPP=0 uv run pytest hgraph_unit_tests/_types/_time_series/ -v

# Run with coverage
uv run pytest hgraph_unit_tests/_types/_time_series/ -v --cov=hgraph.types.time_series
```

### 10.2 Expected Test Counts by Phase

| Phase | Test File | Expected Tests |
|-------|-----------|----------------|
| 1 | test_observer_list.py | ~8 |
| 1 | test_time_array.py | ~10 |
| 1 | test_observer_array.py | ~7 |
| 2 | test_set_delta.py | ~10 |
| 2 | test_map_delta.py | ~10 |
| 2 | test_delta_nav.py | ~7 |
| 3 | test_ts_meta_schema.py | ~20 |
| 4 | test_ts_value.py | ~15 |
| 5 | test_ts_view.py | ~25 |
| 8 | test_ts_value_integration.py | ~10 |
| 8 | test_ts_value_conformance.py | ~10 |
| **Total** | | **~130** |
