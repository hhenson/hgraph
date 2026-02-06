"""
Tests for SignalView.

SignalView provides presence-only semantics for SIGNAL time-series.
Key behaviors:
- value() returns modification state (bool)
- Child signals aggregate modified/valid state
- Reference dereferencing: binds to actual data sources, not REF wrappers
"""

import pytest

from hgraph._feature_switch import is_feature_enabled

# Skip all tests if C++ is not enabled
pytestmark = pytest.mark.skipif(
    not is_feature_enabled("use_cpp"),
    reason="C++ runtime not enabled"
)


@pytest.fixture
def hgraph_module():
    """Import and return the hgraph module (ensures C++ runtime is loaded)."""
    import hgraph._hgraph as _hgraph
    return _hgraph


@pytest.fixture
def signal_view_class(hgraph_module):
    """Get the SignalView class."""
    return hgraph_module.SignalView


@pytest.fixture
def ts_int_meta(hgraph_module):
    """Create TSMeta for TS[int]."""
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    ts_meta = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
    return ts_meta.cpp_type


class TestSignalViewConstruction:
    """Tests for SignalView construction."""

    def test_default_constructor_creates_unbound(self, signal_view_class):
        """Default constructor creates an unbound SignalView."""
        signal = signal_view_class()

        assert signal.bound() == False
        assert signal.has_children() == False

    def test_unbound_signal_not_modified(self, signal_view_class):
        """Unbound signal returns False for modified."""
        signal = signal_view_class()

        assert signal.modified() == False

    def test_unbound_signal_not_valid(self, signal_view_class):
        """Unbound signal returns False for valid."""
        signal = signal_view_class()

        assert signal.valid() == False


class TestSignalViewValue:
    """Tests for SignalView value semantics."""

    def test_value_returns_modified_state(self, signal_view_class):
        """Signal's value() returns its modification state."""
        signal = signal_view_class()

        # Unbound signal - not modified
        assert signal.value() == False
        assert signal.value() == signal.modified()

    def test_delta_value_same_as_value(self, signal_view_class):
        """Signal's delta_value() is the same as value()."""
        signal = signal_view_class()

        assert signal.delta_value() == signal.value()


class TestSignalViewChildPattern:
    """Tests for child signal pattern."""

    def test_child_signal_lazy_creation(self, signal_view_class):
        """Child signals are created lazily on access."""
        signal = signal_view_class()

        assert signal.has_children() == False
        assert signal.child_count() == 0

        # Access child - should create it
        child = signal[0]

        assert signal.has_children() == True
        assert signal.child_count() == 1

    def test_child_signals_extend_on_access(self, signal_view_class):
        """Accessing higher indices creates intermediate children."""
        signal = signal_view_class()

        child_5 = signal[5]

        assert signal.child_count() == 6  # 0, 1, 2, 3, 4, 5

    def test_multiple_child_access_same_object(self, signal_view_class):
        """Accessing same index returns same child object."""
        signal = signal_view_class()

        child_a = signal[0]
        child_b = signal[0]

        # Should be same object (reference)
        assert child_a is child_b

    def test_at_returns_invalid_for_nonexistent(self, signal_view_class):
        """at() returns invalid signal for non-existent index."""
        signal = signal_view_class()

        # No children created yet
        child = signal.at(0)

        # Should be "invalid" (not bound, no children)
        assert child.bound() == False


class TestSignalViewChildAggregation:
    """Tests for child signal aggregation."""

    def test_has_children_makes_signal_bound(self, signal_view_class):
        """Having children makes the signal bound."""
        signal = signal_view_class()

        assert signal.bound() == False

        _ = signal[0]  # Create a child

        assert signal.bound() == True

    def test_unbind_clears_children(self, signal_view_class):
        """unbind() clears all child signals."""
        signal = signal_view_class()

        _ = signal[0]
        _ = signal[1]

        assert signal.has_children() == True
        assert signal.child_count() == 2

        signal.unbind()

        assert signal.has_children() == False
        assert signal.child_count() == 0


class TestSignalViewActivePassive:
    """Tests for active/passive state."""

    def test_default_is_passive(self, signal_view_class):
        """Signal starts in passive state."""
        signal = signal_view_class()

        assert signal.active() == False

    def test_make_active(self, signal_view_class):
        """make_active() sets signal to active."""
        signal = signal_view_class()

        signal.make_active()

        assert signal.active() == True

    def test_make_passive(self, signal_view_class):
        """make_passive() sets signal to passive."""
        signal = signal_view_class()

        signal.make_active()
        assert signal.active() == True

        signal.make_passive()
        assert signal.active() == False

    def test_make_active_propagates_to_existing_children(self, signal_view_class):
        """make_active() activates all existing children."""
        signal = signal_view_class()

        # Create children first
        child0 = signal[0]
        child1 = signal[1]

        assert child0.active() == False
        assert child1.active() == False

        signal.make_active()

        assert child0.active() == True
        assert child1.active() == True

    def test_make_passive_propagates_to_children(self, signal_view_class):
        """make_passive() deactivates all children."""
        signal = signal_view_class()
        signal.make_active()

        child0 = signal[0]
        child1 = signal[1]

        signal.make_passive()

        assert child0.active() == False
        assert child1.active() == False

    def test_new_children_inherit_active_state(self, signal_view_class):
        """New children inherit parent's active state."""
        signal = signal_view_class()
        signal.make_active()

        # Create child after making active
        child = signal[0]

        assert child.active() == True


class TestSignalViewMetadata:
    """Tests for SignalView metadata."""

    def test_ts_meta_returns_signal_meta(self, signal_view_class, hgraph_module):
        """ts_meta() returns the SIGNAL metadata singleton."""
        signal = signal_view_class()

        meta = signal.ts_meta()

        assert meta is not None
        assert meta.kind == hgraph_module.TSKind.SIGNAL

    def test_source_meta_none_when_unbound(self, signal_view_class):
        """source_meta() returns None for unbound signal."""
        signal = signal_view_class()

        assert signal.source_meta() is None


class TestSignalViewBoolConversion:
    """Tests for SignalView bool conversion."""

    def test_unbound_no_children_is_false(self, signal_view_class):
        """Unbound signal with no children converts to False."""
        signal = signal_view_class()

        assert bool(signal) == False

    def test_with_children_is_true(self, signal_view_class):
        """Signal with children converts to True."""
        signal = signal_view_class()

        _ = signal[0]

        assert bool(signal) == True


# ============================================================================
# Phase 3: Field Access and Binding Tests
# ============================================================================


@pytest.fixture
def tsb_meta(hgraph_module):
    """Create TSMeta for TSB[x: TS[int], y: TS[float]]."""
    from hgraph._types._tsb_meta_data import HgTSBTypeMetaData, HgTimeSeriesSchemaTypeMetaData
    from hgraph._types._tsb_type import TimeSeriesSchema
    from hgraph._types._ts_type import TS

    class TestSchema(TimeSeriesSchema):
        x: TS[int]
        y: TS[float]

    schema_meta = HgTimeSeriesSchemaTypeMetaData(TestSchema)
    tsb = HgTSBTypeMetaData(schema_meta)
    return tsb.cpp_type


@pytest.fixture
def ts_int_meta(hgraph_module):
    """Create TSMeta for TS[int]."""
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    ts_meta = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
    return ts_meta.cpp_type


class TestSignalViewFieldAccess:
    """Tests for SignalView field access by name."""

    def test_field_on_unbound_raises_error(self, signal_view_class):
        """field() on unbound signal raises error."""
        signal = signal_view_class()

        with pytest.raises(Exception):
            signal.field("x")

    def test_field_creates_child_at_correct_index(self, signal_view_class, hgraph_module, tsb_meta):
        """field() creates child at the correct field index."""
        from datetime import datetime

        TSValue = hgraph_module.TSValue
        TEST_TIME = datetime(2024, 1, 1, 0, 0, 0)

        # Create TSValue with TSB schema
        ts_value = TSValue(tsb_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # Create signal and bind to the TSB
        signal = signal_view_class()
        signal.bind(ts_view)

        # Access field by name
        child_x = signal.field("x")

        # Should have created children (at least for field "x")
        assert signal.has_children() == True

    def test_field_not_found_raises_error(self, signal_view_class, hgraph_module, tsb_meta):
        """field() raises error when field name not found."""
        from datetime import datetime

        TSValue = hgraph_module.TSValue
        TEST_TIME = datetime(2024, 1, 1, 0, 0, 0)

        ts_value = TSValue(tsb_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        signal = signal_view_class()
        signal.bind(ts_view)

        with pytest.raises(Exception):
            signal.field("nonexistent")

    def test_field_same_as_index_access(self, signal_view_class, hgraph_module, tsb_meta):
        """field() returns same child as index access for same field."""
        from datetime import datetime

        TSValue = hgraph_module.TSValue
        TEST_TIME = datetime(2024, 1, 1, 0, 0, 0)

        ts_value = TSValue(tsb_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        signal = signal_view_class()
        signal.bind(ts_view)

        # Access by field name
        child_x_by_name = signal.field("x")

        # Access by index (x is field 0)
        child_x_by_index = signal[0]

        # Should be the same child
        assert child_x_by_name is child_x_by_index


class TestSignalViewBinding:
    """Tests for SignalView binding to sources."""

    def test_bind_to_ts_sets_source_meta(self, signal_view_class, hgraph_module, ts_int_meta):
        """Binding to TS[int] sets source_meta."""
        from datetime import datetime

        TSValue = hgraph_module.TSValue
        TEST_TIME = datetime(2024, 1, 1, 0, 0, 0)

        ts_value = TSValue(ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        signal = signal_view_class()
        signal.bind(ts_view)

        # source_meta should be set
        assert signal.source_meta() is not None
        assert signal.bound() == True

    def test_bind_to_tsb_sets_source_meta(self, signal_view_class, hgraph_module, tsb_meta):
        """Binding to TSB sets source_meta."""
        from datetime import datetime

        TSValue = hgraph_module.TSValue
        TSKind = hgraph_module.TSKind
        TEST_TIME = datetime(2024, 1, 1, 0, 0, 0)

        ts_value = TSValue(tsb_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        signal = signal_view_class()
        signal.bind(ts_view)

        # source_meta should be the TSB
        assert signal.source_meta() is not None
        assert signal.source_meta().kind == TSKind.TSB
        assert signal.bound() == True

    def test_unbind_clears_source_meta(self, signal_view_class, hgraph_module, ts_int_meta):
        """unbind() clears source_meta."""
        from datetime import datetime

        TSValue = hgraph_module.TSValue
        TEST_TIME = datetime(2024, 1, 1, 0, 0, 0)

        ts_value = TSValue(ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        signal = signal_view_class()
        signal.bind(ts_view)
        assert signal.source_meta() is not None

        signal.unbind()

        assert signal.source_meta() is None
        assert signal.bound() == False


# ============================================================================
# Phase 5: Integration & Conformance Tests
# ============================================================================


@pytest.fixture
def ref_ts_int_meta(hgraph_module, ts_int_meta):
    """Create TSMeta for REF[TS[int]]."""
    TSTypeRegistry = hgraph_module.TSTypeRegistry
    return TSTypeRegistry.instance().ref(ts_int_meta)


@pytest.fixture
def tsl_ts_int_meta(hgraph_module, ts_int_meta):
    """Create TSMeta for TSL[TS[int], 3]."""
    TSTypeRegistry = hgraph_module.TSTypeRegistry
    return TSTypeRegistry.instance().tsl(ts_int_meta, 3)


class TestSignalViewReferenceDereferencing:
    """Tests for SignalView reference dereferencing behavior."""

    def test_ts_meta_always_returns_signal(self, signal_view_class, hgraph_module, ts_int_meta):
        """ts_meta() always returns SIGNAL, regardless of binding."""
        from datetime import datetime

        TSValue = hgraph_module.TSValue
        TSKind = hgraph_module.TSKind
        TEST_TIME = datetime(2024, 1, 1, 0, 0, 0)

        signal = signal_view_class()

        # Unbound - ts_meta should be SIGNAL
        assert signal.ts_meta().kind == TSKind.SIGNAL

        # Bound - ts_meta should still be SIGNAL
        ts_value = TSValue(ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)
        signal.bind(ts_view)

        assert signal.ts_meta().kind == TSKind.SIGNAL


class TestSignalViewChildBindingToComposite:
    """Tests for SignalView child binding to composite types."""

    def test_child_binding_to_tsb_field(self, signal_view_class, hgraph_module, tsb_meta):
        """Child signals bind to TSB fields correctly."""
        from datetime import datetime

        TSValue = hgraph_module.TSValue
        TSKind = hgraph_module.TSKind
        TEST_TIME = datetime(2024, 1, 1, 0, 0, 0)

        ts_value = TSValue(tsb_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        signal = signal_view_class()
        signal.bind(ts_view)

        # Access child by field name
        child_x = signal.field("x")
        child_y = signal.field("y")

        # Children should be bound to their respective field types
        assert child_x.bound() == True
        assert child_y.bound() == True
        assert child_x.source_meta().kind == TSKind.TSValue
        assert child_y.source_meta().kind == TSKind.TSValue


class TestSignalViewModificationTracking:
    """Tests for SignalView modification tracking with bound sources."""

    def test_bound_signal_starts_not_modified(self, signal_view_class, hgraph_module, ts_int_meta):
        """Newly bound signal reports not modified initially."""
        from datetime import datetime

        TSValue = hgraph_module.TSValue
        TEST_TIME = datetime(2024, 1, 1, 0, 0, 0)

        ts_value = TSValue(ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        signal = signal_view_class()
        signal.bind(ts_view)

        # Signal should report not modified initially
        # (no tick has occurred at the current time)
        assert signal.modified() == False

    def test_bound_signal_valid_state(self, signal_view_class, hgraph_module, ts_int_meta):
        """Bound signal's valid state reflects source."""
        from datetime import datetime

        TSValue = hgraph_module.TSValue
        TEST_TIME = datetime(2024, 1, 1, 0, 0, 0)

        ts_value = TSValue(ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        signal = signal_view_class()
        signal.bind(ts_view)

        # Valid state reflects the underlying source
        # TSValue starts as valid by default
        assert signal.valid() == ts_view.valid()


class TestSignalViewConformance:
    """Conformance tests documenting behavior differences with Python implementation.

    Note: The C++ SignalView has some intentional design differences from Python:
    - C++: value() returns modified() result (modification state)
    - Python: value always returns True (presence-only semantics in graph context)

    The C++ behavior provides more granular control for unit testing.
    In actual graph evaluation, signals are typically only accessed when modified.
    """

    def test_value_returns_modification_state(self, signal_view_class):
        """C++ SignalView.value() returns modification state (differs from Python).

        Design note: Python's PythonTimeSeriesSignal.value always returns True,
        as signals in graph context are only accessed when they tick.
        C++ returns the actual modification state for flexibility.
        """
        signal = signal_view_class()

        # C++ behavior: unbound/unmodified signal returns False
        assert signal.value() == signal.modified()
        assert signal.value() == False

    def test_delta_value_same_as_value(self, signal_view_class):
        """delta_value() returns same as value() for SIGNAL.

        This matches Python behavior - SIGNAL has no delta concept.
        """
        signal = signal_view_class()
        assert signal.delta_value() == signal.value()

    def test_last_modified_time_defaults_to_min(self, signal_view_class, hgraph_module):
        """Unbound signal's last_modified_time is MIN_ST."""
        from datetime import datetime

        signal = signal_view_class()

        # Should return MIN_DT (epoch) for uninitialized signal
        lmt = signal.last_modified_time()
        assert lmt == datetime(1970, 1, 1, 0, 0, 0, 0)
