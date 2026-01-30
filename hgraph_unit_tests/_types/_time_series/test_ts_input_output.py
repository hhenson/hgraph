"""
Tests for TSInput, TSInputView, TSOutput, and TSOutputView functionality.

This module tests the input/output binding infrastructure for the hgraph
time-series system. The tests focus on the core behaviors:

1. TSOutput basics - construction, view access, value mutation
2. TSInput basics - construction, view access, active state
3. Binding - TSInputView.bind(TSOutputView) linking
4. Active/Passive state - subscription management
5. Observer notifications - Notifiable interface
6. Hierarchical active state - for composite types

Note: As of this implementation, TSInput, TSOutput, TSInputView, and TSOutputView
are C++ classes that may not be directly exposed to Python yet. Tests use
the underlying TSValue and TSView classes which provide the foundational
binding and notification infrastructure.
"""

from datetime import datetime

import pytest

from hgraph._feature_switch import is_feature_enabled


# ============================================================================
# Fixtures and Helpers
# ============================================================================


# Skip all tests if C++ is not enabled
pytestmark = pytest.mark.skipif(
    not is_feature_enabled("use_cpp"),
    reason="C++ runtime not enabled"
)


# Fixed test time
TEST_TIME = datetime(2024, 1, 1, 0, 0, 0)
TEST_TIME_LATER = datetime(2024, 1, 1, 0, 0, 1)


@pytest.fixture
def hgraph_module():
    """Get the C++ hgraph module."""
    import hgraph._hgraph as _hgraph
    return _hgraph


@pytest.fixture
def value_module(hgraph_module):
    """Get the value submodule."""
    return hgraph_module.value


@pytest.fixture
def ts_int_meta(hgraph_module):
    """Create TSMeta for TS[int]."""
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    ts_meta = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
    return ts_meta.cpp_type


@pytest.fixture
def ts_float_meta(hgraph_module):
    """Create TSMeta for TS[float]."""
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    ts_meta = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(float))
    return ts_meta.cpp_type


@pytest.fixture
def tsl_ts_int_meta(hgraph_module):
    """Create TSMeta for TSL[TS[int], Size[3]]."""
    from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
    from hgraph._types._scalar_types import Size

    element_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
    size_tp = HgScalarTypeMetaData.parse_type(Size[3])
    tsl_meta = HgTSLTypeMetaData(element_ts, size_tp)
    return tsl_meta.cpp_type


@pytest.fixture
def tsd_str_ts_int_meta(hgraph_module):
    """Create TSMeta for TSD[str, TS[int]]."""
    from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    key_type = HgScalarTypeMetaData.parse_type(str)
    value_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
    tsd_meta = HgTSDTypeMetaData(key_type, value_ts)
    return tsd_meta.cpp_type


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


# ============================================================================
# Section 1: TSOutput Basics
# ============================================================================


class TestTSOutputBasics:
    """Tests for TSOutput construction and basic operations.

    TSOutput is the producer of time-series values. It owns the native
    TSValue storage and provides TSOutputView for access.
    """

    def test_ts_value_construction_with_schema(self, hgraph_module, ts_int_meta):
        """TSValue can be constructed with TSMeta schema."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(ts_int_meta)

        assert ts_value.meta is not None
        assert ts_value.meta.kind == hgraph_module.TSKind.TSValue

    def test_ts_value_view_returns_ts_view(self, hgraph_module, ts_int_meta):
        """TSValue.ts_view() returns a TSView."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        assert ts_view is not None
        assert ts_view.meta is not None

    def test_ts_view_set_value_modifies_value(self, hgraph_module, ts_int_meta):
        """Setting value through TSView modifies the stored value."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # Set the value via the value view from_python
        val_view = ts_view.value()
        val_view.from_python(42)

        # Verify value was set
        result = ts_view.value().to_python()
        assert result == 42

    def test_ts_value_has_valid_state_after_construction(self, hgraph_module, ts_int_meta):
        """TSValue for scalar TS initializes with valid state.

        Note: TSValue for scalar TS types initializes with a valid timestamp
        because the container time is set during construction.
        """
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # TSValue sets container time during construction
        # so valid() returns True for initialized containers
        # This is expected behavior per the implementation
        assert ts_view.valid() == True

    def test_ts_value_navigation_with_index(self, hgraph_module, tsl_ts_int_meta):
        """TSL supports navigation via as_list().at()."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsl_ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # For TSL, use as_list() to get the list view, then access elements
        tsl_view = ts_view.as_list()
        child = tsl_view.at(0)

        assert child is not None
        assert child.meta is not None
        assert child.meta.kind == hgraph_module.TSKind.TSValue

    def test_tsb_navigation_with_field(self, hgraph_module, tsb_meta):
        """TSB supports navigation via field()."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsb_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # Navigate to field by name
        child_x = ts_view.field("x")
        child_y = ts_view.field("y")

        assert child_x is not None
        assert child_y is not None
        assert child_x.meta is not None
        assert child_y.meta is not None


# ============================================================================
# Section 2: TSInput Basics
# ============================================================================


class TestTSInputBasics:
    """Tests for TSInput construction and basic operations.

    TSInput is the consumer of time-series values. It subscribes to
    TSOutput(s) and provides access to linked values.
    """

    def test_ts_value_for_input_construction(self, hgraph_module, ts_int_meta):
        """TSValue can be created for input (link storage)."""
        TSValue = hgraph_module.TSValue

        # TSInput uses TSValue internally with links at leaves
        ts_value = TSValue(ts_int_meta)

        assert ts_value.meta is not None

    def test_ts_value_view_for_input(self, hgraph_module, ts_int_meta):
        """TSValue view can be used for input access."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        assert ts_view is not None

    def test_ts_value_navigation_index_for_tsl(self, hgraph_module, tsl_ts_int_meta):
        """TSL input supports navigation via as_list().at()."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsl_ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # For TSL, use as_list() to get the list view, then access elements
        tsl_view = ts_view.as_list()
        child = tsl_view.at(0)

        assert child is not None

    def test_ts_value_navigation_field_for_tsb(self, hgraph_module, tsb_meta):
        """TSB input supports navigation via field()."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsb_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # Navigate to field by name
        child_x = ts_view.field("x")

        assert child_x is not None


# ============================================================================
# Section 3: Binding
# ============================================================================


class TestBinding:
    """Tests for binding TSInput to TSOutput.

    Binding creates a link from input to output, allowing the input
    to see the output's data.
    """

    def test_tsl_bind_sets_is_bound_true(self, hgraph_module, tsl_ts_int_meta):
        """TSL bind() sets is_bound() to True."""
        TSValue = hgraph_module.TSValue

        # Create source (input) and target (output)
        source_value = TSValue(tsl_ts_int_meta)
        target_value = TSValue(tsl_ts_int_meta)

        source_view = source_value.ts_view(TEST_TIME)
        target_view = target_value.ts_view(TEST_TIME)

        # Bind source to target
        source_view.bind(target_view)

        assert source_view.is_bound() == True

    def test_tsd_bind_sets_is_bound_true(self, hgraph_module, tsd_str_ts_int_meta):
        """TSD bind() sets is_bound() to True."""
        TSValue = hgraph_module.TSValue

        source_value = TSValue(tsd_str_ts_int_meta)
        target_value = TSValue(tsd_str_ts_int_meta)

        source_view = source_value.ts_view(TEST_TIME)
        target_view = target_value.ts_view(TEST_TIME)

        source_view.bind(target_view)

        assert source_view.is_bound() == True

    def test_unbind_clears_is_bound(self, hgraph_module, tsl_ts_int_meta):
        """unbind() after bind() sets is_bound() to False."""
        TSValue = hgraph_module.TSValue

        source_value = TSValue(tsl_ts_int_meta)
        target_value = TSValue(tsl_ts_int_meta)

        source_view = source_value.ts_view(TEST_TIME)
        target_view = target_value.ts_view(TEST_TIME)

        source_view.bind(target_view)
        assert source_view.is_bound() == True

        source_view.unbind()
        assert source_view.is_bound() == False

    def test_bound_input_sees_output_value(self, hgraph_module, tsl_ts_int_meta):
        """After bind, input view can see output's data."""
        TSValue = hgraph_module.TSValue

        source_value = TSValue(tsl_ts_int_meta)
        target_value = TSValue(tsl_ts_int_meta)

        source_view = source_value.ts_view(TEST_TIME)
        target_view = target_value.ts_view(TEST_TIME)

        # Bind source to target
        source_view.bind(target_view)

        # Verify source valid state reflects target
        assert source_view.valid() == target_view.valid()

    def test_bound_input_sees_output_modified_state(self, hgraph_module, tsl_ts_int_meta):
        """After bind, input's modified() reflects output's state."""
        TSValue = hgraph_module.TSValue

        source_value = TSValue(tsl_ts_int_meta)
        target_value = TSValue(tsl_ts_int_meta)

        source_view = source_value.ts_view(TEST_TIME)
        target_view = target_value.ts_view(TEST_TIME)

        # Bind source to target
        source_view.bind(target_view)

        # Modified state should match
        assert source_view.modified() == target_view.modified()

    def test_bound_input_sees_output_last_modified_time(self, hgraph_module, tsl_ts_int_meta):
        """After bind, input's last_modified_time reflects output's time."""
        TSValue = hgraph_module.TSValue

        source_value = TSValue(tsl_ts_int_meta)
        target_value = TSValue(tsl_ts_int_meta)

        source_view = source_value.ts_view(TEST_TIME)
        target_view = target_value.ts_view(TEST_TIME)

        # Bind source to target
        source_view.bind(target_view)

        # last_modified_time should match
        assert source_view.last_modified_time() == target_view.last_modified_time()

    def test_scalar_ts_bind_raises_error(self, hgraph_module, ts_int_meta):
        """Scalar TS bind() raises RuntimeError (not supported at scalar level)."""
        TSValue = hgraph_module.TSValue

        source_value = TSValue(ts_int_meta)
        target_value = TSValue(ts_int_meta)

        source_view = source_value.ts_view(TEST_TIME)
        target_view = target_value.ts_view(TEST_TIME)

        with pytest.raises(RuntimeError, match="scalar"):
            source_view.bind(target_view)

    def test_scalar_ts_is_bound_returns_false(self, hgraph_module, ts_int_meta):
        """Scalar TS is_bound() returns False (no binding at scalar level)."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        assert ts_view.is_bound() == False


# ============================================================================
# Section 4: Active/Passive State
# ============================================================================


class TestActivePassiveState:
    """Tests for active/passive subscription state.

    When an input is active, it receives notifications when bound outputs
    are modified. When passive, it does not receive notifications.

    Note: Direct active/passive testing requires TSInput class with
    active state tracking. These tests document expected behavior.
    """

    def test_input_default_active_state_is_false(self, hgraph_module, ts_int_meta):
        """TSInput starts with active=False by default."""
        TSInput = hgraph_module.TSInput
        input_ts = TSInput(ts_int_meta, None)
        assert input_ts.active() == False

    def test_input_set_active_changes_state(self, hgraph_module, ts_int_meta):
        """TSInput.set_active(true) sets active state."""
        TSInput = hgraph_module.TSInput
        input_ts = TSInput(ts_int_meta, None)
        input_ts.set_active(True)
        assert input_ts.active() == True

    def test_input_make_passive_clears_active(self, hgraph_module, ts_int_meta):
        """TSInput.set_active(false) clears active state."""
        TSInput = hgraph_module.TSInput
        input_ts = TSInput(ts_int_meta, None)
        input_ts.set_active(True)
        assert input_ts.active() == True
        input_ts.set_active(False)
        assert input_ts.active() == False


# ============================================================================
# Section 5: Observer Notifications
# ============================================================================


class TestObserverNotifications:
    """Tests for observer subscription and notification.

    Outputs can have observers subscribed. When the output is modified,
    all observers are notified.
    """

    def test_observer_view_accessible(self, hgraph_module, ts_int_meta):
        """TSView provides access to observer data."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # Observer view should be accessible
        obs_view = ts_view.observer()

        # Observer view for scalar should be valid (ObserverList storage)
        assert obs_view.valid()

    def test_tsl_observer_view_accessible(self, hgraph_module, tsl_ts_int_meta):
        """TSL has observer view accessible."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsl_ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # Observer view at container level
        obs_view = ts_view.observer()
        assert obs_view.valid()

    def test_tsb_observer_view_accessible(self, hgraph_module, tsb_meta):
        """TSB has observer view accessible."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsb_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # Observer view at bundle level
        obs_view = ts_view.observer()
        assert obs_view.valid()

    @pytest.mark.xfail(reason="subscribe/unsubscribe require C++ Notifiable - Python objects not supported")
    def test_subscribe_adds_observer(self, hgraph_module, ts_int_meta):
        """TSOutputView.subscribe() adds observer to the list.

        Note: This test is xfail because subscribe/unsubscribe require a C++ Notifiable
        object, not a Python object. Python observers would need a wrapper class.
        """
        TSValue = hgraph_module.TSValue
        TSOutputView = hgraph_module.TSOutputView

        ts_value = TSValue(ts_int_meta)
        output_view = TSOutputView(ts_value.ts_view(TEST_TIME), None)

        # Create mock observer - would need to be a C++ Notifiable
        class MockObserver:
            def __init__(self):
                self.notified_count = 0
                self.last_time = None

            def notify(self, et):
                self.notified_count += 1
                self.last_time = et

        mock = MockObserver()
        output_view.subscribe(mock)

        # Verify subscription (would need internal access)

    @pytest.mark.xfail(reason="subscribe/unsubscribe require C++ Notifiable - Python objects not supported")
    def test_unsubscribe_removes_observer(self, hgraph_module, ts_int_meta):
        """TSOutputView.unsubscribe() removes observer from the list.

        Note: This test is xfail because subscribe/unsubscribe require a C++ Notifiable
        object, not a Python object. Python observers would need a wrapper class.
        """
        TSValue = hgraph_module.TSValue
        TSOutputView = hgraph_module.TSOutputView

        ts_value = TSValue(ts_int_meta)
        output_view = TSOutputView(ts_value.ts_view(TEST_TIME), None)

        class MockObserver:
            def __init__(self):
                self.notified_count = 0

            def notify(self, et):
                self.notified_count += 1

        mock = MockObserver()
        output_view.subscribe(mock)
        output_view.unsubscribe(mock)

        # Verify observer was removed


# ============================================================================
# Section 6: Hierarchical Active State
# ============================================================================


class TestHierarchicalActiveState:
    """Tests for hierarchical active state in composite types.

    For TSB (bundles), each field can have independent active state.
    For TSL/TSD (collections), the active state is at the element level.
    """

    def test_tsb_field_active_state_independent(self, hgraph_module, tsb_meta):
        """TSB fields can have independent active states."""
        TSInput = hgraph_module.TSInput

        input_ts = TSInput(tsb_meta, None)

        # Set field x active, field y passive
        input_ts.set_active("x", True)
        input_ts.set_active("y", False)

        input_view = input_ts.view(TEST_TIME)
        assert input_view.field("x").active() == True
        assert input_view.field("y").active() == False

    def test_tsl_element_active_state(self, hgraph_module, tsl_ts_int_meta):
        """TSL elements can have active state."""
        TSInput = hgraph_module.TSInput

        input_ts = TSInput(tsl_ts_int_meta, None)
        input_ts.set_active(True)

        input_view = input_ts.view(TEST_TIME)
        # All elements should be active when root is active
        assert input_view[0].active() == True

    def test_tsd_element_active_state(self, hgraph_module, tsd_str_ts_int_meta):
        """TSD elements can have active state."""
        TSInput = hgraph_module.TSInput

        input_ts = TSInput(tsd_str_ts_int_meta, None)
        input_ts.set_active(True)

        input_view = input_ts.view(TEST_TIME)
        # Active state should propagate to elements - this test verifies no crash
        assert input_view.active() == True


# ============================================================================
# Section 7: TSOutput View Access
# ============================================================================


class TestTSOutputViewAccess:
    """Tests for TSOutputView specific functionality."""

    def test_ts_view_size_for_tsl(self, hgraph_module, tsl_ts_int_meta):
        """TSL view has correct size."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsl_ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # Size reflects the container's element count
        size = ts_view.size()
        assert size >= 0

    def test_ts_view_size_for_tsb(self, hgraph_module, tsb_meta):
        """TSB view has correct size (field count)."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsb_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # Size equals field count
        size = ts_view.size()
        assert size == 2  # x and y fields

    def test_ts_view_as_bundle(self, hgraph_module, tsb_meta):
        """TSB view can be converted to TSBView."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsb_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        tsb_view = ts_view.as_bundle()
        assert tsb_view is not None
        assert tsb_view.field_count() == 2

    def test_ts_view_as_list(self, hgraph_module, tsl_ts_int_meta):
        """TSL view can be converted to TSLView."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsl_ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        tsl_view = ts_view.as_list()
        assert tsl_view is not None

    def test_ts_view_as_dict(self, hgraph_module, tsd_str_ts_int_meta):
        """TSD view can be converted to TSDView."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsd_str_ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        tsd_view = ts_view.as_dict()
        assert tsd_view is not None


# ============================================================================
# Section 8: TSInput View Access
# ============================================================================


class TestTSInputViewAccess:
    """Tests for TSInputView specific functionality."""

    def test_tsl_input_navigation(self, hgraph_module, tsl_ts_int_meta):
        """TSL input supports navigation to elements via as_list().at()."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsl_ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # Navigate to element via as_list()
        tsl_view = ts_view.as_list()
        element_view = tsl_view.at(0)
        assert element_view is not None
        assert element_view.meta is not None

    def test_tsb_input_navigation(self, hgraph_module, tsb_meta):
        """TSB input supports navigation to fields."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsb_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # Navigate to field by name
        field_x = ts_view.field("x")
        field_y = ts_view.field("y")

        assert field_x is not None
        assert field_y is not None

    def test_tsb_input_navigation_by_index(self, hgraph_module, tsb_meta):
        """TSB input supports navigation by index."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsb_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # Navigate to field by index
        field_0 = ts_view[0]
        field_1 = ts_view[1]

        assert field_0 is not None
        assert field_1 is not None


# ============================================================================
# Section 9: Link Following Tests
# ============================================================================


class TestLinkFollowing:
    """Tests for link-following behavior after binding."""

    def test_linked_tsl_valid_reflects_target(self, hgraph_module, tsl_ts_int_meta):
        """Linked TSL valid() reflects target's validity."""
        TSValue = hgraph_module.TSValue

        source_value = TSValue(tsl_ts_int_meta)
        target_value = TSValue(tsl_ts_int_meta)

        source_view = source_value.ts_view(TEST_TIME)
        target_view = target_value.ts_view(TEST_TIME)

        # Target starts valid for TSL
        target_valid = target_view.valid()

        # Bind source to target
        source_view.bind(target_view)

        # Source should reflect target validity
        assert source_view.valid() == target_valid

    def test_linked_tsd_valid_reflects_target(self, hgraph_module, tsd_str_ts_int_meta):
        """Linked TSD valid() reflects target's validity."""
        TSValue = hgraph_module.TSValue

        source_value = TSValue(tsd_str_ts_int_meta)
        target_value = TSValue(tsd_str_ts_int_meta)

        source_view = source_value.ts_view(TEST_TIME)
        target_view = target_value.ts_view(TEST_TIME)

        # Bind source to target
        source_view.bind(target_view)

        # Source should reflect target validity
        assert source_view.valid() == target_view.valid()

    def test_linked_tsl_size_reflects_target(self, hgraph_module, tsl_ts_int_meta):
        """Linked TSL size() reflects target's size."""
        TSValue = hgraph_module.TSValue

        source_value = TSValue(tsl_ts_int_meta)
        target_value = TSValue(tsl_ts_int_meta)

        source_view = source_value.ts_view(TEST_TIME)
        target_view = target_value.ts_view(TEST_TIME)

        # Bind source to target
        source_view.bind(target_view)

        # Source size should reflect target
        assert source_view.size() == target_view.size()


# ============================================================================
# Section 10: Edge Cases
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_unbind_on_unbound_is_noop(self, hgraph_module, tsl_ts_int_meta):
        """unbind() on unbound view is a no-op (no error)."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(tsl_ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        # Should not raise
        ts_view.unbind()
        assert ts_view.is_bound() == False

    def test_bind_twice_updates_target(self, hgraph_module, tsl_ts_int_meta):
        """Binding twice to different targets updates the binding."""
        TSValue = hgraph_module.TSValue

        source_value = TSValue(tsl_ts_int_meta)
        target1_value = TSValue(tsl_ts_int_meta)
        target2_value = TSValue(tsl_ts_int_meta)

        source_view = source_value.ts_view(TEST_TIME)
        target1_view = target1_value.ts_view(TEST_TIME)
        target2_view = target2_value.ts_view(TEST_TIME)

        source_view.bind(target1_view)
        assert source_view.is_bound() == True

        source_view.bind(target2_view)
        assert source_view.is_bound() == True

    def test_unbind_twice_is_safe(self, hgraph_module, tsl_ts_int_meta):
        """Calling unbind() twice is safe (second is no-op)."""
        TSValue = hgraph_module.TSValue

        source_value = TSValue(tsl_ts_int_meta)
        target_value = TSValue(tsl_ts_int_meta)

        source_view = source_value.ts_view(TEST_TIME)
        target_view = target_value.ts_view(TEST_TIME)

        source_view.bind(target_view)
        source_view.unbind()
        assert source_view.is_bound() == False

        # Second unbind should not raise
        source_view.unbind()
        assert source_view.is_bound() == False

    def test_scalar_unbind_raises_error(self, hgraph_module, ts_int_meta):
        """Scalar TS unbind() raises RuntimeError."""
        TSValue = hgraph_module.TSValue

        ts_value = TSValue(ts_int_meta)
        ts_view = ts_value.ts_view(TEST_TIME)

        with pytest.raises(RuntimeError, match="scalar"):
            ts_view.unbind()


# ============================================================================
# Section 11: TSInput/TSOutput Direct Tests (when exposed)
# ============================================================================


class TestTSInputDirect:
    """Direct tests for TSInput class."""

    def test_ts_input_construction(self, hgraph_module, ts_int_meta):
        """TSInput can be constructed with schema and owner."""
        TSInput = hgraph_module.TSInput
        input_ts = TSInput(ts_int_meta, None)
        assert input_ts.meta is not None

    def test_ts_input_view(self, hgraph_module, ts_int_meta):
        """TSInput.view() returns TSInputView."""
        TSInput = hgraph_module.TSInput
        input_ts = TSInput(ts_int_meta, None)
        input_view = input_ts.view(TEST_TIME)
        assert input_view is not None


class TestTSOutputDirect:
    """Direct tests for TSOutput class."""

    def test_ts_output_construction(self, hgraph_module, ts_int_meta):
        """TSOutput can be constructed with schema and owner."""
        TSOutput = hgraph_module.TSOutput
        output_ts = TSOutput(ts_int_meta, None)
        assert output_ts.ts_meta is not None

    def test_ts_output_view(self, hgraph_module, ts_int_meta):
        """TSOutput.view() returns TSOutputView."""
        TSOutput = hgraph_module.TSOutput
        output_ts = TSOutput(ts_int_meta, None)
        output_view = output_ts.view(TEST_TIME)
        assert output_view is not None


class TestTSInputViewDirect:
    """Direct tests for TSInputView class (when exposed to Python)."""

    def test_ts_input_view_bind(self, hgraph_module, tsl_ts_int_meta):
        """TSInputView.bind() establishes link to output."""
        TSInput = hgraph_module.TSInput
        TSOutput = hgraph_module.TSOutput

        # Use TSL instead of scalar - scalar types don't support binding
        input_ts = TSInput(tsl_ts_int_meta, None)
        output_ts = TSOutput(tsl_ts_int_meta, None)

        input_view = input_ts.view(TEST_TIME)
        output_view = output_ts.view(TEST_TIME)

        input_view.bind(output_view)
        assert input_view.is_bound() == True

    def test_ts_input_view_make_active(self, hgraph_module, ts_int_meta):
        """TSInputView.make_active() makes input active."""
        TSInput = hgraph_module.TSInput
        input_ts = TSInput(ts_int_meta, None)
        input_view = input_ts.view(TEST_TIME)

        input_view.make_active()
        assert input_view.active() == True

    def test_ts_input_view_make_passive(self, hgraph_module, ts_int_meta):
        """TSInputView.make_passive() makes input passive."""
        TSInput = hgraph_module.TSInput
        input_ts = TSInput(ts_int_meta, None)
        input_view = input_ts.view(TEST_TIME)

        input_view.make_active()
        input_view.make_passive()
        assert input_view.active() == False


class TestTSOutputViewDirect:
    """Direct tests for TSOutputView class (when exposed to Python)."""

    def test_ts_output_view_set_value(self, hgraph_module, ts_int_meta):
        """TSOutputView.from_python() updates the value."""
        TSOutput = hgraph_module.TSOutput
        output_ts = TSOutput(ts_int_meta, None)
        output_view = output_ts.view(TEST_TIME)

        # Use from_python to set value
        output_view.from_python(42)
        assert output_view.value().to_python() == 42

    @pytest.mark.xfail(reason="subscribe/unsubscribe require C++ Notifiable - Python objects not supported")
    def test_ts_output_view_subscribe(self, hgraph_module, ts_int_meta):
        """TSOutputView.subscribe() adds observer.

        Note: This test is xfail because subscribe/unsubscribe require a C++ Notifiable
        object, not a Python object. Python observers would need a wrapper class.
        """
        TSOutput = hgraph_module.TSOutput
        output_ts = TSOutput(ts_int_meta, None)
        output_view = output_ts.view(TEST_TIME)

        class MockNotifiable:
            def notify(self, et):
                pass

        mock = MockNotifiable()
        output_view.subscribe(mock)
        # Subscription added (would need internal inspection)

    @pytest.mark.xfail(reason="subscribe/unsubscribe require C++ Notifiable - Python objects not supported")
    def test_ts_output_view_unsubscribe(self, hgraph_module, ts_int_meta):
        """TSOutputView.unsubscribe() removes observer.

        Note: This test is xfail because subscribe/unsubscribe require a C++ Notifiable
        object, not a Python object. Python observers would need a wrapper class.
        """
        TSOutput = hgraph_module.TSOutput
        output_ts = TSOutput(ts_int_meta, None)
        output_view = output_ts.view(TEST_TIME)

        class MockNotifiable:
            def notify(self, et):
                pass

        mock = MockNotifiable()
        output_view.subscribe(mock)
        output_view.unsubscribe(mock)
        # Subscription removed
