"""
Test TSView Link Functionality

Tests for the link (binding) support in TSValue/TSView, enabling binding from
one position in a TSValue to another TSValue. Links are internal storage
mechanisms (like filesystem symlinks) that redirect navigation to a target
location transparently.

Test Categories:
1. Link schema generation tests
2. TSValue link storage tests
3. TSView binding API tests (bind/unbind/is_bound)
4. Edge case tests
"""

from datetime import datetime

import pytest

from hgraph._feature_switch import is_feature_enabled

# A fixed time for testing
TEST_TIME = datetime(2024, 1, 1, 0, 0, 0)


# ============================================================================
# Fixtures and Helpers
# ============================================================================


@pytest.fixture
def hgraph_module():
    """Get the C++ hgraph module, skip if not available."""
    try:
        if not is_feature_enabled("use_cpp"):
            pytest.skip("C++ not enabled")
        import hgraph._hgraph as _hgraph
        _ = _hgraph.TSTypeRegistry
        return _hgraph
    except (ImportError, AttributeError):
        pytest.skip("C++ module or TSTypeRegistry not available")


@pytest.fixture
def ts_type_registry(hgraph_module):
    """Get the TSTypeRegistry instance."""
    return hgraph_module.TSTypeRegistry.instance()


@pytest.fixture
def value_module(hgraph_module):
    """Get the value submodule."""
    return hgraph_module.value


@pytest.fixture
def tsl_ts_int_meta(hgraph_module):
    """Create TSMeta for TSL[TS[int], Size[3]]."""
    from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
    from hgraph._types._type_meta_data import HgTypeMetaData

    element_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
    tsl_meta = HgTSLTypeMetaData(element_ts, HgTypeMetaData.parse_type(int))
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
def ts_int_meta(hgraph_module):
    """Create TSMeta for TS[int]."""
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    ts_meta = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
    return ts_meta.cpp_type


@pytest.fixture
def tss_int_meta(hgraph_module):
    """Create TSMeta for TSS[int]."""
    from hgraph._types._tss_meta_data import HgTSSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    tss_meta = HgTSSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
    return tss_meta.cpp_type


# ============================================================================
# Link Schema Generation Tests
# ============================================================================


def test_tsl_cpp_type_exists(tsl_ts_int_meta):
    """TSL should have a valid cpp_type."""
    assert tsl_ts_int_meta is not None


def test_tsd_cpp_type_exists(tsd_str_ts_int_meta):
    """TSD should have a valid cpp_type."""
    assert tsd_str_ts_int_meta is not None


def test_scalar_ts_cpp_type_exists(ts_int_meta):
    """Scalar TS[int] should have a valid cpp_type."""
    assert ts_int_meta is not None


# ============================================================================
# TSValue Link Storage Tests
# ============================================================================


def test_tsvalue_for_tsl_has_link_view(hgraph_module, tsl_ts_int_meta):
    """TSValue for TSL should have link_view() accessible."""
    TSValue = hgraph_module.TSValue

    ts_value = TSValue(tsl_ts_int_meta)
    link_view = ts_value.link_view()

    # For TSL, link schema is bool, so link_view should be valid
    assert link_view.valid()


def test_tsvalue_for_tsd_has_link_view(hgraph_module, tsd_str_ts_int_meta):
    """TSValue for TSD should have link_view() accessible."""
    TSValue = hgraph_module.TSValue

    ts_value = TSValue(tsd_str_ts_int_meta)
    link_view = ts_value.link_view()

    # For TSD, link schema is bool, so link_view should be valid
    assert link_view.valid()


def test_tsvalue_for_scalar_ts_has_link_view(hgraph_module, ts_int_meta):
    """TSValue for scalar TS[int] has valid link_view (REFLink storage for alternatives)."""
    TSValue = hgraph_module.TSValue

    ts_value = TSValue(ts_int_meta)
    link_view = ts_value.link_view()

    # Scalar types have REFLink storage to support REF→TS alternative conversion
    assert link_view.valid()


# ============================================================================
# TSView Binding API Tests - TSL
# ============================================================================


def test_tsl_is_bound_returns_false_by_default(hgraph_module, tsl_ts_int_meta):
    """TSL is_bound() should return False by default (not bound)."""
    TSValue = hgraph_module.TSValue

    ts_value = TSValue(tsl_ts_int_meta)
    ts_view = ts_value.ts_view(TEST_TIME)  # current_time=0

    assert ts_view.is_bound() is False


def test_tsl_unbind_on_unbound_is_noop(hgraph_module, tsl_ts_int_meta):
    """TSL unbind() on unbound position should be a no-op (no exception)."""
    TSValue = hgraph_module.TSValue

    ts_value = TSValue(tsl_ts_int_meta)
    ts_view = ts_value.ts_view(TEST_TIME)

    # Should not raise
    ts_view.unbind()

    # Still not bound
    assert ts_view.is_bound() is False


def test_tsl_bind_sets_is_bound_true(hgraph_module, tsl_ts_int_meta):
    """TSL bind() should set is_bound() to True."""
    TSValue = hgraph_module.TSValue

    # Create source and target TSValues
    source_value = TSValue(tsl_ts_int_meta)
    target_value = TSValue(tsl_ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    # Bind source to target
    source_view.bind(target_view)

    assert source_view.is_bound() is True


def test_tsl_unbind_after_bind_clears_is_bound(hgraph_module, tsl_ts_int_meta):
    """TSL unbind() after bind() should set is_bound() back to False."""
    TSValue = hgraph_module.TSValue

    source_value = TSValue(tsl_ts_int_meta)
    target_value = TSValue(tsl_ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    # Bind then unbind
    source_view.bind(target_view)
    assert source_view.is_bound() is True

    source_view.unbind()
    assert source_view.is_bound() is False


# ============================================================================
# TSView Binding API Tests - TSD
# ============================================================================


def test_tsd_is_bound_returns_false_by_default(hgraph_module, tsd_str_ts_int_meta):
    """TSD is_bound() should return False by default."""
    TSValue = hgraph_module.TSValue

    ts_value = TSValue(tsd_str_ts_int_meta)
    ts_view = ts_value.ts_view(TEST_TIME)

    assert ts_view.is_bound() is False


def test_tsd_unbind_on_unbound_is_noop(hgraph_module, tsd_str_ts_int_meta):
    """TSD unbind() on unbound position should be a no-op."""
    TSValue = hgraph_module.TSValue

    ts_value = TSValue(tsd_str_ts_int_meta)
    ts_view = ts_value.ts_view(TEST_TIME)

    # Should not raise
    ts_view.unbind()

    assert ts_view.is_bound() is False


def test_tsd_bind_sets_is_bound_true(hgraph_module, tsd_str_ts_int_meta):
    """TSD bind() should set is_bound() to True."""
    TSValue = hgraph_module.TSValue

    source_value = TSValue(tsd_str_ts_int_meta)
    target_value = TSValue(tsd_str_ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    source_view.bind(target_view)

    assert source_view.is_bound() is True


def test_tsd_unbind_after_bind_clears_is_bound(hgraph_module, tsd_str_ts_int_meta):
    """TSD unbind() after bind() should set is_bound() back to False."""
    TSValue = hgraph_module.TSValue

    source_value = TSValue(tsd_str_ts_int_meta)
    target_value = TSValue(tsd_str_ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    source_view.bind(target_view)
    assert source_view.is_bound() is True

    source_view.unbind()
    assert source_view.is_bound() is False


# ============================================================================
# Edge Cases - Error Conditions
# ============================================================================


def test_scalar_ts_bind_raises_error(hgraph_module, ts_int_meta):
    """Scalar TS bind() should raise RuntimeError."""
    TSValue = hgraph_module.TSValue

    source_value = TSValue(ts_int_meta)
    target_value = TSValue(ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    with pytest.raises(RuntimeError, match="scalar"):
        source_view.bind(target_view)


def test_scalar_ts_unbind_raises_error(hgraph_module, ts_int_meta):
    """Scalar TS unbind() should raise RuntimeError."""
    TSValue = hgraph_module.TSValue

    ts_value = TSValue(ts_int_meta)
    ts_view = ts_value.ts_view(TEST_TIME)

    with pytest.raises(RuntimeError, match="scalar"):
        ts_view.unbind()


def test_scalar_ts_is_bound_returns_false(hgraph_module, ts_int_meta):
    """Scalar TS is_bound() should return False (not bound at scalar level)."""
    TSValue = hgraph_module.TSValue

    ts_value = TSValue(ts_int_meta)
    ts_view = ts_value.ts_view(TEST_TIME)

    # Should not raise, returns False
    assert ts_view.is_bound() is False


@pytest.mark.skip(reason="TSS TSValue construction crashes - pre-existing issue unrelated to link support")
def test_tss_bind_raises_error(hgraph_module, tss_int_meta):
    """TSS bind() should raise RuntimeError (not supported)."""
    TSValue = hgraph_module.TSValue

    source_value = TSValue(tss_int_meta)
    target_value = TSValue(tss_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    with pytest.raises(RuntimeError, match="TSS"):
        source_view.bind(target_view)


@pytest.mark.skip(reason="TSS TSValue construction crashes - pre-existing issue unrelated to link support")
def test_tss_unbind_raises_error(hgraph_module, tss_int_meta):
    """TSS unbind() should raise RuntimeError (not supported)."""
    TSValue = hgraph_module.TSValue

    ts_value = TSValue(tss_int_meta)
    ts_view = ts_value.ts_view(TEST_TIME)

    with pytest.raises(RuntimeError, match="TSS"):
        ts_view.unbind()


@pytest.mark.skip(reason="TSS TSValue construction crashes - pre-existing issue unrelated to link support")
def test_tss_is_bound_returns_false(hgraph_module, tss_int_meta):
    """TSS is_bound() should return False."""
    TSValue = hgraph_module.TSValue

    ts_value = TSValue(tss_int_meta)
    ts_view = ts_value.ts_view(TEST_TIME)

    assert ts_view.is_bound() is False


# ============================================================================
# Edge Cases - Multiple Binds
# ============================================================================


def test_tsl_bind_twice_updates_bound_state(hgraph_module, tsl_ts_int_meta):
    """Binding TSL twice should keep is_bound() as True."""
    TSValue = hgraph_module.TSValue

    source_value = TSValue(tsl_ts_int_meta)
    target1_value = TSValue(tsl_ts_int_meta)
    target2_value = TSValue(tsl_ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target1_view = target1_value.ts_view(TEST_TIME)
    target2_view = target2_value.ts_view(TEST_TIME)

    source_view.bind(target1_view)
    assert source_view.is_bound() is True

    # Bind again to different target
    source_view.bind(target2_view)
    assert source_view.is_bound() is True


def test_tsl_unbind_twice_is_safe(hgraph_module, tsl_ts_int_meta):
    """Calling unbind() twice on TSL should be safe (second is no-op)."""
    TSValue = hgraph_module.TSValue

    source_value = TSValue(tsl_ts_int_meta)
    target_value = TSValue(tsl_ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    source_view.bind(target_view)

    source_view.unbind()
    assert source_view.is_bound() is False

    # Second unbind should not raise
    source_view.unbind()
    assert source_view.is_bound() is False


# ============================================================================
# Link View Direct Access Tests
# ============================================================================


def test_tsl_link_view_is_link_target(hgraph_module, tsl_ts_int_meta):
    """TSL link_view should contain a REFLink value (tuple with is_bound, is_linked flags)."""
    TSValue = hgraph_module.TSValue

    ts_value = TSValue(tsl_ts_int_meta)
    link_view = ts_value.link_view()

    assert link_view.valid()
    # REFLink's to_python returns a tuple (is_bound, is_linked)
    link_val = link_view.to_python()
    assert isinstance(link_val, tuple)
    assert link_val == (False, False)  # Not bound or linked by default


def test_tsd_link_view_is_link_target(hgraph_module, tsd_str_ts_int_meta):
    """TSD link_view should contain a REFLink value."""
    TSValue = hgraph_module.TSValue

    ts_value = TSValue(tsd_str_ts_int_meta)
    link_view = ts_value.link_view()

    assert link_view.valid()
    link_val = link_view.to_python()
    assert isinstance(link_val, tuple)
    assert link_val == (False, False)  # Not bound or linked by default


def test_tsl_bind_changes_link_view_value(hgraph_module, tsl_ts_int_meta):
    """After binding TSL, the link_view should reflect the bound state."""
    TSValue = hgraph_module.TSValue

    source_value = TSValue(tsl_ts_int_meta)
    target_value = TSValue(tsl_ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    # Before bind
    link_view_before = source_value.link_view()
    assert link_view_before.to_python() == (False, False)

    # After bind - is_linked becomes True (is_bound stays False for simple binding)
    source_view.bind(target_view)
    link_view_after = source_value.link_view()
    assert link_view_after.to_python() == (False, True)


def test_tsl_unbind_changes_link_view_value(hgraph_module, tsl_ts_int_meta):
    """After unbinding TSL, the link_view should reflect the unbound state."""
    TSValue = hgraph_module.TSValue

    source_value = TSValue(tsl_ts_int_meta)
    target_value = TSValue(tsl_ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    source_view.bind(target_view)
    assert source_value.link_view().to_python() == (False, True)

    source_view.unbind()
    assert source_value.link_view().to_python() == (False, False)


# ============================================================================
# Link Following Tests - TSL
# ============================================================================
# These tests verify that navigation and semantics correctly follow links.


def test_tsl_linked_view_valid_reflects_target_state(hgraph_module, tsl_ts_int_meta):
    """valid() on a linked TSL should reflect the target's validity.

    When source is bound to target:
    - If target is valid, source.valid() should return True
    - Even though source's local data was never set
    """
    TSValue = hgraph_module.TSValue

    source_value = TSValue(tsl_ts_int_meta)
    target_value = TSValue(tsl_ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    # Target is valid (TSL initializes with valid container time)
    assert target_view.valid() is True

    # Bind source to target
    source_view.bind(target_view)

    # After binding, source.valid() should reflect target's validity
    assert source_view.valid() is True, "Linked view should reflect target's valid state"


def test_tsl_linked_view_modified_reflects_target_state(hgraph_module, tsl_ts_int_meta):
    """modified() on a linked TSL should reflect the target's modification state."""
    TSValue = hgraph_module.TSValue

    source_value = TSValue(tsl_ts_int_meta)
    target_value = TSValue(tsl_ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    # Bind source to target
    source_view.bind(target_view)

    # After binding, source.modified() should reflect target's state
    # Both should have same modification state since linked
    assert source_view.modified() == target_view.modified(), \
        "Linked view should reflect target's modified state"


def test_tsl_linked_view_last_modified_time_reflects_target(hgraph_module, tsl_ts_int_meta):
    """last_modified_time() on a linked TSL should return target's time."""
    TSValue = hgraph_module.TSValue

    source_value = TSValue(tsl_ts_int_meta)
    target_value = TSValue(tsl_ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    target_lmt = target_view.last_modified_time()

    # Bind source to target
    source_view.bind(target_view)

    # After binding, source should report target's last_modified_time
    assert source_view.last_modified_time() == target_lmt, \
        "Linked view should return target's last_modified_time"


def test_tsl_linked_size_reflects_target(hgraph_module, tsl_ts_int_meta):
    """size() on a linked TSL should return target's size."""
    TSValue = hgraph_module.TSValue

    source_value = TSValue(tsl_ts_int_meta)
    target_value = TSValue(tsl_ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    # Bind source to target
    source_view.bind(target_view)

    # Size should match target's size
    assert source_view.size() == target_view.size(), \
        "Linked view should return target's size"


# ============================================================================
# Link Following Tests - TSD
# ============================================================================


def test_tsd_linked_view_valid_reflects_target_state(hgraph_module, tsd_str_ts_int_meta):
    """valid() on a linked TSD should reflect the target's validity."""
    TSValue = hgraph_module.TSValue

    source_value = TSValue(tsd_str_ts_int_meta)
    target_value = TSValue(tsd_str_ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    # Bind source to target
    source_view.bind(target_view)

    # After binding, source.valid() should reflect target's validity
    assert source_view.valid() == target_view.valid(), \
        "Linked view should reflect target's valid state"


def test_tsd_linked_size_reflects_target(hgraph_module, tsd_str_ts_int_meta):
    """size() on a linked TSD should return target's size."""
    TSValue = hgraph_module.TSValue

    source_value = TSValue(tsd_str_ts_int_meta)
    target_value = TSValue(tsd_str_ts_int_meta)

    source_view = source_value.ts_view(TEST_TIME)
    target_view = target_value.ts_view(TEST_TIME)

    # Bind source to target
    source_view.bind(target_view)

    # Size should match target's size
    assert source_view.size() == target_view.size(), \
        "Linked view should return target's size"
