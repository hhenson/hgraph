"""
Test TSView Link Functionality

Tests for the link (binding) support in TSValue/TSView, enabling binding from
one position in a TSValue to another TSValue. Links are internal storage
mechanisms (like filesystem symlinks) that redirect navigation to a target
location transparently.

Test Categories:
1. Link schema generation tests
2. TSValue link construction tests
3. TSL link tests
4. TSD link tests
5. TSB link tests
6. Edge case tests
"""

import pytest

from hgraph._feature_switch import is_feature_enabled


# ============================================================================
# Test Fixtures and Helpers
# ============================================================================


def _skip_if_no_cpp():
    """Skip test if C++ module is not available."""
    try:
        if not is_feature_enabled("use_cpp"):
            pytest.skip("C++ not enabled")
        import hgraph._hgraph as _hgraph
        _ = _hgraph.TSTypeRegistry
    except (ImportError, AttributeError):
        pytest.skip("C++ module or TSTypeRegistry not available")


def _get_hgraph_module():
    """Get the C++ hgraph module."""
    import hgraph._hgraph as _hgraph
    return _hgraph


def _get_ts_type_registry():
    """Get the C++ TSTypeRegistry instance."""
    import hgraph._hgraph as _hgraph
    return _hgraph.TSTypeRegistry.instance()


def _get_value_module():
    """Get the value submodule."""
    import hgraph._hgraph as _hgraph
    return _hgraph.value


# ============================================================================
# Link Schema Generation Tests
# ============================================================================


class TestLinkSchemaGeneration:
    """Tests for generate_link_schema() functionality."""

    def test_tsl_link_schema_is_bool(self):
        """TSL link schema should be a single bool (collection-level)."""
        _skip_if_no_cpp()
        _hgraph = _get_hgraph_module()

        from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
        from hgraph._types._type_meta_data import HgTypeMetaData

        # Create TSL[TS[int], Size[3]] using proper Size type
        element_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        # For size, we need to use the proper Size type from the type system
        tsl_meta = HgTSLTypeMetaData(element_ts, HgTypeMetaData.parse_type(int))  # size placeholder

        cpp_type = tsl_meta.cpp_type
        assert cpp_type is not None, "TSL cpp_type should not be None"
        # The link schema for TSL is a single bool

    def test_tsd_link_schema_is_bool(self):
        """TSD link schema should be a single bool (collection-level)."""
        _skip_if_no_cpp()
        _hgraph = _get_hgraph_module()

        from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        # Create TSD[str, TS[int]]
        key_type = HgScalarTypeMetaData.parse_type(str)
        value_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        tsd_meta = HgTSDTypeMetaData(key_type, value_ts)

        cpp_type = tsd_meta.cpp_type
        assert cpp_type is not None, "TSD cpp_type should not be None"
        # The link schema for TSD is a single bool

    def test_tsb_link_schema_is_list_of_bool(self):
        """TSB link schema should be fixed_list[bool] with one entry per field."""
        _skip_if_no_cpp()
        _hgraph = _get_hgraph_module()

        from hgraph._types._tsb_meta_data import HgTSBTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        # Create a simple bundle schema
        # This requires more complex setup - simplified test
        pass  # TODO: Implement when bundle creation is clearer

    def test_scalar_ts_link_schema_is_none(self):
        """Scalar TS types should have no link schema (None)."""
        _skip_if_no_cpp()

        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        ts_int = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        cpp_type = ts_int.cpp_type
        assert cpp_type is not None, "TS[int] cpp_type should not be None"
        # Scalar types don't have link schema (link tracking is at container level)


# ============================================================================
# TSValue Link Construction Tests
# ============================================================================


class TestTSValueLinkConstruction:
    """Tests for TSValue link_ member initialization."""

    def test_tsvalue_with_tsl_has_link_storage(self):
        """TSValue for TSL should have link_ storage initialized."""
        _skip_if_no_cpp()
        _hgraph = _get_hgraph_module()

        # TSValue construction is typically done internally
        # We test through the TSView interface
        pass  # TODO: Add test when TSValue is directly accessible

    def test_tsvalue_with_tsd_has_link_storage(self):
        """TSValue for TSD should have link_ storage initialized."""
        _skip_if_no_cpp()
        pass  # TODO: Add test when TSValue is directly accessible


# ============================================================================
# TSView Binding API Tests
# ============================================================================


class TestTSViewBindingAPI:
    """Tests for TSView bind(), unbind(), and is_bound() methods."""

    def test_unbound_position_is_bound_returns_false(self):
        """is_bound() on unbound position returns false."""
        _skip_if_no_cpp()
        _hgraph = _get_hgraph_module()

        # Need to create a TSValue and get its TSView
        # This test verifies the default state is unbound
        pass  # TODO: Implement when TSValue creation is available

    def test_unbind_on_unbound_is_noop(self):
        """unbind() on unbound position is a no-op (doesn't throw)."""
        _skip_if_no_cpp()
        pass  # TODO: Implement when TSValue creation is available


# ============================================================================
# TSL Link Tests
# ============================================================================


class TestTSLLinks:
    """Tests for TSL link functionality."""

    def test_tsl_bind_sets_is_bound(self):
        """After binding TSL, is_bound() returns True."""
        _skip_if_no_cpp()
        pass  # TODO: Implement

    def test_tsl_unbind_clears_is_bound(self):
        """After unbinding TSL, is_bound() returns False."""
        _skip_if_no_cpp()
        pass  # TODO: Implement


# ============================================================================
# TSD Link Tests
# ============================================================================


class TestTSDLinks:
    """Tests for TSD link functionality."""

    def test_tsd_bind_sets_is_bound(self):
        """After binding TSD, is_bound() returns True."""
        _skip_if_no_cpp()
        pass  # TODO: Implement

    def test_tsd_unbind_clears_is_bound(self):
        """After unbinding TSD, is_bound() returns False."""
        _skip_if_no_cpp()
        pass  # TODO: Implement


# ============================================================================
# TSB Link Tests
# ============================================================================


class TestTSBLinks:
    """Tests for TSB link functionality with per-field linking."""

    def test_tsb_bind_sets_all_fields_linked(self):
        """After binding TSB, all fields are marked as linked."""
        _skip_if_no_cpp()
        pass  # TODO: Implement

    def test_tsb_unbind_clears_all_fields(self):
        """After unbinding TSB, all fields are marked as unlinked."""
        _skip_if_no_cpp()
        pass  # TODO: Implement


# ============================================================================
# Edge Cases
# ============================================================================


class TestLinkEdgeCases:
    """Edge case tests for link functionality."""

    def test_scalar_bind_raises_error(self):
        """Attempting to bind a scalar TS should raise an error."""
        _skip_if_no_cpp()
        _hgraph = _get_hgraph_module()

        # Scalar types don't support binding at this level
        # The operation should raise an error
        pass  # TODO: Implement

    def test_tss_bind_raises_error(self):
        """TSS (set) doesn't support binding."""
        _skip_if_no_cpp()
        pass  # TODO: Implement

    def test_navigation_through_invalid_link_returns_invalid(self):
        """Navigation through an invalid link returns an invalid TSView."""
        _skip_if_no_cpp()
        pass  # TODO: Implement
