"""
Tests for Phase 2: View owner and path tracking, ViewRange/ViewPairRange.

These tests verify:
- View has owner() and path() methods
- Path accumulates through nested access
- ViewRange provides iteration over elements
- ViewPairRange provides iteration over key-value pairs
"""

import pytest

# Skip all tests if C++ module not available
_hgraph = pytest.importorskip("hgraph._hgraph")


def _skip_if_no_cpp():
    """Helper to skip tests when C++ runtime is disabled."""
    from hgraph import _features
    if not _features.USE_CPP_RUNTIME:
        pytest.skip("C++ runtime not enabled")


# =============================================================================
# View owner tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_view_has_owner_property():
    """View should have an owner() method returning the owning Value."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # Create a value
    v = value.Value.from_python({"x": 1, "y": 2})
    view = v.const_view()

    assert hasattr(view, 'owner')
    # Owner should reference the original Value
    assert view.owner is not None


@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_root_view_owner_is_self():
    """Root view's owner should reference the Value it came from."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python([1, 2, 3])
    view = v.const_view()

    # Owner of root view is the Value itself
    assert view.owner is v


@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_nested_view_preserves_owner():
    """Nested views should preserve the same owner."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python({"data": {"nested": 42}})
    root_view = v.const_view()
    nested_view = root_view.at("data").at("nested")

    # Both views should have the same owner
    assert nested_view.owner is v
    assert nested_view.owner is root_view.owner


# =============================================================================
# View path tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_view_has_path_property():
    """View should have a path() method returning the navigation path."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python({"x": 1})
    view = v.const_view()

    assert hasattr(view, 'path')
    # Path should be list-like or have a length
    path = view.path
    assert hasattr(path, '__len__') or hasattr(path, 'size')


@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_root_view_has_empty_path():
    """Root view should have an empty path."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python({"x": 1})
    view = v.const_view()

    path = view.path
    assert len(path) == 0 or path.empty()


@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_bundle_field_access_adds_to_path():
    """Accessing a bundle field should add field name to path."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python({"x": 1, "y": 2})
    root_view = v.const_view()
    field_view = root_view.at("x")

    # Path should contain the field name
    path = field_view.path
    assert len(path) == 1
    # Check path element is "x"
    assert path[0] == "x" or str(path[0]) == "x"


@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_list_index_access_adds_to_path():
    """Accessing a list index should add index to path."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python([10, 20, 30])
    root_view = v.const_view()
    element_view = root_view.at(1)

    # Path should contain the index
    path = element_view.path
    assert len(path) == 1
    # Check path element is index 1
    assert path[0] == 1 or int(path[0]) == 1


@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_nested_access_accumulates_path():
    """Nested access should accumulate path elements."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python({
        "users": [
            {"name": "Alice"},
            {"name": "Bob"},
        ]
    })
    root_view = v.const_view()
    deep_view = root_view.at("users").at(0).at("name")

    path = deep_view.path
    assert len(path) == 3
    # Path should be ["users", 0, "name"]


@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_view_path_string():
    """View should have path_string property for human-readable representation."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python({"a": {"b": 1}})
    view = v.const_view().at("a").at("b")

    assert hasattr(view, 'path_string')
    path_str = view.path_string
    # Should be something like "a.b" or "['a']['b']"
    assert "a" in path_str and "b" in path_str


# =============================================================================
# ViewRange tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_viewrange_is_iterable():
    """ViewRange should be iterable."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # Create a list value to get a ViewRange
    v = value.Value.from_python([1, 2, 3])
    view = v.const_view()

    # Get elements as ViewRange (exact API may vary)
    elements = view.elements()  # or similar method

    # Should be iterable
    count = 0
    for elem in elements:
        count += 1
    assert count == 3


@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_viewrange_has_size():
    """ViewRange should have size() method."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python([1, 2, 3, 4, 5])
    view = v.const_view()
    elements = view.elements()

    assert hasattr(elements, 'size') or hasattr(elements, '__len__')
    assert len(elements) == 5 or elements.size() == 5


@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_viewrange_has_empty():
    """ViewRange should have empty() method."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python([])
    view = v.const_view()
    elements = view.elements()

    assert hasattr(elements, 'empty')
    assert elements.empty()


@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_viewrange_indexed_access():
    """ViewRange should support indexed access."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python([10, 20, 30])
    view = v.const_view()
    elements = view.elements()

    # Should support [] operator
    assert elements[0].to_python() == 10
    assert elements[1].to_python() == 20
    assert elements[2].to_python() == 30


# =============================================================================
# ViewPairRange tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_viewpairrange_is_iterable():
    """ViewPairRange should be iterable yielding (key, value) pairs."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # Create a map value
    v = value.Value.from_python({"a": 1, "b": 2})
    view = v.const_view()

    # Get items as ViewPairRange
    items = view.items()  # or similar method

    # Should yield pairs
    for key, val in items:
        assert key is not None
        assert val is not None


@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_viewpairrange_has_size():
    """ViewPairRange should have size() method."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python({"x": 1, "y": 2, "z": 3})
    view = v.const_view()
    items = view.items()

    assert len(items) == 3 or items.size() == 3


@pytest.mark.skip(reason="Awaiting Phase 2 implementation")
def test_viewpairrange_has_empty():
    """ViewPairRange should have empty() method."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python({})
    view = v.const_view()
    items = view.items()

    assert items.empty()
