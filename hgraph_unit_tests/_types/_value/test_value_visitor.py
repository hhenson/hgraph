"""
Tests for Value visitor pattern - Python bindings.

The visitor pattern is primarily a C++ API (see visitor.h).
These tests verify the Python bindings work correctly for basic cases.

Reference: Value_USER_GUIDE.md Section 8 (Visiting Values)
"""

import pytest

# Try to import the C++ extension module
_hgraph = pytest.importorskip("hgraph._hgraph", reason="C++ extension not available")
value = _hgraph.value  # Value types are in the value submodule

# Import Value types from the C++ extension
try:
    Value = value.PlainValue
    ValueView = value.ValueView
    ConstValueView = value.ConstValueView
except AttributeError:
    pytest.skip("Value types not yet exposed in C++ extension", allow_module_level=True)


# =============================================================================
# Basic visit functionality
# =============================================================================


@pytest.mark.parametrize("input_val,expected", [
    (42, "value:42"),
    (3.14, "value:3.14"),
    ("hello", "value:hello"),
    (True, "value:True"),
])
def test_visit_scalar(input_val, expected):
    """Visit scalar values of different types."""
    v = Value(input_val)
    result = v.const_view().visit(lambda x: f"value:{x}")
    assert result == expected


def test_visit_void_side_effects():
    """Void visitor executes side effects."""
    results = []
    v = Value(42)
    v.const_view().visit_void(lambda x: results.append(x))
    assert results == [42]


# =============================================================================
# Pattern matching
# =============================================================================


@pytest.mark.parametrize("input_val,expected", [
    (42, "int:42"),
    (3.14, "float:3.14"),
    ("hello", "str:hello"),
])
def test_match_by_type(input_val, expected):
    """Pattern match dispatches to correct type handler."""
    v = Value(input_val)
    result = v.const_view().match(
        (int, lambda x: f"int:{x}"),
        (float, lambda x: f"float:{x}"),
        (str, lambda x: f"str:{x}"),
    )
    assert result == expected


def test_match_default_handler():
    """Pattern match uses default handler when no type matches."""
    v = Value(42)
    result = v.const_view().match(
        (str, lambda x: f"str:{x}"),
        (None, lambda x: "default"),
    )
    assert result == "default"


def test_match_no_handler_raises():
    """Pattern match raises error if no handler matches."""
    v = Value(42)
    with pytest.raises(RuntimeError, match="no handler matched"):
        v.const_view().match(
            (str, lambda x: f"str:{x}"),
            (float, lambda x: f"float:{x}"),
        )


# =============================================================================
# Mutable visitors
# =============================================================================


def test_visit_mut_modify_int():
    """Mutable visitor can modify int values."""
    v = Value(42)
    v.view().visit_mut(lambda x: x * 2)
    assert v.const_view().as_int() == 84


def test_visit_mut_modify_string():
    """Mutable visitor can modify string values."""
    v = Value("hello")
    v.view().visit_mut(lambda x: x.upper())
    assert v.const_view().as_string() == "HELLO"


def test_visit_mut_return_none_no_change():
    """Mutable visitor returning None doesn't change value."""
    v = Value(42)
    v.view().visit_mut(lambda x: None)
    assert v.const_view().as_int() == 42


# =============================================================================
# Return types
# =============================================================================


def test_visit_returns_string():
    """Visitor can return string."""
    v = Value(42)
    result = v.const_view().visit(lambda x: str(x))
    assert result == "42"


def test_visit_returns_int():
    """Visitor can return int."""
    v = Value(42)
    result = v.const_view().visit(lambda x: x * 2)
    assert result == 84


def test_visit_returns_none():
    """Visitor can return None."""
    v = Value(42)
    result = v.const_view().visit(lambda x: None)
    assert result is None
