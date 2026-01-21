"""
Tests for Phase 1: Type name lookup and Python type mapping.

These tests verify:
- TypeMeta has a `name` property
- TypeMeta.get("name") looks up types by name
- from_python_type() maps Python types to TypeMeta
- Built-in types are registered with canonical names
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
# TypeMeta.name property tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 1 implementation")
def test_typemeta_has_name_property():
    """TypeMeta should have a name property returning a string or None."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # Get a known TypeMeta
    int_meta = value.get_scalar_type_meta(int)

    # Should have name property
    assert hasattr(int_meta, 'name')
    # For registered types, name should be a string
    assert int_meta.name is None or isinstance(int_meta.name, str)


@pytest.mark.skip(reason="Awaiting Phase 1 implementation")
def test_typemeta_name_is_none_for_anonymous_types():
    """TypeMeta for types registered without names should have name=None."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # Create an anonymous compound type
    # (Exact API depends on implementation)
    # For now, just verify the concept
    pass


# =============================================================================
# TypeMeta.get(name) lookup tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 1 implementation")
@pytest.mark.parametrize("type_name,expected_kind", [
    ("int", "Atomic"),
    ("float", "Atomic"),
    ("str", "Atomic"),
    ("bool", "Atomic"),
])
def test_typemeta_get_builtin_types(type_name, expected_kind):
    """TypeMeta.get() should return correct TypeMeta for built-in types."""
    _skip_if_no_cpp()
    value = _hgraph.value

    meta = value.TypeMeta.get(type_name)

    assert meta is not None, f"TypeMeta.get('{type_name}') returned None"
    assert meta.name == type_name
    assert meta.kind.name == expected_kind


@pytest.mark.skip(reason="Awaiting Phase 1 implementation")
def test_typemeta_get_returns_none_for_unknown():
    """TypeMeta.get() should return None for unregistered type names."""
    _skip_if_no_cpp()
    value = _hgraph.value

    result = value.TypeMeta.get("unknown_type_that_does_not_exist")
    assert result is None


@pytest.mark.skip(reason="Awaiting Phase 1 implementation")
def test_typemeta_get_is_case_sensitive():
    """TypeMeta.get() should be case-sensitive."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # "int" should work
    assert value.TypeMeta.get("int") is not None
    # "Int" or "INT" should not
    assert value.TypeMeta.get("Int") is None
    assert value.TypeMeta.get("INT") is None


# =============================================================================
# from_python_type() tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 1 implementation")
@pytest.mark.parametrize("py_type", [int, float, str, bool])
def test_from_python_type_builtin(py_type):
    """from_python_type() should return TypeMeta for built-in Python types."""
    _skip_if_no_cpp()
    value = _hgraph.value

    meta = value.from_python_type(py_type)

    assert meta is not None, f"from_python_type({py_type}) returned None"
    assert meta.kind.name == "Atomic"


@pytest.mark.skip(reason="Awaiting Phase 1 implementation")
def test_from_python_type_returns_none_for_unregistered():
    """from_python_type() should return None for unregistered Python types."""
    _skip_if_no_cpp()
    value = _hgraph.value

    class CustomUnregisteredClass:
        pass

    result = value.from_python_type(CustomUnregisteredClass)
    assert result is None


@pytest.mark.skip(reason="Awaiting Phase 1 implementation")
def test_from_python_type_consistency_with_get():
    """from_python_type(int) should return same TypeMeta as TypeMeta.get('int')."""
    _skip_if_no_cpp()
    value = _hgraph.value

    meta_by_type = value.from_python_type(int)
    meta_by_name = value.TypeMeta.get("int")

    # Should be the exact same object (identity, not equality)
    assert meta_by_type is meta_by_name


# =============================================================================
# Built-in type registration tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 1 implementation")
def test_builtin_types_have_names():
    """All built-in scalar types should have canonical names."""
    _skip_if_no_cpp()
    value = _hgraph.value

    builtin_types = [
        (int, "int"),
        (float, "float"),
        (str, "str"),
        (bool, "bool"),
    ]

    for py_type, expected_name in builtin_types:
        meta = value.get_scalar_type_meta(py_type)
        assert meta.name == expected_name, f"Expected {py_type} to have name '{expected_name}', got '{meta.name}'"


@pytest.mark.skip(reason="Awaiting Phase 1 implementation")
def test_datetime_types_have_names():
    """Date/time scalar types should have canonical names."""
    _skip_if_no_cpp()
    from datetime import date, datetime, timedelta
    value = _hgraph.value

    datetime_types = [
        (date, "date"),
        (datetime, "datetime"),
        (timedelta, "timedelta"),
    ]

    for py_type, expected_name in datetime_types:
        meta = value.get_scalar_type_meta(py_type)
        assert meta.name == expected_name, f"Expected {py_type} to have name '{expected_name}', got '{meta.name}'"
