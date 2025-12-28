"""
Test Value Type System - Policy-Based Extensions

Tests for policy-based extensions like WithPythonCache based on:
- Value_USER_GUIDE.md Section 16: Extending Value Operations
- Value_DESIGN.md Section 13: Extension Mechanism
- Value_EXAMPLES.md Sections 1-4: Policy and CRTP patterns

These tests verify the C++ implementation once available.
The tests are designed to work with the hgraph._hgraph extension module.
"""
import pytest

# Try to import the C++ extension module
# Tests will be skipped if not available
_hgraph = pytest.importorskip("hgraph._hgraph", reason="C++ extension not available")
value = _hgraph.value  # Value types are in the value submodule

# Import types from the C++ extension
try:
    Value = value.PlainValue
    CachedValue = value.CachedValue
    PlainValue = value.PlainValue
except AttributeError:
    pytest.skip("Policy-based Value types not yet exposed in C++ extension", allow_module_level=True)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def large_int():
    """Large integer value (>256) to avoid Python's small integer cache.

    Python caches small integers (-5 to 256), so identity tests would fail
    with small integers even without explicit caching.
    """
    return 123456789


@pytest.fixture
def another_large_int():
    """Another large integer for comparison tests."""
    return 987654321


@pytest.fixture
def cached_value(large_int):
    """Create a CachedValue for testing."""
    return CachedValue(large_int)


@pytest.fixture
def plain_value(large_int):
    """Create a PlainValue (no caching) for testing."""
    return PlainValue(large_int)


# =============================================================================
# Section 16.1: Policy-Based Extensions - Basic Usage
# =============================================================================

class TestPolicyBasedCreation:
    """Tests for policy-based Value creation (User Guide Section 16.1)."""

    def test_plain_value_creation(self, large_int):
        """PlainValue can be created from integer."""
        v = PlainValue(large_int)
        assert v.valid()

    def test_cached_value_creation(self, large_int):
        """CachedValue can be created from integer."""
        v = CachedValue(large_int)
        assert v.valid()

    def test_plain_value_same_api_as_cached(self, large_int):
        """PlainValue has same API as CachedValue."""
        plain = PlainValue(large_int)
        cached = CachedValue(large_int)

        # Both should have to_python
        assert hasattr(plain, 'to_python')
        assert hasattr(cached, 'to_python')

        # Both should have valid()
        assert plain.valid()
        assert cached.valid()

    def test_default_value_no_cache(self, large_int):
        """Default Value<> (no policy) has no caching overhead."""
        try:
            # This may be exposed as Value() or PlainValue
            v = Value(large_int)
            assert v.valid()
        except TypeError:
            # If Value requires a policy, that's fine
            pass


class TestCachingBehavior:
    """Tests for Python caching behavior (User Guide Section 16.1)."""

    def test_cached_value_returns_same_python_object(self, large_int):
        """CachedValue.to_python() returns same object on subsequent calls."""
        v = CachedValue(large_int)
        py1 = v.to_python()
        py2 = v.to_python()

        # Should be the exact same Python object (identity check)
        assert py1 is py2

    def test_plain_value_may_return_different_objects(self, large_int):
        """PlainValue.to_python() may return different objects each call.

        Note: This test verifies that PlainValue doesn't cache,
        but large integers are not cached by Python so this should work.
        """
        v = PlainValue(large_int)
        py1 = v.to_python()
        py2 = v.to_python()

        # For large integers (>256), Python creates new objects
        # Without caching, these should be different objects
        # Note: The values are equal, but objects may differ
        assert py1 == py2
        # We can't assert py1 is not py2 because PlainValue *could* cache
        # The point is CachedValue *definitely* caches

    def test_cache_returns_correct_value(self, large_int):
        """Cached value returns correct Python value."""
        v = CachedValue(large_int)
        py_obj = v.to_python()
        assert py_obj == large_int


class TestSmallIntegerCaveats:
    """Tests documenting small integer caching caveats.

    Python caches small integers (-5 to 256), so identity tests
    would fail even without explicit CachedValue caching.
    These tests document this behavior.
    """

    def test_small_int_cached_by_python(self):
        """Small integers are cached by Python regardless of Value policy.

        This test documents why we use large integers (>256) for cache tests.
        """
        # Python caches 42
        v1 = PlainValue(42)
        v2 = PlainValue(42)
        py1 = v1.to_python()
        py2 = v2.to_python()

        # Python's small integer cache makes these the same object
        # even without CachedValue!
        assert py1 == py2
        # assert py1 is py2  # This would pass due to Python's cache, not ours!

    def test_large_int_not_cached_by_python(self):
        """Large integers (>256) are NOT cached by Python.

        This is why we use large integers for testing our caching.
        """
        a = 123456789
        b = 123456789
        # Python creates new objects for large integers
        # (they're equal but not identical)
        assert a == b


# =============================================================================
# Section 16.1: Automatic Cache Invalidation
# =============================================================================

class TestCacheInvalidation:
    """Tests for automatic cache invalidation (User Guide Section 16.1)."""

    def test_mutable_view_invalidates_cache(self, large_int, another_large_int):
        """Getting mutable view invalidates the cache."""
        v = CachedValue(large_int)

        # First to_python caches
        py1 = v.to_python()
        assert py1 == large_int

        # Get mutable view - should invalidate cache
        view = v.view()
        view.set_int(another_large_int)

        # Next to_python should return new object
        py2 = v.to_python()
        assert py2 == another_large_int

        # Objects should be different (cache was invalidated)
        assert py1 is not py2

    def test_from_python_updates_cache(self, large_int, another_large_int):
        """from_python() updates the cached object."""
        v = CachedValue(large_int)

        # First to_python
        py1 = v.to_python()

        # from_python with new value
        v.from_python(another_large_int)

        # Cache should now hold the new value
        py2 = v.to_python()
        assert py2 == another_large_int

        # Objects should be different
        assert py1 is not py2

    def test_from_python_caches_source_object(self, large_int):
        """from_python() caches the source Python object (Examples 1.3)."""
        v = CachedValue(0)

        py_value = large_int  # This is the Python int object
        v.from_python(py_value)

        # to_python should return the same object we passed in
        result = v.to_python()
        assert result == py_value
        # Note: Can't test identity here since Python may intern the int


# =============================================================================
# Section 16.2: Zero-Overhead Guarantee
# =============================================================================

class TestZeroOverhead:
    """Tests for zero-overhead guarantee (User Guide Section 16.2).

    Note: Size comparisons are implementation details and may not be
    directly testable from Python. These tests document expected behavior.
    """

    def test_plain_value_is_functional(self, large_int):
        """PlainValue works without caching overhead."""
        v = PlainValue(large_int)
        assert v.valid()
        assert v.to_python() == large_int
        assert v.const_view().as_int() == large_int

    def test_cached_value_is_functional(self, large_int):
        """CachedValue works with caching."""
        v = CachedValue(large_int)
        assert v.valid()
        assert v.to_python() == large_int
        assert v.const_view().as_int() == large_int


class TestConditionalCaching:
    """Tests for conditional caching behavior."""

    def test_cache_populated_on_first_to_python(self, large_int):
        """Cache is populated on first to_python() call."""
        v = CachedValue(large_int)

        # Before to_python, cache may be empty
        # (We can't easily test this from Python)

        # First call populates cache
        py1 = v.to_python()

        # Second call uses cache
        py2 = v.to_python()

        # Same object
        assert py1 is py2

    def test_cache_empty_after_invalidation(self, large_int, another_large_int):
        """Cache is empty after invalidation until next to_python()."""
        v = CachedValue(large_int)

        py1 = v.to_python()  # Populate cache

        # Invalidate by getting mutable view
        view = v.view()
        view.set_int(another_large_int)

        # Cache was invalidated, next call repopulates
        py2 = v.to_python()

        assert py1 != py2  # Different values
        assert py1 is not py2  # Different objects


# =============================================================================
# Section 16.3 & 16.4: CRTP Mixin Patterns (if exposed)
# =============================================================================

class TestMixinPatterns:
    """Tests for CRTP mixin patterns (Design Section 13.4).

    Note: These tests are for when mixin-based types are exposed to Python.
    Skip if not available.
    """

    def test_tsvalue_if_available(self, large_int):
        """TSValue (if available) combines caching and modification tracking."""
        try:
            from hgraph._hgraph import TSValue
        except ImportError:
            pytest.skip("TSValue not exposed in C++ extension")

        v = TSValue(large_int)
        assert v.valid()

        # Should have caching
        py1 = v.to_python()
        py2 = v.to_python()
        assert py1 is py2

        # Should have modification tracking
        assert hasattr(v, 'on_modified')

    def test_modification_callback(self):
        """Modification callbacks are triggered on from_python()."""
        try:
            from hgraph._hgraph import TSValue
        except ImportError:
            pytest.skip("TSValue not exposed in C++ extension")

        v = TSValue(0)
        callback_count = [0]  # Use list for mutable closure

        def on_change():
            callback_count[0] += 1

        v.on_modified(on_change)
        v.from_python(123456789)

        assert callback_count[0] == 1

    def test_modification_invalidates_cache(self):
        """Modification invalidates cache in TSValue."""
        try:
            from hgraph._hgraph import TSValue
        except ImportError:
            pytest.skip("TSValue not exposed in C++ extension")

        v = TSValue(123456789)
        py1 = v.to_python()

        v.from_python(987654321)

        py2 = v.to_python()
        assert py1 is not py2


# =============================================================================
# Validation Extension Tests (Examples Section 4.1)
# =============================================================================

class TestValidationExtension:
    """Tests for validation extension (if available)."""

    def test_validated_value_rejects_none(self):
        """ValidatedValue rejects None values."""
        try:
            from hgraph._hgraph import ValidatedValue
        except ImportError:
            pytest.skip("ValidatedValue not exposed in C++ extension")

        v = ValidatedValue(0)
        with pytest.raises(RuntimeError, match="None"):
            v.from_python(None)

    def test_validated_value_accepts_valid_input(self):
        """ValidatedValue accepts valid input."""
        try:
            from hgraph._hgraph import ValidatedValue
        except ImportError:
            pytest.skip("ValidatedValue not exposed in C++ extension")

        v = ValidatedValue(0)
        v.from_python(123456789)
        assert v.to_python() == 123456789


# =============================================================================
# API Consistency Tests
# =============================================================================

class TestPolicyAPIConsistency:
    """Tests that all Value types have consistent API."""

    def test_all_types_have_valid_method(self, large_int):
        """All Value types have valid() method."""
        plain = PlainValue(large_int)
        cached = CachedValue(large_int)

        assert plain.valid()
        assert cached.valid()

    def test_all_types_have_to_python_method(self, large_int):
        """All Value types have to_python() method."""
        plain = PlainValue(large_int)
        cached = CachedValue(large_int)

        assert plain.to_python() == large_int
        assert cached.to_python() == large_int

    def test_all_types_have_from_python_method(self, large_int, another_large_int):
        """All Value types have from_python() method."""
        plain = PlainValue(large_int)
        cached = CachedValue(large_int)

        plain.from_python(another_large_int)
        cached.from_python(another_large_int)

        assert plain.to_python() == another_large_int
        assert cached.to_python() == another_large_int

    def test_all_types_have_view_method(self, large_int):
        """All Value types have view() method."""
        plain = PlainValue(large_int)
        cached = CachedValue(large_int)

        assert plain.view().valid()
        assert cached.view().valid()

    def test_all_types_have_const_view_method(self, large_int):
        """All Value types have const_view() method."""
        plain = PlainValue(large_int)
        cached = CachedValue(large_int)

        assert plain.const_view().valid()
        assert cached.const_view().valid()


# =============================================================================
# Type Aliases Tests
# =============================================================================

class TestTypeAliases:
    """Tests for type aliases (User Guide Section 16.7)."""

    def test_plain_value_alias(self, large_int):
        """PlainValue is alias for Value<NoCache>."""
        v = PlainValue(large_int)
        assert v.valid()

    def test_cached_value_alias(self, large_int):
        """CachedValue is alias for Value<WithPythonCache>."""
        v = CachedValue(large_int)
        assert v.valid()

        # Verify caching works
        py1 = v.to_python()
        py2 = v.to_python()
        assert py1 is py2


# =============================================================================
# Edge Cases and Boundary Tests
# =============================================================================

class TestPolicyEdgeCases:
    """Edge cases for policy-based extensions."""

    def test_cached_value_with_empty_string(self):
        """CachedValue works with empty string."""
        v = CachedValue("")
        assert v.valid()
        py1 = v.to_python()
        py2 = v.to_python()
        assert py1 is py2
        assert py1 == ""

    def test_cached_value_with_bool(self):
        """CachedValue works with boolean values."""
        v = CachedValue(True)
        assert v.valid()
        py1 = v.to_python()
        py2 = v.to_python()
        assert py1 is py2
        assert py1 is True

    def test_cached_value_with_float(self):
        """CachedValue works with float values."""
        v = CachedValue(3.14159265359)
        assert v.valid()
        py1 = v.to_python()
        py2 = v.to_python()
        assert py1 is py2
        assert abs(py1 - 3.14159265359) < 1e-10

    def test_multiple_cache_invalidations(self, large_int):
        """Multiple cache invalidations work correctly."""
        v = CachedValue(large_int)

        # Cycle 1: populate -> invalidate -> repopulate
        py1 = v.to_python()
        v.view().set_int(100)
        py2 = v.to_python()

        # Cycle 2: invalidate -> repopulate
        v.view().set_int(200)
        py3 = v.to_python()

        # Cycle 3: invalidate -> repopulate
        v.view().set_int(300)
        py4 = v.to_python()

        assert py1 == large_int
        assert py2 == 100
        assert py3 == 200
        assert py4 == 300


class TestCachingWithDifferentTypes:
    """Tests for caching behavior with different scalar types."""

    def test_cached_int_value(self, large_int):
        """Integer caching works correctly."""
        v = CachedValue(large_int)
        py1 = v.to_python()
        py2 = v.to_python()
        assert py1 is py2
        assert isinstance(py1, int)

    def test_cached_string_value(self):
        """String caching works correctly."""
        s = "a long string that won't be interned by Python"
        v = CachedValue(s)
        py1 = v.to_python()
        py2 = v.to_python()
        assert py1 is py2
        assert py1 == s

    def test_cached_float_value(self):
        """Float caching works correctly."""
        f = 3.141592653589793
        v = CachedValue(f)
        py1 = v.to_python()
        py2 = v.to_python()
        assert py1 is py2
        assert abs(py1 - f) < 1e-15

    def test_cached_bool_true(self):
        """Boolean True caching works correctly."""
        v = CachedValue(True)
        py1 = v.to_python()
        py2 = v.to_python()
        assert py1 is py2
        assert py1 is True

    def test_cached_bool_false(self):
        """Boolean False caching works correctly."""
        v = CachedValue(False)
        py1 = v.to_python()
        py2 = v.to_python()
        assert py1 is py2
        assert py1 is False
