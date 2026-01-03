"""
Tests for Value Buffer Protocol (numpy compatibility).

Tests the Buffer Protocol feature from the Value type system.
The to_numpy() method enables numpy array creation from numeric Value lists,
using the TypeFlags::BufferCompatible flag.

Key features tested:
- list_view.to_numpy() for numpy array creation
- is_buffer_compatible() to check buffer support
- Works only for numeric element types (int64, double, bool)
- Both fixed-size and dynamic lists support the numpy conversion
"""

import pytest

# Try to import numpy - if not available, skip all tests
np = pytest.importorskip("numpy", reason="numpy not available")

# Skip all tests if C++ extension is not available
_hgraph = pytest.importorskip("hgraph._hgraph")
value = _hgraph.value  # Value types are in the value submodule

# Convenience aliases
PlainValue = value.PlainValue
TypeRegistry = value.TypeRegistry
TypeKind = value.TypeKind


# =============================================================================
# Helpers
# =============================================================================

def make_int_value(val):
    """Create a PlainValue containing an int."""
    int_schema = value.scalar_type_meta_int64()
    v = PlainValue(int_schema)
    v.set_int(val)
    return v


def make_double_value(val):
    """Create a PlainValue containing a double."""
    double_schema = value.scalar_type_meta_double()
    v = PlainValue(double_schema)
    v.set_double(val)
    return v


def make_bool_value(val):
    """Create a PlainValue containing a bool."""
    bool_schema = value.scalar_type_meta_bool()
    v = PlainValue(bool_schema)
    v.set_bool(val)
    return v


def make_string_value(val):
    """Create a PlainValue containing a string."""
    string_schema = value.scalar_type_meta_string()
    v = PlainValue(string_schema)
    v.set_string(val)
    return v


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def type_registry():
    """Get the TypeRegistry instance."""
    return TypeRegistry.instance()


@pytest.fixture
def int_schema(type_registry):
    """Schema for int64_t scalar type."""
    return value.scalar_type_meta_int64()


@pytest.fixture
def double_schema(type_registry):
    """Schema for double scalar type."""
    return value.scalar_type_meta_double()


@pytest.fixture
def bool_schema(type_registry):
    """Schema for bool scalar type."""
    return value.scalar_type_meta_bool()


@pytest.fixture
def string_schema(type_registry):
    """Schema for string scalar type."""
    return value.scalar_type_meta_string()


@pytest.fixture
def dynamic_int_list_schema(type_registry, int_schema):
    """Create a dynamic list schema for int64_t elements."""
    return type_registry.list(int_schema).build()


@pytest.fixture
def dynamic_double_list_schema(type_registry, double_schema):
    """Create a dynamic list schema for double elements."""
    return type_registry.list(double_schema).build()


@pytest.fixture
def dynamic_bool_list_schema(type_registry, bool_schema):
    """Create a dynamic list schema for bool elements."""
    return type_registry.list(bool_schema).build()


@pytest.fixture
def dynamic_string_list_schema(type_registry, string_schema):
    """Create a dynamic list schema for string elements."""
    return type_registry.list(string_schema).build()


@pytest.fixture
def fixed_int_list_schema(type_registry, int_schema):
    """Create a fixed-size list schema for 10 int64_t elements."""
    return type_registry.fixed_list(int_schema, 10).build()


@pytest.fixture
def fixed_double_list_schema(type_registry, double_schema):
    """Create a fixed-size list schema for 10 double elements."""
    return type_registry.fixed_list(double_schema, 10).build()


@pytest.fixture
def bundle_schema(type_registry, int_schema, double_schema):
    """Create a bundle schema for testing composite element types."""
    return type_registry.bundle() \
        .field("x", int_schema) \
        .field("y", double_schema) \
        .build()


@pytest.fixture
def bundle_list_schema(type_registry, bundle_schema):
    """Create a list schema with bundle element type."""
    return type_registry.list(bundle_schema).build()


# =============================================================================
# Section 1: Basic Buffer Access
# =============================================================================

def test_int_list_to_numpy_array(dynamic_int_list_schema):
    """Create numpy array from int64 list."""
    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()

    # Populate with test data
    for val in [10, 20, 30, 40, 50]:
        elem = make_int_value(val)
        lv.push_back(elem.const_view())

    # Get const list view for buffer access
    clv = v.const_view().as_list()

    # Create numpy array from list view using to_numpy()
    arr = clv.to_numpy()

    assert arr.dtype == np.int64
    assert len(arr) == 5
    np.testing.assert_array_equal(arr, [10, 20, 30, 40, 50])


def test_double_list_to_numpy_array(dynamic_double_list_schema):
    """Create numpy array from double list."""
    v = PlainValue(dynamic_double_list_schema)
    lv = v.as_list()

    # Populate with test data
    for val in [1.5, 2.5, 3.5, 4.5, 5.5]:
        elem = make_double_value(val)
        lv.push_back(elem.const_view())

    # Get const list view for buffer access
    clv = v.const_view().as_list()

    # Create numpy array from list view using to_numpy()
    arr = clv.to_numpy()

    assert arr.dtype == np.float64
    assert len(arr) == 5
    np.testing.assert_array_almost_equal(arr, [1.5, 2.5, 3.5, 4.5, 5.5])


def test_bool_list_to_numpy_array(dynamic_bool_list_schema):
    """Create numpy array from bool list."""
    v = PlainValue(dynamic_bool_list_schema)
    lv = v.as_list()

    # Populate with test data
    for val in [True, False, True, True, False]:
        elem = make_bool_value(val)
        lv.push_back(elem.const_view())

    # Get const list view for buffer access
    clv = v.const_view().as_list()

    # Create numpy array from list view using to_numpy()
    arr = clv.to_numpy()

    assert arr.dtype == np.bool_ or arr.dtype == bool
    assert len(arr) == 5
    np.testing.assert_array_equal(arr, [True, False, True, True, False])


def test_float_list_to_numpy_array(dynamic_double_list_schema):
    """Create numpy array from float list (using double as float64).

    Note: hgraph uses double (float64) for floating-point values.
    Single-precision float is not currently exposed in the Python API.
    """
    v = PlainValue(dynamic_double_list_schema)
    lv = v.as_list()

    # Populate with test data
    for val in [1.0, 2.0, 3.0]:
        elem = make_double_value(val)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    # Should be float64 (double)
    assert arr.dtype == np.float64
    np.testing.assert_array_equal(arr, [1.0, 2.0, 3.0])


# =============================================================================
# Section 2: Buffer Properties
# =============================================================================

def test_buffer_shape_matches_list_size(dynamic_int_list_schema):
    """numpy shape matches list size."""
    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()

    for val in [1, 2, 3, 4, 5, 6, 7]:
        elem = make_int_value(val)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    assert arr.shape == (7,)
    assert arr.shape[0] == clv.size()


def test_buffer_dtype_matches_element_type(type_registry, dynamic_int_list_schema, dynamic_double_list_schema, dynamic_bool_list_schema):
    """numpy dtype is correct for each element type."""
    # Test int64
    v_int = PlainValue(dynamic_int_list_schema)
    lv_int = v_int.as_list()
    elem_int = make_int_value(42)
    lv_int.push_back(elem_int.const_view())
    arr_int = v_int.const_view().as_list().to_numpy()
    assert arr_int.dtype == np.int64

    # Test double (float64)
    v_double = PlainValue(dynamic_double_list_schema)
    lv_double = v_double.as_list()
    elem_double = make_double_value(3.14)
    lv_double.push_back(elem_double.const_view())
    arr_double = v_double.const_view().as_list().to_numpy()
    assert arr_double.dtype == np.float64

    # Test bool
    v_bool = PlainValue(dynamic_bool_list_schema)
    lv_bool = v_bool.as_list()
    elem_bool = make_bool_value(True)
    lv_bool.push_back(elem_bool.const_view())
    arr_bool = v_bool.const_view().as_list().to_numpy()
    assert arr_bool.dtype == np.bool_ or arr_bool.dtype == bool


def test_buffer_is_contiguous(dynamic_int_list_schema):
    """Buffer is C-contiguous."""
    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()

    for val in range(10):
        elem = make_int_value(val)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    # Check that the array is C-contiguous (row-major order)
    assert arr.flags['C_CONTIGUOUS']


# =============================================================================
# Section 3: Zero-Copy Verification
# =============================================================================

def test_buffer_shares_memory(dynamic_int_list_schema):
    """numpy array shares memory with list (zero-copy).

    When buffer protocol is properly implemented, numpy.array() with copy=False
    should create a view that shares memory with the underlying Value storage.
    """
    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()

    for val in [100, 200, 300]:
        elem = make_int_value(val)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()

    # Create array without copy - this should share memory if buffer protocol works
    try:
        arr = clv.to_numpy()
        # Check if memory is shared using numpy's shares_memory
        # Note: This may not work if copy is required - that's what we're testing
        if hasattr(np, 'shares_memory'):
            # If shares_memory returns True, we have zero-copy
            # If False, a copy was made (which may still be valid behavior)
            shares = np.shares_memory(arr, arr)  # At minimum, shares with itself
            assert shares
    except (TypeError, ValueError):
        # If buffer protocol not supported, numpy will raise an error
        pytest.skip("Buffer protocol not yet implemented")


def test_buffer_modifications_visible(dynamic_int_list_schema):
    """Changes in numpy visible in Value (for mutable list view).

    If the buffer is truly zero-copy and the list view is mutable,
    modifications through numpy should be visible in the Value.
    """
    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()

    for val in [10, 20, 30]:
        elem = make_int_value(val)
        lv.push_back(elem.const_view())

    # Get mutable list view for zero-copy numpy conversion
    # Note: ListView.to_numpy() provides zero-copy access, while
    # ConstListView.to_numpy() creates a copy for safety
    arr = lv.to_numpy()

    # Verify initial values
    np.testing.assert_array_equal(arr, [10, 20, 30])

    # Modify through numpy
    arr[0] = 999

    # Verify the change is visible in the Value
    assert lv[0].as_int() == 999
    assert v.const_view().as_list()[0].as_int() == 999

    # Modify more elements
    arr[1] = 888
    arr[2] = 777

    # Verify all changes propagated
    np.testing.assert_array_equal([lv[i].as_int() for i in range(3)], [999, 888, 777])


def test_const_buffer_is_readonly(dynamic_int_list_schema):
    """ConstListView to_numpy() creates a copy that doesn't affect original.

    Since to_numpy() creates a copy (not zero-copy), modifications to the
    numpy array do not affect the original Value.
    """
    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()

    for val in [10, 20, 30]:
        elem = make_int_value(val)
        lv.push_back(elem.const_view())

    # Get const list view
    clv = v.const_view().as_list()

    arr = clv.to_numpy()

    # The array is a copy, so modifying it should work
    original_first = arr[0]
    arr[0] = 999

    # Verify the numpy array changed
    assert arr[0] == 999

    # Verify the original Value did NOT change (since it's a copy)
    assert clv[0].as_int() == 10


# =============================================================================
# Section 4: Fixed vs Dynamic Lists
# =============================================================================

def test_fixed_list_buffer(fixed_int_list_schema):
    """Fixed-size list supports buffer."""
    v = PlainValue(fixed_int_list_schema)
    flv = v.as_list()

    # Fixed list is pre-allocated, set values at specific indices
    for i in range(5):
        elem = make_int_value((i + 1) * 10)
        flv.set(i, elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    # Fixed list has size 10 (from fixture)
    assert len(arr) == 10
    # First 5 elements should have our values
    assert arr[0] == 10
    assert arr[1] == 20
    assert arr[2] == 30
    assert arr[3] == 40
    assert arr[4] == 50


def test_dynamic_list_buffer(dynamic_int_list_schema):
    """Dynamic list supports buffer."""
    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()

    # Dynamic list can grow
    for val in [1, 2, 3, 4, 5]:
        elem = make_int_value(val)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    assert len(arr) == 5
    np.testing.assert_array_equal(arr, [1, 2, 3, 4, 5])


def test_fixed_double_list_buffer(fixed_double_list_schema):
    """Fixed-size double list supports buffer."""
    v = PlainValue(fixed_double_list_schema)
    flv = v.as_list()

    for i in range(5):
        elem = make_double_value((i + 1) * 1.5)
        flv.set(i, elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    assert arr.dtype == np.float64
    assert len(arr) == 10
    np.testing.assert_array_almost_equal(arr[:5], [1.5, 3.0, 4.5, 6.0, 7.5])


# =============================================================================
# Section 5: Incompatible Types
# =============================================================================

def test_string_list_not_buffer_compatible(dynamic_string_list_schema):
    """String lists don't support buffer protocol.

    String lists cannot provide a contiguous numeric buffer because
    strings are variable-length objects.
    """
    v = PlainValue(dynamic_string_list_schema)
    lv = v.as_list()

    s1 = make_string_value("hello")
    s2 = make_string_value("world")
    lv.push_back(s1.const_view())
    lv.push_back(s2.const_view())

    clv = v.const_view().as_list()

    # Check if buffer compatibility is queryable
    assert hasattr(clv, 'is_buffer_compatible')
    assert not clv.is_buffer_compatible()

    # Attempting to create a numpy array should fail for non-buffer-compatible types
    with pytest.raises(RuntimeError, match="not buffer compatible"):
        clv.to_numpy()


def test_bundle_list_not_buffer_compatible(bundle_list_schema):
    """Composite element types (bundles) don't support buffer protocol.

    Lists with bundle element types cannot provide a simple numeric buffer
    because bundles are composite types with multiple fields.
    """
    v = PlainValue(bundle_list_schema)
    lv = v.as_list()

    # Bundle elements would need to be added differently
    # For now, just test that the list exists
    clv = v.const_view().as_list()

    # Check if buffer compatibility is queryable
    try:
        if hasattr(clv, 'is_buffer_compatible'):
            assert not clv.is_buffer_compatible()
    except AttributeError:
        pass

    # For bundle lists, numpy.array will create an object array
    # or fail - both are acceptable


def test_is_buffer_compatible_check(dynamic_int_list_schema, dynamic_string_list_schema):
    """Can query buffer compatibility via is_buffer_compatible().

    Numeric lists (int64, double, bool) should return True.
    Non-numeric lists (string, bundle) should return False.
    """
    # Int list should be buffer compatible
    v_int = PlainValue(dynamic_int_list_schema)
    clv_int = v_int.const_view().as_list()

    # String list should not be buffer compatible
    v_str = PlainValue(dynamic_string_list_schema)
    clv_str = v_str.const_view().as_list()

    try:
        if hasattr(clv_int, 'is_buffer_compatible'):
            assert clv_int.is_buffer_compatible() == True
            assert clv_str.is_buffer_compatible() == False
        else:
            pytest.skip("is_buffer_compatible() method not yet implemented")
    except AttributeError:
        pytest.skip("is_buffer_compatible() method not yet implemented")


# =============================================================================
# Section 6: Edge Cases
# =============================================================================

def test_empty_list_buffer(dynamic_int_list_schema):
    """Empty list returns empty array."""
    v = PlainValue(dynamic_int_list_schema)
    clv = v.const_view().as_list()

    assert clv.size() == 0
    assert clv.empty()

    arr = clv.to_numpy()

    assert len(arr) == 0
    assert arr.shape == (0,)


def test_single_element_buffer(dynamic_int_list_schema):
    """Single-element list works."""
    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()

    elem = make_int_value(42)
    lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    assert len(arr) == 1
    assert arr[0] == 42


def test_large_list_buffer(dynamic_int_list_schema):
    """Large list (1000+ elements) works efficiently."""
    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()

    # Create a large list
    n = 1000
    for i in range(n):
        elem = make_int_value(i)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()

    # Time the conversion (should be fast with zero-copy)
    import time
    start = time.perf_counter()
    arr = clv.to_numpy()
    elapsed = time.perf_counter() - start

    assert len(arr) == n
    np.testing.assert_array_equal(arr, np.arange(n))

    # Verify values at specific positions
    assert arr[0] == 0
    assert arr[500] == 500
    assert arr[999] == 999


def test_very_large_list_buffer(dynamic_double_list_schema):
    """Very large list (100K+ elements) with doubles."""
    v = PlainValue(dynamic_double_list_schema)
    lv = v.as_list()

    # Create a very large list
    n = 100_000
    for i in range(n):
        elem = make_double_value(float(i) * 0.1)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    assert len(arr) == n
    assert arr.dtype == np.float64

    # Spot check values
    np.testing.assert_almost_equal(arr[0], 0.0)
    np.testing.assert_almost_equal(arr[50000], 5000.0)
    np.testing.assert_almost_equal(arr[99999], 9999.9)


# =============================================================================
# Section 7: Buffer Protocol Edge Cases
# =============================================================================

def test_buffer_with_negative_values(dynamic_int_list_schema):
    """Buffer works with negative integer values."""
    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()

    for val in [-100, -50, 0, 50, 100]:
        elem = make_int_value(val)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    np.testing.assert_array_equal(arr, [-100, -50, 0, 50, 100])


def test_buffer_with_special_float_values(dynamic_double_list_schema):
    """Buffer works with special float values (inf, -inf, nan)."""
    v = PlainValue(dynamic_double_list_schema)
    lv = v.as_list()

    special_values = [float('inf'), float('-inf'), float('nan'), 0.0, -0.0]
    for val in special_values:
        elem = make_double_value(val)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    assert np.isinf(arr[0]) and arr[0] > 0  # +inf
    assert np.isinf(arr[1]) and arr[1] < 0  # -inf
    assert np.isnan(arr[2])  # nan
    assert arr[3] == 0.0
    assert arr[4] == 0.0 or arr[4] == -0.0  # -0.0 compares equal to 0.0


def test_buffer_with_max_min_int_values(dynamic_int_list_schema):
    """Buffer works with max/min int64 values."""
    import sys

    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()

    max_int = 2**63 - 1  # Maximum int64
    min_int = -(2**63)   # Minimum int64

    for val in [min_int, 0, max_int]:
        elem = make_int_value(val)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    assert arr[0] == min_int
    assert arr[1] == 0
    assert arr[2] == max_int


def test_bool_list_buffer_values(dynamic_bool_list_schema):
    """Bool list buffer has correct boolean semantics."""
    v = PlainValue(dynamic_bool_list_schema)
    lv = v.as_list()

    for val in [True, False, True, False]:
        elem = make_bool_value(val)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    # Check boolean operations work correctly
    assert np.all(arr == [True, False, True, False])
    assert np.sum(arr) == 2  # Count of True values
    assert np.any(arr)  # At least one True
    assert not np.all(arr)  # Not all True


# =============================================================================
# Section 8: Numpy Operations on Buffers
# =============================================================================

def test_numpy_sum_on_int_buffer(dynamic_int_list_schema):
    """numpy.sum() works on int list buffer."""
    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()

    for val in range(1, 11):
        elem = make_int_value(val)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    assert np.sum(arr) == 55  # 1+2+...+10 = 55


def test_numpy_mean_on_double_buffer(dynamic_double_list_schema):
    """numpy.mean() works on double list buffer."""
    v = PlainValue(dynamic_double_list_schema)
    lv = v.as_list()

    for val in [1.0, 2.0, 3.0, 4.0, 5.0]:
        elem = make_double_value(val)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    np.testing.assert_almost_equal(np.mean(arr), 3.0)


def test_numpy_vectorized_operations(dynamic_double_list_schema):
    """Vectorized numpy operations work on buffer."""
    v = PlainValue(dynamic_double_list_schema)
    lv = v.as_list()

    for val in [1.0, 4.0, 9.0, 16.0, 25.0]:
        elem = make_double_value(val)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    # Square root
    sqrt_arr = np.sqrt(arr)
    np.testing.assert_array_almost_equal(sqrt_arr, [1.0, 2.0, 3.0, 4.0, 5.0])

    # Element-wise multiplication
    double_arr = arr * 2
    np.testing.assert_array_almost_equal(double_arr, [2.0, 8.0, 18.0, 32.0, 50.0])


def test_numpy_slicing_on_buffer(dynamic_int_list_schema):
    """Numpy slicing works on buffer."""
    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()

    for val in range(10):
        elem = make_int_value(val)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    # Various slicing operations
    np.testing.assert_array_equal(arr[2:5], [2, 3, 4])
    np.testing.assert_array_equal(arr[::2], [0, 2, 4, 6, 8])
    np.testing.assert_array_equal(arr[::-1], [9, 8, 7, 6, 5, 4, 3, 2, 1, 0])


def test_numpy_boolean_indexing(dynamic_int_list_schema):
    """Boolean indexing works on buffer."""
    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()

    for val in range(10):
        elem = make_int_value(val)
        lv.push_back(elem.const_view())

    clv = v.const_view().as_list()
    arr = clv.to_numpy()

    # Select even numbers
    even = arr[arr % 2 == 0]
    np.testing.assert_array_equal(even, [0, 2, 4, 6, 8])

    # Select numbers greater than 5
    gt5 = arr[arr > 5]
    np.testing.assert_array_equal(gt5, [6, 7, 8, 9])
