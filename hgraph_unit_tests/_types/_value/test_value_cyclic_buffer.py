"""
Tests for Value CyclicBuffer types.

Tests the CyclicBuffer type from the Value type system.
CyclicBuffer is a fixed-size circular buffer that re-centers on read.
When full, the oldest element is overwritten.

Logical index 0 always refers to the oldest element in the buffer.
"""

import pytest
import numpy as np

# Skip all tests if C++ extension is not available
_hgraph = pytest.importorskip("hgraph._hgraph")
value = _hgraph.value

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
def cyclic_buffer_schema_5(type_registry, int_schema):
    """Create a cyclic buffer schema with capacity 5."""
    return type_registry.cyclic_buffer(int_schema, 5).build()


@pytest.fixture
def cyclic_buffer_schema_3(type_registry, int_schema):
    """Create a cyclic buffer schema with capacity 3."""
    return type_registry.cyclic_buffer(int_schema, 3).build()


@pytest.fixture
def double_cyclic_buffer_schema(type_registry, double_schema):
    """Create a cyclic buffer schema for doubles with capacity 4."""
    return type_registry.cyclic_buffer(double_schema, 4).build()


# =============================================================================
# Basic Creation Tests
# =============================================================================

def test_create_cyclic_buffer(cyclic_buffer_schema_5):
    """Test creating a cyclic buffer."""
    buf = PlainValue(cyclic_buffer_schema_5)
    assert buf.const_view().is_cyclic_buffer()
    assert buf.schema.kind == TypeKind.CyclicBuffer


def test_cyclic_buffer_capacity(cyclic_buffer_schema_5):
    """Test cyclic buffer reports correct capacity."""
    buf = PlainValue(cyclic_buffer_schema_5)
    view = buf.const_view().as_cyclic_buffer()
    assert view.capacity() == 5


def test_cyclic_buffer_initially_empty(cyclic_buffer_schema_5):
    """Test cyclic buffer is initially empty."""
    buf = PlainValue(cyclic_buffer_schema_5)
    view = buf.const_view().as_cyclic_buffer()
    assert len(view) == 0
    assert not view.full()


# =============================================================================
# Push Operations
# =============================================================================

def test_push_back_single(cyclic_buffer_schema_5):
    """Test pushing a single element."""
    buf = PlainValue(cyclic_buffer_schema_5)
    buf_view = buf.view().as_cyclic_buffer()

    elem = make_int_value(42)
    buf_view.push_back(elem.const_view())

    assert len(buf_view) == 1
    assert buf_view[0].as_int() == 42


def test_push_back_multiple(cyclic_buffer_schema_5):
    """Test pushing multiple elements."""
    buf = PlainValue(cyclic_buffer_schema_5)
    buf_view = buf.view().as_cyclic_buffer()

    for i in range(3):
        elem = make_int_value(i * 10)
        buf_view.push_back(elem.const_view())

    assert len(buf_view) == 3
    assert buf_view[0].as_int() == 0
    assert buf_view[1].as_int() == 10
    assert buf_view[2].as_int() == 20


def test_push_back_until_full(cyclic_buffer_schema_3):
    """Test pushing until buffer is full."""
    buf = PlainValue(cyclic_buffer_schema_3)
    buf_view = buf.view().as_cyclic_buffer()

    for i in range(3):
        elem = make_int_value(i)
        buf_view.push_back(elem.const_view())

    assert len(buf_view) == 3
    assert buf_view.full()
    assert buf_view[0].as_int() == 0
    assert buf_view[1].as_int() == 1
    assert buf_view[2].as_int() == 2


# =============================================================================
# Eviction Tests (Cyclic Behavior)
# =============================================================================

def test_eviction_when_full(cyclic_buffer_schema_3):
    """Test that oldest element is evicted when full."""
    buf = PlainValue(cyclic_buffer_schema_3)
    buf_view = buf.view().as_cyclic_buffer()

    # Fill buffer with 0, 1, 2
    for i in range(3):
        elem = make_int_value(i)
        buf_view.push_back(elem.const_view())

    # Push 3 - should evict 0
    elem = make_int_value(3)
    buf_view.push_back(elem.const_view())

    assert len(buf_view) == 3
    assert buf_view.full()
    # Now buffer contains 1, 2, 3 in logical order
    assert buf_view[0].as_int() == 1  # Oldest
    assert buf_view[1].as_int() == 2
    assert buf_view[2].as_int() == 3  # Newest


def test_multiple_evictions(cyclic_buffer_schema_3):
    """Test multiple evictions maintain logical order."""
    buf = PlainValue(cyclic_buffer_schema_3)
    buf_view = buf.view().as_cyclic_buffer()

    # Push 0, 1, 2, 3, 4, 5 into buffer of capacity 3
    for i in range(6):
        elem = make_int_value(i)
        buf_view.push_back(elem.const_view())

    # Buffer should contain 3, 4, 5 (oldest to newest)
    assert len(buf_view) == 3
    assert buf_view[0].as_int() == 3
    assert buf_view[1].as_int() == 4
    assert buf_view[2].as_int() == 5


def test_many_evictions_wrap_around(cyclic_buffer_schema_3):
    """Test buffer behavior after many wrap-arounds."""
    buf = PlainValue(cyclic_buffer_schema_3)
    buf_view = buf.view().as_cyclic_buffer()

    # Push 100 elements
    for i in range(100):
        elem = make_int_value(i)
        buf_view.push_back(elem.const_view())

    # Buffer should contain 97, 98, 99
    assert len(buf_view) == 3
    assert buf_view[0].as_int() == 97
    assert buf_view[1].as_int() == 98
    assert buf_view[2].as_int() == 99


# =============================================================================
# Front/Back Access
# =============================================================================

def test_front_returns_oldest(cyclic_buffer_schema_3):
    """Test front() returns the oldest element."""
    buf = PlainValue(cyclic_buffer_schema_3)
    buf_view = buf.view().as_cyclic_buffer()

    for i in range(5):  # 0, 1, 2, 3, 4 -> buffer has 2, 3, 4
        elem = make_int_value(i)
        buf_view.push_back(elem.const_view())

    assert buf_view.front().as_int() == 2


def test_back_returns_newest(cyclic_buffer_schema_3):
    """Test back() returns the newest element."""
    buf = PlainValue(cyclic_buffer_schema_3)
    buf_view = buf.view().as_cyclic_buffer()

    for i in range(5):  # 0, 1, 2, 3, 4 -> buffer has 2, 3, 4
        elem = make_int_value(i)
        buf_view.push_back(elem.const_view())

    assert buf_view.back().as_int() == 4


# =============================================================================
# Clear Operation
# =============================================================================

def test_clear_empties_buffer(cyclic_buffer_schema_5):
    """Test clear() empties the buffer."""
    buf = PlainValue(cyclic_buffer_schema_5)
    buf_view = buf.view().as_cyclic_buffer()

    # Add some elements
    for i in range(3):
        elem = make_int_value(i)
        buf_view.push_back(elem.const_view())

    assert len(buf_view) == 3

    buf_view.clear()

    assert len(buf_view) == 0
    assert not buf_view.full()


def test_push_after_clear(cyclic_buffer_schema_3):
    """Test that push works correctly after clear."""
    buf = PlainValue(cyclic_buffer_schema_3)
    buf_view = buf.view().as_cyclic_buffer()

    # Fill, clear, refill
    for i in range(3):
        elem = make_int_value(i)
        buf_view.push_back(elem.const_view())

    buf_view.clear()

    for i in range(2):
        elem = make_int_value(i + 100)
        buf_view.push_back(elem.const_view())

    assert len(buf_view) == 2
    assert buf_view[0].as_int() == 100
    assert buf_view[1].as_int() == 101


# =============================================================================
# Iteration
# =============================================================================

def test_iteration_logical_order(cyclic_buffer_schema_3):
    """Test iteration returns elements in logical order (oldest first)."""
    buf = PlainValue(cyclic_buffer_schema_3)
    buf_view = buf.view().as_cyclic_buffer()

    # Push 0, 1, 2, 3, 4 -> buffer contains 2, 3, 4
    for i in range(5):
        elem = make_int_value(i)
        buf_view.push_back(elem.const_view())

    const_view = buf.const_view().as_cyclic_buffer()
    elements = [elem.as_int() for elem in const_view]
    assert elements == [2, 3, 4]


# =============================================================================
# Equality and Hash
# =============================================================================

def test_equal_cyclic_buffers(cyclic_buffer_schema_3):
    """Test equality of cyclic buffers with same content."""
    buf1 = PlainValue(cyclic_buffer_schema_3)
    buf2 = PlainValue(cyclic_buffer_schema_3)

    # Add same elements to both
    for b in [buf1, buf2]:
        view = b.view().as_cyclic_buffer()
        for i in range(3):
            elem = make_int_value(i)
            view.push_back(elem.const_view())

    assert buf1.const_view().equals(buf2.const_view())


def test_unequal_cyclic_buffers(cyclic_buffer_schema_3):
    """Test inequality of cyclic buffers with different content."""
    buf1 = PlainValue(cyclic_buffer_schema_3)
    buf2 = PlainValue(cyclic_buffer_schema_3)

    view1 = buf1.view().as_cyclic_buffer()
    view2 = buf2.view().as_cyclic_buffer()

    for i in range(3):
        elem1 = make_int_value(i)
        elem2 = make_int_value(i + 10)
        view1.push_back(elem1.const_view())
        view2.push_back(elem2.const_view())

    assert not buf1.const_view().equals(buf2.const_view())


def test_equal_after_different_pushes(cyclic_buffer_schema_3):
    """Test buffers are equal if they have same logical content after different paths."""
    buf1 = PlainValue(cyclic_buffer_schema_3)
    buf2 = PlainValue(cyclic_buffer_schema_3)

    # buf1: push 0, 1, 2, 3 -> contains 1, 2, 3
    view1 = buf1.view().as_cyclic_buffer()
    for i in range(4):
        elem = make_int_value(i)
        view1.push_back(elem.const_view())

    # buf2: push 1, 2, 3 directly -> contains 1, 2, 3
    view2 = buf2.view().as_cyclic_buffer()
    for i in range(1, 4):
        elem = make_int_value(i)
        view2.push_back(elem.const_view())

    # Both have logical content [1, 2, 3]
    assert buf1.const_view().equals(buf2.const_view())


def test_hash_consistency(cyclic_buffer_schema_3):
    """Test hash is consistent for equal buffers."""
    buf1 = PlainValue(cyclic_buffer_schema_3)
    buf2 = PlainValue(cyclic_buffer_schema_3)

    for b in [buf1, buf2]:
        view = b.view().as_cyclic_buffer()
        for i in range(3):
            elem = make_int_value(i)
            view.push_back(elem.const_view())

    assert buf1.const_view().hash() == buf2.const_view().hash()


# =============================================================================
# to_numpy Tests
# =============================================================================

def test_to_numpy_int(cyclic_buffer_schema_3):
    """Test to_numpy returns correct numpy array for int buffer."""
    buf = PlainValue(cyclic_buffer_schema_3)
    buf_view = buf.view().as_cyclic_buffer()

    # Push 0, 1, 2, 3, 4 -> buffer contains 2, 3, 4
    for i in range(5):
        elem = make_int_value(i)
        buf_view.push_back(elem.const_view())

    const_view = buf.const_view().as_cyclic_buffer()
    arr = const_view.to_numpy()

    assert isinstance(arr, np.ndarray)
    assert arr.dtype == np.int64
    assert list(arr) == [2, 3, 4]


def test_to_numpy_double(double_cyclic_buffer_schema):
    """Test to_numpy for double buffer."""
    buf = PlainValue(double_cyclic_buffer_schema)
    buf_view = buf.view().as_cyclic_buffer()

    double_schema = value.scalar_type_meta_double()
    for d in [1.5, 2.5, 3.5]:
        elem = PlainValue(double_schema)
        elem.set_double(d)
        buf_view.push_back(elem.const_view())

    const_view = buf.const_view().as_cyclic_buffer()
    arr = const_view.to_numpy()

    assert arr.dtype == np.float64
    np.testing.assert_array_equal(arr, [1.5, 2.5, 3.5])


def test_to_numpy_recentered(cyclic_buffer_schema_3):
    """Test to_numpy returns re-centered (logical order) data."""
    buf = PlainValue(cyclic_buffer_schema_3)
    buf_view = buf.view().as_cyclic_buffer()

    # Push 10, 20, 30, 40, 50 -> buffer contains 30, 40, 50
    for i in range(5):
        elem = make_int_value((i + 1) * 10)
        buf_view.push_back(elem.const_view())

    arr = buf.const_view().as_cyclic_buffer().to_numpy()

    # Array should be in logical order: oldest first
    assert list(arr) == [30, 40, 50]


def test_is_buffer_compatible(cyclic_buffer_schema_3, type_registry):
    """Test is_buffer_compatible returns True for numeric types."""
    buf = PlainValue(cyclic_buffer_schema_3)
    view = buf.const_view().as_cyclic_buffer()
    assert view.is_buffer_compatible()


def test_empty_buffer_to_numpy(cyclic_buffer_schema_3):
    """Test to_numpy on empty buffer."""
    buf = PlainValue(cyclic_buffer_schema_3)
    const_view = buf.const_view().as_cyclic_buffer()

    arr = const_view.to_numpy()
    assert len(arr) == 0
    assert arr.dtype == np.int64


# =============================================================================
# Python Interop (to_python/from_python)
# =============================================================================

def test_to_python_returns_list(cyclic_buffer_schema_3):
    """Test to_python returns a Python list."""
    buf = PlainValue(cyclic_buffer_schema_3)
    buf_view = buf.view().as_cyclic_buffer()

    for i in range(3):
        elem = make_int_value(i * 10)
        buf_view.push_back(elem.const_view())

    py_list = buf.const_view().to_python()
    assert py_list == [0, 10, 20]


def test_to_python_recentered(cyclic_buffer_schema_3):
    """Test to_python returns data in logical order."""
    buf = PlainValue(cyclic_buffer_schema_3)
    buf_view = buf.view().as_cyclic_buffer()

    # Push 0, 1, 2, 3, 4 -> buffer contains 2, 3, 4
    for i in range(5):
        elem = make_int_value(i)
        buf_view.push_back(elem.const_view())

    py_list = buf.const_view().to_python()
    assert py_list == [2, 3, 4]


def test_from_python(cyclic_buffer_schema_5):
    """Test from_python populates buffer from Python list."""
    buf = PlainValue(cyclic_buffer_schema_5)
    buf.view().from_python([10, 20, 30])

    view = buf.const_view().as_cyclic_buffer()
    assert len(view) == 3
    assert view[0].as_int() == 10
    assert view[1].as_int() == 20
    assert view[2].as_int() == 30


def test_from_python_truncates_to_capacity(cyclic_buffer_schema_3):
    """Test from_python only takes up to capacity elements."""
    buf = PlainValue(cyclic_buffer_schema_3)
    buf.view().from_python([1, 2, 3, 4, 5])  # More than capacity

    view = buf.const_view().as_cyclic_buffer()
    # Should only contain first 3 elements
    assert len(view) == 3
    assert view[0].as_int() == 1
    assert view[1].as_int() == 2
    assert view[2].as_int() == 3


# =============================================================================
# Element Type
# =============================================================================

def test_element_type(cyclic_buffer_schema_5, int_schema):
    """Test element_type returns the correct schema."""
    buf = PlainValue(cyclic_buffer_schema_5)
    view = buf.const_view().as_cyclic_buffer()
    assert view.element_type() == int_schema
