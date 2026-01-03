"""
Tests for Queue type in the Value system.

Queue is a FIFO data structure with optional max capacity.
- When unbounded (max_capacity = 0), grows dynamically
- When bounded and full, oldest element is evicted (like cyclic buffer)
- Supports push_back() and pop_front()

These tests verify the Queue implementation including:
- Creation with bounded and unbounded capacity
- Push and pop operations
- FIFO ordering
- Eviction behavior when bounded and full
- Equality and hashing
- Python interop
"""

import pytest


def test_create_unbounded_queue():
    """Create an unbounded queue."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()

    # Create unbounded queue (max_capacity = 0)
    queue_schema = registry.queue(int_schema).build()

    assert queue_schema is not None
    assert queue_schema.kind == value.TypeKind.Queue


def test_create_bounded_queue():
    """Create a bounded queue with max capacity."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()

    # Create bounded queue with max capacity of 5
    queue_schema = registry.queue(int_schema).max_capacity(5).build()

    assert queue_schema is not None
    assert queue_schema.kind == value.TypeKind.Queue


def test_queue_max_capacity():
    """Queue reports correct max capacity."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()

    # Bounded queue
    bounded_schema = registry.queue(int_schema).max_capacity(10).build()
    bounded_v = value.PlainValue(bounded_schema)
    bounded_q = bounded_v.view().as_queue()
    assert bounded_q.max_capacity() == 10
    assert bounded_q.has_max_capacity() == True

    # Unbounded queue
    unbounded_schema = registry.queue(int_schema).build()
    unbounded_v = value.PlainValue(unbounded_schema)
    unbounded_q = unbounded_v.view().as_queue()
    assert unbounded_q.max_capacity() == 0
    assert unbounded_q.has_max_capacity() == False


def test_queue_initially_empty():
    """Newly created queue is empty."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(5).build()

    v = value.PlainValue(queue_schema)
    q = v.const_view().as_queue()

    assert q.size() == 0
    assert len(q) == 0


def test_push_back_single():
    """Push a single element to the queue."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(5).build()

    v = value.PlainValue(queue_schema)
    q = v.view().as_queue()

    elem = value.PlainValue(42)
    q.push_back(elem.const_view())

    assert q.size() == 1
    assert q[0].as_int() == 42


def test_push_back_multiple():
    """Push multiple elements to the queue."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(10).build()

    v = value.PlainValue(queue_schema)
    q = v.view().as_queue()

    for i in range(5):
        elem = value.PlainValue(i * 10)
        q.push_back(elem.const_view())

    assert q.size() == 5
    for i in range(5):
        assert q[i].as_int() == i * 10


def test_pop_front():
    """Pop elements from front of queue."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(10).build()

    v = value.PlainValue(queue_schema)
    q = v.view().as_queue()

    # Push 3 elements: 10, 20, 30
    for val in [10, 20, 30]:
        elem = value.PlainValue(val)
        q.push_back(elem.const_view())

    assert q.size() == 3
    assert q.front().as_int() == 10

    # Pop front
    q.pop_front()
    assert q.size() == 2
    assert q.front().as_int() == 20

    # Pop again
    q.pop_front()
    assert q.size() == 1
    assert q.front().as_int() == 30


def test_pop_front_empty_raises():
    """Pop on empty queue raises."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(5).build()

    v = value.PlainValue(queue_schema)
    q = v.view().as_queue()

    with pytest.raises(IndexError):
        q.pop_front()


def test_fifo_order():
    """Queue maintains FIFO order."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(10).build()

    v = value.PlainValue(queue_schema)
    q = v.view().as_queue()

    # Push 1, 2, 3, 4, 5
    for i in range(1, 6):
        elem = value.PlainValue(i)
        q.push_back(elem.const_view())

    # Pop should return 1, 2, 3, 4, 5 in order
    for expected in range(1, 6):
        assert q.front().as_int() == expected
        q.pop_front()

    assert q.size() == 0


def test_front_and_back():
    """Front and back return correct elements."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(10).build()

    v = value.PlainValue(queue_schema)
    q = v.view().as_queue()

    for i in [10, 20, 30, 40]:
        elem = value.PlainValue(i)
        q.push_back(elem.const_view())

    assert q.front().as_int() == 10  # Oldest
    assert q.back().as_int() == 40   # Newest


def test_bounded_queue_eviction():
    """Bounded queue evicts oldest when full."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(3).build()

    v = value.PlainValue(queue_schema)
    q = v.view().as_queue()

    # Push 1, 2, 3 - queue is now full
    for i in [1, 2, 3]:
        elem = value.PlainValue(i)
        q.push_back(elem.const_view())

    assert q.size() == 3
    assert list(e.as_int() for e in q) == [1, 2, 3]

    # Push 4 - evicts 1
    elem = value.PlainValue(4)
    q.push_back(elem.const_view())

    assert q.size() == 3
    assert list(e.as_int() for e in q) == [2, 3, 4]

    # Push 5 - evicts 2
    elem = value.PlainValue(5)
    q.push_back(elem.const_view())

    assert q.size() == 3
    assert list(e.as_int() for e in q) == [3, 4, 5]


def test_unbounded_queue_grows():
    """Unbounded queue grows to accommodate elements."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).build()  # Unbounded

    v = value.PlainValue(queue_schema)
    q = v.view().as_queue()

    # Push many elements - should grow
    for i in range(100):
        elem = value.PlainValue(i)
        q.push_back(elem.const_view())

    assert q.size() == 100

    # Verify order
    for i in range(100):
        assert q[i].as_int() == i


def test_clear():
    """Clear empties the queue."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(10).build()

    v = value.PlainValue(queue_schema)
    q = v.view().as_queue()

    for i in range(5):
        elem = value.PlainValue(i)
        q.push_back(elem.const_view())

    assert q.size() == 5

    q.clear()

    assert q.size() == 0


def test_push_after_clear():
    """Can push after clearing."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(5).build()

    v = value.PlainValue(queue_schema)
    q = v.view().as_queue()

    # Initial push
    for i in range(3):
        elem = value.PlainValue(i)
        q.push_back(elem.const_view())

    q.clear()

    # Push after clear
    elem = value.PlainValue(100)
    q.push_back(elem.const_view())

    assert q.size() == 1
    assert q[0].as_int() == 100


def test_iteration():
    """Queue supports iteration in FIFO order."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(10).build()

    v = value.PlainValue(queue_schema)
    q = v.view().as_queue()

    values = [10, 20, 30, 40, 50]
    for val in values:
        elem = value.PlainValue(val)
        q.push_back(elem.const_view())

    result = [e.as_int() for e in q]
    assert result == values


def test_equal_queues():
    """Equal queues compare equal."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(5).build()

    v1 = value.PlainValue(queue_schema)
    v2 = value.PlainValue(queue_schema)

    q1 = v1.view().as_queue()
    q2 = v2.view().as_queue()

    for val in [1, 2, 3]:
        elem = value.PlainValue(val)
        q1.push_back(elem.const_view())
        q2.push_back(elem.const_view())

    assert v1.const_view().equals(v2.const_view())


def test_unequal_queues():
    """Queues with different elements compare unequal."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(5).build()

    v1 = value.PlainValue(queue_schema)
    v2 = value.PlainValue(queue_schema)

    q1 = v1.view().as_queue()
    q2 = v2.view().as_queue()

    for val in [1, 2, 3]:
        elem = value.PlainValue(val)
        q1.push_back(elem.const_view())

    for val in [1, 2, 4]:  # Different
        elem = value.PlainValue(val)
        q2.push_back(elem.const_view())

    assert not v1.const_view().equals(v2.const_view())


def test_hash_consistency():
    """Equal queues have equal hashes."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(5).build()

    v1 = value.PlainValue(queue_schema)
    v2 = value.PlainValue(queue_schema)

    q1 = v1.view().as_queue()
    q2 = v2.view().as_queue()

    for val in [1, 2, 3]:
        elem = value.PlainValue(val)
        q1.push_back(elem.const_view())
        q2.push_back(elem.const_view())

    assert v1.const_view().hash() == v2.const_view().hash()


def test_to_python_returns_list():
    """to_python returns a list in FIFO order."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(10).build()

    v = value.PlainValue(queue_schema)
    q = v.view().as_queue()

    for val in [10, 20, 30]:
        elem = value.PlainValue(val)
        q.push_back(elem.const_view())

    py_list = v.const_view().to_python()

    assert isinstance(py_list, list)
    assert py_list == [10, 20, 30]


def test_from_python():
    """from_python populates queue from list."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(10).build()

    v = value.PlainValue(queue_schema)
    v.view().from_python([1, 2, 3, 4, 5])

    q = v.const_view().as_queue()
    assert q.size() == 5
    assert list(e.as_int() for e in q) == [1, 2, 3, 4, 5]


def test_from_python_truncates_to_max_capacity():
    """from_python truncates to max capacity for bounded queues."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(3).build()

    v = value.PlainValue(queue_schema)
    v.view().from_python([1, 2, 3, 4, 5])  # More than capacity

    q = v.const_view().as_queue()
    assert q.size() == 3
    # Should keep first 3 elements
    assert list(e.as_int() for e in q) == [1, 2, 3]


def test_element_type():
    """Queue reports correct element type."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(5).build()

    v = value.PlainValue(queue_schema)
    q = v.const_view().as_queue()

    assert q.element_type() == int_schema


def test_is_queue():
    """is_queue correctly identifies queue types."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).build()

    v = value.PlainValue(queue_schema)
    assert v.const_view().is_queue() == True
    assert v.const_view().is_cyclic_buffer() == False
    assert v.const_view().is_list() == False


def test_to_string():
    """to_string returns readable representation."""
    from hgraph._hgraph import value

    registry = value.TypeRegistry.instance()
    int_schema = value.scalar_type_meta_int64()
    queue_schema = registry.queue(int_schema).max_capacity(10).build()

    v = value.PlainValue(queue_schema)
    q = v.view().as_queue()

    for val in [1, 2, 3]:
        elem = value.PlainValue(val)
        q.push_back(elem.const_view())

    s = v.const_view().to_string()
    assert "Queue" in s
    assert "1" in s
    assert "2" in s
    assert "3" in s
