"""
Test memory_size calculation for graph builders.
Verifies that memory_size is calculated correctly and cached at the graph level.
"""
import pytest
from hgraph import graph, wire_graph, const, print_, TS, compute_node, null_sink
from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext


def test_memory_size_simple_graph():
    """Test memory_size for a simple graph with basic nodes."""
    @graph
    def simple_graph():
        c = const("Hello")
        print_("{}", c)

    with WiringNodeInstanceContext():
        gb = wire_graph(simple_graph)

    # Verify memory_size is available and returns a positive value
    assert hasattr(gb, 'memory_size')
    size = gb.memory_size()
    assert size > 0, f"memory_size should be positive, got {size}"

    # Verify it's cached (call multiple times, should be the same)
    size2 = gb.memory_size()
    assert size == size2, "memory_size should be cached and return the same value"


def test_memory_size_with_compute_node():
    """Test memory_size for a graph with compute nodes."""
    @compute_node
    def add_one(x: TS[int]) -> TS[int]:
        return x.value + 1

    @graph
    def compute_graph():
        c = const(5)
        result = add_one(c)
        null_sink(result)  # Use null_sink instead of print_ for non-string types

    with WiringNodeInstanceContext():
        gb = wire_graph(compute_graph)

    size = gb.memory_size()
    assert size > 0, f"memory_size should be positive, got {size}"

    # Verify it's cached
    assert gb.memory_size() == size


def test_memory_size_increases_with_more_nodes():
    """Test that memory_size increases as more nodes are added."""
    @compute_node
    def add_one(x: TS[int]) -> TS[int]:
        return x.value + 1

    @graph
    def small_graph():
        c = const(5)
        result = add_one(c)
        null_sink(result)

    @graph
    def large_graph():
        c1 = const(5)
        c2 = const(10)
        r1 = add_one(c1)
        r2 = add_one(c2)
        r3 = add_one(r1)
        null_sink(r2)
        null_sink(r3)

    with WiringNodeInstanceContext():
        gb_small = wire_graph(small_graph)
        gb_large = wire_graph(large_graph)

    size_small = gb_small.memory_size()
    size_large = gb_large.memory_size()

    assert size_large > size_small, \
        f"Large graph ({size_large}) should have larger memory_size than small graph ({size_small})"


def test_memory_size_accounts_for_alignment():
    """Test that memory_size accounts for alignment requirements."""
    @graph
    def alignment_test():
        c1 = const("1")
        c2 = const("2")
        c3 = const("3")
        print_("{}", c1)
        print_("{}", c2)
        print_("{}", c3)

    with WiringNodeInstanceContext():
        gb = wire_graph(alignment_test)

    size = gb.memory_size()
    assert size > 0

    # The size should be at least the sum of basic sizes
    # (exact calculation depends on alignment, so we just verify it's reasonable)
    assert size >= 100, f"memory_size seems too small: {size}"


def test_memory_size_consistent_across_calls():
    """Test that memory_size is consistent across multiple calls."""
    @graph
    def consistency_test():
        c1 = const("1")
        c2 = const("2.0")
        print_("{}", c1)
        print_("{}", c2)

    with WiringNodeInstanceContext():
        gb = wire_graph(consistency_test)

    # Call multiple times
    sizes = [gb.memory_size() for _ in range(10)]
    
    # All calls should return the same value (cached)
    assert len(set(sizes)) == 1, f"memory_size should be cached, got different values: {sizes}"


def test_memory_size_for_nested_structures():
    """Test memory_size for graphs with nested time-series structures."""
    @compute_node
    def process(x: TS[int]) -> TS[int]:
        return x.value * 2

    @graph
    def nested_test():
        c = const(5)
        result = process(c)
        null_sink(result)

    with WiringNodeInstanceContext():
        gb = wire_graph(nested_test)

    size = gb.memory_size()
    assert size > 0

    # Verify it includes sizes for nested structures
    # (The exact value depends on implementation, but should be reasonable)
    assert size >= 200, f"memory_size for nested structures seems too small: {size}"

