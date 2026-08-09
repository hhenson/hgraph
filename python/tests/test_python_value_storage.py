"""Output-local, consumer-selected Python value storage (issue #204).

Python-only edges retain the normalized Python value directly. Mixed
Python/native readership keeps canonical C++ storage with an inline Python
cache. Output values are immutable by graph contract (mutating one is UB), so
preserving object identity is within contract and matches upstream hgraph's
reference-passing behaviour.
"""

import hgraph as hg
import pytest
from hgraph import CompoundScalar, TS, compute_node, graph
from hgraph.test import eval_node

_produced_values: list[object] = []
_consumed_values: list[object] = []
_native_read_ids_a: list[int] = []
_native_read_ids_b: list[int] = []


@compute_node
def _produce_tuple(t: TS[int]) -> TS[tuple[int, ...]]:
    value = (t.value, t.value + 1, t.value + 2)
    _produced_values.append(value)
    return value


@compute_node
def _consume_tuple(v: TS[tuple[int, ...]]) -> TS[int]:
    _consumed_values.append(v.value)
    return v.value[0]


def test_py_to_py_chain_passes_the_original_object():
    _produced_values.clear()
    _consumed_values.clear()

    @graph
    def g(t: TS[int]) -> TS[int]:
        return _consume_tuple(_produce_tuple(t))

    assert eval_node(g, [1, 5, 9]) == [1, 5, 9]
    # Every consumed object IS the produced object of the same cycle.
    assert all(
        produced is consumed
        for produced, consumed in zip(_produced_values, _consumed_values)
    )


def test_mixed_native_and_python_readers_share_the_cached_python_object():
    _produced_values.clear()
    _consumed_values.clear()

    @graph
    def g(t: TS[int]) -> TS[int]:
        value = _produce_tuple(t)
        hg.len_(value)  # A native reader forces canonical storage plus a cache.
        return _consume_tuple(value)

    assert eval_node(g, [1, 5, 9]) == [1, 5, 9]
    assert all(
        produced is consumed
        for produced, consumed in zip(_produced_values, _consumed_values)
    )


class _PythonOwnedRecord(CompoundScalar):
    value: int
    label: str


@compute_node
def _produce_python_owned_record(t: TS[int]) -> TS[_PythonOwnedRecord]:
    value = _PythonOwnedRecord(value=t.value, label=f"value-{t.value}")
    _produced_values.append(value)
    return value


@compute_node
def _consume_python_owned_record(value: TS[_PythonOwnedRecord]) -> TS[int]:
    _consumed_values.append(value.value)
    return value.value.value


def test_python_owned_bundle_uses_python_only_storage_for_python_only_edge():
    _produced_values.clear()
    _consumed_values.clear()

    @graph
    def g(t: TS[int]) -> TS[int]:
        return _consume_python_owned_record(_produce_python_owned_record(t))

    assert eval_node(g, [1, 5, 9]) == [1, 5, 9]
    assert all(
        produced is consumed
        for produced, consumed in zip(_produced_values, _consumed_values)
    )


def test_python_owned_bundle_mixed_storage_preserves_python_identity():
    _produced_values.clear()
    _consumed_values.clear()

    @graph
    def g(t: TS[int]) -> TS[int]:
        value = _produce_python_owned_record(t)
        hg.null_sink(value.label)
        return _consume_python_owned_record(value)

    assert eval_node(g, [1, 5, 9]) == [1, 5, 9]
    assert all(
        produced is consumed
        for produced, consumed in zip(_produced_values, _consumed_values)
    )


def test_python_owned_bundle_python_only_storage_normalizes_mapping_input():
    @compute_node
    def produce_mapping(t: TS[int]) -> TS[_PythonOwnedRecord]:
        return {"value": t.value, "label": f"value-{t.value}"}

    @compute_node
    def consume_record(value: TS[_PythonOwnedRecord]) -> TS[bool]:
        return type(value.value) is _PythonOwnedRecord

    @graph
    def g(t: TS[int]) -> TS[bool]:
        return consume_record(produce_mapping(t))

    assert eval_node(g, [1, 2]) == [True, True]


def test_native_output_conversion_is_cached_between_python_readers():
    _native_read_ids_a.clear()
    _native_read_ids_b.clear()

    @compute_node
    def read_a(value: TS[tuple[int, ...]]) -> TS[int]:
        _native_read_ids_a.append(id(value.value))
        return value.value[0]

    @compute_node
    def read_b(value: TS[tuple[int, ...]]) -> TS[int]:
        _native_read_ids_b.append(id(value.value))
        return value.value[0]

    @graph
    def g(value: TS[tuple[int, ...]]) -> TS[int]:
        read_a(value)
        return read_b(value)

    assert eval_node(g, [(1, 2), (3, 4)]) == [1, 3]
    assert _native_read_ids_a == _native_read_ids_b


def test_python_only_storage_normalizes_the_declared_read_shape():
    @compute_node
    def produce_list(t: TS[int]) -> TS[tuple[int, ...]]:
        return [t.value, t.value + 1]

    @compute_node
    def sees_tuple(value: TS[tuple[int, ...]]) -> TS[bool]:
        return type(value.value) is tuple

    @graph
    def g(t: TS[int]) -> TS[bool]:
        return sees_tuple(produce_list(t))

    assert eval_node(g, [1, 2]) == [True, True]


def test_python_only_storage_preserves_nested_integer_range_validation():
    @compute_node
    def produce_large_integer(_: TS[int]) -> TS[tuple[int, ...]]:
        return (1 << 80,)

    @compute_node
    def consume(value: TS[tuple[int, ...]]) -> TS[int]:
        return value.value[0]

    @graph
    def g(t: TS[int]) -> TS[int]:
        return consume(produce_large_integer(t))

    # Node evaluation translates boundary conversion failures to the runtime's
    # diagnostic wrapper; the important parity contract is rejection rather
    # than retaining a value that canonical Int storage cannot represent.
    with pytest.raises(RuntimeError):
        eval_node(g, [1])


def test_cheap_scalar_conversion_remains_current_without_a_cache():
    # The factory declines caching for cheap scalars. Each cycle must still
    # observe the current canonical value.
    @compute_node
    def double(t: TS[int]) -> TS[int]:
        return t.value * 2

    @compute_node
    def add_one(t: TS[int]) -> TS[int]:
        return t.value + 1

    @graph
    def g(t: TS[int]) -> TS[int]:
        return add_one(double(t))

    assert eval_node(g, [1, 2, 3, 4]) == [3, 5, 7, 9]


def test_set_results_stay_on_the_converted_path():
    # Sets are excluded from retained-Python storage: the converted read path
    # returns a frozenset (hgraph parity), never the raw mutable set.
    @compute_node
    def produce_set(t: TS[int]) -> TS[frozenset[int]]:
        return {t.value, t.value + 1}

    @compute_node
    def consume_set(v: TS[frozenset[int]]) -> TS[bool]:
        return type(v.value) is frozenset

    @graph
    def g(t: TS[int]) -> TS[bool]:
        return consume_set(produce_set(t))

    assert eval_node(g, [1, 2]) == [True, True]


def test_switch_teardown_destroys_nested_python_only_storage():
    # Each branch contains a Python-only edge. Branch replacement destroys that
    # output-owned holder with the nested node storage; reused child slots must
    # never serve the previous branch's tuple.
    @compute_node
    def branch_a_value(t: TS[int]) -> TS[tuple[int, ...]]:
        return (t.value + 100,)

    @compute_node
    def branch_b_value(t: TS[int]) -> TS[tuple[int, ...]]:
        return (t.value + 200,)

    @compute_node
    def read(v: TS[tuple[int, ...]]) -> TS[int]:
        return v.value[0]

    @graph
    def branch_a(_: TS[str], ts: TS[int]) -> TS[int]:
        return read(branch_a_value(ts))

    @graph
    def branch_b(_: TS[str], ts: TS[int]) -> TS[int]:
        return read(branch_b_value(ts))

    @graph
    def g(sel: TS[str], t: TS[int]) -> TS[int]:
        return hg.switch_(
            sel,
            {
                "a": lambda key, ts: branch_a(key, ts),
                "b": lambda key, ts: branch_b(key, ts),
            },
            t,
        )

    assert eval_node(g, ["a", None, "b", None, "a"], [1, 2, 3, 4, 5]) == [
        101,
        102,
        203,
        204,
        105,
    ]
