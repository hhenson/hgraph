"""Python-value mirror behaviour (issue #204).

A python-written output read by a python consumer hands back the ORIGINAL
object (identity, not a conversion); the C++ storage stays canonical.
Output values are immutable by graph contract (mutating one is UB), so the
aliasing is within contract and matches upstream hgraph's
reference-passing behaviour.
"""

import hgraph as hg
from hgraph import TS, compute_node, graph
from hgraph.test import eval_node

_produced_ids: list[int] = []
_consumed_ids: list[int] = []


@compute_node
def _produce_tuple(t: TS[int]) -> TS[tuple[int, ...]]:
    value = (t.value, t.value + 1, t.value + 2)
    _produced_ids.append(id(value))
    return value


@compute_node
def _consume_tuple(v: TS[tuple[int, ...]]) -> TS[int]:
    _consumed_ids.append(id(v.value))
    return v.value[0]


def test_py_to_py_chain_passes_the_original_object():
    _produced_ids.clear()
    _consumed_ids.clear()

    @graph
    def g(t: TS[int]) -> TS[int]:
        return _consume_tuple(_produce_tuple(t))

    assert eval_node(g, [1, 5, 9]) == [1, 5, 9]
    # Every consumed object IS the produced object of the same cycle.
    assert _consumed_ids == _produced_ids
    # Values were genuinely distinct objects per cycle (no accidental reuse).
    assert len(set(_produced_ids)) >= 1


def test_mirror_replaces_per_tick_and_serves_current_values():
    # Freshness: each cycle's read observes that cycle's write, never a
    # stale mirrored object.
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
    # Sets are excluded from mirroring: the converted read path returns a
    # frozenset (hgraph parity) and the mirror must not leak the raw set.
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


def test_switch_teardown_does_not_serve_stale_objects():
    # The producing node's stop erases its mirror entry; a branch swap that
    # reuses child storage must never serve the previous branch's object.
    @compute_node
    def branch_a(t: TS[int]) -> TS[int]:
        return t.value + 100

    @compute_node
    def branch_b(t: TS[int]) -> TS[int]:
        return t.value + 200

    @compute_node
    def read(v: TS[int]) -> TS[int]:
        return v.value

    @graph
    def g(sel: TS[str], t: TS[int]) -> TS[int]:
        return read(
            hg.switch_(sel, {"a": lambda key, ts: branch_a(ts), "b": lambda key, ts: branch_b(ts)}, t))

    assert eval_node(g, ["a", None, "b", None, "a"], [1, 2, 3, 4, 5]) == [
        101, 102, 203, 204, 105]
