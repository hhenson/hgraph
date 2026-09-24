"""A type is a runtime value (RFC 0042).

The native scalar ``type`` (RFC 0033's ``TypeCarrier``) crosses into Python
as what a type argument crosses as -- a time-series type, the Python class of
a scalar type -- and back from whatever a type-argument slot accepts. Its
text is its name.
"""

import datetime

import _hgraph

import hgraph as hg
from hgraph import TS, compute_node, graph
from hgraph.test import eval_node

# The native type-value scalar. Python has no annotation of its own for it
# (``type`` is a Python object); a native schema's field is where users meet
# it, so the tests name it directly.
TYPE = _hgraph.value_type("type")


@compute_node
def _pick(x: TS[int]) -> TS[TYPE]:
    return (int, datetime.date, TS[float])[x.value]


@compute_node
def _describe(t: TS[TYPE]) -> TS[str]:
    value = t.value
    return value.__name__ if isinstance(value, type) else repr(value)


def test_a_type_value_crosses_into_and_out_of_python():
    @graph
    def g(x: TS[int]) -> TS[str]:
        return _describe(_pick(x))

    assert eval_node(g, [0, 1, 2]) == ["int", "date", "TS[float]"]


def test_a_types_text_is_its_name():
    @graph
    def g(x: TS[int]) -> TS[str]:
        return hg.str_(_pick(x))

    assert eval_node(g, [0, 1, 2]) == ["int", "date", "TS[float]"]


@compute_node
def _composite(x: TS[int]) -> TS[TYPE]:
    return tuple[int, ...]


@compute_node
def _plain(x: TS[int]) -> TS[int]:
    return x.value


def _locks_per_extra_ticks(g):
    """Type-system lock acquisitions over 50 extra ticks: the graph is built
    for both runs, so only per-tick work differs (the output is dropped, so
    the harness converts nothing back)."""
    from hgraph.debug import runtime_registry_snapshot

    eval_node(g, [1])
    counts = []
    for ticks in (50, 100):
        before = runtime_registry_snapshot().type_system_lock_acquisitions
        eval_node(g, list(range(ticks)))
        counts.append(runtime_registry_snapshot().type_system_lock_acquisitions - before)
    return counts[1] - counts[0]


def test_emitting_a_composite_type_every_tick_takes_no_type_system_locks():
    @graph
    def emits_types(x: TS[int]):
        hg.null_sink(_composite(x))

    @graph
    def emits_ints(x: TS[int]):
        hg.null_sink(_plain(x))

    assert _locks_per_extra_ticks(emits_types) == _locks_per_extra_ticks(emits_ints)
