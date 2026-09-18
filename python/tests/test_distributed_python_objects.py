"""Python objects across a ``dmap_`` boundary (RFC 0040, stage 4).

A value that exists only as a Python object -- an instance of a class used as a
type, or anything behind ``object`` -- has no schema the binary codec can write
by, so its wire form is its pickle. That is never an error: there is no other
choice. It is slow, large and opaque to native code, though, so binding such a
boundary says so, once.

The importable children here also run in fresh worker interpreters.
"""
import logging
from dataclasses import dataclass

import pytest
import hgraph as hg
from hgraph import TS, TSD


@dataclass(frozen=True)
class Quote:
    symbol: str
    price: float


@hg.compute_node
def reprice(ts: TS[Quote]) -> TS[Quote]:
    return Quote(ts.value.symbol, ts.value.price * 2)


@hg.compute_node
def describe(ts: TS[object]) -> TS[object]:
    return {"type": type(ts.value).__name__, "value": ts.value}


def run(child, schema, events, in_process):
    @hg.graph
    def graph(ts: TSD[str, schema]) -> TSD[str, schema]:
        return hg.dmap_(child, ts, __workers__=2, in_process=in_process)
    return hg.eval_node(graph, events)


@pytest.mark.parametrize("in_process", [True, False])
def test_a_class_instance_crosses_the_boundary_both_ways(in_process):
    events = [{"a": Quote("VOD.L", 72.5)}, {"b": Quote("BP.L", 4.25), "a": Quote("VOD.L", 73.0)}]
    assert run(reprice, TS[Quote], events, in_process) == [
        {"a": Quote("VOD.L", 145.0)},
        {"b": Quote("BP.L", 8.5), "a": Quote("VOD.L", 146.0)},
    ]


class Plain:
    """An ordinary class: nothing the codec can write by, so it is pickled."""

    def __init__(self, n):
        self.n = n

    def __eq__(self, other):
        return isinstance(other, Plain) and other.n == self.n

    def __repr__(self):
        return f"Plain({self.n})"


@hg.compute_node
def increment(ts: TS[Plain]) -> TS[Plain]:
    return Plain(ts.value.n + 1)


@pytest.mark.parametrize("in_process", [True, False])
def test_anything_behind_object_crosses_the_boundary(in_process):
    # The bridge gives a value behind ``object`` a native form where it has one
    # (a tuple, a dict, a set, a scalar) and the codec writes that; only what
    # has none -- the Plain instance -- is pickled.
    events = [{"a": (1, "two", 3.0)}, {"a": {"nested": ("x", "y")}, "b": frozenset({1, 2})}, {"c": Plain(5)}]
    assert run(describe, TS[object], events, in_process) == [
        {"a": {"type": "tuple", "value": (1, "two", 3.0)}},
        {"a": {"type": "dict", "value": {"nested": ("x", "y")}}, "b": {"type": "frozenset", "value": frozenset({1, 2})}},
        {"c": {"type": "Plain", "value": Plain(5)}},
    ]


@pytest.mark.parametrize("in_process", [True, False])
def test_an_ordinary_class_is_pickled_and_says_so_once(in_process, capfd):
    events = [{"a": Plain(1)}, {"a": Plain(7), "b": Plain(2)}]
    assert run(increment, TS[Plain], events, in_process) == [{"a": Plain(2)}, {"a": Plain(8), "b": Plain(3)}]
    warnings = [line for line in capfd.readouterr().out.splitlines() if "which are pickled" in line]
    # Named in the author's terms -- the class -- and not once per value.
    assert all("Plain" in line for line in warnings)
    assert len(warnings) <= 2   # at most once per schema per process; a worker is its own process


@pytest.mark.parametrize("in_process", [True, False])
def test_a_dataclass_has_a_schema_and_is_not_pickled(in_process, capfd):
    assert run(reprice, TS[Quote], [{"a": Quote("VOD.L", 1.0)}], in_process) == [{"a": Quote("VOD.L", 2.0)}]
    assert "which are pickled" not in capfd.readouterr().out


def test_a_value_with_no_wire_form_fails_the_run_by_name():
    # A callable behind ``object`` is given the native ``callable`` scalar, which
    # nothing can carry to another process. The refusal names it. It cannot come
    # at wiring here, because ``object`` says nothing about what it will hold.
    @hg.compute_node
    def make_lambda(ts: TS[int]) -> TS[object]:
        return lambda: ts.value

    @hg.graph
    def graph(ts: TSD[str, TS[int]]) -> TSD[str, TS[object]]:
        return hg.dmap_(describe, hg.map_(make_lambda, ts), __workers__=1, in_process=True)

    with pytest.raises(Exception, match="scalar 'callable' has no wire form"):
        hg.eval_node(graph, [{"a": 1}])


def test_an_object_that_cannot_be_pickled_fails_the_run_by_name():
    class Local:   # pickle cannot name a class defined inside a function
        pass

    @hg.compute_node
    def make_local(ts: TS[int]) -> TS[object]:
        return Local()

    @hg.graph
    def graph(ts: TSD[str, TS[int]]) -> TSD[str, TS[object]]:
        return hg.dmap_(describe, hg.map_(make_local, ts), __workers__=1, in_process=True)

    with pytest.raises(Exception, match="(?i)pickle"):
        hg.eval_node(graph, [{"a": 1}])
