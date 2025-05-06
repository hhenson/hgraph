from dataclasses import dataclass
from typing import Generic, TypeVar
from hgraph import SCALAR, TS, CompoundScalar, graph
from hgraph.test import eval_node


def test_recursive_scalar():
    @dataclass
    class RecursiveCompoundScalar(CompoundScalar):
        w: int
        x: "RecursiveCompoundScalar"

    r = RecursiveCompoundScalar(1, RecursiveCompoundScalar(2, None))
    assert r.w == 1
    assert r.x.w == 2
    assert r.x.x is None


T = TypeVar("T")


def test_recursive_generic_scalar():
    @dataclass
    class RecursiveCompoundScalarT(CompoundScalar, Generic[T]):
        w: T
        x: "RecursiveCompoundScalarT"

    r = RecursiveCompoundScalarT[int](1, RecursiveCompoundScalarT[int](2, None))
    assert r.w == 1
    assert r.x.w == 2
    assert r.x.x is None


def test_recursive_scalar_in_graph():
    @dataclass
    class RecursiveCompoundScalarG(CompoundScalar):
        w: int
        x: "RecursiveCompoundScalarG"

    @graph
    def g(ts: TS[RecursiveCompoundScalarG]) -> TS[int]:
        return ts.x.w

    r = RecursiveCompoundScalarG(1, RecursiveCompoundScalarG(2, None))

    assert eval_node(g, [r]) == [2]


def test_recursive_generic_scalar_in_graph():
    @dataclass
    class RecursiveCompoundScalarGT(CompoundScalar, Generic[SCALAR]):
        w: SCALAR
        x: "RecursiveCompoundScalarGT"

    @graph
    def g(ts: TS[RecursiveCompoundScalarGT[int]]) -> TS[int]:
        return ts.x.w

    r = RecursiveCompoundScalarGT[int](1, RecursiveCompoundScalarGT[int](2, None))

    assert eval_node(g, [r]) == [2]


def test_recursive_scalar_2():
    @dataclass
    class RecursiveCompoundScalar2(CompoundScalar):
        w: int
        x: "AnotherRecursiveCompoundScalar"

    @dataclass
    class AnotherRecursiveCompoundScalar(CompoundScalar):
        w: int
        x: "RecursiveCompoundScalar2"

    @graph
    def g(ts: TS[RecursiveCompoundScalar2]) -> TS[int]:
        return ts.x.x.w

    r = RecursiveCompoundScalar2(1, AnotherRecursiveCompoundScalar(2, RecursiveCompoundScalar2(3, None)))

    assert eval_node(g, [r]) == [3]
