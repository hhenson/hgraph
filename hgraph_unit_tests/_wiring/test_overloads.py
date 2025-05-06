from typing import Tuple

import pytest

from hgraph import (
    compute_node,
    TIME_SERIES_TYPE,
    graph,
    TS,
    TSL,
    SIZE,
    Size,
    SCALAR,
    contains_,
    SCALAR_1,
    SCALAR_2,
    RequirementsNotMetWiringError,
    operator,
)
from hgraph.test import eval_node


def test_overloads():

    @operator
    def add(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE: ...

    @compute_node(overloads=add)
    def add_default(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
        return lhs.value + rhs.value

    @graph
    def t_add(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
        return add(lhs, rhs)

    @compute_node(overloads=add)
    def add_ints(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return lhs.value + rhs.value + 1

    @compute_node(overloads=add)
    def add_strs(lhs: TS[str], rhs: TS[str]) -> TS[str]:
        return lhs.value + rhs.value + "~"

    @graph(overloads=add)
    def add_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
        return TSL.from_ts(*[a + b for a, b in zip(lhs, rhs)])

    assert eval_node(t_add[TIME_SERIES_TYPE : TS[int]], lhs=[1, 2, 3], rhs=[1, 5, 7]) == [3, 8, 11]
    assert eval_node(t_add[TIME_SERIES_TYPE : TS[float]], lhs=[1.0, 2.0, 3.0], rhs=[1.0, 5.0, 7.0]) == [2.0, 7.0, 10.0]
    assert eval_node(t_add[TIME_SERIES_TYPE : TS[str]], lhs=["1.", "2.", "3."], rhs=["1.", "5.", "7."]) == [
        "1.1.~",
        "2.5.~",
        "3.7.~",
    ]
    assert eval_node(t_add[TIME_SERIES_TYPE : TSL[TS[int], Size[2]]], lhs=[(1, 1)], rhs=[(2, 2)]) == [{0: 3, 1: 3}]


def test_scalar_overloads():

    @operator
    def add(lhs: TS[SCALAR], rhs: SCALAR) -> TS[SCALAR]: ...

    @compute_node(overloads=add)
    def add_default(lhs: TS[SCALAR], rhs: SCALAR) -> TS[SCALAR]:
        return lhs.value + rhs

    @graph
    def t_add(lhs: TIME_SERIES_TYPE, rhs: SCALAR) -> TIME_SERIES_TYPE:
        return add(lhs, rhs)

    @compute_node(overloads=add)
    def add_ints(lhs: TS[int], rhs: int) -> TS[int]:
        return lhs.value + rhs + 1

    @compute_node(overloads=add)
    def add_ints(lhs: TS[int], rhs: SCALAR) -> TS[int]:
        return lhs.value + rhs + 2

    @compute_node(overloads=add)
    def add_strs(lhs: TS[str], rhs: str) -> TS[str]:
        return lhs.value + rhs + "~"

    @compute_node(overloads=add)
    def add_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: Tuple[SCALAR, ...]) -> TSL[TIME_SERIES_TYPE, SIZE]:
        return tuple(a.value + b for a, b in zip(lhs.values(), rhs))

    assert eval_node(t_add[TIME_SERIES_TYPE : TS[int]], lhs=[1, 2, 3], rhs=1) == [3, 4, 5]
    assert eval_node(t_add[TIME_SERIES_TYPE : TS[float], SCALAR:float], lhs=[1.0, 2.0, 3.0], rhs=1.0) == [2.0, 3.0, 4.0]
    assert eval_node(t_add[TIME_SERIES_TYPE : TS[str]], lhs=["1.", "2.", "3."], rhs=".") == ["1..~", "2..~", "3..~"]
    assert eval_node(t_add[TIME_SERIES_TYPE : TSL[TS[int], Size[2]]], lhs=[(1, 1)], rhs=(2, 2)) == [{0: 3, 1: 3}]


def test_contains():
    @graph
    def main(ts: TS[frozenset[int]], item: TS[int]) -> TS[bool]:
        return contains_(ts, item)

    assert eval_node(main, [frozenset({1}), frozenset({1, 2}), frozenset({3})], [2]) == [False, True, False]


def test_requires():
    @compute_node(requires=lambda m, kw: m[SCALAR] != m[SCALAR_1])
    def add(lhs: TS[SCALAR], rhs: TS[SCALAR_1]) -> TS[SCALAR]:
        return lhs.value + type(lhs.value)(rhs.value)

    assert eval_node(add[SCALAR:int, SCALAR_1:float], 1, 2.0) == [3]
    with pytest.raises(RequirementsNotMetWiringError):
        assert eval_node(add[SCALAR:int, SCALAR_1:int], 1, 2) == [3]
    assert eval_node(add[SCALAR:float, SCALAR_1:int], 1.0, 2) == [3.0]
