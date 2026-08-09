from typing import Callable

from hgraph import apply, call, graph, round_, TS
from hgraph.test import eval_node



def test_round():

    assert eval_node(round_, [1.23456789, 1.235], [2]) == [1.23, 1.24]


def test_call():

    @graph
    def g(v: TS[str]):
        call(print, v)

    eval_node(g, ["hello", "world"])


def test_apply_dynamic_callable_with_positional_arguments():
    def add(lhs: int, rhs: int) -> int:
        return lhs + rhs

    def multiply(lhs: int, rhs: int) -> int:
        return lhs * rhs

    @graph
    def g(fn: TS[Callable], lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return apply[TS[int]](fn, lhs, rhs)

    assert eval_node(g, [add, multiply], [2, 3], [4, 5]) == [6, 15]


def test_apply_skips_invalid_keyword_arguments():
    def scaled_add(lhs: int, rhs: int, *, scale: int = 1) -> int:
        return (lhs + rhs) * scale

    @graph
    def g(fn: TS[Callable], lhs: TS[int], rhs: TS[int], scale: TS[int]) -> TS[int]:
        return apply[TS[int]](fn, lhs, rhs, scale=scale)

    assert eval_node(g, [scaled_add], [1, 2], [10, 20], [None, 3]) == [11, 66]


def test_apply_infers_annotated_plain_callable_output():
    def add(lhs: int, rhs: int) -> int:
        return lhs + rhs

    @graph
    def g(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return apply(add, lhs, rhs)

    assert eval_node(g, [1, 2], [3, 4]) == [4, 6]


def test_call_passes_positional_and_keyword_arguments():
    seen = []

    def observe(value: int, *, label: str):
        seen.append((label, value))

    @graph
    def g(value: TS[int], label: TS[str]):
        call(observe, value, label=label)

    eval_node(g, [1, 2], ["a", "b"])
    assert seen == [("a", 1), ("b", 2)]


def test_apply_preserves_keyword_named_like_an_internal_positional_field():
    def read_keyword(**kwargs) -> int:
        return kwargs["_0"]

    @graph
    def g(value: TS[int]) -> TS[int]:
        return apply(read_keyword, _0=value)

    assert eval_node(g, [7]) == [7]
