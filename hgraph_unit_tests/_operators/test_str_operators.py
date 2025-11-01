from typing import Tuple

from hgraph import match_
from hgraph import (
    mul_,
    contains_,
    TS,
    graph,
    format_,
    TIME_SERIES_TYPE_2,
    TIME_SERIES_TYPE_1,
    TIME_SERIES_TYPE,
    replace,
    split,
    TSL,
    Size,
    join,
    substr,
)
from hgraph.test import eval_node

import pytest
pytestmark = pytest.mark.smoke


def test_mul_str():
    assert eval_node(mul_, ["abc"], [3]) == ["abcabcabc"]


def test_contains_str():
    @graph
    def app(lhs: TS[str], rhs: TS[str]) -> TS[bool]:
        return contains_(lhs, rhs)

    assert eval_node(app, ["abc", None, ""], ["z", "bc", ""]) == [False, True, True]


def test_match():
    assert eval_node(match_, pattern=["a"], s=["a"]) == [{"is_match": True, "groups": ()}]
    assert eval_node(match_, pattern=["(a)"], s=["a"]) == [{"is_match": True, "groups": ("a",)}]
    assert eval_node(match_, pattern=["(a)"], s=["aa"]) == [{"is_match": True, "groups": ("a",)}]
    assert eval_node(match_, pattern=["(a)"], s=["aa"]) == [{"is_match": True, "groups": ("a",)}]
    assert eval_node(match_, pattern=["a"], s=["baa"]) == [{"is_match": True, "groups": ()}]
    assert eval_node(match_, pattern=["a"], s=["b"]) == [{"is_match": False}]


def test_replace():
    assert eval_node(replace, ["a"], ["z"], ["abcabcabc"]) == ["zbczbczbc"]
    assert eval_node(replace, ["^a"], ["z"], ["abcabcabc"]) == ["zbcabcabc"]


def test_split():
    @graph
    def g(s: TS[str], separator: str) -> TS[Tuple[str, ...]]:
        return split(s, separator)

    assert eval_node(g, ["a,b,c"], ",") == [("a", "b", "c")]

    @graph
    def h(s: TS[str], separator: str) -> TS[Tuple[str, str]]:
        return split[TS[Tuple[str, str]]](s, separator)

    assert eval_node(h, ["a,b,c"], ",") == [("a", "b,c")]

    @graph
    def f(s: TS[str], separator: str) -> TIME_SERIES_TYPE:
        return split[TSL[TS[str], Size[2]]](s, separator)

    assert eval_node(f, ["a,b,c"], ",") == [{0: "a", 1: "b,c"}]


def test_join():
    @graph
    def g(s: TSL[TS[str], Size[3]]) -> TS[str]:
        return join(*s, separator=",")

    assert eval_node(g, [("a", "b", "c")]) == ["a,b,c"]

    @graph
    def g(s: TS[Tuple[str, ...]]) -> TS[str]:
        return join(s, separator=",")

    assert eval_node(g, [("a", "b", "c")]) == ["a,b,c"]


def test_join_strict():
    @graph
    def g(s: TSL[TS[str], Size[3]]) -> TS[str]:
        return join(*s, separator=",", __strict__=True)

    assert eval_node(g, [("a", None, "c"), ("a", "b", "c")]) == [None, "a,b,c"]


def test_join_not_strict():
    @graph
    def g(s: TSL[TS[str], Size[3]]) -> TS[str]:
        return join(*s, separator=",", __strict__=False)

    assert eval_node(g, [("a", None, "c"), ("a", "b", "c")]) == ["a,c", "a,b,c"]


def test_format_args():
    @graph
    def format_test(format_str: TS[str], ts1: TIME_SERIES_TYPE_1, ts2: TIME_SERIES_TYPE_2) -> TS[str]:
        return format_(format_str, ts1, ts2)

    f_str = "{} is a test {}"
    ts1 = [1, 2]
    ts2 = ["a", "b"]

    expected = [f_str.format(ts1, ts2) for ts1, ts2 in zip(ts1, ts2)]

    assert eval_node(format_test, [f_str], ts1, ts2) == expected


def test_format_kwargs():
    @graph
    def format_test(format_str: TS[str], ts1: TIME_SERIES_TYPE_1, ts2: TIME_SERIES_TYPE_2) -> TS[str]:
        return format_(format_str, ts1=ts1, ts2=ts2)

    f_str = "{ts1} is a test {ts2}"
    ts1 = [1, 2]
    ts2 = ["a", "b"]

    expected = [f_str.format(ts1=ts1, ts2=ts2) for ts1, ts2 in zip(ts1, ts2)]

    assert eval_node(format_test, [f_str], ts1, ts2) == expected


def test_format_mixed():
    @graph
    def format_test(
        format_str: TS[str], ts: TIME_SERIES_TYPE, ts1: TIME_SERIES_TYPE_1, ts2: TIME_SERIES_TYPE_2
    ) -> TS[str]:
        return format_(format_str, ts, ts1=ts1, ts2=ts2)

    f_str = "{ts1} is a test {ts2}"
    ts = [1.1, 1.2]
    ts1 = [1, 2]
    ts2 = ["a", "b"]

    expected = [f_str.format(ts, ts1=ts1, ts2=ts2) for ts, ts1, ts2 in zip(ts, ts1, ts2)]

    assert eval_node(format_test, [f_str], ts, ts1, ts2) == expected


def test_format_sampled():
    @graph
    def format_test(format_str: TS[str], ts1: TIME_SERIES_TYPE_1, ts2: TIME_SERIES_TYPE_2) -> TS[str]:
        return format_(format_str, ts1, ts2, __sample__=3)

    f_str = "{} is a test {}"
    ts1 = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    ts2 = ["a", "b", "c", "d", "e", "f", "g", "h", "i"]

    expected = [None if (ndx + 1) % 3 != 0 else f_str.format(ts1, ts2) for ndx, (ts1, ts2) in enumerate(zip(ts1, ts2))]

    assert eval_node(format_test, [f_str], ts1, ts2) == expected


def test_substr():
    assert eval_node(substr, ["abcdef"], [0], [3]) == ["abc"]
    assert eval_node(substr, ["abcdef"], [2], [4]) == ["cd"]
    assert eval_node(substr, ["abcdef"], [1], [5]) == ["bcde"]
