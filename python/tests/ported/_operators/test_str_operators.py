from typing import Tuple

from hgraph import match_
from hgraph import (
    REMOVE,
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


def test_split_target_shape_chooses_the_arity_contract():
    """The declared target chooses the contract (issue #810 item 4.9).

    A FIXED ``TSL[..., Size[N]]`` means exactly N parts. Filling only part of a
    declared arity and leaving the rest unset was the divergence -- released
    hgraph raises, as the fixed TUPLE target already did here. More parts than
    N put the remainder in the last slot, which is ``str.split(maxsplit=N-1)``
    and still yields N; both runtimes already agreed on that.

    A DYNAMIC ``TSL[..., Size[-1]]`` takes as many parts as there are, and
    tracks the count. It previously used the list's CURRENT length as the split
    bound, so once the first tick fixed the length every later tick was capped
    at it and a shorter input left stale trailing elements behind.
    """
    import pytest

    @graph
    def fixed_three(s: TS[str]) -> TSL[TS[str], Size[3]]:
        return split[TSL[TS[str], Size[3]]](s, ",")

    with pytest.raises(Exception, match="fixed list arity"):
        eval_node(fixed_three, ["a,b"])

    @graph
    def dynamic(s: TS[str]) -> TSL[TS[str], Size[-1]]:
        return split[TSL[TS[str], Size[-1]]](s, ",")

    # Growing: the second tick is not capped at the first tick's length.
    assert eval_node(dynamic, ["a,b", "a,b,c"]) == [{0: "a", 1: "b"}, {2: "c"}]
    # Truncating: the elements that go are removed, not left behind.
    assert eval_node(dynamic, ["a,b,c", "x"]) == [
        {0: "a", 1: "b", 2: "c"},
        {0: "x", 1: REMOVE, 2: REMOVE},
    ]


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


def test_format_uses_python_boolean_spelling():
    @graph
    def app(value: TS[bool]) -> TS[str]:
        return format_("value={}", value)

    assert eval_node(app, [True, False]) == ["value=True", "value=False"]


def test_substr():
    assert eval_node(substr, ["abcdef"], [0], [3]) == ["abc"]
    assert eval_node(substr, ["abcdef"], [2], [4]) == ["cd"]
    assert eval_node(substr, ["abcdef"], [1], [5]) == ["bcde"]
