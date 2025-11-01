from dataclasses import dataclass

import pytest

from hgraph import (
    compute_node,
    CONTEXT,
    TS,
    graph,
    log_,
    switch_,
    REQUIRED,
    TSD,
    map_,
    pass_through,
    WiringError,
    TIME_SERIES_TYPE,
    format_,
    const,
    CompoundScalar,
    TSB,
    REF,
    lag,
    SCHEDULER,
    MIN_TD,
    combine,
    try_except,
)
from hgraph.test import eval_node

import pytest
pytestmark = pytest.mark.smoke


class _TestContext:
    __instance__ = None

    def __init__(self, msg: str = "non-default"):
        self.msg = msg

    @classmethod
    def instance(cls):
        if _TestContext.__instance__ is None:
            return _TestContext("default")
        return _TestContext.__instance__

    def __enter__(self):
        _TestContext.__instance__ = self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if _TestContext.__instance__ == self:
            _TestContext.__instance__ = None
        else:
            raise ValueError("Exiting context not entered.")


def test_context():
    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[TS[_TestContext]] = None) -> TS[str]:
        return f"{_TestContext.instance().msg} {ts.value}"

    @graph
    def g(ts: TS[bool]) -> TS[str]:
        with const(_TestContext("Hello")):
            return use_context(ts)

    assert eval_node(g, [True, None, False]) == ["Hello True", None, "Hello False"]


def test_no_context():
    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[TS[_TestContext]] = None) -> TS[str]:
        return f"{_TestContext.instance().msg} {ts.value}"

    @graph
    def g(ts: TS[bool]) -> TS[str]:
        return use_context(ts)

    assert eval_node(g, [True, None, False]) == ["default True", None, "default False"]


def test_no_context_but_required():
    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[TS[_TestContext]] = REQUIRED) -> TS[str]:
        return f"{_TestContext.instance().msg} {ts.value}"

    @graph
    def g(ts: TS[bool]) -> TS[str]:
        return use_context(ts)

    from hgraph import WiringError

    with pytest.raises(WiringError):
        eval_node(g, [True, None, False])


def test_context_scalar():
    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[_TestContext] = None) -> TS[str]:
        return f"{_TestContext.instance().msg} {ts.value}"

    @graph
    def g(ts: TS[bool]) -> TS[str]:
        with const(_TestContext("Hello")) as z:
            return use_context(ts)

    assert eval_node(g, [True, None, False]) == ["Hello True", None, "Hello False"]


def test_context_scalar_named():
    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[_TestContext] = REQUIRED["a"]) -> TS[str]:
        return f"{_TestContext.instance().msg}"

    @graph
    def g(ts: TS[bool]) -> TS[str]:
        with const(_TestContext("Hello_A")) as a:
            with const(_TestContext("Hello_Z")) as z:
                return format_("{} {}", use_context(ts), use_context(ts, context="z"))

    assert eval_node(g, [True, None, False]) == ["Hello_A Hello_Z", None, "Hello_A Hello_Z"]


def test_context_scalar_named_required():
    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[_TestContext] = None) -> TS[str]:
        return f"{_TestContext.instance().msg}"

    @graph
    def g(ts: TS[bool]) -> TS[str]:
        return use_context(ts, context=REQUIRED["B"])

    with pytest.raises(WiringError, match="with name B"):
        eval_node(g, [True, None, False])


def test_context_scalar_named_default():
    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[_TestContext] = None) -> TS[str]:
        return f"{_TestContext.instance().msg}"

    @graph
    def g(ts: TS[bool]) -> TS[str]:
        return use_context(ts, context="B")

    assert eval_node(g, [True, None, False]) == ["default", None, "default"]


def test_context_bundle():
    @dataclass(frozen=True)
    class ContextStruct(CompoundScalar, _TestContext):
        a: int
        msg: str = "bundle"

    @compute_node(valid=("ts", "context"))
    def use_context(ts: TS[bool], context: CONTEXT[_TestContext] = None) -> TS[str]:
        return f"{_TestContext.instance().msg}"

    @graph
    def g(ts: TS[bool]) -> TS[str]:
        with combine[TSB[ContextStruct]](a=1, msg="bundle"):
            return use_context(ts)

    assert eval_node(g, [True, None, False]) == ["bundle", None, "bundle"]


def test_context_ranking():
    @compute_node
    def create_context(msg: TS[str]) -> TS[_TestContext]:
        return _TestContext(msg.value)

    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[TS[_TestContext]] = None) -> TS[str]:
        return f"{_TestContext.instance().msg} {ts.value}"

    @graph
    def g(ts: TS[bool], s: TS[str]) -> TS[str]:
        with create_context(
            format_("{}_", s)
        ):  # Now the context node would rank lower than use_context if they were not linked
            return use_context(ts)

    assert eval_node(g, ts=[True, None, False], s=["Hello", None, "Hulla"]) == ["Hello_ True", None, "Hulla_ False"]


def test_context_over_switch():
    @compute_node
    def create_context(msg: TS[str]) -> TS[_TestContext]:
        return _TestContext(msg.value)

    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[TS[_TestContext]] = REQUIRED) -> TS[str]:
        return f"{_TestContext.instance().msg} {ts.value}"

    @graph
    def g(ts: TS[bool], s: TS[str]) -> TS[str]:
        with create_context(format_("{}_", s)):
            return switch_(ts, {True: lambda t: use_context(t), False: lambda t: format_("Chao {}", t)}, ts)

    assert eval_node(g, ts=[True, None, False], s=["Hello", None, "Hulla"]) == [
        "Hello_ True",
        None,
        "Chao False",
    ]


def test_context_over_switch_inside_map():
    @compute_node
    def create_context(msg: TS[str]) -> TS[_TestContext]:
        return _TestContext(msg.value)

    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[TS[_TestContext]] = REQUIRED) -> TS[str]:
        return f"{_TestContext.instance().msg} {ts.value}"

    @graph
    def g(ts: TS[bool], s: TS[str]) -> TS[str]:
        with create_context(format_("{}_", s)):
            return switch_(ts, {True: lambda t: use_context(t), False: lambda t: format_("Chao {}", t)}, ts)

    @graph
    def f(ts: TSD[int, TS[bool]], s: TSD[int, TS[str]]) -> TSD[int, TS[str]]:
        return map_(g, ts, s)

    @graph
    def h(ts: TSD[int, TSD[int, TS[bool]]], s: TSD[int, TS[str]]) -> TSD[int, TSD[int, TS[str]]]:
        return map_(f, ts, pass_through(s))

    assert eval_node(
        h,
        ts=[{1: {1: True}, 2: {2: True}}, {1: {1: False}}, None],
        s=[{1: "Hello", 2: "Chao"}, None, {2: "Ho"}],
        # __trace__={"start": False},
    ) == [{1: {1: "Hello_ True"}, 2: {2: "Chao_ True"}}, {1: {1: "Chao False"}}, {2: {2: "Ho_ True"}}]


def test_context_not_context_manager():
    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[TIME_SERIES_TYPE] = REQUIRED["context"]) -> TS[str]:
        # Sort dict items to ensure consistent ordering across implementations (C++ vs Python)
        sorted_items = sorted(dict(context.value).items())
        sorted_dict = "{" + ", ".join(f"{k}: {v}" for k, v in sorted_items) + "}"
        return f"{sorted_dict} {ts.value}"

    @graph
    def f(ts: TS[bool]) -> TS[str]:
        return use_context(ts)

    @graph
    def g(ts: TS[bool], c: TSD[int, TS[int]]) -> TS[str]:
        with c as context:
            return f(ts)

    # Expected results use sorted dictionary keys to be implementation-agnostic
    assert eval_node(g, [True, None, False], [{1: 1}, {2: 2}, None]) == [
        "{1: 1} True",
        "{1: 1, 2: 2} True",  # Note: sorted order (1, 2) not insertion order
        "{1: 1, 2: 2} False",
    ]


def test_two_contexts():
    @compute_node
    def use_context(a: CONTEXT[TIME_SERIES_TYPE] = "a", b: CONTEXT[TIME_SERIES_TYPE] = "b") -> TS[str]:
        return f"{a.value} {b.value}"

    @graph
    def g(ts1: TS[str], ts2: TS[str]) -> TS[str]:
        with ts1 as a, ts2 as b:
            return use_context()

    assert eval_node(g, ["Hello", None], [None, "World"]) == [None, "Hello World"]


def test_context_wired_explicitly():
    @compute_node
    def use_context(a: CONTEXT[TIME_SERIES_TYPE] = REQUIRED["a"]) -> TS[str]:
        return f"{a.value}"

    @graph
    def g(ts1: TS[str]) -> TS[str]:
        return use_context(ts1)

    assert eval_node(g, ["Hello", None]) == ["Hello", None]


def test_graph_contexts():
    @graph
    def use_context(a: CONTEXT[TIME_SERIES_TYPE] = "a", b: CONTEXT[TIME_SERIES_TYPE] = "b") -> TS[str]:
        return format_("{} {}", a, b, __strict__=False)

    @graph
    def f() -> TS[str]:
        return use_context()

    @graph
    def g(ts1: TS[str], ts2: TS[str]) -> TS[str]:
        with ts1 as a, ts2 as b:
            return f()

    assert eval_node(g, ["Hello", None], [None, "World"]) == ["Hello None", "Hello World"]


def test_stacked_contexts():
    @compute_node
    def use_context(a: CONTEXT[TIME_SERIES_TYPE] = REQUIRED["a"]) -> TS[str]:
        return f"{a.value}"

    @graph
    def f() -> TS[str]:
        return use_context()

    @graph
    def h(ts1: TS[str], ts2: TS[str]) -> TS[str]:
        with ts2 as a:
            return try_except(f).out

    @graph
    def g(ts1: TS[str], ts2: TS[str]) -> TS[str]:
        with ts1 as a:
            return try_except(h, ts1, ts2).out

    assert eval_node(g, ["Hello", None], [None, "World"]) == [None, "World"]


def test_named_context_missing():
    @graph
    def use_context(a: CONTEXT[TS[str]] = "a") -> TS[str]:
        return a

    @graph
    def g(ts1: TS[str]) -> TS[str]:
        return use_context()

    assert eval_node(g, ["Hello", None]) == None
