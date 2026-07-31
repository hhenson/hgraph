"""breakpoint_ (hgraph.test API, ported 2026-07-31): each overload fires
python's breakpoint() on tick and passes the value through unchanged."""

import builtins

import pytest

from hgraph import TS, graph
from hgraph.test import breakpoint_, eval_node


def _capture(monkeypatch):
    calls = []
    monkeypatch.setattr(builtins, "breakpoint", lambda *a, **k: calls.append(1))
    return calls


def test_breakpoint_ts_fires_and_passes_through(monkeypatch):
    calls = _capture(monkeypatch)

    @graph
    def g(t: TS[int]) -> TS[int]:
        return breakpoint_(t)

    assert eval_node(g, [1, 2, 3]) == [1, 2, 3]
    assert len(calls) == 3


def test_breakpoint_conditional_fires_only_when_true(monkeypatch):
    calls = _capture(monkeypatch)

    @graph
    def g(condition: TS[bool], t: TS[int]) -> TS[int]:
        return breakpoint_(condition, t)

    assert eval_node(g, [None, False, True], [1, 2, 3]) == [1, 2, 3]
    assert len(calls) == 1


@pytest.mark.xfail(
    reason="issue #224: variadic **kwargs nodes cannot return TSB[TS_SCHEMA]",
    strict=True)
def test_breakpoint_many_fires_per_modification(monkeypatch):
    calls = _capture(monkeypatch)

    @graph
    def g(a: TS[int], b: TS[int]) -> TS[int]:
        bundle = breakpoint_(a=a, b=b)
        return bundle.a

    assert eval_node(g, [1, None, 3], [None, 4, None]) == [1, None, 3]
    assert len(calls) == 3
