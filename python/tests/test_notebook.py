"""Tests for ``hgraph.notebook`` — interactive sessions over snapshot-run.

Design record: ``docs/source/developer_guide/notebook.rst``.
"""

from datetime import datetime

import pytest

from hgraph import const
from hgraph.notebook import EvalResult, current_session, session


@pytest.fixture()
def nb():
    nb = session()
    yield nb
    nb.close()


def test_incremental_eval_grows_the_graph(nb):
    c = const(42)
    first = c.eval()
    assert first.values == [42]
    assert all(isinstance(when, datetime) for when, _ in first)

    # the SAME session keeps accepting nodes; eval re-runs the grown graph
    d = c + 1
    assert d.eval().values == [43]

    # ports from earlier "cells" still evaluate
    assert c.eval().values == [42]


def test_record_sink_is_reused_per_port(nb):
    c = const(1)
    c.eval()
    c.eval()
    c.eval()
    d = const(2)
    d.eval()
    # 3 evals of c + 1 of d wire exactly TWO record sinks (keyed per port),
    # not one per eval (the upstream leak this design removes).
    assert len(nb._recorded) == 2


def test_eval_result_repr_html(nb):
    result = const("hi").eval()
    assert isinstance(result, EvalResult)
    html = result._repr_html_()
    assert html.startswith("<table>") and "hi" in html


def test_session_reset_discards_the_graph(nb):
    c = const(5)
    assert c.eval().values == [5]
    nb.reset()
    # fresh graph after reset; new wiring accepts nodes
    d = const(6)
    assert d.eval().values == [6]
    # sink map was cleared with the graph
    assert len(nb._recorded) == 1


def test_session_close_stops_ambient_wiring():
    nb = session()
    const(1)
    nb.close()
    with pytest.raises(RuntimeError, match="no active wiring"):
        const(2)
    with pytest.raises(RuntimeError, match="no notebook session"):
        current_session()


def test_session_call_resets_existing():
    first = session()
    second = session()   # re-invoking resets rather than stacking
    assert current_session() is second
    assert first._wiring is None  # the first was closed
    second.close()


def test_eval_requires_a_port(nb):
    with pytest.raises(TypeError, match="wired port"):
        nb.eval(42)


def test_eval_without_session_raises_helpfully():
    c_holder = {}
    nb = session()
    c_holder["port"] = const(1)
    nb.close()
    with pytest.raises(RuntimeError, match="no notebook session"):
        c_holder["port"].eval()


def test_plot_smoke(nb):
    matplotlib = pytest.importorskip("matplotlib")
    matplotlib.use("Agg")

    result = const(1.5).plot(title="t", ylabel="y")
    assert result.values == [1.5]
