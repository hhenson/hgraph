"""The two directions of a TSD delta (RFC 0017 prerequisite).

An OBSERVED delta is ``{removed, modified}``; the AUTHORED one adds
``removed_strict`` for a user returning ``REMOVE``. Conversion escalates to the
authored shape only when strict intent is actually present, because the
observed shape is what every observed-typed slot expects - feedback initial
values, push queues, recordings.

These cover the seams where the wrong shape used to leak through: each of them
failed while the conversion produced the authored shape unconditionally.
"""

import pytest

from hgraph import (
    REMOVE,
    REMOVE_IF_EXISTS,
    TS,
    TSB,
    TSD,
    TSL,
    Size,
    TimeSeriesSchema,
    feedback,
    graph,
)
from hgraph.test import eval_node


def test_feedback_accepts_an_ordinary_tsd_initial_value():
    """A mapping with no strict intent must stay on the observed schema.

    ``feedback`` validates its initial delta against ``delta_value_schema`` and
    plans its storage with it, so an authored value fails at wiring time.
    """

    @graph
    def g() -> TSD[int, TS[int]]:
        fb = feedback(TSD[int, TS[int]], {1: 2})
        return fb()

    assert eval_node(g)[0] == {1: 2}


def test_feedback_accepts_an_empty_tsd_initial_value():
    @graph
    def g() -> TSD[int, TS[int]]:
        fb = feedback(TSD[int, TS[int]], {})
        return fb()

    assert eval_node(g)[0] == {}


def test_nested_tsd_inside_a_tsl_round_trips():
    """Escalation is all-or-nothing, so a nested TSD cannot disagree with its
    parent about which shape the map elements carry."""

    @graph
    def g(ts: TSL[TSD[str, TS[int]], Size[2]]) -> TSL[TSD[str, TS[int]], Size[2]]:
        return ts

    assert eval_node(g, [({"a": 1}, {"b": 2})]) == [{0: {"a": 1}, 1: {"b": 2}}]


def test_nested_tsd_inside_a_tsb_round_trips():
    class Schema(TimeSeriesSchema):
        d: TSD[str, TS[int]]

    @graph
    def g(ts: TSB[Schema]) -> TSD[str, TS[int]]:
        return ts.d

    assert eval_node(g, [{"d": {"a": 1}}]) == [{"a": 1}]


def test_remove_if_exists_is_lenient_for_an_absent_key():
    @graph
    def g(ts: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return ts

    assert eval_node(g, [{"a": 1}, {"a": REMOVE_IF_EXISTS, "absent": REMOVE_IF_EXISTS}]) == [
        {"a": 1},
        {"a": REMOVE},
    ]


def test_remove_removes_a_present_key():
    @graph
    def g(ts: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return ts

    assert eval_node(g, [{"a": 1}, {"a": REMOVE}]) == [{"a": 1}, {"a": REMOVE}]


def test_remove_raises_for_an_absent_key():
    """The whole point of the authored shape: strict intent is honoured."""

    @graph
    def g(ts: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return ts

    with pytest.raises(RuntimeError, match="REMOVE"):
        eval_node(g, [{"a": 1}, {"absent": REMOVE}])


def test_remove_nested_in_a_tsl_raises_for_an_absent_key():
    """Strict intent escalates the whole conversion, so it survives nesting."""

    @graph
    def g(ts: TSL[TSD[str, TS[int]], Size[2]]) -> TSL[TSD[str, TS[int]], Size[2]]:
        return ts

    with pytest.raises(RuntimeError, match="REMOVE"):
        eval_node(g, [({"a": 1}, {"b": 2}), ({"absent": REMOVE}, None)])
