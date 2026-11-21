from datetime import timedelta
from typing import Optional

import pytest

import hgraph as hg
from hgraph.test import eval_node


Price = hg.TS[int]


class Quote(hg.TimeSeriesSchema):
    bid: Price
    ask: Price


Quotes = hg.TSB[Quote]


@hg.compute_node
def quote(phase: Price, _output: Quotes = None) -> Quotes:
    if phase.value == 0:
        return {"bid": 7, "ask": 42}
    if phase.value == 1:
        _output.bid.invalidate()
    if phase.value == 2:
        return {"ask": 43}
    if phase.value == 3:
        return {"bid": 11}


@hg.compute_node(active=("clock",), valid=("clock",))
def read_quote(clock: Price, item: Quotes) -> hg.TS[object]:
    if not item.valid:
        return "invalid"
    return (item.bid.value if item.bid.valid else None, item.ask.value,
            item.bid.modified, item.ask.modified)


@pytest.mark.parametrize("operator", [
    lambda q: hg.drop(q, 0),
    lambda q: hg.take(q, 20),
    lambda q: hg.take(q, timedelta(seconds=1)),
    lambda q: hg.filter_(hg.const(True), q),
    lambda q: hg.freeze(hg.const(False), q),
], ids=["drop", "take", "take-duration", "open-filter", "before-freeze"])
def test_an_open_gate_forwards_a_withdrawal_and_only_publishes_changed_fields(operator):
    @hg.graph
    def run(phase: Price, clock: Price) -> hg.TS[object]:
        return read_quote(clock, operator(quote(phase)))

    assert eval_node(run, [0, 1, 2, 3, 3, None], list(range(6))) == [
        (7, 42, True, True), (None, 42, False, False), (None, 43, False, True),
        (11, 43, True, False), (11, 43, True, False), (11, 43, False, False),
    ]


def test_closed_gates_hold_values_and_reopening_respects_source_state():
    @hg.graph
    def run(phase: Price, clock: Price, flag: hg.TS[bool], mode: str) -> hg.TS[object]:
        source = quote(phase)
        if mode == "take":
            port = hg.take(source, 1)
        elif mode == "never":
            port = hg.take(source, 0)
        elif mode == "skip":
            port = hg.drop(source, 1)
        elif mode == "filter":
            port = hg.filter_(flag, source)
        else:
            port = hg.freeze(flag, source)
        return read_quote(clock, port)

    def prices(mode, flags):
        rows = eval_node(run, [0, 1, 2, 3, None], list(range(5)), flags, mode)
        return [row if row == "invalid" else row[:2] for row in rows]

    assert prices("take", []) == [(7, 42)] * 5
    assert prices("never", []) == ["invalid"] * 5
    assert prices("skip", []) == ["invalid", (None, 42), (None, 43), (11, 43), (11, 43)]
    assert prices("filter", [True, False, None, True]) == [(7, 42)] * 3 + [(11, 43)] * 2
    assert prices("freeze", [False, True]) == [(7, 42)] + [(None, 42)] * 4


def test_a_forwarded_dictionary_retains_an_invalid_child_until_the_key_is_removed():
    book = hg.TSD[str, hg.TS[Optional[int]]]

    @hg.compute_node
    def write(phase: Price, _output: book = None) -> book:
        if phase.value == 0:
            return {"q": 7, "z": 42}
        if phase.value == 1:
            _output["q"].invalidate()
        if phase.value == 2:
            return {"z": 43}
        if phase.value == 3:
            return {"q": hg.REMOVE}
        if phase.value == 4:
            return {"q": hg.Null}
        if phase.value == 5:
            return {"q": 11}

    @hg.compute_node(active=("clock",), valid=("clock",))
    def read(clock: Price, data: book) -> hg.TS[object]:
        if "q" not in data:
            return "absent", data["z"].value
        child = data["q"]
        return (child.value if child.valid else "invalid"), data["z"].value

    @hg.graph
    def run(phase: Price, clock: Price) -> hg.TS[object]:
        return read(clock, hg.drop(write(phase), 0))

    assert eval_node(run, list(range(6)) + [None], list(range(7))) == [
        (7, 42), ("invalid", 42), ("invalid", 43), ("absent", 43), (None, 43), (11, 43), (11, 43),
    ]

def test_losing_a_selected_child_withdraws_the_forwarded_price():
    route = hg.REF[Price]

    class Routes(hg.TimeSeriesSchema):
        selected: route
        neighbor: Price

    bundle = hg.TSB[Routes]

    @hg.compute_node(active=("choice",), valid=("choice",))
    def choose(choice: hg.TS[str], a: route, b: route, _output: bundle = None) -> bundle:
        if choice.value == "clear":
            _output.selected.invalidate()
        elif choice.value == "a":
            return {"selected": a.value, "neighbor": 42}
        else:
            return {"selected": b.value}

    @hg.compute_node(active=("clock",), valid=("clock",))
    def inspect(clock: Price, selected: Price, after: Price, neighbor: Price) -> hg.TS[object]:
        return tuple(item.value if item.valid else None for item in (selected, after, neighbor))

    @hg.graph
    def run(clock: Price, choice: hg.TS[str], a: Price, b: Price) -> hg.TS[object]:
        live = choose(choice, a, b)
        after = hg.drop(live, 0)
        return inspect(clock, live.selected, after.selected, after.neighbor)

    rows = eval_node(run, list(range(8)), ["a", "b", None, None, "clear", None, "a", None],
                     list(range(1, 9)), list(range(11, 19)))
    assert rows == [(x, x, 42) for x in [1, 12, 13, 14, None, None, 7, 8]]
