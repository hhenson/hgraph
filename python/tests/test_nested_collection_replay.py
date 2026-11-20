"""Observed collection ticks survive both Python seed normalization and in-memory recording."""

import pytest

import hgraph as hg


class Quotes(hg.TimeSeriesSchema):
    members: hg.TSS[int]
    prices: hg.TSL[hg.TS[int], hg.Size[-1]]
    stamp: hg.TS[int]


@pytest.mark.parametrize("nested", [False, True], ids=["bundle", "key_and_position"])
def test_replay_preserves_each_named_child_publication_and_omitted_siblings(nested):
    Book = hg.TSD[str, hg.TSL[hg.TSB[Quotes], hg.Size[1]]] if nested else hg.TSB[Quotes]
    same = {"members": frozenset({2}), "prices": ()}
    sparse = {"members": hg.set_delta(added=set(), removed=set(), tp=int), "prices": hg.TSLDelta({})}
    events = [
        {"members": frozenset({1}), "prices": (7,), "stamp": 10},
        same,
        same,
        sparse,
        {**same, "stamp": 11},
        {**sparse, "stamp": 12},
        same,
    ]
    if nested:
        events = [{"book": {0: event}} for event in events]
    seen = []

    @hg.compute_node
    def publish(clock: hg.TS[int]) -> Book:
        return events[clock.value]

    @hg.compute_node(active=("clock",), valid=("clock",))
    def inspect(clock: hg.TS[int], book: Book) -> hg.TS[int]:
        quote = book["book"][0] if nested else book
        members, prices = quote.members, quote.prices
        set_edits = (set(members.added()), set(members.removed())) if members.modified else None
        if prices.modified:
            delta = prices.delta_value
            price_edits = (dict(delta.modified), set(delta.removed), delta.size_modified)
        else:
            price_edits = None
        seen.append((book.modified, set_edits, price_edits, quote.stamp.modified))
        return clock.value

    @hg.graph
    def record(clock: hg.TS[int]) -> hg.TS[int]:
        book = publish(clock)
        hg.record(book, "edits", recordable_id="nested_quotes")
        return inspect(clock, book)

    @hg.graph
    def replay(clock: hg.TS[int]) -> hg.TS[int]:
        return inspect(clock, hg.replay("edits", Book, recordable_id="nested_quotes"))

    clocks = list(range(len(events)))
    with hg.GlobalState():
        hg.set_record_replay_config(hg.IN_MEMORY)
        hg.eval_node(record, clocks)
        live = list(seen)
        seen.clear()
        hg.eval_node(replay, clocks)
        assert seen == live
        seen.clear()
        hg.eval_node(inspect, clocks, events)
        assert seen == live

    assert live[0] == (True, ({1}, set()), ({0: 7}, set(), 1), True)
    assert live[1] == (True, ({2}, {1}), ({}, {0}, 0), False)
    for step in (2, 3, 6):
        assert live[step][1:] == (None, None, False)
    assert live[4] == (True, None, None, True)
    if not nested:
        assert live[3][0] is False
    assert live[5] == (True, None, None, True)
