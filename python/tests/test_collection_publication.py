import pytest

import hgraph as hg
from hgraph.test import eval_node


class Quote(hg.TimeSeriesSchema):
    bid: hg.TS[int]
    ask: hg.TS[int]


class Shelf(hg.TimeSeriesSchema):
    members: hg.TSS[int]
    book: hg.TSD[str, hg.TS[int]]


def test_whole_set_ticks_even_when_membership_is_unchanged():
    @hg.compute_node
    def publish(step: hg.TS[int]) -> hg.TSS[int]:
        return (frozenset({1}), frozenset({1}), {1}, frozenset(), frozenset(), set())[step.value]

    @hg.compute_node
    def read(members: hg.TSS[int]) -> hg.TS[object]:
        return tuple(sorted(members.value)), tuple(sorted(members.added())), tuple(sorted(members.removed()))

    @hg.graph
    def run(step: hg.TS[int]) -> hg.TS[object]:
        return read(publish(step))

    assert eval_node(run, list(range(6))) == [
        ((1,), (1,), ()), ((1,), (), ()), None, ((), (), (1,)), ((), (), ()), None
    ]


def test_eval_node_replays_whole_set_publications_and_skips_sparse_noops():
    @hg.compute_node
    def count_changes(members: hg.TSS[int]) -> hg.TS[int]:
        return len(members.added()) + len(members.removed())

    whole = frozenset({1})
    empty_patch = hg.set_delta(added=set(), removed=set(), tp=int)
    assert eval_node(count_changes, [whole, whole, empty_patch, whole]) == [1, 0, None, 0]


def test_empty_bundle_patch_initializes_a_dictionary_child_once():
    @hg.compute_node
    def publish(step: hg.TS[int]) -> hg.TSD[str, hg.TSB[Quote]]:
        return ({"a": {}}, {"a": {}}, {"a": {"bid": 1}}, {"a": {}}, {"b": {}})[step.value]

    @hg.compute_node
    def read(quotes: hg.TSD[str, hg.TSB[Quote]]) -> hg.TS[object]:
        return tuple(sorted(key for key, quote in quotes.items() if quote.valid))

    @hg.graph
    def run(step: hg.TS[int]) -> hg.TS[object]:
        return read(publish(step))

    assert eval_node(run, list(range(5))) == [("a",), None, ("a",), None, ("a", "b")]


@pytest.mark.parametrize("positional", [False, True], ids=["named", "positional"])
def test_sparse_bundle_fields_share_nested_removal_and_no_change_semantics(positional):
    empty_set = hg.set_delta(added=set(), removed=set(), tp=int)

    def fields(members, book):
        return (members, book) if positional else {"members": members, "book": book}

    @hg.compute_node
    def publish(step: hg.TS[int]) -> hg.TSB[Shelf]:
        return (
            fields({1}, {"a": 1, "b": 2}), fields(empty_set, None),
            fields(None, {"a": hg.REMOVE_IF_EXISTS}), fields(None, {"a": hg.REMOVE_IF_EXISTS}),
            fields(None, {"b": hg.REMOVE}),
        )[step.value]

    @hg.compute_node
    def read(shelf: hg.TSB[Shelf]) -> hg.TS[object]:
        return tuple(sorted(shelf.members.value)), tuple(sorted(shelf.book.value))

    @hg.graph
    def run(step: hg.TS[int]) -> hg.TS[object]:
        return read(publish(step))

    assert eval_node(run, list(range(5))) == [
        ((1,), ("a", "b")), None, ((1,), ("b",)), None, ((1,), ())
    ]



def test_positional_bundle_samples_apply_nested_patches_through_eval_node():
    @hg.compute_node
    def read(shelf: hg.TSB[Shelf]) -> hg.TS[tuple[str, ...]]:
        return tuple(sorted(shelf.book.value))

    assert eval_node(read, [
        (None, None), (None, {"a": 1, "b": 2}), (None, {"a": hg.REMOVE}),
        (None, {"a": hg.REMOVE_IF_EXISTS}),
    ]) == [None, ("a", "b"), ("b",), None]


def test_invalidated_collections_discard_members_and_growing_positions():
    @hg.compute_node
    def book(step: hg.TS[int], _output: hg.TSD_OUT[str, hg.TS[int]] = None) -> hg.TSD[str, hg.TS[int]]:
        if step.value == 1:
            _output.invalidate()
            return
        return {"a": 1} if step.value == 0 else {"b": 2}

    @hg.compute_node
    def members(step: hg.TS[int], _output: hg.TSS_OUT[int] = None) -> hg.TSS[int]:
        if step.value == 1:
            _output.invalidate()
            return
        return {1} if step.value == 0 else {2}

    growing = hg.TSL[hg.TS[int], hg.Size[-1]]

    @hg.compute_node
    def readings(step: hg.TS[int], _output: hg.TS_OUT[growing] = None) -> growing:
        if step.value == 1:
            _output.invalidate()
            return
        return hg.TSLDelta({0: 1, 1: 2}, size_modified=2) if step.value == 0 else hg.TSLDelta({0: 5})

    @hg.compute_node(active=("clock",), valid=("clock",))
    def read(clock: hg.TS[int], quotes: hg.TSD[str, hg.TS[int]], items: hg.TSS[int], data: growing) -> hg.TS[object]:
        if not quotes.valid:
            for collection in (quotes, items, data):
                assert collection.value is None
                assert collection.delta_value is None
                assert collection.last_modified_time == hg.MIN_DT
        return (quotes.valid, items.valid, data.valid), tuple(sorted(quotes.keys())), tuple(sorted(items.values())), len(data)

    @hg.graph
    def run(step: hg.TS[int]) -> hg.TS[object]:
        return read(step, book(step), members(step), readings(step))

    assert eval_node(run, [0, 1, 2]) == [
        ((True, True, True), ("a",), (1,), 2), ((False, False, False), (), (), 0),
        ((True, True, True), ("b",), (2,), 1),
    ]
