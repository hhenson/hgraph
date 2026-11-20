import pytest

import hgraph as hg
from hgraph.test import eval_node


class Quote(hg.TimeSeriesSchema):
    bid: hg.TS[int]
    ask: hg.TS[int]


class Shelf(hg.TimeSeriesSchema):
    members: hg.TSS[int]
    book: hg.TSD[str, hg.TS[int]]


def test_whole_set_ticks_only_when_membership_changes():
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
        ((1,), (1,), ()), None, None, ((), (), (1,)), None, None
    ]


def test_eval_node_skips_equal_whole_sets_and_sparse_noops():
    @hg.compute_node
    def count_changes(members: hg.TSS[int]) -> hg.TS[int]:
        return len(members.added()) + len(members.removed())

    whole = frozenset({1})
    empty_patch = hg.set_delta(added=set(), removed=set(), tp=int)
    assert eval_node(count_changes, [whole, whole, empty_patch, whole]) == [1, None, None, None]


@pytest.mark.parametrize(("schema", "writes", "samples"), [
    (hg.TSS[int], [frozenset(), frozenset(), frozenset({1}), frozenset({1}), frozenset(), frozenset()],
     [1, None, 3, None, 5, None]),
    (hg.TSL[hg.TS[int], hg.Size[-1]], [(), (), (1, 2), (1, 2), (), ()],
     [1, None, 3, 4, 5, None]),
    (hg.TSD[int, hg.TS[int]], [{}, {}, {0: 7}, {0: 7}, {0: hg.REMOVE}, {}],
     [1, None, 3, 4, 5, None]),
    (hg.TS[int], [7] * 6, [1, 2, 3, 4, 5, 6]),
], ids=["set", "growing-list", "dictionary", "scalar"])
def test_whole_value_sampling_and_last_modified_time(schema, writes, samples):
    seen = []

    @hg.compute_node
    def publish(step: hg.TS[int]) -> schema:
        return writes[step.value - 1]

    @hg.sink_node(active=("step",), valid=("step",))
    def inspect(step: hg.TS[int], source: schema):
        seen.append((source.valid, source.modified, source.last_modified_time))

    @hg.graph
    def run(step: hg.TS[int]) -> hg.TS[int]:
        source = publish(step)
        inspect(step, source)
        return hg.sample(source, step)

    assert eval_node(run, list(range(1, 7))) == samples
    last_modified = hg.MIN_DT
    for index, (sample, reading) in enumerate(zip(samples, seen, strict=True)):
        if sample is not None:
            last_modified = hg.MIN_ST + index * hg.MIN_TD
        assert reading == (True, sample is not None, last_modified)


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


@pytest.mark.parametrize("dictionary", [True, False], ids=["dictionary", "growing-list"])
def test_reinserted_quote_starts_empty_after_whole_owner_invalidation(dictionary):
    collection = hg.TSD[int, hg.TSB[Quote]] if dictionary else hg.TSL[hg.TSB[Quote], hg.Size[-1]]

    def remove_quote(output):
        if dictionary:
            output.pop(1)
        else:
            output.resize(1)

    @hg.compute_node
    def write(step: hg.TS[int], _output: hg.TS_OUT[collection] = None) -> collection:
        if step.value == 0:
            return {0: {"bid": 10}, 1: {"bid": 20}}

        remove_quote(_output)
        retained = dict(_output.removed_items())[1]
        previous_bid = retained["bid"].value
        _output.invalidate()
        _output.invalidate()
        assert not _output.valid
        assert not _output.modified
        assert retained["bid"].value == previous_bid

        # Writing another child must not revive the old contents of position/key 1.
        _output.value = {0: {"ask": 7}}
        if dictionary:
            assert not _output.get_or_create(1).valid
            assert _output.delta_value[1] is hg.REMOVE
        _output.value = {1: {}}
        assert _output[1].valid
        assert not _output[1]["bid"].valid
        if step.value == 1:
            _output[1]["bid"].value = 30
            remove_quote(_output)
            _output.value = {1: {}}
            assert _output[1]["bid"].value == 30

    @hg.compute_node
    def read(quotes: collection) -> hg.TS[str]:
        bid = quotes[1]["bid"]
        return str(bid.value) if bid.valid else "empty"

    @hg.graph
    def run(step: hg.TS[int]) -> hg.TS[str]:
        return read(write(step))

    assert eval_node(run, [0, 1, 2]) == ["20", "30", "empty"]


@pytest.mark.parametrize("duration", [False, True], ids=["count", "duration"])
def test_recreated_window_keeps_only_new_samples_and_expiry(duration):
    period = 4 * hg.MIN_TD if duration else 4
    minimum = 0 * hg.MIN_TD if duration else 0
    window = hg.TSW[int, hg.WindowSize[period], hg.WindowSize[minimum]]
    book = hg.TSD[str, window]

    @hg.compute_node
    def write(step: hg.TS[int], _output: hg.TS_OUT[book] = None) -> book:
        if step.value == 0:
            return {"a": 1, "b": 20}
        if step.value == 1:
            _output.pop("b")
            retained = dict(_output.removed_items())["b"]
            _output.invalidate()
            assert tuple(retained.value) == (20,)
        else:
            _output.invalidate()
        return {"b": 20 + step.value}

    @hg.compute_node
    def read(items: book) -> hg.TS[tuple[int, ...]]:
        samples = items["b"]
        for when in samples.value_times:
            assert when == samples.last_modified_time
        return tuple(samples.value)

    @hg.graph
    def run(step: hg.TS[int]) -> hg.TS[tuple[int, ...]]:
        return read(write(step))

    result = eval_node(run, [0, 1, 2, None, None, None, None, None])
    assert result == [(20,), (21,), (22,), None, None, None, None, () if duration else None]
