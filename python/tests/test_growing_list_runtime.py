import pytest

from hgraph import (
    CLOCK, IN_MEMORY, MIN_ST, MIN_TD, REMOVE, STATE, TS, TSL, TSLDelta, GlobalState, Size,
    compute_node, eval_node, graph, map_, record, replay, set_record_replay_config, sink_node,
)


Growing = TSL[TS[int], Size[-1]]
Fixed = TSL[TS[int], Size[2]]


@pytest.mark.parametrize("first", [(), TSLDelta(size_modified=0)], ids=["whole", "sparse"])
def test_empty_growing_list_publications_and_size_only_changes(first):
    edits = [first, TSLDelta(size_modified=0), (), (), TSLDelta(size_modified=2),
             (10, 20), TSLDelta(removed=(0, 1)), ()]
    observed = []

    @compute_node
    def publish(step: TS[int]) -> Growing:
        return edits[step.value]

    @compute_node(valid=())
    def inspect(items: Growing, step: TS[int]) -> TS[int]:
        observed.append((items.valid, items.all_valid, items.modified, len(items),
                         tuple(child.valid for child in items.values()),
                         tuple(sorted(items.removed_keys())), items.size_modified))
        return step.value

    @graph
    def run(step: TS[int]) -> TS[int]:
        return inspect(publish(step), step)

    assert eval_node(run, list(range(len(edits)))) == list(range(len(edits)))
    assert observed == [
        (True, True, True, 0, (), (), None),
        (True, True, False, 0, (), (), None),
        (True, True, True, 0, (), (), None),
        (True, True, True, 0, (), (), None),
        (True, False, True, 2, (False, False), (), 2),
        (True, True, True, 2, (True, True), (), None),
        (True, False, True, 2, (False, False), (0, 1), None),
        (True, True, True, 0, (), (0, 1), 0),
    ]


def test_typed_replay_keeps_empty_publications_and_skips_noop_sparse_edits():
    @graph
    def identity(items: Growing) -> Growing:
        return items

    edits = [(), (), TSLDelta(size_modified=0), TSLDelta(), None, (), (1,), (), ()]
    assert eval_node(identity, edits) == [
        TSLDelta(), TSLDelta(), None, None, None, TSLDelta(),
        TSLDelta({0: 1}, size_modified=1), TSLDelta(removed={0}, size_modified=0), TSLDelta(),
    ]


def test_sparse_recording_replays_two_whole_empty_publications_at_their_original_times():
    @compute_node
    def publish(step: TS[int]) -> Growing:
        return () if step.value != 1 else TSLDelta(size_modified=0)

    @graph
    def write(step: TS[int]) -> None:
        record(publish(step), "items", recordable_id="growing")

    observed = []

    @sink_node
    def inspect(items: Growing, clock: CLOCK = None):
        observed.append((clock.evaluation_time, len(items)))

    @graph
    def read() -> None:
        inspect(replay("items", Growing, recordable_id="growing"))

    with GlobalState():
        set_record_replay_config(IN_MEMORY)
        eval_node(write, [0, 1, 2])
        eval_node(read, __end_time__=MIN_ST + 3 * MIN_TD)
    assert observed == [(MIN_ST, 0), (MIN_ST + 2 * MIN_TD, 0)]


def test_clearing_the_final_values_of_an_owned_fixed_list_preserves_its_extent_and_validity():
    @compute_node
    def publish(step: TS[int]) -> Fixed:
        return [(10, 20), {0: REMOVE, 1: REMOVE}, {1: 5}][step.value]

    @compute_node(valid=())
    def inspect(items: Fixed) -> TS[tuple[bool, bool, bool, bool]]:
        return items.valid, items.all_valid, items[0].valid, items[1].valid

    @graph
    def run(step: TS[int]) -> TS[tuple[bool, bool, bool, bool]]:
        return inspect(publish(step))

    assert eval_node(run, [0, 1, 2]) == [
        (True, True, True, True), (True, False, False, False), (True, False, False, True),
    ]


@pytest.mark.parametrize(("index", "expected"), [
    (1, [2, -1, -1, 7]),
    (-2, [2, -1, -1, 1]),
])
def test_growing_list_selection_follows_the_current_extent(index, expected):
    @compute_node(valid=())
    def inspect(selected: TS[int], clock: TS[int]) -> TS[int]:
        return selected.value if selected.valid else -1

    @graph
    def run(items: Growing, clock: TS[int]) -> TS[int]:
        return inspect(items[index], clock)

    assert eval_node(run, [(1, 2, 3), TSLDelta(size_modified=1), None, {1: 7}], [0, 1, 2, 3]) == expected


@pytest.mark.parametrize("form", ["node", "annotated graph", "unannotated graph"])
def test_mapped_list_sink_retires_each_dropped_child_and_starts_a_fresh_one(form):
    starts, evaluations, stops = [], [], []

    @sink_node
    def observe(ndx: TS[int], item: TS[int], state: STATE = None):
        evaluations.append((state.identity, ndx.value, item.value))
        state.position = ndx.value

    @observe.start
    def start(state: STATE = None):
        state.identity = len(starts)
        starts.append(state.identity)

    @observe.stop
    def stop(state: STATE = None, clock: CLOCK = None):
        stops.append((state.identity, state.position, clock.evaluation_time))

    @graph
    def annotated(ndx: TS[int], item: TS[int]) -> None:
        observe(ndx, item)

    @graph
    def unannotated(ndx: TS[int], item: TS[int]):
        observe(ndx, item)

    body = {"node": observe, "annotated graph": annotated, "unannotated graph": unannotated}[form]

    @graph
    def run(items: Growing) -> Growing:
        assert map_(body, items) is None
        return items

    eval_node(run, [(10, 20, 30), TSLDelta(size_modified=1), {1: 40}, {0: 50}])
    identities = {value: (identity, pos) for identity, pos, value in evaluations}
    assert len(evaluations) == 5
    assert {value: pos for value, (_, pos) in identities.items()} == {10: 0, 20: 1, 30: 2, 40: 1, 50: 0}
    assert identities[10] == identities[50]
    assert len(starts) == 4
    assert {identities[v][0] for v in (10, 20, 30, 40)} == set(starts)
    retired = {(identity, pos) for identity, pos, when in stops if when == MIN_ST + MIN_TD}
    surviving = {(identity, pos) for identity, pos, when in stops if when > MIN_ST + MIN_TD}
    assert retired == {identities[20], identities[30]}
    assert surviving == {identities[10], identities[40]}
    assert len(stops) == 4


def test_unannotated_value_returning_graph_still_produces_a_mapped_list():
    @graph
    def twice(item: TS[int]):
        return item + item

    @graph
    def run(items: Growing) -> Growing:
        return map_(twice, items)

    assert eval_node(run, [(2, 3)]) == [TSLDelta({0: 4, 1: 6}, size_modified=2)]
