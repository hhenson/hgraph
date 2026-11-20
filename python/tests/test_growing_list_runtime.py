import pytest

from hgraph import (
    CLOCK, IN_MEMORY, MIN_DT, MIN_ST, MIN_TD, REF, REMOVE, STATE, TS, TSB, TSD, TSL, TSLDelta,
    GlobalState, Size, TimeSeriesReference, TimeSeriesSchema,
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
        (True, True, False, 0, (), (), None),
        (True, True, False, 0, (), (), None),
        (False, False, False, 2, (False, False), (), 2),
        (True, True, True, 2, (True, True), (), None),
        (False, False, False, 2, (False, False), (0, 1), None),
        (True, True, True, 0, (), (0, 1), 0),
    ]


def test_typed_replay_keeps_empty_publications_and_skips_noop_sparse_edits():
    @graph
    def identity(items: Growing) -> Growing:
        return items

    edits = [(), (), TSLDelta(size_modified=0), TSLDelta(), None, (), (1,), (), ()]
    assert eval_node(identity, edits) == [
        TSLDelta(), None, None, None, None, None,
        TSLDelta({0: 1}, size_modified=1), TSLDelta(removed={0}, size_modified=0), None,
    ]


def test_sparse_recording_replays_initial_empty_and_truncation_at_their_original_times():
    @compute_node
    def publish(step: TS[int]) -> Growing:
        return [(), TSLDelta(size_modified=0), (), (7,), (), ()][step.value]

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
        eval_node(write, list(range(6)))
        eval_node(read, __end_time__=MIN_ST + 6 * MIN_TD)
    assert observed == [(MIN_ST, 0), (MIN_ST + 3 * MIN_TD, 1), (MIN_ST + 4 * MIN_TD, 0)]


def test_clearing_the_final_fixed_position_keeps_the_extent_and_loses_the_value():
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
        (True, True, True, True), (False, False, False, False), (True, False, False, True),
    ]


@pytest.mark.parametrize("schema", [Growing, Fixed], ids=["growing", "fixed"])
def test_held_extent_and_public_clock_agree_on_live_and_replayed_list_inputs(schema):
    edits = [TSLDelta(size_modified=2) if schema is Growing else {},
             {1: 20}, {0: 10}, {1: REMOVE}, {0: REMOVE}, None,
             TSLDelta(size_modified=2) if schema is Growing else {}, {1: 30}]
    observations = []

    @compute_node
    def publish(step: TS[int]) -> schema:
        return edits[step.value]

    @compute_node(active=("clock",), valid=("clock",))
    def read(items: schema, clock: TS[int]) -> TS[int]:
        root = (items.valid, items.modified, items.last_modified_time, items.delta_value is None)
        owner = items.output
        assert root == (owner.valid, owner.modified, owner.last_modified_time, owner.delta_value is None)
        observations.append((root, len(items), tuple(child.value if child.valid else None for child in items.values())))
        if not items.valid:
            assert items.value is None and owner.value is None
        return clock.value

    @graph
    def live(step: TS[int]) -> TS[int]:
        return read(publish(step), step)

    clock = list(range(len(edits)))
    eval_node(live, clock)
    missing = (False, False, MIN_DT, True)
    def publication(step):
        return (True, True, MIN_ST + step * MIN_TD, False)

    expected = [(missing, 2, (None, None)), (publication(1), 2, (None, 20)),
                (publication(2), 2, (10, 20)), (publication(3), 2, (10, None)),
                (missing, 2, (None, None)), (missing, 2, (None, None)),
                (missing, 2, (None, None)), (publication(7), 2, (None, 30))]
    assert observations == expected
    observations.clear()
    eval_node(read, edits, clock)
    assert observations == expected


@pytest.mark.parametrize("schema", [Growing, Fixed], ids=["growing", "fixed"])
def test_whole_invalidation_suppresses_modification_on_a_passive_direct_peer(schema):
    seen = []

    @compute_node
    def produce(clock: TS[int], _output: schema = None) -> schema:
        if clock.value == 0: return (3, 5)
        if clock.value == 1: _output.invalidate()

    @compute_node(active=("clock",), valid=("clock",))
    def inspect(items: schema, clock: TS[int]) -> TS[int]:
        seen.append((items.valid, items.modified, items.last_modified_time,
                     items.output.modified, len(items)))
        return clock.value

    @graph
    def run(clock: TS[int]) -> TS[int]:
        return inspect(produce(clock), clock)

    eval_node(run, [0, 1, 2])
    empty_extent = 0 if schema is Growing else 2
    assert seen == [(True, True, MIN_ST, True, 2),
                    (False, False, MIN_DT, False, empty_extent),
                    (False, False, MIN_DT, False, empty_extent)]


def test_withdrawing_a_list_reference_reports_the_detached_route_once():
    @compute_node(active=("selected",), valid=("selected",))
    def choose(selected: TS[bool], source: REF[Growing]) -> REF[Growing]:
        return source.value if selected.value else TimeSeriesReference.make()

    @compute_node(active=("clock",), valid=("clock",))
    def inspect(clock: TS[int], items: Growing) -> TS[tuple[bool, bool]]:
        return items.valid, items.modified

    @graph
    def run(clock: TS[int], selected: TS[bool], source: Growing) -> TS[tuple[bool, bool]]:
        return inspect(clock, choose(selected, source))

    assert eval_node(run, [0, 1, 2, 3], [True, False, None, True], [(1, 2)]) == [
        (True, True), (False, True), (False, False), (True, True),
    ]


class ListField(TimeSeriesSchema):
    items: Growing


@pytest.mark.parametrize("parent,key", [
    (TSB[ListField], "items"), (TSD[str, Growing], "items"),
    (TSL[Growing, Size[2]], 0), (TSL[Growing, Size[-1]], 0),
], ids=["bundle", "dictionary", "fixed-list", "growing-list"])
def test_recorded_vacant_child_keeps_its_extent_until_it_republishes(parent, key):
    edits = [{key: (2,)}, {key: {0: REMOVE}}, {key: TSLDelta(size_modified=3)}, None, {key: {2: 7}}]
    if key == 0:
        edits[0][1] = (19,)  # Keep the enclosing list live while its first position is vacant.
    states = []

    @compute_node
    def publish(clock: TS[int]) -> parent:
        return edits[clock.value]

    @compute_node(active=("clock",), valid=("clock",))
    def inspect(clock: TS[int], holder: parent) -> TS[int]:
        child = holder[key]
        states.append((child.valid, child.modified, tuple(item.value if item.valid else None for item in child.values())))
        return clock.value

    @graph
    def live(clock: TS[int]) -> TS[int]:
        holder = publish(clock)
        record(holder, "holder", recordable_id="nested-list")
        return inspect(clock, holder)

    @graph
    def recorded(clock: TS[int]) -> TS[int]:
        return inspect(clock, replay("holder", parent, recordable_id="nested-list"))

    clock = list(range(len(edits)))
    expected = [(True, True, (2,)), (False, False, (None,)), (False, False, (None, None, None)),
                (False, False, (None, None, None)), (True, True, (None, None, 7))]
    with GlobalState():
        set_record_replay_config(IN_MEMORY)
        for node, args in [(live, (clock,)), (recorded, (clock,)), (inspect, (clock, edits))]:
            states.clear()
            assert eval_node(node, *args) == clock
            assert states == expected


def test_recorded_dictionary_removes_vacant_list_keys_and_accepts_their_reinsertion():
    book = TSD[str, Growing]
    edits = [
        {"hold": (1,), "holes": TSLDelta(size_modified=2)},
        {"hold": {0: REMOVE}}, {"hold": REMOVE, "holes": REMOVE}, None,
        {"hold": (), "holes": {0: 9}},
    ]
    states = []

    @compute_node
    def publish(clock: TS[int]) -> book:
        return edits[clock.value]

    @compute_node(active=("clock",), valid=("clock",))
    def inspect(clock: TS[int], items: book) -> TS[int]:
        states.append({key: (items[key].valid, len(items[key])) for key in items.keys()})
        return clock.value

    @graph
    def live(clock: TS[int]) -> TS[int]:
        items = publish(clock)
        record(items, "book", recordable_id="vacant-keys")
        return inspect(clock, items)

    @graph
    def recorded(clock: TS[int]) -> TS[int]:
        return inspect(clock, replay("book", book, recordable_id="vacant-keys"))

    clock = list(range(len(edits)))
    expected = [{"hold": (True, 1), "holes": (False, 2)}, {"hold": (False, 1), "holes": (False, 2)},
                {}, {}, {"hold": (True, 0), "holes": (True, 1)}]
    with GlobalState():
        set_record_replay_config(IN_MEMORY)
        for node, args in [(live, (clock,)), (recorded, (clock,)), (inspect, (clock, edits))]:
            states.clear()
            assert eval_node(node, *args) == clock
            assert states == expected


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

    # Withdrawing every value keeps its slots. A later structural shrink
    # still retires the two dropped graphs while the list has no current value.
    eval_node(run, [(10, 20, 30), TSLDelta(removed=(0, 1, 2)),
                    TSLDelta(size_modified=1), {1: 40}, {0: 50}])
    identities = {value: (identity, pos) for identity, pos, value in evaluations}
    assert len(evaluations) == 5
    assert {value: pos for value, (_, pos) in identities.items()} == {10: 0, 20: 1, 30: 2, 40: 1, 50: 0}
    assert identities[10] == identities[50]
    assert len(starts) == 4
    assert {identities[v][0] for v in (10, 20, 30, 40)} == set(starts)
    retired = {(identity, pos) for identity, pos, when in stops if when == MIN_ST + 2 * MIN_TD}
    surviving = {(identity, pos) for identity, pos, when in stops if when > MIN_ST + 2 * MIN_TD}
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
