"""Accepted runtime rules from runtime_spec/validation.md, DV-01/02/05/06/08/10."""
from datetime import timedelta

import pytest

import hgraph as hg
from hgraph.test import eval_node


@pytest.fixture(params=[hg.TSS[int], hg.TSD[int, hg.TS[int]]], ids=["set", "dictionary"])
def collection_source(request):
    schema = request.param
    is_set = schema == hg.TSS[int]

    @hg.compute_node
    def source(action: hg.TS[int], _output: hg.TS_OUT[schema] = None) -> schema:
        if action.value < 0:
            _output.invalidate()
        elif action.value == 0:
            return frozenset() if is_set else {}
        else:
            return {action.value} if is_set else {action.value: 7}

    return schema, source


@pytest.mark.parametrize("passive", [False, True], ids=["active", "passive"])
def test_direct_collection_input_observes_whole_invalidation(collection_source, passive):
    schema, source = collection_source
    activity = ("probe",) if passive else ("watched", "probe")

    @hg.compute_node(active=activity, valid=())
    def read(watched: schema, probe: hg.TS[bool]) -> hg.TS[tuple[bool, bool]]:
        assert watched.valid == watched.output.valid
        assert watched.modified == watched.output.modified
        return watched.valid, watched.modified

    @hg.graph
    def run(action: hg.TS[int], probe: hg.TS[bool]) -> hg.TS[tuple[bool, bool]]:
        return read(source(action), probe)

    # A coincident probe exposes the clear to a passive reader. The second
    # effective clear has no independent wake.
    assert eval_node(run, [1, -1, -1, 2, -1, None], [None, True, True, True, None, True]) == [
        None if passive else (True, True), (False, False), (False, False),
        (True, True), None if passive else (False, False), (False, False),
    ]


@pytest.mark.parametrize("simultaneous", [False, True], ids=["separate", "simultaneous"])
@pytest.mark.parametrize("duration", [False, True], ids=["count-window", "duration-window"])
def test_window_reset_depends_on_a_modified_collection(collection_source, duration, simultaneous):
    _, source = collection_source
    period, minimum = (timedelta(seconds=2), timedelta(0)) if duration else (5, 1)

    @hg.compute_node
    def snapshot(window: hg.TSW[int, hg.WINDOW_SIZE, hg.WINDOW_SIZE_MIN]) -> hg.TS[tuple[int, ...]]:
        return tuple(window.value)

    @hg.graph
    def run(actions: hg.TS[int], price: hg.TS[int]) -> hg.TS[tuple[int, ...]]:
        return snapshot(hg.to_window(price, period, minimum, reset=source(actions)))

    # First a valid empty resets the existing window. Its subsequent clear and
    # a repeated clear leave prices in place. A later publication resets with
    # no price, and the final reset precedes its simultaneous price.
    actions = [None, 0, None, -1, -1, None, 1, None, 2, None]
    prices = [4, 10, 20, 30 if simultaneous else None, None, None if simultaneous else 30, None, 40, 50, 60]
    retained = (10, 20, 30)
    assert eval_node(run, actions, prices, __end_time__=hg.MIN_ST + len(actions) * hg.MIN_TD) == [
        (4,), (10,), (10, 20), retained if simultaneous else None, None,
        None if simultaneous else retained, (), (40,), (50,), (50, 60),
    ]


def test_collection_clear_wakes_modified_without_rearming_take(collection_source):
    _, source = collection_source

    @hg.graph
    def modification(actions: hg.TS[int]) -> hg.TS[bool]:
        return hg.modified(source(actions))

    @hg.graph
    def take_one(actions: hg.TS[int], price: hg.TS[int]) -> hg.TS[int]:
        return hg.take(price, reset=source(actions), count=1)

    actions = [None, 0, None, -1, -1, None, 1, None]
    assert eval_node(modification, actions) == [False, True, False, False, None, None, True, False]
    assert eval_node(take_one, actions, [4, 10, 20, 30, None, 40, None, 50]) == [
        4, 10, None, None, None, None, None, 50,
    ]


def test_keyed_reference_withdrawal_wakes_with_its_removed_members(collection_source):
    schema, source = collection_source

    @hg.compute_node(active=("choose",), valid=("choose",))
    def select(choose: hg.TS[bool], value: hg.REF[schema]) -> hg.REF[schema]:
        return value.value if choose.value else hg.TimeSeriesReference.make()

    @hg.compute_node(valid=())
    def read(value: schema) -> hg.TS[object]:
        removed = value.removed() if schema == hg.TSS[int] else value.removed_keys()
        return value.valid, value.modified, tuple(sorted(removed))

    @hg.graph
    def run(actions: hg.TS[int], choose: hg.TS[bool]) -> hg.TS[object]:
        return read(select(choose, source(actions)))

    assert eval_node(run, [1, None, None, None], [True, False, None, True]) == [
        (True, True, ()), (False, True, (1,)), None, (True, True, ()),
    ]


def test_dictionary_membership_and_key_set_clock():
    observations = []

    @hg.compute_node
    def source(step: hg.TS[int], _output: hg.TSD = None) -> hg.TSD[str, hg.TS[int]]:
        if step.value == 1:
            _output.get_or_create("X")
        elif step.value in (2, 6):
            _output.get_or_create("X").value = 7
        elif step.value == 3:
            _output["X"].invalidate()
        elif step.value == 4:
            del _output["X"]

    @hg.compute_node(valid=("step",), active=("step",))
    def observe(step: hg.TS[int], ts: hg.TSD[str, hg.TS[int]]) -> hg.TS[int]:
        observations.append((set(ts.added_keys()), set(ts.removed_keys()), ts.key_set.modified))
        if ts.key_set.modified:
            expected = {hg.Removed("X")} if step.value == 4 else {"X"}
            assert set(ts.key_set.delta_value) == expected
        return step.value

    @hg.graph
    def run(step: hg.TS[int]) -> hg.TS[int]:
        return observe(step, source(step))

    eval_node(run, list(range(8)))
    assert observations == [
        (set(), set(), False), ({"X"}, set(), True),
        (set(), set(), False), (set(), set(), False),
        (set(), {"X"}, True), (set(), set(), False),
        ({"X"}, set(), True), (set(), set(), False),
    ]


def test_dictionary_reference_sampling_and_withdrawal():
    observations = []

    @hg.compute_node(valid=("selector",), active=("selector",))
    def choose(selector: hg.TS[int], a: hg.REF[hg.TSD[str, hg.TS[int]]],
               b: hg.REF[hg.TSD[str, hg.TS[int]]]) -> hg.REF[hg.TSD[str, hg.TS[int]]]:
        return [a.value, b.value, hg.TimeSeriesReference.make()][selector.value]

    @hg.compute_node(valid=("step",), active=("step",))
    def observe(step: hg.TS[int], ts: hg.TSD[str, hg.TS[int]]) -> hg.TS[int]:
        observations.append({
            "valid": ts.valid,
            "modified": ts.modified,
            "delta": dict(ts.delta_value) if ts.modified else None,
            "removed": {k: v.value for k, v in ts.removed_items()},
            "times": {k: v.last_modified_time for k, v in ts.items()},
        })
        return step.value

    @hg.graph
    def run(step: hg.TS[int], a: hg.TSD[str, hg.TS[int]], b: hg.TSD[str, hg.TS[int]],
            selector: hg.TS[int]) -> hg.TS[int]:
        return observe(step, choose(selector, a, b))

    eval_node(run, list(range(6)), [{"X": 1, "Z": 9}, None, None, None, None, None],
              [{"Y": 2, "Z": 3}, {"Y": 4}, None, None, None, None], [0, None, 1, 2, 0, None])
    assert [o["delta"] for o in observations] == [
        {"X": 1, "Z": 9}, None, {"X": hg.REMOVE, "Y": 4, "Z": 3},
        {"Y": hg.REMOVE, "Z": hg.REMOVE}, {"X": 1, "Z": 9}, None,
    ]
    assert observations[2]["removed"] == {"X": 1}
    assert observations[3]["removed"] == {"Y": 4, "Z": 3}
    assert observations[3]["valid"] is False
    assert observations[3]["modified"] is True
    assert observations[2]["times"] == {"Y": hg.MIN_ST + 2 * hg.MIN_TD, "Z": hg.MIN_ST + 2 * hg.MIN_TD}
    assert observations[4]["times"] == {"X": hg.MIN_ST + 4 * hg.MIN_TD, "Z": hg.MIN_ST + 4 * hg.MIN_TD}


def test_scalar_reference_unbind_resets_observed_time():
    observations = []

    @hg.compute_node
    def choose(step: hg.TS[int], a: hg.REF[hg.TS[int]]) -> hg.REF[hg.TS[int]]:
        return a.value if step.value == 0 else hg.TimeSeriesReference.make()

    @hg.compute_node(valid=("step",), active=("step",))
    def observe(step: hg.TS[int], ts: hg.TS[int]) -> hg.TS[int]:
        observations.append((ts.valid, ts.modified, ts.last_modified_time))
        return step.value

    @hg.graph
    def run(step: hg.TS[int], a: hg.TS[int]) -> hg.TS[int]:
        return observe(step, choose(step, a))

    eval_node(run, [0, 1], [7, None])
    assert observations == [(True, True, hg.MIN_ST), (False, False, hg.MIN_DT)]


def test_saved_reference_expires_before_next_dictionary_mutation():
    observations = []

    @hg.compute_node(valid=("step",), active=("step",))
    def save(step: hg.TS[int], ref: hg.REF[hg.TS[int]], _state: hg.STATE = None) -> hg.REF[hg.TS[int]]:
        if step.value == 0:
            _state.saved = ref.value
        if step.value in (0, 3, 5):
            return _state.saved

    @hg.compute_node(valid=("step",), active=("step",))
    def observe(step: hg.TS[int], ts: hg.TS[int]) -> hg.TS[int]:
        observations.append((ts.value if ts.valid else None, ts.modified))
        return step.value

    @hg.graph
    def run(step: hg.TS[int], values: hg.TSD[str, hg.TS[int]]) -> hg.TS[int]:
        return observe(step, save(step, values["X"]))

    eval_node(run, list(range(7)), [{"X": 7}, None, {"X": hg.REMOVE}, None, {"X": 9}, None, None])
    assert observations == [(7, True), (7, False), (7, False), (None, False),
                            (None, False), (None, False), (None, False)]
