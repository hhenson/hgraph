"""Owned windows under keyed groups and growing lists follow their live slots."""

import hgraph as hg
import pytest


Window = hg.TSW[int, hg.WindowSize[2 * hg.MIN_TD], hg.WindowSize[0 * hg.MIN_TD]]
Lanes = hg.TSL[Window, hg.Size[-1]]
Groups = hg.TSD[str, Lanes]


@hg.compute_node
def write_group(command: hg.TS[str], _output: Groups = None) -> Groups:
    if command.value == "seed siblings":
        return {"desk": {0: 2, 1: 3}, "other": {0: 5}}
    if command.value == "drop siblings":
        _output.pop("other")
        _output["desk"].resize(1)
        return
    if command.value == "drop key":
        _output.pop("desk")
        return
    lanes = _output.get_or_create("desk")
    if command.value == "drop lane":
        lanes.resize(0)
    elif command.value == "revive lane":
        lanes.resize(0)
        lanes.resize(1)
    elif command.value == "revive key":
        _output.pop("desk")
        _output.get_or_create("desk")
    else:
        lanes.resize(1)
        lanes[0].value = int(command.value)


@pytest.mark.parametrize("remove", ["drop key", "drop lane"])
def test_removing_a_slot_cancels_its_wake_and_reusing_it_starts_a_new_history(remove):
    root_times = []
    retained = []

    class ObserveRoot(hg.EvaluationLifeCycleObserver):
        def on_before_graph_evaluation(self, graph):
            if graph.graph_id == ():
                root_times.append(graph.evaluation_clock.evaluation_time)

    @hg.compute_node
    def count(groups: Groups) -> hg.TS[int]:
        lanes = groups.get("desk")
        if lanes is None:
            for key, removed_lanes in groups.removed_items():
                window = removed_lanes[0]
                retained.append((key, tuple(window.value), tuple(window.value_times)))
            return -1
        if not len(lanes):
            for index, window in lanes.removed_items():
                retained.append((index, tuple(window.value), tuple(window.value_times)))
            return -1
        return len(lanes[0])

    @hg.graph
    def app(command: hg.TS[str]) -> hg.TS[int]:
        return count(write_group(command))

    # The retired sample from t0 must never schedule t3. The same name/index
    # holds only the t2 sample, which leaves in its own quiet cycle at t5.
    assert hg.eval_node(app, ["4", remove, "7"], __observers__=[ObserveRoot()]) == [1, -1, 1, None, None, 0]
    assert retained == [("desk" if remove == "drop key" else 0, (4,), (hg.MIN_ST,))]
    assert hg.MIN_ST + 3 * hg.MIN_TD not in root_times


@pytest.mark.parametrize("revive", ["revive key", "revive lane"])
def test_same_cycle_slot_revival_keeps_the_original_window_deadline(revive):
    departures = []

    @hg.compute_node
    def read(groups: Groups) -> hg.TS[int]:
        window = groups["desk"][0]
        if window.modified:
            departures.extend(window.window_delta["removed"])
        return len(window)

    @hg.graph
    def app(command: hg.TS[str]) -> hg.TS[int]:
        return read(write_group(command))

    trace = hg.eval_node(app, ["4", revive])
    assert trace == [1, 1, None, 0]
    assert departures == [{"value": 4, "time": hg.MIN_ST}]


def test_nested_window_arrival_at_its_deadline_publishes_one_ordered_edit():
    changes = []

    @hg.compute_node
    def read(groups: Groups) -> hg.TS[int]:
        window = groups["desk"][0]
        change = window.window_delta
        changes.append((bool(change["cleared"]), tuple(change["added"]),
                        [(s["value"], s["time"]) for s in change["removed"]]))
        return len(window)

    @hg.graph
    def app(command: hg.TS[str]) -> hg.TS[int]:
        return read(write_group(command))

    assert hg.eval_node(app, ["4", "5", None, "9"]) == [1, 2, None, 2, 1, None, 0]
    assert changes == [
        (False, (4,), []), (False, (5,), []),
        (False, (9,), [(4, hg.MIN_ST)]), (False, (), [(5, hg.MIN_ST + hg.MIN_TD)]),
        (False, (), [(9, hg.MIN_ST + 3 * hg.MIN_TD)]),
    ]


def test_replay_of_a_dynamic_slot_observes_its_recorded_quiet_interval():
    @hg.compute_node(valid=("groups",))
    def read(groups: Groups, probe: hg.SIGNAL) -> hg.TS[int]:
        return len(groups["desk"][0])

    # The harness is an authoritative producer. Its t0 observation stays held
    # past the live t3 deadline and is still visible at the independent t4 probe.
    assert hg.eval_node(read, [{"desk": {0: 7}}], [None, None, None, None, True]) == [1, None, None, None, 1]


def test_removing_other_keys_and_positions_leaves_a_siblings_deadline_armed():
    @hg.compute_node
    def read(groups: Groups) -> hg.TS[int]:
        return sum(sum(lane.value) for lanes in groups.values() for lane in lanes.values())

    @hg.graph
    def app(command: hg.TS[str]) -> hg.TS[int]:
        return read(write_group(command))

    # Of the three t0 samples only desk[0] survives t1. It expires at t3,
    # leaving the t2 sample in that same lane until its own deadline at t5.
    assert hg.eval_node(app, ["seed siblings", "drop siblings", "7"]) == [10, 2, 9, 7, None, 0]


class PriceRow(hg.TimeSeriesSchema):
    prices: hg.TSD[str, Window]


def test_removed_list_item_keeps_its_bundled_dictionary_window_readable():
    rows_type = hg.TSL[hg.TSB[PriceRow], hg.Size[-1]]
    retained = []

    @hg.compute_node
    def write(seed: hg.TS[bool], _output: rows_type = None) -> rows_type:
        if seed.value:
            return {0: {"prices": {"old": 7}}}
        _output.resize(0)

    @hg.compute_node
    def read(rows: rows_type) -> hg.TS[int]:
        for index, row in rows.removed_items():
            price = row.prices["old"]
            retained.append((index, tuple(price.value), tuple(price.value_times)))
        return len(rows)

    @hg.graph
    def app(seed: hg.TS[bool]) -> hg.TS[int]:
        return read(write(seed))

    assert hg.eval_node(app, [True, False]) == [1, 0]
    assert retained == [(0, (7,), (hg.MIN_ST,))]
