"""Public graph-clock behavior for live, owned duration windows."""

from datetime import timedelta

import pytest

import hgraph as hg


def test_direct_owner_expires_equal_timestamp_samples_after_the_inclusive_boundary():
    start = hg.MIN_ST + 10 * hg.MIN_TD
    period = 2 * hg.MIN_TD
    duration = hg.WindowSize[period]
    immediate = hg.WindowSize[timedelta(0)]
    owner_calls = []
    removals = []

    @hg.compute_node
    def owner(
        seed: hg.TS[int], _output: hg.TSW_OUT[int, duration, immediate] = None,
    ) -> hg.TSW[int, duration, immediate]:
        owner_calls.append(seed.last_modified_time)
        _output.replace_samples([seed.value, seed.value + 1])

    @hg.compute_node(valid=("window",))
    def read(window: hg.TSW[int, hg.WINDOW_SIZE, hg.WINDOW_SIZE_MIN], probe: hg.SIGNAL) -> hg.TS[tuple[int, ...]]:
        if window.modified and window.window_delta["removed"]:
            removals.append((window.last_modified_time, window.window_delta["removed"]))
        return tuple(window.value)

    @hg.graph
    def app(seed: hg.TS[int], probe: hg.SIGNAL) -> hg.TS[tuple[int, ...]]:
        return read(owner(seed), probe)

    # Probe at the exact boundary and again after the one quiet expiry.
    assert hg.eval_node(app, [4], [None, None, True, None, None, True], __start_time__=start) == [
        (4, 5), None, (4, 5), (), None, (),
    ]
    assert owner_calls == [start]
    assert removals == [(start + period + hg.MIN_TD, [
        {"time": start, "value": 4}, {"time": start, "value": 5},
    ])]


def test_rebinding_a_window_reports_only_the_selected_owners_departure():
    observations = []

    @hg.compute_node
    def read(window: hg.TSW[int, hg.WINDOW_SIZE, hg.WINDOW_SIZE_MIN]) -> hg.TS[tuple[int, ...]]:
        observations.append(window.window_delta)
        return tuple(window.value)

    @hg.graph
    def app(select: hg.TS[bool], left: hg.TS[int], right: hg.TS[int]) -> hg.TS[tuple[int, ...]]:
        selected = hg.if_then_else(select, hg.to_window(left, 2 * hg.MIN_TD, 0 * hg.MIN_TD),
                                   hg.to_window(right, 2 * hg.MIN_TD, 0 * hg.MIN_TD))
        return read(selected)

    assert hg.eval_node(app, [True, False], [3], [7]) == [(3,), (7,), None, ()]
    assert len(observations) == 3
    assert observations[1]["replacement"] == [{"time": hg.MIN_ST, "value": 7}]
    assert observations[2]["removed"] == [{"time": hg.MIN_ST, "value": 7}]


def test_stopping_the_last_window_owner_cancels_its_graph_wakeup():
    root_times = []

    class ObserveRoot(hg.EvaluationLifeCycleObserver):
        def on_before_graph_evaluation(self, graph):
            if graph.graph_id == ():
                root_times.append(graph.evaluation_clock.evaluation_time)

    @hg.graph
    def child(value: hg.TS[int]) -> hg.TS[int]:
        return hg.sum_(hg.to_window(value, 2 * hg.MIN_TD, 0 * hg.MIN_TD))

    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return hg.map_(child, values)

    assert hg.eval_node(app, [{"a": 4}, {"a": hg.REMOVE}], __observers__=[ObserveRoot()]) == [
        {"a": 4}, {"a": hg.REMOVE},
    ]
    assert root_times[:2] == [hg.MIN_ST, hg.MIN_ST + hg.MIN_TD]
    assert hg.MIN_ST + 3 * hg.MIN_TD not in root_times


def test_realtime_graph_delivers_a_quiet_departure_with_its_original_timestamp():
    period = timedelta(milliseconds=10)
    observations = []

    @hg.sink_node
    def read(window: hg.TSW[int, hg.WINDOW_SIZE, hg.WINDOW_SIZE_MIN], engine: hg.EvaluationEngineApi = None):
        observations.append((window.last_modified_time, tuple(window.value), window.window_delta["removed"]))
        if not len(window):
            engine.request_engine_stop()

    @hg.graph
    def app():
        read(hg.to_window(hg.const(7), period, timedelta(0)))

    # The end time bounds a missing expiry. A successful departure stops the run.
    hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3))
    (arrival, initial, _), (departure, final, removed) = observations
    assert initial == (7,)
    assert final == ()
    assert departure > arrival + period
    assert removed == [{"time": arrival, "value": 7}]


def test_lag_replays_the_departure_after_its_delay_with_the_original_sample_time():
    changes = []

    @hg.compute_node
    def read(window: hg.TSW[int, hg.WINDOW_SIZE, hg.WINDOW_SIZE_MIN]) -> hg.TS[int]:
        changes.append((window.last_modified_time, window.window_delta["removed"]))
        return len(window)

    @hg.graph
    def app(value: hg.TS[int]) -> hg.TS[int]:
        window = hg.to_window(value, 2 * hg.MIN_TD, timedelta(0))
        return read(hg.lag(window, hg.MIN_TD))

    assert hg.eval_node(app, [7]) == [None, 1, None, None, 0]
    assert changes == [
        (hg.MIN_ST + hg.MIN_TD, []),
        (hg.MIN_ST + 4 * hg.MIN_TD, [{"time": hg.MIN_ST, "value": 7}]),
    ]


def test_gated_window_preserves_its_held_state_until_the_operator_publishes():
    @hg.compute_node(valid=("window",))
    def read(window: hg.TSW[int, hg.WINDOW_SIZE, hg.WINDOW_SIZE_MIN], probe: hg.SIGNAL) -> hg.TS[int]:
        return sum(window.value)

    @hg.graph
    def first_two(value: hg.TS[int], probe: hg.SIGNAL) -> hg.TS[int]:
        window = hg.to_window(value, 2 * hg.MIN_TD, timedelta(0))
        return read(hg.take(window, 2), probe)

    @hg.graph
    def gated(value: hg.TS[int], gate: hg.TS[bool], probe: hg.SIGNAL) -> hg.TS[int]:
        window = hg.to_window(value, 2 * hg.MIN_TD, timedelta(0))
        return read(hg.filter_(gate, window), probe)

    assert hg.eval_node(first_two, [3, 7], [None, None, None, None, None, True]) == [3, 10, None, None, None, 10]
    assert hg.eval_node(gated, [3, 7], [True, False, None, None, None, True],
                        [None, None, None, None, True]) == [3, None, None, None, 3, 0]


def test_sampling_a_list_of_windows_keeps_the_source_times_in_every_snapshot():
    observed = []

    @hg.compute_node
    def read(windows: hg.TSL[hg.TSW[int, hg.WINDOW_SIZE, hg.WINDOW_SIZE_MIN], hg.Size[1]]) -> hg.TS[int]:
        window = windows[0]
        observed.append((tuple(window.value_times), window.window_delta["replacement"]))
        return len(window)

    @hg.graph
    def app(trigger: hg.SIGNAL, values: hg.TS[int]) -> hg.TS[int]:
        windows = hg.TSL.from_ts(hg.to_window(values, 3 * hg.MIN_TD, timedelta(0)))
        return read(hg.sample(trigger, windows))

    assert hg.eval_node(app, [None, None, True, None, True], [4, 7]) == [None, None, 2, None, 1]
    first = {"value": 4, "time": hg.MIN_ST}
    second = {"value": 7, "time": hg.MIN_ST + hg.MIN_TD}
    assert observed == [
        ((first["time"], second["time"]), [first, second]),
        ((second["time"],), [second]),
    ]


def test_sampling_a_partial_plain_list_withdraws_and_recovers_its_first_slot():
    pair = hg.TSL[hg.TS[int], hg.Size[2]]

    @hg.compute_node
    def publish(phase: hg.TS[int], _output: hg.TSL = None) -> pair:
        if phase.value == 0:
            return (4, 9)
        if phase.value == 1:
            _output[0].invalidate()
        else:
            return {0: 6}

    @hg.compute_node
    def first(values: pair) -> hg.TS[int]:
        return values[0].value if values[0].valid else -1

    @hg.graph
    def app(trigger: hg.SIGNAL, phase: hg.TS[int]) -> hg.TS[int]:
        return first(hg.sample(trigger, publish(phase)))

    assert hg.eval_node(app, [True, None, True, None, True], [0, 1, None, 2]) == [4, None, -1, None, 6]


def test_eval_node_window_input_replays_its_authored_quiet_interval():
    period = hg.WindowSize[2 * hg.MIN_TD]
    minimum = hg.WindowSize[timedelta(0)]

    @hg.compute_node
    def read(window: hg.TSW[int, period, minimum]) -> hg.TS[int]:
        return sum(window.value)

    # The dense harness normalizes the legacy arrivals into timestamped deltas.
    # The second entry evicts the first; the replay owns both publication times.
    assert hg.eval_node(read, [7, None, None, None, None, 9]) == [7, None, None, None, None, 9]


@pytest.mark.parametrize("operator, release", [("throttle", 5), ("time_lag", 6), ("proxy_lag", 5), ("gate", 5)])
def test_buffered_window_releases_one_snapshot_with_the_last_surviving_sample(operator, release):
    samples = []

    @hg.compute_node
    def read(window: hg.TSW[int, hg.WINDOW_SIZE, hg.WINDOW_SIZE_MIN]) -> hg.TS[int]:
        samples.append((window.last_modified_time, window.window_delta))
        return len(window)

    @hg.graph
    def app(value: hg.TS[int], delay: hg.TS[timedelta], pulse: hg.TS[bool], opened: hg.TS[bool]) -> hg.TS[int]:
        window = hg.to_window(value, 2 * hg.MIN_TD, timedelta(0))
        if operator == "throttle":
            return read(hg.throttle(window, 5 * hg.MIN_TD, delay_first_tick=True))
        if operator == "time_lag":
            return read(hg.lag(window, delay))
        if operator == "proxy_lag":
            return read(hg.lag(window, 1, pulse))
        return read(hg.gate(opened, window, -1))

    # 3 arrives at t0 and leaves at t3. 7 arrives at t4. The buffered
    # operator releases once after all three events, while 7 is still held.
    result = hg.eval_node(app, [3, None, None, None, 7],
                          [6 * hg.MIN_TD, None, None, 3 * hg.MIN_TD, 2 * hg.MIN_TD],
                          [True, None, None, None, None, True, None, None, None, True],
                          [False, None, None, None, None, True])
    assert result[:release + 1] == [None] * release + [1]
    time, first = samples[0]
    assert time == hg.MIN_ST + release * hg.MIN_TD
    assert first["replacement"] == [{"value": 7, "time": hg.MIN_ST + 4 * hg.MIN_TD}]
    assert [size for size in result if size is not None] == [1, 0]


_TwoMicrosecondWindow = hg.TSW[int, hg.WindowSize[2 * hg.MIN_TD], hg.WindowSize[timedelta(0)]]
_KeyedWindows = hg.TSD[str, _TwoMicrosecondWindow]


@hg.compute_node
def _append_keyed_sample(value: hg.TS[int]) -> _KeyedWindows:
    return {"live": value.value}


@hg.compute_node
def _keyed_window_size(windows: _KeyedWindows) -> hg.TS[int]:
    return len(windows["live"].value)


@hg.graph
def _keyed_window_graph(value: hg.TS[int]) -> hg.TS[int]:
    return _keyed_window_size(_append_keyed_sample(value))


def test_late_realtime_window_evaluates_each_scheduled_departure_at_its_logical_time():
    # Start in the wall clock's past so the executor is already late for
    # both departures. No sleeps or wall-clock tolerance enter the assertion.
    start = hg.MIN_ST
    trace = hg.eval_node(_keyed_window_graph, [7, 2], __run_mode__=hg.EvaluationMode.REAL_TIME,
                         __start_time__=start, __end_time__=start + 6 * hg.MIN_TD)
    assert trace == [1, 2, None, 1, 0]


def test_recreating_a_mapped_graph_schedules_only_its_new_dynamic_windows():
    @hg.graph
    def desks(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return hg.map_(_keyed_window_graph, values)

    # The removed desk's old deadline at 3 is quiet. Its replacement, born
    # at 2 with a fresh history, delivers its own quiet departure at 5.
    assert hg.eval_node(desks, [{"desk": 7}, {"desk": hg.REMOVE}, {"desk": 9}]) == [
        {"desk": 1}, {"desk": hg.REMOVE}, {"desk": 1}, None, None, {"desk": 0},
    ]


@pytest.mark.parametrize("run_mode", [hg.EvaluationMode.SIMULATION, hg.EvaluationMode.REAL_TIME])
def test_window_removal_is_readable_by_after_graph_observers_and_its_copy_outlives_the_run(run_mode):
    snapshots = []
    owners = set()

    class InspectWindow(hg.EvaluationLifeCycleObserver):
        def on_after_graph_evaluation(self, graph):
            for node in graph.nodes:
                if node.node_id in owners:
                    change = node.output.window_delta
                    if change and change["removed"]:
                        snapshots.append(change)

    @hg.compute_node
    def owner(value: hg.TS[int], node: hg.NODE = None) -> _TwoMicrosecondWindow:
        owners.add(node.node_id)
        return value.value

    @hg.compute_node
    def size(window: _TwoMicrosecondWindow) -> hg.TS[int]:
        return len(window)

    @hg.graph
    def app(value: hg.TS[int]) -> hg.TS[int]:
        return size(owner(value))

    assert hg.eval_node(app, [4, 5], __run_mode__=run_mode, __start_time__=hg.MIN_ST,
                        __end_time__=hg.MIN_ST + 6 * hg.MIN_TD, __observers__=[InspectWindow()]) == [
        1, 2, None, 1, 0,
    ]
    assert [change["removed"] for change in snapshots] == [
        [{"value": 4, "time": hg.MIN_ST}], [{"value": 5, "time": hg.MIN_ST + hg.MIN_TD}],
    ]


def test_releasing_a_completed_run_twice_preserves_its_recorded_fixed_window_eviction():
    import _hgraph

    window = hg.TSW[int, hg.WindowSize[1], hg.WindowSize[1]]
    wiring = _hgraph.Wiring()
    source = wiring.wire("replay", ("entries",), {}, output_type=window.handle)
    wiring.set_replay("entries", [4, 9], window.handle)
    wiring.wire("record", (source, "prices"), {})
    run = wiring.run()
    captured = run.recorded("prices")[1]
    run.release_completed_step()
    run.release_completed_step()

    # Both the independent Python object and a new read of the stored record
    # still describe the sample that left the fixed-count window.
    expected = [{"value": 4, "time": hg.MIN_ST}]
    assert captured["removed"] == expected
    assert run.recorded("prices")[1]["removed"] == expected
    assert list(captured["added"]) == [9]


def test_fixed_window_departure_copies_keep_their_original_values_and_times_after_ring_wraps():
    departures = []

    @hg.compute_node
    def observe(window: hg.TSW[int, hg.WINDOW_SIZE, hg.WINDOW_SIZE_MIN]) -> hg.TS[tuple[int, ...]]:
        change = window.window_delta
        if change["removed"]:
            departures.append(change)
        return tuple(window.value)

    @hg.graph
    def rolling(value: hg.TS[int]) -> hg.TS[tuple[int, ...]]:
        return observe(hg.to_window(value, 2, 1))

    assert hg.eval_node(rolling, [10, 20, 30, 40, 50, 60]) == [
        (10,), (10, 20), (20, 30), (30, 40), (40, 50), (50, 60),
    ]
    assert [change["removed"] for change in departures] == [
        [{"value": value, "time": hg.MIN_ST + offset * hg.MIN_TD}]
        for offset, value in enumerate((10, 20, 30, 40))
    ]
