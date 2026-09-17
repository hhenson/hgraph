"""Python authoring parity for native parent-clocked asynchronous pipelines."""
from datetime import timedelta
import threading
import time

import hgraph as hg
import numpy as np
import pytest


@hg.graph
def identity(value: hg.TIME_SERIES_TYPE) -> hg.TIME_SERIES_TYPE:
    return value


@hg.graph
def multiply(value: hg.TS[int], factor: int = 2) -> hg.TS[int]:
    return value * factor


@hg.compute_node
def delayed(value: hg.TS[int], scheduler: hg.SCHEDULER = None,
            state: hg.STATE = None) -> hg.TS[int]:
    if value.modified:
        state.pending = value.value
        scheduler.schedule(hg.MIN_TD)
    elif scheduler.is_scheduled_now:
        return state.pending


def collector(trace, *, delay=0):
    @hg.sink_node
    def collect(value: hg.TIME_SERIES_TYPE, clock: hg.CLOCK = None):
        if delay:
            time.sleep(delay)
        trace.append((clock.evaluation_time, value.value, value.delta_value))
    return collect


@pytest.mark.parametrize("capacity", [1, 3, 256])
def test_sink_is_ordered_drained_and_runs_on_child_thread(capacity):
    trace, threads = [], []

    @hg.sink_node
    def child(value: hg.TS[int]):
        time.sleep(0.0001)
        threads.append(threading.get_ident())
        trace.append(value.value)

    @hg.graph
    def app(value: hg.TS[int]) -> None:
        assert hg.spawn_(child, value, __capacity_frames__=capacity) is None

    assert hg.eval_node(app, list(range(50))) is None
    assert trace == list(range(50))
    assert len(set(threads)) == 1
    assert threads[0] != threading.get_ident()


def test_pipeline_scalar_config_named_inputs_and_fragments():
    trace = []
    fragment = hg.pipeline_([hg.bind_(multiply, factor=3), identity])

    @hg.graph
    def app(value: hg.TS[int]) -> None:
        hg.spawn_(hg.pipeline_([fragment, multiply, collector(trace)]), value=value,
                  __capacity_frames__=1)

    hg.eval_node(app, [1, 2, None, 7])
    assert [v for _, v, _ in trace] == [6, 12, 42]
    assert [t for t, _, _ in trace] == [hg.MIN_ST, hg.MIN_ST + hg.MIN_TD,
                                        hg.MIN_ST + 3 * hg.MIN_TD]


def test_external_binding_uses_historical_value_and_is_active():
    trace, expected = [], []

    @hg.compute_node
    def slow(value: hg.TS[int]) -> hg.TS[int]:
        time.sleep(0.001)
        return value.value

    @hg.graph
    def combine(value: hg.TS[int], side: hg.TS[int]) -> hg.TS[int]:
        return value + side

    @hg.graph
    def app(value: hg.TS[int], side: hg.TS[int]) -> None:
        hg.spawn_(hg.pipeline_([
            slow, hg.bind_(combine, side=side), collector(trace),
        ]), value, __capacity_frames__=1)
        collector(expected)(combine(value, side))

    hg.eval_node(app, [1, None, 3, None, 5], [10, 20, None, 40, 50])
    assert trace == expected
    assert [v for _, v, _ in trace] == [11, 21, 23, 43, 55]


def test_pending_child_timer_requests_parent_progress_without_input_ticks():
    trace = []

    @hg.graph
    def app(value: hg.TS[int]) -> None:
        hg.spawn_(hg.pipeline_([delayed, collector(trace)]), value)

    hg.eval_node(app, [7])
    assert trace == [(hg.MIN_ST + hg.MIN_TD, 7, 7)]


def test_no_formal_inputs_can_request_clock_progress():
    trace = []

    @hg.graph
    def child() -> None:
        collector(trace)(hg.const(7, delay=3 * hg.MIN_TD))

    @hg.graph
    def app() -> None:
        hg.spawn_(child)

    hg.eval_node(app)
    assert trace == [(hg.MIN_ST + 3 * hg.MIN_TD, 7, 7)]


def test_child_timer_cannot_pass_explicit_parent_end_time():
    trace = []

    @hg.graph
    def child() -> None:
        collector(trace)(hg.const(7, delay=timedelta(days=1)))

    @hg.graph
    def app() -> None:
        hg.spawn_(child)

    hg.eval_node(app, __end_time__=hg.MIN_ST + timedelta(seconds=1))
    assert trace == []


class Row(hg.TimeSeriesSchema):
    count: hg.TS[int]
    label: hg.TS[str]


SCHEMAS = [
    (hg.TS[int], [1, None, 4]),
    (hg.TS[str], ["a", "", "b"]),
    (hg.TS[tuple[int, ...]], [(1, 2), (), (4,)]),
    (hg.TSS[str], [{"a", "b"}, {hg.Removed("a")}, {"c"}]),
    (hg.TSD[str, hg.TS[int]], [{"a": 1, "b": 2}, {"a": hg.REMOVE}, {"a": 3}]),
    (hg.TSL[hg.TS[int], hg.Size[2]], [{0: 1}, {1: 2}, {0: 5}]),
    (hg.TSB[Row], [{"count": 1}, {"label": "x"}, {"count": 3}]),
    (hg.TSD[str, hg.TSB[Row]], [{"a": {"count": 1}}, {"a": {"label": "x"}},
                              {"a": hg.REMOVE}, {"a": {"label": "y"}}]),
]


@pytest.mark.parametrize("schema,events", SCHEMAS)
def test_structural_value_and_delta_match_synchronous_graph(schema, events):
    trace, expected = [], []

    @hg.graph
    def app(value: schema) -> None:
        hg.spawn_(hg.pipeline_([identity, collector(trace)]), value,
                  __capacity_frames__=1)
        collector(expected)(value)

    hg.eval_node(app, events)
    assert trace == expected


@pytest.mark.parametrize("period", [3, timedelta(microseconds=3)])
def test_windows_keep_complete_history_and_timestamps(period):
    trace, expected = [], []
    minimum = 1 if isinstance(period, int) else hg.MIN_TD

    @hg.graph
    def app(value: hg.TS[int]) -> None:
        window = hg.to_window(value, period, minimum)
        hg.spawn_(hg.pipeline_([identity, collector(trace, delay=0.0001)]), window,
                  __capacity_frames__=1)
        collector(expected)(window)

    hg.eval_node(app, [1, 2, None, 4, 5, 6])
    assert len(trace) == len(expected)
    for actual, reference in zip(trace, expected):
        assert actual[0] == reference[0]
        np.testing.assert_equal(actual[1], reference[1])
        np.testing.assert_equal(actual[2], reference[2])


def test_child_failure_propagates_and_teardown_runs():
    stopped = []

    @hg.sink_node
    def broken(value: hg.TS[int]):
        raise ValueError("spawn child broke")

    @broken.stop
    def stop():
        stopped.append(True)

    @hg.graph
    def app(value: hg.TS[int]) -> None:
        hg.spawn_(broken, value, __capacity_frames__=1)

    with pytest.raises(Exception, match="spawn child broke"):
        hg.eval_node(app, list(range(20)))
    assert stopped == [True]


@pytest.mark.parametrize("phase", ["start", "stop"])
def test_child_lifecycle_failures_cross_thread_boundary(phase):
    @hg.sink_node
    def child(value: hg.TS[int]):
        pass

    def fail():
        raise ValueError(f"spawn {phase} failure")

    getattr(child, phase)(fail)

    @hg.graph
    def app(value: hg.TS[int]) -> None:
        hg.spawn_(child, value)

    with pytest.raises(Exception, match=f"spawn {phase} failure"):
        hg.eval_node(app, [1])


def test_identical_spawn_calls_have_independent_lifecycle_and_effects():
    trace = []
    sink = collector(trace)

    @hg.graph
    def app(value: hg.TS[int]) -> None:
        hg.spawn_(sink, value)
        hg.spawn_(sink, value)

    hg.eval_node(app, [1, 2])
    assert sorted(v for _, v, _ in trace) == [1, 1, 2, 2]


def test_frame_larger_than_byte_capacity_fails_instead_of_waiting():
    @hg.graph
    def app(value: hg.TS[str]) -> None:
        hg.spawn_(collector([]), value, __capacity_bytes__=8)

    with pytest.raises(Exception, match="capacity_bytes"):
        hg.eval_node(app, ["a" * 100])


def test_child_request_stop_fails_closed():
    @hg.graph
    def child(value: hg.TS[bool]) -> None:
        hg.stop_engine(value)

    @hg.graph
    def app(value: hg.TS[bool]) -> None:
        hg.spawn_(child, value)

    with pytest.raises(Exception, match="child requested stop"):
        hg.eval_node(app, [True, True, True])


@pytest.mark.parametrize("operation", ["map", "mesh", "reduce"])
def test_nested_graphs_retain_membership_and_state(operation):
    trace, expected = [], []

    @hg.compute_node
    def total(value: hg.TS[int], state: hg.STATE = None) -> hg.TS[int]:
        state.total = getattr(state, "total", 0) + value.value
        return state.total

    @hg.graph
    def stage(value: hg.TSD[str, hg.TS[int]]) -> hg.TIME_SERIES_TYPE:
        if operation == "map":
            return hg.map_(total, value)
        if operation == "mesh":
            return hg.mesh_(total, value)
        return hg.reduce(hg.add_, value, 0)

    @hg.graph
    def app(value: hg.TSD[str, hg.TS[int]]) -> None:
        hg.spawn_(hg.pipeline_([stage, collector(trace)]), value)
        collector(expected)(stage(value))

    hg.eval_node(app, [{"a": 1, "b": 2}, {"a": 3}, {"b": hg.REMOVE},
                       {"b": 5}, {"a": hg.REMOVE, "b": hg.REMOVE}])
    assert trace == expected


def test_signal_bound_input_preserves_tick_times():
    trace, expected = [], []

    @hg.compute_node(valid=("value",))
    def stage(value: hg.TS[int], pulse: hg.SIGNAL) -> hg.TS[tuple[int, bool]]:
        return value.value, pulse.modified

    @hg.graph
    def app(value: hg.TS[int], pulse: hg.SIGNAL) -> None:
        hg.spawn_(hg.pipeline_([identity, hg.bind_(stage, pulse=pulse), collector(trace)]), value)
        collector(expected)(stage(value, pulse))

    hg.eval_node(app, [1, None, 3, None], [True, True, None, True])
    assert trace == expected


def test_spawn_requires_sink_and_pipeline_rejects_ambiguous_flow():
    @hg.graph
    def non_sink(value: hg.TS[int]) -> None:
        hg.spawn_(identity, value)

    with pytest.raises(Exception, match="sink"):
        hg.eval_node(non_sink, [1])

    @hg.graph
    def two_inputs(left: hg.TS[int], right: hg.TS[int]) -> hg.TS[int]:
        return left + right

    @hg.graph
    def ambiguous(value: hg.TS[int]) -> None:
        hg.spawn_(hg.pipeline_([identity, two_inputs, collector([])]), value)

    with pytest.raises(Exception, match="one|1|unbound"):
        hg.eval_node(ambiguous, [1])


def test_bind_rejects_duplicate_and_unknown_parameters():
    with pytest.raises(TypeError, match="already bound"):
        hg.bind_(hg.bind_(multiply, factor=2), factor=3)

    @hg.graph
    def app(value: hg.TS[int]) -> None:
        hg.spawn_(hg.pipeline_([hg.bind_(multiply, typo=2), collector([])]), value)

    with pytest.raises(TypeError, match="typo"):
        hg.eval_node(app, [1])


@pytest.mark.parametrize("capacity", [0, -1, True, 1.5])
def test_invalid_capacities_rejected(capacity):
    with pytest.raises(ValueError, match="positive integer"):
        hg.spawn_(collector([]), __capacity_frames__=capacity)


def test_empty_pipeline_rejected():
    with pytest.raises(ValueError, match="at least one"):
        hg.pipeline_([])


def test_hidden_parent_port_capture_is_rejected():
    @hg.graph
    def app(value: hg.TS[int]) -> None:
        @hg.graph
        def child() -> None:
            collector([])(value)
        hg.spawn_(child)

    with pytest.raises(Exception, match="capture|wiring|graph|port"):
        hg.eval_node(app, [1])


def test_child_stop_during_start_fails_and_joins():
    @hg.sink_node
    def child(value: hg.TS[int]):
        pass

    @child.start
    def start(engine: hg.EvaluationEngineApi = None):
        engine.request_engine_stop()

    @hg.graph
    def app(value: hg.TS[int]) -> None:
        hg.spawn_(child, value)

    with pytest.raises(Exception, match="child requested stop during start"):
        hg.eval_node(app, [1])
