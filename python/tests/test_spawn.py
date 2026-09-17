"""Process isolation and behavioral parity through the public spawn_ API."""
from datetime import timedelta
import os
from pathlib import Path
import pickle
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


@hg.sink_node
def record(value: hg.TIME_SERIES_TYPE, path: str, delay: float = 0.0, clock: hg.CLOCK = None):
    if delay:
        time.sleep(delay)
    # Test-only trace serialization, never used by the worker bootstrap or IPC.
    with open(f"{path}.{os.getpid()}", "ab") as stream:
        pickle.dump((clock.evaluation_time, value.value, value.delta_value, os.getpid()), stream)


@record.start
def record_start(path: str):
    Path(f"{path}.start.{os.getpid()}").touch()


@record.stop
def record_stop(path: str):
    Path(f"{path}.stop.{os.getpid()}").touch()


class Trace:
    def __init__(self, path):
        self.path = str(path)

    def rows(self):
        rows = []
        for path in sorted(Path(self.path).parent.glob(Path(self.path).name + ".*")):
            if not path.suffix[1:].isdigit() or ".start." in path.name or ".stop." in path.name:
                continue
            with path.open("rb") as stream:
                while True:
                    try:
                        rows.append(pickle.load(stream))
                    except EOFError:
                        break
        return rows

    def values(self):
        return [row[:3] for row in self.rows()]

    def stage(self, delay=0.0):
        return hg.bind_(record, path=self.path, delay=delay)

    def lifecycle(self, phase):
        return list(Path(self.path).parent.glob(Path(self.path).name + f".{phase}.*"))


@hg.compute_node
def delayed(value: hg.TS[int], scheduler: hg.SCHEDULER = None,
            state: hg.STATE = None) -> hg.TS[int]:
    if value.modified:
        state.pending = value.value
        scheduler.schedule(hg.MIN_TD)
    elif scheduler.is_scheduled_now:
        return state.pending


@hg.compute_node
def slow(value: hg.TS[int]) -> hg.TS[int]:
    time.sleep(0.001)
    return value.value


@hg.graph
def combine(value: hg.TS[int], side: hg.TS[int]) -> hg.TS[int]:
    return value + side


@hg.graph
def timer(path: str, delay: timedelta = 3 * hg.MIN_TD) -> None:
    record(hg.const(7, delay=delay), path)


@hg.sink_node
def broken(value: hg.TS[int], path: str, phase: str = "eval"):
    if phase == "eval":
        raise ValueError("spawn eval failure")
    if phase == "crash":
        os._exit(23)
    if phase == "hang":
        time.sleep(30)


@broken.start
def broken_start(phase: str, path: str, engine: hg.EvaluationEngineApi = None):
    Path(path + ".pid").write_text(str(os.getpid()))
    if phase == "start":
        raise ValueError("spawn start failure")
    if phase == "stop_request":
        engine.request_engine_stop()


@broken.stop
def broken_stop(phase: str, path: str):
    Path(path + ".stopped").touch()
    if phase == "stop":
        raise ValueError("spawn stop failure")


@hg.graph
def stop_child(value: hg.TS[bool]) -> None:
    hg.stop_engine(value)


@hg.compute_node
def total(value: hg.TS[int], state: hg.STATE = None) -> hg.TS[int]:
    state.total = getattr(state, "total", 0) + value.value
    return state.total


@hg.graph
def nested(value: hg.TSD[str, hg.TS[int]], operation: str) -> hg.TIME_SERIES_TYPE:
    if operation == "map":
        return hg.map_(total, value)
    if operation == "mesh":
        return hg.mesh_(total, value)
    return hg.reduce(hg.add_, value, 0)


@hg.compute_node(valid=("value",))
def signal_stage(value: hg.TS[int], pulse: hg.SIGNAL) -> hg.TS[tuple[int, bool]]:
    return value.value, pulse.modified


@pytest.mark.parametrize("capacity", [1, 3, 256])
def test_sink_is_ordered_drained_and_runs_in_child_process(tmp_path, capacity):
    trace = Trace(tmp_path / "trace")
    @hg.graph
    def app(value: hg.TS[int]) -> None:
        assert hg.spawn_(trace.stage(0.0001), value, __capacity_frames__=capacity) is None
    assert hg.eval_node(app, list(range(50))) is None
    assert [v for _, v, _ in trace.values()] == list(range(50))
    pids = {row[3] for row in trace.rows()}
    assert len(pids) == 1 and os.getpid() not in pids
    assert len(trace.lifecycle("start")) == len(trace.lifecycle("stop")) == 1


def test_pipeline_scalar_config_named_inputs_and_fragments(tmp_path):
    trace = Trace(tmp_path / "trace")
    fragment = hg.pipeline_([hg.bind_(multiply, factor=3), identity])
    @hg.graph
    def app(value: hg.TS[int]) -> None:
        hg.spawn_(hg.pipeline_([fragment, multiply, trace.stage()]), value=value, __capacity_frames__=1)
    hg.eval_node(app, [1, 2, None, 7])
    assert trace.values() == [(hg.MIN_ST + n * hg.MIN_TD, v, v) for n, v in [(0, 6), (1, 12), (3, 42)]]


def test_external_binding_uses_historical_value_and_is_active(tmp_path):
    trace, expected = Trace(tmp_path / "trace"), Trace(tmp_path / "expected")
    @hg.graph
    def app(value: hg.TS[int], side: hg.TS[int]) -> None:
        hg.spawn_(hg.pipeline_([slow, hg.bind_(combine, side=side), trace.stage()]), value, __capacity_frames__=1)
        record(combine(value, side), expected.path)
    hg.eval_node(app, [1, None, 3, None, 5], [10, 20, None, 40, 50])
    assert trace.values() == expected.values()
    assert [v for _, v, _ in trace.values()] == [11, 21, 23, 43, 55]


def test_pending_child_timer_requests_parent_progress_without_input_ticks(tmp_path):
    trace = Trace(tmp_path / "trace")
    @hg.graph
    def app(value: hg.TS[int]) -> None:
        hg.spawn_(hg.pipeline_([delayed, trace.stage()]), value)
    hg.eval_node(app, [7])
    assert trace.values() == [(hg.MIN_ST + hg.MIN_TD, 7, 7)]


def test_no_formal_inputs_can_request_clock_progress(tmp_path):
    trace = Trace(tmp_path / "trace")
    @hg.graph
    def app() -> None:
        hg.spawn_(hg.bind_(timer, path=trace.path))
    hg.eval_node(app)
    assert trace.values() == [(hg.MIN_ST + 3 * hg.MIN_TD, 7, 7)]


def test_child_timer_cannot_pass_explicit_parent_end_time(tmp_path):
    trace = Trace(tmp_path / "trace")
    @hg.graph
    def app() -> None:
        hg.spawn_(hg.bind_(timer, path=trace.path, delay=timedelta(days=1)))
    hg.eval_node(app, __end_time__=hg.MIN_ST + timedelta(seconds=1))
    assert trace.values() == []


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
def test_structural_value_and_delta_match_synchronous_graph(tmp_path, schema, events):
    trace, expected = Trace(tmp_path / "trace"), Trace(tmp_path / "expected")
    @hg.graph
    def app(value: schema) -> None:
        hg.spawn_(hg.pipeline_([identity, trace.stage()]), value, __capacity_frames__=1)
        record(value, expected.path)
    hg.eval_node(app, events)
    assert trace.values() == expected.values()


@pytest.mark.parametrize("period", [3, timedelta(microseconds=3)])
def test_windows_keep_complete_history_and_timestamps(tmp_path, period):
    trace, expected = Trace(tmp_path / "trace"), Trace(tmp_path / "expected")
    minimum = 1 if isinstance(period, int) else hg.MIN_TD
    @hg.graph
    def app(value: hg.TS[int]) -> None:
        window = hg.to_window(value, period, minimum)
        hg.spawn_(hg.pipeline_([identity, trace.stage(0.0001)]), window, __capacity_frames__=1)
        record(window, expected.path)
    hg.eval_node(app, [1, 2, None, 4, 5, 6])
    actual, reference = trace.values(), expected.values()
    assert len(actual) == len(reference)
    for a, b in zip(actual, reference):
        assert a[0] == b[0]
        np.testing.assert_equal(a[1], b[1])
        np.testing.assert_equal(a[2], b[2])


@pytest.mark.parametrize("phase", ["start", "eval", "stop", "crash", "hang", "stop_request"])
def test_worker_failures_propagate_and_teardown_is_bounded(tmp_path, phase):
    path = str(tmp_path / "failure")
    @hg.graph
    def app(value: hg.TS[int]) -> None:
        hg.spawn_(hg.bind_(broken, path=path, phase=phase), value,
                  __capacity_frames__=1, __worker_timeout__=3.0)
    before = time.monotonic()
    pattern = {"crash": "closed|exited|pipe|connection|Broken", "hang": "deadline|timed out|timeout",
               "stop_request": "child requested stop during start"}.get(phase, f"spawn {phase} failure")
    with pytest.raises(Exception, match=pattern):
        hg.eval_node(app, list(range(20)))
    assert time.monotonic() - before < 15
    if phase in ("eval", "stop", "stop_request"):
        assert Path(path + ".stopped").exists()
    pidfile = Path(path + ".pid")
    if pidfile.exists() and os.name != "nt":
        with pytest.raises(ProcessLookupError):
            os.kill(int(pidfile.read_text()), 0)


def test_identical_spawn_calls_have_independent_processes(tmp_path):
    trace = Trace(tmp_path / "trace")
    @hg.graph
    def app(value: hg.TS[int]) -> None:
        hg.spawn_(trace.stage(), value)
        hg.spawn_(trace.stage(), value)
    hg.eval_node(app, [1, 2])
    assert sorted(v for _, v, _ in trace.values()) == [1, 1, 2, 2]
    assert len({row[3] for row in trace.rows()}) == 2
    assert len(trace.lifecycle("start")) == len(trace.lifecycle("stop")) == 2


def test_frame_larger_than_byte_capacity_fails_instead_of_waiting(tmp_path):
    trace = Trace(tmp_path / "trace")
    @hg.graph
    def app(value: hg.TS[str]) -> None:
        hg.spawn_(trace.stage(), value, __capacity_bytes__=8)
    with pytest.raises(Exception, match="capacity_bytes"):
        hg.eval_node(app, ["a" * 100])


def test_child_request_stop_fails_closed():
    @hg.graph
    def app(value: hg.TS[bool]) -> None:
        hg.spawn_(stop_child, value)
    with pytest.raises(Exception, match="child requested stop"):
        hg.eval_node(app, [True, True, True])


@pytest.mark.parametrize("operation", ["map", "mesh", "reduce"])
def test_nested_graphs_retain_membership_and_state(tmp_path, operation):
    trace, expected = Trace(tmp_path / "trace"), Trace(tmp_path / "expected")
    @hg.graph
    def app(value: hg.TSD[str, hg.TS[int]]) -> None:
        hg.spawn_(hg.pipeline_([hg.bind_(nested, operation=operation), trace.stage()]), value)
        record(nested(value, operation), expected.path)
    hg.eval_node(app, [{"a": 1, "b": 2}, {"a": 3}, {"b": hg.REMOVE}, {"b": 5}, {"a": hg.REMOVE, "b": hg.REMOVE}])
    assert trace.values() == expected.values()


def test_signal_bound_input_preserves_tick_times(tmp_path):
    trace, expected = Trace(tmp_path / "trace"), Trace(tmp_path / "expected")
    @hg.graph
    def app(value: hg.TS[int], pulse: hg.SIGNAL) -> None:
        hg.spawn_(hg.pipeline_([identity, hg.bind_(signal_stage, pulse=pulse), trace.stage()]), value)
        record(signal_stage(value, pulse), expected.path)
    hg.eval_node(app, [1, None, 3, None], [True, True, None, True])
    assert trace.values() == expected.values()


def test_spawn_requires_sink_and_pipeline_rejects_ambiguous_flow(tmp_path):
    trace = Trace(tmp_path / "trace")
    @hg.graph
    def non_sink(value: hg.TS[int]) -> None:
        hg.spawn_(identity, value)
    with pytest.raises(Exception, match="sink"):
        hg.eval_node(non_sink, [1])
    @hg.graph
    def ambiguous(value: hg.TS[int]) -> None:
        hg.spawn_(hg.pipeline_([identity, combine, trace.stage()]), value)
    with pytest.raises(Exception, match="one|1|unbound"):
        hg.eval_node(ambiguous, [1])


def test_bind_rejects_duplicate_and_unknown_parameters(tmp_path):
    trace = Trace(tmp_path / "trace")
    with pytest.raises(TypeError, match="already bound"):
        hg.bind_(hg.bind_(multiply, factor=2), factor=3)
    @hg.graph
    def app(value: hg.TS[int]) -> None:
        hg.spawn_(hg.pipeline_([hg.bind_(multiply, typo=2), trace.stage()]), value)
    with pytest.raises(TypeError, match="typo"):
        hg.eval_node(app, [1])


@pytest.mark.parametrize("capacity", [0, -1, True, 1.5])
def test_invalid_capacities_rejected(capacity):
    with pytest.raises(ValueError, match="positive integer"):
        hg.spawn_(record, __capacity_frames__=capacity)


@pytest.mark.parametrize("timeout", [0, -1, True, float("inf"), float("nan"), 86401])
def test_invalid_timeouts_rejected(timeout):
    with pytest.raises(ValueError, match="positive"):
        hg.spawn_(record, __worker_timeout__=timeout)


def test_empty_pipeline_rejected():
    with pytest.raises(ValueError, match="at least one"):
        hg.pipeline_([])


def test_closure_stage_is_rejected_before_launch():
    @hg.graph
    def app(value: hg.TS[int]) -> None:
        @hg.sink_node
        def child(value: hg.TS[int]):
            pass
        hg.spawn_(child, value)
    with pytest.raises(Exception, match="importable|closure"):
        hg.eval_node(app, [1])
