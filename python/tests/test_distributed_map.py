"""The importable children here also run in fresh worker interpreters."""
import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import hgraph as hg
from hgraph import TS, TSD


@hg.compute_node
def running_total(ts: TS[int], state: hg.STATE = None,
                  engine: hg.EvaluationEngineApi = None) -> TS[int]:
    assert engine.evaluation_mode == "externally_driven"
    state.total = getattr(state, "total", 0) + ts.value
    return state.total


@hg.compute_node
def worker_pid(ts: TS[int]) -> TS[int]:
    return os.getpid()


@hg.compute_node
def delayed(ts: TS[int], state: hg.STATE = None, scheduler: hg.SCHEDULER = None) -> TS[int]:
    if ts.modified:
        state.pending = ts.value
        scheduler.schedule(hg.MIN_TD)
    elif scheduler.is_scheduled_now:
        return state.pending


@hg.compute_node
def broken(ts: TS[int]) -> TS[int]:
    raise ValueError("distributed child failure")


@hg.graph
def native_child(ts: TS[int]) -> TS[int]:
    return ts * 3


@hg.compute_node
def echo_float(ts: TS[float]) -> TS[float]:
    return ts.value


@hg.compute_node
def echo_text(ts: TS[str]) -> TS[str]:
    return ts.value


@hg.compute_node
def echo_bool(ts: TS[bool]) -> TS[bool]:
    return ts.value


@hg.compute_node
def echo_time(ts: TS[datetime]) -> TS[datetime]:
    return ts.value


@hg.compute_node
def echo_duration(ts: TS[timedelta]) -> TS[timedelta]:
    return ts.value


@hg.compute_node
def tuple_total(ts: TS[tuple[int, ...]]) -> TS[int]:
    return sum(ts.value)


@hg.compute_node
def with_key(key: TS[str], ts: TS[int]) -> TS[str]:
    return f"{key.value}:{ts.value}"


@hg.compute_node
def lifecycle_child(ts: TS[int], engine: hg.EvaluationEngineApi = None) -> TS[int]:
    assert engine.start_time < engine.end_time
    return ts.value


@lifecycle_child.stop
def lifecycle_stop():
    Path(os.environ["HGRAPH_TEST_DMAP_STOP_FILE"]).write_text("stopped", encoding="utf-8")


@hg.compute_node
def stop_failure(ts: TS[int]) -> TS[int]:
    return ts.value


@stop_failure.stop
def fail_stop():
    raise ValueError("distributed stop failure")


def run(child, events, workers=2, in_process=False):
    @hg.graph
    def graph(ts: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return hg.dmap_(child, ts, __workers__=workers, in_process=in_process)
    return hg.eval_node(graph, events)


@pytest.mark.parametrize("in_process", [True, False])
@pytest.mark.parametrize("workers", [1, 3])
def test_state_churn_and_quiet_cycles(workers, in_process):
    events = [{"a": 1, "b": 2}, {"a": 3}, {"b": hg.REMOVE}, None, {"b": 7}]
    assert run(running_total, events, workers, in_process) == [
        {"a": 1, "b": 2}, {"a": 4}, {"b": hg.REMOVE}, None, {"b": 7}]


def test_default_hosting_uses_a_separate_process():
    output = run(worker_pid, [{"a": 1}])
    assert output[0]["a"] > 0
    assert output[0]["a"] != os.getpid()


@pytest.mark.parametrize("in_process", [True, False])
def test_worker_schedules_are_driven_by_the_parent(in_process):
    @hg.graph
    def local(ts: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return hg.map_(delayed, ts)
    events = [{"a": 1}, None, {"a": 3}, None]
    expected = hg.eval_node(local, events)
    assert expected == [{}, {"a": 1}, None, {"a": 3}]
    assert run(delayed, events, in_process=in_process) == expected


@pytest.mark.parametrize("in_process", [True, False])
def test_python_graph_composes_native_nodes(in_process):
    assert run(native_child, [{"a": 4}, {"b": 2}], in_process=in_process) == [
        {"a": 12}, {"b": 6}]


def test_worker_exception_reaches_the_client():
    with pytest.raises(Exception, match="distributed child failure"):
        run(broken, [{"a": 1}])
    assert run(native_child, [{"a": 2}]) == [{"a": 6}]


def test_local_callable_is_refused_before_launch():
    with pytest.raises(Exception, match="importable module"):
        run(lambda ts: ts + 1, [{"a": 1}])


@pytest.mark.parametrize("workers", [0, -1, True, 1.5])
def test_invalid_worker_counts(workers):
    with pytest.raises(Exception, match="positive integer"):
        run(native_child, [{"a": 1}], workers)


@pytest.mark.parametrize("child,input_type,output_type,value", [
    (echo_float, float, float, 3.25),
    (echo_text, str, str, "hello\0\u03bb"),
    (echo_bool, bool, bool, False),
    (echo_time, datetime, datetime, datetime(2024, 3, 4, 5, 6)),
    (echo_duration, timedelta, timedelta, timedelta(seconds=17)),
    (tuple_total, tuple[int, ...], int, (2, 3, 7)),
])
def test_native_binary_schema_families(child, input_type, output_type, value):
    @hg.graph
    def graph(ts: TSD[int, TS[input_type]]) -> TSD[int, TS[output_type]]:
        return hg.dmap_(child, ts)
    expected = sum(value) if child is tuple_total else value
    assert hg.eval_node(graph, [{4: value}]) == [{4: expected}]


def test_mapped_key_injection():
    @hg.graph
    def graph(ts: TSD[str, TS[int]]) -> TSD[str, TS[str]]:
        return hg.dmap_(with_key, ts)
    assert hg.eval_node(graph, [{"a": 2, "b": 3}]) == [{"a": "a:2", "b": "b:3"}]


def test_worker_stop_completes_before_run_returns(tmp_path, monkeypatch):
    stopped = tmp_path / "stop.txt"
    monkeypatch.setenv("HGRAPH_TEST_DMAP_STOP_FILE", str(stopped))
    assert run(lifecycle_child, [{"a": 2}]) == [{"a": 2}]
    assert stopped.read_text(encoding="utf-8") == "stopped"


@pytest.mark.parametrize("in_process", [True, False])
def test_stop_errors_fail_the_run_and_release_the_pool(in_process):
    with pytest.raises(Exception, match="distributed stop failure|worker exited with code"):
        run(stop_failure, [{"a": 2}], in_process=in_process)
    assert run(native_child, [{"a": 2}], in_process=in_process) == [{"a": 6}]
