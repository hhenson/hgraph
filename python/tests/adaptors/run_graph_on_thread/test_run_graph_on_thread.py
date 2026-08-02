from datetime import datetime, timedelta, timezone

import hgraph as hg
import pytest

from hgraph.adaptors.run_graph_on_thread import (
    RunGraphOutput,
    publish_output,
    run_graph_on_thread,
    run_graph_on_thread_impl,
)
from hgraph.adaptors.run_graph_on_thread.run_graph_on_thread import (
    _OUTPUT_CALLBACK,
    _typed_output,
)


@hg.graph
def _sum_graph(a: hg.TS[int], b: hg.TS[int]):
    publish_output(a + b)


def test_typed_output_schema_can_be_used_by_eval_node():
    schema = RunGraphOutput[hg.TS[int]]
    assert hg.TSB[schema].handle.is_tsb

    @hg.graph
    def typed(raw: hg.TS[object]) -> hg.TS[int]:
        return _typed_output[hg.OUT : hg.TS[int]](raw)

    assert hg.eval_node(typed, [3, 4], resolution_dict={"raw": hg.TS[object]}) == [3, 4]


def test_typed_output_schema_supports_standard_scalar_schema_operations():
    schema = RunGraphOutput[hg.TS[int]]

    assert schema.scalar_type() is None
    scalar_schema = schema.to_scalar_schema()
    assert scalar_schema.__annotations__ == {
        "out": int,
        "started": bool,
        "finished": bool,
        "status": str,
    }
    assert schema.from_scalar_schema(scalar_schema) is schema


def test_publish_output_can_publish_full_values_instead_of_deltas():
    captured = []

    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]):
        publish_output(values, delta=False)

    state = hg.GlobalState()
    state[_OUTPUT_CALLBACK] = captured.append
    with hg.GlobalContext(state):
        assert hg.eval_node(app, [{"a": 1}, {"b": 2}]) is None

    assert captured == [{"a": 1}, {"a": 1, "b": 2}]


@pytest.mark.parametrize("call_style", ["out_keyword", "positional_path"])
def test_run_graph_on_thread_simulation_mode(call_style):
    captured = []

    @hg.sink_node
    def capture(result: hg.TSB[RunGraphOutput[hg.TS[int]]], engine: hg.EvaluationEngineApi = None):
        captured.append(result.value)
        if result.finished.valid and result.finished.value:
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("thread-test", run_graph_on_thread_impl)
        params = {
            "a": 1,
            "b": 2,
            "start_time": hg.MIN_ST,
            "end_time": hg.MIN_ST + timedelta(seconds=1),
            "capture_values": True,
            "cleanup_on_error": True,
            "trace_back_depth": 2,
        }
        if call_style == "out_keyword":
            result = run_graph_on_thread(
                _sum_graph,
                global_state={"seed": 1},
                params=params,
                out_=hg.TS[int],
                path="thread-test",
            )
        else:
            result = run_graph_on_thread(
                _sum_graph,
                {"seed": 1},
                params,
                hg.TS[int],
                "thread-test",
            )
        capture(result)

    with hg.GlobalContext(hg.GlobalState()):
        hg.run_graph(
            app,
            run_mode=hg.EvaluationMode.REAL_TIME,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=5),
        )

    assert any(value.get("out") == 3 for value in captured)
    assert captured[-1]["finished"] is True
    assert captured[-1]["status"] == "OK"


def test_run_graph_on_thread_forwards_time_bounds_to_child_parameters():
    captured = []

    @hg.graph
    def child(start_time: datetime, end_time: datetime):
        publish_output(hg.const(f"{start_time.isoformat()}|{end_time.isoformat()}"))

    @hg.sink_node
    def capture(
        result: hg.TSB[RunGraphOutput[hg.TS[str]]],
        engine: hg.EvaluationEngineApi = None,
    ):
        if result.out.modified:
            captured.append(result.out.value)
        if result.finished.valid and result.finished.value:
            engine.request_engine_stop()

    start_time = hg.MIN_ST
    end_time = hg.MIN_ST + timedelta(seconds=1)
    expected = f"{start_time.isoformat()}|{end_time.isoformat()}"

    @hg.graph
    def app():
        hg.register_adaptor("thread-time-parameters", run_graph_on_thread_impl)
        result = run_graph_on_thread[hg.TS[str]](
            child,
            params={"start_time": start_time, "end_time": end_time},
            path="thread-time-parameters",
        )
        capture(result)

    with hg.GlobalContext(hg.GlobalState()):
        hg.run_graph(
            app,
            run_mode=hg.EvaluationMode.REAL_TIME,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
        )

    assert captured == [expected]


def test_run_graph_on_thread_reports_child_runtime_failure():
    captured = []

    @hg.compute_node
    def fail(value: hg.TS[int]) -> hg.TS[int]:
        raise RuntimeError("expected child failure")

    @hg.graph
    def child():
        publish_output(fail(hg.const(1)))

    @hg.sink_node
    def capture(
        result: hg.TSB[RunGraphOutput[hg.TS[int]]],
        engine: hg.EvaluationEngineApi = None,
    ):
        captured.append(result.value)
        if result.finished.valid and result.finished.value:
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("thread-failure", run_graph_on_thread_impl)
        capture(
            run_graph_on_thread[hg.TS[int]](
                child,
                path="thread-failure",
            )
        )

    with hg.GlobalContext(hg.GlobalState()):
        hg.run_graph(
            app,
            run_mode=hg.EvaluationMode.REAL_TIME,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
        )

    assert captured[-1]["finished"] is True
    assert captured[-1]["status"].startswith("ERROR:")
    assert "expected child failure" in captured[-1]["status"]


def test_run_graph_on_thread_two_clients_preserve_their_output_order():
    captured = {"left": [], "right": []}

    @hg.generator
    def values(label: str) -> hg.TS[str]:
        for index in range(3):
            yield hg.MIN_ST + index * hg.MIN_TD, f"{label}:{index}"

    @hg.compute_node
    def with_seed(
        value: hg.TS[str],
        global_state: hg.GlobalState = None,
    ) -> hg.TS[str]:
        return f"{value.value}:{global_state['seed']}"

    @hg.graph
    def child(label: str):
        publish_output(with_seed(values(label)))

    @hg.sink_node(valid=())
    def capture(
        left: hg.TSB[RunGraphOutput[hg.TS[str]]],
        right: hg.TSB[RunGraphOutput[hg.TS[str]]],
        engine: hg.EvaluationEngineApi = None,
    ):
        if left.out.modified:
            captured["left"].append(left.out.value)
        if right.out.modified:
            captured["right"].append(right.out.value)
        if (
            left.finished.valid
            and left.finished.value
            and right.finished.valid
            and right.finished.value
        ):
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("thread-order", run_graph_on_thread_impl)

        def params(label):
            return hg.const(
                {
                    "label": label,
                    "run_mode": hg.EvaluationMode.SIMULATION,
                    "start_time": hg.MIN_ST,
                    "end_time": hg.MIN_ST + 4 * hg.MIN_TD,
                },
                tp=hg.TS[dict[str, object]],
            )

        left = run_graph_on_thread[hg.TS[str]](
            child,
            global_state=hg.const(
                {"seed": 11}, tp=hg.TS[dict[str, object]]),
            params=params("left"),
            path="thread-order",
        )
        right = run_graph_on_thread[hg.TS[str]](
            child,
            global_state=hg.const(
                {"seed": 22}, tp=hg.TS[dict[str, object]]),
            params=params("right"),
            path="thread-order",
        )
        capture(left, right)

    with hg.GlobalContext(hg.GlobalState()):
        hg.run_graph(
            app,
            run_mode=hg.EvaluationMode.REAL_TIME,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
        )

    assert captured == {
        "left": ["left:0:11", "left:1:11", "left:2:11"],
        "right": ["right:0:22", "right:1:22", "right:2:22"],
    }
