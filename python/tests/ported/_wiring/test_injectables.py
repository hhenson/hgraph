# Ported from ext/main/hgraph_unit_tests/_wiring/test_injectables.py
import pytest

from hgraph import (
    compute_node,
    graph,
    const,
    evaluate_graph,
    GraphConfiguration,
    GlobalState,
    EvaluationEngineApi,
    EvaluationMode,
    TIME_SERIES_TYPE,
    LOGGER,
    NODE,
    sink_node,
    TS,
)
from hgraph.test import eval_node


def test_logger_injectable(capsys):
    @compute_node
    def log_and_pass_through(ts: TIME_SERIES_TYPE, logger: LOGGER = None) -> TIME_SERIES_TYPE:
        logger.info(f"Tick: {ts.delta_value}")
        return ts.delta_value

    assert eval_node(log_and_pass_through, [1, None, 2]) == [1, None, 2]

    read = capsys.readouterr()
    log = read.err
    if log != "":
        assert "Tick: 1" in log
        assert "Tick: 2" in log


def test_global_state_injectable_for_node():
    @compute_node
    def add_offset(ts: TS[int], _global_state: GlobalState = None) -> TS[int]:
        return ts.value + _global_state["offset"]

    with GlobalState(offset=10):
        assert eval_node(add_offset, [1, None, 2]) == [11, None, 12]


def test_evaluation_engine_api_is_native_and_call_scoped():
    retained = []
    phases = []

    @compute_node
    def stop_on_two(ts: TS[int], engine: EvaluationEngineApi = None) -> TS[int]:
        retained.append(engine)
        assert engine.evaluation_mode == EvaluationMode.SIMULATION
        assert engine.start_time is not None
        assert engine.end_time is not None
        assert engine.evaluation_clock.evaluation_time is not None
        assert not engine.is_stop_requested
        if ts.value == 2:
            engine.request_engine_stop()
            assert engine.is_stop_requested
        return ts.value

    @stop_on_two.start
    def start(engine: EvaluationEngineApi = None):
        phases.append(("start", engine.evaluation_mode))

    @stop_on_two.stop
    def stop(engine: EvaluationEngineApi = None):
        phases.append(("stop", engine.is_stop_requested))

    assert eval_node(stop_on_two, [1, 2, 3]) == [1, 2, None]
    assert phases == [("start", EvaluationMode.SIMULATION), ("stop", True)]
    with pytest.raises(RuntimeError, match="outside its node's evaluation"):
        _ = retained[-1].is_stop_requested


def test_global_state_injectable_for_graph():
    @graph
    def read_from_state(_global_state: GlobalState = None) -> TS[int]:
        return const(_global_state["value"])

    with GlobalState(value=42):
        result = evaluate_graph(read_from_state, GraphConfiguration())

    assert [v for _, v in result] == [42]


def test_node_self_injectable_is_native_and_call_scoped():
    retained = []
    phases = []

    @compute_node
    def source(trigger: TS[int], node: NODE = None) -> TS[int]:
        retained.append(node)
        assert node.started
        assert node.has_input
        assert node.has_output
        assert node.node_id[-1] == node.node_ndx
        return trigger.value

    @source.start
    def start(node: NODE = None):
        phases.append(("start", node.started))
        node.notify()

    @source.stop
    def stop(node: NODE = None):
        phases.append(("stop", node.started))

    assert eval_node(source, [42]) == [42]
    assert phases == [("start", False), ("stop", True)]
    with pytest.raises(RuntimeError, match="outside its node's evaluation"):
        _ = retained[-1].node_id


def test_node_self_injectable_for_sink_nodes():
    seen = []

    @sink_node
    def inspect(ts: TS[int], node: NODE = None):
        seen.append((ts.value, node.has_input, node.has_output, node.node_ndx))

    assert eval_node(inspect, [1, 2]) is None
    assert [(value, has_input, has_output) for value, has_input, has_output, _ in seen] == [
        (1, True, False),
        (2, True, False),
    ]


def test_sink_stop_reads_final_time_series_input_before_deactivation():
    stopped = []

    @sink_node
    def observe(value: TS[int]):
        pass

    @observe.stop
    def stop(value: TS[int]):
        stopped.append((value.valid, value.value))

    @graph
    def app(value: TS[int]) -> TS[int]:
        observe(value)
        return value

    assert eval_node(app, [1, 2]) == [1, 2]
    assert stopped == [(True, 2)]


@pytest.mark.skip(reason="deviation: const_fn is not ported (record_replay_table.rst P1 - "
                          "a wiring-time computation is a plain function; const-evaluable "
                          "operators cover the replay/table cases)")
def test_global_state_injectable_for_const_fn_direct_call():
    pass
