from hgraph import (
    compute_node,
    graph,
    const_fn,
    const,
    evaluate_graph,
    GraphConfiguration,
    GlobalState,
    TIME_SERIES_TYPE,
    LOGGER,
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


def test_global_state_injectable_for_graph():
    @graph
    def read_from_state(_global_state: GlobalState = None) -> TS[int]:
        return const(_global_state["value"])

    with GlobalState(value=42):
        result = evaluate_graph(read_from_state, GraphConfiguration())

    assert [v for _, v in result] == [42]


def test_global_state_injectable_for_const_fn_direct_call():
    @const_fn
    def read_from_state(_global_state: GlobalState = None) -> TS[int]:
        return _global_state["value"]

    with GlobalState(value=7):
        assert read_from_state().value == 7
