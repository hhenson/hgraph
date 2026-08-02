import pytest

import hgraph as hg


def test_eval_node_delivers_nested_lifecycle_events_with_scoped_views():
    class Recorder(hg.EvaluationLifeCycleObserver):
        def __init__(self):
            self.graph_ids = []
            self.node_ids = []
            self.started_graphs = []
            self.stopped_graphs = []
            self.retained_graph = None
            self.retained_node = None

        def on_after_start_graph(self, graph):
            self.started_graphs.append(graph.graph_id)

        def on_after_stop_graph(self, graph):
            self.stopped_graphs.append(graph.graph_id)

        def on_before_graph_evaluation(self, graph):
            self.graph_ids.append(graph.graph_id)
            self.retained_graph = graph

        def on_before_node_evaluation(self, node):
            self.node_ids.append((node.node_id, node.graph.graph_id, node.label))
            self.retained_node = node

    @hg.compute_node
    def add_one(value: hg.TS[int]) -> hg.TS[int]:
        return value.value + 1

    @hg.graph
    def mapped(values: hg.TSD[int, hg.TS[int]]) -> hg.TSD[int, hg.TS[int]]:
        return hg.map_(add_one, values)

    observer = Recorder()
    assert hg.eval_node(mapped, [{1: 4, 2: 5}], __observers__=[observer]) == [
        {1: 5, 2: 6}
    ]

    assert () in observer.graph_ids
    assert any(graph_id for graph_id in observer.graph_ids)
    assert any(node_id[:-1] == graph_id for node_id, graph_id, _ in observer.node_ids)
    assert len([graph_id for graph_id in observer.started_graphs if graph_id]) >= 2
    assert len([graph_id for graph_id in observer.stopped_graphs if graph_id]) >= 2
    with pytest.raises(RuntimeError, match="outside its lifecycle callback"):
        _ = observer.retained_graph.label
    with pytest.raises(RuntimeError, match="outside its node's evaluation"):
        _ = observer.retained_node.label


def test_graph_configuration_delivers_custom_lifecycle_observers():
    events = []

    class Recorder(hg.EvaluationLifeCycleObserver):
        def on_before_start_graph(self, graph):
            events.append(("start", graph.label, graph.graph_id))

        def on_after_stop_graph(self, graph):
            events.append(("stop", graph.label, graph.graph_id))

    @hg.graph
    def app() -> hg.TS[int]:
        return hg.const(7)

    result = hg.evaluate_graph(
        app,
        hg.GraphConfiguration(life_cycle_observers=(Recorder(),)),
    )

    assert [value for _, value in result] == [7]
    assert events[0][0] == "start"
    assert events[-1][0] == "stop"


def test_python_lifecycle_observer_failures_propagate_and_cleanup():
    stopped = []

    @hg.sink_node
    def sink(value: hg.TS[int]):
        pass

    @sink.stop
    def stop_sink():
        stopped.append(True)

    class FailingObserver(hg.EvaluationLifeCycleObserver):
        def on_before_graph_evaluation(self, graph):
            raise ValueError(f"observer failed for {graph.label}")

    @hg.graph
    def app():
        sink(hg.const(1))

    with pytest.raises(RuntimeError, match="observer failed"):
        hg.evaluate_graph(
            app,
            hg.GraphConfiguration(life_cycle_observers=(FailingObserver(),)),
        )
    assert stopped == [True]


def test_cleanup_on_error_controls_node_stop():
    stopped = []

    @hg.sink_node
    def fail(value: hg.TS[int]):
        raise RuntimeError("node failed")

    @fail.stop
    def stop_fail():
        stopped.append(True)

    @hg.graph
    def app():
        fail(hg.const(1))

    with pytest.raises(RuntimeError, match="node failed"):
        hg.evaluate_graph(app, hg.GraphConfiguration(cleanup_on_error=True))
    assert stopped == [True]

    try:
        hg.evaluate_graph(app, hg.GraphConfiguration(cleanup_on_error=False))
    except RuntimeError as error:
        assert "node failed" in str(error)
        assert hasattr(error, "_hgraph_failed_run")
        # The exception retains the failed executor for inspection, so stop is
        # deferred while the exception remains alive.
        assert stopped == [True]
    else:
        pytest.fail("the failing graph completed successfully")

    # Releasing the exception releases the retained executor and performs the
    # mandatory final teardown.
    assert stopped == [True, True]


def test_graph_configuration_controls_uncaught_error_detail():
    @hg.compute_node
    def divide(lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[int]:
        if rhs.value == 0:
            raise RuntimeError("division by zero")
        return lhs.value // rhs.value

    @hg.graph
    def app() -> hg.TS[int]:
        return divide(hg.const(9), hg.const(0))

    with pytest.raises(RuntimeError) as exc_info:
        hg.evaluate_graph(
            app,
            hg.GraphConfiguration(trace_back_depth=2, capture_values=True),
        )

    message = str(exc_info.value)
    assert "division by zero" in message
    assert "Activation Back Trace" in message
    # issue #247: the failing node is named by the user function
    assert "divide" in message
    assert "value={_0: 9, _1: 0}" in message


def test_graph_configuration_rejects_negative_traceback_depth():
    @hg.graph
    def app() -> hg.TS[int]:
        return hg.const(1)

    with pytest.raises(ValueError, match="non-negative"):
        hg.evaluate_graph(app, hg.GraphConfiguration(trace_back_depth=-1))
