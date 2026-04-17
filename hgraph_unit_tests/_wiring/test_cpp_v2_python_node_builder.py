from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta

import hgraph._hgraph as _hgraph

from hgraph import GraphConfiguration, TS, compute_node, evaluate_graph, graph, sink_node
from hgraph._runtime._evaluation_clock import EvaluationClock
from hgraph._runtime._evaluation_engine import EvaluationEngineApi
from hgraph._runtime._node import Node, SCHEDULER
from hgraph._runtime._traits import Traits
from hgraph._types._recordable_state import RECORDABLE_STATE
from hgraph._types._scalar_types import LOGGER, STATE, CompoundScalar
from hgraph._types._tsb_type import TimeSeriesSchema
from hgraph import const as v2_const
from hgraph._wiring._graph_builder import wire_graph
from hgraph._wiring._wiring_node_class import PythonWiringNodeClass
from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext


@contextmanager
def use_v2_python_node_builder():
    previous_builder = PythonWiringNodeClass.BUILDER_CLASS
    PythonWiringNodeClass.BUILDER_CLASS = _hgraph.NodeBuilder
    try:
        yield
    finally:
        PythonWiringNodeClass.BUILDER_CLASS = previous_builder


def test_v2_python_compute_nodes_run_with_core_injectables():
    events = []

    assert _hgraph.NodeBuilder.NODE_SIGNATURE is _hgraph.NodeSignature
    assert _hgraph.NodeBuilder.NODE_TYPE_ENUM is _hgraph.NodeTypeEnum

    @compute_node
    def py_counter(
        ts: TS[int],
        _clock: EvaluationClock = None,
        _api: EvaluationEngineApi = None,
        _scheduler: SCHEDULER = None,
        _state: STATE = None,
        _traits: Traits = None,
        _node: Node = None,
        _output: TS[int] = None,
        _logger: LOGGER = None,
    ) -> TS[int]:
        _state.calls = getattr(_state, "calls", 0) + 1
        if _state.calls == 1:
            _scheduler.schedule(timedelta(milliseconds=1), tag="again")
        _traits.set_traits(last_eval=_clock.evaluation_time, saw_start=True)
        _logger.info("py_counter eval")
        previous = _output.value if _output is not None and _output.valid else 0
        events.append(
            {
                "call": _state.calls,
                "node_index": _node.node_ndx,
                "start_time": _api.start_time,
                "eval_time": _clock.evaluation_time,
                "previous": previous,
            }
        )
        return ts.value + previous + 1

    @sink_node
    def py_sink(ts: TS[int], _traits: Traits = None):
        events.append(("sink", ts.value, _traits.get_trait("last_eval")))

    @graph
    def g():
        py_sink(py_counter(v2_const(1)))

    with use_v2_python_node_builder(), WiringNodeInstanceContext():
        graph_builder = wire_graph(g)

    cpp_builders = [builder for builder in graph_builder.node_builders if isinstance(builder, _hgraph.NodeBuilder)]
    if isinstance(graph_builder, _hgraph.GraphBuilder):
        assert tuple(builder.implementation_name for builder in cpp_builders) == ("const", "py_counter", "py_sink")
    else:
        assert tuple(builder.implementation_name for builder in cpp_builders) == ("py_counter", "py_sink")
    py_counter_builder = next(builder for builder in cpp_builders if builder.implementation_name == "py_counter")
    assert py_counter_builder.signature.node_type is _hgraph.NodeTypeEnum.COMPUTE_NODE

    if not isinstance(graph_builder, _hgraph.GraphBuilder):
        return

    with use_v2_python_node_builder():
        evaluate_graph(g, GraphConfiguration(end_time=timedelta(milliseconds=2)))

    compute_events = [event for event in events if isinstance(event, dict)]
    assert [event["call"] for event in compute_events] == [1, 2]
    assert [event[1] for event in events if isinstance(event, tuple)] == [2, 4]
    assert all(event["node_index"] == 1 for event in compute_events)
    assert compute_events[0]["previous"] == 0
    assert compute_events[1]["previous"] == 2


def test_v2_python_nodes_can_self_notify_during_start_without_declaring_scheduler():
    compute_times = []
    sink_values = []

    @compute_node(active=(), valid=())
    def py_start_notify(ts: TS[int], _clock: EvaluationClock = None, _node: Node = None, _state: STATE = None) -> TS[int]:
        del ts
        _state.calls = getattr(_state, "calls", 0) + 1
        compute_times.append(_clock.evaluation_time)
        return _state.calls

    @py_start_notify.start
    def py_start_notify_start(_clock: EvaluationClock = None, _node: Node = None):
        _node.notify(_clock.evaluation_time + timedelta(milliseconds=5))

    @sink_node
    def py_sink(ts: TS[int]):
        sink_values.append(ts.value)

    @graph
    def g():
        py_sink(py_start_notify(v2_const(1)))

    with use_v2_python_node_builder(), WiringNodeInstanceContext():
        graph_builder = wire_graph(g)

    if not isinstance(graph_builder, _hgraph.GraphBuilder):
        return

    config = GraphConfiguration(end_time=timedelta(milliseconds=1))
    with use_v2_python_node_builder():
        evaluate_graph(g, config)

    assert compute_times == [config.start_time]
    assert sink_values == [1]


def test_v2_python_unset_output_value_is_none():
    seen = []
    output = []

    @compute_node
    def read_unset_output(ts: TS[int], _output: TS[bool] = None) -> TS[bool]:
        del ts
        seen.append((_output.valid, _output.value))
        return _output.value is None

    @sink_node
    def collect(ts: TS[bool]):
        output.append(ts.value)

    @graph
    def g():
        collect(read_unset_output(v2_const(1)))

    with use_v2_python_node_builder(), WiringNodeInstanceContext():
        graph_builder = wire_graph(g)

    if not isinstance(graph_builder, _hgraph.GraphBuilder):
        return

    with use_v2_python_node_builder():
        evaluate_graph(g, GraphConfiguration(end_time=timedelta(milliseconds=1)))

    assert output == [True]
    assert seen == [(False, None)]


def test_v2_python_recordable_state_is_available():
    seen = []

    @dataclass(frozen=True)
    class LocalState(TimeSeriesSchema):
        last: TS[int]

    @compute_node
    def py_recordable(ts: TS[int], _recordable_state: RECORDABLE_STATE[LocalState] = None) -> TS[int]:
        _recordable_state.last.value = ts.value + 10
        seen.append(_recordable_state.last.value)
        return _recordable_state.last.value

    @sink_node
    def py_sink(ts: TS[int]):
        seen.append(ts.value)

    @graph
    def g():
        py_sink(py_recordable(v2_const(5)))

    with use_v2_python_node_builder(), WiringNodeInstanceContext():
        graph_builder = wire_graph(g)

    if not isinstance(graph_builder, _hgraph.GraphBuilder):
        return

    with use_v2_python_node_builder():
        evaluate_graph(g, GraphConfiguration(end_time=timedelta(milliseconds=1)))

    assert seen == [15, 15]
