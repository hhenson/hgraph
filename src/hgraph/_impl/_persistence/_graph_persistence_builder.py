from datetime import date, datetime, timedelta, time
from pathlib import Path
from typing import Mapping, Any, Callable

from hgraph import NodeTypeEnum, HgStateType, HgSchedulerType, HgTimeSeriesTypeMetaData, TimeSeriesOutput, \
    HgTSSTypeMetaData, raise_error, PythonTimeSeriesValueOutput, NodeImpl, STATE, NodeSchedulerImpl, NodeSignature, \
    sink_node, SIGNAL, HgTSTypeMetaData, EvaluationLifeCycleObserver
from hgraph._runtime._data_writer import DataWriter, DataReader
from hgraph._runtime._node import Node, InjectableTypes
from hgraph._runtime._graph import Graph


class Suspendable:

    def suspend(self, writer: DataWriter):
        ...

    def resume(self, reader: DataReader):
        ...


class GraphSuspendable(Suspendable):
    """
    A graph can either be suspended by tracking the pull and push nodes and replaying all ticks in simulation mode
    before cutting over to real-time, or we need to save the complete state of the graph, there are too many edge
    cases otherwise.

    The only other approach is to make data aware nodes that operate over persisted data by default and can restore
    state on the fly.
    """

    def __init__(self, graph: Graph):
        self._graph = graph
        self._node_suspendables: list[Suspendable] = [NodeSuspendable(node) for node in graph.nodes]

    def suspend(self, writer: DataWriter):
        writer.write_datetime(self._graph.evaluation_clock.evaluation_time)
        for node_suspendable in self._node_suspendables:
            node_suspendable.suspend(writer)

    def resume(self, reader: DataReader):
        dt = reader.read_datetime()
        self._graph.evaluation_clock._evaluation_time = dt
        for node_persisters in self._node_suspendables:
            node_persisters.resume(reader)


class NodeSuspendable(Suspendable):

    def __init__(self, node: Node):
        self._node = node
        self._suspendables: list[Suspendable] = []
        if node.signature.node_type in (NodeTypeEnum.PULL_SOURCE_NODE, NodeTypeEnum.PUSH_SOURCE_NODE):
            self._suspendables.append(TimeSeriesOutputSuspendable(node.signature.time_series_output, node.output))
        if node.signature.uses_state:
            k, v = next(iter((k, v) for k, v in node.signature.scalars.items() if type(v) is HgStateType))
            self._suspendables.append(StateSuspendable(k, v, node))
        if node.signature.uses_scheduler:
            k, v = next(iter((k, v) for k, v in node.signature.scalars.items() if type(v) is HgSchedulerType))
            self._suspendables.append(SchedulerSuspendable(k, node))
        if node.signature.uses_output_feedback:
            k, v = next(iter((k, v) for k, v in node.signature.scalars.items() if type(v) is HgStateType))
            self._suspendables.append(TimeSeriesOutputSuspendable(k, v, node))

    def suspend(self, writer: DataWriter):
        for suspendable in self._suspendables:
            suspendable.suspend(writer)

    def resume(self, reader: DataReader):
        for suspendable in self._suspendables:
            suspendable.resume(reader)


class TimeSeriesOutputSuspendable(Suspendable):

    def __init__(self, tp: HgTimeSeriesTypeMetaData, output: TimeSeriesOutput):
        self.tp = tp
        self.output = output

    def suspend(self, writer: DataWriter):
        {
            HgTSTypeMetaData: lambda writer: self.suspend_ts(writer)
        }.get(type(self.tp), lambda x: raise_error(f"Unsupported output type: {self.tp}"))(writer)

    def suspend_ts(self, writer: DataWriter):
        {
            bool: lambda: writer.write_boolean(self.output.value),
            int: lambda: writer.write_int(self.output.value),
            float: lambda: writer.write_float(self.output.value),
            str: lambda: writer.write_string(self.output.value),
            date: lambda: writer.write_date(self.output.value),
            datetime: lambda: writer.write_datetime(self.output.value),
            timedelta: lambda: writer.write_time_delta(self.output.value),
            time: lambda: writer.write_time(self.output.value),
        }.get(self.tp.value_scalar_tp.py_type,
              lambda: raise_error(f"Unsupported scalar type: {self.tp.value_scalar_tp.py_type}"))()
        writer.write_datetime(self.output.last_modified_time)

    def resume(self, reader: DataReader):
        {
            HgTSTypeMetaData: lambda reader: self.resume_ts(reader)
        }.get(type(self.tp), lambda x: raise_error(f"Unsupported output type: {self.tp}"))(reader)

    def resume_ts(self, reader: DataReader):
        output: PythonTimeSeriesValueOutput = self.output
        output.value = {
            bool: lambda: reader.read_boolean(),
            int: lambda: reader.read_int(),
            float: lambda: reader.read_float(),
            str: lambda: reader.read_string(),
            date: lambda: reader.read_date(),
            datetime: lambda: reader.read_datetime(),
            timedelta: lambda: reader.read_time_delta(),
            time: lambda: reader.read_time(),
        }.get(self.tp.value_scalar_tp.py_type,
              lambda: raise_error(f"Unsupported scalar type: {self.tp.value_scalar_tp.py_type}"))()
        output._last_modified_time = reader.read_datetime()


class StateSuspendable(Suspendable):

    def __init__(self, key: str, value: HgStateType, node: Node):
        self._key = key
        self._value = value
        self._node = node

    def suspend(self, writer: DataWriter):
        node: NodeImpl = self._node
        state: STATE = node._kwargs[self._key]
        if state.is_updated():
            writer.write_boolean(True)
            value = state.as_schema
            import pickle
            import io
            output_bytes = io.BytesIO()
            pickle.dump(value, output_bytes)
            writer.write_bytes(output_bytes.getvalue())
            state.reset_updated()
        else:
            writer.write_boolean(False)

    def resume(self, reader: DataReader):
        if reader.read_boolean():
            node: NodeImpl = self._node
            state: STATE = node._kwargs[self._key]
            import pickle
            import io
            state_value = reader.read_bytes()
            state._value = pickle.load(io.BytesIO(state_value))


class SchedulerSuspendable(Suspendable):

    def __init__(self, key: str, node: Node):
        self._key = key
        self._node = node

    def suspend(self, writer: DataWriter):
        scheduler: NodeSchedulerImpl = self._node.scheduler
        if scheduler.is_scheduled:
            writer.write_boolean(True)
            events = scheduler._scheduled_events
            writer.write_int(len(events))
            for event in events:
                writer.write_datetime(event[0])
                writer.write_string(s if (s := event[1]) else "")
        else:
            writer.write_boolean(False)

    def resume(self, reader: DataReader):
        if reader.read_boolean():
            scheduler: NodeSchedulerImpl = self._node.scheduler
            for _ in range(reader.read_int()):
                scheduler.schedule(reader.read_datetime(), s if (s := reader.read_string()) else None)


def node_requires_suspension(node: Node) -> bool:
    """Is this not required to be persisted"""
    return bool(node.signature.node_type in (NodeTypeEnum.PULL_SOURCE_NODE, NodeTypeEnum.PUSH_SOURCE_NODE) or \
                (node.signature.injectable_inputs & (
                        InjectableTypes.STATE | InjectableTypes.SCHEDULER | InjectableTypes.OUTPUT)))


class FileDataWriter(DataWriter):

    def __init__(self, path: Path, filename: str, dt: datetime) -> None:
        self._path = path
        self._filename = filename
        path.mkdir(parents=True, exist_ok=True)
        self._file = path.joinpath(f'{filename}_{dt:%Y%m%d_%H%M%S}.dat').open(mode="wb+")

    def write(self, b: bytes):
        self._file.write(b)

    def close(self):
        self._file.close()


class FileDataReader(DataReader):

    def __init__(self, restore_file: Path) -> None:
        self._restore_file = restore_file
        self._file = restore_file.open(mode="rb")

    def read(self, size: int) -> bytes:
        return self._file.read(size)


class SuspendNode(NodeImpl):

    def __init__(self, node_ndx: int, owning_graph_id: tuple[int, ...], signature: NodeSignature,
                 scalars: Mapping[str, Any], eval_fn: Callable = None, start_fn: Callable = None,
                 stop_fn: Callable = None):
        super().__init__(node_ndx, owning_graph_id, signature, scalars, eval_fn, start_fn, stop_fn)
        self._suspender: GraphSuspendable = None

    def do_start(self):
        self._suspender = GraphSuspendable(self.graph)

    def do_eval(self):
        if self._kwargs['signal'].modified:
            self.graph.evaluation_engine_api.add_after_evaluation_notification(self.suspend)
            self.graph.evaluation_engine_api.request_engine_stop()

    def suspend(self):
        writer = FileDataWriter(self._kwargs['path'], self._kwargs['filename'],
                                self.graph.evaluation_clock.evaluation_time)
        self._suspender.suspend(writer)

    def do_stop(self):
        self._suspender = None


@sink_node(node_impl=SuspendNode)
def suspender(signal: SIGNAL, path: Path, filename: str):
    """
    Suspends the graph when the signal is ticked.
    """


class RestoreGraphObserver(EvaluationLifeCycleObserver):

    def __init__(self, path: Path, filename: str):
        self._path = path
        self._filename = filename
        files = sorted([p for p in path.iterdir() if p.name.startswith(filename) and p.name.endswith(".dat")])
        if files:
            self._load_file = files[-1]
        else:
            self._load_file = None

    def on_before_start_graph(self, graph: "Graph"):
        if self._load_file and graph.parent_node is None:
            file_reader = FileDataReader(self._load_file)
            GraphSuspendable(graph).resume(file_reader)

