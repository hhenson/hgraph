from datetime import datetime

from hgraph._impl._runtime._node import NodeImpl
from hgraph._runtime._constants import MIN_DT
from hgraph._runtime._data_writer import DataWriter, DataReader
from hgraph._runtime._graph_recorder import GraphRecorder
from hgraph._runtime._node import Node, NodeDelegate


# TODO: This needs a proper implementation


class DataWriterGraphRecorder(GraphRecorder):

    def __init__(self, writer: DataWriter = None, reader: DataReader = None):
        self._writer: DataWriter | None = writer
        self._reader: DataReader | None = reader
        # if we have a reader extract the time from the reader
        self._first_recorded_time: datetime = MIN_DT if self._reader is None else self._reader.read_datetime()
        self._last_recorded_time: datetime = MIN_DT if self._reader is None else self._reader.read_datetime()

    def record_node(self, node: Node) -> Node:
        if self._writer is None:
            return node
        else:
            return RecorderNode(node, self._writer)

    def replay_node(self, node: Node) -> Node:
        return ReplayNode(node, self._reader) if self._reader is not None else node

    def last_recorded_time(self) -> datetime:
        return self._last_recorded_time

    def begin_cycle(self, dt: datetime):
        pass

    def end_cycle(self, dt: datetime):
        pass

    def first_recorded_time(self) -> datetime:
        pass

    def begin_replay(self):
        pass

    def next_cycle(self) -> datetime | None:
        pass


class RecorderNode(NodeDelegate):

    def __init__(self, node, data_writer: DataWriter):
        super().__init__(node)
        self._data_writer = data_writer

    def eval(self):
        super().eval()
        if self.output.modified:
            self._data_writer.write(self)


class ReplayNode(NodeImpl):

    def __init__(self, node, data_writer: DataReader):
        super().__init__(node.node_ndx, node.owning_graph_id, node.signature, node.scalars)
        self.node = node
        self.data_writer = data_writer

    def eval(self):
        self.data_writer.read(self.node)
