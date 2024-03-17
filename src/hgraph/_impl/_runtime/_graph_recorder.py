from datetime import datetime
from pathlib import Path

from hgraph._runtime._constants import MIN_DT
from hgraph._runtime._data_writer import DataWriter, DataReader
from hgraph._impl._runtime._node import NodeImpl
from hgraph._runtime._node import Node, NodeDelegate
from hgraph._runtime._graph_recorder import GraphRecorder

# TODO: This needs a proper implementation

class DataWriterGraphRecorder(GraphRecorder):

    def __init__(self, path: Path, prefix: str):
        self._path = path
        self._prefix = prefix

    def record_node(self, node: Node) -> Node:
        return node

    def replay_node(self, node: Node) -> Node:
        return node

    def last_recorded_time(self) -> datetime:
        return MIN_DT


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

