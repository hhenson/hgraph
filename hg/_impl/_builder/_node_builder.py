from dataclasses import dataclass
from typing import Callable

from hg._builder._node_builder import NodeBuilder
from hg._impl._runtime._node import NodeImpl, GeneratorNodeImpl
from hg._types._time_series_types import TimeSeriesOutput
from hg._types._tsb_type import TimeSeriesBundleInput
from hg._runtime._node import Node


__all__ = ("PythonNodeBuilder", "PythonGeneratorNodeBuilder")


@dataclass(frozen=True)
class PythonNodeBuilder(NodeBuilder):
    eval_fn: Callable = None  # The eval fn must be supplied.
    start_fn: Callable = None
    stop_fn: Callable = None

    def make_instance(self, owning_graph_id: tuple[int, ...]) -> Node:
        node = NodeImpl(
            node_ndx=self.node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
            eval_fn=self.eval_fn,
            start_fn=self.start_fn,
            stop_fn=self.stop_fn
        )

        if self.input_builder:
            ts_input: TimeSeriesBundleInput = self.input_builder.make_instance(owning_node=node)
            node.input = ts_input

        if self.output_builder:
            ts_output: TimeSeriesOutput = self.output_builder.make_instance(owning_node=node)
            node.output = ts_output

        return node

    def release_instance(self, item: Node):
        pass


@dataclass(frozen=True)
class PythonGeneratorNodeBuilder(NodeBuilder):
    eval_fn: Callable = None  # This is the generator function

    def make_instance(self, owning_graph_id: tuple[int, ...]) -> Node:
        node: Node = GeneratorNodeImpl(
            node_ndx=self.node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
            eval_fn=self.eval_fn
        )

        if self.output_builder:
            ts_output: TimeSeriesOutput = self.output_builder.make_instance(owning_node=node)
            node.output = ts_output

        return node

    def release_instance(self, item: Node):
        pass
