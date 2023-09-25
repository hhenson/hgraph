from dataclasses import dataclass
from typing import Callable, Any, Optional

from _pytest.nodes import Node
from frozendict import frozendict

from hg._impl._builder._builder import Builder, ITEM
from hg._impl._runtime._node import NodeImpl
from hg._runtime import NodeSignature
from hg._types._time_series_types import TimeSeriesInput, TimeSeriesOutput
from hg._types._tsb_type import TimeSeriesBundleInput


class NodeBuilder(Builder[Node]):

    def make_instance(self, owning_graph_id: tuple[int, ...]) -> Node:
        raise NotImplementedError()

    def release_instance(self, item: Node):
        raise NotImplementedError()


class InputBuilder(Builder[TimeSeriesInput]):

    def make_instance(self, owning_node: Node = None, owning_input: TimeSeriesInput = None) -> TimeSeriesInput:
        """One of owning_node or owning_input must be defined."""
        pass

    def release_instance(self, item: TimeSeriesInput):
        pass


class OutputBuilder(Builder[TimeSeriesOutput]):

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None) -> TimeSeriesOutput:
        """One of owning_node or owning_output must be defined."""
        pass

    def release_instance(self, item: TimeSeriesOutput):
        pass


@dataclass
class PythonNodeBuilder(NodeBuilder):
    node_ndx: int
    signature: NodeSignature
    scalars: frozendict[str, Any]
    eval_fn: Callable
    start_fn: Callable
    stop_fn: Callable
    input_builder: Optional[InputBuilder] = None
    output_builder: Optional[OutputBuilder] = None

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
            ts_output: TimeSeriesOutput = self.output_builder.make_instance(ownning_node=node)
            node.output = ts_output

        return node

    def release_instance(self, item: Node):
        pass