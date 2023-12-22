from dataclasses import dataclass
from typing import TYPE_CHECKING, Mapping, cast, Optional

from hgraph._builder._node_builder import NodeBuilder
from hgraph._impl._runtime._map_node import PythonTsdMapNodeImpl
from hgraph._types._time_series_types import TimeSeriesOutput
from hgraph._types._tsb_type import TimeSeriesBundleInput

if TYPE_CHECKING:
    from hgraph._builder._graph_builder import GraphBuilder


@dataclass(frozen=True)
class PythonTsdMapNodeBuilder(NodeBuilder):
    nested_graph: Optional["GraphBuilder"] = None  # This is the generator function
    input_node_ids: Mapping[str, int] | None = None  # The nodes representing the stub inputs in the nested graph.
    output_node_id: int | None = None  # The node representing the stub output in the nested graph.
    multiplexed_args: frozenset[str] | None = None  # The inputs that need to be de-multiplexed.

    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx: int) -> PythonTsdMapNodeImpl:
        node = PythonTsdMapNodeImpl(
            node_ndx=node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
            nested_graph_builder=self.nested_graph,
            input_node_ids=self.input_node_ids,
            output_node_id=self.output_node_id,
            multiplexed_args=self.multiplexed_args
        )

        if self.input_builder:
            ts_input: TimeSeriesBundleInput = cast(TimeSeriesBundleInput,
                                                   self.input_builder.make_instance(owning_node=node))
            node.input = ts_input

        if self.output_builder:
            ts_output: TimeSeriesOutput = self.output_builder.make_instance(owning_node=node)
            node.output = ts_output

        return node

    def release_instance(self, item: PythonTsdMapNodeImpl):
        """Nothing to do"""


@dataclass(frozen=True)
class PythonTslMapNodeBuilder(NodeBuilder):
    nested_graph: Optional["GraphBuilder"] = None  # This is the generator function
    input_node_ids: Mapping[str, int] | None = None  # The nodes representing the stub inputs in the nested graph.
    output_node_id: int | None = None  # The node representing the stub output in the nested graph.
    multiplexed_args: frozenset[str] | None = None  # The inputs that need to be de-multiplexed.

    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx: int) -> PythonTsdMapNodeImpl:
        node = PythonTslMapNodeImpl(
            node_ndx=node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
            nested_graph_builder=self.nested_graph,
            input_node_ids=self.input_node_ids,
            output_node_id=self.output_node_id,
            multiplexed_args=self.multiplexed_args
        )

        if self.input_builder:
            ts_input: TimeSeriesBundleInput = cast(TimeSeriesBundleInput,
                                                   self.input_builder.make_instance(owning_node=node))
            node.input = ts_input

        if self.output_builder:
            ts_output: TimeSeriesOutput = self.output_builder.make_instance(owning_node=node)
            node.output = ts_output

        return node

    def release_instance(self, item: PythonTsdMapNodeImpl):
        """Nothing to do"""


