from dataclasses import dataclass
from typing import TYPE_CHECKING, Mapping, cast

from hgraph._builder._node_builder import NodeBuilder
from hgraph._impl._runtime._map_node import PythonMapNodeImpl
from hgraph._types._time_series_types import TimeSeriesOutput
from hgraph._types._tsb_type import TimeSeriesBundleInput

if TYPE_CHECKING:
    from hgraph._builder._graph_builder import GraphBuilder


@dataclass(frozen=True)
class PythonMapNodeBuilder(NodeBuilder):
    nested_graph: "GraphBuilder" = None  # This is the generator function
    input_node_ids: Mapping[str, int] = None  # The nodes representing the stub inputs in the nested graph.
    output_node_id: int = None  # The node representing the stub output in the nested graph.
    multiplexed_args: frozenset[str] =None  # The inputs that need to be de-multiplexed.

    def make_instance(self, owning_graph_id: tuple[int, ...]) -> PythonMapNodeImpl:
        node = PythonMapNodeImpl(
            node_ndx=self.node_ndx,
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

    def release_instance(self, item: PythonMapNodeImpl):
        pass
