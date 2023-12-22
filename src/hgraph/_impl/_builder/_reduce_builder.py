from dataclasses import dataclass
from typing import TYPE_CHECKING, cast, Optional

from hgraph._builder._node_builder import NodeBuilder
from hgraph._impl._runtime._reduce_node import PythonReduceNodeImpl
from hgraph._types._time_series_types import TimeSeriesOutput
from hgraph._types._tsb_type import TimeSeriesBundleInput

if TYPE_CHECKING:
    from hgraph._builder._graph_builder import GraphBuilder


__all__ = ("PythonReduceNodeBuilder",)


@dataclass(frozen=True)
class PythonReduceNodeBuilder(NodeBuilder):
    nested_graph: Optional["GraphBuilder"] = None  # This is the generator function
    input_node_ids: tuple[int, int] | None = None  # The nodes representing the stub inputs in the nested graph.
    output_node_id: int | None = None  # The node representing the stub output in the nested graph.

    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx: int) -> PythonReduceNodeImpl:
        node = PythonReduceNodeImpl(
            node_ndx=node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
            nested_graph_builder=self.nested_graph,
            input_node_ids=self.input_node_ids,
            output_node_id=self.output_node_id,
        )

        if self.input_builder:
            ts_input: TimeSeriesBundleInput = cast(TimeSeriesBundleInput,
                                                   self.input_builder.make_instance(owning_node=node))
            node.input = ts_input

        if self.output_builder:
            ts_output: TimeSeriesOutput = self.output_builder.make_instance(owning_node=node)
            node.output = ts_output

        return node

    def release_instance(self, item: PythonReduceNodeImpl):
        """Nothing to do"""
