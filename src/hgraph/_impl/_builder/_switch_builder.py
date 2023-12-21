from dataclasses import dataclass
from typing import Mapping, TYPE_CHECKING, cast

from hgraph._types._tsb_type import TimeSeriesBundleInput
from hgraph._types._time_series_types import TimeSeriesOutput
from hgraph._impl._runtime._switch_node import PythonSwitchNodeImpl
from hgraph._types._scalar_types import SCALAR
from hgraph._builder._node_builder import NodeBuilder


if TYPE_CHECKING:
    from hgraph._builder._graph_builder import GraphBuilder


@dataclass(frozen=True)
class PythonSwitchNodeBuilder(NodeBuilder):
    nested_graphs: Mapping[SCALAR, "GraphBuilder"] | None = None  # This is the generator function
    # The nodes representing the stub inputs in the nested graph.
    input_node_ids: Mapping[SCALAR, Mapping[str, int]] | None = None
    output_node_id: Mapping[SCALAR, int] | None = None  # The node representing the stub output in the nested graph.
    reload_on_ticked: bool = False

    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx: int) -> PythonSwitchNodeImpl:
        node = PythonSwitchNodeImpl(
            node_ndx=node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
            nested_graph_builders=self.nested_graphs,
            input_node_ids=self.input_node_ids,
            output_node_ids=self.output_node_id,
            reload_on_ticked=self.reload_on_ticked
        )

        if self.input_builder:
            ts_input: TimeSeriesBundleInput = cast(TimeSeriesBundleInput,
                                                   self.input_builder.make_instance(owning_node=node))
            node.input = ts_input

        if self.output_builder:
            ts_output: TimeSeriesOutput = self.output_builder.make_instance(owning_node=node)
            node.output = ts_output

        return node

    def release_instance(self, item: PythonSwitchNodeImpl):
        """Nothing to be done here"""
