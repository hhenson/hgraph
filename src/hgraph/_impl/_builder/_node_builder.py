from abc import ABC
from copy import copy
from dataclasses import dataclass
from inspect import isfunction
from typing import Callable, TypeVar

from hgraph._builder._node_builder import NodeBuilder
from hgraph._impl._runtime._node import (
    BaseNodeImpl,
    NodeImpl,
    GeneratorNodeImpl,
    PythonPushQueueNodeImpl,
    PythonLastValuePullNodeImpl,
)
from hgraph._types._time_series_types import TimeSeriesOutput
from hgraph._types._tsb_type import TimeSeriesBundleInput

__all__ = (
    "PythonNodeBuilder",
    "PythonGeneratorNodeBuilder",
    "PythonPushQueueNodeBuilder",
    "PythonLastValuePullNodeImpl",
    "PythonBaseNodeBuilder",
)


NODE = TypeVar("NODE", bound=BaseNodeImpl)


class PythonBaseNodeBuilder(NodeBuilder, ABC):

    def _build_inputs_and_outputs(self, node: NODE) -> NODE:
        if self.input_builder:
            ts_input: TimeSeriesBundleInput = self.input_builder.make_instance(owning_node=node)
            node.input = ts_input

        if self.output_builder:
            ts_output: TimeSeriesOutput = self.output_builder.make_instance(owning_node=node)
            node.output = ts_output

        if self.error_builder:
            ts_error_output: TimeSeriesOutput = self.error_builder.make_instance(owning_node=node)
            node.error_output = ts_error_output

        if self.recordable_state_builder:
            ts_output: TimeSeriesOutput = self.recordable_state_builder.make_instance(owning_node=node)
            node.recordable_state = ts_output

        return node


@dataclass(frozen=True)
class PythonNodeBuilder(PythonBaseNodeBuilder):
    eval_fn: Callable = None  # The eval fn must be supplied.
    start_fn: Callable = None
    stop_fn: Callable = None

    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx: int) -> NodeImpl:
        node = NodeImpl(
            node_ndx=node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
            eval_fn=self.eval_fn if isfunction(self.eval_fn) else copy(self.eval_fn),
            start_fn=self.start_fn,
            stop_fn=self.stop_fn,
        )

        return self._build_inputs_and_outputs(node)

    def release_instance(self, item: NodeImpl):
        """Nothing to do"""


@dataclass(frozen=True)
class PythonGeneratorNodeBuilder(PythonBaseNodeBuilder):
    eval_fn: Callable = None  # This is the generator function

    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx: int) -> GeneratorNodeImpl:
        node = GeneratorNodeImpl(
            node_ndx=node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
            eval_fn=self.eval_fn,
        )

        return self._build_inputs_and_outputs(node)

    def release_instance(self, item: GeneratorNodeImpl):
        """Nothing to do"""


@dataclass(frozen=True)
class PythonPushQueueNodeBuilder(PythonBaseNodeBuilder):
    eval_fn: Callable = None  # This is the generator function

    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx: int) -> PythonPushQueueNodeImpl:
        node = PythonPushQueueNodeImpl(
            node_ndx=node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
            eval_fn=self.eval_fn,
        )

        return self._build_inputs_and_outputs(node)

    def release_instance(self, item: PythonPushQueueNodeImpl):
        """Nothing to do"""


@dataclass(frozen=True)
class PythonLastValuePullNodeBuilder(PythonBaseNodeBuilder):

    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx: int) -> PythonLastValuePullNodeImpl:
        node = PythonLastValuePullNodeImpl(
            node_ndx=node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
        )

        return self._build_inputs_and_outputs(node)

    def release_instance(self, item: PythonLastValuePullNodeImpl):
        """Nothing to do"""
