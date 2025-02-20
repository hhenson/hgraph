from datetime import datetime
from string import Formatter
from typing import Mapping, Any

from hgraph import GlobalState, TimeSeriesReferenceInput, TimeSeriesReference
from hgraph._builder._graph_builder import GraphBuilder
from hgraph._impl._runtime._nested_evaluation_engine import (
    PythonNestedNodeImpl,
    NestedEvaluationEngine,
    NestedEngineEvaluationClock,
)
from hgraph._impl._runtime._node import NodeImpl
from hgraph._runtime._graph import Graph
from hgraph._runtime._node import NodeSignature, Node

__all__ = ("PythonComponentNodeImpl",)


class PythonComponentNodeImpl(PythonNestedNodeImpl):

    def __init__(
        self,
        node_ndx: int,
        owning_graph_id: tuple[int, ...],
        signature: NodeSignature,
        scalars: Mapping[str, Any],
        nested_graph_builder: GraphBuilder = None,
        input_node_ids: Mapping[str, int] = None,
        output_node_id: int = None,
    ):
        super().__init__(node_ndx, owning_graph_id, signature, scalars)
        self.nested_graph_builder: GraphBuilder = nested_graph_builder
        self.input_node_ids: Mapping[str, int] = input_node_ids
        self.output_node_id: int = output_node_id
        self._active_graph: Graph | None = None
        self._last_evaluation_time: datetime | None = None

    def _wire_graph(self):
        """Connect inputs and outputs to the nodes inputs and outputs"""
        if self._active_graph:
            return  # Already wired in
        id_, ready = self.recordable_id()
        if not ready:
            return
        if (gs := GlobalState.instance()).get(k := f"component::{id_}", None) is not None:
            raise RuntimeError(f"Component[{id_}] {self.signature.signature} already exists in graph")
        else:
            gs[k] = True  # Just write a marker for now

        self._active_graph = self.nested_graph_builder.make_instance(self.node_id, self, label=id_)
        self._active_graph.traits.set_traits(recordable_id=id_)
        self._active_graph.evaluation_engine = NestedEvaluationEngine(
            self.graph.evaluation_engine, NestedEngineEvaluationClock(self.graph.engine_evaluation_clock, self)
        )
        self._active_graph.initialise()

        for arg, node_ndx in self.input_node_ids.items():
            node: NodeImpl = self._active_graph.nodes[node_ndx]
            node.notify()
            ts = self.input[arg]
            node.input = node.input.copy_with(__init_args__=dict(owning_node=node), ts=ts)
            # Now we need to re-parent the pruned ts input.
            ts.re_parent(node.input)

        if self.output_node_id:
            node: Node = self._active_graph.nodes[self.output_node_id]
            # Replace the nodes output with the map node's output
            node.output = self.output

        if self.is_started | self.is_starting:
            self._active_graph.start()

    def initialise(self):
        self._wire_graph()

    def do_start(self):
        if self._active_graph:
            self._active_graph.start()
        else:
            self._wire_graph()
            if not self._active_graph:
                self.graph.schedule_node(self.node_ndx, self.graph.evaluation_clock.evaluation_time)

    def do_stop(self):
        if self._active_graph:
            self._active_graph.stop()

    def dispose(self):
        if self._active_graph:
            GlobalState.instance().pop(f"component::{self._active_graph.label}", None)
            self._active_graph.dispose()
            self._active_graph = None

    def eval(self):
        if self._active_graph is None:
            self._wire_graph()
            if self._active_graph is None:
                # Still pending
                return

        self.mark_evaluated()
        self._active_graph.evaluation_clock.reset_next_scheduled_evaluation_time()
        self._active_graph.evaluate_graph()
        self._active_graph.evaluation_clock.reset_next_scheduled_evaluation_time()

    def nested_graphs(self):
        if self._active_graph:
            return {0: self._active_graph}
        else:
            return {}

    def recordable_id(self) -> tuple[str, bool]:
        """The id and True or no id and False if required inputs are not ready yet"""
        outer_id = self.graph.traits.get_trait_or("recordable_id")
        id_ = (
            f"{'' if outer_id is None else outer_id}{'' if outer_id is None else '::'}{self.signature.record_replay_id}"
        )
        dependencies = [k for _, k, _, _ in Formatter().parse(id_) if k is not None]
        if any(k == "" for k in dependencies):
            raise RuntimeError(
                f"recordable_id: {id_} in signature: {self.signature.signature} has non-labeled format descriptors"
            )
        if dependencies:
            ts_values = [k for k in dependencies if k not in self.scalars]
            if ts_values and (
                not (self.is_started or self.is_starting) or not all(_get_ts_valid(self.inputs[k]) for k in ts_values)
            ):
                return id_, False
            args = {k: self.scalars[k] if k in self.scalars else _get_ts_value(self.inputs[k]) for k in dependencies}
            return id_.format(**args), True
        else:
            return id_, True


def _get_ts_valid(ts) -> bool:
    if ts.valid:
        v = ts.value
        if TimeSeriesReference.is_instance(v):
            return v.has_output and v.output.valid
        else:
            return True
    return False


def _get_ts_value(ts) -> Any:
    v = ts.value
    if TimeSeriesReference.is_instance(v):
        # This must have an output and it must be valid
        return v.output.value
    else:
        return v
