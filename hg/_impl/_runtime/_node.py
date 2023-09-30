import functools
from dataclasses import dataclass
from typing import Optional, Mapping, TYPE_CHECKING, Callable, Any

from frozendict import frozendict

from hg._runtime import Node, NodeSignature, Graph

if TYPE_CHECKING:
    from hg._types._ts_type import TimeSeriesInput, TimeSeriesOutput
    from hg._types._tsb_type import TimeSeriesBundleInput, TimeSeriesBundleOutput


@dataclass
class NodeImpl(Node):
    """
    Provide a basic implementation of the Node as a reference implementation.
    """
    node_ndx: int
    owning_graph_id: tuple[int, ...]
    signature: NodeSignature
    scalars: frozendict[str, Any]
    graph: Graph
    eval_fn: Callable
    start_fn: Callable = None
    stop_fn: Callable = None
    input: Optional["TimeSeriesBundleInput"] = None
    output: Optional["TimeSeriesOutput"] = None
    is_started: bool = False
    _kwargs: dict[str, Any] = None

    @functools.cached_property
    def node_id(self) -> tuple[int, ...]:
        """ Computed once and then cached """
        return self.owning_graph_id + (self.node_ndx,)

    @property
    def inputs(self) -> Optional[Mapping[str, "TimeSeriesInput"]]:
        return {k: self.input[k] for k in self.signature.time_series_inputs}

    @property
    def outputs(self) -> Optional[Mapping[str, "TimeSeriesOutput"]]:
        if len(self.signature.time_series_outputs) == 1:
            return {'out': self.output}
        else:
            return {k: self.output[k] for k in self.signature.time_series_outputs}

    def initialise(self):
        # TODO: Simplistic intialisation, this does not take into account STATE, or wired Outputs, etc.
        self._kwargs = {arg: self.input[arg] if arg in self.signature.time_series_inputs else self.scalars[arg] for arg
                        in self.signature.args}

    def eval(self):
        out = self.eval_fn(**self._kwargs)
        if out is not None:
            self.output.apply_result(out)

    def start(self):
        # TODO: Ultimately the start fn should have it's own call signature.
        if self.start_fn is not None:
            self.start_fn(**self._kwargs)

    def stop(self):
        # TODO: Ultimately the stop fn should have it's own call signature.
        if self.stop_fn is not None:
            self.stop_fn(**self._kwargs)

    def dispose(self):
        self._kwargs = None  # For neatness purposes only, not required here.

    def notify(self):
        """Notify the graph that this node needs to be evaluated."""
        # if self._