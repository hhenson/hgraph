import functools
from dataclasses import dataclass
from typing import Optional, Mapping, TYPE_CHECKING, Callable, Any, Iterator

from hg._runtime import NodeSignature, Graph, Node

if TYPE_CHECKING:
    from hg._types._ts_type import TimeSeriesInput, TimeSeriesOutput
    from hg._types._tsb_type import TimeSeriesBundleInput

__all__ = ("NodeImpl",)


@dataclass
class NodeImpl:  # Node
    """
    Provide a basic implementation of the Node as a reference implementation.
    """
    node_ndx: int
    owning_graph_id: tuple[int, ...]
    signature: NodeSignature
    scalars: Mapping[str, Any]
    graph: Graph = None
    eval_fn: Callable = None
    start_fn: Callable = None
    stop_fn: Callable = None
    input: Optional["TimeSeriesBundleInput"] = None
    output: Optional["TimeSeriesOutput"] = None
    is_started: bool = False
    _kwargs: dict[str, Any] = None

    @functools.cached_property
    def node_id(self) -> tuple[int, ...]:
        """ Computed once and then cached """
        return self.owning_graph_id + tuple([self.node_ndx])

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
        pass

    def _initialise_kwargs(self):
        from hg._types._scalar_type_meta_data import Injector
        extras = {}
        for k, s in self.scalars.items():
            if isinstance(s, Injector):
                extras[k] = s(self)
        self._kwargs = {k: v for k, v in {**(self.input or {}), **self.scalars, **extras}.items() if
                                k in self.signature.args}

    def _initialise_inputs(self):
        if self.input:
            for k, ts in self.input.items():
                ts: TimeSeriesInput
                if self.signature.active_inputs is None or k in self.signature.active_inputs:
                    ts.make_active()

    def eval(self):
        if self.input:
            args = self.signature.valid_inputs if self.signature.valid_inputs else self.signature.time_series_inputs.keys()
            if not all(self.input[k].valid for k in args):
                return  # We should look into caching the result of this check.
                # This check could perhaps be set on a separate call?
        out = self.eval_fn(**self._kwargs)
        if out is not None:
            self.output.apply_result(out)

    def start(self):
        # TODO: Ultimately the start fn should have it's own call signature.
        self._initialise_kwargs()
        self._initialise_inputs()
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
        self.graph.schedule_node(self.node_ndx, self.graph.context.current_engine_time)


class GeneratorNodeImpl(NodeImpl):  # Node
    generator: Iterator = None
    next_value: object = None

    def start(self):
        self._initialise_kwargs()
        self.generator = self.eval_fn(**self._kwargs)
        self.graph.schedule_node(self.node_ndx, self.graph.context.current_engine_time)

    def eval(self):
        time, out = next(self.generator, (None, None))
        if out is not None and time is not None and time <= self.graph.context.current_engine_time:
            self.output.apply_result(out)
            self.next_value = None
            self.eval()  # We are going to apply now! Prepare next step,
            return
            # This should ultimately either produce no result or a result that is to be scheduled

        if self.next_value is not None:
            self.output.apply_result(self.next_value)
            self.next_value = None

        if time is not None and out is not None:
            self.next_value = out
            self.graph.schedule_node(self.node_ndx, time)
