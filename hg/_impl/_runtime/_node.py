from dataclasses import dataclass
from typing import Optional, Mapping, TYPE_CHECKING, Callable, Any

from frozendict import frozendict

from hg._runtime import Node, NodeSignature

if TYPE_CHECKING:
    from hg._types._ts_type import TimeSeriesInput, TimeSeriesOutput
    from hg._types._tsb_type import TimeSeriesBundleInput, TimeSeriesBundleOutput


@dataclass
class NodeImpl(Node):
    """
    Provide a basic implementation of the Node as a reference implementation.
    """
    signature: NodeSignature
    input: Optional["TimeSeriesBundleInput"]
    output: Optional["TimeSeriesOutput"]
    eval_fn: Callable
    scalars: dict[str, Any]
    is_started: bool = False
    start_fn: Callable = None
    stop_fn: Callable = None
    _kwargs: dict[str, Any] = None

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
