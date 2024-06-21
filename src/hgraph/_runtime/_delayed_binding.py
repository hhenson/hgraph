from dataclasses import dataclass
from typing import Generic

from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_port import WiringPort

__all__ = ("delayed_binding",)


@dataclass
class DelayedRanking:
    _previous_ranks: tuple[WiringPort, ...]
    _self: WiringPort


@dataclass
class DelayedBindingWiringPort(Generic[TIME_SERIES_TYPE]):
    _delegate: "_DelayedBindingWiringPort[TIME_SERIES_TYPE]"
    _bound: bool = False

    def __call__(self, ts: TIME_SERIES_TYPE = None) -> TIME_SERIES_TYPE:
        if ts is None:
            return self._delegate

        if self._bound:
            from hgraph._wiring._wiring_errors import CustomMessageWiringError
            raise CustomMessageWiringError(f"delayed_binding is already bounded")
        self._bound = True
        self._delegate.bind(ts)


@dataclass(frozen=True)
class _DelayedBindingWiringPort(WiringPort, Generic[TIME_SERIES_TYPE]):
    """
    A wiring port that is not yet bound to a node. This is used in the graph builder to create a placeholder for
    a wiring port that will be bound later.
    """
    node_instance: "WiringNodeInstance" = None
    output_type: HgTimeSeriesTypeMetaData = None

    def bind(self, ts: TIME_SERIES_TYPE):
        if self.output_type != ts.output_type:
            raise CustomMessageWiringError("The output type of the delayed binding port does not match the output type "
                                           "of the port being bound")

        object.__setattr__(self, "node_instance", ts.node_instance)
        object.__setattr__(self, "path", ts.path)


def delayed_binding(tp_: type[TIME_SERIES_TYPE]) -> DelayedBindingWiringPort[TIME_SERIES_TYPE]:
    """
    Supports using a time-series value before it has been defined. This can be useful when interleaving
    different computations, but keeping the definitions of the computations separate. This will not allow
    cycles to be formed in the final graph. This operator is possibly the only operator that will allow
    for accidental creation of cycles in the graph, as such, it should be used with great care.

    Example usage:

    ```
    binding = delayed_binding(TS[int])  # Declare the delayed biding giving it the type it represents
    ...
    out = my_compute_node(..., binding())  # Use the delayed_binding as an input.
    binding(out)  # Bind the value of the delayed_binding
    ```
    """
    return DelayedBindingWiringPort(_delegate=_DelayedBindingWiringPort(output_type=HgTimeSeriesTypeMetaData.parse_type(tp_)),)
