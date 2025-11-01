from dataclasses import dataclass, field
from typing import Generic, Callable

from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
from hgraph._types._tsb_meta_data import HgTSBTypeMetaData
from hgraph._types._time_series_types import TIME_SERIES_TYPE, TIME_SERIES_TYPE_1
from hgraph._types._tss_meta_data import HgTSSTypeMetaData
from hgraph._types._typing_utils import nth
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

    _output_type: HgTimeSeriesTypeMetaData = None
    derived_wps: list["_DelayedBindingWiringPortItem"] = field(hash=False, default=None)

    def __post_init__(self): ...

    @property
    def output_type(self) -> HgTimeSeriesTypeMetaData:
        return self._output_type

    def bind(self, ts: TIME_SERIES_TYPE):
        if self.output_type != ts.output_type.dereference():
            raise CustomMessageWiringError(
                f"The output type of the delayed binding port does not match the output type {self.output_type}"
                f"of the port being bound {ts.output_type.dereference()}"
            )

        object.__setattr__(self, "node_instance", ts.node_instance)
        object.__setattr__(self, "path", ts.path)

        if self.derived_wps is not None:
            for dp in self.derived_wps:
                dp.bind(ts)

    def add_derived(self, derivator: Callable[[WiringPort], WiringPort], tp: type[TIME_SERIES_TYPE_1] = None):
        if self.derived_wps is None:
            object.__setattr__(self, "derived_wps", [])
        self.derived_wps.append(
            dp := _DelayedBindingWiringPortItem(node_instance=None, derivator=derivator, _output_type=tp)
        )
        return dp

    @property
    def key_set(self):
        if isinstance(self.output_type.dereference(), HgTSDTypeMetaData):
            return self.add_derived(lambda x: x.key_set, HgTSSTypeMetaData(self.output_type.key_tp))

        raise AttributeError(f"{self.output_type} does not have a key_set property")

    def __error__(self, trace_back_depth: int = 1, capture_values: bool = False) -> "WiringPort":
        raise AttributeError(f"__error__ not supported on delayed bindings")

    def __getitem__(self, item: str) -> "WiringPort":
        if isinstance(self.output_type.dereference(), (HgTSDTypeMetaData, HgTSLTypeMetaData)):
            return self.add_derived(lambda x: x[item], self.output_type.value_tp)
        if isinstance(self.output_type.dereference(), (HgTSBTypeMetaData)):
            return self.add_derived(
                lambda x: getattr(x, item), nth(self.output_type.bundle_schema_tp.meta_data_schema.values(), item)
            )

        return super().__getitem__(item)

    def __getattr__(self, item):
        if isinstance(self.output_type.dereference(), HgTSBTypeMetaData):
            return self.add_derived(
                lambda x: getattr(x, item), self.output_type.bundle_schema_tp.meta_data_schema[item]
            )

        return super().__getattr__(item)

    def copy_with(self, **kwargs):
        if isinstance(self.output_type.dereference(), HgTSBTypeMetaData):
            return self.add_derived(lambda x: x.copy_with(**kwargs), self.output_type)

        return super().copy_with(**kwargs)


@dataclass(frozen=True)
class _DelayedBindingWiringPortItem(_DelayedBindingWiringPort, Generic[TIME_SERIES_TYPE]):
    """
    A wiring port that is not yet bound to a node is derived from another delayed wiring port.
    """

    derivator: Callable = None

    def __post_init__(self): ...

    def bind(self, ts: TIME_SERIES_TYPE_1):
        derived = self.derivator(ts)
        super().bind(derived)


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
    return DelayedBindingWiringPort(
        _delegate=_DelayedBindingWiringPort(node_instance=None, _output_type=HgTimeSeriesTypeMetaData.parse_type(tp_))
    )
