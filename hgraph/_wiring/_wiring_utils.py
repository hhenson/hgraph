from collections.abc import Set
from dataclasses import dataclass
from typing import Mapping, Any, cast, TYPE_CHECKING, _GenericAlias, Callable

from frozendict import frozendict

from hgraph._types._ref_meta_data import HgREFTypeMetaData
from hgraph._types._ref_type import REF
from hgraph._types._scalar_types import Size
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._ts_type import TS
from hgraph._types._tsb_type import TSB
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
from hgraph._types._tsd_type import TSD
from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
from hgraph._types._tsl_type import TSL
from hgraph._types._tss_type import TSS
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._stub_wiring_node import create_input_stub, create_output_stub
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext
from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass
from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_port import WiringPort

if TYPE_CHECKING:
    from hgraph._builder._graph_builder import GraphBuilder

__all__ = (
    "stub_wiring_port",
    "StubWiringPort",
    "as_reference",
    "wire_nested_graph",
    "extract_stub_node_indices",
    "pretty_str_types",
)


@dataclass(frozen=True)
class StubWiringPort(WiringPort):
    _value_tp: HgTypeMetaData | None = None

    @property
    def output_type(self) -> HgTypeMetaData:
        return self._value_tp


def stub_wiring_port(value_tp: HgTimeSeriesTypeMetaData) -> WiringPort:
    return StubWiringPort(node_instance=None, _value_tp=value_tp)


def as_reference(tp_: HgTimeSeriesTypeMetaData, is_multiplexed: bool = False) -> HgTypeMetaData:
    """
    Create a reference type for the supplied type if the type is not already a reference type.
    """
    if is_multiplexed:
        # If multiplexed type, we want references to the values not the whole output.
        if type(tp_) is HgTSDTypeMetaData:
            tp_: HgTSDTypeMetaData
            return HgTSDTypeMetaData(
                tp_.key_tp,
                tp_.value_tp.as_reference(),
            )
        elif type(tp_) is HgTSLTypeMetaData:
            tp_: HgTSLTypeMetaData
            return HgTSLTypeMetaData(
                tp_.value_tp.as_reference(),
                tp_.size_tp,
            )
        else:
            raise CustomMessageWiringError(f"Unable to create reference for multiplexed type: {tp_}")
    else:
        return tp_.as_reference()


def wire_nested_graph(
    fn: WiringNodeClass,
    input_types: Mapping[str, HgTypeMetaData],
    scalars: Mapping[str, Any],
    outer_wiring_node_signature: WiringNodeSignature,
    key_arg: str,
    depth: int = 1,
    input_stub_fn: Callable[[REF[TIME_SERIES_TYPE]], REF[TIME_SERIES_TYPE]] = None,
    output_stub_fn: Callable[[REF[TIME_SERIES_TYPE]], REF[TIME_SERIES_TYPE]] = None,
) -> ["GraphBuilder", tuple]:
    """
    Wire the inner function using stub inputs and wrap stub outputs.
    The outer wiring node signature is used to supply to the wiring graph context, this is for error and stack trace
    uses.
    """
    from hgraph._wiring._graph_builder import create_graph_builder
    from hgraph._builder._ts_builder import TimeSeriesBuilderFactory

    if temp_factory := not TimeSeriesBuilderFactory.has_instance():
        TimeSeriesBuilderFactory.declare_default_factory()

    with WiringNodeInstanceContext(depth), WiringGraphContext(outer_wiring_node_signature) as context:
        inputs_ = {}
        for k, v in input_types.items():
            if v.is_scalar:
                inputs_[k] = scalars[k]
            else:
                inputs_[k] = create_input_stub(k, cast(HgTimeSeriesTypeMetaData, v), k == key_arg, input_stub_fn)

        out = fn(**inputs_)
        if out is not None and out.output_type is not None:
            create_output_stub(cast(WiringPort, out), output_stub_fn)
        sink_nodes = context.pop_sink_nodes()
        reassignable = context.pop_reassignable_items()
        builder = create_graph_builder(sink_nodes, False)

    if temp_factory:
        TimeSeriesBuilderFactory.un_declare()

    return builder, reassignable


def extract_stub_node_indices(inner_graph, input_args: Set[str]) -> tuple[frozendict[str, int], int]:
    """Process the stub graph identifying the input and output nodes for the associated stubs."""

    input_node_ids = {}
    output_node_id = None
    STUB_PREFIX = "stub:"
    STUB_PREFIX_LEN = len(STUB_PREFIX)
    for node_ndx, node_builder in enumerate(inner_graph.node_builders):
        if (inner_node_signature := node_builder.signature).name.startswith(STUB_PREFIX):
            if (arg := inner_node_signature.name[STUB_PREFIX_LEN:]) in input_args:
                input_node_ids[arg] = node_ndx
            elif arg == "__out__":
                output_node_id = node_ndx
    return frozendict(input_node_ids), output_node_id


def pretty_str_types(value: Any) -> str:
    if isinstance(value, _GenericAlias):
        value: _GenericAlias
        return f"{pretty_str_types(value.__origin__)}[{', '.join(pretty_str_types(a) for a in value.__args__)}]"

    if isinstance(value, type):
        return {
            TS: "TS",
            TSL: "TSL",
            TSD: "TSD",
            TSB: "TSB",
            TSS: "TSS",
            Size: "Size",
        }.get(value, value.__name__)

    return str(value)
