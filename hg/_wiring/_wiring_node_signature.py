from dataclasses import dataclass
from enum import Enum
from typing import Type, get_type_hints, Any, Optional, TypeVar

from hg._runtime import SourceCodeDetails
from hg._types._scalar_type_meta_data import HgScalarTypeMetaData
from hg._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hg._types._type_meta_data import HgTypeMetaData

__all__ = ("extract_signature", "WiringNodeType", "WiringNodeSignature")


class WiringNodeType(Enum):
    PUSH_SOURCE_NODE = 0
    PULL_SOURCE_NODE = 1
    COMPUTE_NODE = 2
    SINK_NODE = 3
    GRPAH = 4


def extract_hg_type(tp) -> HgTypeMetaData:
    tp_ = HgTypeMetaData.parse(tp)
    if tp_ is None:
        raise RuntimeError("Unexpected Type in the bagging area: ", tp)
    return tp_


def extract_hg_time_series_type(tp) -> HgTimeSeriesTypeMetaData:
    if tp is None:
        return None

    tp_ = HgTimeSeriesTypeMetaData.parse(tp)
    if tp_ is None:
        raise RuntimeError("Unexpected Type in the bagging area: ", tp)

    return tp_


def extract_scalar_type(tp: Type) -> HgScalarTypeMetaData:
    tp_ = HgScalarTypeMetaData.parse(tp)
    if tp_ is None:
        raise RuntimeError("Unexpected Type in the bagging area: ", tp)
    return tp_


@dataclass(frozen=True, unsafe_hash=True)
class WiringNodeSignature:
    """
    The wiring node signature is similar to the final node signature, but it deals with templated node instances,
    which are only partially specified, one we build the final data structure then we will convert to formally
    specified signatures.
    """
    node_type: WiringNodeType
    name: str  # This will come from the function
    args: tuple[str]
    defaults: dict[str, Any]
    input_types: dict[str, HgTypeMetaData]
    output_type: Optional[HgTypeMetaData]
    src_location: SourceCodeDetails
    unresolved_args: tuple[str]
    time_series_args: tuple[str]
    # It is not possible to have an unresolved output with un-resolved inputs as we resolve output using information
    # supplied via inputs
    label: Optional[str] = None  # A label if provided, this can help to disambiguate the node

    @property
    def is_resolved(self) -> bool:
        return not self.unresolved_args and (not self.output_type or self.output_type.is_resolved)

    def build_resolution_dict(self, pre_resolved_types: dict[TypeVar, HgTypeMetaData],
                              **kwargs) -> dict[TypeVar, HgTypeMetaData]:
        """Expect kwargs to be a dict of arg to type mapping / value mapping"""
        resolution_dict = dict(pre_resolved_types) if pre_resolved_types else {}
        for arg, meta_data in self.input_types.items():
            # This will validate the input type against the signature's type so don't short-cut this logic!
            meta_data.build_resolution_dict(resolution_dict, kwargs[arg])
        return resolution_dict

    def resolve_inputs(self, resolution_dict: dict[TypeVar, HgTypeMetaData]) -> dict[str, HgTypeMetaData]:
        if self.is_resolved:
            return self.input_types

        input_types = {}
        for arg, meta_data in self.input_types.items():
            input_types[arg] = meta_data.resolve(resolution_dict)
        return input_types

    def resolve_output(self, resolution_dict: dict[TypeVar, HgTypeMetaData]) -> Optional[HgTypeMetaData]:
        if self.output_type is None:
            return None
        return self.output_type.resolve(resolution_dict)


def extract_signature(fn, wiring_node_type: WiringNodeType) -> WiringNodeSignature:
    """
    Performs signature extract that will work for python 3.9 (and possibly above)
    :param fn:
    :return:
    """
    name = fn.__name__
    annotations = get_type_hints(fn)
    code = fn.__code__
    args: tuple[str, ...] = code.co_varnames[:code.co_argcount]
    filename = code.co_filename
    first_line = code.co_firstlineno
    if fn_defaults := fn.__defaults__:
        defaults = {k: v for k, v in zip(args[len(args) - len(fn_defaults):], fn_defaults)}
    else:
        defaults = {}
    # Once we start defaulting, all attributes must be defaulted, so we can count backward
    # to know where to apply the defaults.
    input_types: dict[str, HgTypeMetaData] = {k: extract_hg_type(v) for k, v in annotations.items() if k != "return"}
    output_type = extract_hg_time_series_type(annotations.get("return", None))
    unresolved_inputs = tuple(a for a in args if not input_types[a].is_resolved)
    time_series_inputs = tuple(a for a in args if not input_types[a].is_scalar)

    # Validations to ensure the signature matches the node type
    if wiring_node_type in (WiringNodeType.PULL_SOURCE_NODE, WiringNodeType.PUSH_SOURCE_NODE):
        assert len(time_series_inputs) == 0, \
            f"source node '{name}' has time-series inputs ({time_series_inputs}) (not a valid signature)"
        assert output_type is not None, f"source node '{name}' has no output (not a valid signature)"
    elif wiring_node_type is WiringNodeType.COMPUTE_NODE:
        assert len(time_series_inputs) > 0, \
            f"compute node '{name}' has no time-series inputs (not a valid signature)"
        assert output_type is not None, f"compute node '{name}' has no output (not a valid signature)"
    elif wiring_node_type is WiringNodeType.SINK_NODE:
        assert len(time_series_inputs) > 0, \
            f"sink node '{name}' has no time-series inputs (not a valid signature)"
        assert output_type is None, f"sink node '{name}' has an output (not a valid signature)"

    # Note graph signatures can be any of the above, so additional validation would need to be performed in the
    # graph expaction logic.

    return WiringNodeSignature(node_type=wiring_node_type,
                               name=name,
                               args=args,
                               defaults=defaults,
                               input_types=input_types,
                               output_type=output_type,
                               src_location=SourceCodeDetails(file=filename, start_line=first_line),
                               unresolved_args=unresolved_inputs,
                               time_series_args=time_series_inputs,
                               label=None)



