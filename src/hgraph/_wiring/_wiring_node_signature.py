from dataclasses import dataclass
from enum import Enum
from inspect import isfunction, signature
from typing import Type, get_type_hints, Any, Optional, TypeVar, Mapping, cast, Callable, GenericAlias, _GenericAlias
from functools import reduce
from operator import or_
from typing import Type, get_type_hints, Any, Optional, TypeVar, Mapping, cast

from frozendict import frozendict

from hgraph._types._scalar_type_meta_data import HgEvaluationClockType, HgEvaluationEngineApiType, HgStateType, \
    HgReplayType
from hgraph._runtime._node import InjectableTypes
from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData, HgOutputType, HgSchedulerType, \
    HgTypeOfTypeMetaData
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._type_meta_data import HgTypeMetaData, AUTO_RESOLVE
from hgraph._types._type_meta_data import ParseError
from hgraph._types._tsb_meta_data import HgTimeSeriesSchemaTypeMetaData, HgTSBTypeMetaData
from hgraph._wiring._source_code_details import SourceCodeDetails
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import IncorrectTypeBinding

__all__ = ("extract_signature", "WiringNodeType", "WiringNodeSignature", "extract_hg_type",
           "extract_hg_time_series_type", "extract_scalar_type", "extract_injectable_inputs")


class WiringNodeType(Enum):
    PUSH_SOURCE_NODE = 0
    PULL_SOURCE_NODE = 1
    COMPUTE_NODE = 2
    SINK_NODE = 3
    GRAPH = 4
    STUB = 5  # A stub acts as a placeholder for a node that will never be part of the runtime or builder graph
    REF_SVC = 6
    SUBS_SVC = 7
    REQ_REP_SVC = 8
    SVC_IMPL = 9


def extract_hg_type(tp) -> HgTypeMetaData:
    tp_ = HgTypeMetaData.parse_type(tp)
    if tp_ is None:
        raise ParseError(f"'{tp}' is not a valid HgType")
    return tp_


def extract_hg_time_series_type(tp) -> HgTimeSeriesTypeMetaData | None:
    if tp is None or tp is type(None):
        return

    tp_ = HgTimeSeriesTypeMetaData.parse_type(tp)
    if tp_ is None:
        raise ParseError(f"'{tp}' is not a valid HgTimeSeriesType")

    return tp_


def extract_scalar_type(tp: Type) -> HgScalarTypeMetaData:
    tp_ = HgScalarTypeMetaData.parse_type(tp)
    if tp_ is None:
        raise ParseError(f"'{tp}' is not a valid HgScalarType")
    return tp_


@dataclass(frozen=True, unsafe_hash=True, )
class WiringNodeSignature:
    """
    The wiring node signature is similar to the final node signature, but it deals with templated node instances,
    which are only partially specified, one we build the final data structure then we will convert to formally
    specified signatures.
    """
    node_type: WiringNodeType
    name: str  # This will come from the function
    args: tuple[str, ...]  # Require in order enumeration.
    defaults: frozendict[str, Any]
    input_types: frozendict[str, HgTypeMetaData]  # Inputs are both scalar and time-series at this point
    output_type: HgTimeSeriesTypeMetaData | None  # By definition outputs must be time-series if they are defined
    src_location: SourceCodeDetails
    active_inputs: frozenset[str] | None
    valid_inputs: frozenset[str] | None
    all_valid_inputs: frozenset[str] | None
    unresolved_args: frozenset[str]
    time_series_args: frozenset[str]
    injectable_inputs: InjectableTypes = InjectableTypes(0)
    # It is not possible to have an unresolved output with un-resolved inputs as we resolve output using information
    # supplied via inputs
    label: str | None = None  # A label if provided, this can help to disambiguate the node
    record_and_replay_id: str | None = None

    @property
    def uses_scheduler(self) -> bool:
        return InjectableTypes.SCHEDULER in self.injectable_inputs

    @property
    def uses_clock(self) -> bool:
        return InjectableTypes.CLOCK in self.injectable_inputs

    @property
    def uses_engine(self) -> bool:
        return InjectableTypes.ENGINE_API in self.injectable_inputs

    @property
    def uses_state(self) -> bool:
        return InjectableTypes.STATE in self.injectable_inputs

    @property
    def uses_output_feedback(self) -> bool:
        return InjectableTypes.OUTPUT in self.injectable_inputs

    def as_dict(self) -> dict:
        return dict(node_type=self.node_type, name=self.name, args=self.args, defaults=self.defaults,
                    input_types=self.input_types, output_type=self.output_type, src_location=self.src_location,
                    active_inputs=self.active_inputs, valid_inputs=self.valid_inputs,
                    all_valid_inputs=self.all_valid_inputs,
                    unresolved_args=self.unresolved_args, time_series_args=self.time_series_args,
                    injectable_inputs=self.injectable_inputs, label=self.label,
                    record_and_replay_id=self.record_and_replay_id)

    def copy_with(self, **kwargs: Any) -> "WiringNodeSignature":
        kwargs_ = self.as_dict() | kwargs
        return WiringNodeSignature(**kwargs_)

    @property
    def signature(self) -> str:
        args = (f'{arg}: {str(self.input_types[arg])}'
                for arg in self.args)
        return_ = '' if self.output_type is None else f" -> {str(self.output_type)}"
        return f"{self.name}({', '.join(args)}){return_}"

    @property
    def is_resolved(self) -> bool:
        return not self.unresolved_args and (not self.output_type or self.output_type.is_resolved)

    @property
    def is_weakly_resolved(self) -> bool:
        return not self.unresolved_args

    @property
    def scalar_inputs(self) -> Mapping[str, HgScalarTypeMetaData]:
        """Split out scalar inputs from time-series inputs """
        return frozendict({k: v for k, v in self.input_types.items() if v.is_scalar})

    @property
    def time_series_inputs(self) -> Mapping[str, HgTimeSeriesTypeMetaData]:
        return frozendict({k: v for k, v in self.input_types.items() if not v.is_scalar})

    def build_resolution_dict(self, pre_resolved_types: dict[TypeVar, HgTypeMetaData | Callable],
                              kwarg_types, kwargs) -> dict[TypeVar, HgTypeMetaData]:
        """Expect kwargs to be a dict of arg to type mapping / value mapping"""
        resolution_dict: dict[TypeVar, HgTypeMetaData] = {k: v for k, v in pre_resolved_types.items() if
                                                          isinstance(v, HgTypeMetaData)} if pre_resolved_types else {}
        resolvers_dict: dict[TypeVar, Callable] = {k: v for k, v in pre_resolved_types.items() if
                                                          isfunction(v)} if pre_resolved_types else {}
        for arg, meta_data in self.input_types.items():
            # This will validate the input type against the signature's type so don't short-cut this logic!
            with WiringContext(current_arg=arg):
                if not meta_data.is_scalar and (kwt := kwarg_types.get(arg)) is not None and kwt.is_scalar:
                    meta_data: HgTimeSeriesTypeMetaData
                    meta_data.build_resolution_dict_from_scalar(resolution_dict, kwt, kwargs[arg])
                else:
                    meta_data.build_resolution_dict(resolution_dict, kwarg_types.get(arg))
        # now ensures all "resolved" items are actually resolved
        out_dict = {}
        all_resolved = True
        for k, v in resolution_dict.items():
            out_dict[k] = v if v.is_resolved else v.resolve(resolution_dict)
            all_resolved &= out_dict[k].is_resolved

        if resolvers_dict:
            scalars = {k: v for k, v in kwargs.items() if (kwt := kwarg_types.get(k)) and kwt.is_scalar}
            for k, v in pre_resolved_types.items():
                if isfunction(v) and k not in out_dict:
                    resolved = v(resolution_dict, scalars)
                    if isinstance(resolved, (type, GenericAlias, _GenericAlias, TypeVar)):
                        resolved = HgTypeMetaData.parse_type(resolved)
                    out_dict[k] = resolved

        if not all_resolved:
            raise ParseError(f"Unable to build a resolved resolution dictionary, due to:"
                             f"{';'.join(f' {k}: {v}' for k, v in out_dict.items() if not v.is_resolved)}")
        return out_dict

    def resolve_inputs(self, resolution_dict: dict[TypeVar, HgTypeMetaData], weak=False) -> Mapping[
        str, HgTypeMetaData]:
        if self.is_resolved:
            return self.input_types

        input_types = {}
        for arg, meta_data in self.input_types.items():
            input_types[arg] = meta_data.resolve(resolution_dict, weak)
        return frozendict(input_types)

    def resolve_output(self, resolution_dict: dict[TypeVar, HgTypeMetaData], weak=False) -> Optional[HgTypeMetaData]:
        if self.output_type is None:
            return None
        out_type = self.output_type.resolve(resolution_dict, weak)
        if type(out_type) == HgTimeSeriesSchemaTypeMetaData:
            raise IncorrectTypeBinding(HgTSBTypeMetaData(out_type), out_type)
        return out_type

    def resolve_valid_inputs(self, **kwargs) -> frozenset[str]:
        optional_inputs = set(k for k in self.time_series_args if kwargs[k] is None)
        if optional_inputs:
            if self.valid_inputs:
                return frozenset(k for k in self.valid_inputs if k not in optional_inputs)
            else:
                return frozenset(k for k in self.time_series_args if k not in optional_inputs)
        else:
            return self.valid_inputs

    def resolve_all_valid_inputs(self, **kwargs) -> frozenset[str] | None:
        optional_inputs = set(k for k in self.time_series_args if kwargs[k] is None)
        if optional_inputs:
            if self.all_valid_inputs:
                # Remove any optional inputs from validity requirements if not provided.
                return frozenset(k for k in self.all_valid_inputs if k not in optional_inputs)
            else:
                return None
        else:
            return self.all_valid_inputs

    def resolve_auto_resolve_kwargs(self, resolution_dict, kwarg_types, kwargs, resolved_inputs):
        new_resolved_inputs = {}
        for arg, v in self.defaults.items():
            if v is AUTO_RESOLVE:
                kwargs[arg] = kwarg_types[arg].value_tp.resolve(resolution_dict).py_type
                new_resolved_inputs[arg] = kwarg_types[arg].resolve(resolution_dict)
        if new_resolved_inputs:
            return frozendict(dict(resolved_inputs, **new_resolved_inputs))
        else:
            return resolved_inputs

    def resolve_auto_const_and_type_kwargs(self, kwarg_types, kwargs):
        """
        If there are scalar inputs that should be time-series inputs, then convert them to const inputs.
        If there are type-of-type inputs, then use the resolved type instead of the input type. This ensures
        we use fully resolved inputs when doing comparisons of WiringNodeInstance'. Which reduces the number
        of duplicate nodes we create (especially for const nodes).
        """
        for arg, v in self.input_types.items():
            if not v.is_scalar and kwarg_types[arg].is_scalar:
                from hgraph.nodes import const
                kwargs[arg] = const(kwargs[arg], tp=v.py_type)
            if type(v) is HgTypeOfTypeMetaData:
                kwargs[arg] = cast(HgTypeOfTypeMetaData, v).value_tp.py_type

    def validate_resolved_types(self, kwarg_types, kwargs):
        for k, v in self.input_types.items():
            from hgraph._wiring._wiring_port import WiringPort
            if isinstance(kwargs[k], WiringPort):
                if not v.dereference().matches(kwargs[k].output_type.dereference()):
                    raise IncorrectTypeBinding(v, kwarg_types[k])
            else:
                if not v.dereference().matches(kwarg_types[k].dereference()):
                    raise IncorrectTypeBinding(v, kwarg_types[k])

def extract_signature(fn, wiring_node_type: WiringNodeType,
                      active_inputs: frozenset[str] | None = None,
                      valid_inputs: frozenset[str] | None = None,
                      all_valid_inputs: frozenset[str] | None = None) -> WiringNodeSignature:
    """
    Performs signature extract that will work for python 3.9 (and possibly above)
    :param fn:
    :return:
    """
    name = fn.__name__
    annotations = get_type_hints(fn)
    code = fn.__code__
    args: tuple[str, ...] = tuple(signature(fn).parameters.keys())
    filename = code.co_filename
    first_line = code.co_firstlineno
    if fn_defaults := fn.__defaults__:
        defaults = frozendict((k, v) for k, v in zip(args[len(args) - len(fn_defaults):], fn_defaults))
    else:
        defaults = frozendict()
    # Once we start defaulting, all attributes must be defaulted, so we can count backward
    # to know where to apply the defaults.
    input_types: frozendict[str, HgTypeMetaData] = frozendict(
        (k, extract_hg_type(v) if (k != "_output" or defaults.get("_output", True) is not None) else HgOutputType(v))
        for
        k, v in annotations.items() if k != "return")
    output_type = extract_hg_time_series_type(annotations.get("return", None))
    if output_type is not None and type(output_type) is HgTimeSeriesSchemaTypeMetaData:
        raise ParseError(f"The output type is not valid, did you mean TSB[{output_type.py_type.__name__}]")
    unresolved_inputs = frozenset(a for a in args if not input_types[a].is_resolved)
    time_series_inputs = frozenset(a for a in args if not input_types[a].is_scalar)

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

    if active_inputs is not None:
        assert all(a in input_types for a in active_inputs), \
            f"active inputs {active_inputs} are not in the signature for {name}"
    if valid_inputs is not None:
        assert all(a in input_types for a in valid_inputs), \
            f"valid inputs {valid_inputs} are not in the signature for {name}"
    if all_valid_inputs is not None:
        assert all(a in input_types for a in all_valid_inputs), \
            f"all_valid inputs {all_valid_inputs} are not in signature for {name}"
        if valid_inputs is not None:
            assert len(set(all_valid_inputs).intersection(valid_inputs)) == 0, \
                f"valid and all_valid inputs are overlapping, {all_valid_inputs}, {valid_inputs} for {name}"
    # Note graph signatures can be any of the above, so additional validation would need to be performed in the
    # graph expansion logic.

    injectable_inputs = extract_injectable_inputs(**input_types)

    return WiringNodeSignature(
        node_type=wiring_node_type,
        name=name,
        args=args,
        defaults=defaults,
        input_types=input_types,
        output_type=output_type,
        active_inputs=active_inputs,
        valid_inputs=valid_inputs,
        all_valid_inputs=all_valid_inputs,
        src_location=SourceCodeDetails(file=filename, start_line=first_line),
        unresolved_args=unresolved_inputs,
        time_series_args=time_series_inputs,
        injectable_inputs=injectable_inputs,
        label=None,
        record_and_replay_id=None
    )


def extract_injectable_inputs(**kwargs) -> InjectableTypes:
    return reduce(or_,
                  ({
                       HgSchedulerType: InjectableTypes.SCHEDULER,
                       HgEvaluationClockType: InjectableTypes.CLOCK,
                       HgEvaluationEngineApiType: InjectableTypes.ENGINE_API,
                       HgStateType: InjectableTypes.STATE,
                       HgOutputType: InjectableTypes.OUTPUT,
                       HgReplayType: InjectableTypes.REPLAY_STATE,
                   }.get(type(v), InjectableTypes(0)) for v in kwargs.values()),
                  InjectableTypes(0)
                  )
