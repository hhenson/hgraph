from dataclasses import dataclass
from enum import Enum
from functools import reduce
from inspect import isfunction, signature, Parameter
from operator import or_
from typing import Callable, GenericAlias, _GenericAlias
from typing import Type, get_type_hints, Any, Optional, TypeVar, Mapping, cast

from frozendict import frozendict

from hgraph._runtime._node import InjectableTypes
from hgraph._types._scalar_type_meta_data import (
    HgEvaluationClockType,
    HgEvaluationEngineApiType,
    HgStateType,
    HgRecordableStateType,
    RecordableStateInjector,
    HgLoggerType,
    HgNodeType,
)
from hgraph._types._scalar_type_meta_data import (
    HgScalarTypeMetaData,
    HgOutputType,
    HgSchedulerType,
    HgTypeOfTypeMetaData,
)
from hgraph._types._scalar_types import DEFAULT, Size
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._time_series_types import OUT
from hgraph._types._tsb_meta_data import HgTimeSeriesSchemaTypeMetaData, HgTSBTypeMetaData
from hgraph._types._type_meta_data import HgTypeMetaData, AUTO_RESOLVE
from hgraph._types._type_meta_data import ParseError
from hgraph._wiring._source_code_details import SourceCodeDetails
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import IncorrectTypeBinding, CustomMessageWiringError, RequirementsNotMetWiringError

__all__ = (
    "extract_signature",
    "WiringNodeType",
    "WiringNodeSignature",
    "extract_hg_type",
    "extract_hg_time_series_type",
    "extract_scalar_type",
    "extract_injectable_inputs",
)


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
    OPERATOR = 10  # Similar to STUB, expect in this case will be replaced with a matched node.
    ADAPTOR = 11
    ADAPTOR_IMPL = 12
    SERVICE_ADAPTOR = 13
    SERVICE_ADAPTOR_IMPL = 14
    COMPONENT = 15  # A graph with constraints that allows for record, replay, etc.
    CONST_FN = 16  # A constant value function based node.


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


@dataclass(frozen=True, unsafe_hash=True, kw_only=True)
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
    context_inputs: frozenset[str] | None
    unresolved_args: frozenset[str]
    time_series_args: frozenset[str]
    injectable_inputs: InjectableTypes = InjectableTypes(0)
    # It is not possible to have an unresolved output with un-resolved inputs as we resolve output using information
    # supplied via inputs
    label: str | None = None  # A label if provided, this can help to disambiguate the node
    record_and_replay_id: str | None = None
    deprecated: str | bool = False
    requires: Callable[[...], bool] | None = None
    kw_only_args: tuple[str, ...] = None
    var_arg: str = None
    var_kwarg: str = None
    default_type_arg: TypeVar = None

    def __repr__(self):
        return self.signature

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
    def uses_recordable_state(self) -> bool:
        return InjectableTypes.RECORDABLE_STATE in self.injectable_inputs

    def _recordable_state(self) -> tuple[str, HgRecordableStateType] | tuple[None, None]:
        return next(((arg, tp) for arg, tp in self.input_types.items() if type(tp) is HgRecordableStateType), (None, None))

    @property
    def recordable_state_arg(self) -> str | None:
        return self._recordable_state()[0]

    @property
    def recordable_state(self) -> RecordableStateInjector | None:
        return self._recordable_state()[1]

    @property
    def uses_output_feedback(self) -> bool:
        return InjectableTypes.OUTPUT in self.injectable_inputs

    def as_dict(self) -> dict:
        return dict(
            node_type=self.node_type,
            name=self.name,
            args=self.args,
            defaults=self.defaults,
            input_types=self.input_types,
            output_type=self.output_type,
            src_location=self.src_location,
            active_inputs=self.active_inputs,
            valid_inputs=self.valid_inputs,
            all_valid_inputs=self.all_valid_inputs,
            context_inputs=self.context_inputs,
            unresolved_args=self.unresolved_args,
            time_series_args=self.time_series_args,
            injectable_inputs=self.injectable_inputs,
            label=self.label,
            record_and_replay_id=self.record_and_replay_id,
            deprecated=self.deprecated,
            requires=self.requires,
            kw_only_args=self.kw_only_args,
            var_arg=self.var_arg,
            var_kwarg=self.var_kwarg,
            default_type_arg=self.default_type_arg,
        )

    def copy_with(self, **kwargs: Any) -> "WiringNodeSignature":
        kwargs_ = self.as_dict() | kwargs
        return WiringNodeSignature(**kwargs_)

    @property
    def signature(self) -> str:
        args = (f"{arg}: {str(self.input_types[arg])}" for arg in self.args)
        return_ = "" if self.output_type is None else f" -> {str(self.output_type)}"
        return f"{self.name}({', '.join(args)}){return_}"

    @property
    def is_resolved(self) -> bool:
        return not self.unresolved_args and (not self.output_type or self.output_type.is_resolved)

    @property
    def is_weakly_resolved(self) -> bool:
        return not self.unresolved_args

    @property
    def type_vars(self):
        typevars = set()
        if not self.is_resolved:
            for arg, v in self.input_types.items():
                typevars.update(v.type_vars)
            for k, v in self.defaults.items():
                if isinstance(v, TypeVar):
                    typevars.add(v)
            if self.output_type is not None:
                typevars.update(self.output_type.type_vars)
        return frozenset(typevars)

    @property
    def scalar_inputs(self) -> Mapping[str, HgScalarTypeMetaData]:
        """Split out scalar inputs from time-series inputs"""
        return frozendict({k: v for k, v in self.input_types.items() if v.is_scalar})

    @property
    def time_series_inputs(self) -> Mapping[str, HgTimeSeriesTypeMetaData]:
        return frozendict({k: v for k, v in self.input_types.items() if not v.is_scalar})

    @property
    def positional_inputs(self) -> Mapping[str, HgTypeMetaData]:
        return frozendict({k: v for k, v in self.input_types.items() if k not in self.kw_only_args})

    @property
    def kw_only_inputs(self) -> Mapping[str, HgTypeMetaData]:
        return frozendict({k: v for k, v in self.input_types.items() if k in self.kw_only_args})

    @property
    def non_autoresolve_inputs(self) -> Mapping[str, HgTypeMetaData]:
        return frozendict({k: v for k, v in self.input_types.items() if self.defaults.get(k) is not AUTO_RESOLVE})

    @property
    def non_injectable_or_auto_resolvable_inputs(self) -> Mapping[str, HgTypeMetaData]:
        return frozendict({
            k: v
            for k, v in self.input_types.items()
            if self.defaults.get(k) is not AUTO_RESOLVE and not v.is_injectable
        })

    @property
    def non_defaulted_arguments(self) -> Mapping[str, HgTypeMetaData]:
        """
        This can be used to work out what inputs may be required, this does not catch *args or **kwargs type
        args though.
        """
        return frozendict({k: v for k, v in self.input_types.items() if k not in self.defaults})

    def convert_kwargs_to_types(self, _ensure_match=True, **kwargs) -> dict[str, HgTypeMetaData]:
        """Attempt to convert input types to better support type resolution"""
        # We only need to extract un-resolved values
        kwarg_types = {}
        for k, v in self.input_types.items():
            with WiringContext(current_arg=k):
                arg = kwargs.get(k, self.defaults.get(k))
                if arg is None:
                    if v.is_injectable:
                        # For injectables we expect the value to be None, and the type must already be resolved.
                        kwarg_types[k] = v
                    else:
                        # We should wire in a null source
                        if k in (self.var_arg, self.var_kwarg):
                            kwarg_types[k] = self.input_types[k].parse_value(arg)
                        elif k in self.defaults:
                            kwarg_types[k] = v
                        elif _ensure_match:
                            raise CustomMessageWiringError(
                                f"Argument '{k}' is not marked as optional, but no value was supplied"
                            )
                if k in self.time_series_args:
                    # This should then get a wiring node, and we would like to extract the output type,
                    # But this is optional, so we should ensure that the type is present
                    if arg is None:
                        continue  # We will wire in a null source later
                    from hgraph import WiringPort

                    if not isinstance(arg, WiringPort):
                        if k == self.var_arg and isinstance(arg, (list, tuple)):
                            tp = self.input_types[k].parse_value(arg)
                        elif k == self.var_kwarg and isinstance(arg, (dict, frozendict)):
                            tp = self.input_types[k].parse_value(arg)
                        else:
                            tp = HgScalarTypeMetaData.parse_value(arg)
                        kwarg_types[k] = tp
                    elif arg.output_type:
                        kwarg_types[k] = arg.output_type
                    elif _ensure_match:
                        raise ParseError(
                            f"{k}: {v} = {arg}, argument supplied is not a valid source or compute_node output"
                        )
                elif type(v) is HgTypeOfTypeMetaData:
                    if not isinstance(arg, (type, GenericAlias, _GenericAlias, TypeVar)) and arg is not AUTO_RESOLVE:
                        # This is not a type of something (Have seen this as being an instance of HgTypeMetaData)
                        raise IncorrectTypeBinding(v, arg)
                    v = HgTypeMetaData.parse_type(arg) if arg is not AUTO_RESOLVE else v.value_tp
                    kwarg_types[k] = HgTypeOfTypeMetaData(v)
                else:
                    if arg is None:
                        kwarg_types[k] = v
                    else:
                        tp = v.parse_value(arg)  # try the annotation type first
                        if tp is None:
                            tp = HgScalarTypeMetaData.parse_value(arg)
                        kwarg_types[k] = tp
                        if tp is None:
                            if k in self.unresolved_args:
                                if _ensure_match:
                                    raise ParseError(
                                        f"In {self.name}, {k}: {v} = {arg}; arg is not parsable, "
                                        "but we require type resolution"
                                    )
                            else:
                                # If the signature was not unresolved, then we can use the signature, but the input value
                                # May yet be incorrectly typed.
                                kwarg_types[k] = v
        return kwarg_types

    def try_build_resolution_dict(self, pre_resolved_types: dict[TypeVar, HgTypeMetaData | Callable]):
        from hgraph._wiring._wiring_node_class import extract_kwargs

        pre_resolved_types = pre_resolved_types or {}
        kwargs = extract_kwargs(self, _ensure_match=False)
        kwarg_types = self.convert_kwargs_to_types(**kwargs, _ensure_match=False)
        return self.build_resolution_dict(pre_resolved_types, kwarg_types, kwargs, weak=True)

    def build_resolution_dict(
        self, pre_resolved_types: dict[TypeVar, HgTypeMetaData | Callable], kwarg_types, kwargs, weak=False
    ) -> dict[TypeVar, HgTypeMetaData]:
        """Expect kwargs to be a dict of arg to type mapping / value mapping"""
        resolution_dict: dict[TypeVar, HgTypeMetaData] = (
            {k: v for k, v in pre_resolved_types.items() if isinstance(v, HgTypeMetaData)} if pre_resolved_types else {}
        )
        resolvers_dict: dict[TypeVar, Callable] = (
            {k: v for k, v in pre_resolved_types.items() if isfunction(v)} if pre_resolved_types else {}
        )
        args = self.input_types.keys()
        delayed_resolution = []
        for _1 in range(len(args) + 1):  # Limit the number of goes to try and resolve everything
            for arg in args:
                meta_data = self.input_types[arg]

                # This will validate the input type against the signature's type so don't short-cut this logic!
                with WiringContext(current_arg=arg):
                    kwt = kwarg_types.get(arg)
                    kwarg = kwargs.get(arg)
                    if isinstance(kwarg, TypeVar) and (kwarg in self.type_vars or kwarg in resolution_dict):
                        if kwarg in resolution_dict:
                            kwt = HgTypeOfTypeMetaData(resolution_dict[kwarg])
                            kwarg_types[arg] = kwt
                        else:
                            delayed_resolution.append(arg)
                            continue
                    if kwt is not None and not meta_data.is_scalar and kwt.is_scalar:
                        meta_data: HgTimeSeriesTypeMetaData
                        meta_data.build_resolution_dict_from_scalar(resolution_dict, kwt, kwarg)
                    elif kwt is None and weak:
                        pass
                    else:
                        meta_data.build_resolution_dict(resolution_dict, kwt)
            # now ensures all "resolved" items are actually resolved
            out_dict = {}
            all_resolved = 0
            for k, v in resolution_dict.items():
                if v is not None:
                    v_resolved = v.resolve(resolution_dict, weak=True)
                    if v_resolved.is_resolved:
                        out_dict[k] = v_resolved
                        all_resolved += 1

            if delayed_resolution:
                args = delayed_resolution
                for a in args:
                    if a not in resolution_dict and (kwarg := kwargs.get(a)):
                        resolution_dict[kwarg] = HgTypeMetaData.parse_type(kwarg)
                delayed_resolution = []

            if resolvers_dict:
                scalars = {k: v for k, v in kwargs.items() if (kwt := kwarg_types.get(k)) and kwt.is_scalar}
                for k, v in resolvers_dict.items():
                    if k not in out_dict:
                        resolved = v(resolution_dict, scalars)
                        if isinstance(resolved, (type, GenericAlias, _GenericAlias, TypeVar)):
                            resolved = HgTypeMetaData.parse_type(resolved)
                        elif isinstance(resolved, Size):
                            resolved = HgTypeMetaData.parse_value(resolved)
                        resolution_dict[k] = resolved
                        out_dict[k] = resolved

            if not delayed_resolution and all_resolved == len(resolution_dict):
                break

        all_resolved = 0
        for k, v in resolution_dict.items():
            if v is not None:
                all_resolved += 1 if k in out_dict and out_dict[k].is_resolved else 0

        if not weak and all_resolved < len(resolution_dict):
            raise ParseError(
                "Unable to build a resolved resolution dictionary, due to:"
                f"{';'.join(f' {k}: {v}' for k, v in out_dict.items() if not v.is_resolved)}"
            )
        return out_dict

    def resolve_inputs(
        self, resolution_dict: dict[TypeVar, HgTypeMetaData], weak=False
    ) -> Mapping[str, HgTypeMetaData]:
        if self.is_resolved:
            return self.input_types

        resolution_dict: dict[TypeVar, HgTypeMetaData] = (
            {k: v for k, v in resolution_dict.items() if isinstance(v, HgTypeMetaData)} if resolution_dict else {}
        )

        input_types = {}
        for arg, meta_data in self.input_types.items():
            input_types[arg] = meta_data.resolve(resolution_dict, weak)
        return frozendict(input_types)

    def resolve_output(self, resolution_dict: dict[TypeVar, HgTypeMetaData], weak=False) -> Optional[HgTypeMetaData]:
        if self.output_type is None:
            return None

        resolution_dict: dict[TypeVar, HgTypeMetaData] = (
            {k: v for k, v in resolution_dict.items() if isinstance(v, HgTypeMetaData)} if resolution_dict else {}
        )

        out_type = self.output_type.resolve(resolution_dict, weak)
        if type(out_type) == HgTimeSeriesSchemaTypeMetaData:
            raise IncorrectTypeBinding(HgTSBTypeMetaData(out_type), out_type)
        return out_type

    def resolve_valid_inputs(self, resolution_dict, **kwargs) -> tuple[frozenset[str], bool]:
        valid_inputs = (
            self.valid_inputs(resolution_dict, kwargs) if isfunction(self.valid_inputs) else self.valid_inputs
        )
        optional_inputs = set(k for k in self.time_series_args if kwargs[k] is None)
        if optional_inputs:
            if valid_inputs is not None:
                return (r := frozenset(k for k in valid_inputs if k not in optional_inputs)), r != self.valid_inputs
            else:
                return frozenset(k for k in self.time_series_args if k not in optional_inputs), True
        else:
            return frozenset(valid_inputs) if valid_inputs is not None else None, valid_inputs != self.valid_inputs

    def resolve_all_valid_inputs(self, resolution_dict, **kwargs) -> tuple[frozenset[str] | None, bool]:
        all_valid_inputs = (
            self.all_valid_inputs(resolution_dict, kwargs)
            if isfunction(self.all_valid_inputs)
            else self.all_valid_inputs
        )
        optional_inputs = set(k for k in self.time_series_args if kwargs[k] is None)
        if optional_inputs:
            if all_valid_inputs:
                # Remove any optional inputs from validity requirements if not provided.
                return (
                    r := frozenset(k for k in all_valid_inputs if k not in optional_inputs)
                ), r != self.all_valid_inputs
            else:
                return None, all_valid_inputs != self.all_valid_inputs
        else:
            return (
                frozenset(all_valid_inputs) if all_valid_inputs is not None else None
            ), all_valid_inputs != self.all_valid_inputs

    def resolve_active_inputs(self, resolution_dict, **kwargs) -> tuple[frozenset[str], bool]:
        active_inputs = (
            self.active_inputs(resolution_dict, kwargs) if isfunction(self.active_inputs) else self.active_inputs
        )
        optional_inputs = set(k for k in self.time_series_args if kwargs[k] is None)
        if optional_inputs:
            if active_inputs:
                return (r := frozenset(k for k in active_inputs if k not in optional_inputs)), r != self.active_inputs
            else:
                return None, active_inputs != self.active_inputs
        else:
            return frozenset(active_inputs) if active_inputs is not None else None, active_inputs != self.active_inputs

    def resolve_auto_resolve_kwargs(self, resolution_dict, kwarg_types, kwargs, resolved_inputs):
        new_resolved_inputs = {}
        for arg, v in self.defaults.items():
            if v is AUTO_RESOLVE:
                if arg == "__resolution_dict__":
                    kwargs[arg] = resolution_dict
                else:
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
        from hgraph._wiring._wiring_port import WiringPort

        for arg, v in self.input_types.items():
            v = v.dereference()
            if not v.is_scalar:
                if kwarg_types[arg].is_scalar:
                    if not v.scalar_type().matches(kwarg_types[arg]):
                        raise IncorrectTypeBinding(v, kwarg_types[arg])
                    from hgraph import const

                    kwargs[arg] = const(kwargs[arg], tp=v.py_type)
                elif not isinstance(kwargs[arg], WiringPort) and isinstance(
                    kwargs[arg], (tuple, list, dict, frozendict)
                ):
                    kwargs[arg] = v.py_type.from_ts(kwargs[arg], __type__=v)
            if type(v) is HgTypeOfTypeMetaData:
                kwargs[arg] = cast(HgTypeOfTypeMetaData, v).value_tp.py_type

    def resolve_context_kwargs(
        self,
        kwargs,
        kwarg_types,
        resolved_inputs,
        valid_inputs,
        has_valid_overrides,
        all_valid_inputs,
        has_all_valid_overrides,
    ):
        for arg, tp in self.time_series_inputs.items():
            if tp.is_context_wired:
                from hgraph import REQUIRED

                if (v := kwargs.get(arg, None)) is None or v is REQUIRED or isinstance(v, (REQUIRED, str)):
                    from hgraph import TimeSeriesContextTracker
                    from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext

                    if c := TimeSeriesContextTracker.instance().get_context(
                        self.input_types[arg].value_tp,
                        WiringNodeInstanceContext.instance(),
                        name=str(v) if isinstance(v, (REQUIRED, str)) else None,
                    ):
                        kwargs[arg] = c
                        kwarg_types[arg] = c.output_type
                        if not tp.ts_type.matches(c.output_type.dereference()):
                            # the context must be a bundle so replace the context type and require it to be all-valid
                            resolved_inputs = resolved_inputs | {arg: c.output_type.dereference()}
                            all_valid_inputs = frozenset((all_valid_inputs or set()) | {arg})
                            has_all_valid_overrides = (
                                has_all_valid_overrides or all_valid_inputs is None or arg not in all_valid_inputs
                            )
                        else:
                            valid_inputs = frozenset((valid_inputs or set()) | {arg})
                            has_valid_overrides = has_valid_overrides or valid_inputs is None or arg not in valid_inputs
                    elif v is REQUIRED:
                        raise CustomMessageWiringError(
                            f"Context of type {tp} for argument '{arg}' is required, but not found"
                        )
                    elif type(v) is REQUIRED:
                        raise CustomMessageWiringError(
                            f"Context of type {tp} with name {v} for argument '{arg}' is required, but not found"
                        )
                    else:
                        from hgraph import nothing

                        kwargs[arg] = nothing[OUT : tp.ts_type]()
                        kwarg_types[arg] = tp.ts_type
                        valid_inputs = frozenset((valid_inputs or set(self.time_series_inputs.keys())) - {arg})
                        has_valid_overrides = has_valid_overrides or valid_inputs is None or arg not in valid_inputs

        return resolved_inputs, valid_inputs, has_valid_overrides, all_valid_inputs, has_all_valid_overrides

    def validate_requirements(self, resolution_dict: dict[TypeVar, HgTypeMetaData], kwargs):
        if self.requires:
            if (err := self.requires(resolution_dict, kwargs)) is not True:
                error = f": {err}," if err is not False else ""
                raise RequirementsNotMetWiringError(
                    f"Failed requirements check{error} with resolution dict {resolution_dict}"
                )

    def validate_resolved_types(self, kwarg_types, kwargs):
        with WiringContext(current_kwargs=kwargs):
            for k, v in self.input_types.items():
                from hgraph._wiring._wiring_port import WiringPort

                with WiringContext(current_arg=k):
                    if isinstance(kwargs[k], WiringPort):
                        if not v.dereference().matches(kwargs[k].output_type.dereference()):
                            raise IncorrectTypeBinding(v, kwarg_types[k])
                    else:
                        if not v.dereference().matches(kwarg_types[k].dereference()):
                            raise IncorrectTypeBinding(v, kwarg_types[k])


def extract_signature(
    fn,
    wiring_node_type: WiringNodeType,
    active_inputs: frozenset[str] | None = None,
    valid_inputs: frozenset[str] | None = None,
    all_valid_inputs: frozenset[str] | None = None,
    deprecated: bool = False,
    requires: Callable[[...], bool] | None = None,
    record_and_replay_id: str | None = None,
) -> WiringNodeSignature:
    """
    Performs signature extract that will work for python 3.9 (and possibly above)
    :param fn:
    :return:
    """
    name = fn.__name__
    annotations = get_type_hints(fn)
    code = fn.__code__
    parameters = signature(fn).parameters
    args: tuple[str, ...] = tuple(parameters.keys())
    kw_only_args: tuple[str, ...] = tuple(
        k for k, p in parameters.items() if p.kind in (Parameter.KEYWORD_ONLY, Parameter.VAR_KEYWORD)
    )
    defaults = {k: p.default for k, p in parameters.items() if p.default is not p.empty}
    var_arg = next((p.name for p in parameters.values() if p.kind == Parameter.VAR_POSITIONAL), None)
    var_kwarg = next((p.name for p in parameters.values() if p.kind == Parameter.VAR_KEYWORD), None)
    if var_arg or var_kwarg:
        if wiring_node_type == WiringNodeType.OPERATOR:
            # Remove var_args from operators - they are decorative
            args = tuple(a for a in args if a not in (var_arg, var_kwarg) or a in annotations)
            kw_only_args = tuple(a for a in kw_only_args if a not in (var_arg, var_kwarg) or a in annotations)
        else:  # var args and kwargs are always optional
            if var_arg:
                defaults[var_arg] = defaults.get(var_arg, None)
            if var_kwarg:
                defaults[var_kwarg] = defaults.get(var_kwarg, None)

    if default_type_arg_name := next((k for k, v in annotations.items() if isinstance(v, DEFAULT)), None):
        tp = annotations[default_type_arg_name].tp
        annotations[default_type_arg_name] = tp
        default_type_arg = tp
    elif default_type_arg_name := next((k for k, v in defaults.items() if isinstance(v, DEFAULT)), None):
        tp = defaults[default_type_arg_name].tp
        defaults[default_type_arg_name] = tp
        default_type_arg = tp
    else:
        default_type_arg = None

    filename = code.co_filename
    first_line = code.co_firstlineno
    # Once we start defaulting, all attributes must be defaulted, so we can count backward
    # to know where to apply the defaults.
    input_types: frozendict[str, HgTypeMetaData] = frozendict(
        (k, extract_hg_type(v) if (k != "_output" or defaults.get("_output", True) is not None) else HgOutputType(v))
        for k, v in annotations.items()
        if k != "return"
    )
    output_type = extract_hg_time_series_type(annotations.get("return", None))
    if output_type is not None and type(output_type) is HgTimeSeriesSchemaTypeMetaData:
        raise ParseError(f"The output type is not valid, did you mean TSB[{output_type.py_type.__name__}]")
    unresolved_inputs = frozenset(a for a in args if not input_types[a].is_resolved)
    time_series_inputs = frozenset(a for a in args if not input_types[a].is_scalar)
    context_inputs = frozenset(a for a in args if input_types[a].is_context_manager)

    # Validations to ensure the signature matches the node type
    if wiring_node_type in (WiringNodeType.PULL_SOURCE_NODE, WiringNodeType.PUSH_SOURCE_NODE):
        assert (
            len(time_series_inputs) == 0
        ), f"source node '{name}' has time-series inputs ({time_series_inputs}) (not a valid signature)"
        assert output_type is not None, f"source node '{name}' has no output (not a valid signature)"
    elif wiring_node_type is WiringNodeType.COMPUTE_NODE:
        assert len(time_series_inputs) > 0, f"compute node '{name}' has no time-series inputs (not a valid signature)"
        assert output_type is not None, f"compute node '{name}' has no output (not a valid signature)"
    elif wiring_node_type is WiringNodeType.SINK_NODE:
        assert len(time_series_inputs) > 0, f"sink node '{name}' has no time-series inputs (not a valid signature)"
        assert output_type is None, f"sink node '{name}' has an output (not a valid signature)"

    if active_inputs is not None and not isfunction(active_inputs):
        assert all(
            a in input_types for a in active_inputs
        ), f"active inputs {active_inputs} are not in the signature for {name}"
    if valid_inputs is not None and not isfunction(valid_inputs):
        assert all(
            a in input_types for a in valid_inputs
        ), f"valid inputs {valid_inputs} are not in the signature for {name}"
    if all_valid_inputs is not None and not isfunction(all_valid_inputs):
        assert all(
            a in input_types for a in all_valid_inputs
        ), f"all_valid inputs {all_valid_inputs} are not in signature for {name}"
        if valid_inputs is not None and not isfunction(valid_inputs):
            assert (
                len(set(all_valid_inputs).intersection(valid_inputs)) == 0
            ), f"valid and all_valid inputs are overlapping, {all_valid_inputs}, {valid_inputs} for {name}"

    # Note graph signatures can be any of the above, so additional validation would need to be performed in the
    # graph expansion logic.

    injectable_inputs = extract_injectable_inputs(**input_types)
    if record_and_replay_id is None:
        scalars = [f"{{{k}}}" for k in args if k not in time_series_inputs]
        record_and_replay_id = f"{name}{'::' if scalars else ''}{'_'.join(scalars)}"

    return WiringNodeSignature(
        node_type=wiring_node_type,
        name=name,
        args=args,
        defaults=frozendict(defaults),
        input_types=input_types,
        output_type=output_type,
        active_inputs=active_inputs,
        valid_inputs=valid_inputs,
        all_valid_inputs=all_valid_inputs,
        context_inputs=context_inputs,
        src_location=SourceCodeDetails(file=filename, start_line=first_line),
        unresolved_args=unresolved_inputs,
        time_series_args=time_series_inputs,
        injectable_inputs=injectable_inputs,
        label=None,
        record_and_replay_id=record_and_replay_id,
        deprecated=deprecated,
        requires=requires,
        kw_only_args=kw_only_args,
        var_arg=var_arg,
        var_kwarg=var_kwarg,
        default_type_arg=default_type_arg,
    )


def extract_injectable_inputs(**kwargs) -> InjectableTypes:
    return reduce(
        or_,
        (
            {
                HgSchedulerType: InjectableTypes.SCHEDULER,
                HgEvaluationClockType: InjectableTypes.CLOCK,
                HgEvaluationEngineApiType: InjectableTypes.ENGINE_API,
                HgStateType: InjectableTypes.STATE,
                HgRecordableStateType: InjectableTypes.RECORDABLE_STATE,
                HgOutputType: InjectableTypes.OUTPUT,
                HgLoggerType: InjectableTypes.LOGGER,
                HgNodeType: InjectableTypes.NODE,
            }.get(type(v), InjectableTypes(0))
            for v in kwargs.values()
        ),
        InjectableTypes(0),
    )
