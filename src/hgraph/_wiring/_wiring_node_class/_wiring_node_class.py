import inspect
from copy import copy
from dataclasses import replace
from types import GenericAlias
from typing import Callable, Any, TypeVar, _GenericAlias, Mapping, TYPE_CHECKING, Tuple

from frozendict import frozendict

from hgraph._types._scalar_type_meta_data import RecordableStateInjector
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._tsb_meta_data import HgTSBTypeMetaData, HgTimeSeriesSchemaTypeMetaData
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import WiringError, MissingInputsError, WiringFailureError, CustomMessageWiringError
from hgraph._wiring._wiring_node_instance import (
    WiringNodeInstance,
    create_wiring_node_instance,
)
from hgraph._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType
from hgraph._wiring._wiring_port import _wiring_port_for, WiringPort

if TYPE_CHECKING:
    from hgraph._builder._node_builder import NodeBuilder
    from hgraph._builder._input_builder import InputBuilder
    from hgraph._builder._output_builder import OutputBuilder
    from hgraph._runtime._node import NodeSignature

__all__ = (
    "BaseWiringNodeClass",
    "PreResolvedWiringNodeWrapper",
    "WiringNodeClass",
    "create_input_output_builders",
    "create_wiring_node_instance",
    "extract_kwargs",
    "extract_resolution_dict",
    "prepare_kwargs",
    "validate_and_resolve_signature",
)


class WiringNodeClass:
    """
    The wiring node template, this has the signature and information required to construct a run-time node.
    The template is instantiable to form a WiringNodeInstance, the instance can be used to build a wiring graph.
    """

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        self.signature: WiringNodeSignature = signature
        self.fn: Callable = fn
        self.allow_overloads = False

    def __call__(self, *args, **kwargs) -> "WiringNodeInstance":
        raise NotImplementedError()

    def _convert_item(self, item) -> dict[TypeVar, HgTypeMetaData | Callable]:
        if isinstance(item, dict):
            item = tuple(slice(k, v) for k, v in item.items())
        elif isinstance(item, slice):
            item = (item,)  # Normalise all items into a tuple
        elif isinstance(item, (type, GenericAlias, _GenericAlias, HgTypeMetaData)):
            if len(tpv := self.signature.type_vars) == 1:
                item = (slice(tuple(tpv)[0], item),)
            elif self.signature.default_type_arg:
                item = (slice(self.signature.default_type_arg, item),)
            else:
                raise WiringError(f"Can not figure out which type parameter to assign {item} to.")

        out = {}
        for s in item:
            assert s.step is None, f"Signature of type resolution is incorrect, expect TypeVar: Type, ... got {s}"
            assert s.start is not None, f"Signature of type resolution is incorrect, expect TypeVar: Type, ... got {s}"
            assert s.stop is not None, "signature of type resolution is incorrect, None is not a valid type"
            assert isinstance(
                s.start, TypeVar
            ), f"Signature of type resolution is incorrect first item must be of type TypeVar, got {s.start}"
            if isinstance(s.stop, (type, _GenericAlias, HgTypeMetaData)):
                parsed = HgTypeMetaData.parse_type(s.stop)
                out[s.start] = parsed
                assert parsed is not None, f"Can not resolve {s.stop} into a valid scalar or time-series type"
                assert parsed.is_resolved, f"The resolved value {s.stop} is not resolved, this is not supported."
            elif inspect.isfunction(s.stop):
                out[s.start] = s.stop
        return out

    def __getitem__(self, item) -> "WiringNodeClass":
        """
        Expected use:
        ```python
        my_node[SCALAR: int, TIME_SERIES_TYPE: TS[int]](...)
        ```
        """
        raise NotImplementedError()

    def __eq__(self, other):
        return self.signature == other.signature and self.fn == other.fn

    def __hash__(self):
        return hash(self.signature) ^ hash(self.fn)

    def resolve_signature(
        self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **kwargs
    ) -> "WiringNodeSignature":
        """Resolve the signature of this node based on the inputs"""
        raise NotImplementedError()

    def create_node_builder_instance(
        self,
        resolved_wiring_signature: "WiringNodeSignature",
        node_signature: "NodeSignature",
        scalars: Mapping[str, Any],
    ) -> "NodeBuilder":
        """Create the appropriate node builder for the node this wiring node represents
        :param resolved_wiring_signature:
        :param node_ndx:
        :param node_signature:
        :param scalars:
        """
        raise NotImplementedError()

    @property
    def error_output_type(self) -> "HgTimeSeriesTypeMetaData":
        from hgraph import NodeError
        from hgraph import TS

        return HgTimeSeriesTypeMetaData.parse_type(TS[NodeError])

    @property
    def state_output_type(self) -> "HgTSBTypeMetaData":
        if self.signature.uses_recordable_state:
            state_tp: RecordableStateInjector = self.signature.input_types[self.signature.recordable_state_arg]
            return state_tp.tsb_type
        else:
            raise CustomMessageWiringError(f"This node does not make use of recordable state")


def extract_kwargs(
    signature: WiringNodeSignature,
    *args,
    _ignore_defaults: bool = False,
    _ensure_match: bool = True,
    _args_offset: int = 0,
    **kwargs,
) -> dict[str, Any]:
    """
    Converts args to kwargs based on the signature.
    If _ignore_defaults is True, then the defaults are not applied.
    If _ensure_match is True, then the final kwargs must match the signature exactly.
    _allow_missing_count is the number of missing arguments that are allowed.
    """
    kwargs = copy(kwargs)
    kwargs_ = {}
    args_offset = _args_offset
    args_used = 0
    pos_args = len(signature.args) - (len(signature.kw_only_args) if signature.kw_only_args else 0)
    for i, (k, arg) in enumerate(zip(signature.args[_args_offset:pos_args], args)):
        args_offset += 1
        if k == signature.var_arg:
            kwargs_[k] = args[i:]
            args_used = len(args)
            break
        else:
            kwargs_[k] = arg
            args_used += 1

    if signature.var_arg:
        j = 0
        while f"{signature.var_arg}-{j}" in kwargs:
            kwargs_[signature.var_arg] = kwargs_.get(signature.var_arg, []) + [kwargs.pop(f"{signature.var_arg}-{j}")]
            j += 1

    if args_used < len(args):
        raise SyntaxError(
            f"[{signature.signature}] Too many positional arguments provided, expected {signature.args}, got {args}"
        )

    if any(k in kwargs for k in kwargs_):
        raise SyntaxError(
            f"[{signature.signature}] The following keys are duplicated: {[k for k in kwargs_ if k in kwargs]}"
        )

    for k in signature.args[args_offset:]:
        if kwargs and k in kwargs:
            kwargs_[k] = kwargs.pop(k)
        if k == signature.var_kwarg and k not in kwargs_:
            kwargs_[k] = kwargs
            kwargs = {}
            break

    if kwargs:
        raise SyntaxError(
            f"[{signature.signature}] Has unexpected kwarg names {tuple(kwargs.keys())}, expected: {signature.args}"
        )

    if not _ignore_defaults:
        kwargs_ |= {k: v for k, v in signature.defaults.items() if k not in kwargs_}  # Add in defaults
    else:
        # Ensure we have all blanks filled in to make validations work
        kwargs_ |= {
            k: v if k in signature.defaults else None for k, v in signature.defaults.items() if k not in kwargs_
        }

    if _ensure_match and any(arg not in kwargs_ for arg in signature.args[_args_offset:]):
        raise SyntaxError(
            f"[{signature.signature}] Has incorrect kwargs names "
            f"{[arg for arg in kwargs_ if arg not in signature.args]} "
            f"expected: {[arg for arg in signature.args if arg not in kwargs_]}"
        )

    if _ensure_match and len(kwargs_) < len(signature.args) - _args_offset:
        raise MissingInputsError(kwargs_)

    return kwargs_


def prepare_kwargs(signature: WiringNodeSignature, *args, _ignore_defaults: bool = False, **kwargs) -> dict[str, Any]:
    """
    Extract the args and kwargs, apply defaults and validate the input shape as correct.
    This does not validate the types, just that all args are provided.
    """
    if len(args) + len(kwargs) > len(signature.args) and not signature.var_arg and not signature.var_kwarg:
        raise SyntaxError(
            f"[{signature.signature}] More arguments are provided than are defined for this function -"
            f" {len(args)} positional and {kwargs.keys()}"
        )
    kwargs_ = extract_kwargs(signature, *args, _ignore_defaults=_ignore_defaults, **kwargs)
    return kwargs_


class BaseWiringNodeClass(WiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        super().__init__(signature, fn)
        self.start_fn: Callable = None
        self.stop_fn: Callable = None

    def __repr__(self):
        return self.signature.signature

    def overload(self, other: "WiringNodeClass"):
        if getattr(self, "overload_list", None) is None:
            from hgraph._wiring._wiring_node_class._operator_wiring_node import OverloadedWiringNodeHelper

            self.overload_list = OverloadedWiringNodeHelper(self)

        self.overload_list.overload(other)

    def resolve_with(self, resolved_types: dict[TypeVar, HgTypeMetaData]):
        return PreResolvedWiringNodeWrapper(
            signature=self.signature, fn=self.fn, underlying_node=self, resolved_types=resolved_types
        )

    def __getitem__(self, item) -> WiringNodeClass:
        if item:
            return self.resolve_with(self._convert_item(item))
        else:
            return self

    def _prepare_kwargs(self, *args, **kwargs) -> dict[str, Any]:
        """
        Extract the args and kwargs, apply defaults and validate the input shape as correct.
        This does not validate the types, just that all args are provided.
        """
        kwargs_ = prepare_kwargs(self.signature, *args, **kwargs)
        return kwargs_

    def resolve_signature(
        self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **kwargs
    ) -> "WiringNodeSignature":
        _, resolved_signature, _ = validate_and_resolve_signature(
            self.signature, *args, __pre_resolved_types__=__pre_resolved_types__, **kwargs
        )
        return resolved_signature

    def _check_overloads(self, *args, **kwargs) -> Tuple[bool, "WiringPort"]:
        if not getattr(self, "skip_overload_check", None):
            if (overload_helper := getattr(self, "overload_list", None)) is not None:
                from hgraph._wiring._wiring_node_class._operator_wiring_node import OverloadedWiringNodeHelper

                overload_helper: OverloadedWiringNodeHelper
                best_overload = overload_helper.get_best_overload(*args, **kwargs)
                best_overload: WiringNodeClass
                if best_overload is not self:
                    return True, best_overload(*args, **kwargs)

        return False, None

    def __call__(
        self,
        *args,
        __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None,
        __return_sink_wp__: bool = False,
        __recordable_id__: str = None,
        **kwargs,
    ) -> "WiringPort":
        """
        Supports most use-cases of extracting the call args and preparing the appropriate wiring port.
        :param args: Args as defined by the wiring signature.
        :param __pre_resolved_types__: A dictionary of TypeVars that have been pre-resolved.
        :param __return_sink_wp__:  If True will return a stub wiring port when the node is sink node.
        :param __recordable_id__: The id (or partial id) to use when recording this element.
        :param kwargs: The kwargs to supply to the function
        :return: A WiringPort (or None in the case of a sink_node and not stub requested)
        """

        # TODO: Capture the call site information (line number / file etc.) for better error reporting.
        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            # Now validate types and resolve any un-resolved types and provide an updated signature.
            kwargs_, resolved_signature, _ = validate_and_resolve_signature(
                self.signature,
                *args,
                __pre_resolved_types__=__pre_resolved_types__,
                __recordable_id__=__recordable_id__,
                **kwargs,
            )

            if self.signature.deprecated:
                import warnings

                warnings.warn(
                    f"{self.signature.signature} is deprecated and will be removed in a future version."
                    f"{(' ' + self.signature.deprecated) if type(self.signature.deprecated) is str else ''}",
                    DeprecationWarning,
                    stacklevel=3,
                )
            wiring_node_instance = create_wiring_node_instance(self, resolved_signature, frozendict(kwargs_))
            # Select the correct wiring port for the TS type! That we can provide useful wiring syntax
            # to support this like out.p1 on a bundle or out.s1 on a ComplexScalar, etc.

            if resolved_signature.node_type is WiringNodeType.SINK_NODE:
                from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext

                WiringGraphContext.instance().add_sink_node(wiring_node_instance)

                if __return_sink_wp__:
                    return _wiring_port_for(None, wiring_node_instance, tuple())
            else:
                # Whilst a graph could represent a sink signature, it is not a node, we return the wiring port
                # as it is used by the GraphWiringNodeClass to validate the resolved signature with that of the returned
                # output
                port = _wiring_port_for(resolved_signature.output_type, wiring_node_instance, tuple())
                from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext

                WiringGraphContext.instance().add_node(port)

                if resolved_signature.uses_recordable_state:
                    from hgraph._operators._record_replay import RecordReplayContext, RecordReplayEnum, record
                    mode = RecordReplayContext.instance().mode
                    if RecordReplayEnum.RECORD in mode:
                        record(port.__state__, "__state__", recordable_id=resolved_signature.record_and_replay_id)

                return port

    def _validate_signature(self, fn):
        sig = inspect.signature(fn)
        args = self.signature.args
        if not all(arg in args for arg in sig.parameters.keys()):
            raise WiringError(f"{fn.__name__} has arguments that are not named in main signature {self.signature.name}")

    def start(self, fn: Callable):
        """Decorator to indicate the start function for a node"""
        self._validate_signature(fn)
        self.start_fn = fn
        return self

    def stop(self, fn: Callable):
        """Decorator to indicate the stop function for a node"""
        self._validate_signature(fn)
        self.stop_fn = fn
        return self


def extract_resolution_dict(
    signature: WiringNodeSignature, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable], **kwargs
):
    """
    Performs the logic of extracting the resolution dictionary as is used by the dispatch operator to deal
    with ensuring overloads are correctly captured upfront.
    """
    _ = kwargs.pop("__recordable_id__", None)  # Remove if preset
    kwargs = prepare_kwargs(signature, *args, **kwargs)
    # Extract any additional required type resolution information from inputs
    kwarg_types = signature.convert_kwargs_to_types(**kwargs)
    return signature.build_resolution_dict(__pre_resolved_types__, kwarg_types, kwargs)


def validate_and_resolve_signature(
    signature: WiringNodeSignature,
    *args,
    __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable],
    __enforce_output_type__: bool = True,
    __recordable_id__: str = None,
    **kwargs,
) -> tuple[dict[str, Any], WiringNodeSignature, dict[TypeVar, HgTypeMetaData]]:
    """
    Insure the inputs wired in match the signature of this node and resolve any missing types.
    """
    # Validate that all inputs have been received and apply the defaults.
    record_replay_id = __recordable_id__
    kwargs = prepare_kwargs(signature, *args, **kwargs)
    WiringContext.current_kwargs = kwargs
    try:
        from hgraph._wiring._markers import _PassivateMarker
        passive_keys = set(k for k, v in kwargs.items() if isinstance(v, _PassivateMarker))
        if passive_keys:
            # Unpack passive keys
            kwargs = {k: v.value if k in passive_keys else v for k, v in kwargs.items()}
        # Extract any additional required type resolution information from inputs
        kwarg_types = signature.convert_kwargs_to_types(**kwargs)
        # Do the resolve to ensure types match as well as actually resolve the types.
        resolution_dict = signature.build_resolution_dict(__pre_resolved_types__, kwarg_types, kwargs)
        resolved_inputs = signature.resolve_inputs(resolution_dict)
        resolved_output = signature.resolve_output(resolution_dict, weak=not __enforce_output_type__)
        valid_inputs, has_valid_overrides = signature.resolve_valid_inputs(resolution_dict, **kwargs)
        all_valid_inputs, has_all_valid_overrides = signature.resolve_all_valid_inputs(resolution_dict, **kwargs)
        active_inputs, has_active_overrides = signature.resolve_active_inputs(resolution_dict, **kwargs)
        if passive_keys:
            active_inputs = _adjust_active_inputs(signature, active_inputs, passive_keys)
        resolved_inputs, valid_inputs, has_valid_overrides, all_valid_inputs, has_all_valid_overrides = (
            signature.resolve_context_kwargs(
                kwargs,
                kwarg_types,
                resolved_inputs,
                valid_inputs,
                has_valid_overrides,
                all_valid_inputs,
                has_all_valid_overrides,
            )
        )
        resolved_inputs = signature.resolve_auto_resolve_kwargs(resolution_dict, kwarg_types, kwargs, resolved_inputs)

        if (
            signature.is_resolved
            and not has_valid_overrides
            and not has_all_valid_overrides
            and not has_active_overrides
        ):
            signature.resolve_auto_const_and_type_kwargs(kwarg_types, kwargs)
            signature.validate_resolved_types(kwarg_types, kwargs)
            signature.validate_requirements(resolution_dict, kwargs)
            if passive_keys:
                signature = signature.copy_with(active_inputs=active_inputs)
            return (
                kwargs,
                signature if record_replay_id is None else signature.copy_with(record_and_replay_id=record_replay_id),
                resolution_dict,
            )
        else:
            # Only need to re-create if we actually resolved the signature.
            resolve_signature = signature.copy_with(
                input_types=resolved_inputs,
                output_type=resolved_output,
                active_inputs=active_inputs,
                valid_inputs=valid_inputs,
                all_valid_inputs=all_valid_inputs,
                unresolved_args=frozenset(),
                record_and_replay_id=record_replay_id,
            )
            if resolve_signature.is_resolved and __enforce_output_type__ or resolve_signature.is_weakly_resolved:
                resolve_signature.resolve_auto_const_and_type_kwargs(kwarg_types, kwargs)
                signature.validate_resolved_types(kwarg_types, kwargs)
                signature.validate_requirements(resolution_dict, kwargs)
                return kwargs, resolve_signature, resolution_dict
            else:
                raise WiringError(f"{resolve_signature.name} was not able to resolve itself")
    except Exception as e:
        if isinstance(e, WiringError):
            raise e
        from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext

        path = "\n".join(str(p) for p in WiringGraphContext.wiring_path())
        raise WiringFailureError(
            f"Failure resolving signature for {signature.signature} due to: {str(e)}, graph call stack:\n{path}"
        ) from e


def _adjust_active_inputs(signature: WiringNodeSignature, active_inputs: tuple[str, ...], passive_inputs: set[str]) -> WiringNodeSignature:
    if active_inputs is None:
        active_inputs = tuple(signature.time_series_inputs.keys())
    active_inputs = tuple(k for k in active_inputs if k not in passive_inputs)
    if not active_inputs:
        raise CustomMessageWiringError("There are not active inputs remaining")
    return active_inputs


def create_input_output_builders(
    node_signature: "NodeSignature", error_type: "HgTimeSeriesTypeMetaData"
) -> tuple["InputBuilder", "OutputBuilder", "OutputBuilder"]:
    from hgraph import TimeSeriesBuilderFactory

    factory: TimeSeriesBuilderFactory = TimeSeriesBuilderFactory.instance()
    output_type = node_signature.time_series_output
    if ts_inputs := node_signature.time_series_inputs:
        from hgraph._types._tsb_type import UnNamedTimeSeriesSchema

        un_named_bundle = HgTSBTypeMetaData(
            HgTimeSeriesSchemaTypeMetaData(
                UnNamedTimeSeriesSchema.create_resolved_schema(
                    {k: ts_inputs[k] for k in filter(lambda k_: k_ in ts_inputs, node_signature.args)}
                )
            )
        )
        input_builder = factory.make_input_builder(un_named_bundle)
    else:
        input_builder = None
    output_builder = None if output_type is None else factory.make_output_builder(output_type)
    error_builder = factory.make_error_builder(error_type) if node_signature.capture_exception else None
    return input_builder, output_builder, error_builder


class PreResolvedWiringNodeWrapper(BaseWiringNodeClass):
    """Wraps a WiringNodeClass_ instance with the associated resolution dictionary"""

    underlying_node: BaseWiringNodeClass
    resolved_types: dict[TypeVar, HgTypeMetaData]

    def __init__(
        self,
        signature: WiringNodeSignature,
        fn: Callable,
        underlying_node: BaseWiringNodeClass,
        resolved_types: dict[TypeVar, HgTypeMetaData],
    ):
        super().__init__(
            replace(
                signature,
                input_types=signature.resolve_inputs(resolved_types, True),
                output_type=signature.resolve_output(resolved_types, True),
            ),
            fn,
        )
        if isinstance(underlying_node, PreResolvedWiringNodeWrapper):
            # We don't want to create unnecessary chains so unwrap and create the super set result.
            underlying_node = underlying_node.underlying_node
            resolved_types = underlying_node.resolved_types | resolved_types
        self.underlying_node = underlying_node
        self.resolved_types = resolved_types
        self.skip_overload_check = getattr(underlying_node, "skip_overload_check", False)

    def resolve_signature(self, *args, __pre_resolved_types__=None, **kwargs) -> "WiringNodeSignature":
        more_resolved_types = __pre_resolved_types__ or {}
        return self.underlying_node.resolve_signature(
            *args, __pre_resolved_types__={**self.resolved_types, **more_resolved_types}, **kwargs
        )

    def resolve_with(self, resolved_types: dict[TypeVar, HgTypeMetaData]) -> "PreResolvedWiringNodeWrapper":
        further_resolved = PreResolvedWiringNodeWrapper(
            signature=self.underlying_node.signature,
            fn=self.fn,
            underlying_node=self.underlying_node,
            resolved_types={**self.resolved_types, **resolved_types},
        )
        return further_resolved

    def __call__(self, *args, **kwargs) -> "WiringPort":
        more_resolved_types = kwargs.pop("__pre_resolved_types__", None) or {}
        pre_resolved_types = {**self.resolved_types, **more_resolved_types}

        return self.underlying_node(*args, __pre_resolved_types__=pre_resolved_types, **kwargs)

    def __getitem__(self, item):
        if item:
            further_resolved = self.resolve_with(self._convert_item(item))
            if (overload_helper := getattr(self, "overload_list", None)) is not None:
                for o, r in overload_helper.overloads:
                    if o is not self:
                        further_resolved.overload(o[item])
            return further_resolved
        else:
            return self

    def __getattr__(self, item):
        if (fn := getattr(self.underlying_node, item)) is not None and inspect.ismethod(fn):
            if "__pre_resolved_types__" in inspect.signature(fn).parameters:
                return lambda *args, **kwargs: fn(*args, **kwargs, __pre_resolved_types__=self.resolved_types)
            else:
                return lambda *args, **kwargs: fn(*args, **kwargs)

        raise AttributeError(f"Attribute {item} not found on {self.underlying_node}")

    def __repr__(self):
        sig = self.underlying_node.signature
        args = (f"{arg}: {str(sig.input_types[arg])}" for arg in sig.args)
        return_ = "" if sig.output_type is None else f" -> {str(sig.output_type)}"
        type_params = ",".join(
            f'{k}:{v if not inspect.isfunction(v) else "{}"}' for k, v in self.resolved_types.items()
        )
        return f"{sig.name}[{type_params}]({', '.join(args)}){return_}"

    def start(self, fn: Callable):
        self.underlying_node.start(fn)

    def stop(self, fn: Callable):
        self.underlying_node.stop(fn)

    @property
    def overload_list(self):
        return getattr(self.underlying_node, "overload_list", None)
