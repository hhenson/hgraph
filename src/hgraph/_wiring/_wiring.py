import inspect
from dataclasses import dataclass, replace
from functools import cached_property
from types import GenericAlias
from typing import Callable, Any, TypeVar, _GenericAlias, Optional, Mapping, TYPE_CHECKING, Generic, Tuple, List, \
    MutableMapping

from frozendict import frozendict
from more_itertools import nth

from hgraph._types._scalar_type_meta_data import HgTypeOfTypeMetaData, HgScalarTypeMetaData
from hgraph._types._scalar_types import SCALAR
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._tsb_meta_data import HgTSBTypeMetaData, HgTimeSeriesSchemaTypeMetaData
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
from hgraph._types._tsd_type import KEY_SET_ID
from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
from hgraph._types._tss_type import TSS
from hgraph._types._type_meta_data import HgTypeMetaData, ParseError, AUTO_RESOLVE
from hgraph._wiring._source_code_details import SourceCodeDetails
from hgraph._wiring._wiring_context import WiringContext, WIRING_CONTEXT
from hgraph._wiring._wiring_errors import WiringError, IncorrectTypeBinding, MissingInputsError, \
    CustomMessageWiringError
from hgraph._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType

if TYPE_CHECKING:
    from hgraph._types._tsb_type import TimeSeriesSchema
    from hgraph._builder._node_builder import NodeBuilder
    from hgraph._builder._input_builder import InputBuilder
    from hgraph._builder._output_builder import OutputBuilder
    from hgraph._runtime._node import NodeSignature, NodeTypeEnum
    from hgraph._builder._graph_builder import Edge

__all__ = ("WiringNodeClass", "BaseWiringNodeClass", "PreResolvedWiringNodeWrapper",
           "CppWiringNodeClass", "PythonGeneratorWiringNodeClass", "PythonWiringNodeClass", "WiringGraphContext",
           "GraphWiringNodeClass", "WiringNodeInstance", "WiringPort", "prepare_kwargs",)


class WiringNodeClass:
    """
    The wiring node template, this has the signature and information required to construct a run-time node.
    The template is instantiable to form a WiringNodeInstance, the instance can be used to build a wiring graph.
    """

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        self.signature: WiringNodeSignature = signature
        self.fn: Callable = fn

    def __call__(self, *args, **kwargs) -> "WiringNodeInstance":
        raise NotImplementedError()

    def _convert_item(self, item) -> dict[TypeVar, HgTypeMetaData]:
        if isinstance(item, slice):
            item = (item,)  # Normalise all items into a tuple
        out = {}
        for s in item:
            assert s.step is None, f"Signature of type resolution is incorrect, expect TypeVar: Type, ... got {s}"
            assert s.start is not None, f"Signature of type resolution is incorrect, expect TypeVar: Type, ... got {s}"
            assert s.stop is not None, "signature of type resolution is incorrect, None is not a valid type"
            assert isinstance(s.start,
                              TypeVar), f"Signature of type resolution is incorrect first item must be of type TypeVar, got {s.start}"
            parsed = HgTypeMetaData.parse(s.stop)
            out[s.start] = parsed
            assert parsed is not None, f"Can not resolve {s.stop} into a valid scalar or time-series type"
            assert parsed.is_resolved, f"The resolved value {s.stop} is not resolved, this is not supported."
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
        return hash(self.signature)

    def resolve_signature(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None,
                          **kwargs) -> "WiringNodeSignature":
        """Resolve the signature of this node based on the inputs"""
        raise NotImplementedError()

    def create_node_builder_instance(self, node_signature: "NodeSignature", scalars: Mapping[str, Any]) \
            -> "NodeBuilder":
        """Create the appropriate node builder for the node this wiring node represents
        :param node_ndx:
        :param node_signature:
        :param scalars:
        """
        raise NotImplementedError()


def extract_kwargs(signature: WiringNodeSignature, *args,
                   _ignore_defaults: bool = False,
                   _ensure_match: bool = True,
                   _args_offset: int = 0,
                   **kwargs) -> dict[str, Any]:
    """
    Converts args to kwargs based on the signature.
    If _ignore_defaults is True, then the defaults are not applied.
    If _ensure_match is True, then the final kwargs must match the signature exactly.
    _allow_missing_count is the number of missing arguments that are allowed.
    """
    kwargs_ = {k: arg for k, arg in zip(signature.args[_args_offset:], args)}  # Map the *args to keys
    if any(k in kwargs for k in kwargs_):
        raise SyntaxError(
            f"[{signature.signature}] The following keys are duplicated: {[k for k in kwargs_ if k in kwargs]}")
    kwargs_ |= kwargs  # Merge in the current kwargs
    if not _ignore_defaults:
        kwargs_ |= {k: v for k, v in signature.defaults.items() if k not in kwargs_}  # Add in defaults
    else:
        # Ensure we have all blanks filled in to make validations work
        kwargs_ |= {k: v if k in signature.defaults else None for k, v in signature.defaults.items() if
                    k not in kwargs_}
    if _ensure_match and any(arg not in kwargs_ for arg in signature.args):
        raise SyntaxError(f"[{signature.signature}] Has incorrect kwargs names "
                          f"{[arg for arg in kwargs_ if arg not in signature.args]} "
                          f"expected: {[arg for arg in signature.args if arg not in kwargs_]}")
    # Filter kwargs to ensure only valid keys are present, this will also align the order of kwargs with args.
    kwargs_ = {k: kwargs_[k] for k in signature.args if k in kwargs_}
    if len(kwargs_) < len(signature.args) - _args_offset:
        raise MissingInputsError(kwargs_)
    return kwargs_


def prepare_kwargs(signature: WiringNodeSignature, *args, _ignore_defaults: bool = False, **kwargs) -> dict[str, Any]:
    """
    Extract the args and kwargs, apply defaults and validate the input shape as correct.
    This does not validate the types, just that all args are provided.
    """
    if len(args) + len(kwargs) > len(signature.args):
        raise SyntaxError(
            f"[{signature.signature}] More arguments are provided than are defined for this function")
    kwargs_ = extract_kwargs(signature, *args, _ignore_defaults=_ignore_defaults, **kwargs)
    return kwargs_


class BaseWiringNodeClass(WiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        super().__init__(signature, fn)
        self.start_fn: Callable = None
        self.stop_fn: Callable = None

    def overload(self, other: "WiringNodeClass"):
        if getattr(self, "overload_list", None) is None:
            self.overload_list = OverloadedWiringNodeHelper(self)

        self.overload_list.overload(other)

    def __getitem__(self, item) -> WiringNodeClass:
        return PreResolvedWiringNodeWrapper(signature=self.signature, fn=self.fn,
                                            underlying_node=self, resolved_types=self._convert_item(item))

    def _prepare_kwargs(self, *args, **kwargs) -> dict[str, Any]:
        """
        Extract the args and kwargs, apply defaults and validate the input shape as correct.
        This does not validate the types, just that all args are provided.
        """
        kwargs_ = prepare_kwargs(self.signature, *args, **kwargs)
        return kwargs_

    def _convert_kwargs_to_types(self, **kwargs) -> dict[str, HgTypeMetaData]:
        """Attempt to convert input types to better support type resolution"""
        # We only need to extract un-resolved values
        kwarg_types = {}
        for k, v in self.signature.input_types.items():
            with WiringContext(current_arg=k):
                arg = kwargs.get(k, self.signature.defaults.get(k))
                if arg is None:
                    if v.is_injectable:
                        # For injectables we expect the value to be None, and the type must already be resolved.
                        kwarg_types[k] = v
                    else:
                        # We should wire in a null source
                        if k in self.signature.defaults:
                            kwarg_types[k] = v
                        else:
                            raise CustomMessageWiringError(
                                f"Argument '{k}' is not marked as optional, but no value was supplied")
                if k in filter(lambda k_: k_ in self.signature.time_series_args, self.signature.args):
                    # This should then get a wiring node, and we would like to extract the output type,
                    # But this is optional, so we should ensure that the type is present
                    if arg is None:
                        continue  # We will wire in a null source later
                    if not isinstance(arg, WiringPort):
                        tp = HgScalarTypeMetaData.parse(arg)
                        kwarg_types[k] = tp
                    elif arg.output_type:
                        kwarg_types[k] = arg.output_type
                    else:
                        raise ParseError(
                            f'{k}: {v} = {arg}, argument supplied is not a valid source or compute_node output')
                elif type(v) is HgTypeOfTypeMetaData:
                    if not isinstance(arg, (type, GenericAlias, _GenericAlias, TypeVar)) and arg is not AUTO_RESOLVE:
                        # This is not a type of something (Have seen this as being an instance of HgTypeMetaData)
                        raise IncorrectTypeBinding(v, arg)
                    v = HgTypeMetaData.parse(arg) if arg is not AUTO_RESOLVE else v.value_tp
                    kwarg_types[k] = HgTypeOfTypeMetaData(v)
                else:
                    tp = HgScalarTypeMetaData.parse(arg)
                    kwarg_types[k] = tp
                    if tp is None:
                        if k in self.signature.unresolved_args:
                            raise ParseError(f"In {self.signature.name}, {k}: {v} = {arg}; arg is not parsable, "
                                             f"but we require type resolution")
                        else:
                            # If the signature was not unresolved, then we can use the signature, but the input value
                            # May yet be incorrectly typed.
                            kwarg_types[k] = v
        return kwarg_types

    def resolve_signature(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None,
                          **kwargs) -> "WiringNodeSignature":
        _, resolved_signature = self._validate_and_resolve_signature(*args,
                                                                     __pre_resolved_types__=__pre_resolved_types__,
                                                                     **kwargs)
        return resolved_signature

    def _validate_and_resolve_signature(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData], **kwargs) \
            -> tuple[dict[str, Any], WiringNodeSignature]:
        """
        Insure the inputs wired in match the signature of this node and resolve any missing types.
        """
        # Validate that all inputs have been received and apply the defaults.
        kwargs = self._prepare_kwargs(*args, **kwargs)
        WiringContext.current_kwargs = kwargs
        try:
            # Extract any additional required type resolution information from inputs
            kwarg_types = self._convert_kwargs_to_types(**kwargs)
            # Do the resolve to ensure types match as well as actually resolve the types.
            resolution_dict = self.signature.build_resolution_dict(__pre_resolved_types__, kwarg_types, kwargs)
            resolved_inputs = self.signature.resolve_inputs(resolution_dict)
            resolved_output = self.signature.resolve_output(resolution_dict)
            valid_inputs = self.signature.resolve_valid_inputs(**kwargs)
            resolved_inputs = self.signature.resolve_auto_resolve_kwargs(resolution_dict, kwarg_types, kwargs,
                                                                         resolved_inputs)

            if self.signature.is_resolved:
                self.signature.resolve_auto_const_and_type_kwargs(kwarg_types, kwargs)
                return kwargs, self.signature
            else:
                # Only need to re-create if we actually resolved the signature.
                resolve_signature = WiringNodeSignature(
                    node_type=self.signature.node_type,
                    name=self.signature.name,
                    args=self.signature.args,
                    defaults=self.signature.defaults,
                    input_types=resolved_inputs,
                    output_type=resolved_output,
                    src_location=self.signature.src_location,
                    active_inputs=self.signature.active_inputs,
                    valid_inputs=valid_inputs,
                    unresolved_args=frozenset(),
                    time_series_args=self.signature.time_series_args,
                    uses_scheduler=self.signature.uses_scheduler,  # This should not differ based on resolution
                    label=self.signature.label)
                if resolve_signature.is_resolved:
                    resolve_signature.resolve_auto_const_and_type_kwargs(kwarg_types, kwargs)
                    return kwargs, resolve_signature
                else:
                    raise WiringError(f"{resolve_signature.name} was not able to resolve itself")
        except Exception as e:
            if isinstance(e, WiringError):
                raise e
            path = '\n'.join(str(p) for p in WiringGraphContext.wiring_path())
            raise WiringError(f"Failure resolving signature, graph call stack:\n{path}") from e

    def _check_overloads(self, *args, **kwargs) -> "WiringPort":
        if (overload_helper := getattr(self, "overload_list", None)) is not None:
            overload_helper: OverloadedWiringNodeHelper
            best_overload = overload_helper.get_best_overload(*args, **kwargs)
            best_overload: WiringNodeClass
            if best_overload is not self:
                return best_overload(*args, **kwargs)

    def __call__(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None,
                 **kwargs) -> "WiringPort":

        if (r := self._check_overloads(*args, **kwargs, __pre_resolved_types__=__pre_resolved_types__)) is not None:
            return r

        # TODO: Capture the call site information (line number / file etc.) for better error reporting.
        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            # Now validate types and resolve any un-resolved types and provide an updated signature.
            kwargs_, resolved_signature = self._validate_and_resolve_signature(*args,
                                                                               __pre_resolved_types__=__pre_resolved_types__,
                                                                               **kwargs)

            # TODO: This mechanism to work out rank may fail when using a delayed binding?
            match resolved_signature.node_type:
                case WiringNodeType.PUSH_SOURCE_NODE:
                    rank = 0
                case WiringNodeType.PULL_SOURCE_NODE:
                    rank = 1
                case WiringNodeType.COMPUTE_NODE | WiringNodeType.SINK_NODE:
                    rank = max(v.rank for k, v in kwargs_.items() if
                               v is not None and k in self.signature.time_series_args) + 1
                case _:
                    rank = -1

            wiring_node_instance = WiringNodeInstance(self, resolved_signature, frozendict(kwargs_), rank=rank)
            # Select the correct wiring port for the TS type! That we can provide useful wiring syntax
            # to support this like out.p1 on a bundle or out.s1 on a ComplexScalar, etc.

            if resolved_signature.node_type is WiringNodeType.SINK_NODE:
                WiringGraphContext.instance().add_sink_node(wiring_node_instance)
            else:
                # Whilst a graph could represent a sink signature, it is not a node, we return the wiring port
                # as it is used by the GraphWiringNodeClass to validate the resolved signature with that of the returned
                # output
                return _wiring_port_for(resolved_signature.output_type, wiring_node_instance, tuple())

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


def create_input_output_builders(node_signature: "NodeSignature") -> tuple["InputBuilder", "OutputBuilder"]:
    from hgraph import TimeSeriesBuilderFactory
    factory: TimeSeriesBuilderFactory = TimeSeriesBuilderFactory.instance()
    output_type = node_signature.time_series_output
    if ts_inputs := node_signature.time_series_inputs:
        from hgraph._types._tsb_type import UnNamedTimeSeriesSchema
        un_named_bundle = HgTSBTypeMetaData(HgTimeSeriesSchemaTypeMetaData(
            UnNamedTimeSeriesSchema.create_resolved_schema(
                {k: ts_inputs[k] for k in filter(lambda k_: k_ in ts_inputs, node_signature.args)})
        ))
        input_builder = factory.make_input_builder(un_named_bundle)
    else:
        input_builder = None
    return input_builder, None if output_type is None else factory.make_output_builder(output_type)


class PreResolvedWiringNodeWrapper(WiringNodeClass):
    """Wraps a WiringNodeClass_ instance with the associated resolution dictionary"""

    underlying_node: BaseWiringNodeClass
    resolved_types: dict[TypeVar, HgTypeMetaData]

    def __init__(self, signature: WiringNodeSignature, fn: Callable,
                 underlying_node: BaseWiringNodeClass, resolved_types: dict[TypeVar, HgTypeMetaData]):
        super().__init__(replace(signature, input_types=signature.resolve_inputs(resolved_types, True),
                                 output_type=signature.resolve_output(resolved_types, True)), fn)
        if isinstance(underlying_node, PreResolvedWiringNodeWrapper):
            # We don't want to create unnecessary chains so unwrap and create the super set result.
            underlying_node = underlying_node.underlying_node
            resolved_types = underlying_node.resolved_types | resolved_types
        self.underlying_node = underlying_node
        self.resolved_types = resolved_types

    def resolve_signature(self, *args, __pre_resolved_types__=None, **kwargs) -> "WiringNodeSignature":
        return self.underlying_node.resolve_signature(*args, __pre_resolved_types__=self.resolved_types, **kwargs)

    def __call__(self, *args, **kwargs) -> "WiringNodeInstance":
        return self.underlying_node(*args, __pre_resolved_types__=self.resolved_types, **kwargs)


class OverloadedWiringNodeHelper:
    """
    This meta wiring node class deals with graph/node declaration overloads, for example when we have an implementation
    of a node that is generic

        def n(t: TIME_SERIES_TYPE)

    and another one that is more specific like

        def n(t: TS[int])

    in this case if wired with TS[int] input we should choose the more specific implementation and the generic one in
    other cases.

    This problem becomes slightly trickier with more inputs or more complex types, consider:

        def m(t1: TIME_SERIES_TYPE, t2: TIME_SERIES_TYPE)  # choice 1
        def m(t1: TS[SCALAR], t2: TS[SCALAR])  # choice 2
        def m(t1: TS[int], t2: TIME_SERIES_TYPE)  # choice 3

    What should we wire provided two TS[int] inputs? In this case choice 2 is the right answer because it is more
    specific about ints inputs even if choice 3 matches one of the input types exactly. We consider a signature with
    top level generic inputs as always less specified than a signature with generics as parameters to specific
    collection types. This rule applies recursively so TSL[V, 2] is less specific than TSL[TS[SCALAR], 2]
    """

    overloads: List[Tuple[WiringNodeClass, float]]

    def __init__(self, base: WiringNodeClass):
        self.overloads = [(base, self._calc_rank(base.signature))]

    def overload(self, impl: WiringNodeClass):
        self.overloads.append((impl, self._calc_rank(impl.signature)))

    @staticmethod
    def _calc_rank(signature: WiringNodeSignature) -> float:
        return sum(t.operator_rank for t in signature.input_types.values())

    def get_best_overload(self, *args, **kwargs):
        candidates = []
        for c, r in self.overloads:
            try:
                # Attempt to resolve the signature, if this fails then we don't have a candidate
                c.resolve_signature(*args, **kwargs)
                candidates.append((c, r))
            except Exception:
                pass
        if not candidates:
            raise WiringError(
                f"{self.overloads[0][0].signature.name} cannot be wired with given parameters - no matching candidates found")

        best_candidates = sorted(candidates, key=lambda x: x[1])
        if len(best_candidates) > 1 and best_candidates[0][1] == best_candidates[1][1]:
            raise WiringError(
                f"{self.overloads[0][0].signature.name} overloads are ambiguous with given parameters - more than one top candidate")

        return best_candidates[0][0]


class CppWiringNodeClass(BaseWiringNodeClass):
    ...


class PythonGeneratorWiringNodeClass(BaseWiringNodeClass):

    def create_node_builder_instance(self, node_signature, scalars) -> "NodeBuilder":
        from hgraph._impl._builder import PythonGeneratorNodeBuilder
        from hgraph import TimeSeriesBuilderFactory
        factory: TimeSeriesBuilderFactory = TimeSeriesBuilderFactory.instance()
        output_type = node_signature.time_series_output
        assert output_type is not None, "PythonGeneratorWiringNodeClass must have a time series output"
        return PythonGeneratorNodeBuilder(signature=node_signature,
                                          scalars=scalars,
                                          input_builder=None,
                                          output_builder=factory.make_output_builder(output_type),
                                          eval_fn=self.fn)


class PythonPushQueueWiringNodeClass(BaseWiringNodeClass):

    def create_node_builder_instance(self, node_signature, scalars) -> "NodeBuilder":
        from hgraph._impl._builder import PythonPushQueueNodeBuilder
        from hgraph import TimeSeriesBuilderFactory
        factory: TimeSeriesBuilderFactory = TimeSeriesBuilderFactory.instance()
        output_type = node_signature.time_series_output
        assert output_type is not None, "PythonPushQueueWiringNodeClass must have a time series output"
        return PythonPushQueueNodeBuilder(signature=node_signature,
                                          scalars=scalars,
                                          input_builder=None,
                                          output_builder=factory.make_output_builder(output_type),
                                          eval_fn=self.fn)


class PythonLastValuePullWiringNodeClass(BaseWiringNodeClass):

    def create_node_builder_instance(self, node_signature, scalars) -> "NodeBuilder":
        from hgraph._impl._builder._node_builder import PythonLastValuePullNodeBuilder
        from hgraph import TimeSeriesBuilderFactory
        factory: TimeSeriesBuilderFactory = TimeSeriesBuilderFactory.instance()
        output_type = node_signature.time_series_output
        assert output_type is not None, "PythonLastValuePullWiringNodeClass must have a time series output"
        return PythonLastValuePullNodeBuilder(
            signature=node_signature,
            scalars=scalars,
            input_builder=None,
            output_builder=factory.make_output_builder(output_type)
        )


class PythonWiringNodeClass(BaseWiringNodeClass):

    def create_node_builder_instance(self, node_signature, scalars) -> "NodeBuilder":
        from hgraph._impl._builder import PythonNodeBuilder
        input_builder, output_builder = create_input_output_builders(node_signature)

        return PythonNodeBuilder(signature=node_signature,
                                 scalars=scalars,
                                 input_builder=input_builder,
                                 output_builder=output_builder,
                                 eval_fn=self.fn,
                                 start_fn=self.start_fn,
                                 stop_fn=self.stop_fn)


class WiringGraphContext:
    """
    Used to track the call stack and to track sink nodes for the graph.
    """

    __shelved_stack__: [["WiringGraphContext"]] = []
    __stack__: ["WiringGraphContext"] = []

    __strict__: bool = False

    @classmethod
    def is_strict(cls) -> bool:
        return WiringGraphContext.__strict__

    @classmethod
    def set_strict(cls, strict: bool):
        WiringGraphContext.__strict__ = strict

    @classmethod
    def shelve_wiring(cls):
        """
        Put the current wiring stack on the shelf in order to build a fresh wiring stack, this is useful for nested
        engine generates such as branch.
        """
        WiringGraphContext.__shelved_stack__.append(WiringGraphContext.__stack__)
        WiringGraphContext.__stack__ = []

    @classmethod
    def un_shelve_wiring(cls):
        """Replace the stack with the previously shelved stack"""
        WiringGraphContext.__stack__ = WiringGraphContext.__shelved_stack__.pop()

    @classmethod
    def wiring_path(cls) -> [SourceCodeDetails]:
        """Return a graph call stack"""
        # TODO: Look into how this could be improved to include call site information.
        # The first entry is the root node of the graph stack
        return [graph.wiring_node_signature.src_location for graph in reversed(cls.__stack__[1:])]

    @classmethod
    def instance(cls) -> "WiringGraphContext":
        return WiringGraphContext.__stack__[-1]

    def __init__(self, node_signature: Optional[WiringNodeSignature]):
        """
        If we are wiring the root graph, then there is no wiring node. In this case None is
        passed in.
        """
        self._wiring_node_signature: WiringNodeSignature = node_signature
        self._sink_nodes: ["WiringNodeInstance"] = []

    @property
    def sink_nodes(self) -> tuple["WiringNodeInstance", ...]:
        return tuple(self._sink_nodes)

    def has_sink_nodes(self) -> bool:
        return bool(self._sink_nodes)

    @property
    def wiring_node_signature(self) -> WiringNodeSignature:
        return self._wiring_node_signature

    def add_sink_node(self, node: "WiringNodeInstance"):
        self._sink_nodes.append(node)

    def pop_sink_nodes(self) -> ["WiringNodeInstance"]:
        """
        Remove sink nodes that are on this graph context.
        This is useful when building a nested graph
        """
        sink_nodes = self._sink_nodes
        self._sink_nodes = []
        return sink_nodes

    def __enter__(self):
        WiringGraphContext.__stack__.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        WiringGraphContext.__stack__.pop()
        if WiringGraphContext.__stack__:
            # For now lets bubble the sink nodes up.
            # It may be useful to track the sink nodes in the graph they are produced.
            # The alternative would be to track them only on the root node.
            WiringGraphContext.__stack__[-1]._sink_nodes.extend(self._sink_nodes)


class GraphWiringNodeClass(BaseWiringNodeClass):

    def __call__(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None,
                 **kwargs) -> "WiringPort":

        if (r := self._check_overloads(*args, **kwargs, __pre_resolved_types__=__pre_resolved_types__)) is not None:
            return r

        # We don't want graph and node signatures to operate under different rules as this would make
        # moving between node and graph implementations problematic, so resolution rules of the signature
        # hold
        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            kwargs_, resolved_signature = self._validate_and_resolve_signature(*args,
                                                                               __pre_resolved_types__=__pre_resolved_types__,
                                                                               **kwargs)

            # But graph nodes are evaluated at wiring time, so this is the graph expansion happening here!
            with WiringGraphContext(self.signature) as g:
                out: WiringPort = self.fn(**kwargs_)
                if output_type := resolved_signature.output_type:
                    if output_type.dereference() != out.output_type.dereference():
                        raise WiringError(f"'{self.signature.name}' declares it's output as '{str(output_type)}' but "
                                          f"'{str(out.output_type)}' was returned from the graph")
                elif WiringGraphContext.is_strict() and not g.has_sink_nodes():
                    raise WiringError(f"'{self.signature.name}' does not seem to do anything")
                return out


class StubWiringNodeClass(BaseWiringNodeClass):

    def __call__(self, *args, **kwargs) -> "WiringPort":
        """Sub wiring classes are not callable"""
        raise NotImplementedError()

    def create_node_builder_instance(self, node_signature, scalars) -> "NodeBuilder":
        """Sub wiring classes do not create node builders"""
        raise NotImplementedError()


class NonPeeredWiringNodeClass(StubWiringNodeClass):
    """Used to represent Non-graph nodes to use when creating non-peered wiring ports"""

    def __call__(self, _tsb_meta_type: HgTSBTypeMetaData, **kwargs) -> "WiringPort":
        ...


@dataclass(frozen=True, eq=False, unsafe_hash=True)  # We will write our own equality check, but still want a hash
class WiringNodeInstance:
    node: WiringNodeClass
    resolved_signature: WiringNodeSignature
    inputs: frozendict[str, Any]  # This should be a mix of WiringPort for time series inputs and scalar values.
    rank: int

    def __eq__(self, other):
        return type(self) is type(other) and self.node == other.node and \
            self.resolved_signature == other.resolved_signature and self.rank == other.rank and \
            self.inputs.keys() == other.inputs.keys() and \
            all(v.__orig_eq__(other.inputs[k]) if hasattr(v, '__orig_eq__') else v == other.inputs[k]
                for k, v in self.inputs.items())
        # Deal with possible WiringPort equality issues due to operator overloading in the syntactical sugar wrappers

    @property
    def is_stub(self) -> bool:
        return isinstance(self.node, StubWiringNodeClass)

    @property
    def output_type(self) -> HgTimeSeriesTypeMetaData:
        return self.resolved_signature.output_type

    @property
    def node_signature(self) -> "NodeSignature":
        from hgraph._runtime import NodeSignature, NodeTypeEnum
        return NodeSignature(
            name=self.resolved_signature.name,
            node_type=NodeTypeEnum(self.resolved_signature.node_type.value),
            args=self.resolved_signature.args,
            time_series_inputs=self.resolved_signature.time_series_inputs,
            time_series_output=self.resolved_signature.output_type,
            scalars=self.resolved_signature.scalar_inputs,
            src_location=self.resolved_signature.src_location,
            active_inputs=self.resolved_signature.active_inputs,
            valid_inputs=self.resolved_signature.valid_inputs,
            uses_scheduler=self.resolved_signature.uses_scheduler
        )

    def create_node_builder_and_edges(self, node_map: MutableMapping["WiringNodeInstance", int],
                                      nodes: ["NodeBuilder"]) -> tuple["NodeBuilder", set["Edge"]]:
        """Create an runtime node instance"""
        # Collect appropriate inputs and construct the node
        node_index = len(nodes)
        node_map[self] = node_index  # Update this wiring nodes index in the graph

        scalars = frozendict({k: t.injector if t.is_injectable else self.inputs[k] for k, t in
                              self.resolved_signature.scalar_inputs.items()})

        node_builder = self.node.create_node_builder_instance(self.node_signature, scalars)
        # Extract out edges

        edges = set()
        for ndx, arg in enumerate(raw_arg for raw_arg in self.resolved_signature.args if
                                  raw_arg in self.resolved_signature.time_series_args):
            input_: WiringPort = self.inputs.get(arg)
            if input_ is not None:
                edges.update(input_.edges_for(node_map, node_index, (ndx,)))

        return node_builder, edges


def _wiring_port_for(tp: HgTypeMetaData, node_instance: WiringNodeInstance, path: [int, ...]) -> "WiringPort":
    return {
        HgTSDTypeMetaData: lambda: TSDWiringPort(node_instance, path),
        HgTSBTypeMetaData: lambda: TSBWiringPort(node_instance, path),
        HgTSLTypeMetaData: lambda: TSLWiringPort(node_instance, path),
    }.get(type(tp), lambda: WiringPort(node_instance, path))()


@dataclass(frozen=True)
class WiringPort:
    """
    A wiring port is the abstraction that describes the src of an edge in a wiring graph. This source is used to
    connect to a destination node in the graph, typically an input in the graph.
    The port consists of a reference to a node instance, this is the node in the graph to connect to, and a path, this
    is the selector used to identify to which time-series owned by the node this portion of the edge refers to.

    For a simple time-series (e.g. TS[SCALAR]), the path is an empty tuple. For more complex time-series containers,
    the path can be any valid SCALAR value that makes sense in the context of the container.
    The builder will ultimately walk the path calling __getitem__ the time-series until the path is completed.

    For example, node.output[p1][p2][p3] for a path of (p1, p2, p3).
    """
    node_instance: WiringNodeInstance
    path: tuple[SCALAR, ...] = tuple()  # The path from out () to the time-series to be bound.

    @property
    def has_peer(self) -> bool:
        return not isinstance(self.node_instance.node, NonPeeredWiringNodeClass)

    def edges_for(self, node_map: Mapping["WiringNodeInstance", int], dst_node_ndx: int, dst_path: tuple[SCALAR, ...]) \
            -> set["Edge"]:
        """Return the edges required to bind this output to the dst_node"""
        assert self.has_peer, \
            "Can not bind a non-peered node, the WiringPort must be sub-classed and override this method"

        from hgraph._builder._graph_builder import Edge
        return {Edge(node_map[self.node_instance], self.path, dst_node_ndx, dst_path)}

    @property
    def output_type(self) -> HgTimeSeriesTypeMetaData:
        output_type = self.node_instance.output_type
        for p in self.path:
            # This is the path within a TSB
            output_type = output_type[p]
        return output_type

    @property
    def rank(self) -> int:
        return self.node_instance.rank


@dataclass(frozen=True)
class TSDWiringPort(WiringPort, Generic[SCALAR, TIME_SERIES_TYPE]):

    @property
    def key_set(self) -> TSS[str]:
        return WiringPort(self.node_instance, self.path + (KEY_SET_ID,))


@dataclass(frozen=True)
class TSBWiringPort(WiringPort):

    @cached_property
    def __schema__(self) -> "TimeSeriesSchema":
        return self.output_type.bundle_schema_tp.py_type

    @property
    def as_schema(self):
        """Support the as_schema syntax"""
        return self

    def __getattr__(self, item):
        return self._wiring_port_for(item)

    def _wiring_port_for(self, item):
        """Support the path selection using property names"""
        schema: TimeSeriesSchema = self.__schema__
        if type(item) is str:
            arg = item
            ndx = schema.index_of(item)
        elif type(item) is int:
            ndx = item
            arg = nth(schema.__meta_data_schema__.keys(), item)
        else:
            raise AttributeError(f"'{item}' is not typeof str or int")
        tp = schema.__meta_data_schema__[arg]
        if self.has_peer:
            path = self.path + (ndx,)
            node_instance = self.node_instance
        else:
            input_wiring_port = self.node_instance.inputs[arg]
            node_instance = input_wiring_port.node_instance
            path = input_wiring_port.path
        return _wiring_port_for(tp, node_instance, path)

    def __getitem__(self, item):
        return self._wiring_port_for(item)

    def edges_for(self, node_map: Mapping["WiringNodeInstance", int], dst_node_ndx: int,
                  dst_path: tuple[SCALAR, ...]) -> \
            set["Edge"]:
        edges = set()
        if self.has_peer:
            from hgraph._builder._graph_builder import Edge
            edges.add(Edge(node_map[self.node_instance], self.path, dst_node_ndx, dst_path))
        else:
            for ndx, arg in enumerate(self.__schema__.__meta_data_schema__):
                wiring_port = self._wiring_port_for(arg)
                edges.update(wiring_port.edges_for(node_map, dst_node_ndx, dst_path + (ndx,)))
        return edges


@dataclass(frozen=True)
class TSLWiringPort(WiringPort):

    def __getitem__(self, item):
        """Return the wiring port for an individual TSL element"""
        output_type: HgTSLTypeMetaData = self.output_type
        tp_ = output_type.value_tp
        size_ = output_type.size
        if not size_.FIXED_SIZE:
            raise CustomMessageWiringError(
                "Currently we are unable to select a time-series element from an unbounded TSL")
        elif item >= size_.SIZE:
            # Unfortuantly zip seems to depend on an IndexError being raised, so try and provide
            # as much useful context in the error message as possible
            msg = f"When resolving '{WIRING_CONTEXT.signature}' \n"
            f"Trying to select an element from a TSL that is out of bounds: {item} >= {size_.SIZE}"
            raise IndexError(msg)

        if self.has_peer:
            path = self.path + (item,)
            node_instance = self.node_instance
        else:
            args = self.node_instance.resolved_signature.args
            ts_args = self.node_instance.resolved_signature.time_series_args
            arg = nth(filter(lambda k_: k_ in ts_args, args), item)
            input_wiring_port = self.node_instance.inputs[arg]
            node_instance = input_wiring_port.node_instance
            path = input_wiring_port.path
        return _wiring_port_for(tp_, node_instance, path)

    def edges_for(self, node_map: Mapping["WiringNodeInstance", int], dst_node_ndx: int,
                  dst_path: tuple[SCALAR, ...]) -> \
            set["Edge"]:
        edges = set()
        if self.has_peer:
            from hgraph._builder._graph_builder import Edge
            edges.add(Edge(node_map[self.node_instance], self.path, dst_node_ndx, dst_path))
        else:
            # This should work as we don't support unbounded TSLs as non-peered nodes at the moment.
            for ndx in range(self.output_type.size.SIZE):
                wiring_port = self[ndx]
                edges.update(wiring_port.edges_for(node_map, dst_node_ndx, dst_path + (ndx,)))
        return edges

