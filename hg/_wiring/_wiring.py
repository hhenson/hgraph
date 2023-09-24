from dataclasses import dataclass
from types import GenericAlias
from typing import Callable, Any, TypeVar, _GenericAlias

__all__ = (
    "WiringNodeClass", "CppWiringNodeClass", "PythonWiringNodeClass", "WiringGraphContext", "GraphWiringNodeClass",
    "PythonGeneratorWiringNodeClass")

from frozendict import frozendict

from hg._runtime import SourceCodeDetails, Node
from hg._types import HgTypeMetaData, HgTimeSeriesTypeMetaData, HgScalarTypeMetaData, ParseError
from hg._types._scalar_type_meta_data import HgTypeOfTypeMetaData
from hg._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType


# TODO: Add ability to speicify resolution of inputs / outputs at wiring time. In which case unresolved outputs are possible!

class WiringError(RuntimeError):
    ...


class WiringNodeClass:
    """
    The wiring node template, this has the signature and information required to construct a run-time node.
    The template is instantiable to form a WiringNodeInstance, the instance can be used to build a wiring graph.
    """

    def __call__(self, *args, **kwargs) -> "WiringNodeInstance":
        raise NotImplementedError()

    def _convert_item(self, item) -> dict[TypeVar, HgTypeMetaData]:
        if isinstance(item, slice):
            item = (item,)  # Normalise all items into a tuple
        out = {}
        for s in item:
            assert s.step is None, f"Signature of type resolution is incorrect, expect TypeVar: Type, ... got {s}"
            assert s.start is not None, f"Signature of type resolution is incorrect, expect TypeVar: Type, ... got {s}"
            assert s.stop is not None, f"signature of type resolution is incorrect, None is not a valid type"
            assert isinstance(s.start,
                              TypeVar), f"Signature of type resolution is incorrect first item must be of type TypeVar, got {s.start}"
            out[s.start] = (parsed := HgTypeMetaData.parse(s.stop))
            assert parsed is not None, f"Can not resolve {s.stop} into a valid scalar or time-series type"
            assert parsed.is_resolved, f"The resolved value {s.stop} is not resolved, this is not supported."

    def __getitem__(self, item) -> "WiringNodeClass":
        """
        Expected use:
        ```python
        my_node[SCALAR: int, TIME_SERIES_TYPE: TS[int]](...)
        ```
        """
        raise NotImplementedError()


@dataclass(frozen=True, unsafe_hash=True)
class BaseWiringNodeClass(WiringNodeClass):
    signature: WiringNodeSignature
    fn: Callable

    def __getitem__(self, item) -> WiringNodeClass:
        return PreResolvedWiringNodeWrapper(self, self._convert_item(item))

    def _prepare_kwargs(self, *args, **kwargs) -> dict[str, Any]:
        """
        Extract the args and kwargs, apply defaults and validate the input shape as correct.
        This does not validate the types, just that all args are provided.
        """
        if len(args) + len(kwargs) > len(self.signature.args):
            raise SyntaxError(
                f"[{self.signature.name}] More arguments are provided than are defined for this function")
        kwargs_ = {k: arg for k, arg in zip(self.signature.args, args)}  # Map the *args to keys
        if any(k in kwargs for k in kwargs_):
            raise SyntaxError(
                f"[{self.signature.name}] The following keys are duplicated: {[k for k in kwargs_ if k in kwargs]}")
        kwargs_ |= kwargs  # Merge in the current kwargs
        kwargs_ |= {k: v for k, v in self.signature.defaults.items() if k not in kwargs_}  # Add in defaults
        # TODO: add support for useful defaults for things like null_source inputs.
        if len(kwargs_) < len(self.signature.args):
            raise SyntaxError(
                f"[{self.signature.name}] Missing the following inputs: {[k for k in self.signature.args if k not in kwargs_]}")
        if any(arg not in kwargs_ for arg in self.signature.args):
            raise SyntaxError(f"[{self.signature.name}] Has incorrect kwargs names "
                              f"{[arg for arg in kwargs_ if arg not in self.signature.args]} "
                              f"expected: {[arg for arg in self.signature.args if arg not in kwargs_]}")
        return kwargs_

    def _convert_kwargs_to_types(self, **kwargs) -> dict[str, HgTypeMetaData]:
        """Attempt to convert input types to better support type resolution"""
        # We only need to extract un-resolved values
        kwarg_types = {}
        for k, v in self.signature.input_types.items():
            arg = kwargs.get(k, self.signature.defaults.get(k))
            if arg is None:
                if v.is_injectable:
                    # For injectables we expect the value to be None, and the type must already be resolved.
                    kwarg_types[k] = v
                else:
                    raise ParseError(f'{k}: {v} argument is required but not supplied')
            if k in self.signature.time_series_args:
                # This should then get a wiring node, and we would like to extract the output type,
                # But this is optional so we should ensure that the type is present
                if not isinstance(arg, WiringPort):
                    raise ParseError(f'{k}: {v} = {arg}, argument is not a time-series value')
                if arg.output_type:
                    kwarg_types[k] = arg.output_type
                else:
                    raise ParseError(
                        f'{k}: {v} = {arg}, argument supplied is not a valid source or compute_node output')
            elif type(v) is HgTypeOfTypeMetaData:
                if not isinstance(arg, (type, GenericAlias, _GenericAlias)):
                    raise ParseError(f"Input {k} in {self.signature.name} is expected to be a type, "
                                     f"but got '{arg}' instead")
                v = HgTypeMetaData.parse(arg)
                kwarg_types[k] = HgTypeOfTypeMetaData(v)
            else:
                kwarg_types[k] = (tp := HgScalarTypeMetaData.parse(arg))
                if tp is None:
                    if k in self.signature.unresolved_args:
                        raise ParseError(f"In {self.signature.name}, {k}: {v} = {arg}; arg is not parsable, "
                                         f"but we require type resolution")
                    else:
                        # If the signature was not unresolved, then we can use the signature, but the input value
                        # May yet be incorrectly typed.
                        kwarg_types[k] = v
        return kwarg_types

    def _validate_and_resolve_signature(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData], **kwargs) \
            -> tuple[dict[str, Any], WiringNodeSignature]:
        """
        Insure the inputs wired in match the signature of this node and resolve any missing types.
        """
        # Validate that all inputs have been received and apply the defaults.
        kwargs = self._prepare_kwargs(*args, **kwargs)
        try:
            # Extract any additional required type resolution information from inputs
            kwarg_types = self._convert_kwargs_to_types(**kwargs)
            # Do the resolve to ensure types match as well as actually resolve the types.
            resolution_dict = self.signature.build_resolution_dict(__pre_resolved_types__, **kwarg_types)
            resolved_inputs = self.signature.resolve_inputs(resolution_dict)
            resolved_output = self.signature.resolve_output(resolution_dict)
            if self.signature.is_resolved:
                return kwargs, self.signature
            else:
                # Only need to re-create if we actually resolved the signature.
                resolve_signature = WiringNodeSignature(self.signature.node_type, self.signature.name,
                                                        self.signature.args,
                                                        self.signature.defaults, resolved_inputs, resolved_output,
                                                        self.signature.src_location, tuple(),
                                                        self.signature.time_series_args,
                                                        self.signature.label)
                if resolve_signature.is_resolved:
                    return kwargs, resolve_signature
                else:
                    raise ParseError(f"{resolve_signature.name} was not able to resolve itself")
        except Exception as e:
            path = '\n'.join(str(p) for p in WiringGraphContext.wiring_path())
            raise WiringError(f"Failure resolving signature, graph call stack:\n{path}") from e

    def __call__(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None,
                 **kwargs) -> "WiringPort":
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
                rank = max(v.rank for k, v in kwargs_.items() if k in self.signature.time_series_args) + 1
            case default:
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
            return WiringPort(wiring_node_instance)


@dataclass(frozen=True)
class PreResolvedWiringNodeWrapper(WiringNodeClass):
    """Wraps a WiringNodeClass_ instance with the associated resolution dictionary"""

    underlying_node: BaseWiringNodeClass
    resolved_types: dict[TypeVar, HgTypeMetaData]

    def __call__(self, *args, **kwargs) -> "WiringNodeInstance":
        return self.underlying_node(*args, __pre_resolved_types__=self.resolved_types, **kwargs)

    def __getitem__(self, item) -> WiringNodeClass:
        resolved_types = dict(self.resolved_types)
        resolved_types |= self._convert_item(item)
        return PreResolvedWiringNodeWrapper(self.underlying_node, resolved_types)


class CppWiringNodeClass(BaseWiringNodeClass):
    ...


class PythonGeneratorWiringNodeClass(BaseWiringNodeClass):
    ...


class PythonWiringNodeClass(BaseWiringNodeClass):
    ...


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
    def wiring_path(self) -> [SourceCodeDetails]:
        """Return a graph call stack"""
        # TODO: Look into how this could be improved to include call site information.
        # The first entry is the root node of the graph stack
        return [graph.graph_wiring_node_class.signature.src_location for graph in reversed(self.__stack__[1:])]

    @classmethod
    def instance(cls) -> "WiringGraphContext":
        return WiringGraphContext.__stack__[-1]

    def __init__(self, wiring_node: "GraphWiringNodeClass"):
        self._graph_wiring_node_class: "GraphWiringNodeClass" = wiring_node
        self._sink_nodes: ["WiringNodeInstance"] = []

    @property
    def sink_nodes(self) -> tuple["WiringNodeInstance", ...]:
        return tuple(self._sink_nodes)

    def has_sink_nodes(self) -> bool:
        return bool(self._sink_nodes)

    @property
    def graph_wiring_node_class(self) -> "GraphWiringNodeClass":
        return self._graph_wiring_node_class

    def add_sink_node(self, node: "WiringNodeInstance"):
        self._sink_nodes.append(node)

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
        # We don't want graph and node signatures to operate under different rules as this would make
        # moving between node and graph implementations problematic, so resolution rules of the signature
        # hold
        kwargs_, resoled_signature = self._validate_and_resolve_signature(*args, __pre_resolved_types__=__pre_resolved_types__,
                                                                 **kwargs)

        # But graph nodes are evaluated at wiring time, so this is the graph expansion happening here!
        with WiringGraphContext(self) as g:
            out: WiringPort = self.fn(*args, **kwargs)
            if output_type := resoled_signature.output_type:
                if output_type != out.output_type:
                    raise WiringError(f"'{self.signature.name}' declares it's output as '{str(output_type)}' but "
                                      f"'{str(out.output_type)}' was returned from the graph")
            elif WiringGraphContext.is_strict() and not g.has_sink_nodes():
                raise WiringError(f"'{self.signature.name}' does not seem to do anything")
            return out


@dataclass(frozen=True)
class WiringNodeInstance:
    node: WiringNodeClass
    resolved_signature: WiringNodeSignature
    inputs: frozendict[str, Any]  # This should be a mix of WiringPort for time series inputs and scalar values.
    rank: int

    @property
    def output_type(self) -> HgTimeSeriesTypeMetaData:
        return self.resolved_signature.output_type

    def create_node_instance(self, node_map: ["WiringNodeInstance", int], nodes: [Node]) -> Node:
        """Create an runtime node instance"""
        # Collect appropriate inputs and construct the node
        # TODO: do construction


@dataclass(frozen=True)
class WiringPort:
    node_instance: WiringNodeInstance

    @property
    def output_type(self) -> HgTimeSeriesTypeMetaData:
        return self.node_instance.output_type

    @property
    def rank(self) -> int:
        return self.node_instance.rank


@dataclass(frozen=True)
class TSB_WiringPort(WiringPort):
    path: tuple[str, ...]

    @property
    def output_type(self) -> HgTimeSeriesTypeMetaData:
        output_type = self.node_instance.output_type
        for p in self.path:
            # This is the parth within a TSB
            output_type = output_type[p]
