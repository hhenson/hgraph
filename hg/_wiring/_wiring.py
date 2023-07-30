from dataclasses import dataclass
from typing import Callable, Any, TypeVar

__all__ = (
    "WiringNodeClass", "CppWiringNodeClass", "PythonWiringNodeClass", "WiringGraphContext", "GraphWiringNodeClass",
    "PythonGeneratorWiringNodeClass")

from hg._runtime import SourceCodeDetails, Node
from hg._types import HgTypeMetaData, HgTimeSeriesTypeMetaData, HgScalarTypeMetaData
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


@dataclass(frozen=True)
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


    def _validate_and_resolve_signature(self, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData], **kwargs) \
            -> WiringNodeSignature:
        """
        Insure the inptuts wired in match the signature of this node and resolve any missing types.
        """
        # Do we need to parse the scalar values into types? I think so
        kwarg_types = {k: v.output_type if k in self.signature.time_series_args else
                        HgScalarTypeMetaData.parse(v) for k, v in kwargs.items()}
        # Do the resolve to ensure types match as well as actually resolve the types.
        resolution_dict = self.signature.build_resolution_dict(__pre_resolved_types__, **kwarg_types)
        resolved_inputs = self.signature.resolve_inputs(resolution_dict)
        resolved_output = self.signature.resolve_output(resolution_dict)
        if self.signature.is_resolved:
            return self.signature
        else:
            # Only need to re-create if we actually resolved the signature.
            return WiringNodeSignature(self.signature.node_type, self.signature.name, self.signature.args,
                                            self.signature.defaults, resolved_inputs, resolved_output,
                                            self.signature.src_location, tuple(), self.signature.time_series_args,
                                            self.signature.label)



    def __call__(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None,
                 **kwargs) -> "WiringPort":
        # Validate that all inputs have been received and apply the defaults.
        kwargs_ = self._prepare_kwargs(*args, **kwargs)
        # Now validate types and resolve any un-resolved types and provide an updated signature.
        try:
            resolved_signature = self._validate_and_resolve_signature(__pre_resolved_types__=__pre_resolved_types__,
                                                                      **kwargs_)
            assert resolved_signature.is_resolved, \
                f"Call to {self.signature.name} did not successfully resolve all TypeVar's"
        except Exception as e:
            path = '\n'.join(str(p) for p in WiringGraphContext.wiring_path())
            raise WiringError(f"Failure resolving signature, graph call stack:\n{path}") from e

        wiring_node_instance = WiringNodeInstance(self, resolved_signature, kwargs_,
                                                  rank=max((v.rank for k, v in kwargs_.items()
                                                           if k in self.signature.time_series_args), default=1))
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
        self.underlying_node(*args, __pre_resolved_types__=self.resolved_types, **kwargs)

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
        wiring_port = super().__call__(*args, __pre_resolved_types__=__pre_resolved_types__, **kwargs)

        # But graph nodes are evaluated at wiring time, so this is the graph expansion happening here!
        with WiringGraphContext(self) as g:
            out: WiringPort = self.fn(*args, **kwargs)
            if (output_type := wiring_port.output_type):
                if output_type != out.output_type:
                    raise WiringError(f"'{self.signature.name}' declares it's output as '{str(output_type)}' but "
                                      f"'{str(out.output_type)}' was returned from the graph")
            elif WiringGraphContext.is_strict() and not g.has_sink_nodes():
                    raise WiringError(f"'{self.signature.name}' does not seem to do anything")
            return out


@dataclass(frozen=True, unsafe_hash=True)
class WiringNodeInstance:
    node: WiringNodeClass
    resolved_signature: WiringNodeSignature
    inputs: dict[str, Any]  # This should be a mix of WiringPort for time series inputs and scalar values.
    rank: int

    @property
    def output_type(self) -> HgTimeSeriesTypeMetaData:
        return self.resolved_signature.output_type

    def create_node_instance(self, node_map: ["WiringNodeInstance", int], nodes: [Node]) -> Node:
        """Create an runtime node instance"""
        # Collect appropriate inputs and construct the node
        # TODO: do construction


@dataclass
class WiringPort:
    node_instance: WiringNodeInstance

    @property
    def output_type(self) -> HgTimeSeriesTypeMetaData:
        return self.node_instance.output_type

    @property
    def rank(self) -> int:
        return self.node_instance.rank


@dataclass
class TSB_WiringPort(WiringPort):
    path: list[str]

    @property
    def output_type(self) -> HgTimeSeriesTypeMetaData:
        output_type = self.node_instance.output_type
        for p in self.path:
            # This is the parth within a TSB
            output_type = output_type[p]


