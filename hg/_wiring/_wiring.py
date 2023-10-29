import typing
from dataclasses import dataclass
from types import GenericAlias
from typing import Callable, Any, TypeVar, _GenericAlias, Optional, Mapping

from frozendict import frozendict

from hg import HgTSBTypeMetaData, HgTimeSeriesSchemaTypeMetaData
from hg._builder._graph_builder import Edge
from hg._types import HgTypeMetaData, HgTimeSeriesTypeMetaData, HgScalarTypeMetaData, ParseError
from hg._types._scalar_type_meta_data import HgTypeOfTypeMetaData
from hg._types._tsb_type import UnNamedTimeSeriesSchema
from hg._types._type_meta_data import IncorrectTypeBinding, WiringError
from hg._wiring._source_code_details import SourceCodeDetails
from hg._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType

if typing.TYPE_CHECKING:
    from hg._builder._node_builder import NodeBuilder
    from hg._runtime._node import NodeSignature, NodeTypeEnum

__all__ = ("WiringNodeClass", "BaseWiringNodeClass", "PreResolvedWiringNodeWrapper",
           "CppWiringNodeClass", "PythonGeneratorWiringNodeClass", "PythonWiringNodeClass", "WiringGraphContext",
           "GraphWiringNodeClass", "WiringNodeInstance", "WiringPort",)


# TODO: Add ability to specify resolution of inputs / outputs at wiring time.
#  In which case unresolved outputs are possible!


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

    def __eq__(self, other):
        return self.signature == other.signature and self.fn == other.fn

    def __hash__(self):
        return hash(self.signature)

    def create_node_builder_instance(self, node_ndx: int, node_signature: "NodeSignature", scalars: Mapping[str, Any]) \
            -> "NodeBuilder":
        """Create the appropriate node builder for the node this wiring node represents
        :param node_ndx:
        :param node_signature:
        :param scalars:
        """
        raise NotImplementedError()


class BaseWiringNodeClass(WiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        super().__init__(signature, fn)
        self.start_fn: Callable = None
        self.stop_fn: Callable = None

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
            try:
                resolution_dict = self.signature.build_resolution_dict(__pre_resolved_types__, **kwarg_types)
            except IncorrectTypeBinding as e:
                inp_value = kwargs[e.arg]
                if isinstance(inp_value, WiringPort):
                    inp_value = inp_value.node_instance.resolved_signature.signature
                else:
                    inp_value = str(inp_value)
                msg = f"When resolving '{self.signature.signature}' \n" \
                      f"Argument '{e.arg}: {e.expected_type}' <- '{e.actual_type}' from '{inp_value}'"
                import sys
                print(f"\n"
                      f"Wiring Error\n"
                      f"============\n"
                      f"\n"
                      f"{msg}", file=sys.stderr)
                raise WiringError(msg) from e
            resolved_inputs = self.signature.resolve_inputs(resolution_dict)
            resolved_output = self.signature.resolve_output(resolution_dict)
            if self.signature.is_resolved:
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
                    valid_inputs=self.signature.valid_inputs,
                    unresolved_args=tuple(),
                    time_series_args=self.signature.time_series_args,
                    label=self.signature.label)
                if resolve_signature.is_resolved:
                    return kwargs, resolve_signature
                else:
                    raise WiringError(f"{resolve_signature.name} was not able to resolve itself")
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

    def start(self, fn: Callable):
        """Decorator to indicate the start function for a node"""
        # TODO: validate the signature of the start function.
        self.start_fn = fn
        return self

    def stop(self, fn: Callable):
        """Decorator to indicate the stop function for a node"""
        # TODO: validate the signature of the stop function.
        self.stop_fn = fn
        return self


class PreResolvedWiringNodeWrapper(WiringNodeClass):
    """Wraps a WiringNodeClass_ instance with the associated resolution dictionary"""

    underlying_node: BaseWiringNodeClass
    resolved_types: dict[TypeVar, HgTypeMetaData]

    def __init__(self, signature: WiringNodeSignature, fn: Callable,
                 underlying_node: BaseWiringNodeClass, resolved_types: dict[TypeVar, HgTypeMetaData]):
        super().__init__(signature, fn)
        self.underlying_node = underlying_node
        self.resolved_types = resolved_types

    def __call__(self, *args, **kwargs) -> "WiringNodeInstance":
        return self.underlying_node(*args, __pre_resolved_types__=self.resolved_types, **kwargs)

    def __getitem__(self, item) -> WiringNodeClass:
        resolved_types = dict(self.resolved_types)
        resolved_types |= self._convert_item(item)
        return PreResolvedWiringNodeWrapper(self.underlying_node, resolved_types)


class CppWiringNodeClass(BaseWiringNodeClass):
    ...


class PythonGeneratorWiringNodeClass(BaseWiringNodeClass):

    def create_node_builder_instance(self, node_ndx, node_signature, scalars) -> "NodeBuilder":
        from hg._impl._builder import PythonGeneratorNodeBuilder
        from hg import TimeSeriesBuilderFactory
        factory: TimeSeriesBuilderFactory = TimeSeriesBuilderFactory.instance()
        output_type = node_signature.time_series_output
        assert output_type is not None, "PythonGeneratorWiringNodeClass must have a time series output"
        return PythonGeneratorNodeBuilder(node_ndx=node_ndx,
                                          signature=node_signature,
                                          scalars=scalars,
                                          input_builder=None,
                                          output_builder=factory.make_output_builder(output_type),
                                          eval_fn=self.fn)


class PythonWiringNodeClass(BaseWiringNodeClass):

    # TODO: Put in logic to support the construction of start_fn and stop_fn
    # Using the syntax:
    #  @<my_node_name>.start  # / stop
    #  def _(...):
    #      ...

    def create_node_builder_instance(self, node_ndx, node_signature, scalars) -> "NodeBuilder":
        from hg._impl._builder import PythonNodeBuilder
        from hg import TimeSeriesBuilderFactory
        factory: TimeSeriesBuilderFactory = TimeSeriesBuilderFactory.instance()
        output_type = node_signature.time_series_output
        if ts_inputs := node_signature.time_series_inputs:
            un_named_bundle = HgTSBTypeMetaData(HgTimeSeriesSchemaTypeMetaData(
                UnNamedTimeSeriesSchema.create_resolved_schema(ts_inputs)
            ))
            input_builder = factory.make_input_builder(un_named_bundle)
        else:
            input_builder = None

        return PythonNodeBuilder(node_ndx=node_ndx,
                                 signature=node_signature,
                                 scalars=scalars,
                                 input_builder=input_builder,
                                 output_builder=None if output_type is None else \
                                     factory.make_output_builder(output_type),
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
    def wiring_path(self) -> [SourceCodeDetails]:
        """Return a graph call stack"""
        # TODO: Look into how this could be improved to include call site information.
        # The first entry is the root node of the graph stack
        return [graph.graph_wiring_node_class.signature.src_location for graph in reversed(self.__stack__[1:])]

    @classmethod
    def instance(cls) -> "WiringGraphContext":
        return WiringGraphContext.__stack__[-1]

    def __init__(self, wiring_node: Optional["GraphWiringNodeClass"]):
        """
        If we are wiring the root graph, then there is no wiring node. In this case None is
        passed in.
        """
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
        kwargs_, resoled_signature = self._validate_and_resolve_signature(*args,
                                                                          __pre_resolved_types__=__pre_resolved_types__,
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

    @property
    def node_signature(self) -> "NodeSignature":
        from hg._runtime import NodeSignature, NodeTypeEnum
        return NodeSignature(name=self.resolved_signature.name,
                             node_type=NodeTypeEnum(self.resolved_signature.node_type.value),
                             args=self.resolved_signature.args,
                             time_series_inputs=self.resolved_signature.time_series_inputs,
                             time_series_output=self.resolved_signature.output_type,
                             scalars=self.resolved_signature.scalar_inputs,
                             src_location=self.resolved_signature.src_location,
                             active_inputs=self.resolved_signature.active_inputs,
                             valid_inputs=self.resolved_signature.valid_inputs)

    def create_node_builder_and_edges(self, node_map: Mapping["WiringNodeInstance", int], nodes: ["NodeBuilder"]) -> \
            tuple["NodeBuilder", set[Edge]]:
        """Create an runtime node instance"""
        # Collect appropriate inputs and construct the node
        node_index = len(nodes)
        node_map[self] = node_index  # Update this wiring nodes index in the graph

        scalars = frozendict({k: t.injector if t.is_injectable else self.inputs[k] for k, t in
                              self.resolved_signature.scalar_inputs.items()})

        node_builder = self.node.create_node_builder_instance(node_index, self.node_signature, scalars)
        # Extract out edges

        edges = set()
        for ndx, arg in enumerate(raw_arg for raw_arg in self.resolved_signature.time_series_inputs):
            input_: WiringPort = self.inputs[arg]
            edge = Edge(node_map[input_.node_instance], input_.path, node_index, (ndx,))
            edges.add(edge)
            # TODO: When dealing with more complex binding structures (such as TSBs) extracting the edges will be more complex.

        return node_builder, edges


@dataclass(frozen=True)
class WiringPort:
    node_instance: WiringNodeInstance
    path: [int, ...] = 0,  # The path from out (0,) to the time-series to be bound.

    @property
    def output_type(self) -> HgTimeSeriesTypeMetaData:
        return self.node_instance.output_type

    @property
    def rank(self) -> int:
        return self.node_instance.rank

# @dataclass(frozen=True)
# class TSBWiringPort(WiringPort):
#     path: tuple[str, ...]  # The path through the node elements
#
#     def edge_path(self, node_map: Mapping["WiringNodeInstance", int]) -> tuple[int, ...]:
#         path_ = [node_map[self.node_instance]]
#         for p in self.path:
#         return super().edge_path(node_map)
#
#     @property
#     def output_type(self) -> HgTimeSeriesTypeMetaData:
#         output_type = self.node_instance.output_type
#         for p in self.path:
#             # This is the parth within a TSB
#             output_type = output_type[p]
#         return output_type
