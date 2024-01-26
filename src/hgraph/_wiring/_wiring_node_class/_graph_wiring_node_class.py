from typing import Optional, TypeVar

from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._source_code_details import SourceCodeDetails
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass
from hgraph._wiring._wiring_port import WiringPort
from hgraph._wiring._wiring_errors import WiringError

__all__ = ('WiringGraphContext', "GraphWiringNodeClass")


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
                    if not output_type.dereference().matches(out.output_type.dereference()):
                        raise WiringError(f"'{self.signature.name}' declares it's output as '{str(output_type)}' but "
                                          f"'{str(out.output_type)}' was returned from the graph")
                elif WiringGraphContext.is_strict() and not g.has_sink_nodes():
                    raise WiringError(f"'{self.signature.name}' does not seem to do anything")
                return out
