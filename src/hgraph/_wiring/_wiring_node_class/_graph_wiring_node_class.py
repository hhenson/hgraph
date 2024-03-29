import sys
from collections import defaultdict
from copy import copy
from graphlib import TopologicalSorter
from itertools import chain
from typing import Optional, TypeVar, Callable, Tuple, Dict

from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._types._ts_type_var_meta_data import HgTsTypeVarTypeMetaData
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._source_code_details import SourceCodeDetails
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass
from hgraph._wiring._wiring_port import WiringPort
from hgraph._wiring._wiring_errors import WiringError, CustomMessageWiringError

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
        return [graph.wiring_node_signature.src_location for graph in reversed(cls.__stack__[1:])
                if graph.wiring_node_signature]

    @classmethod
    def wiring_path_name(cls) -> str:
        """Return a graph call stack in names of graphs"""
        return '.'.join(graph.wiring_node_signature.name for graph in cls.__stack__[1:]
                if graph.wiring_node_signature)

    @classmethod
    def instance(cls) -> "WiringGraphContext":
        return WiringGraphContext.__stack__[-1] if WiringGraphContext.__stack__ else None

    def __init__(self, node_signature: Optional[WiringNodeSignature]):
        """
        If we are wiring the root graph, then there is no wiring node. In this case None is
        passed in.
        """
        self._wiring_node_signature: WiringNodeSignature = node_signature
        self._sink_nodes: ["WiringNodeInstance"] = []
        self._other_nodes: [Tuple["WiringPort", dict]] = []
        self._service_clients: [Tuple["WiringNodeClass", str]] = []
        self._service_implementations: Dict[Tuple["WiringNodeClass", str], Tuple["ServiceImplNodeClass", dict]] = {}
        self._built_services = {}

        self._current_frame = sys._getframe(1)

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

    def add_node(self, node: "WiringPort"):
        i = 1
        prev_f = None
        while self._current_frame != (f := sys._getframe(i)):
            if i > 20: return
            prev_f = f
            i += 1

        self._other_nodes.append((node, prev_f.f_locals))

    def label_nodes(self):
        """
        Label the nodes in the graph with the graph name
        """
        for port, locals in self._other_nodes:
            varname = next((k for k, v in locals.items() if v is port), None)
            if varname and port.path == ():
                port.node_instance.set_label(varname)

    def register_service_client(self, service: "ServiceInterfaceNodeClass", path: str):
        """
        Register a service client with the graph context
        """
        self._service_clients.append((service, path))

    def register_service_impl(self, service: "ServiceInterfaceNodeClass", path: str, impl: "ServiceImplNodeClass", kwargs):
        """
        Register a service client with the graph context
        """
        self._service_implementations[(service, path)] = (impl, kwargs)

    def add_built_service_impl(self, path, node):
        self.add_sink_node(node)
        self._built_services[path] = node

    def build_services(self):
        """
        Build the service implementations for the graph
        """
        service_clients = copy(self._service_clients)
        dependencies = {}
        while True:
            services_to_build_by_path = {}
            for service, path in set(service_clients):
                if item := self._service_implementations.get((service, path)):
                    impl, kwargs = item
                    services_to_build_by_path[path] = (service, impl, kwargs)
                else:
                    CustomMessageWiringError(f'No implementation found for service: {service.signature.name} at path: {path}')

            for path, (service, impl, kwargs) in services_to_build_by_path.items():
                if path not in self._built_services:
                    clients_before = len(self._service_clients)

                    impl, kwargs = self._service_implementations[(service, path)]
                    impl(path=path, **kwargs)

                    new_clients = self._service_clients[clients_before:]
                    dependencies.update({path: set(p for _, p in new_clients)})

            if self._service_clients == service_clients:
                break
            else:
                service_clients = copy(self._service_clients)

        ordered = list(TopologicalSorter(dependencies).static_order())
        for i, path in enumerate(ordered):
            object.__setattr__(self._built_services[path], 'rank', i + 1)


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
            WiringGraphContext.__stack__[-1]._service_clients.extend(self._service_clients)
            WiringGraphContext.__stack__[-1]._service_implementations.update(self._service_implementations)
            WiringGraphContext.__stack__[-1]._built_services.update(self._built_services)


class GraphWiringNodeClass(BaseWiringNodeClass):

    def __call__(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None,
                 **kwargs) -> "WiringPort":

        found_overload, r = self._check_overloads(*args, **kwargs, __pre_resolved_types__=__pre_resolved_types__)
        if found_overload:
            return r

        # We don't want graph and node signatures to operate under different rules as this would make
        # moving between node and graph implementations problematic, so resolution rules of the signature
        # hold
        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            kwargs_, resolved_signature = self._validate_and_resolve_signature(*args,
                                                                               __pre_resolved_types__=__pre_resolved_types__,
                                                                               __enforce_output_type__=False,
                                                                               **kwargs)

            # But graph nodes are evaluated at wiring time, so this is the graph expansion happening here!
            with WiringGraphContext(self.signature) as g:
                out: WiringPort = self.fn(**kwargs_)
                WiringGraphContext.instance().label_nodes()
                if output_type := resolved_signature.output_type:
                    from hgraph import HgTSBTypeMetaData
                    if not isinstance(out, WiringPort):
                        if isinstance(out, dict) and isinstance(output_type, HgTSBTypeMetaData):
                            out = output_type.py_type.from_ts(**out)
                        elif isinstance(out, dict) and isinstance(output_type, HgTsTypeVarTypeMetaData):
                            for c in output_type.constraints:
                                if isinstance(c, HgTSBTypeMetaData) and \
                                        all((t:= c.bundle_schema_tp.meta_data_schema.get(k)) and t.matches(v.output_type.dereference()) for k, v in out.items()):
                                    out = c.py_type.from_ts(**out)
                                    break
                            else:
                                raise WiringError(f"Expected a time series of type '{str(output_type)}' but got a dict of "
                                                  f"{{{', '.join(f'{k}:{str(v.output_type)}' for k, v in out.items())}}}")
                        else:
                            try:
                                # use build resolution dict from scalar as a proxy for "is this scalar a valid const value for this time series"
                                output_type.build_resolution_dict_from_scalar({}, HgTypeMetaData.parse_value(out), out)
                                from hgraph.nodes import const
                                out = const(out, tp=output_type.py_type)
                            except Exception as e:
                                raise WiringError(f"Expected a time series of type '{str(output_type)}' but got '{str(out)}'") from e

                    if not output_type.dereference().matches(out.output_type.dereference()):
                        raise WiringError(f"'{self.signature.name}' declares it's output as '{str(output_type)}' but "
                                          f"'{str(out.output_type)}' was returned from the graph")
                elif WiringGraphContext.is_strict() and not g.has_sink_nodes():
                    raise WiringError(f"'{self.signature.name}' does not seem to do anything")
                return out
