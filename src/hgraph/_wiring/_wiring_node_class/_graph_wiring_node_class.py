from contextlib import ExitStack, nullcontext
import inspect
import sys
import types
from copy import copy
from graphlib import TopologicalSorter
from typing import Optional, TypeVar, Callable, Tuple, Dict

from frozendict import frozendict

from hgraph._types._ts_type_var_meta_data import HgTsTypeVarTypeMetaData
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._source_code_details import SourceCodeDetails
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import WiringError, CustomMessageWiringError
from hgraph._wiring._wiring_node_class._service_interface_node_class import ServiceInterfaceNodeClass
from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext
from hgraph._wiring._wiring_node_class._wiring_node_class import (
    BaseWiringNodeClass,
    PreResolvedWiringNodeWrapper,
    validate_and_resolve_signature,
)
from hgraph._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType
from hgraph._wiring._wiring_observer import WiringObserverContext
from hgraph._wiring._wiring_port import WiringPort

__all__ = ("WiringGraphContext", "GraphWiringNodeClass")


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
        return [
            graph.wiring_node_signature.src_location
            for graph in reversed(cls.__stack__[1:])
            if graph.wiring_node_signature
        ]

    @classmethod
    def wiring_path_name(cls) -> str:
        return cls.__stack__[-1]._wiring_path_name if WiringGraphContext.__stack__ else ""

    @property
    def _wiring_path_name(self) -> str:
        """Return a graph call stack in names of graphs"""

        path = ".".join(
            graph.wiring_node_signature.name
            + (":" + graph.wiring_node_signature.label if graph.wiring_node_signature.label else "")
            for i, graph in enumerate(self.__stack__[1:])
            if graph.wiring_node_signature
        )

        self.__dict__["_wiring_path_name"] = path
        return path

    @classmethod
    def instance(cls) -> "WiringGraphContext":
        return WiringGraphContext.__stack__[-1] if WiringGraphContext.__stack__ else None

    def __init__(self, node_signature: Optional[WiringNodeSignature] = None, temporary: bool = False):
        """
        If we are wiring the root graph, then there is no wiring node. In this case None is
        passed in.
        """
        self._wiring_node_signature: WiringNodeSignature = node_signature
        self._temporary = temporary

        self._sink_nodes: list["WiringNodeInstance"] = []
        self._other_nodes: list[Tuple["WiringPort", dict]] = []
        self._service_clients: list[
            Tuple[
                "WiringNodeClass",
                str,
                dict[TypeVar, HgTypeMetaData],
                "WiringNodeInstance",
                bool,
            ]
        ] = []
        self._service_stubs: list[
            Tuple["WiringNodeClass", str, dict[TypeVar, HgTypeMetaData]], "WiringNodeInstance"
        ] = []
        self._context_clients: list[Tuple[str, int, "WiringNOdeInstance"]] = []
        self._service_implementations: Dict[str, Tuple["WiringNodeClass", "ServiceImplNodeClass", dict]] = {}
        self._built_services = {}
        self._service_build_contexts = []

        self._current_frame = sys._getframe(1)

    @property
    def wiring_node_signature(self) -> WiringNodeSignature:
        return self._wiring_node_signature

    def add_sink_node(self, node: "WiringNodeInstance"):
        self._sink_nodes.append(node)

    def has_sink_nodes(self) -> bool:
        return bool(self._sink_nodes)

    @property
    def sink_nodes(self) -> tuple["WiringNodeInstance", ...]:
        return tuple(self._sink_nodes)

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
        try:
            while self._current_frame != (f := sys._getframe(i)):
                if i > 10:
                    return
                prev_f = f
                i += 1
        except ValueError:
            return

        self._other_nodes.append((node, prev_f.f_locals))

    def label_nodes(self):
        """
        Label the nodes in the graph with the variable name if the output was assigned to a local variable
        """
        for port, locals in self._other_nodes:
            varname = next((k for k, v in locals.items() if v is port), None)
            if varname and port.path == ():
                port.node_instance.label = varname

    def register_service_client(
        self,
        service: "ServiceInterfaceNodeClass",
        path: str,
        type_map: dict = None,
        node: "WiringNodeInstance" = None,
        receive=True,
    ):
        """
        Register a service client with the graph context
        """
        self._service_clients.append(
            (service, path, (frozendict(type_map) if type_map else frozendict()), node, receive)
        )

    def register_service_stub(
        self, service: "ServiceInterfaceNodeClass", full_typed_path: str, node: "WiringNodeInstance" = None
    ):
        """
        Register a service stub with the graph context
        """
        self._service_stubs.append((service, full_typed_path, node))

    def register_service_impl(
        self,
        service: "ServiceInterfaceNodeClass",
        path: str,
        impl: "ServiceImplNodeClass",
        kwargs,
        resolution_dict=None,
    ):
        """
        Register a service client with the graph context
        """
        if service is not None:
            if path is None:
                path = service.default_path()

            if not service.is_full_path(path):
                path = service.full_path(path)

        if resolution_dict:
            path = f"{path}[{ServiceInterfaceNodeClass._resolution_dict_to_str(resolution_dict)}]"

        if (prev_impl := self._service_implementations.get(path)) and prev_impl is not None and prev_impl[1] != impl:
            CustomMessageWiringError(
                f"Service implementation already registered for service at path: {path}: "
                f"{prev_impl[0].signature.signature} with {prev_impl[1]}"
            )

        self._service_implementations[path] = (service, impl, kwargs)

    def find_service_impl(self, path, service=None, resolution_dict=None, quiet=False):
        if path.endswith("]"):
            typed_path = path
            path = path.split("[", 1)[0]
        elif resolution_dict:
            typed_path = f"{path}[{ServiceInterfaceNodeClass._resolution_dict_to_str(resolution_dict)}]"
        else:
            typed_path = None

        def find_impl(path):
            for c in WiringGraphContext.__stack__:
                if item := c._service_implementations.get(path):
                    return item

        if typed_path and (item := find_impl(typed_path)):
            return item
        elif item := find_impl(path):
            return item
        elif service and (item := find_impl(service.full_path(None))):
            return item
        else:
            if not quiet:
                raise CustomMessageWiringError(f"No service implementation found for path: {typed_path or path}")

    def add_built_service_impl(self, path, node):
        if node:
            self.add_sink_node(node)

        self._built_services[path] = node

    def built_services(self):
        return self._built_services

    def is_service_built(self, path, resolution_dict=None):
        if resolution_dict:
            path = f"{path}[{ServiceInterfaceNodeClass._resolution_dict_to_str(resolution_dict)}]"

        return self._built_services.get(path)

    def register_context_client(self, path, depth, node):
        self._context_clients.append((path, depth, node))

    def reassign_context_clients(self, clients, node):
        self._context_clients = [(path, depth, node) for path, depth, _ in clients]

    def remove_context_clients(self, path, depth):
        clients = [node for c_path, c_depth, node in self._context_clients if c_path == path and c_depth == depth]
        self._context_clients = [
            (c_path, c_depth, node)
            for c_path, c_depth, node in self._context_clients
            if not (c_path == path and c_depth == depth)
        ]
        return clients

    def pop_context_clients(self):
        r = self._context_clients
        self._context_clients = []
        return r

    def reassign_service_clients(self, clients, node):
        self._service_clients.extend(
            [(service, path, type_map, node, receive) for service, path, type_map, _, receive in clients]
        )

    def pop_service_clients(self):
        r = self._service_clients
        self._service_clients = []
        return r

    def reassign_service_stubs(self, stubs, node):
        self._service_stubs.extend([(service, full_typed_path, node) for service, full_typed_path, _ in stubs])

    def pop_service_stubs(self):
        r = self._service_stubs
        self._service_stubs = []
        return r

    def pop_reassignable_items(self):
        return self.pop_service_clients(), self.pop_service_stubs(), self.pop_context_clients()

    def reassign_items(self, items, node):
        sc, ss, cc = items
        self.reassign_service_clients(sc, node)
        self.reassign_service_stubs(ss, node)
        self.reassign_context_clients(cc, node)

    def registered_service_clients(self, service):
        return tuple(
            (path, type_map, node, receive)
            for s, path, type_map, node, receive in self._service_clients
            if s == service
        )

    def add_service_build_context(self, context, name=None):
        assert (
            WiringNodeInstanceContext.instance() is WiringNodeInstanceContext.__stack__[0]
        ), f"Service build context must be in the top level graph (i.e. not nested). {context} does not appear to be"
        assert not any(
            n == name for _, n in self._service_build_contexts
        ), f"Service build context with name {name} already exists in the graph"
        self._service_build_contexts.append((context, name))

    def _build_service(self, impl, **kwargs):
        with ExitStack() if self._service_build_contexts else nullcontext() as stack:
            for c, n in self._service_build_contexts:
                if n:
                    exec(f"{n} = c")
                c.__enter__()
                stack.push(c)

            impl(**kwargs)

    def build_services(self):
        """
        Build the service implementations for the graph
        """
        service_clients = [(service, path, type_map) for service, path, type_map, _, _ in self._service_clients]
        service_full_paths = {}
        catch_all_services_processed = set()
        loop_count = 0
        while True:
            while True:
                # these are catch-all services and adaptors that will figure out if they need to wire anything by themselves
                for path, impl, kwargs in (
                    (p, i, kw) for p, (s, i, kw) in self._service_implementations.items() if s is None
                ):
                    if path not in self._built_services:
                        self._build_service(impl, path=path, __pre_resolved_types__={}, **kwargs)
                        self.add_built_service_impl(path, None)

                # Now build 'normal' services
                services_to_build = dict()
                for service, path, type_map in set(service_clients):
                    typed_path = service.typed_full_path(path, type_map)
                    if typed_path in self._built_services:
                        service_full_paths[(service, path, type_map)] = typed_path
                        continue

                    if item := self.find_service_impl(path, service, type_map, quiet=True):
                        interface, impl, kwargs = item
                    else:
                        clients = [
                            node.wiring_path_name
                            for s, p, t, node, _ in self._service_clients
                            if s == service and p == path and t == type_map
                        ]
                        raise CustomMessageWiringError(
                            f"No implementation found for service: {service.signature.name} at path: {path} requested"
                            f" by {clients}"
                        )

                    if isinstance(interface, PreResolvedWiringNodeWrapper):
                        interface = interface.underlying_node

                    if interface != service:
                        raise CustomMessageWiringError(
                            f"service implementation for path {path} implements "
                            f"{interface.signature.signature} which does not match the client "
                            f"expecting {service.signature.signature}"
                        )

                    services_to_build[typed_path] = (service, path, impl, kwargs, type_map)
                    service_full_paths[(service, path, type_map)] = typed_path

                for typed_path in services_to_build:
                    if typed_path not in self._built_services:
                        service, path, impl, kwargs, type_map = services_to_build[typed_path]
                        self._build_service(
                            impl, path=path, __interface__=service, __pre_resolved_types__=type_map, **kwargs
                        )

                if len(self._service_clients) == len(service_clients):
                    break
                else:
                    service_clients = [
                        (service, path, type_map) for service, path, type_map, _, _ in self._service_clients
                    ]

            if len(self._service_clients) == len(service_clients):
                break
            else:
                service_clients = [(service, path, type_map) for service, path, type_map, _, _ in self._service_clients]

        for service, path, type_map, node, receive in self._service_clients:
            if service.signature.node_type != WiringNodeType.REQ_REP_SVC:
                node: "WiringNodeInstance"

                full_typed_path = service_full_paths[(service, path, type_map)]
                if receive is False:
                    self._built_services[full_typed_path].add_indirect_dependency(node)
                elif node.is_source_node:
                    node.add_indirect_dependency(self._built_services[full_typed_path])
                else:
                    node.add_indirect_dependency(self._built_services[full_typed_path])

        for service, full_path, node in self._service_stubs:
            node: "WiringNodeInstance"
            self._built_services[full_path].add_indirect_dependency(node)

    def __enter__(self):
        WiringGraphContext.__stack__.append(self)

        if self._wiring_node_signature:
            WiringObserverContext.instance().notify_enter_graph_wiring(self._wiring_node_signature)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._wiring_node_signature:
            WiringObserverContext.instance().notify_exit_graph_wiring(self._wiring_node_signature, exc_val)

        assert WiringGraphContext.__stack__.pop() is self
        if not self._temporary and WiringGraphContext.__stack__:
            # For now lets bubble the sink nodes up.
            # It may be useful to track the sink nodes in the graph they are produced.
            # The alternative would be to track them only on the root node.
            WiringGraphContext.__stack__[-1]._sink_nodes.extend(self._sink_nodes)
            WiringGraphContext.__stack__[-1]._service_clients.extend(self._service_clients)
            WiringGraphContext.__stack__[-1]._service_stubs.extend(self._service_stubs)
            WiringGraphContext.__stack__[-1]._context_clients.extend(self._context_clients)
            WiringGraphContext.__stack__[-1]._service_implementations.update(self._service_implementations)
            WiringGraphContext.__stack__[-1]._built_services.update(self._built_services)

            for c, n in self._service_build_contexts:
                WiringGraphContext.__stack__[-1].add_service_build_context(c, n)

        del self._current_frame
        del self._other_nodes


class GraphWiringNodeClass(BaseWiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        if signature.var_arg or signature.var_kwarg:
            co = fn.__code__
            kw_only_code = co.replace(
                co_flags=co.co_flags & ~(inspect.CO_VARARGS | inspect.CO_VARKEYWORDS),
                co_argcount=0,
                co_posonlyargcount=0,
                co_kwonlyargcount=len(signature.args),
            )
            fn = types.FunctionType(
                kw_only_code, fn.__globals__, name=fn.__name__, argdefs=fn.__defaults__, closure=fn.__closure__
            )

        super().__init__(signature, fn)

    def __call__(
        self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **kwargs
    ) -> "WiringPort":

        if self.signature.deprecated:
            import warnings

            warnings.warn(
                f"{self.signature.signature} is deprecated and will be removed in a future version."
                f"{(' ' + self.signature.deprecated) if type(self.signature.deprecated) is str else ''}",
                DeprecationWarning,
                stacklevel=3,
            )

        kwargs.pop("__return_sink_wp__", None)  # not applicable to graphs

        # We don't want graph and node signatures to operate under different rules as this would make
        # moving between node and graph implementations problematic, so resolution rules of the signature
        # hold
        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            kwargs_, resolved_signature, _ = validate_and_resolve_signature(
                self.signature,
                *args,
                __pre_resolved_types__=__pre_resolved_types__,
                __enforce_output_type__=False,
                **kwargs,
            )

            # But graph nodes are evaluated at wiring time, so this is the graph expansion happening here!
            with WiringGraphContext(resolved_signature) as g:
                out: WiringPort = self.fn(**kwargs_)
                WiringGraphContext.instance().label_nodes()
                if output_type := resolved_signature.output_type:
                    from hgraph import HgTSBTypeMetaData

                    if not isinstance(out, WiringPort):
                        if isinstance(out, dict) and isinstance(output_type, HgTSBTypeMetaData):
                            out = output_type.py_type.from_ts(**out)
                        elif isinstance(out, dict) and isinstance(output_type, HgTsTypeVarTypeMetaData):
                            for c in output_type.constraints:
                                if isinstance(c, HgTSBTypeMetaData) and all(
                                    (t := c.bundle_schema_tp.meta_data_schema.get(k))
                                    and t.matches(v.output_type.dereference())
                                    for k, v in out.items()
                                ):
                                    out = c.py_type.from_ts(**out)
                                    break
                            else:
                                raise WiringError(
                                    f"Expected a time series of type '{str(output_type)}' but got a dict of "
                                    f"{{{', '.join(f'{k}:{str(v.output_type)}' for k, v in out.items())}}}"
                                )
                        else:
                            try:
                                # use build resolution dict from scalar as a proxy for "is this scalar a valid const value for this time series"
                                output_type.build_resolution_dict_from_scalar({}, HgTypeMetaData.parse_value(out), out)
                                from hgraph import const

                                out = const(out, tp=output_type.py_type)
                            except Exception as e:
                                raise WiringError(
                                    f"{self.signature} was expected to return a time series of type"
                                    f" '{str(output_type)}' but returned '{str(out)}'"
                                ) from e

                    if not output_type.dereference().matches(out.output_type.dereference()):
                        raise WiringError(
                            f"'{self.signature.name}' declares its output as '{str(output_type)}' but "
                            f"'{str(out.output_type)}' was returned from the graph"
                        )
                elif WiringGraphContext.is_strict() and not g.has_sink_nodes():
                    raise WiringError(f"'{self.signature.name}' does not seem to do anything")
                return out
