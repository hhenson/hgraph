from typing import Callable, Sequence, TypeVar

from frozendict import frozendict

from hgraph._types._scalar_type_meta_data import HgAtomicType
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext, GraphWiringNodeClass
from hgraph._wiring._wiring_node_class._service_interface_node_class import validate_signature_against_impl_signature
from hgraph._wiring._wiring_node_class._wiring_node_class import (
    WiringNodeClass,
    validate_and_resolve_signature,
)
from hgraph._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType

__all__ = ("AdaptorImplNodeClass",)


class AdaptorImplNodeClass(GraphWiringNodeClass):
    wire_service_outputs_directly = True

    def __init__(self, signature: WiringNodeSignature, fn: Callable, interfaces=None):
        self.implementation_graph = GraphWiringNodeClass(signature, fn)

        # Public signature has "path" and only scalar inputs
        has_path = "path" in signature.args
        super().__init__(
            signature.copy_with(
                args=(("path",) if not has_path else ()) + tuple(signature.scalar_inputs.keys()),
                input_types=frozendict(dict(signature.scalar_inputs) | {"path": HgAtomicType.parse_type(str)}),
                time_series_args=tuple(),
            ),
            fn,
        )
        if interfaces is None:
            raise CustomMessageWiringError("No interfaces provided")
        self.interfaces = (interfaces,) if not isinstance(interfaces, Sequence) else interfaces

        # Ensure the service impl signature is valid given the signature definitions of the interfaces.
        self.validate_signature_vs_interfaces(signature, fn, self.interfaces)

    def _validate_service_not_already_bound(
        self, path: str | None, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None
    ):
        if WiringGraphContext.instance().is_service_built(path, __pre_resolved_types__):
            raise CustomMessageWiringError(
                f"This path: '{path}' has already been registered for this service implementation"
            )

    def __call__(
        self,
        *args,
        __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None,
        __interface__: WiringNodeSignature = None,
        **kwargs,
    ) -> "WiringPort":
        with WiringContext(
            current_wiring_node=self,
            current_signature=self.signature,
            wire_service_outputs_directly=self.wire_service_outputs_directly,
        ):
            path = kwargs.get("path")
            pre_resolved_types_all = __pre_resolved_types__ or {}

            from hgraph import AdaptorNodeClass

            __interface__: AdaptorNodeClass

            if self.interfaces != ():
                if __interface__ is None:
                    __interface__ = self.interfaces[0]

                if not __interface__.is_full_path(path):
                    full_path = __interface__.full_path(path)
                else:
                    full_path = path
                    path = __interface__.path_from_full_path(full_path)
            else:
                full_path = path

            path = path.replace("/from_graph", "").replace("/to_graph", "")
            scalars = {k: v for k, v in pre_resolved_types_all.items() if k in self.signature.scalar_inputs}
            pre_resolved_types = {k: v for k, v in pre_resolved_types_all.items() if k not in self.signature.scalar_inputs}

            kwargs["path"] = path

            kwargs_, resolved_signature, resolution_dict = validate_and_resolve_signature(
                self.signature, *args, __pre_resolved_types__=pre_resolved_types, **(kwargs | scalars)
            )

            if len(self.interfaces) == 1:
                interface_resolution_dict = {k: v for k, v in resolution_dict.items() if k in __interface__.signature.type_vars}
                full_typed_path = __interface__.typed_full_path(full_path, interface_resolution_dict | scalars)
                self._validate_service_not_already_bound(full_path, interface_resolution_dict | scalars)

                from_graph = None
                from_graph_kwargs = {}
                if __interface__.signature.time_series_inputs:
                    with WiringGraphContext(node_signature=resolved_signature):
                        from_graph = __interface__.wire_impl_inputs_stub(path, interface_resolution_dict, **scalars)
                        from_graph_kwargs = from_graph.as_dict()

                to_graph = self.implementation_graph.__call__(
                    __pre_resolved_types__=resolution_dict, **kwargs_, **from_graph_kwargs
                )
                if to_graph is not None:
                    with WiringGraphContext(node_signature=resolved_signature):
                        __interface__.wire_impl_out_stub(path, to_graph, interface_resolution_dict, **scalars)

                built_services = WiringGraphContext.instance().built_services()
                if built_services.get(full_typed_path) is None:
                    anchor_node = getattr(from_graph, "node_instance", None) or getattr(to_graph, "node_instance", None)
                    WiringGraphContext.instance().add_built_service_impl(full_typed_path, anchor_node)
            else:  # multiadaptor/multiservice implementations use the interface stub APIs to wire up the service
                self._validate_service_not_already_bound(full_path, pre_resolved_types_all)
                with WiringGraphContext(node_signature=resolved_signature):
                    self.implementation_graph.__call__(__pre_resolved_types__=resolution_dict, **kwargs_)

    def __eq__(self, other):
        return super().__eq__(other) and self.interfaces == other.interfaces

    def __hash__(self):
        return super().__hash__() ^ hash(self.interfaces)

    def validate_signature_vs_interfaces(
        self, signature: WiringNodeSignature, fn: Callable, interfaces: Sequence[WiringNodeClass]
    ) -> WiringNodeSignature:
        if len(interfaces) == 1:
            validate_signature_against_impl_signature(signature, interfaces[0])
        else:
            pass  # multiservice/multiadaptor implementations use the interface stub APIs to wire up the service so checking happens there
