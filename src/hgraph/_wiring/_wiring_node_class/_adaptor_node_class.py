from typing import Callable, TypeVar

from frozendict import frozendict

from hgraph._types._scalar_types import is_keyable_scalar
from hgraph._types._ts_meta_data import HgTSTypeMetaData
from hgraph._types._tsb_type import TSB
from hgraph._types._tss_type import TSS
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext
from hgraph._wiring._wiring_node_class._service_interface_node_class import ServiceInterfaceNodeClass
from hgraph._wiring._wiring_node_class._wiring_node_class import validate_and_resolve_signature
from hgraph._wiring._wiring_node_signature import WiringNodeSignature

__all__ = ("AdaptorNodeClass",)

from hgraph._wiring._wiring_port import _wiring_port_for


class AdaptorNodeClass(ServiceInterfaceNodeClass):
    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        if not signature.defaults.get("path"):
            signature = signature.copy_with(defaults=frozendict(signature.defaults | {"path": None}))
        super().__init__(signature, fn)

    def full_path(self, user_path: str | None) -> str:
        if user_path is None:
            user_path = f"{self.fn.__module__}"
        return f"adaptor://{user_path}/{self.fn.__name__}"

    def __call__(
        self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **kwargs
    ) -> "WiringPort":

        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            kwargs_, resolved_signature, resolution_dict = validate_and_resolve_signature(
                self.signature, *args, __pre_resolved_types__=__pre_resolved_types__, **kwargs
            )

            with WiringGraphContext(self.signature) as g:
                inputs = {k: v for k, v in kwargs_.items() if k in resolved_signature.time_series_inputs}
                scalars = {k: v for k, v in kwargs_.items() if k in resolved_signature.scalar_inputs and k != "path"}

                resolution_dict |= scalars

                from_graph_path = self.typed_full_path(kwargs_.get("path") + "/from_graph", resolution_dict)
                to_graph_path = self.typed_full_path(kwargs_.get("path") + "/to_graph", resolution_dict)

                from hgraph import TIME_SERIES_TYPE, TSD, combine
                from hgraph.nodes import capture_output_to_global_state, get_shared_reference_output

                inputs_from_graph = combine(**inputs)
                client = capture_output_to_global_state(from_graph_path, inputs_from_graph, __return_sink_wp__=True)
                g.register_service_stub(self, from_graph_path, client.node_instance)

                out = get_shared_reference_output[resolved_signature.output_type](to_graph_path)
                g.register_service_client(self, to_graph_path, resolution_dict, out.node_instance)
                return out

    def wire_impl_inputs_stub(self, path, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None):
        from hgraph.nodes import get_shared_reference_output
        from hgraph import last_value_source_node, ts_schema

        path = self.path_from_full_path(path) if self.is_full_path(path) else path

        resolution_dict = self.signature.try_build_resolution_dict(__pre_resolved_types__)
        from_graph_path = self.typed_full_path(path + "/from_graph", resolution_dict)
        from_graph_type = TSB[ts_schema(**self.signature.time_series_inputs)]
        from_graph = get_shared_reference_output[from_graph_type](from_graph_path)

        WiringGraphContext.instance().add_built_service_impl(from_graph_path, from_graph.node_instance)
        return from_graph

    def wire_impl_out_stub(self, path, out, __pre_resolved_types__=None):
        from hgraph.nodes import capture_output_to_global_state

        path = self.path_from_full_path(path) if self.is_full_path(path) else path

        to_graph_path = self.typed_full_path(
            path + "/to_graph", self.signature.try_build_resolution_dict(__pre_resolved_types__)
        )
        capture_output_to_global_state(to_graph_path, out)
        WiringGraphContext.instance().add_built_service_impl(to_graph_path, out.node_instance)

    def register_impl(
        self, path: str, impl: "NodeBuilder", __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None, **kwargs
    ):
        from hgraph import register_service

        register_service(
            self.full_path(path + "/from_graph"),
            impl,
            self.signature.try_build_resolution_dict(__pre_resolved_types__),
            **kwargs,
        )
        register_service(
            self.full_path(path + "/to_graph"),
            impl,
            self.signature.try_build_resolution_dict(__pre_resolved_types__),
            **kwargs,
        )
