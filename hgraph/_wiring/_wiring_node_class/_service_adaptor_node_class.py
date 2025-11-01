from typing import Callable, TypeVar

from frozendict import frozendict

from hgraph import TS
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._tsd_type import TSD
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext
from hgraph._wiring._wiring_node_class._service_interface_node_class import ServiceInterfaceNodeClass
from hgraph._wiring._wiring_node_class._wiring_node_class import validate_and_resolve_signature
from hgraph._wiring._wiring_node_signature import WiringNodeSignature

__all__ = ("ServiceAdaptorNodeClass",)

from hgraph.nodes._service_utils import (
    write_adaptor_request,
    adaptor_request,
    capture_output_node_to_global_state,
    request_id,
)


class ServiceAdaptorNodeClass(ServiceInterfaceNodeClass):
    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        if not signature.defaults.get("path"):
            signature = signature.copy_with(defaults=frozendict(signature.defaults | {"path": None}))
        super().__init__(signature, fn)

    def full_path(self, user_path: str | None) -> str:
        if user_path is None:
            user_path = self.default_path()
        return f"svc_adaptor://{user_path}/{self.fn.__name__}"

    def __call__(
        self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **kwargs
    ) -> "WiringPort":
        id = self.from_graph(*args, __pre_resolved_types__=__pre_resolved_types__, **kwargs)
        return self.to_graph(*args, __request_id__=id, __pre_resolved_types__=__pre_resolved_types__, **kwargs)

    def from_graph(
        self,
        *args,
        __request_id__: TS[int] = None,
        __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None,
        **kwargs,
    ) -> "WiringPort":

        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            kwargs_, resolved_signature, resolution_dict = validate_and_resolve_signature(
                self.signature,
                *args,
                __pre_resolved_types__=__pre_resolved_types__,
                **kwargs,
            )

            with WiringGraphContext(self.signature) as g:
                inputs = {k: v for k, v in kwargs_.items() if k in resolved_signature.time_series_inputs}
                scalars = {k: v for k, v in kwargs_.items() if k in resolved_signature.scalar_inputs and k != "path"}

                resolution_dict |= scalars

                path = kwargs_.get("path") or self.default_path()
                from_graph_full_path = self.full_path(path + "/from_graph")
                from_graph_typed_path = self.typed_full_path(from_graph_full_path, resolution_dict)

                id = __request_id__ or request_id(hash((path, frozendict(inputs), frozendict(scalars))))
                for k, v in inputs.items():
                    if v is not None:
                        client = write_adaptor_request(
                            from_graph_typed_path, k, v, requestor_id=id, __return_sink_wp__=True
                        )
                        g.register_service_client(
                            self, from_graph_full_path, resolution_dict, client.node_instance, receive=False
                        )

                return id

    def to_graph(
        self,
        *args,
        __request_id__: TS[int],
        __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None,
        __no_ts_inputs__: bool = False,
        **kwargs,
    ) -> "WiringPort":
        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            kwargs_, resolved_signature, resolution_dict = validate_and_resolve_signature(
                (
                    self.signature.copy_with(
                        args=tuple(k for k in self.signature.args if self.signature.input_types[k].is_scalar),
                        input_types={k: v for k, v in self.signature.input_types.items() if v.is_scalar},
                        time_series_args=set(),
                    )
                    if __no_ts_inputs__
                    else self.signature
                ),
                *args,
                __pre_resolved_types__=__pre_resolved_types__,
                **kwargs,
            )

            with WiringGraphContext(self.signature) as g:
                scalars = {k: v for k, v in kwargs_.items() if k in resolved_signature.scalar_inputs and k != "path"}
                resolution_dict |= scalars

                path = kwargs_.get("path") or self.default_path()
                to_graph_full_path = self.full_path(path + "/to_graph")
                to_graph_typed_path = self.typed_full_path(to_graph_full_path, resolution_dict)

                from hgraph.nodes import get_shared_reference_output

                id = __request_id__
                if resolved_signature.output_type is not None:
                    out = get_shared_reference_output[TSD[int, resolved_signature.output_type]](to_graph_typed_path)
                    g.register_service_client(self, to_graph_full_path, resolution_dict, out.node_instance)
                    return out[id]

    def wire_impl_inputs_stub(
        self, path, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **scalars
    ):
        path = self.path_from_full_path(path) if self.is_full_path(path) else path
        resolution_dict = self.signature.try_build_resolution_dict(__pre_resolved_types__)
        from_graph_path = self.typed_full_path(path + "/from_graph", resolution_dict | scalars)

        from hgraph import create_input_stub, TS

        service_top = create_input_stub(from_graph_path, HgTypeMetaData.parse_type(TS[bool]), False)
        WiringGraphContext.instance().add_built_service_impl(from_graph_path, service_top.node_instance)

        from_graph = {}
        for k, t in self.signature.time_series_inputs.items():
            from_graph[k] = adaptor_request[TIME_SERIES_TYPE : t.resolve(resolution_dict)](from_graph_path, k)
            from_graph[k].node_instance.add_indirect_dependency(service_top.node_instance)
            capture_output_node_to_global_state(f"{from_graph_path}/{k}", from_graph[k])

        from hgraph import combine

        return combine(**from_graph)

    def wire_impl_out_stub(self, path, out, __pre_resolved_types__=None, **scalars):
        from hgraph.nodes import capture_output_to_global_state

        if out is None:
            return

        path = self.path_from_full_path(path) if self.is_full_path(path) else path

        to_graph_path = self.typed_full_path(
            path + "/to_graph", self.signature.try_build_resolution_dict(__pre_resolved_types__) | scalars
        )
        bottom = capture_output_to_global_state(to_graph_path, out, __return_sink_wp__=True)
        WiringGraphContext.instance().add_built_service_impl(to_graph_path, bottom.node_instance)

    def register_impl(
        self, path: str, impl: "NodeBuilder", __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None, **kwargs
    ):
        from hgraph import register_service

        if path:
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
        else:
            assert (
                not __pre_resolved_types__
            ), "Type parameters for general adaptor registrations (with no path) are not implemented"
            register_service(None, impl, None, **kwargs)
