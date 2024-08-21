from typing import Callable, TypeVar

from frozendict import frozendict

from hgraph._wiring._wiring_context import WiringContext
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._tsb_type import TSB, ts_schema
from hgraph._types._tsd_type import TSD
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext
from hgraph._wiring._wiring_node_class._service_interface_node_class import ServiceInterfaceNodeClass
from hgraph._wiring._wiring_node_class._wiring_node_class import validate_and_resolve_signature
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_port import WiringPort

__all__ = ("RequestReplyServiceNodeClass",)


class RequestReplyServiceNodeClass(ServiceInterfaceNodeClass):
    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        if not signature.defaults.get("path"):
            signature = signature.copy_with(defaults=frozendict(signature.defaults | {"path": None}))
        super().__init__(signature, fn)

    def full_path(self, user_path: str | None) -> str:
        if user_path is None:
            user_path = self.default_path()
        return f"reqrepl_svc://{user_path}/{self.fn.__name__}"

    def __call__(
        self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **kwargs
    ) -> "WiringPort":

        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            kwargs_, resolved_signature, resolution_dict = validate_and_resolve_signature(
                self.signature, *args, __pre_resolved_types__=__pre_resolved_types__, **kwargs
            )

            # But graph nodes are evaluated at wiring time, so this is the graph expansion happening here!
            with WiringGraphContext(self.signature) as g:
                path = kwargs_.get("path") or self.default_path()
                full_path = self.full_path(path)
                typed_full_path = self.typed_full_path(path, resolution_dict)

                from hgraph.nodes._service_utils import _request_service
                from hgraph.nodes._service_utils import _request_reply_service
                from hgraph import TIME_SERIES_TYPE_1

                ts_kwargs = {k: v for k, v in kwargs_.items() if k in resolved_signature.time_series_args}
                if resolved_signature.output_type is not None:
                    port = _request_reply_service[TIME_SERIES_TYPE_1 : resolved_signature.output_type](
                        path=typed_full_path,
                        request=TSB[ts_schema(**resolved_signature.time_series_inputs)].from_ts(**ts_kwargs),
                    )
                else:
                    port = _request_service(
                        path=typed_full_path,
                        request=TSB[ts_schema(**resolved_signature.time_series_inputs)].from_ts(**ts_kwargs),
                    )

                g.register_service_client(self, full_path, resolution_dict or None, port.node_instance)
                return port

    def wire_impl_inputs_stub(self, path, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None):
        from hgraph.nodes import get_shared_reference_output

        typed_full_path = self.typed_full_path(path, self.signature.try_build_resolution_dict(__pre_resolved_types__))

        request_tps = {}
        requests = {}
        for arg in self.signature.time_series_args:
            interface_tp = self.signature.input_types[arg].resolve(resolution_dict=__pre_resolved_types__)
            request_tps[arg] = TSD[int, interface_tp.py_type]
            requests[arg] = get_shared_reference_output[TIME_SERIES_TYPE : request_tps[arg]](
                f"{typed_full_path}/request_{arg}_out"
            )

        WiringGraphContext.instance().add_built_service_impl(typed_full_path, None)

        return TSB[ts_schema(**request_tps)].from_ts(**requests)

    def wire_impl_out_stub(self, path, out, __pre_resolved_types__=None):
        from hgraph.nodes import write_service_replies

        write_service_replies(
            self.typed_full_path(path, self.signature.try_build_resolution_dict(__pre_resolved_types__)), out
        )

    def impl_signature(self, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None):
        resolution_dict = self.signature.try_build_resolution_dict(__pre_resolved_types__)
        return self.signature.copy_with(
            input_types=frozendict(
                {
                    k: HgTypeMetaData.parse_type(TSD[int, v.resolve(resolution_dict)])
                    for k, v in self.signature.time_series_inputs.items()
                }
                | self.signature.scalar_inputs
            ),
            output_type=(
                HgTypeMetaData.parse_type(TSD[int, self.signature.output_type.resolve(resolution_dict).py_type])
                if self.signature.output_type
                else None
            ),
        )
