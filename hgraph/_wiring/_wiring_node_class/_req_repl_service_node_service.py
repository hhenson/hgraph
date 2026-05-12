from typing import Callable, TypeVar

from frozendict import frozendict

from hgraph._types._scalar_type_meta_data import HgAtomicType
from hgraph._wiring._wiring_context import WiringContext
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._tsb_type import TSB, ts_schema
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
from hgraph._types._tsd_type import TSD
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext
from hgraph._wiring._wiring_node_class._pull_source_node_class import last_value_source_node
from hgraph._wiring._wiring_node_class._service_interface_node_class import ServiceInterfaceNodeClass
from hgraph._wiring._wiring_node_class._wiring_node_class import validate_and_resolve_signature
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_port import WiringPort, _wiring_port_for

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

    def wire_outside_stubs(
        self,
        path,
        __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None,
        wire_outputs: bool = True,
        **scalars,
    ):
        from hgraph.nodes import capture_output_node_to_global_state, capture_output_to_global_state

        path = self.path_from_full_path(path) if self.is_full_path(path) else path
        typed_full_path = self.typed_full_path(path, self.signature.try_build_resolution_dict(__pre_resolved_types__))
        impl_signature = self.impl_signature(__pre_resolved_types__)

        for arg in impl_signature.time_series_args:
            tp = impl_signature.input_types[arg]
            request_node = last_value_source_node(f"{typed_full_path}/request_{arg}", tp.resolve(__pre_resolved_types__))
            request = _wiring_port_for(tp, request_node, tuple())
            capture_output_node_to_global_state(f"{typed_full_path}/request_{arg}", request)
            capture_output_to_global_state(f"{typed_full_path}/request_{arg}_out", request)
            WiringGraphContext.instance().register_service_stub(self, typed_full_path, request_node)

        if wire_outputs and impl_signature.output_type is not None:
            replies_node = last_value_source_node(
                f"{typed_full_path}/replies_fb", impl_signature.output_type.resolve(__pre_resolved_types__)
            )
            replies = _wiring_port_for(impl_signature.output_type, replies_node, tuple())
            capture_output_node_to_global_state(f"{typed_full_path}/replies_fb", replies)
            capture_output_to_global_state(f"{typed_full_path}/replies", replies)
            WiringGraphContext.instance().register_service_stub(self, typed_full_path, replies_node)

    def wire_impl_inputs_stub(
        self, path, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **scalars
    ):
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

    def wire_impl_out_stub(self, path, out, __pre_resolved_types__=None, **scalars):
        from hgraph._wiring._wiring_context import WIRING_CONTEXT

        typed_full_path = self.typed_full_path(path, self.signature.try_build_resolution_dict(__pre_resolved_types__))
        if WIRING_CONTEXT.wire_service_outputs_directly:
            from hgraph.nodes import capture_output_to_global_state

            capture_output_to_global_state(f"{typed_full_path}/replies", out)
        else:
            from hgraph.nodes import write_service_replies

            write_service_replies(typed_full_path, out)

    def impl_signature(self, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None):
        resolution_dict = self.signature.try_build_resolution_dict(__pre_resolved_types__)
        requestor_id_tp = HgAtomicType.parse_type(int)
        return self.signature.copy_with(
            input_types=frozendict(
                {
                    k: HgTSDTypeMetaData(requestor_id_tp, v.resolve(resolution_dict, weak=True))
                    for k, v in self.signature.time_series_inputs.items()
                }
                | self.signature.scalar_inputs
            ),
            output_type=(
                HgTSDTypeMetaData(requestor_id_tp, self.signature.output_type.resolve(resolution_dict, weak=True))
                if self.signature.output_type
                else None
            ),
        )
