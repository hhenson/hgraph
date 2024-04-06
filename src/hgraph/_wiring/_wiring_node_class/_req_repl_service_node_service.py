from functools import cached_property
from typing import Callable, TypeVar

from frozendict import frozendict

from hgraph import HgTypeMetaData, WiringContext, WiringGraphContext, TSB, ts_schema, TSD, TIME_SERIES_TYPE, WiringPort
from hgraph._wiring._wiring_node_class._service_interface_node_class import ServiceInterfaceNodeClass
from hgraph._wiring._wiring_node_signature import WiringNodeSignature

__all__ = ("RequestReplyServiceNodeClass",)


class RequestReplyServiceNodeClass(ServiceInterfaceNodeClass):
    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        if not signature.defaults.get("path"):
            signature = signature.copy_with(defaults=frozendict(signature.defaults | {"path": None}))
        super().__init__(signature, fn)

    def full_path(self, user_path: str | None) -> str:
        if user_path is None:
            user_path = f"{self.fn.__module__}"
        return f"reqrepl_svc://{user_path}/{self.fn.__name__}"

    def __call__(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **kwargs) -> "WiringPort":

        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            kwargs_, resolved_signature = self._validate_and_resolve_signature(*args,
                                                                               __pre_resolved_types__=__pre_resolved_types__,
                                                                               **kwargs)

            # But graph nodes are evaluated at wiring time, so this is the graph expansion happening here!
            with WiringGraphContext(self.signature) as g:
                full_path = self.full_path(kwargs_.get("path"))
                g.register_service_client(self, full_path)

                from hgraph.nodes._service_utils import _request_service
                from hgraph.nodes._service_utils import _request_reply_service
                from hgraph import TIME_SERIES_TYPE_1

                ts_kwargs = {k: v for k, v in kwargs_.items() if k in resolved_signature.time_series_args}
                if resolved_signature.output_type is not None:
                    return _request_reply_service[TIME_SERIES_TYPE_1: resolved_signature.output_type](
                        path=full_path,
                        request=TSB[ts_schema(**resolved_signature.time_series_inputs)].from_ts(**ts_kwargs))
                else:
                    return _request_service(
                        path=full_path,
                        request=TSB[ts_schema(**resolved_signature.time_series_inputs)].from_ts(**ts_kwargs))

    def wire_impl_inputs_stub(self, path):
        from hgraph.nodes import get_shared_reference_output

        full_path = self.full_path(path)

        request_tps = {}
        requests = {}
        for arg in self.signature.time_series_args:
            interface_tp = self.signature.input_types[arg]
            request_tps[arg] = TSD[int, interface_tp.py_type]
            requests[arg] = get_shared_reference_output[TIME_SERIES_TYPE: request_tps[arg]](f"{full_path}_request_{arg}_out")

        WiringGraphContext.instance().add_built_service_impl(full_path, None)

        return TSB[ts_schema(**request_tps)].from_ts(**requests)

    def wire_impl_out_stub(self, path, out):
        from hgraph.nodes import write_service_replies
        write_service_replies(self.full_path(path), out)

    @cached_property
    def impl_signature(self):
        return self.signature.copy_with(
            input_types=frozendict(
                {k: HgTypeMetaData.parse_type(TSD[int, v]) for k, v in self.signature.time_series_inputs.items()}
                | self.signature.scalar_inputs),
            output_type=HgTypeMetaData.parse_type(TSD[int, self.signature.output_type.py_type]) if self.signature.output_type else None
        )