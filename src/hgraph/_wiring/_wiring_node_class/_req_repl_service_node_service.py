from typing import Callable, TypeVar

from frozendict import frozendict

from hgraph import HgTypeMetaData, WiringContext, WiringGraphContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._service_interface_node_class import ServiceInterfaceNodeClass
from hgraph._wiring._wiring_node_signature import WiringNodeSignature

__all__ = ("RequestReplyServiceNodeClass",)


class RequestReplyServiceNodeClass(ServiceInterfaceNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        if not signature.defaults.get("path"):
            signature = signature.copy_with(defaults=frozendict(signature.defaults | {"path": None}))
        super().__init__(signature, fn)
        if (l := len(signature.time_series_args)) != 1:
            raise CustomMessageWiringError(f"Expected 1 time-series argument, got {l}")

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
                g.register_service_client(self, kwargs_.get("path") or '')

                from hgraph.nodes._service_utils import _request_service
                from hgraph import TIME_SERIES_TYPE_1
                return _request_service[TIME_SERIES_TYPE_1: resolved_signature.output_type](path=self.full_path(kwargs_["path"]),
                                  request=kwargs_[next(iter(resolved_signature.time_series_args))])
