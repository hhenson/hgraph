from typing import Callable, TypeVar

from frozendict import frozendict

from hgraph import HgTypeMetaData, WiringContext, WiringGraphContext, TSS, TSB
from hgraph._types._scalar_types import is_keyable_scalar
from hgraph._types._ts_meta_data import HgTSTypeMetaData
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._service_interface_node_class import ServiceInterfaceNodeClass
from hgraph._wiring._wiring_node_signature import WiringNodeSignature

__all__ = ("SubscriptionServiceNodeClass",)

from hgraph._wiring._wiring_port import _wiring_port_for


class SubscriptionServiceNodeClass(ServiceInterfaceNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        if not signature.defaults.get("path"):
            signature = signature.copy_with(defaults=frozendict(signature.defaults | {"path": None}))
        super().__init__(signature, fn)
        if (l := len(signature.time_series_args)) != 1:
            raise CustomMessageWiringError(f"Expected 1 time-series argument, got {l}")
        ts_type = signature.input_types[next(iter(signature.time_series_args))]
        if type(ts_type) is not HgTSTypeMetaData or not is_keyable_scalar(ts_type.value_scalar_tp.py_type):
            raise CustomMessageWiringError(f"The subscription property must be a TS[KEYABLE_SCALAR], {ts_type} is not")

    def full_path(self, user_path: str | None) -> str:
        if user_path is None:
            user_path = f"{self.fn.__module__}"
        return f"subs_svc://{user_path}/{self.fn.__name__}"

    def __call__(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **kwargs) -> "WiringPort":

        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            kwargs_, resolved_signature, resolution_dict = self._validate_and_resolve_signature(*args,
                                                                               __pre_resolved_types__=__pre_resolved_types__,
                                                                               **kwargs)

            # But graph nodes are evaluated at wiring time, so this is the graph expansion happening here!
            with WiringGraphContext(self.signature) as g:
                typed_full_path = self.typed_full_path(kwargs_.get("path"), resolution_dict)
                full_path = self.full_path(kwargs_.get("path"))
                g.register_service_client(self, full_path, resolution_dict)

                from hgraph.nodes._service_utils import _subscribe
                from hgraph import TIME_SERIES_TYPE
                return _subscribe[TIME_SERIES_TYPE: resolved_signature.output_type](
                    path=typed_full_path,
                    key=kwargs_[next(iter(resolved_signature.time_series_args))])

    def wire_impl_inputs_stub(self, path, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None):
        from hgraph.nodes import capture_output_node_to_global_state
        from hgraph import last_value_source_node, ts_schema

        resolution_dict = self.signature.try_build_resolution_dict(__pre_resolved_types__)
        typed_path = self.typed_full_path(path, resolution_dict)

        arg = next(iter(self.signature.time_series_args))
        subscriptions = last_value_source_node(f"{self.signature.name}_{arg}",
                                               (tp := TSS[self.signature.input_types[arg].resolve(resolution_dict).scalar_type().py_type]))
        subscriptions.set_label(f"{typed_path}/inputs/{arg}")
        subscriptions = _wiring_port_for(tp, subscriptions, tuple())
        capture_output_node_to_global_state(f"{typed_path}/subs", subscriptions)

        WiringGraphContext.instance().add_built_service_impl(typed_path, None)

        return TSB[ts_schema(**{arg: HgTypeMetaData.parse_type(tp)})].from_ts(**{arg: subscriptions})

    def wire_impl_out_stub(self, path, out, __pre_resolved_types__=None):
        from hgraph.nodes import capture_output_to_global_state
        capture_output_to_global_state(
            f"{self.typed_full_path(path, self.signature.try_build_resolution_dict(__pre_resolved_types__))}/out", out)

    def register_impl(self, path: str, impl: "NodeBuilder", __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None):
        from hgraph import register_service
        register_service(path, impl, self.signature.try_build_resolution_dict(__pre_resolved_types__))