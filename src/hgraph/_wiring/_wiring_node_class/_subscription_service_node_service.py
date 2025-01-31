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
from hgraph._wiring._wiring_port import _wiring_port_for

__all__ = ("SubscriptionServiceNodeClass",)


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
            user_path = self.default_path()
        return f"subs_svc://{user_path}/{self.fn.__name__}"

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

                from hgraph import TIME_SERIES_TYPE, TSD
                from hgraph.nodes import write_subscription_key, get_shared_reference_output

                key = kwargs_[next(iter(resolved_signature.time_series_args))]
                key_type = key.output_type.dereference().value_scalar_tp
                value_type = resolved_signature.output_type

                client = write_subscription_key(typed_full_path, key, __return_sink_wp__=True)
                g.register_service_client(self, full_path, resolution_dict, client.node_instance)

                out = get_shared_reference_output[TIME_SERIES_TYPE : TSD[key_type, value_type]](
                    f"{typed_full_path}/out"
                )
                g.register_service_client(self, full_path, resolution_dict, out.node_instance)

                return out[key]

    def wire_impl_inputs_stub(self, path, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None):
        from hgraph.nodes import capture_output_node_to_global_state
        from hgraph import last_value_source_node, ts_schema

        resolution_dict = self.signature.try_build_resolution_dict(__pre_resolved_types__)
        typed_path = self.typed_full_path(path, resolution_dict)

        arg = next(iter(self.signature.time_series_args))
        subscriptions = last_value_source_node(
            f"{self.signature.name}_{arg}",
            (tp := TSS[self.signature.input_types[arg].resolve(resolution_dict).scalar_type().py_type]),
        )
        subscriptions.label = f"{typed_path}/inputs/{arg}"
        subscriptions = _wiring_port_for(tp, subscriptions, tuple())
        capture_output_node_to_global_state(f"{typed_path}/subs", subscriptions)

        WiringGraphContext.instance().add_built_service_impl(typed_path, None)

        return TSB[ts_schema(**{arg: HgTypeMetaData.parse_type(tp)})].from_ts(**{arg: subscriptions})

    def wire_impl_out_stub(self, path, out, __pre_resolved_types__=None):
        from hgraph.nodes import capture_output_to_global_state

        capture_output_to_global_state(
            f"{self.typed_full_path(path, self.signature.try_build_resolution_dict(__pre_resolved_types__))}/out", out
        )

    def register_impl(
        self, path: str, impl: "NodeBuilder", __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None
    ):
        from hgraph import register_service

        register_service(path, impl, self.signature.try_build_resolution_dict(__pre_resolved_types__))
