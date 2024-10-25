from typing import Callable, Sequence, TypeVar

from hgraph._types._scalar_type_meta_data import HgAtomicType
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._adaptor_impl_node_class import AdaptorImplNodeClass
from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext
from hgraph._wiring._wiring_node_class._wiring_node_class import (
    WiringNodeClass,
    validate_and_resolve_signature,
)
from hgraph._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType

__all__ = ("ServiceAdaptorImplNodeClass",)


class ServiceAdaptorImplNodeClass(AdaptorImplNodeClass):
    def __call__(
        self,
        *args,
        __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None,
        __interface__: WiringNodeSignature = None,
        **kwargs,
    ) -> "WiringPort":
        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            path = kwargs.get("path")

            from hgraph import AdaptorNodeClass

            __interface__: AdaptorNodeClass

            if __interface__ is None:
                __interface__ = self.interfaces[0]

            if not __interface__.is_full_path(path):
                full_path = __interface__.full_path(path)
            else:
                full_path = path
                path = __interface__.path_from_full_path(full_path)

            path = path.replace("/from_graph", "").replace("/to_graph", "")

            self._validate_service_not_already_bound(full_path, __pre_resolved_types__)

            scalars = {k: v for k, v in __pre_resolved_types__.items() if k in __interface__.signature.scalar_inputs}
            pre_resolved_types = {
                k: v for k, v in __pre_resolved_types__.items() if k not in self.signature.scalar_inputs
            }

            kwargs["path"] = path

            kwargs_, resolved_signature, resolution_dict = validate_and_resolve_signature(
                self.signature, *args, __pre_resolved_types__=pre_resolved_types, **(kwargs | scalars)
            )

            with WiringGraphContext(node_signature=self.signature):
                from_graph = __interface__.wire_impl_inputs_stub(path, resolution_dict, **scalars).as_dict()
                to_graph = self.implementation_graph.__call__(
                    __pre_resolved_types__=resolution_dict,
                    **{k: v for k, v in kwargs_.items() if k not in from_graph},
                    **from_graph
                )
                __interface__.wire_impl_out_stub(path, to_graph, resolution_dict, **scalars)

    def validate_signature_vs_interfaces(
        self, signature: WiringNodeSignature, fn: Callable, interfaces: Sequence[WiringNodeClass]
    ) -> WiringNodeSignature:
        """
        Simple adaptor implementation has the same interface as the adaptor it implements
        """
        from hgraph import HgTSDTypeMetaData

        if len(interfaces) == 1:
            interface_sig: WiringNodeSignature = interfaces[0].signature
            match interface_sig.node_type:
                case WiringNodeType.SERVICE_ADAPTOR:
                    for arg, ts_type in signature.time_series_inputs.items():
                        if not isinstance(ts_type, HgTSDTypeMetaData):
                            raise CustomMessageWiringError(
                                f"service adaptors inputs all must be TSD: {arg} is {ts_type.py_type}"
                            )
                        if not ts_type.key_tp.matches(HgAtomicType(int)):
                            raise CustomMessageWiringError(
                                f"service adaptors inputs keys must be int: {arg}'s is {ts_type.key_tp}"
                            )
                        if not ts_type.value_tp.matches((ts_int_type := interface_sig.input_types.get(arg))):
                            raise CustomMessageWiringError(
                                f"The implementation input {arg}: {ts_type} type value does not match {ts_int_type}"
                            )
                    if any(arg not in signature.time_series_inputs for arg in interface_sig.time_series_inputs):
                        raise CustomMessageWiringError(
                            "The implementation has missing inputs compared to the service signature"
                        )
                    if signature.output_type is not None:
                        output = signature.output_type.dereference()
                        if not isinstance(output, HgTSDTypeMetaData):
                            raise CustomMessageWiringError(
                                f"service adaptors output must be TSD: output is {output.py_type}"
                            )
                        if not output.key_tp.matches(HgAtomicType(int)):
                            raise CustomMessageWiringError(f"service adaptors output key must be int: {output.key_tp}")
                        if not output.value_tp.matches(interface_sig.output_type.dereference()):
                            raise CustomMessageWiringError(
                                "The output type does not match that of the subscription service signature"
                            )
                    elif interface_sig.output_type is not None:
                        raise CustomMessageWiringError(
                            "The implementation has missing output compared to the service signature"
                        )
                case _:
                    raise CustomMessageWiringError(f"Unknown service type: {interface_sig.node_type}")
        else:
            pass  # multiservice/multiadaptor implementations use the interface stub APIs to wire up the service so checking happens there
