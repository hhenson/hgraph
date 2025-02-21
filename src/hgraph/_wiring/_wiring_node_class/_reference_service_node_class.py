from typing import Mapping, Any, TYPE_CHECKING, TypeVar, Callable

from hgraph._types._ref_meta_data import HgTypeMetaData
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._service_interface_node_class import ServiceInterfaceNodeClass
from hgraph._wiring._wiring_node_class._wiring_node_class import (
    create_input_output_builders,
    validate_and_resolve_signature,
)
from hgraph._wiring._wiring_node_signature import WiringNodeSignature

if TYPE_CHECKING:
    from hgraph._runtime._node import NodeSignature
    from hgraph._builder._node_builder import NodeBuilder

__all__ = ("ReferenceServiceNodeClass",)


class ReferenceServiceNodeClass(ServiceInterfaceNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        if signature.output_type is None:
            raise CustomMessageWiringError("A reference service must have a return type.")
        signature = signature.copy_with(output_type=signature.output_type.as_reference())
        super().__init__(signature, fn)

    def full_path(self, user_path: str | None) -> str:
        if user_path is None:
            user_path = self.default_path()

        return f"ref_svc://{user_path}/{self.fn.__name__}"

    def __call__(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None, **kwargs) -> "WiringPort":
        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            kwargs_, resolved_signature, resolution_dict = validate_and_resolve_signature(
                self.signature, *args, __pre_resolved_types__=__pre_resolved_types__, **kwargs
            )

            path = kwargs_.get("path") or self.default_path()
            full_path = self.full_path(path)
            typed_full_path = self.typed_full_path(path, resolution_dict)

            from hgraph.nodes import get_shared_reference_output
            out = get_shared_reference_output[TIME_SERIES_TYPE: resolved_signature.output_type.dereference()](
                typed_full_path
            )

            from hgraph import WiringGraphContext

            WiringGraphContext.instance().register_service_client(
                self, full_path, resolution_dict or None, out.node_instance
            )

            return out

    def wire_impl_out_stub(self, path, out, __pre_resolved_types__=None):
        from hgraph.nodes import capture_output_to_global_state
        from hgraph import WiringGraphContext

        typed_full_path = self.typed_full_path(path, self.signature.try_build_resolution_dict(__pre_resolved_types__))
        capture_output_to_global_state(typed_full_path, out)

        WiringGraphContext.instance().add_built_service_impl(typed_full_path, None)

    def register_impl(
        self, path: str, impl: "NodeBuilder", __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None, **kwargs
    ):
        """
        Register an implementation for a service instance. This is useful when there in only one service which has
        type resolution required.
        """
        from hgraph import register_service

        register_service(path, impl, self.signature.try_build_resolution_dict(__pre_resolved_types__), **kwargs)
