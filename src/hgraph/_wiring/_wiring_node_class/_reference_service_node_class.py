from typing import Callable, Mapping, Any, TYPE_CHECKING

from frozendict import frozendict

from hgraph._types._ref_meta_data import HgREFTypeMetaData
from hgraph._wiring._wiring_node_class._wiring_node_class import create_input_output_builders
from hgraph._wiring._wiring_node_class._service_interface_node_class import ServiceInterfaceNodeClass
from hgraph._wiring._wiring_node_signature import WiringNodeSignature

if TYPE_CHECKING:
    from hgraph._runtime._node import NodeSignature
    from hgraph._builder._node_builder import NodeBuilder

__all__ = ("ReferenceServiceNodeClass",)


class ReferenceServiceNodeClass(ServiceInterfaceNodeClass):

    def full_path(self, user_path: str | None) -> str:
        if user_path is None:
            user_path = f"{self.fn.__module__}"

        return f"ref_svc://{user_path}/{self.fn.__name__}"

    def create_node_builder_instance(self, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        output_type = node_signature.time_series_output
        if type(output_type) is not HgREFTypeMetaData:
            node_signature = node_signature.copy_with(time_series_output=HgREFTypeMetaData(output_type))

        from hgraph._impl._builder import PythonNodeImplNodeBuilder
        input_builder, output_builder, error_builder = create_input_output_builders(node_signature,
                                                                                    self.error_output_type)

        scalars = frozendict({"path": self.full_path(scalars.get("path"))})

        from hgraph._impl._runtime._node import BaseNodeImpl

        class _PythonReferenceServiceStubSourceNode(BaseNodeImpl):

            def do_eval(self):
                """The service must be available by now, so we can retrieve the output reference."""
                from hgraph._runtime._global_state import GlobalState
                service_output_reference = GlobalState.instance().get(self.scalars["path"])
                if service_output_reference is None:
                    raise RuntimeError(f"Could not find reference service for path: {self.scalars['path']}")
                # TODO: The output needs to be a reference value output so we can set the value and continue!
                self.output.value = service_output_reference

            def do_start(self):
                """Make sure we get notified to serve the service output reference"""
                self.notify()

            def do_stop(self):
                ...

        return PythonNodeImplNodeBuilder(
            signature=node_signature,
            scalars=scalars,
            input_builder=input_builder,
            output_builder=output_builder,
            error_builder=error_builder,
            node_impl=_PythonReferenceServiceStubSourceNode
        )
