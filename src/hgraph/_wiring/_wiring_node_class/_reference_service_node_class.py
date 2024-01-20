from typing import Callable, Mapping, Any

from frozendict import frozendict

from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass, create_input_output_builders
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._types._ref_meta_data import HgREFTypeMetaData

__all__ = ("ReferenceServiceNodeClass",)


class ReferenceServiceNodeClass(BaseWiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        if not signature.output_type.is_reference:
            # The output must be a reference type, if it already is a reference then we are fine.
            signature = signature.copy_with(output_type=HgREFTypeMetaData(signature.output_type))
        super().__init__(signature, fn)

    def create_node_builder_instance(self, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        from hgraph._impl._builder import PythonNodeImplNodeBuilder
        input_builder, output_builder, error_builder = create_input_output_builders(node_signature,
                                                                                    self.error_output_type)

        path = scalars.get("path")
        if path is None:
            path = f"{self.fn.__module__}.{self.fn.__name__}"

        path = f"ref_svc://{path}.{self.fn.__name__}"
        scalars = frozendict({"path": path})

        from hgraph._impl._builder._node_builder import BaseNodeImpl

        class _PythonReferenceServiceStubSourceNode(BaseNodeImpl):

            def do_eval(self):
                """The service must be available by now, so we can retrieve the output reference."""
                from hgraph._runtime._global_state import GlobalState
                service_output_reference = GlobalState.instance().get(self.scalars["path"])
                if service_output_reference is None:
                    raise RuntimeError(f"Could not find reference service for path: {self.scalars['path']}")
                self.output = service_output_reference

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


