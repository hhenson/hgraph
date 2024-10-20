from typing import Callable, Mapping, Any, TYPE_CHECKING

from hgraph._types._scalar_type_meta_data import HgRecordableStateType
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass, create_input_output_builders

if TYPE_CHECKING:
    from hgraph._runtime._node import Node
    from hgraph._runtime._node import NodeSignature
    from hgraph._builder._node_builder import NodeBuilder


class NodeImplWiringNodeClass(BaseWiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: "Node"):
        super().__init__(signature, fn)

    def create_node_builder_instance(
        self,
        resolved_wiring_signature: "WiringNodeSignature",
        node_signature: "NodeSignature",
        scalars: Mapping[str, Any],
    ) -> "NodeBuilder":
        from hgraph._impl._builder._node_impl_builder import PythonNodeImplNodeBuilder

        input_builder, output_builder, error_builder = create_input_output_builders(
            node_signature, self.error_output_type
        )

        recordable_state_builder = None
        if node_signature.uses_recordable_state:
            from hgraph import TimeSeriesBuilderFactory

            for v in scalars.values():
                if type(v) == HgRecordableStateType:
                    v: HgRecordableStateType
                    recordable_state_builder = TimeSeriesBuilderFactory.instance().make_output_builder(v.state_type)
                    break
            if recordable_state_builder is None:
                raise CustomMessageWiringError("Recordable state injectable not found")

        return PythonNodeImplNodeBuilder(
            signature=node_signature,
            scalars=scalars,
            input_builder=input_builder,
            output_builder=output_builder,
            error_builder=error_builder,
            recordable_state_builder=recordable_state_builder,
            node_impl=self.fn,
        )
