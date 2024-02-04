from typing import TYPE_CHECKING

from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass, create_input_output_builders

if TYPE_CHECKING:
    from hgraph._builder._node_builder import NodeBuilder


__all__ = ("PythonWiringNodeClass", "PythonPushQueueWiringNodeClass",
           "PythonGeneratorWiringNodeClass")


class PythonGeneratorWiringNodeClass(BaseWiringNodeClass):

    def create_node_builder_instance(self, node_signature, scalars) -> "NodeBuilder":
        from hgraph._impl._builder import PythonGeneratorNodeBuilder
        from hgraph import TimeSeriesBuilderFactory
        factory: TimeSeriesBuilderFactory = TimeSeriesBuilderFactory.instance()
        output_type = node_signature.time_series_output
        assert output_type is not None, "PythonGeneratorWiringNodeClass must have a time series output"
        return PythonGeneratorNodeBuilder(
            signature=node_signature,
            scalars=scalars,
            input_builder=None,
            output_builder=factory.make_output_builder(output_type),
            error_builder=factory.make_error_builder() if node_signature.capture_exception else None,
            eval_fn=self.fn
        )


class PythonPushQueueWiringNodeClass(BaseWiringNodeClass):

    def create_node_builder_instance(self, node_signature, scalars) -> "NodeBuilder":
        from hgraph._impl._builder import PythonPushQueueNodeBuilder
        from hgraph import TimeSeriesBuilderFactory
        factory: TimeSeriesBuilderFactory = TimeSeriesBuilderFactory.instance()
        output_type = node_signature.time_series_output
        assert output_type is not None, "PythonPushQueueWiringNodeClass must have a time series output"
        return PythonPushQueueNodeBuilder(
            signature=node_signature,
            scalars=scalars,
            input_builder=None,
            output_builder=factory.make_output_builder(output_type),
            error_builder=factory.make_error_builder() if node_signature.capture_exception else None,
            eval_fn=self.fn
        )


class PythonWiringNodeClass(BaseWiringNodeClass):

    def create_node_builder_instance(self, node_signature, scalars) -> "NodeBuilder":
        from hgraph._impl._builder import PythonNodeBuilder
        input_builder, output_builder, error_builder = create_input_output_builders(node_signature,
                                                                                    self.error_output_type)

        return PythonNodeBuilder(signature=node_signature,
                                 scalars=scalars,
                                 input_builder=input_builder,
                                 output_builder=output_builder,
                                 error_builder=error_builder,
                                 eval_fn=self.fn,
                                 start_fn=self.start_fn,
                                 stop_fn=self.stop_fn)
