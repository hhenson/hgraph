from typing import Callable, TypeVar

from frozendict import frozendict

from hgraph._types._tsb_type import TSB
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext
from hgraph._wiring._wiring_node_class._service_interface_node_class import ServiceInterfaceNodeClass
from hgraph._wiring._wiring_node_class._wiring_node_class import validate_and_resolve_signature
from hgraph._wiring._wiring_node_signature import WiringNodeSignature

__all__ = ("AdaptorNodeClass",)


class AdaptorNodeClass(ServiceInterfaceNodeClass):
    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        if not signature.defaults.get("path"):
            signature = signature.copy_with(defaults=frozendict(signature.defaults | {"path": None}))
        super().__init__(signature, fn)

    def full_path(self, user_path: str | None) -> str:
        if user_path is None:
            user_path = self.default_path()
        return f"adaptor://{user_path}/{self.fn.__name__}"

    def __call__(
        self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **kwargs
    ) -> "WiringPort":
        self.from_graph(*args, __pre_resolved_types__=__pre_resolved_types__, **kwargs)
        return self.to_graph(*args, __pre_resolved_types__=__pre_resolved_types__, **kwargs)

    def from_graph(
        self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **kwargs
    ) -> "WiringPort":

        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            kwargs_, resolved_signature, resolution_dict = validate_and_resolve_signature(
                self.signature, *args, __pre_resolved_types__=__pre_resolved_types__, **kwargs
            )

            with WiringGraphContext(self.signature) as g:
                inputs = {k: v for k, v in kwargs_.items() if k in resolved_signature.time_series_inputs}
                scalars = {k: v for k, v in kwargs_.items() if k in resolved_signature.scalar_inputs and k != "path"}

                resolution_dict |= scalars

                path = kwargs_.get("path") or self.default_path()
                from_graph_full_path = self.full_path(path + "/from_graph")
                from_graph_typed_path = self.typed_full_path(from_graph_full_path, resolution_dict)

                from hgraph import combine
                from hgraph.nodes import capture_output_to_global_state, get_shared_reference_output

                inputs_from_graph = combine(**inputs)
                client = capture_output_to_global_state(
                    from_graph_typed_path, inputs_from_graph, __return_sink_wp__=True
                )
                g.register_service_client(
                    self, from_graph_full_path, resolution_dict, client.node_instance, receive=False
                )

    def to_graph(
        self,
        *args,
        __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None,
        __no_ts_inputs__: bool = False,
        **kwargs,
    ) -> "WiringPort":

        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            kwargs_, resolved_signature, resolution_dict = validate_and_resolve_signature(
                (
                    self.signature.copy_with(
                        args=tuple(k for k in self.signature.args if self.signature.input_types[k].is_scalar),
                        input_types={k: v for k, v in self.signature.input_types.items() if v.is_scalar},
                        time_series_args=set(),
                    )
                    if __no_ts_inputs__
                    else self.signature
                ),
                *args,
                __pre_resolved_types__=__pre_resolved_types__,
                **kwargs,
            )

            if resolved_signature.output_type:
                with WiringGraphContext(self.signature) as g:
                    scalars = {
                        k: v for k, v in kwargs_.items() if k in resolved_signature.scalar_inputs and k != "path"
                    }
                    resolution_dict |= scalars

                    path = kwargs_.get("path") or self.default_path()
                    to_graph_full_path = self.full_path(path + "/to_graph")
                    to_graph_typed_path = self.typed_full_path(to_graph_full_path, resolution_dict)

                    from hgraph.nodes import get_shared_reference_output

                    out = get_shared_reference_output[resolved_signature.output_type](to_graph_typed_path)
                    g.register_service_client(self, to_graph_full_path, resolution_dict, out.node_instance)
                    return out

    def wire_impl_inputs_stub(
        self, path, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **scalars
    ):
        from hgraph.nodes import get_shared_reference_output
        from hgraph import ts_schema

        path = self.path_from_full_path(path) if self.is_full_path(path) else path

        resolution_dict = self.signature.try_build_resolution_dict(__pre_resolved_types__)
        from_graph_path = self.typed_full_path(path + "/from_graph", resolution_dict | scalars)
        from_graph_type = TSB[
            ts_schema(**{k: v.resolve(resolution_dict) for k, v in self.signature.time_series_inputs.items()})
        ]
        from_graph = get_shared_reference_output[from_graph_type](from_graph_path, strict=False)

        WiringGraphContext.instance().add_built_service_impl(from_graph_path, from_graph.node_instance)
        return from_graph

    def wire_impl_out_stub(self, path, out, __pre_resolved_types__=None, **scalars):
        from hgraph.nodes import capture_output_to_global_state

        path = self.path_from_full_path(path) if self.is_full_path(path) else path

        to_graph_path = self.typed_full_path(
            path + "/to_graph", self.signature.try_build_resolution_dict(__pre_resolved_types__) | scalars
        )
        bottom = capture_output_to_global_state(to_graph_path, out, __return_sink_wp__=True)
        WiringGraphContext.instance().add_built_service_impl(to_graph_path, bottom.node_instance)

    def register_impl(
        self, path: str, impl: "NodeBuilder", __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None, **kwargs
    ):
        resolution_dict = self.signature.try_build_resolution_dict(__pre_resolved_types__)

        if path is None:
            path = self.default_path()
            WiringGraphContext.instance().register_service_impl(
                self, self.full_path(path), impl, kwargs, resolution_dict
            )
        else:
            WiringGraphContext.instance().register_service_impl(
                self, self.full_path(path + "/from_graph"), impl, kwargs, resolution_dict
            )
            WiringGraphContext.instance().register_service_impl(
                self, self.full_path(path + "/to_graph"), impl, kwargs, resolution_dict
            )
