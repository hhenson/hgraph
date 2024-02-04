from collections import defaultdict
from typing import Callable, Mapping, Any, TYPE_CHECKING

from frozendict import frozendict

from hgraph import GlobalState, Removed
from hgraph._types._scalar_types import is_keyable_scalar
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._types._ts_meta_data import HgTSTypeMetaData
from hgraph._types._ref_meta_data import HgREFTypeMetaData
from hgraph._wiring._wiring_node_class._wiring_node_class import create_input_output_builders
from hgraph._wiring._wiring_node_class._service_interface_node_class import ServiceInterfaceNodeClass
from hgraph._wiring._wiring_node_signature import WiringNodeSignature

if TYPE_CHECKING:
    from hgraph._runtime._node import NodeSignature
    from hgraph._builder._node_builder import NodeBuilder
    from hgraph._impl._runtime._node import PythonLastValuePullNodeImpl
    from hgraph._types._ts_type import TS
    from hgraph._types._scalar_types import SCALAR


__all__ = ("SubscriptionServiceNodeClass",)


class SubscriptionServiceNodeClass(ServiceInterfaceNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        if not signature.defaults.get("path"):
            signature = signature.copy_with(defaults=frozendict(signature.defaults |  {"path":  None}))
        super().__init__(signature, fn)
        if (l := len(signature.time_series_args)) != 1:
            raise CustomMessageWiringError(f"Expected 1 time-series argument, got {l}")
        ts_type = signature.input_types[next(iter(signature.time_series_args))]
        if type(ts_type) is not HgTSTypeMetaData or not is_keyable_scalar(ts_type.value_scalar_tp.py_type):
            raise CustomMessageWiringError("The subscription property must be a TS[KEYABLE_SCALAR]")

    def full_path(self, user_path: str | None) -> str:
        if user_path is None:
            user_path = f"{self.fn.__module__}"
        return f"subs_svc://{user_path}/{self.fn.__name__}"

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

        class _PythonServiceOutputStubSourceNode(BaseNodeImpl):

            def __init__(self, node_ndx: int, owning_graph_id:
            tuple[int, ...], signature: "NodeSignature", scalars: Mapping[str, Any]):
                super().__init__(node_ndx, owning_graph_id, signature, scalars)
                self._first_eval: bool = False
                self._old_value: "SCALAR" = None
                self._svc_node_in: "PythonLastValuePullNodeImpl" | None = None
                self._subscription_id: object = object()
                self._tracker: dict[str, set] = None
                self._ts: "TS[SCALAR]" = None

            def do_eval(self):
                """The service must be available by now, so we can retrieve the output reference."""
                if self._first_eval:
                    self._first_eval = False
                    from hgraph._runtime._global_state import GlobalState
                    path = self.scalars["path"]
                    _svc_node_out = GlobalState.instance().get(f"{path}_out")
                    self._svc_node_in = GlobalState.instance().get(f"{path}_subs")
                    if _svc_node_out is None:
                        raise RuntimeError(f"Could not find reference service for path: {self.scalars['path']}")
                    # The output must hold the reference of the inner graph by now.
                    self.output.value = _svc_node_out.output.value

                if self._input.modified:
                    (s := self._tracker[(v := self._ts.value)]).add(self._subscription_id)
                    set_delta = set()
                    if self._old_value:
                        (s_old := self._tracker[self._old_value]).remove(self._subscription_id)
                        if not s_old:
                            set_delta.add(Removed(self._old_value))
                    self._old_value = v
                    if len(s) == 1:
                        set_delta = set(v)
                    if set_delta:
                        self._svc_node_in.apply_value(set_delta)

            def do_start(self):
                """Make sure we get notified to serve the service output reference"""
                self.notify()
                path = self.scalars["path"]
                tracker_path = f"{path}_tracker"
                self._tracker = GlobalState.instance().get(tracker_path)
                if not self._tracker:
                    self._tracker = defaultdict(set)
                    GlobalState.instance()[tracker_path] = self._tracker
                self._ts = self.input[next(iter(self.signature.time_series_inputs))]

            def do_stop(self):
                if self._ts.valid:
                    s = self._tracker[(v := self._ts.value)].remove(self._subscription_id)
                    if not s:
                        del self._tracker[self._ts.value]
                        self._svc_node_in.apply_value(Removed(v))

        return PythonNodeImplNodeBuilder(
            signature=node_signature,
            scalars=scalars,
            input_builder=input_builder,
            output_builder=output_builder,
            error_builder=error_builder,
            node_impl=_PythonServiceOutputStubSourceNode
        )
