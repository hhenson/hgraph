from dataclasses import dataclass
from typing import cast

from frozendict import frozendict

from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._scalar_types import SCALAR
from hgraph._wiring._decorators import pull_source_node, sink_node
from hgraph._wiring._wiring import WiringNodeClass, PythonLastValuePullWiringNodeClass
from hgraph._wiring._wiring_node_instance import WiringNodeInstance
from hgraph._wiring._wiring_port import _wiring_port_for, WiringPort
from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData


__all__ = ("feedback",)


@pull_source_node
def _feedback() -> TIME_SERIES_TYPE:
    ...


@sink_node(active=("ts",), valid=("ts",))
def _feedback_sink(ts: TIME_SERIES_TYPE, ts_self: TIME_SERIES_TYPE):
    """This binds the value of ts to the _feedback source node"""
    ts_self.output.owning_node.copy_from_input(ts)


def feedback(tp_: type[TIME_SERIES_TYPE], default: SCALAR = None) -> TIME_SERIES_TYPE:
    """
    Provides a mechanism to allow for cycles in the code without breaking the DAG nature of the graph.
    The ``feedback`` method creates a special node that can be used to wire into nodes prior to the value
    is connected. The mechanism used to emit the value bound to the feedback in exactly one engine cycle's time.
    Thus, the cycle is created but broken into two evaluations of the engine.

    It is good practice to mark inputs that use feedback values as being not-active. This avoids the node being
    called when the data is returned to the function. This is reasonable as most often these inputs are required
    to compute the next results, but is not the source of action to compute a next result.

    Example usage:

    ```
    fb = feedback(TS[int], 0)  # Declare the feedback node with a default value of 0
    ...
    out = my_compute_node(..., fb())  # Use the feedback as an input.
    fb(out)  # Bind the value to the feedback node
    ```
    """
    changes = {"name": "feedback"}
    inputs = {}
    if default is not None:
        default_type = HgScalarTypeMetaData.parse(default)
        changes["args"] = tuple(["default"])
        changes["input_types"] = frozendict({"default": default_type})
        inputs["default"] = default

    signature = cast(WiringNodeClass, _feedback[TIME_SERIES_TYPE: tp_]).resolve_signature().copy_with(**changes)
    node_instance = WiringNodeInstance(node=PythonLastValuePullWiringNodeClass(signature, None),
                                       resolved_signature=signature,
                                       inputs=frozendict({"default": default}),
                                       rank=1)
    real_wiring_port = _wiring_port_for(node_instance.output_type, node_instance, tuple())
    return FeedbackWiringPort(_delegate=real_wiring_port)


@dataclass
class FeedbackWiringPort:

    _delegate: WiringPort
    _bound: bool = False

    def __call__(self, ts: TIME_SERIES_TYPE = None) -> TIME_SERIES_TYPE:
        if ts is None:
            return self._delegate

        if self._bound:
            raise CustomMessageWiringError(f"feeback is already bounded")
        self._bound = True
        _feedback_sink(ts, self._delegate)



