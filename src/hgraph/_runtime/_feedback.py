from dataclasses import dataclass
from typing import Generic, TYPE_CHECKING

from hgraph._types._scalar_types import SCALAR
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._wiring._decorators import sink_node

if TYPE_CHECKING:
    from hgraph._wiring._wiring_port import WiringPort

__all__ = ("feedback",)


@sink_node(active=("ts",), valid=("ts",))
def _feedback_sink(ts: TIME_SERIES_TYPE, ts_self: TIME_SERIES_TYPE):
    """This binds the value of ts to the _feedback source node"""
    ts_self.output.owning_node.copy_from_input(ts)


@dataclass
class FeedbackWiringPort(Generic[TIME_SERIES_TYPE]):

    _delegate: "WiringPort"
    _bound: bool = False

    def __call__(self, ts: TIME_SERIES_TYPE = None) -> TIME_SERIES_TYPE:
        if ts is None:
            return self._delegate

        if self._bound:
            from hgraph._wiring._wiring_errors import CustomMessageWiringError
            raise CustomMessageWiringError(f"feedback is already bounded")
        self._bound = True
        _feedback_sink(ts, self._delegate)


def feedback(tp_: type[TIME_SERIES_TYPE], default: SCALAR = None) -> FeedbackWiringPort[TIME_SERIES_TYPE]:
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
    from hgraph._wiring._wiring_port import _wiring_port_for
    from hgraph._wiring._wiring_node_class._pull_source_node_class import last_value_source_node
    node_instance = last_value_source_node("feedback", tp_, default)
    real_wiring_port = _wiring_port_for(node_instance.output_type, node_instance, tuple())
    return FeedbackWiringPort(_delegate=real_wiring_port)




