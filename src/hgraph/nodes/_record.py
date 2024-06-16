from datetime import datetime
from typing import Any

from hgraph import sink_node, TIME_SERIES_TYPE, EvaluationClock, GlobalState, STATE


__all__ = ("record", "get_recorded_value")


@sink_node
def record(
    ts: TIME_SERIES_TYPE,
    label: str = "out",
    record_delta_values: bool = True,
    _clock: EvaluationClock = None,
    _state: STATE = None,
):
    """
    This node will record the values of the time series into the provided list.
    """
    _state.record_value.append((_clock.evaluation_time, ts.delta_value if record_delta_values else ts.value))


@record.start
def record_start(label: str, _state: STATE):
    value = []
    global_state = GlobalState.instance()
    global_state[f"nodes.{record.signature.name}.{label}"] = value
    _state.record_value = value


def get_recorded_value(label: str = "out") -> list[tuple[datetime, Any]]:
    """
    Returns the recorded values for the given label.
    """
    global_state = GlobalState.instance()
    return global_state[f"nodes.{record.signature.name}.{label}"]
