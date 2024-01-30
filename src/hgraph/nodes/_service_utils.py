from hgraph import sink_node, REF, TIME_SERIES_TYPE, GlobalState


__all__ = ("capture_output_to_global_state",)


@sink_node
def capture_output_to_global_state(path: str, ts: REF[TIME_SERIES_TYPE]):
    """This node serves to capture the output of a service node and record the output reference in the global state."""


@capture_output_to_global_state.start
def capture_output_to_global_state_start(path: str, ts: REF[TIME_SERIES_TYPE]):
    """Place the reference into the global state"""
    GlobalState.instance()[path] = ts.value


@capture_output_to_global_state.stop
def capture_output_to_global_state_stop(path: str):
    """Clean up references"""
    del GlobalState.instance()[path]