from typing import Any

from hg import graph, run_graph, GlobalState, MIN_TD, HgTypeMetaData, HgTSTypeMetaData
from hg.nodes import replay, record, SimpleArrayReplaySource, set_replay_values, get_recorded_value


def eval_node(node, *args, resolution_dict: [str, Any] = None, **kwargs):
    """
    Evaluates a node using the supplied arguments.
    This will detect time-series inputs in the node and will convert array inputs into time-series inputs.
    If the node returns a result, the results will be collected and returned as an array.

    For nodes that require resolution, it is possible to supply a resolution dictionary to assist
    in resolving correct types when setting up the replay nodes.
    """

    # kwargs_ = prepare_kwargs(node, *args, **kwargs)  # TODO extract values when args are supplied
    kwargs_ = kwargs

    @graph
    def eval_node_graph():
        inputs = {}
        for ts_arg in node.signature.time_series_inputs.keys():
            if resolution_dict is not None and ts_arg in resolution_dict:
                ts_type = resolution_dict[ts_arg]
            else:
                ts_type: HgTypeMetaData = node.signature.input_types[ts_arg]
                if not ts_type.is_resolved:
                    # Attempt auto resolve
                    ts_type = HgTypeMetaData.parse(kwargs_[ts_arg][0])
                    if ts_type is None or not ts_type.is_resolved:
                        raise RuntimeError(
                            f"Unable to auto resolve type for '{ts_arg}', "
                            f"signature type is '{node.signature.input_types[ts_arg]}'")
                    ts_type = HgTSTypeMetaData(ts_type)
                    print(f"Auto resolved type for '{ts_arg}' to '{ts_type}'")
                    ts_type = ts_type.py_type
            inputs[ts_arg] = replay(ts_arg, ts_type)
        for scalar_args in node.signature.scalar_inputs.keys():
            inputs[scalar_args] = kwargs_[scalar_args]

        out = node(**inputs)

        if node.signature.output_type is not None:
            # For now, not to worry about un_named bundle outputs
            record(out)

    GlobalState.reset()
    for ts_arg in node.signature.time_series_inputs.keys():
        set_replay_values(ts_arg, SimpleArrayReplaySource(kwargs_[ts_arg]))
    run_graph(eval_node_graph)

    results = get_recorded_value() if node.signature.output_type is not None else []
    # Extract the results into a list of values without time-stamps, place a None when there is no recorded value.
    if results:
        out = []
        result_iter = iter(results)
        result = next(result_iter)
        for t in _time_iter(results[0][0], results[-1][0], MIN_TD):
            if t == result[0]:
                out.append(result[1])
                result = next(result_iter, None)
            else:
                out.append(None)
        return out


def _time_iter(start, end, delta):
    t = start
    while t <= end:
        yield t
        t += delta