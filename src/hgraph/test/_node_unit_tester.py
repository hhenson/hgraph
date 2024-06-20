from contextlib import nullcontext
from datetime import datetime
from itertools import zip_longest
from typing import Any

from hgraph import (
    graph,
    run_graph,
    GlobalState,
    MIN_TD,
    HgTypeMetaData,
    HgTSTypeMetaData,
    prepare_kwargs,
    MIN_ST,
    MIN_DT,
    WiringContext,
    WiringError,
    EvaluationLifeCycleObserver,
)
from hgraph.nodes import replay, record, SimpleArrayReplaySource, set_replay_values, get_recorded_value


def eval_node(
    node,
    *args,
    resolution_dict: [str, Any] = None,
    __trace__: bool = False,
    __trace_wiring__: bool = False,
    __observers__: list[EvaluationLifeCycleObserver] = None,
    __elide__: bool = False,
    __start_time__: datetime = None,
    __end_time__: datetime = None,
    **kwargs,
):
    """
    Evaluates a node using the supplied arguments.
    This will detect time-series inputs in the node and will convert array inputs into time-series inputs.
    If the node returns a result, the results will be collected and returned as an array.

    For nodes that require resolution, it is possible to supply a resolution dictionary to assist
    in resolving correct types when setting up the replay nodes.

    :param node: The node to evaluate
    :param args: Arguments to pass to the node
    :param kwargs: Keyword arguments to pass to the node
    :param resolution_dict: Dictionary of resolution keys and values to pass to the node (this should be at the input parameter level)
    :param __trace__: If True, the trace will be printed to the console.
    :param __trace_wiring__: If True, the wiring trace will be printed to the console.
    :param __observers__: If not None, the observers will be added to the evaluation results.
    :param __elide__: If True, only the ticked values will be returned. If the value is False every potential
                      engine cycle will have a result (None if it did not tick).
    :param __start_time__: If not None, the time at which to start evaluation.
    :param __end_time__: If not None, the time at which to end evaluation.
    """
    if not hasattr(node, "signature"):
        if callable(node):
            raise RuntimeError(f"The node '{node}' should be decorated with either a node or graph decorator")
        else:
            raise RuntimeError(f"The node '{node}' does not appear to be a node or graph function")
    try:
        with WiringContext(current_signature=node.signature):
            kwargs_ = prepare_kwargs(node.signature, *args, _ignore_defaults=True, **kwargs)
    except WiringError as e:
        e.print_error()
        raise e

    time_series_inputs = tuple(arg for arg in node.signature.args if arg in node.signature.time_series_inputs)

    @graph
    def eval_node_graph():
        inputs = {}
        for ts_arg in time_series_inputs:
            arg_value = kwargs_.get(ts_arg)
            if arg_value is None:
                continue
            if ts_arg == node.signature.var_arg and ts_arg not in kwargs:
                # this was collected into *arg hence needs to be transposed to be correct shape for TSL replay
                arg_value = list(zip_longest(*(a if hasattr(a, "__iter__") else [a] for a in arg_value)))
            if ts_arg == node.signature.var_kwarg and ts_arg not in kwargs:
                # this was collected into **kwarg hence needs to be transposed to be correct shape for TSB replay
                arg_value = list(
                    {k: v for k, v in zip(arg_value.keys(), i)}
                    for i in zip_longest(*(a if hasattr(a, "__iter__") else [a] for a in arg_value.values()))
                )
            if resolution_dict is not None and ts_arg in resolution_dict:
                ts_type = resolution_dict[ts_arg]
            else:
                ts_type: HgTypeMetaData = node.signature.input_types[ts_arg]
                if not ts_type.is_resolved:
                    # Attempt auto resolve
                    v_ = arg_value
                    if not hasattr(v_, "__iter__"):  # Dealing with scalar to time-series support
                        v_ = [v_]
                    ts_type = HgTypeMetaData.parse_value(next(i for i in v_ if i is not None))
                    if ts_type is None or not ts_type.is_resolved:
                        raise RuntimeError(
                            f"Unable to auto resolve type for '{ts_arg}', "
                            f"signature type is '{node.signature.input_types[ts_arg]}'"
                        )
                    ts_type = HgTSTypeMetaData(ts_type)
                    print(f"Auto resolved type for '{ts_arg}' to '{ts_type}'")
                ts_type = ts_type.py_type if not ts_type.is_context_wired else ts_type.ts_type.py_type
            inputs[ts_arg] = replay(ts_arg, ts_type)
            is_list = hasattr(arg_value, "__len__")
            set_replay_values(ts_arg, SimpleArrayReplaySource(arg_value if is_list else [arg_value]))
        for scalar_args in node.signature.scalar_inputs.keys():
            inputs[scalar_args] = kwargs_[scalar_args]

        out = node(**inputs)

        if node.signature.output_type is not None:
            # For now, not to worry about un_named bundle outputs
            record(out)

    with GlobalState() if GlobalState._instance is None else nullcontext():
        max_count = 0
        for ts_arg in time_series_inputs:
            v = kwargs_[ts_arg]
            if v is None:
                continue
            # Dealing with scalar to time-series support
            max_count = max(max_count, len(v) if (is_list := hasattr(v, "__len__")) else 1)
        run_graph(
            eval_node_graph,
            life_cycle_observers=__observers__,
            start_time=__start_time__,
            end_time=__end_time__,
            __trace__=__trace__,
            __trace_wiring__=__trace_wiring__,
        )

        results = get_recorded_value() if node.signature.output_type is not None else []
        if results:
            # For push nodes, there are no time-series inputs, so we compute size of the result from the result.
            max_count = max(max_count, int((results[-1][0] - MIN_DT) / MIN_TD))
        # Extract the results into a list of values without time-stamps, place a None when there is no recorded value.
        if results:
            out = []
            if not __elide__:
                result_iter = iter(results)
                result = next(result_iter)
                for t in _time_iter(MIN_ST, MIN_ST + max_count * MIN_TD, MIN_TD):
                    if result and t == result[0]:
                        out.append(result[1])
                        result = next(result_iter, None)
                    else:
                        out.append(None)
            else:
                out = [result[1] for result in results]
            return out


def _time_iter(start, end, delta):
    t = start
    while t < end:
        yield t
        t += delta
