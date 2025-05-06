from contextlib import nullcontext
from datetime import datetime
from itertools import zip_longest
from typing import Any

from hgraph import (
    graph,
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
    SimpleArrayReplaySource,
    set_replay_values,
    get_recorded_value,
    evaluate_graph,
    GraphConfiguration,
    MAX_ET,
)
from hgraph._impl._operators._record_replay_in_memory import replay_from_memory, record_to_memory


def eval_node(
    node,
    *args,
    resolution_dict: [str, Any] = None,
    __trace__: bool = False,
    __trace_wiring__: bool = False,
    __observers__: list[EvaluationLifeCycleObserver] = None,
    __elide__: bool = False,
    __start_time__: datetime = MIN_ST,
    __end_time__: datetime = None,
    **kwargs,
):
    """
    Evaluates a node using the supplied arguments. This will construct a graph to wrap the node or graph supplied
    with logic to feed in the supplied inputs into the node and capture the results. The function will then run the
    graph and return the captured results.

    .. note:: This only works with SIMULATION mode graphs.

    The inputs to the node are lists of values (or None) that can be supplied to an output of the inputs' type.
    If the node returns a result, the results will be collected and returned as an array. Results are captured
    as ``delta_value`` s.

    For example:

    ::

        @compute_node
        def my_func(ts: TS[int]) -> TS[int]:
            return ts.value

        assert eval_node(my_func, [1, 2, 3]) == [1, 2, 3]

    The ``eval_node`` takes the ``graph`` or node to evaluate, and then any parameters to pass to the node.
    The parameters are supplied as a list of values. The node interprets the list as the values to tick into the node
    starting from ``MIN_ST`` and incrementing by ``MIN_TD`` for each entry. If the list has ``None`` as an element
    this is interpreted as the input not receiving a tick at that time.

    The result returned are the ticks as they appeared, with ``None`` representing no value ticked at the time-point.
    The result will be padded to the last input time.


    For nodes that require resolution, it is possible to supply a resolution dictionary to assist
    in resolving the correct types when setting up the replay nodes.
    This is an example using the resolution dictionary:

    ::

        @compute_node
        def my_func(ts: OUT) -> OUT:
            return ts.value

        assert eval_node(my_func, [1, 2, 3], resolution_dict={OUT: TS[int]}) == [1, 2, 3]

    There are a number of additional modifiers that can be supplied, these affect the calling of the run loop or
    the presentation of the results.

    The most useful of these include ``__elide__`` which, when set to ``True``, will reduce the result to only the
    values that actually ticked. Note that this does not provide when the values ticked, just the order in which they
    ticked. This can be useful when the time between ticks is large.

    Another useful option is ``__start_time__`` which will allow the start time to be adjusted, this can be useful
    when performing a test that requires a particular start time.

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
            inputs[ts_arg] = replay_from_memory(ts_arg, ts_type)
            is_list = hasattr(arg_value, "__len__")
            set_replay_values(
                ts_arg, SimpleArrayReplaySource((arg_value if is_list else [arg_value]), start_time=__start_time__)
            )
        for scalar_args in node.signature.scalar_inputs.keys():
            inputs[scalar_args] = kwargs_[scalar_args]

        out = node(**inputs)

        if node.signature.output_type is not None:
            # For now, not to worry about un_named bundle outputs
            record_to_memory(out)

    with GlobalState() if not GlobalState.has_instance() else nullcontext():
        max_count = 0
        for ts_arg in time_series_inputs:
            v = kwargs_[ts_arg]
            if v is None:
                continue
            # Dealing with scalar to time-series support
            max_count = max(max_count, len(v) if (is_list := isinstance(v, (list, tuple))) else 1)
            if not is_list:
                kwargs_[ts_arg] = [v]
        evaluate_graph(
            eval_node_graph,
            GraphConfiguration(
                life_cycle_observers=__observers__ if __observers__ is not None else tuple(),
                start_time=__start_time__ if __start_time__ is not None else MIN_ST,
                end_time=__end_time__ if __end_time__ is not None else MAX_ET,
                trace=__trace__,
                trace_wiring=__trace_wiring__,
            ),
        )

        results = get_recorded_value() if node.signature.output_type is not None else []
        if results:
            # For push nodes, there are no time-series inputs, so we compute size of the result from the result.
            max_count = max(max_count, 1 + int((results[-1][0] - __start_time__) / MIN_TD))
        # Extract the results into a list of values without time-stamps, place a None when there is no recorded value.
        if results:
            out = []
            if not __elide__:
                result_iter = iter(results)
                result = next(result_iter)
                for t in _time_iter(__start_time__, __start_time__ + max_count * MIN_TD, MIN_TD):
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
