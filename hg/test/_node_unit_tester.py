from typing import Any

from hg import graph, WiringNodeClass, run_graph
from hg.nodes import replay, record


def eval_node(node, *args, resolution_dict: [str, Any], **kwargs):
    """
    Evaluates a node using the supplied arguments.
    This will detect time-series inputs in the node and will convert array inputs into time-series inputs.
    If the node returns a result, the results will be collected and returned as an array.

    For nodes that require resolution, it is possible to supply a resolution dictionary to assist
    in resolving correct types when setting up the replay nodes.
    """

    #kwargs_ = prepare_kwargs(node, *args, **kwargs)  # TODO extract values when args are supplied
    kwargs_ = kwargs

    node: WiringNodeClass
    results = []

    @graph
    def eval_node_graph():
        inputs = {}
        for ts_arg in node.signature.time_series_inputs.keys():
            inputs[ts_arg] = replay(kwargs_[ts_arg],
                                    resolution_dict[ts_arg] if ts_arg in resolution_dict else
                                    node.signature.input_types[ts_arg])
        for scalar_args in node.signature.scalar_inputs.keys():
            inputs[scalar_args] = kwargs_[scalar_args]

        out = node(**inputs)

        if node.signature.output_type is not None:
            record(out, results)

    run_graph(eval_node_graph)

    return results
