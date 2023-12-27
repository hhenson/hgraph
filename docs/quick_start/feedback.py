from hgraph import feedback, TS, graph, compute_node
from hgraph.test import eval_node


@compute_node(active=("ts",), valid=("ts",))
def i_need_feedback(ts: TS[float], prev_ts: TS[float]) -> TS[float]:
    return ts.value + prev_ts.value


@graph
def use_feedback(ts: TS[float]) -> TS[float]:
    fb = feedback(TS[float], 0.0)
    out = i_need_feedback(ts, fb())
    fb(out)
    return out


print(eval_node(use_feedback, [1.0, 2.0, 3.0]))
