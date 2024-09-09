Feedback
========

Feedback is the ability to take a result produced by the graph and re-introduce the value
in the next engine-cycle, giving us the illusion of creating a cycle in the graph.

To use this facility, you create a new variable as such:

```python
from hgraph import feedback, TS, graph, compute_node


@compute_node(active=("ts",), valid=("ts",))
def i_need_feedback(ts: TS[float], prev_ts: TS[float]) -> TS[float]:
    return ts.value + prev_ts.value


@graph
def use_feedback(ts: TS[float]) -> TS[float]:
    fb = feedback(TS[float], 0.0)
    out = i_need_feedback(ts, fb())
    fb(out)
    return out
```

Note that we exclude the feedback input from the active state in the compute node.
This is to avoid being activated and re-computing when the feedback ticks in the
next engine cycle. If this was not done, you effectively create an infinite loop.

Also note that accessing the 'output' you use the empty parenthesis (``fb()``),
then to bind the output into the feedback loop, you use the parenthesis again, but this
time with the value as an argument (``fb(out)``).

The option of providing a default can be useful. This works in a similar manner as per
the ``const``.
