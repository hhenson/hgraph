from datetime import timedelta
from typing import Callable

from hgraph import (
    push_queue,
    TS,
    GlobalState,
    sink_node,
    graph,
    run_graph,
    EvaluationMode,
    schedule,
    count,
    adaptor_impl,
    adaptor,
    register_service,
    register_adaptor,
)
from hgraph.test import eval_node


def test_adaptor():
    @push_queue(TS[int])
    def top(sender: Callable[[int], None], path: str) -> TS[int]:
        GlobalState.instance()[f"{path}/queue"] = sender
        return None

    @sink_node
    def bottom(path: str, ts: TS[int]):
        sender = GlobalState.instance().get(f"{path}/queue")
        sender(ts.value)

    @adaptor
    def my_adaptor(path: str, ts: TS[int]) -> TS[int]: ...

    @adaptor_impl(interfaces=my_adaptor)
    def my_adaptor_impl(path: str, ts: TS[int]) -> TS[int]:
        bottom(path, ts)
        return top(path)

    @graph
    def g() -> TS[int]:
        register_adaptor("test_adaptor", my_adaptor_impl)
        return my_adaptor("test_adaptor", count(schedule(timedelta(milliseconds=10), max_ticks=10)))

    result = run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(milliseconds=250), __trace__=True)

    assert [x[1] for x in result] == list(range(1, 11))
