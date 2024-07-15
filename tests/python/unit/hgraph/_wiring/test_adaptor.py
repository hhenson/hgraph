from datetime import timedelta
from itertools import chain
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
    log_,
    combine,
    Size,
    TSL,
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


def test_adaptor_with_parameters():
    @push_queue(TS[int])
    def top(sender: Callable[[int], None], path: str) -> TS[int]:
        GlobalState.instance()[f"{path}/queue"] = sender
        return None

    @sink_node
    def bottom(path: str, ts: TS[int]):
        sender = GlobalState.instance().get(f"{path}/queue")
        sender(ts.value)

    @adaptor
    def my_adaptor(path: str, b: bool, ts: TS[int]) -> TS[int]: ...

    @adaptor_impl(interfaces=my_adaptor)
    def my_adaptor_impl(path: str, b: bool, ts: TS[int]) -> TS[int]:
        bottom(path, ts if b else ts + 1)
        return top(path)

    @graph
    def g() -> TSL[TS[int], Size[2]]:
        register_adaptor("test_adaptor", my_adaptor_impl)
        a1 = my_adaptor("test_adaptor", False, count(schedule(timedelta(milliseconds=10), max_ticks=10)))
        a2 = my_adaptor("test_adaptor", True, count(schedule(timedelta(milliseconds=11), max_ticks=10)))
        return combine(a1, a2)

    result = run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(milliseconds=250), __trace__=True)

    assert [x[1] for x in result] == list(chain(*zip([{0: x} for x in range(2, 12)], [{1: x} for x in range(1, 11)])))
