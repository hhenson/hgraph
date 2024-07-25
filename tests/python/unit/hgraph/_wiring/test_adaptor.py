from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass, field
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
    TSD,
    service_adaptor,
    service_adaptor_impl,
    map_,
    STATE,
    CompoundScalar,
    const,
    TSS,
    format_,
    subscription_service,
    service_impl,
    collect,
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

    result = run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(milliseconds=1000), __trace__=True)

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
        path = f"{path}_{b}"  # path comes in its plain form i.e. without parameters baked in so we have to do it here
        bottom(path, ts if b else ts + 1)
        return top(path)

    @graph
    def g() -> TSL[TS[int], Size[2]]:
        register_adaptor("test_adaptor", my_adaptor_impl)
        a1 = my_adaptor("test_adaptor", False, count(schedule(timedelta(milliseconds=10), max_ticks=10)))
        a2 = my_adaptor("test_adaptor", True, count(schedule(timedelta(milliseconds=11), max_ticks=10)))
        return combine(a1, a2)

    result = run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(milliseconds=250))

    assert [x[1] for x in result] == list(chain(*zip([{0: x} for x in range(2, 12)], [{1: x} for x in range(1, 11)])))


def test_multi_client_adaptor_w_parameters():
    # in this test we create a multi-client adaptor that takes parameters and have two clients in a map_
    # to one instance of the adaptor and one client to another instance of the adaptor with different parameters

    @push_queue(TSD[int, TS[int]])
    def top(sender: Callable[[int], None], path: str) -> TS[int]:
        GlobalState.instance()[f"{path}/queue"] = sender
        return None

    @dataclass
    class ThreadExec(CompoundScalar):
        exec: Executor = field(default_factory=lambda: ThreadPoolExecutor(max_workers=1))

    @sink_node
    def bottom(path: str, ts: TSD[int, TS[int]], _state: STATE[ThreadExec] = None):
        sender = GlobalState.instance().get(f"{path}/queue")
        _state.exec.submit(sender, ts.delta_value)

    @bottom.stop
    def bottom_stop(_state):
        _state.exec.shutdown()

    @service_adaptor
    def my_adaptor(path: str, b: bool, ts: TS[int]) -> TS[int]: ...

    @service_adaptor_impl(interfaces=my_adaptor)
    def my_adaptor_impl(path: str, b: bool, ts: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        bottom(path, map_(lambda t: t if b else t + 1, ts))
        return top(path)

    @graph
    def g() -> TSL[TS[int], Size[3]]:
        register_adaptor("test_adaptor", my_adaptor_impl)
        a = map_(
            lambda key: my_adaptor(
                "test_adaptor", False, count(schedule(combine[TS[timedelta]](milliseconds=key), max_ticks=5))
            ),
            __keys__=const(frozenset({100, 110}), TSS[int]),
        )
        a3 = my_adaptor("test_adaptor", True, count(schedule(timedelta(milliseconds=120), max_ticks=5)))
        return combine(a[100], a[110], a3)

    result = run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(milliseconds=1000))

    assert [x[1] for x in result] == list(
        chain(*zip([{0: x} for x in range(2, 7)], [{1: x} for x in range(2, 7)], [{2: x} for x in range(1, 6)]))
    )


def test_mutli_adaptor_sink_only():
    @service_adaptor
    def my_adaptor(path: str, ts: TS[int]): ...

    @service_adaptor_impl(interfaces=my_adaptor)
    def my_adaptor_impl(path: str, ts: TSD[int, TS[int]]):
        log_(f"{path}: {{}}", ts)

    @graph
    def g(ts: TS[int]):
        register_adaptor("test_adaptor", my_adaptor_impl)
        my_adaptor("test_adaptor", ts)

    eval_node(g, [1, None, 2])


def test_adaptor_source_only():
    @adaptor
    def my_adaptor(path: str) -> TS[int]: ...

    @adaptor_impl(interfaces=my_adaptor)
    def my_adaptor_impl(path: str) -> TS[int]:
        return count(schedule(timedelta(microseconds=10), initial_delay=True, max_ticks=10))

    @graph
    def g() -> TS[int]:
        register_adaptor("test_adaptor", my_adaptor_impl)
        return my_adaptor("test_adaptor")

    assert eval_node(g, __elide__=True) == list(range(1, 11))


def test_adaptor_and_service():
    @push_queue(TSD[int, TS[int]])
    def top(sender: Callable[[int], None], path: str) -> TS[int]:
        GlobalState.instance()[f"{path}/queue"] = sender
        return None

    @sink_node
    def bottom(path: str, ts: TSS[int]):
        sender = GlobalState.instance().get(f"{path}/queue")
        sender({x: x for x in ts.added()})

    @adaptor
    def my_adaptor(path: str, ts: TSS[int]) -> TSD[int, TS[int]]: ...

    @adaptor_impl(interfaces=my_adaptor)
    def my_adaptor_impl(path: str, ts: TSS[int]) -> TSD[int, TS[int]]:
        bottom(path, ts)
        return top(path)

    @subscription_service
    def my_service(path: str, ts: TS[int]) -> TS[int]: ...

    @service_impl(interfaces=my_service)
    def my_service_impl(path: str, ts: TSS[int]) -> TSD[int, TS[int]]:
        return my_adaptor(path, ts)

    @graph
    def g() -> TSD[int, TS[int]]:
        register_adaptor("test", my_adaptor_impl)
        register_service("test", my_service_impl)

        requests = collect[TSS](count(schedule(timedelta(milliseconds=10), max_ticks=10)))
        return map_(lambda key: my_service("test", key), __keys__=requests)

    result = run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(milliseconds=250))

    assert [x[1] for x in result if x[1]] == [{x: x} for x in list(range(1, 11))]
