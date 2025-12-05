from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import timedelta
from itertools import chain
from typing import Callable

import pytest

from hgraph import (
    push_queue,
    TS,
    GlobalState,
    sink_node,
    graph,
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
    subscription_service,
    service_impl,
    collect,
    GraphConfiguration,
    evaluate_graph,
    switch_,
    TIME_SERIES_TYPE,
    convert,
    REMOVE,
    feedback,
    stop_engine,
    compute_node,
    EvaluationEngineApi,
)
from hgraph._wiring._decorators import GRAPH_SIGNATURE
from hgraph.nodes._service_utils import write_adaptor_request, adaptor_request, capture_output_node_to_global_state
from hgraph.test import eval_node


# ============================================================================
# Shared helper functions for adaptor tests
# ============================================================================

@push_queue(TS[int])
def queue_source(sender: Callable[[int], None], path: str) -> TS[int]:
    """Push queue source that stores sender in GlobalState for later use."""
    GlobalState.instance()[f"{path}/queue"] = sender
    return None


@push_queue(TSD[int, TS[int]])
def tsd_queue_source(sender: Callable[[int], None], path: str) -> TSD[int, TS[int]]:
    """Push queue source for TSD that stores sender in GlobalState."""
    GlobalState.instance()[f"{path}/queue"] = sender
    return None


@sink_node
def queue_sink(path: str, ts: TS[int]):
    """Sink node that forwards values to the queue sender stored in GlobalState."""
    sender = GlobalState.instance().get(f"{path}/queue")
    sender(ts.value)


@sink_node
def tsd_queue_sink(path: str, ts: TSD[int, TS[int]]):
    """Sink node for TSD that forwards delta values to the queue sender."""
    sender = GlobalState.instance().get(f"{path}/queue")
    sender(ts.delta_value)


@sink_node
def tss_queue_sink(path: str, ts: TSS[int]):
    """Sink node for TSS that forwards added values as dict to the queue sender."""
    sender = GlobalState.instance().get(f"{path}/queue")
    sender({x: x for x in ts.added()})


@sink_node
def stop_on_value(ts: TS[int], expected_last: int, _engine: EvaluationEngineApi = None):
    """Stop engine when a single TS[int] reaches the expected value."""
    if ts.value == expected_last:
        _engine.request_engine_stop()


@sink_node
def stop_on_tsl_values(
    ts: TSL[TS[int], Size[2]],
    expected0: int,
    expected1: int,
    _engine: EvaluationEngineApi = None,
    _state: STATE = None,
):
    """Stop engine when both elements of a TSL[TS[int], Size[2]] reach expected values."""
    if not hasattr(_state, 'done0'):
        _state.done0 = False
        _state.done1 = False
    if ts[0].modified and ts[0].value == expected0:
        _state.done0 = True
    if ts[1].modified and ts[1].value == expected1:
        _state.done1 = True
    if _state.done0 and _state.done1:
        _engine.request_engine_stop()


@sink_node
def stop_on_tsd_value(
    ts: TSD[int, TS[int]],
    expected_key: int,
    expected_value: int,
    _engine: EvaluationEngineApi = None,
):
    """Stop engine when a TSD contains the expected key with expected value."""
    if expected_key in ts.delta_value and ts.delta_value[expected_key] == expected_value:
        _engine.request_engine_stop()


# ============================================================================
# Tests
# ============================================================================

def test_adaptor():
    @adaptor
    def my_adaptor(path: str, ts: TS[int]) -> TS[int]: ...

    @adaptor_impl(interfaces=my_adaptor)
    def my_adaptor_impl(path: str, ts: TS[int]) -> TS[int]:
        queue_sink(path, ts)
        return queue_source(path)

    @graph
    def g() -> TS[int]:
        register_adaptor("test_adaptor", my_adaptor_impl)
        result = my_adaptor("test_adaptor", count(schedule(timedelta(milliseconds=10), max_ticks=10)))
        stop_on_value(result, 10)
        return result

    result = evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(milliseconds=1000)))

    assert [x[1] for x in result] == list(range(1, 11))


def test_adaptor_with_parameters():
    @adaptor
    def my_adaptor(path: str, b: bool, ts: TS[int]) -> TS[int]: ...

    @adaptor_impl(interfaces=my_adaptor)
    def my_adaptor_impl(path: str, b: bool, ts: TS[int]) -> TS[int]:
        path = f"{path}_{b}"  # path comes in its plain form i.e. without parameters baked in so we have to do it here
        queue_sink(path, ts if b else ts + 1)
        return queue_source(path)

    @graph
    def g() -> TSL[TS[int], Size[2]]:
        register_adaptor("test_adaptor", my_adaptor_impl)
        a1 = my_adaptor("test_adaptor", False, count(schedule(timedelta(milliseconds=10), max_ticks=10)))
        a2 = my_adaptor("test_adaptor", True, count(schedule(timedelta(milliseconds=11), max_ticks=10)))
        result = combine(a1, a2)
        stop_on_tsl_values(result, 11, 10)  # False adds +1, so 10+1=11; True keeps as-is so 10
        return result

    result = evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(milliseconds=250)))

    assert [x[1] for x in result] == list(chain(*zip([{0: x} for x in range(2, 12)], [{1: x} for x in range(1, 11)])))


def test_adaptor_with_impl_parameters():
    @service_adaptor
    def my_adaptor(path: str, ts: TS[int]) -> TS[int]: ...

    @service_adaptor_impl(interfaces=my_adaptor)
    def my_adaptor_impl(path: str, b: bool, ts: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        path = f"{path}_{b}"  # path comes in its plain form i.e. without parameters baked in so we have to do it here
        tsd_queue_sink(path, ts if b else map_(lambda x: x + 1, ts))
        return tsd_queue_source(path)

    @graph
    def g() -> TSL[TS[int], Size[2]]:
        register_adaptor(None, my_adaptor_impl, b=False)
        a1 = my_adaptor("test_adaptor", ts=count(schedule(timedelta(milliseconds=10), max_ticks=10)))
        a2 = my_adaptor("test_adaptor", ts=count(schedule(timedelta(milliseconds=11), max_ticks=10)))
        result = combine(a1, a2)
        stop_on_tsl_values(result, 11, 11)  # Both add +1, so 10+1=11 for both
        return result

    result = evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(milliseconds=250)))

    assert [x[1] for x in result] == list(chain(*zip([{0: x} for x in range(2, 12)], [{1: x} for x in range(2, 12)])))


@pytest.mark.xfail(reason="Fails in CICD too often")
def test_multi_client_adaptor_w_parameters():
    # in this test we create a multi-client adaptor that takes parameters and have two clients in a map_
    # to one instance of the adaptor and one client to another instance of the adaptor with different parameters

    @dataclass
    class ThreadExec(CompoundScalar):
        exec: Executor = field(default_factory=lambda: ThreadPoolExecutor(max_workers=1))

    @sink_node
    def threaded_tsd_sink(path: str, ts: TSD[int, TS[int]], _state: STATE[ThreadExec] = None):
        sender = GlobalState.instance().get(f"{path}/queue")
        _state.exec.submit(sender, ts.delta_value)

    @threaded_tsd_sink.stop
    def threaded_tsd_sink_stop(_state):
        _state.exec.shutdown()

    @service_adaptor
    def my_adaptor(path: str, b: bool, ts: TS[int]) -> TS[int]: ...

    @service_adaptor_impl(interfaces=my_adaptor)
    def my_adaptor_impl(path: str, b: bool, ts: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        threaded_tsd_sink(path, map_(lambda t: t if b else t + 1, ts))
        return tsd_queue_source(path)

    @compute_node
    def check_count(ts: TSL[TS[int], Size[3]], expected: int, _state: STATE = None) -> TS[bool]:
        if not hasattr(_state, 'count'):
            _state.count = 0
        _state.count += 1
        return _state.count >= expected

    @graph
    def stop_at_count(ts: TSL[TS[int], Size[3]], expected: int) -> TSL[TS[int], Size[3]]:
        done = check_count(ts, expected)
        stop_engine(done, "Reached expected count")
        return ts

    # TODO: Find a way to make this test more reliable when under load.
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
        return stop_at_count(combine(a[100], a[110], a3), 15)

    config = GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=1))
    result = evaluate_graph(g, config)
    final_result = [(k, v) for d in [x[1] for x in result] for k, v in d.items()]
    expected = [
        (k, v)
        for d in chain(
            *zip([{0: x} for x in range(2, 7)], [{1: x} for x in range(2, 7)], [{2: x} for x in range(1, 6)])
        )
        for k, v in d.items()
    ]
    assert final_result == expected


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
    @adaptor
    def my_adaptor(path: str, ts: TSS[int]) -> TSD[int, TS[int]]: ...

    @adaptor_impl(interfaces=my_adaptor)
    def my_adaptor_impl(path: str, ts: TSS[int]) -> TSD[int, TS[int]]:
        tss_queue_sink(path, ts)
        return tsd_queue_source(path)

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
        result = map_(lambda key: my_service("test", key), __keys__=requests)
        stop_on_tsd_value(result, 10, 10)  # Stop when key 10 has value 10
        return result

    result = evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(milliseconds=250)))

    assert [x[1] for x in result if x[1]] == [{x: x} for x in list(range(1, 11))]


@pytest.mark.skip("not implemented yet")
def test_service_impl_via_adaptor():
    @service_adaptor
    def my_adaptor(path: str, ts: TS[int]) -> TS[int]: ...

    @adaptor_impl(interfaces=my_adaptor)
    def my_adaptor_impl(path: str, service_impl: GRAPH_SIGNATURE, ts: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return service_impl(path, feedback(ts)())

    @service_impl
    def my_service_impl(path: str, ts: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return map_(lambda x: x + 1, ts)

    @graph
    def g() -> TS[int]:
        register_adaptor("test", my_adaptor_impl, service_impl=my_service_impl)
        return my_adaptor("test", count(schedule(timedelta(milliseconds=10), max_ticks=10)))


def test_write_adaptor_request():
    @graph
    def w(path: str, arg: str, request: TS[bool], request_id: TS[int]) -> TS[int]:
        write_adaptor_request(path, arg, request, request_id, __return_sink_wp__=True)
        return request_id

    @graph
    def g(i: TS[int], x: TS[bool]) -> TSD[int, TS[bool]]:
        s = map_(
            lambda key, v: switch_(
                v, {True: lambda x, i: w("test", "foo", x, i), False: lambda x, i: w("test", "foo", x, i)}, x=v, i=key
            ),
            convert[TSD](key=i, ts=x),
        )

        out = adaptor_request[TIME_SERIES_TYPE : TS[bool]]("test", "foo")
        out.node_instance.add_indirect_dependency(s.node_instance)
        capture_output_node_to_global_state(f"test/foo", out)
        return out

    assert eval_node(g, i=[1, None, 2], x=[True, False, True]) == [
        {1: True},
        {1: False},
        {1: REMOVE, 2: True},
    ]
