"""Push queues inside service implementations.

A service/adaptor implementation is not a nested graph in hg_cpp: registration
records a materializer candidate, and ``Wiring::build_services()`` — reached only
from ``finish_top_level`` — runs it with ``WiredFn::wire(target, ...)``, which
inlines the implementation into the top-level wiring. An implementation's
``@push_queue`` is therefore an ordinary root-graph push-prefix node.

Upstream Python compiles a ``@service_impl`` into a nested graph node built with
``create_graph_builder(sink_nodes, False)`` and rejects push sources inside it;
genuine nested children here still reject them, which the last test pins.

The C++ counterpart is ``tests/cpp/test_service_push_sources.cpp``. These tests
deliberately use no external resources — the only other Python coverage of
push-inside-implementation lives in the tornado/sql/json/delta adaptor suites,
and the one ``@service_impl`` case (kafka) needs a broker.
"""

import datetime
import threading
import time

import pytest

import hgraph as hg
from hgraph import TS, TSD, TSS, graph


def _run(g, seconds=2.0):
    end = datetime.datetime.now(datetime.UTC).replace(tzinfo=None) + datetime.timedelta(
        seconds=seconds)
    hg.run_graph(g, end_time=end, run_mode=hg.EvaluationMode.REAL_TIME)


def _feeder(*values, delay=0.15):
    """A ``@push_queue`` start hook that feeds ``values`` from a side thread.

    Returns ``(hook, threads)``; ``threads`` is populated when the graph starts
    the push node, so tests join it after the run rather than before.
    """
    threads = []

    def hook(sender):
        def feed():
            time.sleep(delay)
            for value in values:
                sender(value)
                time.sleep(0.02)

        thread = threading.Thread(target=feed)
        threads.append(thread)
        thread.start()

    return hook, threads


def test_reference_service_impl_may_own_a_push_queue():
    collected = []
    hook, threads = _feeder(42)

    @hg.reference_service
    def live_ticks(path: str = "ticks") -> TS[int]: ...

    @hg.service_impl(interfaces=live_ticks)
    def live_ticks_impl(path: str = "ticks") -> TS[int]:
        @hg.push_queue(TS[int])
        def source(sender):
            hook(sender)

        return source()

    @hg.sink_node
    def collect(value: TS[int]) -> None:
        collected.append(value.value)

    @graph
    def live() -> None:
        hg.register_service("ticks", live_ticks_impl)
        collect(live_ticks(path="ticks"))

    _run(live)
    for thread in threads:
        thread.join()
    assert collected == [42]


def test_subscription_service_impl_may_own_a_push_queue():
    collected = []
    hook, threads = _feeder(99)

    @hg.subscription_service
    def quotes(key: TS[str], path: str = "quotes") -> TS[int]: ...

    @hg.service_impl(interfaces=quotes)
    def quotes_impl(keys: TSS[str], path: str = "quotes") -> TSD[str, TS[int]]:
        @hg.push_queue(TS[int])
        def source(sender):
            hook(sender)

        tick = source()
        # Broadcast each pushed value to every live subscription key.
        return hg.map_(lambda t: t, __keys__=keys, t=tick)

    @hg.sink_node
    def collect(value: TS[int]) -> None:
        collected.append(value.value)

    @graph
    def live() -> None:
        hg.register_service("quotes", quotes_impl)
        collect(quotes(hg.const("instrument"), path="quotes"))

    _run(live)
    for thread in threads:
        thread.join()
    assert collected == [99]


def test_request_reply_service_impl_may_own_a_push_queue():
    collected = []
    hook, threads = _feeder(55)

    @hg.request_reply_service
    def echo(value: TS[int], path: str = "echo") -> TS[int]: ...

    @hg.service_impl(interfaces=echo)
    def echo_impl(value: TSD[int, TS[int]], path: str = "echo") -> TSD[int, TS[int]]:
        @hg.push_queue(TS[int])
        def source(sender):
            hook(sender)

        tick = source()
        # Reply to every outstanding request with the latest pushed value.
        return hg.map_(lambda v, t: t, value, t=tick)

    @hg.sink_node
    def collect(value: TS[int]) -> None:
        collected.append(value.value)

    @graph
    def live() -> None:
        hg.register_service("echo", echo_impl)
        collect(echo(hg.const(1), path="echo"))

    _run(live)
    for thread in threads:
        thread.join()
    assert collected == [55]


def test_push_queue_inside_a_nested_graph_is_still_rejected():
    """The complementary edge: inlining is what makes the impl case legal."""

    @hg.push_queue(TS[int])
    def source(sender):
        pass

    @graph
    def child(key: TS[str]) -> TS[int]:
        return source()

    @graph
    def live() -> None:
        keys = hg.convert[TSS](hg.const("a"))
        hg.debug_print("out", hg.map_(child, __keys__=keys))

    with pytest.raises(Exception, match="[Nn]ested graphs do not support push source"):
        _run(live, seconds=0.5)
