"""A reply-less request/reply service delivers in the client's own cycle.

RFC 0012. A request/reply service declaring no response is a sink: clients
send, the implementation consumes, nothing comes back. It used to be wired on
the rank-free request transport, which forwards on the NEXT cycle - rank-freedom
that exists to permit request/reply *cycles* via the response feedback edge. A
reply-less service builds no feedback, so it paid the latency for nothing.

The same shape as a sink-only adaptor always delivered in the same cycle; these
tests pin that the two now agree.
"""

import hgraph as hg
from hgraph import TS, TSD, graph
from hgraph.test import eval_node


def _cycle(clock):
    return int((clock.evaluation_time - hg.MIN_ST) / hg.MIN_TD)


def test_replyless_service_delivers_in_the_senders_cycle():
    seen = []

    @hg.request_reply_service
    def publish(path: str, value: TS[int]): ...

    @hg.service_impl(interfaces=publish)
    def publish_impl(values: TSD[int, TS[int]]):
        @hg.sink_node
        def observe(ts: TSD[int, TS[int]], _clock: hg.CLOCK = None):
            for _, v in ts.modified_items():
                seen.append((_cycle(_clock), v.value))

        observe(values)

    @graph
    def client(value: TS[int]) -> TS[int]:
        hg.register_service("events", publish_impl)
        publish("events", value)
        return value

    assert eval_node(
        client, [1, None, 2], __end_time__=hg.MIN_ST + 6 * hg.MIN_TD) == [1, None, 2]
    # Sent at cycles 0 and 2; seen at 0 and 2, not 1 and 3.
    assert seen == [(0, 1), (2, 2)], seen


def test_replyless_service_and_sink_only_adaptor_agree_on_timing():
    """The point of the change: two spellings of the same shape, same cycle."""
    service_seen = []
    adaptor_seen = []

    @hg.request_reply_service
    def svc_publish(path: str, value: TS[int]): ...

    @hg.service_impl(interfaces=svc_publish)
    def svc_impl(values: TSD[int, TS[int]]):
        @hg.sink_node
        def observe(ts: TSD[int, TS[int]], _clock: hg.CLOCK = None):
            for _, v in ts.modified_items():
                service_seen.append((_cycle(_clock), v.value))

        observe(values)

    @hg.adaptor
    def adp_publish(value: TS[int], path: str = "a"): ...

    @hg.adaptor_impl(interfaces=adp_publish)
    def adp_impl(value: TS[int], path: str = "a"):
        @hg.sink_node
        def observe(ts: TS[int], _clock: hg.CLOCK = None):
            adaptor_seen.append((_cycle(_clock), ts.value))

        observe(value)

    @graph
    def via_service(value: TS[int]) -> TS[int]:
        hg.register_service("s", svc_impl)
        svc_publish("s", value)
        return value

    @graph
    def via_adaptor(value: TS[int]) -> TS[int]:
        hg.register_adaptor("a", adp_impl)
        adp_publish(value, path="a")
        return value

    end = hg.MIN_ST + 6 * hg.MIN_TD
    eval_node(via_service, [1, None, 2], __end_time__=end)
    eval_node(via_adaptor, [1, None, 2], __end_time__=end)
    assert service_seen == adaptor_seen, (service_seen, adaptor_seen)


def test_replyless_service_keys_multiple_clients_by_request_id():
    """Keying survives the scheduling change - only the timing moved."""
    seen = []

    @hg.request_reply_service
    def publish(path: str, value: TS[int]): ...

    @hg.service_impl(interfaces=publish)
    def publish_impl(values: TSD[int, TS[int]]):
        @hg.sink_node
        def observe(ts: TSD[int, TS[int]]):
            seen.append({k: v.value for k, v in ts.modified_items()})

        observe(values)

    @graph
    def client(value: TS[int]) -> TS[int]:
        hg.register_service("events", publish_impl)
        publish("events", value)
        publish("events", value + 100)
        return value

    eval_node(client, [1], __end_time__=hg.MIN_ST + 6 * hg.MIN_TD)
    # Two clients, two distinct request ids, one cumulative delta.
    assert seen and len(seen[0]) == 2, seen
    assert sorted(seen[0].values()) == [1, 101], seen


def test_reply_full_request_reply_timing_is_unchanged():
    """The rank-free path is load-bearing when a response exists."""
    @hg.request_reply_service
    def double(value: TS[int], path: str = "d") -> TS[int]: ...

    @hg.service_impl(interfaces=double)
    def double_impl(value: TSD[int, TS[int]], path: str = "d") -> TSD[int, TS[int]]:
        return hg.map_(lambda v: v * 2, value)

    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_service("d", double_impl)
        return double(value, path="d")

    # Request and response each cross their own transport boundary.
    out = eval_node(app, [5], __end_time__=hg.MIN_ST + 6 * hg.MIN_TD)
    assert out == [None, None, 10], out
