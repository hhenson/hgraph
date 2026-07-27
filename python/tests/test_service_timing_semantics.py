"""Service-boundary and mapped-service reduction timing semantics.

The scheduling matrix in ``docs/source/developer_guide/services.rst`` is the
design record: request stubs forward next cycle by design, and a late or
duplicate subscription samples the existing shared output in the same cycle.
Released hgraph observably differs on both edges (same-cycle adaptor round
trips; one-cycle re-subscription re-delivery). See the Accepted Deviations
list in ``roadmap.rst`` and parity issues #64 / #66.

Issue #95 is a separate reduce boundary: released hgraph waits when a keyed
mapped slot is live but not yet valid, whereas hg_cpp reduces the currently
valid values. The eventual service payloads and aggregates remain identical.
"""

import hgraph as hg
from hgraph import TS, TSD, TSS, graph
from hgraph.test import eval_node


def test_service_adaptor_roundtrip_takes_one_transport_cycle():
    # Issue #64: released hgraph completes the adaptor round trip in the
    # same engine cycle ([1]); hg_cpp's request stub forwards next cycle by
    # design, so the reply lands one cycle later.
    @hg.service_adaptor
    def echo(request: TS[int]) -> TS[int]: ...

    @hg.service_adaptor_impl(interfaces=echo)
    def echo_impl(path: str, request: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return hg.map_(lambda value: value + 0, request)

    @graph
    def roundtrip(value: TS[int]) -> TS[int]:
        hg.register_adaptor(None, echo_impl)
        return echo(value)

    out = eval_node(roundtrip, [1], __end_time__=hg.MIN_ST + 3 * hg.MIN_TD)
    assert out == [None, 1]


def test_resubscription_samples_existing_value_in_the_same_cycle():
    # Issue #66: re-subscribing a previously computed symbol samples the
    # existing shared-output entry in the same cycle; released hgraph takes
    # the usual one-cycle transport hop again. First subscriptions match
    # upstream exactly (value one cycle after subscribe).
    @hg.subscription_service
    def quote(path: str, symbol: TS[str]) -> TS[int]: ...

    @graph
    def quote_value(symbol: TS[str]) -> TS[int]:
        return hg.len_(symbol) * 7

    @hg.service_impl(interfaces=quote)
    def quote_values(symbol: TSS[str]) -> TSD[str, TS[int]]:
        return hg.map_(quote_value, __keys__=symbol, __key_arg__="symbol")

    @graph
    def subscribe(symbol: TS[str]) -> TS[int]:
        hg.register_service("live", quote_values)
        return quote("live", symbol)

    out = eval_node(
        subscribe,
        ["rates", None, "fx", "rates"],
        __end_time__=hg.MIN_ST + 7 * hg.MIN_TD,
    )
    assert out == [None, 35, None, 35]


def test_request_reply_inside_a_mapped_switch_keeps_late_keys():
    @hg.request_reply_service
    def adjust(path: str, request: TS[int]) -> TS[int]: ...

    @hg.service_impl(interfaces=adjust)
    def adjust_impl(request: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return hg.map_(lambda value: value + 2, request)

    @graph
    def alpha(value: TS[int]) -> TS[int]:
        return adjust("mapped-request-reply", value)

    @graph
    def beta(value: TS[int]) -> TS[int]:
        return value * 2 - 2

    @graph
    def per_key(key: TS[str], value: TS[int], selector: TS[str]) -> TS[int]:
        del key
        return hg.switch_(selector, {"alpha": alpha, "beta": beta}, value)

    @graph
    def app(values: TSD[str, TS[int]], selector: TS[str]) -> TS[int]:
        hg.register_service("mapped-request-reply", adjust_impl)
        mapped = hg.map_(per_key, values, selector)
        return hg.reduce(lambda lhs, rhs: lhs + rhs, mapped, 0)

    assert eval_node(
        app,
        [{"k1": 3}, {"k2": 4}, None, {"k1": hg.REMOVE}],
        ["alpha", None, "beta", None],
        __end_time__=hg.MIN_ST + 10 * hg.MIN_TD,
    ) == [None, None, 10, 6]


def test_subscription_inside_a_mapped_switch_keeps_late_keys():
    @hg.subscription_service
    def quote(path: str, symbol: TS[str]) -> TS[int]: ...

    @graph
    def quote_value(symbol: TS[str]) -> TS[int]:
        return hg.len_(symbol) * 2

    @hg.service_impl(interfaces=quote)
    def quote_impl(symbol: TSS[str]) -> TSD[str, TS[int]]:
        return hg.map_(quote_value, __keys__=symbol, __key_arg__="symbol")

    @graph
    def alpha(value: TS[int]) -> TS[int]:
        return value + 2

    @graph
    def beta(value: TS[int]) -> TS[int]:
        return value * 2 - 2

    @graph
    def per_key(key: TS[str], value: TS[int], selector: TS[str]) -> TS[int]:
        quoted = quote("mapped-subscription", key) + value
        return hg.switch_(selector, {"alpha": alpha, "beta": beta}, quoted)

    @graph
    def app(values: TSD[str, TS[int]], selector: TS[str]) -> TS[int]:
        hg.register_service("mapped-subscription", quote_impl)
        mapped = hg.map_(per_key, values, selector)
        return hg.reduce(lambda lhs, rhs: lhs + rhs, mapped, 0)

    # Issue #95: k1's first response is already valid while k2 is a phantom
    # mapped slot. hg_cpp publishes that valid subset (9); released hgraph
    # waits one cycle. Once k2 becomes valid, both publish 26 and then 14.
    assert eval_node(
        app,
        [{"k1": 3}, {"k2": 4}, None, {"k1": hg.REMOVE}],
        ["alpha", None, "beta", None],
        __end_time__=hg.MIN_ST + 10 * hg.MIN_TD,
    ) == [None, 9, 26, 14]
