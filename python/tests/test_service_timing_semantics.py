"""Service-boundary and mapped-service reduction timing semantics.

The scheduling matrix in ``docs/source/developer_guide/services.rst`` is the
design record: adaptors are rank-correct and same-cycle, subscription changes
are delivered in order, and self-coupled request/reply retains one temporal
break while publishing its response directly.

Issue #95 is a separate reduce boundary: released hgraph waits when a keyed
mapped slot is live but not yet valid, whereas hg_cpp reduces the currently
valid values. The eventual service payloads and aggregates remain identical.

The same valid-output rule applies to an unreduced map: when a mapped child
becomes transiently invalid, hg_cpp removes its element until the child
validates again instead of retaining a stale value behind an empty delta.
"""

import pytest

import hgraph as hg
from hgraph import TS, TSD, TSS, graph
from hgraph.test import eval_node


_MIXED_SERVICE_INTERFACE_ORDERS = (
    "rR", "Rr", "sS", "Ss", "qQ", "Qq",
    "rs", "sr", "rq", "qr", "sq", "qs",
    "rsq", "rqs", "srq", "sqr", "qrs", "qsr",
)


@pytest.mark.parametrize("order", _MIXED_SERVICE_INTERFACE_ORDERS)
def test_multi_interface_service_flavours_are_order_independent(order):
    """Every mixed service pack shares one implementation in either order.

    Lowercase codes are reference, subscription, and request/reply; uppercase
    codes are a second interface of the same flavour. Wiring the implementation
    stubs and clients in the declared order catches order-sensitive endpoint
    discovery and transport-planner finalization.
    """
    @hg.reference_service
    def current(path: str = "mixed") -> TS[int]: ...

    @hg.reference_service
    def previous(path: str = "mixed") -> TS[int]: ...

    @hg.subscription_service
    def price(key: TS[int], path: str = "mixed") -> TS[int]: ...

    @hg.subscription_service
    def premium_price(key: TS[int], path: str = "mixed") -> TS[int]: ...

    @hg.request_reply_service
    def adjust(value: TS[int], path: str = "mixed") -> TS[int]: ...

    @hg.request_reply_service
    def adjust_ten(value: TS[int], path: str = "mixed") -> TS[int]: ...

    by_flavour = {
        "r": current, "R": previous,
        "s": price, "S": premium_price,
        "q": adjust, "Q": adjust_ten,
    }
    compositions = []

    @hg.service_impl(interfaces=tuple(by_flavour[item] for item in order))
    def impl(path: str):
        compositions.append(tuple(order))
        for flavour in order:
            if flavour == "r":
                current.wire_impl_out_stub(path, hg.const(10))
            elif flavour == "R":
                previous.wire_impl_out_stub(path, hg.const(11))
            elif flavour == "s":
                keys = price.wire_impl_inputs_stub(path).key
                price.wire_impl_out_stub(
                    path,
                    hg.map_(lambda key: hg.const(20), __keys__=keys),
                )
            elif flavour == "S":
                keys = premium_price.wire_impl_inputs_stub(path).key
                premium_price.wire_impl_out_stub(
                    path,
                    hg.map_(lambda key: hg.const(21), __keys__=keys),
                )
            elif flavour == "q":
                requests = adjust.wire_impl_inputs_stub(path).value
                adjust.wire_impl_out_stub(
                    path,
                    hg.map_(lambda value: value + 1, requests),
                )
            else:
                requests = adjust_ten.wire_impl_inputs_stub(path).value
                adjust_ten.wire_impl_out_stub(
                    path,
                    hg.map_(lambda value: value + 10, requests),
                )

    @graph
    def app(key: TS[int], request: TS[int]) -> TS[int]:
        hg.register_service("mixed", impl)
        clients = []
        for flavour in order:
            if flavour == "r":
                clients.append(current(path="mixed"))
            elif flavour == "R":
                clients.append(previous(path="mixed"))
            elif flavour == "s":
                clients.append(price(key, path="mixed"))
            elif flavour == "S":
                clients.append(premium_price(key, path="mixed"))
            elif flavour == "q":
                clients.append(adjust(request, path="mixed"))
            else:
                clients.append(adjust_ten(request, path="mixed"))
        result = clients[0]
        for client in clients[1:]:
            result = result + client
        return result

    expected = sum(
        {"r": 10, "R": 11, "s": 20, "S": 21, "q": 8, "Q": 17}[item]
        for item in order
    )
    expected_trace = (
        [None, expected] if any(item in "sSqQ" for item in order)
        else [expected]
    )
    assert eval_node(
        app, [2], [7], __end_time__=hg.MIN_ST + 6 * hg.MIN_TD,
    ) == expected_trace
    assert compositions == [tuple(order)]


def test_service_adaptor_roundtrip_is_same_cycle():
    # Issue #64: service adaptors rank the client before the implementation
    # and the reply after it, completing the round trip in one engine cycle.
    @hg.service_adaptor
    def echo(request: TS[int]) -> TS[int]: ...

    @hg.service_adaptor_impl(interfaces=echo)
    def echo_impl(path: str, request: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return hg.map_(lambda value: value + 0, request)

    @graph
    def roundtrip(value: TS[int]) -> TS[int]:
        hg.register_adaptor(None, echo_impl)
        return echo(value)

    out = eval_node(roundtrip, [1, 2, 3], __end_time__=hg.MIN_ST + 5 * hg.MIN_TD)
    assert out == [1, 2, 3]


def test_subscription_replacement_waits_for_fresh_value_and_preserves_transitions():
    # Issue #66: a fast rates -> fx -> rates sequence must reach the
    # implementation as three distinct transitions. The second rates child is
    # fresh; a cached first-generation value must not leak during replacement.
    calls = {}
    transitions = []

    @hg.subscription_service
    def quote(path: str, symbol: TS[str]) -> TS[int]: ...

    @hg.compute_node
    def generation(symbol: TS[str]) -> TS[int]:
        key = symbol.value
        calls[key] = calls.get(key, 0) + 1
        return calls[key]

    @graph
    def quote_value(symbol: TS[str]) -> TS[int]:
        return generation(symbol)

    @hg.sink_node
    def observe(symbols: TSS[str]):
        transitions.append((sorted(symbols.added()), sorted(symbols.removed())))

    @hg.service_impl(interfaces=quote)
    def quote_values(symbol: TSS[str]) -> TSD[str, TS[int]]:
        observe(symbol)
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
    assert out == [None, 1, None, None, 2]
    assert transitions == [(["rates"], []), (["fx"], ["rates"]), (["rates"], ["fx"])]
    assert calls == {"rates": 2, "fx": 1}


def test_decoupled_subscription_from_to_graph_is_same_cycle():
    """An external response source does not pay a synthetic subscription cycle."""
    transitions = []

    @hg.subscription_service
    def quote(symbol: TS[str], path: str = "direct-quotes") -> TS[int]: ...

    @hg.reference_service
    def transport_ready(path: str = "direct-quotes") -> TS[bool]: ...

    @hg.sink_node
    def observe(symbols: TSS[str]):
        transitions.append((sorted(symbols.added()), sorted(symbols.removed())))

    @hg.service_impl(interfaces=(quote, transport_ready))
    def quote_transport(path: str):
        symbols = hg.from_graph(quote, path)
        observe(symbols)
        hg.to_graph(
            quote,
            hg.const({"rates": 70}, TSD[str, TS[int]]),
            path,
        )
        hg.to_graph(transport_ready, hg.const(True), path)

    @graph
    def app(symbol: TS[str]) -> TS[int]:
        hg.register_service("direct-quotes", quote_transport)
        return quote(symbol, path="direct-quotes")

    assert eval_node(
        app,
        ["rates"],
        __end_time__=hg.MIN_ST + 3 * hg.MIN_TD,
    ) == [70]
    assert transitions == [(["rates"], [])]


def test_service_dependent_subscription_defers_only_its_key_relay():
    @hg.reference_service
    def offset(path: str = "subscription-offset") -> TS[int]: ...

    @hg.service_impl(interfaces=offset)
    def offset_impl(path: str = "subscription-offset") -> TS[int]:
        return hg.const(10)

    @hg.subscription_service
    def quote(symbol: TS[str], path: str = "dependent-quotes") -> TS[int]: ...

    @hg.service_impl(interfaces=quote)
    def quote_impl(symbols: TSS[str]) -> TSD[str, TS[int]]:
        amount = offset(path="subscription-offset")
        return hg.map_(
            lambda symbol, value: hg.len_(symbol) * 10 + value,
            __keys__=symbols,
            __key_arg__="symbol",
            value=amount,
        )

    @graph
    def app(symbol: TS[str]) -> TS[int]:
        hg.register_service("subscription-offset", offset_impl)
        hg.register_service("dependent-quotes", quote_impl)
        return quote(symbol, path="dependent-quotes")

    assert eval_node(
        app,
        ["rates"],
        __end_time__=hg.MIN_ST + 5 * hg.MIN_TD,
    ) == [None, 60]


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
    ) == [0, 5, 10, 6]


def test_request_reply_switch_flip_removes_transiently_invalid_map_output():
    @hg.request_reply_service
    def adjust(path: str, request: TS[int]) -> TS[int]: ...

    @hg.service_impl(interfaces=adjust)
    def adjust_impl(request: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return hg.map_(lambda value: value + 2, request)

    @graph
    def alpha(value: TS[int]) -> TS[int]:
        return adjust("mapped-request-reply-removal", value)

    @graph
    def beta(value: TS[int]) -> TS[int]:
        return value * 2 - 2

    @graph
    def per_key(key: TS[str], value: TS[int], selector: TS[str]) -> TS[int]:
        del key
        return hg.switch_(selector, {"alpha": alpha, "beta": beta}, value)

    @graph
    def app(
        values: TSD[str, TS[int]], selector: TS[str]
    ) -> TSD[str, TS[int]]:
        hg.register_service("mapped-request-reply-removal", adjust_impl)
        return hg.map_(per_key, values, selector)

    # Issues #105/#117/#119/#133/#145: changing branches invalidates the
    # switch output while the request/reply response is in flight. A TSD
    # contains only valid child outputs, so the old beta value is explicitly
    # removed and the alpha response adds the key on the following tick.
    assert eval_node(
        app,
        [{"k1": 3}, None],
        ["beta", "alpha"],
        __end_time__=hg.MIN_ST + 6 * hg.MIN_TD,
    ) == [{"k1": 4}, {"k1": hg.REMOVE}, {"k1": 5}]


def test_service_dependent_request_reply_retains_full_feedback():
    @hg.reference_service
    def offset(path: str = "dependency") -> TS[int]: ...

    @hg.service_impl(interfaces=offset)
    def offset_impl(path: str = "dependency") -> TS[int]:
        return hg.const(1)

    @hg.request_reply_service
    def adjust(value: TS[int], path: str = "dependent") -> TS[int]: ...

    @hg.service_impl(interfaces=adjust)
    def adjust_impl(values: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        increment = offset(path="dependency")
        return hg.map_(lambda value, amount: value + amount,
                       values, amount=increment)

    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_service("dependency", offset_impl)
        hg.register_service("dependent", adjust_impl)
        return adjust(value, path="dependent")

    assert eval_node(
        app, [5], __end_time__=hg.MIN_ST + 5 * hg.MIN_TD,
    ) == [None, None, 6]


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

    # Issue #95: reduction is over the currently-valid subset. Its explicit
    # identity is observable while every mapped switch terminal is invalid,
    # then k1 publishes 9 while k2 is still a phantom slot. Released hgraph
    # waits at those points; once k2 is valid, both publish 26 and then 14.
    assert eval_node(
        app,
        [{"k1": 3}, {"k2": 4}, None, {"k1": hg.REMOVE}],
        ["alpha", None, "beta", None],
        __end_time__=hg.MIN_ST + 10 * hg.MIN_TD,
    ) == [0, 9, 26, 14]
