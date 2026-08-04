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

import hgraph as hg
from hgraph import TS, TSD, TSS, graph
from hgraph.test import eval_node


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
