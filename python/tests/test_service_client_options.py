"""Service clients may pass wiring-time scalar options to the implementation.

RFC 0011 step 1. Adaptor clients have always had this channel
(``_record_client_config``); it was reached only from the adaptor client stub.
The store was already keyed by ``stub.flavour`` and ``_bind_registered_impl``
already merged it generically, so lifting it onto services is a matter of
recording from ``_ServiceStub.__call__``.

For a source-only interface this is the *only* client parameterisation channel,
which is why it has to exist on the service surface before source-only adaptors
can collapse onto reference services.
"""

import pytest

import hgraph as hg
from hgraph import TS, TSD, TSS, graph
from hgraph.test import eval_node


def test_reference_service_client_supplies_scalar_options():
    observed = []

    @hg.reference_service
    def scaled(path: str = "scaled", multiplier: int = 1) -> TS[int]: ...

    @hg.service_impl(interfaces=scaled)
    def scaled_impl(path: str = "scaled", multiplier: int = 1) -> TS[int]:
        observed.append(multiplier)
        return hg.const(multiplier)

    @graph
    def app() -> TS[int]:
        hg.register_service("scaled", scaled_impl)
        return scaled(path="scaled", multiplier=7)

    assert eval_node(app) == [7]
    assert observed == [7], observed


def test_subscription_service_client_supplies_scalar_options():
    observed = []

    @hg.subscription_service
    def quotes(key: TS[str], path: str = "quotes", multiplier: int = 1) -> TS[int]: ...

    @hg.service_impl(interfaces=quotes)
    def quotes_impl(
        keys: TSS[str], path: str = "quotes", multiplier: int = 1,
    ) -> TSD[str, TS[int]]:
        observed.append(multiplier)
        return hg.map_(lambda key: hg.const(multiplier), __keys__=keys)

    @graph
    def app() -> TS[int]:
        hg.register_service("quotes", quotes_impl)
        return quotes(hg.const("k"), path="quotes", multiplier=4)

    assert eval_node(app, __end_time__=hg.MIN_ST + 5 * hg.MIN_TD)[-1] == 4
    assert observed == [4], observed


def test_request_reply_service_client_supplies_scalar_options():
    observed = []

    @hg.request_reply_service
    def scale(value: TS[int], path: str = "scale", multiplier: int = 1) -> TS[int]: ...

    @hg.service_impl(interfaces=scale)
    def scale_impl(
        value: TSD[int, TS[int]], path: str = "scale", multiplier: int = 1,
    ) -> TSD[int, TS[int]]:
        observed.append(multiplier)
        return hg.map_(lambda v: v * multiplier, value)

    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_service("scale", scale_impl)
        return scale(value, path="scale", multiplier=3)

    assert eval_node(
        app, [5], __end_time__=hg.MIN_ST + 5 * hg.MIN_TD)[-1] == 15
    assert observed == [3], observed


def test_service_clients_must_agree_on_options():
    @hg.reference_service
    def scaled(path: str = "scaled", multiplier: int = 1) -> TS[int]: ...

    @hg.service_impl(interfaces=scaled)
    def scaled_impl(path: str = "scaled", multiplier: int = 1) -> TS[int]:
        return hg.const(multiplier)

    @graph
    def app() -> TS[int]:
        hg.register_service("scaled", scaled_impl)
        first = scaled(path="scaled", multiplier=2)
        second = scaled(path="scaled", multiplier=3)
        return first + second

    # The flavour is named in the diagnostic - the store is shared with
    # adaptors, so the message must read correctly for a service too.
    with pytest.raises(
            hg.WiringError,
            match=r"reference service 'scaled' clients .*disagree.*multiplier"):
        eval_node(app)


def test_service_registration_option_supplies_a_value_clients_do_not_declare():
    observed = []

    # The stub declares no default for ``multiplier``, so a client that does
    # not pass one records nothing for it and the registration value is used.
    # (A stub-level default is recorded as if the client had passed it -
    # ``bind`` applies defaults - which is the pre-existing adaptor semantic
    # this shares. See the RFC note on defaulted client options.)
    @hg.reference_service
    def scaled(path: str = "scaled") -> TS[int]: ...

    @hg.service_impl(interfaces=scaled)
    def scaled_impl(path: str = "scaled", multiplier: int = 1) -> TS[int]:
        observed.append(multiplier)
        return hg.const(multiplier)

    @graph
    def app() -> TS[int]:
        hg.register_service("scaled", scaled_impl, multiplier=9)
        return scaled(path="scaled")

    assert eval_node(app) == [9]
    assert observed == [9], observed


def test_service_registration_option_conflicting_with_client_is_rejected():
    @hg.reference_service
    def scaled(path: str = "scaled", multiplier: int = 1) -> TS[int]: ...

    @hg.service_impl(interfaces=scaled)
    def scaled_impl(path: str = "scaled", multiplier: int = 1) -> TS[int]:
        return hg.const(multiplier)

    @graph
    def app() -> TS[int]:
        hg.register_service("scaled", scaled_impl, multiplier=9)
        return scaled(path="scaled", multiplier=2)

    with pytest.raises(hg.WiringError, match="conflicts with"):
        eval_node(app)


def test_distinct_paths_keep_separate_options():
    observed = []

    @hg.reference_service
    def scaled(path: str = "scaled", multiplier: int = 1) -> TS[int]: ...

    @hg.service_impl(interfaces=scaled)
    def scaled_impl(path: str = "scaled", multiplier: int = 1) -> TS[int]:
        observed.append((path, multiplier))
        return hg.const(multiplier)

    @graph
    def app() -> TS[int]:
        hg.register_service("a", scaled_impl)
        hg.register_service("b", scaled_impl)
        return scaled(path="a", multiplier=2) + scaled(path="b", multiplier=5)

    assert eval_node(app) == [7]
    assert sorted(observed) == [("a", 2), ("b", 5)], observed
