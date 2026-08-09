"""Service implementations may take time-series inputs supplied at registration.

RFC 0011 step 2. Adaptors have always had this through ``manual_adaptor``;
``register_service_impl`` had no ``inputs`` parameter at all. A service
implementation may now declare time-series parameters BEYOND the ones its
interface supplies, and ``register_service`` binds them.

Registration inputs follow the flavour's own transport input, matching the
adaptor contract.
"""

import pytest

import hgraph as hg
from hgraph import TS, TSD, TSS, graph
from hgraph.test import eval_node


def test_reference_service_impl_takes_a_registration_input():
    @hg.reference_service
    def offset_rate(path: str = "rate") -> TS[int]: ...

    @hg.service_impl(interfaces=offset_rate)
    def offset_rate_impl(offset: TS[int], path: str = "rate") -> TS[int]:
        return offset + 100

    @graph
    def app() -> TS[int]:
        hg.register_service("rate", offset_rate_impl, offset=hg.const(5))
        return offset_rate(path="rate")

    assert eval_node(app) == [105]


def test_subscription_service_impl_takes_a_registration_input():
    @hg.subscription_service
    def quotes(key: TS[str], path: str = "quotes") -> TS[int]: ...

    @hg.service_impl(interfaces=quotes)
    def quotes_impl(
        keys: TSS[str], base: TS[int], path: str = "quotes",
    ) -> TSD[str, TS[int]]:
        # ``keys`` is the interface input; ``base`` comes from registration.
        return hg.map_(lambda key, b: b, __keys__=keys, b=base)

    @graph
    def app() -> TS[int]:
        hg.register_service("quotes", quotes_impl, base=hg.const(11))
        return quotes(hg.const("k"), path="quotes")

    assert eval_node(app, __end_time__=hg.MIN_ST + 5 * hg.MIN_TD)[-1] == 11


def test_request_reply_service_impl_takes_a_registration_input():
    @hg.request_reply_service
    def scale(value: TS[int], path: str = "scale") -> TS[int]: ...

    @hg.service_impl(interfaces=scale)
    def scale_impl(
        value: TSD[int, TS[int]], factor: TS[int], path: str = "scale",
    ) -> TSD[int, TS[int]]:
        return hg.map_(lambda v, f: v * f, value, f=factor)

    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_service("scale", scale_impl, factor=hg.const(3))
        return scale(value, path="scale")

    assert eval_node(app, [5], __end_time__=hg.MIN_ST + 5 * hg.MIN_TD)[-1] == 15


def test_registration_input_may_be_a_plain_value():
    # Non-port values are lifted, as they are for manual adaptors.
    @hg.reference_service
    def offset_rate(path: str = "rate") -> TS[int]: ...

    @hg.service_impl(interfaces=offset_rate)
    def offset_rate_impl(offset: TS[int], path: str = "rate") -> TS[int]:
        return offset + 1

    @graph
    def app() -> TS[int]:
        hg.register_service("rate", offset_rate_impl, offset=41)
        return offset_rate(path="rate")

    assert eval_node(app) == [42]


def test_missing_registration_input_is_reported():
    @hg.reference_service
    def offset_rate(path: str = "rate") -> TS[int]: ...

    @hg.service_impl(interfaces=offset_rate)
    def offset_rate_impl(offset: TS[int], path: str = "rate") -> TS[int]:
        return offset + 1

    @graph
    def app() -> TS[int]:
        hg.register_service("rate", offset_rate_impl)
        return offset_rate(path="rate")

    with pytest.raises(hg.WiringError, match="time-series configuration 'offset'"):
        eval_node(app)


def test_registration_inputs_combine_with_scalar_options():
    observed = []

    @hg.reference_service
    def offset_rate(path: str = "rate") -> TS[int]: ...

    @hg.service_impl(interfaces=offset_rate)
    def offset_rate_impl(
        offset: TS[int], path: str = "rate", label: str = "none",
    ) -> TS[int]:
        observed.append(label)
        return offset + 1

    @graph
    def app() -> TS[int]:
        hg.register_service("rate", offset_rate_impl, offset=1, label="tagged")
        return offset_rate(path="rate")

    assert eval_node(app) == [2]
    assert observed == ["tagged"], observed
