"""Service and adaptor resolvers follow the shared wiring resolution order."""

import pytest

import hgraph as hg
from hgraph import TS, graph
from hgraph.test import eval_node


def test_explicit_service_and_adaptor_specializations_skip_bound_resolvers():
    calls = []

    def unexpected(surface):
        def resolver(mapping):
            calls.append(surface)
            raise AssertionError(f"{surface} resolver must not run")

        return resolver

    @hg.reference_service(resolvers={hg.SCALAR: unexpected("reference")})
    def reference() -> TS[hg.SCALAR]: ...

    @hg.subscription_service(resolvers={hg.SCALAR: unexpected("subscription")})
    def subscription(key: TS[int]) -> TS[hg.SCALAR]: ...

    @hg.request_reply_service(resolvers={hg.SCALAR: unexpected("request/reply")})
    def request_reply(request: TS[int]) -> TS[hg.SCALAR]: ...

    @hg.adaptor(resolvers={hg.SCALAR: unexpected("adaptor")})
    def adaptor(request: TS[int]) -> TS[hg.SCALAR]: ...

    @hg.service_adaptor(resolvers={hg.SCALAR: unexpected("service adaptor")})
    def service_adaptor(request: TS[int]) -> TS[hg.SCALAR]: ...

    assert reference[hg.SCALAR:int] is not None
    assert subscription[hg.SCALAR:int] is not None
    assert request_reply[hg.SCALAR:int] is not None
    assert adaptor[hg.SCALAR:int] is not None
    assert service_adaptor[hg.SCALAR:int] is not None
    assert calls == []


def test_service_and_adaptor_resolvers_receive_bound_scalar_values():
    calls = {
        "reference": [],
        "subscription": [],
        "request/reply": [],
        "adaptor": [],
        "service adaptor": [],
    }

    def selected_type(surface):
        def resolver(mapping, response_type):
            calls[surface].append(response_type)
            return response_type

        return resolver

    @hg.reference_service(resolvers={hg.SCALAR: selected_type("reference")})
    def reference(
        response_type: type, path: str = "missing-reference",
    ) -> TS[hg.SCALAR]: ...

    @hg.subscription_service(
        resolvers={hg.SCALAR: selected_type("subscription")})
    def subscription(
        key: TS[int], response_type: type,
        path: str = "missing-subscription",
    ) -> TS[hg.SCALAR]: ...

    @hg.request_reply_service(
        resolvers={hg.SCALAR: selected_type("request/reply")})
    def request_reply(
        request: TS[int], response_type: type,
        path: str = "missing-request-reply",
    ) -> TS[hg.SCALAR]: ...

    @hg.adaptor(resolvers={hg.SCALAR: selected_type("adaptor")})
    def adaptor(
        request: TS[int], response_type: type,
        path: str = "missing-adaptor",
    ) -> TS[hg.SCALAR]: ...

    @hg.service_adaptor(
        resolvers={hg.SCALAR: selected_type("service adaptor")})
    def service_adaptor(
        request: TS[int], response_type: type,
        path: str = "missing-service-adaptor",
    ) -> TS[hg.SCALAR]: ...

    @graph
    def app(value: TS[int]) -> TS[int]:
        reference(str, path="missing-reference")
        subscription(value, str, path="missing-subscription")
        request_reply(value, str, path="missing-request-reply")
        adaptor(value, str, path="missing-adaptor")
        service_adaptor(value, str, path="missing-service-adaptor")
        return value

    # The missing implementations are intentional: all resolver calls happen
    # through the public graph wiring surface before service materialization.
    with pytest.raises((hg.WiringError, ValueError)):
        eval_node(app, [1])

    assert all(values and set(values) == {str} for values in calls.values())
