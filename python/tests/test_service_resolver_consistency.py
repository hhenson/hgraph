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


def test_type_only_service_and_adaptor_resolvers_run_with_concrete_transports():
    @hg.reference_service(
        resolvers={hg.SIZE: lambda mapping, width: width})
    def sized_reference(
        width: int,
        size: type[hg.SIZE] = hg.AUTO_RESOLVE,
        path: str = "sized-reference",
    ) -> TS[int]: ...

    @hg.service_impl(interfaces=sized_reference)
    def sized_reference_impl(
        size: type[hg.SIZE] = hg.AUTO_RESOLVE,
    ) -> TS[int]:
        return hg.const(size.SIZE)

    @hg.adaptor(resolvers={hg.SIZE: lambda mapping, width: width})
    def sized_adaptor(
        value: TS[int],
        width: int,
        size: type[hg.SIZE] = hg.AUTO_RESOLVE,
        path: str = "sized-adaptor",
    ) -> TS[int]: ...

    @hg.adaptor_impl(interfaces=sized_adaptor)
    def sized_adaptor_impl(
        value: TS[int],
        size: type[hg.SIZE] = hg.AUTO_RESOLVE,
    ) -> TS[int]:
        return value + size.SIZE

    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_service("sized-reference", sized_reference_impl)
        hg.register_adaptor("sized-adaptor", sized_adaptor_impl)
        reference = sized_reference(3, path="sized-reference")
        adapted = sized_adaptor(value, 4, path="sized-adaptor")
        return reference + adapted

    assert eval_node(app, [1, 2]) == [8, 9]
