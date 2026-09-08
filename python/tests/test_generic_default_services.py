from typing import Type, TypeVar

import hgraph as hg
from hgraph.test import eval_node


def test_generic_default_reference_service_supports_multiple_specializations():
    value_type = TypeVar("value_type", int, str)

    @hg.reference_service
    def generic_default_service(
        path: str = "generic-default-service",
    ) -> hg.TS[value_type]: ...

    @hg.service_impl(interfaces=generic_default_service)
    def generic_default_service_impl(
        value_tp: Type[value_type] = hg.AUTO_RESOLVE,
    ) -> hg.TS[value_type]:
        return hg.const(1 if value_tp is int else "text")

    @hg.graph
    def app() -> hg.TS[str]:
        hg.register_service(None, generic_default_service_impl)
        return hg.format_(
            "{}:{}",
            generic_default_service[int](path="integer"),
            generic_default_service[str](path="text"),
        )

    assert eval_node(app) == ["1:text"]


def test_generic_default_adaptor_supports_multiple_specializations():
    value_type = TypeVar("value_type", int, str)

    @hg.adaptor
    def generic_default_adaptor(
        value: hg.TS[value_type], path: str = "generic-default-adaptor"
    ) -> hg.TS[value_type]: ...

    @hg.adaptor_impl(interfaces=generic_default_adaptor)
    def generic_default_adaptor_impl(
        value: hg.TS[value_type],
    ) -> hg.TS[value_type]:
        return value

    @hg.graph
    def app(integer: hg.TS[int], text: hg.TS[str]) -> hg.TS[int]:
        hg.register_adaptor(None, generic_default_adaptor_impl)
        hg.null_sink(generic_default_adaptor(text, path="text"))
        return generic_default_adaptor(integer, path="integer")

    assert eval_node(app, [1, 2], ["a", "b"]) == [1, 2]


def test_generic_default_service_adaptor_supports_multiple_specializations():
    value_type = TypeVar("value_type", int, str)

    @hg.service_adaptor
    def generic_default_service_adaptor(
        request: hg.TS[value_type], path: str = "generic-default-service-adaptor"
    ) -> hg.TS[value_type]: ...

    @hg.service_adaptor_impl(interfaces=generic_default_service_adaptor)
    def generic_default_service_adaptor_impl(
        requests: hg.TSD[int, hg.TS[value_type]],
    ) -> hg.TSD[int, hg.TS[value_type]]:
        return requests

    @hg.graph
    def app(integer: hg.TS[int], text: hg.TS[str]) -> hg.TS[int]:
        hg.register_adaptor(None, generic_default_service_adaptor_impl)
        hg.null_sink(generic_default_service_adaptor(text, path="text"))
        return generic_default_service_adaptor(integer, path="integer")

    assert eval_node(app, [1, 2], ["a", "b"]) == [1, 2]
