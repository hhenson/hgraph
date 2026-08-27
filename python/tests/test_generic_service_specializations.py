from typing import Type, TypeVar

import hgraph as hg
from hgraph.test import eval_node


def test_generic_multi_service_materializes_each_requested_specialization():
    value_type = TypeVar("SERVICE_VALUE", int, str)
    materialized = []

    @hg.reference_service
    def first(path: str = "shared") -> hg.TS[value_type]: ...

    @hg.reference_service
    def second(path: str = "shared") -> hg.TS[value_type]: ...

    @hg.service_impl(interfaces=(first, second))
    def impl(path: str, tp: Type[value_type] = hg.AUTO_RESOLVE):
        materialized.append(tp)
        value = 1 if tp is int else "text"
        first[tp].wire_impl_out_stub(path, hg.const(value))
        second[tp].wire_impl_out_stub(path, hg.const(value))

    @hg.graph
    def app() -> hg.TS[str]:
        hg.register_service("shared", impl)
        return hg.format_(
            "{}:{}:{}:{}", first[int](), second[int](), first[str](), second[str]()
        )

    assert eval_node(app) == ["1:1:text:text"]
    assert materialized == [int, str]
