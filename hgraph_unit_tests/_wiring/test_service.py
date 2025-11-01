from typing import Type

from frozendict import frozendict

from hgraph import (
    reference_service,
    TSD,
    TS,
    service_impl,
    graph,
    generator,
    register_service,
    default_path,
    TSS,
    subscription_service,
    map_,
    TSL,
    SIZE,
    request_reply_service,
    contains_,
    NUMBER,
    AUTO_RESOLVE,
    KEYABLE_SCALAR,
    SCALAR,
    SCALAR_1,
    TIME_SERIES_TYPE,
    flip,
    format_,
    const,
    null_sink,
    merge,
    sample,
    if_,
    debug_print,
    mesh_,
    set_service_output,
    get_service_inputs,
    MIN_ST,
)
from hgraph import pass_through_node
from hgraph.test import eval_node


import pytest
pytestmark = pytest.mark.smoke


def test_reference_service():

    @reference_service
    def my_service(path: str = None) -> TSD[str, TS[str]]:
        """The service description"""

    @service_impl(interfaces=my_service)
    def my_service_impl() -> TSD[str, TS[str]]:
        return const(frozendict({"test": "a value"}), TSD[str, TS[str]])

    @graph
    def main() -> TS[str]:
        register_service(default_path, my_service_impl)
        return my_service()["test"]

    assert eval_node(main) == ["a value"]


def test_subscription_service():

    @subscription_service
    def my_subs_service(path: str, subscription: TS[str]) -> TS[str]:
        """The service description"""

    @graph
    def subscription_instance(key: TS[str]) -> TS[str]:
        return key

    @service_impl(interfaces=my_subs_service)
    def my_subs_service_impl(subscription: TSS[str]) -> TSD[str, TS[str]]:
        return map_(pass_through_node, __keys__=subscription, __key_arg__="ts")

    @graph
    def main(
        subscription_topic1: TS[str], subscription_topic2: TS[str], subscription_topic3: TS[str]
    ) -> TSL[TS[str], SIZE]:
        register_service(default_path, my_subs_service_impl)
        return TSL.from_ts(
            pass_through_node(my_subs_service(default_path, subscription_topic1)),  # To remove reference semantics
            pass_through_node(my_subs_service(default_path, subscription_topic2)),
            pass_through_node(my_subs_service(default_path, subscription_topic3)),
        )

    assert eval_node(
        main,
        ["subscription_topic1", None, None],
        ["subscription_topic2", None, None],
        [
            None,
            None,
            "subscription_topic1",
        ],
    ) == [None, {0: "subscription_topic1", 1: "subscription_topic2"}, {2: "subscription_topic1"}]


def test_request_reply_service():
    @request_reply_service
    def add_one_service(path: str, ts: TS[int]) -> TS[int]:
        """The service description"""

    @service_impl(interfaces=add_one_service)
    def add_one_service_impl(ts: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return map_(lambda x: x + 1, ts)

    @graph
    def main(x: TS[int]) -> TS[int]:
        register_service(default_path, add_one_service_impl)
        return add_one_service(default_path, x)

    assert eval_node(main, [1]) == [None, None, 2]


def test_request_reply_service2():
    @request_reply_service
    def add_service(path: str, ts: TS[int], ts1: TS[int]) -> TS[int]:
        """The service description"""

    @service_impl(interfaces=add_service)
    def add_service_impl(ts: TSD[int, TS[int]], ts1: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return map_(lambda x, y: x + y, ts, ts1)

    @graph
    def main(x: TS[int], y: TS[int]) -> TS[int]:
        register_service(default_path, add_service_impl)
        return add_service(default_path, x, y)

    assert eval_node(main, [1], [2]) == [None, None, 3]


def test_recursive_request_reply_service():
    @request_reply_service
    def add_one_service(path: str, ts: TS[int]) -> TS[int]:
        """The service description"""

    @graph
    def _add_one_service_impl(ts: TS[int]) -> TS[int]:
        z, nz = if_(ts == 0, ts)
        return merge(sample(z, 1), add_one_service("default_path", nz - 1) + 1)

    @service_impl(interfaces=add_one_service)
    def add_one_service_impl(ts: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return map_(_add_one_service_impl, ts)

    @graph
    def main(x: TS[int]) -> TS[int]:
        register_service("default_path", add_one_service_impl)
        return add_one_service("default_path", x)

    assert eval_node(main, [3])[-1] == 4


def test_two_services():
    @request_reply_service
    def add_one_service(path: str, ts: TS[int]) -> TS[int]:
        """The service description"""

    @service_impl(interfaces=add_one_service)
    def add_one_service_impl(ts: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return map_(lambda x: x + 1, ts)

    @service_impl(interfaces=add_one_service)
    def add_one_service_impl_2(ts: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return map_(lambda x: add_one_service("one_path", x) + 1, ts)

    @graph
    def main(x: TS[int]) -> TS[int]:
        register_service("another_path", add_one_service_impl_2)
        register_service("one_path", add_one_service_impl)
        return add_one_service("another_path", x)

    assert eval_node(main, [1]) == [None, None, None, None, 3]


def test_multiservice():
    @request_reply_service
    def submit(path: str, ts: TS[int]): ...

    @reference_service
    def receive(path: str) -> TSS[int]: ...

    @subscription_service
    def subscribe(path: str, ts: TS[int]) -> TS[bool]: ...

    @service_impl(interfaces=(submit, receive, subscribe))
    def impl(path: str):
        submissions: TSD[int, TS[int]] = get_service_inputs(path, submit).ts
        items = flip(submissions).key_set
        set_service_output(path, receive, items)
        set_service_output(
            path,
            subscribe,
            map_(
                lambda key, i: contains_(i, key),
                __keys__=subscribe.wire_impl_inputs_stub(path).ts,
                i=pass_through_node(items),
            ),
        )

    @graph
    def multiservice_test(ts1: TS[int], ts2: TS[int]) -> TSD[int, TS[bool]]:
        register_service("submit", impl)
        submit("submit", ts1)
        submit("submit", ts2)
        return map_(lambda key: subscribe("submit", key), __keys__=receive("submit"))

    assert eval_node(multiservice_test, [1, None], [None, 2]) == [None, {}, {1: True}, {2: True}]


def test_generic_ref_service():
    @reference_service
    def numbers_service(path: str) -> TS[NUMBER]:
        """The service description"""

    @service_impl(interfaces=numbers_service)
    def numbers_service_impl(tp: Type[NUMBER] = AUTO_RESOLVE) -> TS[NUMBER]:
        return const(tp(1))

    @graph
    def main(x: TS[bool], tp: Type[NUMBER] = AUTO_RESOLVE) -> TS[NUMBER]:
        null_sink(x)
        numbers_service[NUMBER:tp].register_impl("default_path", numbers_service_impl)
        return numbers_service[NUMBER:tp]("default_path")

    assert eval_node(main[NUMBER:int], [None]) == [1]
    assert eval_node(main[NUMBER:float], [None]) == [1.0]


def test_generic_ref_service_different_impls():
    @reference_service
    def numbers_service(path: str) -> TS[NUMBER]:
        """The service description"""

    @service_impl(interfaces=numbers_service)
    def numbers_service_impl_int() -> TS[int]:
        return const(1)

    @service_impl(interfaces=numbers_service)
    def numbers_service_impl_float() -> TS[float]:
        return const(2.0)

    @graph
    def main(x: TS[bool], tp: Type[NUMBER] = AUTO_RESOLVE) -> TS[NUMBER]:
        null_sink(x)
        numbers_service[NUMBER:int].register_impl("default_path", numbers_service_impl_int)
        numbers_service[NUMBER:float].register_impl("default_path", numbers_service_impl_float)
        return numbers_service[NUMBER:tp]("default_path")

    assert eval_node(main[NUMBER:int], [None]) == [1]
    assert eval_node(main[NUMBER:float], [None]) == [2.0]


def test_generic_sub_service():
    @subscription_service
    def numbers_service(path: str, ts: TS[KEYABLE_SCALAR]) -> TS[KEYABLE_SCALAR]:
        """The service description"""

    @service_impl(interfaces=numbers_service)
    def numbers_service_impl(ts: TSS[KEYABLE_SCALAR]) -> TSD[KEYABLE_SCALAR, TS[KEYABLE_SCALAR]]:
        return map_(lambda key: key, __keys__=ts)

    @graph
    def main(x: TS[KEYABLE_SCALAR]) -> TS[KEYABLE_SCALAR]:
        numbers_service.register_impl("default_path", numbers_service_impl)
        return numbers_service("default_path", x)

    assert eval_node(main[KEYABLE_SCALAR:int], [1]) == [None, 1]
    assert eval_node(main[KEYABLE_SCALAR:str], ["2."]) == [None, "2."]


def test_generic_rr_service():
    @request_reply_service
    def add_one_service(path: str, ts: TS[NUMBER]) -> TS[NUMBER]:
        """The service description"""

    @service_impl(interfaces=add_one_service)
    def add_one_service_impl(ts: TSD[int, TS[NUMBER]]) -> TSD[int, TS[NUMBER]]:
        return map_(lambda x: x + 1, ts)

    @graph
    def main(x: TS[int]) -> TS[int]:
        register_service("default_path", add_one_service_impl)
        return add_one_service("default_path", x)

    assert eval_node(main, [1]) == [None, None, 2]


def test_generic_multi_service():
    @request_reply_service
    def submit(path: str, ts: TS[KEYABLE_SCALAR]): ...

    @reference_service
    def receive(path: str) -> TSS[KEYABLE_SCALAR]: ...

    @subscription_service
    def subscribe(ts: TS[KEYABLE_SCALAR], path: str = default_path) -> TS[bool]: ...

    @service_impl(interfaces=(submit, receive, subscribe))
    def impl(path: str, tp: Type[KEYABLE_SCALAR] = AUTO_RESOLVE):
        submissions: TSD[tp, TS[tp]] = submit[KEYABLE_SCALAR:tp].wire_impl_inputs_stub(path).ts
        items = flip(submissions).key_set
        receive[KEYABLE_SCALAR:tp].wire_impl_out_stub(path, items)
        subscribe[KEYABLE_SCALAR:tp].wire_impl_out_stub(
            path,
            map_(
                lambda key, i: contains_(i, key),
                __keys__=subscribe[KEYABLE_SCALAR:tp].wire_impl_inputs_stub(path).ts,
                i=pass_through_node(items),
            ),
        )

    @graph
    def multiservice_test(ts1: TS[SCALAR], ts2: TS[SCALAR], tp: Type[SCALAR] = AUTO_RESOLVE) -> TSD[SCALAR, TS[bool]]:
        register_service("submit", impl)
        submit("submit", ts1)
        submit("submit", ts2)
        return map_(lambda key: subscribe(key, path="submit"), __keys__=receive[KEYABLE_SCALAR:tp]("submit"))

    assert eval_node(multiservice_test[SCALAR:int], [1, None], [None, 2]) == [None, {}, {1: True}, {2: True}]


def test_generic_multi_service_with_non_trivial_resolve():
    @request_reply_service
    def submit(
        path: str, ts: TS[KEYABLE_SCALAR], tp: Type[TIME_SERIES_TYPE] = TS[KEYABLE_SCALAR]
    ) -> TIME_SERIES_TYPE: ...

    @reference_service
    def receive(path: str) -> TSS[KEYABLE_SCALAR]: ...

    @subscription_service(resolvers={SCALAR_1: str})
    def subscribe(path: str, ts: TS[KEYABLE_SCALAR]) -> TS[SCALAR_1]: ...

    @service_impl(interfaces=(submit, receive, subscribe))
    def impl(path: str, tp: Type[KEYABLE_SCALAR] = AUTO_RESOLVE, tp_2: Type[TIME_SERIES_TYPE] = TS[KEYABLE_SCALAR]):
        submissions: TSD[tp, TS[tp]] = submit[KEYABLE_SCALAR:tp].wire_impl_inputs_stub(path).ts
        submit[KEYABLE_SCALAR:tp].wire_impl_out_stub(path, submissions)

        items = map_(lambda key: format_("{}", key), __keys__=flip(submissions).key_set)

        receive[KEYABLE_SCALAR:tp].wire_impl_out_stub(path, items.key_set)
        subscribe[KEYABLE_SCALAR:tp].wire_impl_out_stub(
            path, map_(lambda i: i, __keys__=subscribe[KEYABLE_SCALAR:tp].wire_impl_inputs_stub(path).ts, i=items)
        )

    @graph
    def multiservice_test(
        ts1: TS[SCALAR], ts2: TS[SCALAR], tp: Type[SCALAR] = AUTO_RESOLVE
    ) -> TSD[SCALAR, TS[SCALAR_1]]:
        register_service("submit", impl)
        debug_print("ts1", submit("submit", ts1))
        debug_print("ts2", submit("submit", ts2))
        return map_(lambda key: subscribe("submit", key), __keys__=receive[KEYABLE_SCALAR:tp]("submit"))

    assert eval_node(multiservice_test[SCALAR:int, SCALAR_1:str], [1, None], [None, 2]) == [
        None,
        {},
        {1: "1"},
        {2: "2"},
    ]


def test_resolution_of_service_vs_impl_with_additional_template_params():
    @reference_service
    def receive(path: str = default_path) -> TS[KEYABLE_SCALAR]: ...

    @service_impl(interfaces=(receive,))
    def impl(tp: type[SCALAR]) -> TS[KEYABLE_SCALAR]:
        return const(tp(), TS[str])

    @graph
    def g_test() -> TS[str]:
        receive[KEYABLE_SCALAR:str].register_impl(default_path, impl, tp=str)
        return receive[KEYABLE_SCALAR:str]()

    assert eval_node(g_test) == [""]


def test_service_in_a_mesh():
    @reference_service
    def receive(path: str = default_path) -> TS[KEYABLE_SCALAR]: ...

    @service_impl(interfaces=(receive,))
    def impl(tp: type[SCALAR]) -> TS[KEYABLE_SCALAR]:
        return const(tp(), TS[str])

    @graph
    def g_test() -> TSD[int, TS[str]]:
        receive[KEYABLE_SCALAR:str].register_impl(default_path, impl, tp=str)
        return mesh_(lambda key: receive[KEYABLE_SCALAR:str](), __keys__=const(frozenset({1, 2}), TSS[int]))

    assert eval_node(g_test) == [{1: "", 2: ""}]


def test_service_impl_with_generics():

    @reference_service
    def data(path: str = default_path) -> TS[str]: ...

    @service_impl(interfaces=(data,))
    def impl(tp: type[SCALAR]) -> TS[str]:
        return const("Test", TS[str])

    @graph
    def g_test() -> TS[str]:
        register_service(default_path, impl, tp=int)
        return data()

    assert eval_node(g_test) == ["Test"]


def test_service_impl_over_node():
    @reference_service
    def data(path: str = default_path) -> TS[str]: ...

    @service_impl(interfaces=(data,))
    @generator
    def impl() -> TS[str]:
        yield MIN_ST, "Test"

    @graph
    def g_test() -> TS[str]:
        register_service(default_path, impl)
        return data()

    assert eval_node(g_test) == ["Test"]
