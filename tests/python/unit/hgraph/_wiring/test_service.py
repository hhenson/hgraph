import pytest
from frozendict import frozendict

from hgraph import reference_service, TSD, TS, service_impl, graph, register_service, default_path, \
    subscription_service, TSS, map_, TSL, SIZE, request_reply_service
from hgraph.nodes import const, pass_through
from hgraph.test import eval_node


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
        return map_(pass_through, __keys__=subscription, __key_arg__="ts")

    @graph
    def main(subscription_topic1: TS[str], subscription_topic2: TS[str], subscription_topic3: TS[str]) -> TSL[TS[str], SIZE]:
        register_service(default_path, my_subs_service_impl)
        return TSL.from_ts(
            pass_through(my_subs_service(default_path, subscription_topic1)),  # To remove reference semantics
            pass_through(my_subs_service(default_path, subscription_topic2)),
            pass_through(my_subs_service(default_path, subscription_topic3))
        )

    assert eval_node(main,
                     ["subscription_topic1", None, None],
                     ["subscription_topic2", None, None],
                     [None, None, "subscription_topic1",],
                     __trace__=True) == [None, {0: "subscription_topic1", 1: "subscription_topic2"}, {2: "subscription_topic1"}]


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

    assert eval_node(main, [1]) == [None, 2]


def test_two_services():
    @request_reply_service
    def add_one_service(path: str, ts: TS[int]) -> TS[int]:
        """The service description"""

    @service_impl(interfaces=add_one_service)
    def add_one_service_impl(ts: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return map_(lambda x: x + 1, ts)

    @service_impl(interfaces=add_one_service)
    def add_one_service_impl_2(ts: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return map_(lambda x: add_one_service('one_path', x) + 1, ts)

    @graph
    def main(x: TS[int]) -> TS[int]:
        register_service('another_path', add_one_service_impl_2)
        register_service('one_path', add_one_service_impl)
        return add_one_service('another_path', x)

    assert eval_node(main, [1]) == [None, None, 3]
