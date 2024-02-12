import pytest
from frozendict import frozendict

from hgraph import reference_service, TSD, TS, service_impl, graph, register_service, default_path, \
    subscription_service, TSS, map_
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
    def main(subscription_topic: TS[str]) -> TS[str]:
        register_service(default_path, my_subs_service_impl)
        return pass_through(my_subs_service(default_path, subscription_topic))  # To remove reference semantics

    assert eval_node(main, ["subscription_topic",], __trace__=True) == [None, "subscription_topic"]
