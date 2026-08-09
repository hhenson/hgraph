"""``@service_impl(interfaces=())`` may serve as a catch-all.

RFC 0011 step 6. ``register_catch_all_service_implementation_candidate`` has
no flavour at all - it sweeps the recorded client endpoints and claims every
one no exact candidate claimed. It was reachable only through
``@adaptor_impl(interfaces=())``; it is no longer adaptor-exclusive.
"""

import pytest

import hgraph as hg
from hgraph import TS, graph
from hgraph.test import eval_node


def test_service_catch_all_serves_an_unclaimed_path():
    @hg.adaptor
    def echo(value: TS[int], path: str = "e") -> TS[int]: ...

    @hg.service_impl(interfaces=())
    def catch_all():
        # Claims whatever endpoint asked and was not otherwise implemented.
        request = hg.from_graph(echo, "unclaimed")
        hg.to_graph(echo, request + 1, "unclaimed")

    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_service(None, catch_all)
        return echo(value, path="unclaimed")

    assert eval_node(app, [4]) == [5]


def test_exact_service_registration_takes_precedence_over_the_catch_all():
    @hg.adaptor
    def echo(value: TS[int], path: str = "e") -> TS[int]: ...

    @hg.adaptor_impl(interfaces=echo)
    def exact_impl(value: TS[int], path: str = "e") -> TS[int]:
        return value + 100

    @hg.service_impl(interfaces=())
    def catch_all():
        # Only "other" is left unclaimed; "exact" belongs to exact_impl.
        request = hg.from_graph(echo, "other")
        hg.to_graph(echo, request + 1, "other")

    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_adaptor("exact", exact_impl)
        hg.register_service(None, catch_all)
        return echo(value, path="exact") + echo(value, path="other")

    # 104 from the exact implementation, 5 from the catch-all.
    assert eval_node(app, [4]) == [109]
