"""``from_graph`` / ``to_graph`` work for services, not just adaptors.

RFC 0011 step 3. These are the adaptor spelling of ``impl_input`` /
``impl_output``. They are aliases onto the same wiring - ``service::impl_output``
and ``adaptor::to_graph`` already build the same stub registration, shared
output source, capture and rank anchor - so the observable behaviour must be
identical either way.

By-stub publishing currently requires an implementation scope, which only a
multi-interface registration opens; these tests therefore use multi-interface
implementations. Step 4 adds the lazy single-interface by-stub registration,
at which point the same verbs work there too.
"""

import pytest

import hgraph as hg
from hgraph import TS, TSD, TSS, graph
from hgraph.test import eval_node


def test_service_from_to_graph_match_impl_input_output_exactly():
    @hg.subscription_service
    def price(key: TS[str], path: str = "p") -> TS[int]: ...

    @hg.request_reply_service
    def double(value: TS[int], path: str = "p") -> TS[int]: ...

    @hg.service_impl(interfaces=(price, double))
    def impl_verbs(path: str):
        keys = hg.impl_input(price, path)
        hg.impl_output(price, hg.map_(lambda key: hg.const(3), __keys__=keys), path)
        requests = hg.impl_input(double, path)
        hg.impl_output(double, hg.map_(lambda v: v * 2, requests), path)

    @hg.service_impl(interfaces=(price, double))
    def graph_verbs(path: str):
        keys = hg.from_graph(price, path)
        hg.to_graph(price, hg.map_(lambda key: hg.const(3), __keys__=keys), path)
        requests = hg.from_graph(double, path)
        hg.to_graph(double, hg.map_(lambda v: v * 2, requests), path)

    @graph
    def with_impl_verbs(value: TS[int]) -> TS[int]:
        hg.register_service("a", impl_verbs)
        return price(hg.const("k"), path="a") + double(value, path="a")

    @graph
    def with_graph_verbs(value: TS[int]) -> TS[int]:
        hg.register_service("b", graph_verbs)
        return price(hg.const("k"), path="b") + double(value, path="b")

    end = hg.MIN_ST + 6 * hg.MIN_TD
    via_impl = eval_node(with_impl_verbs, [6], __end_time__=end)
    via_graph = eval_node(with_graph_verbs, [6], __end_time__=end)
    assert via_impl == via_graph, (via_impl, via_graph)
    # 3 (subscription) + 12 (request/reply doubling 6)
    assert via_impl[-1] == 15, via_impl


def test_to_graph_publishes_a_reference_service_output():
    @hg.reference_service
    def rate(path: str = "r") -> TS[int]: ...

    @hg.request_reply_service
    def double(value: TS[int], path: str = "r") -> TS[int]: ...

    @hg.service_impl(interfaces=(rate, double))
    def impl(path: str):
        hg.to_graph(rate, hg.const(21), path)
        requests = hg.from_graph(double, path)
        hg.to_graph(double, hg.map_(lambda v: v * 2, requests), path)

    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_service("r", impl)
        return rate(path="r") + double(value, path="r")

    out = eval_node(app, [2], __end_time__=hg.MIN_ST + 6 * hg.MIN_TD)
    assert out[-1] == 25, out


def test_reference_service_has_no_from_graph():
    @hg.reference_service
    def rate(path: str = "r") -> TS[int]: ...

    @hg.request_reply_service
    def double(value: TS[int], path: str = "r") -> TS[int]: ...

    @hg.service_impl(interfaces=(rate, double))
    def impl(path: str):
        hg.from_graph(rate, path)

    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_service("r", impl)
        return rate(path="r") + double(value, path="r")

    with pytest.raises(hg.WiringError, match="no client input"):
        eval_node(app, [1])
