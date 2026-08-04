from dataclasses import dataclass
from typing import Generic, Type, TypeVar

import pytest

import hgraph as hg
from hgraph import NUMBER, NUMBER_2, TS, TSB, TSD, TimeSeriesSchema, graph
from hgraph.reflection import resolved_type
from hgraph.test import eval_node


def test_numeric_vocabulary_is_a_constrained_native_generic():
    @hg.compute_node
    def numeric_rhs(lhs: TS[NUMBER], rhs: TS[NUMBER_2]) -> TS[NUMBER_2]:
        return rhs.value

    @graph
    def int_float(lhs: TS[int], rhs: TS[float]) -> TS[float]:
        return numeric_rhs(lhs, rhs)

    @graph
    def invalid(lhs: TS[str], rhs: TS[float]) -> TS[float]:
        return numeric_rhs(lhs, rhs)

    assert eval_node(int_float, [1, 2], [1.5, 2.5]) == [1.5, 2.5]
    with pytest.raises(hg.IncorrectTypeBinding, match="numeric_rhs.*expects"):
        eval_node(invalid, ["not numeric"], [1.5])


def test_specialized_numeric_reference_services_share_one_interface_name():
    @hg.reference_service
    def numeric_reference() -> TS[NUMBER]: ...

    integer_reference = numeric_reference[NUMBER:int]
    float_reference = numeric_reference[NUMBER:float]

    @hg.service_impl(interfaces=integer_reference)
    def integer_reference_impl() -> TS[int]:
        return hg.const(1, tp=TS[int])

    @hg.service_impl(interfaces=float_reference)
    def float_reference_impl() -> TS[float]:
        return hg.const(0.5, tp=TS[float])

    @graph
    def integer_client(value: TS[int]) -> TS[int]:
        hg.register_service("numbers", integer_reference_impl)
        return value + hg.passive(integer_reference(path="numbers"))

    @graph
    def float_client(value: TS[float]) -> TS[float]:
        hg.register_service("numbers", float_reference_impl)
        return value + hg.passive(float_reference(path="numbers"))

    assert eval_node(integer_client, [2]) == [3]
    assert eval_node(float_client, [2.0]) == [2.5]
    with pytest.raises(TypeError, match="NUMBER must be one of int, float"):
        numeric_reference[NUMBER:str]


def test_service_client_reflection_retains_generic_bindings():
    reflected = []

    @hg.reference_service
    def numeric_reference(path: str) -> TS[NUMBER]: ...

    integer_reference = numeric_reference[NUMBER:int]

    @hg.service_impl(interfaces=integer_reference)
    def integer_reference_impl() -> TS[int]:
        return hg.const(1)

    @graph
    def client() -> TS[int]:
        hg.register_service("numbers", integer_reference_impl)
        value = integer_reference(path="numbers")
        reflected.extend(
            hg.WiringGraphContext.instance().registered_service_clients(
                numeric_reference))
        return value

    assert eval_node(client) == [1]
    assert len(reflected) == 1
    path, type_map, node, receive = reflected[0]
    assert path.startswith("ref_svc://numbers[")
    assert NUMBER in type_map, type_map
    assert resolved_type(type_map[NUMBER]) is int
    assert node is None
    assert receive is True


def test_specialized_service_impl_receives_user_path_without_type_suffix():
    @hg.reference_service
    def numeric_reference(path: str) -> TS[NUMBER]: ...

    integer_reference = numeric_reference[NUMBER:int]

    @hg.service_impl(interfaces=integer_reference)
    def integer_reference_impl(path: str) -> TS[int]:
        return hg.const(1 if path == "numbers" else 0)

    @graph
    def client() -> TS[int]:
        hg.register_service("numbers", integer_reference_impl)
        return integer_reference(path="numbers")

    assert eval_node(client) == [1]


def test_default_service_impl_serves_custom_path_and_receives_that_path():
    observed = []

    @hg.reference_service
    def value(path: str = "default") -> TS[str]: ...

    @hg.service_impl(interfaces=value)
    def value_impl(path: str = "default") -> TS[str]:
        observed.append(path)
        return hg.const(path)

    @graph
    def client() -> TS[str]:
        hg.register_service(None, value_impl)
        return value(path="custom")

    assert eval_node(client) == ["custom"]
    assert observed == ["custom"]


def test_registration_is_allowed_in_flattened_graph_but_not_map_body():
    @hg.reference_service
    def value(path: str) -> TS[int]: ...

    @hg.service_impl(interfaces=value)
    def value_impl() -> TS[int]:
        return hg.const(1)

    @graph
    def flattened_helper() -> TS[int]:
        hg.register_service("flattened", value_impl)
        return value(path="flattened")

    @graph
    def flattened_client() -> TS[int]:
        return flattened_helper()

    assert eval_node(flattened_client) == [1]

    @graph
    def mapped_item(ts: TS[int]) -> TS[int]:
        hg.register_service("mapped", value_impl)
        return ts

    @graph
    def mapped_client(ts: hg.TSD[str, TS[int]]) -> hg.TSD[str, TS[int]]:
        return hg.map_(mapped_item, ts)

    with pytest.raises(
        RuntimeError,
        match="cannot be registered inside an isolated sub-graph",
    ):
        eval_node(mapped_client, [{"item": 1}])


def test_single_service_type_variable_accepts_direct_specialization():
    output = TypeVar("SERVICE_OUTPUT", bound=TS[object])

    @hg.reference_service
    def value() -> output: ...

    specialized = value[TS[int]]

    assert specialized.descriptor is not None
    assert specialized._resolution.bindings["SERVICE_OUTPUT"] == TS[int].handle


def test_reference_service_specializes_a_schema_type_variable():
    class First(TimeSeriesSchema):
        value: TS[int]

    class Second(TimeSeriesSchema):
        value: TS[str]

    schema = TypeVar("SERVICE_SCHEMA", First, Second)

    class State(TimeSeriesSchema, Generic[schema]):
        current: TSB[schema]

    @hg.reference_service
    def states() -> TSD[str, TSB[State[schema]]]: ...

    specialized = states[schema:First]

    assert specialized.descriptor is not None
    assert specialized._resolution.bindings["SERVICE_SCHEMA"] == TSB[First].handle


def test_mixed_multi_service_interfaces_share_a_generic_specialization():
    class First(TimeSeriesSchema):
        value: TS[int]

    class Second(TimeSeriesSchema):
        value: TS[str]

    schema = TypeVar("SHARED_SERVICE_SCHEMA", First, Second)

    @hg.reference_service
    def state() -> TSB[schema]: ...

    @hg.request_reply_service
    def echo(value: TS[int]) -> TS[int]: ...

    @hg.service_impl(interfaces=(state, echo))
    def impl(path: str):
        state[schema:First].wire_impl_out_stub(
            path, TSB[First].from_ts(value=hg.const(1)))
        requests = echo.wire_impl_inputs_stub(path).value
        echo.wire_impl_out_stub(path, hg.map_(lambda value: value, requests))

    @graph
    def client(value: TS[int]) -> TS[int]:
        hg.register_service("shared", impl)
        state[schema:First](path="shared")
        return echo(value, path="shared")

    assert eval_node(
        client, [7], __end_time__=hg.MIN_ST + 4 * hg.MIN_TD,
    )[-1] == 7


def test_default_multi_service_impl_is_atomic_at_custom_path():
    observed = []

    @hg.reference_service
    def first(path: str = "default") -> TS[int]: ...

    @hg.reference_service
    def second(path: str = "default") -> TS[str]: ...

    @hg.service_impl(interfaces=(first, second))
    def impl(path: str = "default"):
        observed.append(path)
        first.wire_impl_out_stub(path, hg.const(1))
        second.wire_impl_out_stub(path, hg.const(path))

    @graph
    def client() -> TS[str]:
        hg.register_service(None, impl)
        return second(path="custom")

    assert eval_node(client) == ["custom"]
    assert observed == ["custom"]


def test_specialized_numeric_request_reply_services_use_native_exchange():
    @hg.request_reply_service
    def numeric_adjust(request: TS[NUMBER]) -> TS[NUMBER]: ...

    integer_adjust = numeric_adjust[NUMBER:int]
    float_adjust = numeric_adjust[NUMBER:float]

    @hg.service_impl(interfaces=integer_adjust)
    def integer_adjust_impl(requests: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return hg.map_(lambda value: value + 1, requests)

    @hg.service_impl(interfaces=float_adjust)
    def float_adjust_impl(requests: TSD[int, TS[float]]) -> TSD[int, TS[float]]:
        return hg.map_(lambda value: value + 0.5, requests)

    @graph
    def integer_client(value: TS[int]) -> TS[int]:
        hg.register_service("adjust", integer_adjust_impl)
        return integer_adjust(value, path="adjust")

    @graph
    def float_client(value: TS[float]) -> TS[float]:
        hg.register_service("adjust", float_adjust_impl)
        return float_adjust(value, path="adjust")

    assert eval_node(integer_client, [1], __end_time__=hg.MIN_ST + 3 * hg.MIN_TD) == [None, 2]
    assert eval_node(float_client, [1.5], __end_time__=hg.MIN_ST + 3 * hg.MIN_TD) == [None, 2.0]


def test_request_reply_accepts_positional_path_and_multiple_requests():
    @hg.request_reply_service
    def add(path: str, lhs: TS[int], rhs: TS[int]) -> TS[int]: ...

    @hg.service_impl(interfaces=add)
    def add_impl(
        lhs: TSD[int, TS[int]], rhs: TSD[int, TS[int]],
    ) -> TSD[int, TS[int]]:
        return hg.map_(lambda x, y: x + y, lhs, rhs)

    @graph
    def client(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        hg.register_service("arithmetic", add_impl)
        return add("arithmetic", lhs, rhs)

    assert eval_node(
        client, [1], [2], __end_time__=hg.MIN_ST + 3 * hg.MIN_TD,
    ) == [None, 3]


def test_replyless_request_reply_service_captures_requests():
    seen = []

    @hg.request_reply_service
    def publish(path: str, value: TS[int]): ...

    @hg.service_impl(interfaces=publish)
    def publish_impl(values: TSD[int, TS[int]]):
        @hg.sink_node
        def observe(ts: TSD[int, TS[int]]):
            seen.extend(value.value for _, value in ts.modified_items())

        observe(values)

    @graph
    def client(value: TS[int]) -> TS[int]:
        hg.register_service("events", publish_impl)
        publish("events", value)
        return value

    assert eval_node(
        client, [1, None, 2], __end_time__=hg.MIN_ST + 5 * hg.MIN_TD,
    ) == [1, None, 2]
    assert seen == [1, 2]


@pytest.mark.parametrize("higher_order", [hg.map_, hg.mesh_], ids=["map", "mesh"])
def test_higher_order_child_calls_outer_request_reply_service(higher_order):
    @hg.request_reply_service
    def add_one(path: str, value: TS[int]) -> TS[int]: ...

    @hg.service_impl(interfaces=add_one)
    def add_one_impl(values: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return hg.map_(lambda value: value + 1, values)

    @graph
    def mapped(value: TS[int]) -> TS[int]:
        return add_one("one_path", value) + 1

    @graph
    def client(values: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        hg.register_service("one_path", add_one_impl)
        return higher_order(mapped, values)

    assert eval_node(
        client,
        [{1: 10, 2: 20}],
        __end_time__=hg.MIN_ST + 6 * hg.MIN_TD,
    ) == [{}, {1: 12, 2: 22}]


def test_mapped_subscription_result_uses_the_declared_value_schema():
    @hg.subscription_service
    def price(key: TS[str]) -> TS[float]: ...

    @hg.service_impl(interfaces=price)
    def price_impl(keys: hg.TSS[str]) -> TSD[str, TS[float]]:
        return hg.map_(lambda key: hg.const(3.0), __keys__=keys)

    @graph
    def lookup(key: TS[str]) -> TS[float]:
        return price(key)

    @graph
    def client(key: TS[str]) -> TS[float]:
        hg.register_service(hg.default_path, price_impl)
        keys = hg.convert[hg.TSS](key)
        prices = hg.map_(lookup, __keys__=keys)
        return hg.reduce(hg.add_, prices, 0.0)

    assert eval_node(
        client,
        ["instrument"],
        __end_time__=hg.MIN_ST + 5 * hg.MIN_TD,
    ) == [0.0, 3.0]


def test_mapped_request_reply_service_can_call_itself_recursively():
    @hg.request_reply_service
    def add_one(path: str, value: TS[int]) -> TS[int]: ...

    @graph
    def add_one_item(value: TS[int]) -> TS[int]:
        zero, non_zero = hg.if_(value == 0, value)
        return hg.merge(
            hg.sample(zero, 1),
            add_one("recursive", non_zero - 1) + 1,
        )

    @hg.service_impl(interfaces=add_one)
    def add_one_impl(values: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return hg.map_(add_one_item, values)

    @graph
    def client(value: TS[int]) -> TS[int]:
        hg.register_service("recursive", add_one_impl)
        return add_one("recursive", value)

    result = eval_node(client, [3])
    assert result[-1] == 4


def test_generic_multi_service_reuses_shared_registration_specialization():
    @hg.request_reply_service
    def submit(
        path: str,
        ts: TS[hg.KEYABLE_SCALAR],
        tp: Type[hg.TIME_SERIES_TYPE] = TS[hg.KEYABLE_SCALAR],
    ) -> hg.TIME_SERIES_TYPE: ...

    @hg.reference_service
    def receive(path: str) -> hg.TSS[hg.KEYABLE_SCALAR]: ...

    @hg.subscription_service(resolvers={hg.SCALAR_1: str})
    def subscribe(path: str, ts: TS[hg.KEYABLE_SCALAR]) -> TS[hg.SCALAR_1]: ...

    @hg.service_impl(interfaces=(submit, receive, subscribe))
    def impl(
        path: str,
        tp: Type[hg.KEYABLE_SCALAR] = hg.AUTO_RESOLVE,
        tp_2: Type[hg.TIME_SERIES_TYPE] = TS[hg.KEYABLE_SCALAR],
    ):
        submissions: TSD[tp, TS[tp]] = submit[
            hg.KEYABLE_SCALAR:tp
        ].wire_impl_inputs_stub(path).ts
        submit[hg.KEYABLE_SCALAR:tp].wire_impl_out_stub(path, submissions)

        items = hg.map_(
            lambda key: hg.format_("{}", key),
            __keys__=hg.flip(submissions).key_set,
        )
        receive[hg.KEYABLE_SCALAR:tp].wire_impl_out_stub(path, items.key_set)
        subscription_keys = subscribe[
            hg.KEYABLE_SCALAR:tp
        ].wire_impl_inputs_stub(path).ts
        subscribe[hg.KEYABLE_SCALAR:tp].wire_impl_out_stub(
            path,
            hg.map_(
                lambda item: item,
                __keys__=subscription_keys,
                item=items,
            ),
        )

    @graph
    def client(
        ts1: TS[hg.SCALAR],
        ts2: TS[hg.SCALAR],
        tp: Type[hg.SCALAR] = hg.AUTO_RESOLVE,
    ) -> TSD[hg.SCALAR, TS[hg.SCALAR_1]]:
        hg.register_service("submit", impl)
        submit("submit", ts1)
        submit("submit", ts2)
        return hg.map_(
            lambda key: subscribe("submit", key),
            __keys__=receive[hg.KEYABLE_SCALAR:tp]("submit"),
        )

    assert eval_node(
        client[hg.SCALAR:int, hg.SCALAR_1:str],
        [1, None],
        [None, 2],
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    ) == [None, {}, {1: "1"}, {2: "2"}]
    assert eval_node(
        client[hg.SCALAR:str, hg.SCALAR_1:str],
        ["first", None],
        [None, "second"],
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    ) == [None, {}, {"first": "first"}, {"second": "second"}]


def test_same_named_service_specializations_can_have_different_reply_schemas():
    @hg.request_reply_service
    def submit(value: TS[hg.KEYABLE_SCALAR]): ...

    replyless = submit[hg.KEYABLE_SCALAR:int]

    @hg.request_reply_service
    def submit(
        value: TS[hg.KEYABLE_SCALAR],
        tp: Type[hg.TIME_SERIES_TYPE] = TS[hg.KEYABLE_SCALAR],
    ) -> hg.TIME_SERIES_TYPE: ...

    replying = submit[hg.KEYABLE_SCALAR:int]

    assert replyless.descriptor.name == "submit"
    assert replying.descriptor.name == "submit"
    assert replyless.descriptor is not replying.descriptor


def test_subscription_client_samples_existing_value_when_key_becomes_valid():
    @hg.subscription_service
    def subscribe(path: str, key: TS[str]) -> TS[str]: ...

    @hg.service_impl(interfaces=subscribe)
    def subscribe_impl(keys: hg.TSS[str]) -> TSD[str, TS[str]]:
        return hg.map_(hg.pass_through_node, __keys__=keys, __key_arg__="ts")

    @graph
    def clients(first: TS[str], second: TS[str], late: TS[str]) -> hg.TSL[TS[str], hg.Size[3]]:
        hg.register_service(hg.default_path, subscribe_impl)
        return hg.TSL.from_ts(
            hg.pass_through_node(subscribe(hg.default_path, first)),
            hg.pass_through_node(subscribe(hg.default_path, second)),
            hg.pass_through_node(subscribe(hg.default_path, late)),
        )

    assert eval_node(
        clients,
        ["topic-1", None, None],
        ["topic-2", None, None],
        [None, None, "topic-1"],
    ) == [None, {0: "topic-1", 1: "topic-2"}, {2: "topic-1"}]


def test_subscription_client_follows_a_late_mapped_bundle_value():
    @dataclass
    class Result(hg.TimeSeriesSchema):
        value: TS[int]

    @hg.subscription_service
    def lookup(key: TS[str]) -> hg.TSB[Result]: ...

    @hg.compute_node
    def as_reference(value: hg.REF[TS[int]]) -> hg.REF[TS[int]]:
        return value.value

    @graph
    def delayed_value(key: TS[str]) -> hg.TSB[Result]:
        return hg.TSB[Result].from_ts(
            value=as_reference(hg.lag(hg.len_(key), hg.MIN_TD))
        )

    @hg.service_impl(interfaces=lookup)
    def lookup_impl(keys: hg.TSS[str]) -> TSD[str, hg.TSB[Result]]:
        # Keep the mapped callable unannotated: its inferred structural REF
        # terminal is private to the child graph and must not leak through the
        # service's declared TSB value schema.
        return hg.map_(lambda key: delayed_value(key), __keys__=keys)

    @graph
    def client(key: TS[str]) -> TS[int]:
        hg.register_service(hg.default_path, lookup_impl)
        return lookup(key).value

    assert eval_node(
        client,
        ["abc"],
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    ) == [None, None, 3]


def test_subscription_client_const_lifts_a_plain_request_value():
    @hg.subscription_service
    def quote(symbol: TS[str]) -> TS[str]: ...

    @hg.service_impl(interfaces=quote)
    def quote_impl(symbols: hg.TSS[str]) -> TSD[str, TS[str]]:
        return hg.map_(hg.pass_through_node, __keys__=symbols, __key_arg__="ts")

    @graph
    def client() -> TS[str]:
        hg.register_service("quotes", quote_impl)
        return quote("EURUSD", path="quotes")

    assert eval_node(
        client, __end_time__=hg.MIN_ST + 3 * hg.MIN_TD,
    ) == [None, "EURUSD"]
