from dataclasses import dataclass
from typing import Generic, TypeVar

import pytest

import hgraph as hg
from hgraph.test import eval_node


T = TypeVar("T")


class _TestContext:
    __instance__ = None

    def __init__(self, msg: str = "non-default"):
        self.msg = msg

    @classmethod
    def instance(cls):
        if cls.__instance__ is None:
            return cls("default")
        return cls.__instance__

    def __enter__(self):
        _TestContext.__instance__ = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if _TestContext.__instance__ is not self:
            raise ValueError("Exiting context not entered")
        _TestContext.__instance__ = None


def test_optional_named_context_uses_the_node_default_when_absent():
    @hg.compute_node
    def use_context(
        ts: hg.TS[bool], context: hg.CONTEXT[_TestContext] = None,
    ) -> hg.TS[str]:
        return _TestContext.instance().msg

    @hg.graph
    def app(ts: hg.TS[bool]) -> hg.TS[str]:
        return use_context(ts, context="missing")

    assert eval_node(app, [True, None, False]) == ["default", None, "default"]


def test_required_named_context_still_fails_when_absent():
    @hg.compute_node
    def use_context(
        ts: hg.TS[bool], context: hg.CONTEXT[_TestContext] = None,
    ) -> hg.TS[str]:
        return _TestContext.instance().msg

    @hg.graph
    def app(ts: hg.TS[bool]) -> hg.TS[str]:
        return use_context(ts, context=hg.REQUIRED["missing"])

    with pytest.raises(hg.WiringError, match="with name missing"):
        eval_node(app, [True])


def test_generic_context_resolves_from_the_published_port():
    @hg.compute_node
    def read_context(
        trigger: hg.TS[bool],
        value: hg.CONTEXT[hg.TIME_SERIES_TYPE] = hg.REQUIRED["value"],
    ) -> hg.TS[str]:
        return f"{dict(value.value)} {trigger.value}"

    @hg.graph
    def app(
        trigger: hg.TS[bool], values: hg.TSD[int, hg.TS[int]],
    ) -> hg.TS[str]:
        with values as value:
            return read_context(trigger)

    assert eval_node(app, [True, False], [{1: 2}, {2: 3}]) == [
        "{1: 2} True",
        "{1: 2, 2: 3} False",
    ]


def test_two_generic_contexts_resolve_by_name():
    @hg.compute_node
    def join_contexts(
        lhs: hg.CONTEXT[hg.TIME_SERIES_TYPE] = "lhs",
        rhs: hg.CONTEXT[hg.TIME_SERIES_TYPE] = "rhs",
    ) -> hg.TS[str]:
        return f"{lhs.value} {rhs.value}"

    @hg.graph
    def app(lhs_value: hg.TS[str], rhs_value: hg.TS[str]) -> hg.TS[str]:
        with lhs_value as lhs, rhs_value as rhs:
            return join_contexts()

    assert eval_node(app, ["Hello", None], [None, "World"]) == [
        None,
        "Hello World",
    ]


def test_context_input_accepts_an_explicit_port_override():
    @hg.compute_node
    def read_context(
        value: hg.CONTEXT[hg.TIME_SERIES_TYPE] = hg.REQUIRED["value"],
    ) -> hg.TS[str]:
        return f"{value.value}"

    @hg.graph
    def app(value: hg.TS[str]) -> hg.TS[str]:
        return read_context(value)

    assert eval_node(app, ["Hello", None]) == ["Hello", None]


def test_context_crosses_stacked_try_except_boundaries():
    @hg.compute_node
    def read_context(
        value: hg.CONTEXT[hg.TIME_SERIES_TYPE] = hg.REQUIRED["value"],
    ) -> hg.TS[str]:
        return f"{value.value}"

    @hg.graph
    def inner() -> hg.TS[str]:
        return read_context()

    @hg.graph
    def middle(outer: hg.TS[str], inner_value: hg.TS[str]) -> hg.TS[str]:
        with inner_value as value:
            return hg.try_except(inner).out

    @hg.graph
    def app(outer: hg.TS[str], inner_value: hg.TS[str]) -> hg.TS[str]:
        with outer as value:
            return hg.try_except(middle, outer, inner_value).out

    assert eval_node(app, ["Hello", None], [None, "World"]) == [None, "World"]


def test_graph_context_parameters_resolve_before_composition():
    @hg.graph
    def join_contexts(
        lhs: hg.CONTEXT[hg.TIME_SERIES_TYPE] = "lhs",
        rhs: hg.CONTEXT[hg.TIME_SERIES_TYPE] = "rhs",
    ) -> hg.TS[str]:
        return hg.format_("{} {}", lhs, rhs, __strict__=False)

    @hg.graph
    def nested() -> hg.TS[str]:
        return join_contexts()

    @hg.graph
    def app(lhs_value: hg.TS[str], rhs_value: hg.TS[str]) -> hg.TS[str]:
        with lhs_value as lhs, rhs_value as rhs:
            return nested()

    assert eval_node(app, ["Hello", None], [None, "World"]) == [
        "Hello None",
        "Hello World",
    ]


def test_missing_optional_graph_context_is_an_invalid_output():
    @hg.graph
    def read_context(value: hg.CONTEXT[hg.TS[str]] = "value") -> hg.TS[str]:
        return value

    @hg.graph
    def app(trigger: hg.TS[bool]) -> hg.TS[str]:
        return read_context()

    assert eval_node(app, [True]) is None


def test_context_crosses_nested_map_and_switch_boundaries():
    @hg.compute_node
    def make_context(value: hg.TS[str]) -> hg.TS[_TestContext]:
        return _TestContext(value.value)

    @hg.compute_node
    def read_context(
        value: hg.TS[bool],
        context: hg.CONTEXT[hg.TS[_TestContext]] = hg.REQUIRED,
    ) -> hg.TS[str]:
        return f"{_TestContext.instance().msg} {value.value}"

    @hg.graph
    def inner(value: hg.TS[bool], prefix: hg.TS[str]) -> hg.TS[str]:
        with make_context(hg.format_("{}-", prefix)):
            return hg.switch_(
                value,
                {
                    True: lambda selected: read_context(selected),
                    False: lambda selected: hg.format_("plain {}", selected),
                },
                value,
            )

    @hg.graph
    def app(
        values: hg.TSD[int, hg.TS[bool]],
        prefixes: hg.TSD[int, hg.TS[str]],
    ) -> hg.TSD[int, hg.TS[str]]:
        return hg.map_(inner, values, prefixes)

    assert eval_node(app, [{1: True, 2: False}], [{1: "one", 2: "two"}]) == [
        {1: "one- True", 2: "plain False"}
    ]


def test_context_is_visible_inside_a_default_path_service():
    @hg.reference_service
    def value_service(path: str = "value") -> hg.TS[str]: ...

    @hg.service_impl(interfaces=value_service)
    def value_impl(path: str = "value") -> hg.TS[str]:
        return hg.get_context[hg.TS[str]]("value_context")

    @hg.graph
    def app() -> hg.TS[str]:
        with hg.const("context value") as value_context:
            out = value_service()
            hg.register_service(None, value_impl)
            return out

    assert eval_node(app, __elide__=True) == ["context value"]


def test_compound_context_selects_a_compatible_base_type():
    @dataclass(frozen=True)
    class ContextValue(hg.CompoundScalar, _TestContext):
        count: int
        msg: str = "bundle"

    @hg.compute_node(valid=("trigger", "context"))
    def read_context(
        trigger: hg.TS[bool], context: hg.CONTEXT[_TestContext] = None,
    ) -> hg.TS[str]:
        return _TestContext.instance().msg

    @hg.graph
    def app(trigger: hg.TS[bool]) -> hg.TS[str]:
        with hg.combine[hg.TSB[ContextValue]](count=1, msg="bundle"):
            return read_context(trigger)

    assert eval_node(app, [True, None, False]) == ["bundle", None, "bundle"]


def test_parameterized_dataclass_context_selects_a_compatible_base_type():
    @dataclass(frozen=True)
    class ContextValue(_TestContext, Generic[T]):
        value: T
        msg: str = "generic bundle"

    @hg.compute_node(valid=("trigger", "context"))
    def read_context(
        trigger: hg.TS[bool], context: hg.CONTEXT[_TestContext] = None,
    ) -> hg.TS[str]:
        return _TestContext.instance().msg

    @hg.graph
    def app(trigger: hg.TS[bool]) -> hg.TS[str]:
        with hg.combine[hg.TSB[ContextValue[int]]](
            value=1, msg="generic bundle"
        ):
            return hg.switch_(
                trigger,
                {
                    True: lambda selected: read_context(selected),
                    False: lambda selected: hg.format_(
                        "{} false", read_context(selected)
                    ),
                },
                trigger,
            )

    assert eval_node(app, [True, None, False]) == [
        "generic bundle",
        None,
        "generic bundle false",
    ]


def test_parameterized_dataclass_context_from_service_crosses_switch_boundary():
    @dataclass(frozen=True)
    class ContextValue(_TestContext, Generic[T]):
        value: T
        msg: str

    @hg.subscription_service
    def lookup(key: hg.TS[str]) -> hg.TSB[ContextValue[int]]: ...

    @hg.service_impl(interfaces=lookup)
    def lookup_impl(keys: hg.TSS[str]) -> hg.TSD[str, hg.TSB[ContextValue[int]]]:
        return hg.map_(
            lambda key: hg.TSB[ContextValue[int]].from_ts(value=1, msg=key),
            __keys__=keys,
        )

    @hg.compute_node(valid=("trigger", "context"))
    def read_context(
        trigger: hg.TS[bool], context: hg.CONTEXT[_TestContext] = None,
    ) -> hg.TS[str]:
        return _TestContext.instance().msg

    @hg.graph
    def app(trigger: hg.TS[bool], key: hg.TS[str]) -> hg.TS[str]:
        hg.register_service(hg.default_path, lookup_impl)
        with lookup(key):
            return hg.switch_(
                trigger,
                {
                    True: lambda selected: read_context(selected),
                    False: lambda selected: hg.format_(
                        "{} false", read_context(selected)
                    ),
                },
                trigger,
            )

    assert eval_node(app, [True, None, False], ["context"]) == [
        None,
        "context",
        "context false",
    ]


def test_tuple_spelled_subscription_impl_retains_nested_context_transport():
    @hg.reference_service
    def inner_service(path: str = "inner") -> hg.TS[str]: ...

    @hg.service_impl(interfaces=(inner_service,))
    def inner_impl(path: str = "inner") -> hg.TS[str]:
        selected = hg.get_context[hg.TS[str]]("selected")
        return hg.switch_(
            selected,
            {
                "selected": lambda value: value,
                "other": lambda value: hg.nothing[hg.TS[str]](),
            },
            selected,
        )

    @hg.subscription_service
    def outer_service(request: hg.TS[str], path: str = "outer") -> hg.TS[str]: ...

    @hg.service_impl(interfaces=(outer_service,))
    def outer_impl(
        request: hg.TSS[str], path: str = "outer",
    ) -> hg.TSD[str, hg.TS[str]]:
        return hg.map_(
            lambda request: inner_service(),
            __keys__=request,
            __key_arg__="request",
        )

    @hg.graph
    def app() -> hg.TS[str]:
        with hg.const("selected") as selected:
            out = outer_service("x")
            hg.register_service(None, outer_impl)
            hg.register_service(None, inner_impl)
            hg.WiringGraphContext.instance().build_services()
            return out

    assert eval_node(app, __elide__=True) == ["selected"]
