import inspect
from dataclasses import InitVar, dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Callable, Generic, Optional, TypeVar

import _hgraph
import hgraph as hg
import pytest
from hgraph import (
    CompoundScalar,
    TS,
    TSB,
    TSD,
    TSW,
    TimeSeriesSchema,
    WindowSize,
    combine,
    compound_scalar,
    compute_node,
    const,
    default,
    from_json_builder,
    graph,
    mesh_,
    operator,
    to_json_builder,
)
# White-box: these tests assert on the interned C++ value-type metadata
# (qualified names, generic specialisation identity), which has no public
# introspection surface — the module under test is imported directly.
from hgraph._types import _value_type
from hgraph.reflection import fields
from hgraph.test import eval_node


def test_compound_scalar_qualified_names_and_hierarchy():
    @dataclass
    class Base(CompoundScalar, namespace="tests.orders", abstract=True):
        order_id: int

    @dataclass
    class Limit(Base):
        price: float

    base = _value_type(Base)
    limit = _value_type(Limit)

    assert base.name == "tests.orders::Base"
    assert base.namespace == "tests.orders"
    assert base.local_name == "Base"
    assert limit.name.endswith("::Limit")
    assert limit != base


def test_compound_scalar_default_namespace_includes_enclosing_scope():
    @dataclass
    class Local(CompoundScalar):
        value: int

    meta = _value_type(Local)
    assert meta.namespace == f"{__name__}.test_compound_scalar_default_namespace_includes_enclosing_scope.<locals>"
    assert meta.local_name == "Local"


def test_compound_scalar_dictionary_conversion_and_sparse_anonymous_values():
    @dataclass(frozen=True)
    class Inner(CompoundScalar):
        value: int

    @dataclass(frozen=True)
    class Outer(CompoundScalar):
        amount: float
        inner: Inner
        label: Optional[str] = None
        internal: str = field(
            default="implementation detail", init=False,
            metadata={"hidden": True},
        )

    value = Outer(amount=2.0, inner=Inner(value=1))
    assert value.to_dict() == {"amount": 2.0, "inner": {"value": 1}}
    assert tuple(inspect.signature(CompoundScalar.from_dict).parameters) == ("d",)
    assert (
        Outer.from_dict(
            d={
                "amount": 2.0,
                "inner": {"value": 1},
                "internal": "ignored because it is not part of the logical schema",
                "ignored": "unknown fields retain upstream's filtering behavior",
            }
        )
        == value
    )

    predicate = compound_scalar(a=int, b=int)
    sparse = predicate(a=1)
    assert sparse.b is None
    assert sparse.to_dict() == {"a": 1}
    assert predicate.from_dict({"a": 1, "ignored": 2}) == sparse


def test_compound_scalar_generic_specializations_are_invariant():
    value_type = TypeVar("value_type")

    @dataclass
    class Box(CompoundScalar, Generic[value_type], namespace="tests.generics"):
        value: value_type

    integer_box = _value_type(Box[int])
    string_box = _value_type(Box[str])

    assert integer_box != string_box
    assert integer_box.name == "tests.generics::Box[int]"
    assert string_box.name == "tests.generics::Box[str]"
    assert _value_type(Box[int]) == integer_box


def test_compound_scalar_callable_field_preserves_callable_signature():
    @dataclass(frozen=True)
    class CallbackConfig(CompoundScalar):
        callback: Callable[[str, int], str]

    @compute_node
    def invoke(config: TS[CallbackConfig]) -> TS[str]:
        return config.value.callback("value", 2)

    config = CallbackConfig(callback=lambda value, count: value * count)
    assert eval_node(invoke, [config]) == ["valuevalue"]


def test_combine_constructs_the_declared_concrete_base_alternative():
    @dataclass(frozen=True)
    class Base(CompoundScalar):
        value: int

    @dataclass(frozen=True)
    class Derived(Base):
        label: str

    @graph
    def build(value: TS[int]) -> TS[Base]:
        return combine[TS[Base]](value=value)

    assert eval_node(build, [3]) == [Base(value=3)]


def test_tsd_base_key_accepts_a_derived_key_port():
    @dataclass(frozen=True)
    class BaseKey(CompoundScalar):
        key: str

    @dataclass(frozen=True)
    class DerivedKey(BaseKey):
        qualifier: int

    @graph
    def lookup(values: TSD[BaseKey, TS[int]], key: TS[DerivedKey]) -> TS[int]:
        return values[key]

    key = DerivedKey(key="value", qualifier=1)
    assert eval_node(lookup, [{key: 7}], [key]) == [7]


def test_frozen_generic_compound_scalar_plain_argument_preserves_specialization():
    value_type = TypeVar("value_type")

    @dataclass(frozen=True)
    class Box(CompoundScalar, Generic[value_type], namespace="tests.generic_scalar_argument"):
        value: value_type

    @operator
    def read_box(box: Box[float], trigger: TS[bool]) -> TS[float]: ...

    @graph(overloads=read_box)
    def read_float_box(box: Box[float], trigger: TS[bool]) -> TS[float]:
        return const(box.value, TS[float])

    # Materialise another specialization first: selection must use the value
    # fields, not registration order.
    _value_type(Box[int])

    @graph
    def app(trigger: TS[bool]) -> TS[float]:
        return read_box(Box[float](value=1.5), trigger)

    assert eval_node(app, [True]) == [1.5]


def test_concrete_generic_child_reuses_specialized_parent_fields():
    value_type = TypeVar("value_type")

    @dataclass
    class Base(CompoundScalar, Generic[value_type], namespace="tests.generic_hierarchy", abstract=True):
        value: value_type

    @dataclass
    class IntegerValue(Base[int]):
        label: str

    integer_base = _value_type(Base[int])
    integer_value = _value_type(IntegerValue)
    string_base = _value_type(Base[str])

    assert integer_value != integer_base
    assert integer_value != string_base

    @compute_node
    def is_integer_value(value: TS[Base[int]]) -> TS[bool]:
        return isinstance(value.value, IntegerValue)

    assert eval_node(is_integer_value, [IntegerValue(value=1, label="one")]) == [True]


def test_multiple_inheritance_is_visible_through_each_parent():
    @dataclass
    class Identified(CompoundScalar, namespace="tests.multiple", abstract=True):
        identifier: int

    @dataclass
    class Priced(CompoundScalar, namespace="tests.multiple", abstract=True):
        price: float

    @dataclass
    class Order(Identified, Priced):
        venue: str

    @compute_node
    def identified_is_order(value: TS[Identified]) -> TS[bool]:
        return isinstance(value.value, Order)

    @compute_node
    def priced_is_order(value: TS[Priced]) -> TS[bool]:
        return isinstance(value.value, Order)

    order = Order(identifier=1, price=2.5, venue="X")
    assert eval_node(identified_is_order, [order]) == [True]
    assert eval_node(priced_is_order, [order]) == [True]


def test_python_node_round_trips_concrete_value_through_polymorphic_base_output():
    @dataclass
    class Base(CompoundScalar, namespace="tests.runtime", abstract=True):
        value: int

    @dataclass
    class Derived(Base):
        label: str

    @compute_node
    def upcast(value: TS[Derived]) -> TS[Base]:
        return value.value

    @compute_node
    def is_derived(value: TS[Base]) -> TS[bool]:
        return isinstance(value.value, Derived)

    @graph
    def app(value: TS[Derived]) -> TS[bool]:
        return is_derived(upcast(value))

    assert eval_node(app, [Derived(value=1, label="one")]) == [True]


def test_base_annotation_closes_over_defined_python_subclasses():
    @dataclass
    class Base(CompoundScalar, namespace="tests.closed", abstract=True):
        value: int

    @dataclass
    class Derived(Base):
        label: str

    @compute_node
    def is_derived(value: TS[Base]) -> TS[bool]:
        return isinstance(value.value, Derived)

    assert eval_node(is_derived, [Derived(value=1, label="one")]) == [True]


def test_closed_bundle_output_rejects_an_unrelated_compound_scalar():
    @dataclass
    class Base(CompoundScalar, namespace="tests.closed_error", abstract=True):
        value: int

    @dataclass
    class Derived(Base):
        label: str

    @dataclass
    class Unrelated(CompoundScalar, namespace="tests.closed_error"):
        value: int

    @compute_node
    def invalid_output(value: TS[int]) -> TS[Base]:
        return Unrelated(value.value)

    with pytest.raises(
        RuntimeError,
        match=r"Python type '.*\.Unrelated' is not an instance of closed Bundle",
    ):
        eval_node(invalid_output, [1])


def test_eval_node_finalizes_subclasses_before_const_lifting():
    @dataclass
    class Base(CompoundScalar, namespace="tests.const_lift", abstract=True):
        value: int

    @dataclass
    class Derived(Base):
        label: str

    @graph
    def app() -> TS[Base]:
        return const(Derived(value=1, label="one"), TS[Base])

    assert eval_node(app) == [Derived(value=1, label="one")]


def test_default_forwards_derived_fallback_through_non_abstract_base():
    @dataclass(frozen=True)
    class Base(CompoundScalar, namespace="tests.default_covariance"):
        value: int

    @dataclass(frozen=True)
    class Derived(Base):
        label: str

    fallback = Derived(value=1, label="one")

    @graph
    def app(value: TS[Base]) -> TS[Base]:
        return default(value, fallback)

    assert eval_node(app, [None]) == [fallback]


def test_polymorphic_union_accepts_canonical_derived_with_polymorphic_field():
    @dataclass(frozen=True)
    class Model(CompoundScalar, namespace="tests.nested_union_source", abstract=True):
        name: str

    @dataclass(frozen=True)
    class ConcreteModel(Model):
        parameter: int

    @dataclass(frozen=True)
    class Base(CompoundScalar, namespace="tests.nested_union_source", abstract=True):
        value: int

    @dataclass(frozen=True)
    class Derived(Base):
        model: Model

    fallback = Derived(value=1, model=ConcreteModel(name="m", parameter=2))

    @graph
    def app(value: TS[Base]) -> TS[Base]:
        return default(value, fallback)

    assert eval_node(app, [None]) == [fallback]


def test_mesh_normalizes_realized_composite_dependency_keys():
    @dataclass(frozen=True)
    class Option(CompoundScalar, namespace="tests.mesh_realized_key"):
        value: int

    @dataclass(frozen=True)
    class SpecialOption(Option):
        label: str

    @dataclass(frozen=True)
    class Request(CompoundScalar, namespace="tests.mesh_realized_key"):
        name: str
        option: Option

    @graph
    def dependency(key: TS[Request], link: TS[Request]) -> TS[int]:
        return default(mesh_(dependency)[link], 0) + 1

    @graph
    def app(links: TSD[Request, TS[Request]]) -> TSD[Request, TS[int]]:
        return mesh_(dependency, links)

    option = Option(0)
    root = Request("root", option)
    leaf = Request("leaf", option)
    assert eval_node(app, [{root: leaf}]) == [{root: 2, leaf: 1}]


def test_wide_polymorphic_keys_preserve_public_map_mesh_reduce_api():
    @dataclass(frozen=True)
    class Base(CompoundScalar, namespace="tests.graph_pool", abstract=True):
        identifier: int

    @dataclass(frozen=True)
    class Small(Base):
        pass

    @dataclass(frozen=True)
    class Large(Base):
        a: str
        b: str
        c: str

    @graph
    def identity(value: TS[int]) -> TS[int]:
        return value

    @graph
    def app(values: TSD[Base, TS[int]]) -> TS[int]:
        mapped = hg.map_(identity, values)
        meshed = mesh_(identity, values)
        return hg.reduce(hg.add_, mapped, 0) + hg.reduce(hg.add_, meshed, 0)

    small = Small(identifier=1)
    large = Large(identifier=2, a="large-a", b="large-b", c="large-c")
    with hg.GlobalContext(hg.GlobalState()):
        hg.set_pooled_compound_scalar_storage()
        assert eval_node(
            app,
            [
                {small: 2, large: 5},
                {small: 3},
                {large: hg.REMOVE},
                {small: hg.REMOVE},
            ],
        ) == [14, 16, 6, 0]


def test_wide_polymorphic_keys_cross_realtime_push_queue_as_external_values():
    import datetime
    import threading
    import time

    @dataclass(frozen=True)
    class Base(CompoundScalar, namespace="tests.graph_pool_push", abstract=True):
        identifier: int

    @dataclass(frozen=True)
    class Small(Base):
        pass

    @dataclass(frozen=True)
    class Large(Base):
        a: str
        b: str
        c: str

    small = Small(identifier=1)
    large = Large(identifier=2, a="large-a", b="large-b", c="large-c")
    observed = []
    threads = []

    @hg.push_queue(TSD[Base, TS[int]])
    def source(sender):
        def feed():
            time.sleep(0.05)
            sender({small: 2, large: 5})
            time.sleep(0.02)
            sender({small: 3, large: hg.REMOVE})

        thread = threading.Thread(target=feed)
        threads.append(thread)
        thread.start()

    @graph
    def identity(value: TS[int]) -> TS[int]:
        return value

    @hg.sink_node
    def capture(value: TS[int]) -> None:
        observed.append(value.value)

    @graph
    def app() -> None:
        capture(hg.reduce(hg.add_, hg.map_(identity, source()), 0))

    end = (
        datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
        + datetime.timedelta(seconds=0.5)
    )
    with hg.GlobalContext(hg.GlobalState()):
        hg.set_pooled_compound_scalar_storage()
        hg.run_graph(app, end_time=end, run_mode=hg.EvaluationMode.REAL_TIME)
    for thread in threads:
        thread.join()

    assert observed == [0, 7, 3]


def test_polymorphic_bundle_field_uses_the_graph_realization():
    @dataclass
    class Base(CompoundScalar, namespace="tests.nested", abstract=True):
        value: int

    @dataclass
    class Derived(Base):
        label: str

    @dataclass
    class Envelope(CompoundScalar, namespace="tests.nested"):
        item: Base

    @compute_node
    def contains_derived(value: TS[Envelope]) -> TS[bool]:
        return isinstance(value.value.item, Derived)

    assert eval_node(
        contains_derived,
        [Envelope(item=Derived(value=1, label="one"))],
    ) == [True]


def test_covariant_field_annotation_reuses_inherited_native_schema():
    @dataclass
    class Base(CompoundScalar, namespace="tests.covariant_field", abstract=True):
        value: int

    @dataclass
    class Derived(Base):
        label: str

    @dataclass
    class Container(CompoundScalar, namespace="tests.covariant_field"):
        item: Base

    @dataclass
    class NarrowContainer(Container):
        item: Derived

    native_fields = dict(_value_type(NarrowContainer).fields)
    assert native_fields["item"] == _value_type(Base)
    assert fields(NarrowContainer)["item"] is Derived

    @compute_node
    def is_derived(value: TS[NarrowContainer]) -> TS[bool]:
        return isinstance(value.value.item, Derived)

    assert eval_node(
        is_derived,
        [NarrowContainer(item=Derived(value=1, label="one"))],
    ) == [True]


def test_tsb_output_field_uses_the_graph_realization():
    @dataclass
    class Result(CompoundScalar, namespace="tests.tsb_output", abstract=True):
        value: int

    @dataclass
    class DetailedResult(Result):
        detail: str

    class HandlerOutput(TimeSeriesSchema):
        response: TS[Result]
        audit: TS[str]

    @compute_node
    def handler(value: TS[int]) -> TSB[HandlerOutput]:
        return {
            "response": DetailedResult(value=value.value, detail="accepted"),
            "audit": f"value={value.value}",
        }

    assert eval_node(handler, [7]) == [{
        "response": DetailedResult(value=7, detail="accepted"),
        "audit": "value=7",
    }]


def test_tsb_output_keyed_field_uses_the_graph_realization():
    @dataclass
    class Result(CompoundScalar, namespace="tests.tsb_keyed_output", abstract=True):
        value: int

    @dataclass
    class DetailedResult(Result):
        detail: str

    class HandlerOutput(TimeSeriesSchema):
        response: TSD[int, TS[Result]]
        audit: TS[str]

    @compute_node
    def handler(value: TS[int]) -> TSB[HandlerOutput]:
        return {
            "response": {1: DetailedResult(value=value.value, detail="accepted")},
            "audit": f"value={value.value}",
        }

    assert eval_node(handler, [7]) == [{
        "response": {1: DetailedResult(value=7, detail="accepted")},
        "audit": "value=7",
    }]


def test_compact_container_preserves_mixed_derived_elements_across_node_input():
    @dataclass
    class Base(CompoundScalar, namespace="tests.container", abstract=True):
        value: int

    @dataclass
    class Derived(Base):
        label: str

    @dataclass
    class OtherDerived(Base):
        quantity: int

    @dataclass
    class Batch(CompoundScalar, namespace="tests.container"):
        items: tuple[Base, ...]

    @compute_node
    def preserves_derived_types(value: TS[Batch]) -> TS[bool]:
        return (
            isinstance(value.value.items[0], Derived)
            and isinstance(value.value.items[1], OtherDerived)
        )

    assert eval_node(
        preserves_derived_types,
        [Batch(items=(
            Derived(value=1, label="one"),
            OtherDerived(value=2, quantity=10),
        ))],
    ) == [True]


def test_configured_json_discriminator_selects_the_concrete_class():
    @dataclass
    class Base(
        CompoundScalar,
        namespace="tests.json",
        abstract=True,
        discriminator="kind",
    ):
        value: int

    @dataclass
    class Derived(Base, namespace="tests.json"):
        label: str

    decode = from_json_builder(Base)
    encode = to_json_builder(Base)
    value = decode(
        '{"kind": "Derived", "value": 1, "label": "one"}'
    )

    assert isinstance(value, Derived)
    assert '"kind": "tests.json::Derived"' in encode(value)


def test_self_recursive_compound_scalar_allocates_children_on_demand():
    @dataclass
    class RecursiveValue(CompoundScalar, namespace="tests.recursion"):
        value: int
        next: Optional["RecursiveValue"] = None

    @compute_node
    def child_value(value: TS[RecursiveValue]) -> TS[int]:
        child = value.value.next
        return -1 if child is None else child.value

    result = eval_node(
        child_value,
        [RecursiveValue(1, RecursiveValue(2)), RecursiveValue(3)],
    )

    assert result == [2, -1]


def test_mutually_recursive_compound_scalars_resolve_as_one_closed_group():
    @dataclass
    class Left(CompoundScalar, namespace="tests.mutual_recursion"):
        value: int
        right: Optional["Right"] = None

    @dataclass
    class Right(CompoundScalar, namespace="tests.mutual_recursion"):
        value: int
        left: Optional[Left] = None

    @graph
    def nested_value(value: TS[Left]) -> TS[int]:
        return value.right.left.value

    source = Left(1, Right(2, Left(3)))
    assert eval_node(nested_value, [source]) == [3]


def test_compound_scalar_readback_reconstructs_frozen_slotted_value_without_init():
    initialized = []

    @dataclass(frozen=True, slots=True, kw_only=True)
    class Value(CompoundScalar, namespace="tests.reconstruction"):
        number: int
        label: str

        def __post_init__(self):
            initialized.append((self.number, self.label))

    @compute_node
    def inspect(value: TS[Value]) -> TS[bool]:
        current = value.value
        return isinstance(current, Value) and current.number == 7 and current.label == "seven"

    source = Value(number=7, label="seven")
    assert eval_node(inspect, [source]) == [True]
    assert initialized == [(7, "seven")]


def test_compound_scalar_output_still_accepts_attribute_proxy():
    @dataclass
    class Value(CompoundScalar, namespace="tests.attribute_proxy"):
        number: int
        label: str

    class Proxy:
        def __init__(self, number, label):
            self._values = {"number": number, "label": label}

        def __getattr__(self, name):
            return self._values[name]

    @compute_node
    def build(value: TS[int]) -> TS[Value]:
        return Proxy(value.value, str(value.value))

    assert eval_node(build, [3]) == [Value(3, "3")]


def test_inherited_field_replaced_by_init_var_remains_in_logical_schema():
    @dataclass(frozen=True)
    class Base(CompoundScalar, namespace="tests.computed", abstract=True):
        name: str = ""

    @dataclass(frozen=True)
    class Derived(Base):
        # ExprClass uses this shape for a computed inherited field: the
        # InitVar removes the dataclass field while a hidden field stores an
        # explicit override. The logical CompoundScalar field remains name.
        name: InitVar[str] = "derived"
        _override_name: str = field(
            default="derived", init=False, metadata={"hidden": True})

    @compute_node
    def inspect(value: TS[Base]) -> TS[str]:
        return value.value.name

    meta = _value_type(Derived)
    assert [name for name, _ in meta.fields] == ["name"]
    assert eval_node(inspect, [Derived()]) == ["derived"]


def test_computed_init_var_bundle_uses_public_constructor_on_readback():
    class ReadOnlyComputed:
        def __get__(self, instance, owner=None):
            if instance is None:
                return self
            return instance.__dict__.get("_override_name", "computed")

        def __set__(self, instance, value):
            raise AttributeError("computed field is read-only")

    computed = ReadOnlyComputed()

    @dataclass(frozen=True)
    class Base(CompoundScalar, namespace="tests.computed_readback", abstract=True):
        name: str = ""

    @dataclass(frozen=True)
    class Derived(Base):
        name: InitVar[str] = computed
        _override_name: str = field(
            default="computed", init=False, metadata={"hidden": True})

        def __post_init__(self, name):
            if name is not computed:
                object.__setattr__(self, "_override_name", name)

    @compute_node
    def inspect(value: TS[Base]) -> TS[str]:
        return value.value.name

    assert eval_node(inspect, [Derived(name="overridden")]) == ["overridden"]


def test_compound_scalar_with_custom_new_preserves_interned_identity():
    from typing import ClassVar

    @dataclass(frozen=True, init=False)
    class Interned(CompoundScalar, namespace="tests.interned_readback"):
        key: int
        _instances: ClassVar[dict[int, "Interned"]] = {}

        def __new__(cls, key):
            if key in cls._instances:
                return cls._instances[key]
            value = super().__new__(cls)
            object.__setattr__(value, "key", key)
            cls._instances[key] = value
            return value

    @compute_node
    def identity(value: TS[Interned]) -> TS[Interned]:
        return value.value

    value = Interned(7)
    assert eval_node(identity, [value])[0] is value


def test_optional_scalar_fields_use_unset_bundle_fields_for_none():
    @dataclass(frozen=True)
    class Value(CompoundScalar, namespace="tests.optional_fields"):
        number: Optional[int] = None
        labels: Optional[tuple[str, ...]] = None

    @compute_node
    def identity(value: TS[Value]) -> TS[Value]:
        return value.value

    values = [Value(), Value(number=7, labels=("seven",))]
    assert eval_node(identity, values) == values


def test_time_series_schema_can_be_lifted_from_compound_scalar():
    @dataclass(frozen=True)
    class Value(CompoundScalar, namespace="tests.schema_lift"):
        number: int
        label: str

    schema = TimeSeriesSchema.from_scalar_schema(Value)

    assert schema.scalar_type() is Value
    assert schema.to_scalar_schema() is Value
    assert schema.__annotations__ == {"number": TS[int], "label": TS[str]}
    assert TSB[schema].handle.is_tsb


def test_time_series_schema_to_scalar_schema_preserves_inheritance():
    class BaseBundle(TimeSeriesSchema):
        number: TS[int]

    class DerivedBundle(BaseBundle):
        label: TS[str]

    base_scalar = BaseBundle.to_scalar_schema()
    derived_scalar = DerivedBundle.to_scalar_schema()

    assert issubclass(derived_scalar, base_scalar)
    assert derived_scalar.__annotations__ == {"label": str}
    assert derived_scalar(number=7, label="seven").number == 7


def test_time_series_schema_to_scalar_schema_supports_windows():
    class WindowBundle(TimeSeriesSchema):
        tick: TSW[int, WindowSize[3]]
        duration: TSW[str, WindowSize[timedelta(seconds=3)]]

    scalar = WindowBundle.to_scalar_schema()

    assert scalar.__annotations__ == {"tick": int, "duration": str}
    assert scalar(tick=7, duration="seven").duration == "seven"


def test_recursive_base_reference_inside_container_uses_owned_storage():
    @dataclass(frozen=True)
    class Base(CompoundScalar, namespace="tests.container_recursion", abstract=True):
        name: str

    @dataclass(frozen=True)
    class Leaf(Base):
        children: tuple[tuple[Base, int], ...] = ()

    @compute_node
    def identity(value: TS[Base]) -> TS[Base]:
        return value.value

    child = Leaf("child")
    parent = Leaf("parent", ((child, 2),))
    assert eval_node(identity, [parent]) == [parent]


def test_string_valued_enum_field_uses_native_enum_storage():
    class Method(Enum):
        PHYSICAL = "physical"
        FINANCIAL = "financial"

    @dataclass(frozen=True)
    class Settlement(CompoundScalar, namespace="tests.string_enum"):
        method: Method

    @compute_node
    def identity(value: TS[Settlement]) -> TS[Settlement]:
        return value.value

    value = Settlement(Method.FINANCIAL)
    assert eval_node(identity, [value]) == [value]


def test_compound_scalar_tsb_ignores_dataclass_kw_only_marker():
    from dataclasses import KW_ONLY
    from typing import Generic, TypeVar

    Value = TypeVar("Value")

    @dataclass(frozen=True)
    class Quoted(CompoundScalar, Generic[Value], namespace="tests.kw_only"):
        _: KW_ONLY
        value: Value

    assert [name for name, _ in _value_type(Quoted[int]).fields] == ["value"]
    assert [name for name, _ in _hgraph.ts_field_types(TSB[Quoted[int]].handle)] == ["value"]
    assert TSB[Quoted].pattern.ts_kind == _hgraph.TS_KIND_TSB


def test_combine_compound_scalar_resolves_string_field_annotations():
    @dataclass(frozen=True)
    class Deferred(CompoundScalar, namespace="tests.deferred_annotation"):
        value: "int"

    @graph
    def make() -> TS[Deferred]:
        return combine[TS[Deferred]](value=1)

    assert eval_node(make) == [Deferred(1)]
