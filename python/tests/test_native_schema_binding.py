"""A namespaced CompoundScalar binds to its native schema by name (RFC 0042).

When a class declares ``namespace=`` and ``<namespace>::<ClassName>`` is
already a registered native schema, the class is that schema's Python face:
the native schema decides storage, and the class is validated against it
field by field. ``_hgraph.qualified_bundle_vt`` registers a named bundle in
the C++ registry exactly as a C++ declaration does, so each test declares its
own "native" schema under a name no other test uses.
"""

import dataclasses
import datetime
import typing

import _hgraph
import pytest

import hgraph as hg
from hgraph import TS, CompoundScalar, compute_node, graph
from hgraph._types import _value_type
from hgraph.test import eval_node

NS = "hgraph.test.binding"
TYPE = _hgraph.value_type("type")


def _native(name, **fields):
    return _hgraph.qualified_bundle_vt(NS, name, list(fields.items()))


def _vt(name):
    return _hgraph.value_type(name)


def test_a_namespaced_class_binds_to_the_native_schema_of_its_name():
    native = _native("Point", x=_vt("int"), label=_vt("str"))

    @dataclasses.dataclass(frozen=True)
    class Point(CompoundScalar, namespace=NS):
        x: int
        label: str

    assert _value_type(Point).name == native.name == f"{NS}::Point"

    @compute_node
    def make(x: TS[int]) -> TS[Point]:
        return Point(x=x.value, label=f"p{x.value}")

    @compute_node
    def read(p: TS[Point]) -> TS[str]:
        assert type(p.value) is Point
        return f"{p.value.label}:{p.value.x}"

    @graph
    def g(x: TS[int]) -> TS[str]:
        return read(make(x))

    assert eval_node(g, [1, 2]) == ["p1:1", "p2:2"]


def test_python_spells_a_native_type_value_type_at_any_depth():
    _native("Shape", tp=TYPE, types=_hgraph.tuple_vt(TYPE))

    @dataclasses.dataclass(frozen=True)
    class Shape(CompoundScalar, namespace=NS):
        tp: type
        types: tuple[type, ...]

    shape = Shape(tp=TS[int], types=(int, datetime.date))

    @compute_node
    def read(s: TS[Shape]) -> TS[str]:
        return f"{s.value.tp!r} {[t.__name__ for t in s.value.types]}"

    @graph
    def g() -> TS[str]:
        return read(hg.const(shape, tp=TS[Shape]))

    assert eval_node(g) == ["TS[int] ['int', 'date']"]


def test_fields_in_a_different_order_are_refused():
    _native("Ordered", a=_vt("int"), b=_vt("str"))

    @dataclasses.dataclass(frozen=True)
    class Ordered(CompoundScalar, namespace=NS):
        b: str
        a: int

    with pytest.raises(ValueError, match=r"fields \['b', 'a'\] do not match"):
        _value_type(Ordered)


def test_a_missing_field_is_refused():
    _native("Missing", a=_vt("int"), b=_vt("str"))

    @dataclasses.dataclass(frozen=True)
    class Missing(CompoundScalar, namespace=NS):
        a: int

    with pytest.raises(ValueError, match=r"fields \['a'\] do not match"):
        _value_type(Missing)


def test_a_field_of_the_wrong_type_is_refused_by_name():
    _native("WrongType", a=_vt("int"), b=_vt("str"))

    @dataclasses.dataclass(frozen=True)
    class WrongType(CompoundScalar, namespace=NS):
        a: int
        b: int

    with pytest.raises(ValueError, match=r"WrongType\.b: .* stores 'str'"):
        _value_type(WrongType)


def test_type_does_not_stand_for_an_ordinary_native_field():
    _native("NotAType", a=_vt("int"))

    @dataclasses.dataclass(frozen=True)
    class NotAType(CompoundScalar, namespace=NS):
        a: type

    with pytest.raises(ValueError, match=r"NotAType\.a: .* stores 'int'"):
        _value_type(NotAType)


def test_a_namespaced_class_with_no_native_twin_registers_from_its_annotations():
    @dataclasses.dataclass(frozen=True)
    class Unbound(CompoundScalar, namespace=NS):
        a: int
        tp: type

    fields = dict((name, vt.name) for name, vt in _value_type(Unbound).fields)
    assert fields == {"a": "int", "tp": _vt("object").name}


def test_a_second_class_of_a_different_shape_for_the_same_schema_is_refused():
    # An identical redefinition (a module reload) re-registers, as for any
    # CompoundScalar; a different reconstruction shape is a second face.
    _native("Twice", a=_vt("int"))

    @dataclasses.dataclass(frozen=True)
    class Twice(CompoundScalar, namespace=NS):
        a: int

    _value_type(Twice)

    def second():
        @dataclasses.dataclass(frozen=True)
        class Twice(CompoundScalar, namespace=NS):
            a: int = 0

        return _value_type(Twice)

    with pytest.raises(TypeError, match="different reconstruction shape"):
        second()


def test_a_subclass_inherits_the_native_fields():
    _native("Base", tp=TYPE)

    @dataclasses.dataclass(frozen=True)
    class Base(CompoundScalar, namespace=NS):
        tp: type

    @dataclasses.dataclass(frozen=True)
    class Derived(Base):
        extra: int = 0

    fields = [(name, vt.name) for name, vt in _value_type(Derived).fields]
    assert fields == [("tp", TYPE.name), ("extra", "int")]


def test_a_generic_specialisation_has_its_own_name_and_never_binds():
    _native("Boxed", a=_vt("int"))
    T = typing.TypeVar("T")

    @dataclasses.dataclass(frozen=True)
    class Boxed(CompoundScalar, typing.Generic[T], namespace=NS):
        a: T

    assert _value_type(Boxed[int]).name == f"{NS}::Boxed[int]"


def test_a_type_position_is_checked_where_it_sits_not_by_rewriting_the_name():
    # Python maps ``type`` and ``object`` to one scalar, so only the
    # annotation can say which position is a native type value.
    OBJECT = _vt("object")
    _hgraph.qualified_bundle_vt(NS, "Swapped", [("pair", _hgraph.fixed_tuple_vt([OBJECT, TYPE]))])

    @dataclasses.dataclass(frozen=True)
    class Swapped(CompoundScalar, namespace=NS):
        pair: tuple[type, object]

    with pytest.raises(ValueError, match=r"Swapped\.pair"):
        _value_type(Swapped)

    _hgraph.qualified_bundle_vt(NS, "Ordered2", [("pair", _hgraph.fixed_tuple_vt([OBJECT, TYPE]))])

    @dataclasses.dataclass(frozen=True)
    class Ordered2(CompoundScalar, namespace=NS):
        pair: tuple[object, type]

    assert _value_type(Ordered2).name == f"{NS}::Ordered2"


def test_an_empty_namespace_binds_to_a_top_level_native_schema():
    _hgraph.qualified_bundle_vt("", "RFC0042TopLevel", [("a", _vt("int"))])

    @dataclasses.dataclass(frozen=True)
    class RFC0042TopLevel(CompoundScalar, namespace=""):
        a: int

    assert _value_type(RFC0042TopLevel).name == "RFC0042TopLevel"


def test_a_self_recursive_native_schema_binds_through_its_owned_edge():
    _hgraph.recursive_bundle_vt(
        NS, "RecNode", [("value", _vt("int")), ("next", None)], [], False, "__type__", [], "")

    @dataclasses.dataclass(frozen=True)
    class RecNode(CompoundScalar, namespace=NS):
        value: int
        next: typing.Optional["RecNode"] = None

    fields = [(name, vt.name) for name, vt in _value_type(RecNode).fields]
    assert fields == [("value", "int"), ("next", f"Owned[{NS}::RecNode]")]
