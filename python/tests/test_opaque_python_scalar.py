from dataclasses import dataclass
from typing import Type, TypeVar

import _hgraph

from hgraph import AUTO_RESOLVE, TS, compute_node, const, graph, operator
from hgraph.reflection import resolved_type
from hgraph.test import eval_node


class OpaqueBase:
    pass


class OpaqueDerived(OpaqueBase):
    pass


class OtherOpaque:
    pass


def _scalar_value_type(ts_type):
    return _hgraph.ts_value_vt(ts_type.handle)


def test_opaque_python_classes_have_distinct_native_type_identity():
    assert TS[OpaqueBase] != TS[OtherOpaque]
    assert _scalar_value_type(TS[OpaqueBase]) != _scalar_value_type(TS[OtherOpaque])


def test_native_opaque_value_type_reflects_to_declared_python_class():
    assert resolved_type(_scalar_value_type(TS[OpaqueBase])) is OpaqueBase
    assert resolved_type(_scalar_value_type(TS[OtherOpaque])) is OtherOpaque


def test_opaque_python_scalar_reflection_is_not_changed_by_later_annotations():
    base_value_type = _scalar_value_type(TS[OpaqueBase])

    TS[OtherOpaque]
    TS[type[OtherOpaque]]

    assert resolved_type(base_value_type) is OpaqueBase
    assert resolved_type(_scalar_value_type(TS[object])) is object


def test_parameterized_python_annotations_share_their_origin_identity():
    assert TS[type[OpaqueBase]] == TS[type[OpaqueDerived]]


def test_type_of_opaque_class_uses_its_origin_for_wiring():
    @graph
    def accept_base_type(value: TS[type[OpaqueBase]]) -> TS[type[OpaqueBase]]:
        return value

    @graph
    def app(value: TS[type[OpaqueDerived]]) -> TS[type[OpaqueBase]]:
        return accept_base_type(value)

    assert eval_node(app, [OpaqueDerived]) == [OpaqueDerived]


def test_python_owned_bundle_field_can_narrow_a_class_type():
    @dataclass(frozen=True)
    class HolderBase:
        value_type: Type[OpaqueBase]

    @dataclass(frozen=True)
    class HolderDerived(HolderBase):
        value_type: Type[OpaqueDerived]

    assert TS[HolderDerived] != TS[HolderBase]


def test_opaque_python_scalar_auto_resolve_is_stable():
    scalar = TypeVar("scalar")

    @compute_node
    def type_name(value: TS[scalar], tp: type[scalar] = AUTO_RESOLVE) -> TS[str]:
        return tp.__name__

    TS[OtherOpaque]

    assert eval_node(type_name, [OpaqueBase()]) == ["OpaqueBase"]


def test_opaque_python_scalar_is_covariant_for_wiring():
    @graph
    def accept_base(value: TS[OpaqueBase]) -> TS[OpaqueBase]:
        return value

    @graph
    def app(value: TS[OpaqueDerived]) -> TS[OpaqueBase]:
        return accept_base(value)

    value = OpaqueDerived()
    assert eval_node(app, [value]) == [value]


def test_constrained_zero_input_operator_distinguishes_opaque_classes():
    scalar = TypeVar("scalar", OpaqueBase, OtherOpaque)

    @operator
    def load(tp: type[scalar] = AUTO_RESOLVE) -> TS[str]: ...

    @graph(overloads=load, requires=lambda m: resolved_type(m[scalar]) is OpaqueBase)
    def load_base() -> TS[str]:
        return const("base")

    @graph(overloads=load, requires=lambda m: resolved_type(m[scalar]) is OtherOpaque)
    def load_other() -> TS[str]:
        return const("other")

    assert eval_node(load[OpaqueBase]) == ["base"]
    assert eval_node(load[OtherOpaque]) == ["other"]


def test_opaque_python_values_round_trip_without_wrapping():
    @compute_node
    def echo(value: TS[OpaqueBase]) -> TS[OpaqueBase]:
        return value.value

    value = OpaqueDerived()
    result = eval_node(echo, [value])

    assert result == [value]
    assert result[0] is value


def test_const_infers_opaque_identity_without_overriding_native_scalars():
    value = OpaqueDerived()

    @graph
    def opaque_value() -> TS[OpaqueDerived]:
        return const(value)

    @graph
    def native_int() -> TS[int]:
        return const(2)

    opaque_result = eval_node(opaque_value)
    assert opaque_result == [value]
    assert opaque_result[0] is value
    assert eval_node(native_int) == [2]


def test_object_remains_the_common_opaque_python_top_type():
    @graph
    def accept_object(value: TS[object]) -> TS[object]:
        return value

    @graph
    def from_base(value: TS[OpaqueBase]) -> TS[object]:
        return accept_object(value)

    @graph
    def from_other(value: TS[OtherOpaque]) -> TS[object]:
        return accept_object(value)

    base = OpaqueBase()
    other = OtherOpaque()
    assert eval_node(from_base, [base]) == [base]
    assert eval_node(from_other, [other]) == [other]


def test_nearest_opaque_base_overload_beats_object_overload():
    @operator
    def describe(value: TS[object]) -> TS[str]: ...

    @graph(overloads=describe)
    def describe_object(value: TS[object]) -> TS[str]:
        return const("object")

    @graph(overloads=describe)
    def describe_base(value: TS[OpaqueBase]) -> TS[str]:
        return const("base")

    @graph
    def app(value: TS[OpaqueDerived]) -> TS[str]:
        return describe(value)

    assert eval_node(app, [OpaqueDerived()]) == ["base"]
