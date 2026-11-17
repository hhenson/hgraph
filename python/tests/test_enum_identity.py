"""A Python ``Enum`` is a value type known by its class, never by its name.

Two classes of one name in two modules are two types, a user's enum that
shares a name with one of hgraph's own is not taken for it, and hgraph's own
enums keep their native types.
"""
from enum import Enum

import hgraph as hg
from hgraph import TS
from hgraph._types import _value_type
from hgraph.test import eval_node


def _enum(name, members, module):
    """An ``Enum`` class as a module of that name would define it."""
    return Enum(name, members, module=module, qualname=name)


def _passed_through(enum_class, values):
    @hg.compute_node
    def keep(value: TS[enum_class]) -> TS[enum_class]:
        return value.value

    return eval_node(keep, values)


def test_two_enums_of_one_name_and_different_members_are_two_types():
    first = _enum("Side", {"BUY": 1, "SELL": 2}, "enum_identity.orders")
    second = _enum("Side", {"LEFT": 1, "RIGHT": 2}, "enum_identity.layout")

    assert _value_type(first) != _value_type(second)
    assert _passed_through(first, [first.BUY, first.SELL]) == [first.BUY, first.SELL]
    assert _passed_through(second, [second.RIGHT, second.LEFT]) == [second.RIGHT, second.LEFT]


def test_two_enums_of_one_name_and_one_member_table_keep_their_own_classes():
    # The case a name could not tell apart: the same members under the same
    # name. Each value still reads back as a member of its own class.
    first = _enum("Colour", {"RED": 1, "BLUE": 2}, "enum_identity.paint")
    second = _enum("Colour", {"RED": 1, "BLUE": 2}, "enum_identity.light")

    assert _value_type(first) != _value_type(second)
    for enum_class in (first, second, first):
        result = _passed_through(enum_class, [enum_class.BLUE])
        assert result == [enum_class.BLUE]
        assert type(result[0]) is enum_class


def test_a_users_enum_is_not_taken_for_hgraphs_own_of_that_name():
    ordering = _enum("CmpResult", {"LT": 0, "EQ": 1, "GT": 2}, "enum_identity.ordering")

    assert _value_type(ordering) != _value_type(hg.CmpResult)
    assert _passed_through(ordering, [ordering.GT, ordering.LT]) == [ordering.GT, ordering.LT]
    assert type(_passed_through(ordering, [ordering.EQ])[0]) is ordering


def test_hgraphs_own_enums_keep_their_native_types():
    import _hgraph

    assert _value_type(hg.CmpResult) == _hgraph.value_type("CmpResult")
    assert _value_type(hg.DivideByZero) == _hgraph.value_type("DivideByZero")
    assert _value_type(hg.ToTableMode) == _hgraph.value_type("ToTableMode")

    @hg.graph
    def compared(lhs: TS[int], rhs: TS[int]) -> TS[hg.CmpResult]:
        return hg.cmp_(lhs, rhs)

    assert eval_node(compared, [1, 2, 3], [2, 2, 2]) == [
        hg.CmpResult.LT, hg.CmpResult.EQ, hg.CmpResult.GT]


def test_an_enum_is_registered_once_under_its_module_and_qualified_name():
    side = _enum("Side", {"BID": 1, "ASK": 2}, "enum_identity.book")

    registered = _value_type(side)
    assert registered.name == "enum_identity.book.Side"
    assert _value_type(side) == registered

    class Nested:
        class Mode(Enum):
            ON = 1
            OFF = 2

    assert _value_type(Nested.Mode).name == (
        f"{__name__}.test_an_enum_is_registered_once_under_its_module_and_qualified_name"
        ".<locals>.Nested.Mode")
