from dataclasses import dataclass
from typing import TypeVar, Generic, cast

from hgraph import TimeSeriesSchema, TS, compute_node, TSB, graph, REF, TIME_SERIES_TYPE, PythonTimeSeriesReference, \
    switch_

from hg_oap.quanity.unit import Unit

UNIT = TypeVar("UNIT", bound=Unit)
UNIT_1 = TypeVar("UNIT_1", bound=Unit)


@dataclass
class Quantity(TimeSeriesSchema, Generic[UNIT]):
    """
    Quantities belong to a unit family, that is the magnitude (or value) is a representation of a particular dimension,
    for example length. A unit conversion is only possible within a unit family. Thus, length cannot be converted
    to temperature for example.
    """
    value: TS[float]
    unit: TS[UNIT]


@graph
def value_as(qty: TSB[Quantity[UNIT]], unit: TS[UNIT_1]) -> TS[float]:
    """Returns the quantity in the unit requested"""
    return switch_(
        {
            True: lambda q, u: q.value,
            False: lambda q, u: _value_as(q, u),
        }, qty.unit == unit, qty, unit
    )


@compute_node(valid=("qty", "unit"))
def _value_as(qty: TSB[Quantity[UNIT]], unit: TS[UNIT_1]) -> TS[float]:
    """
    Helper function to calculate the value of a quantity in a different unit
    """
    return cast(Unit, qty.unit.value).convert_to(qty.value.value, unit.value)


@graph
def convert_to(qty: TSB[Quantity[UNIT]], unit: TS[UNIT_1]) -> TSB[Quantity[UNIT_1]]:
    """
    Returns a quantity instance using the unit provided (converting the value to the new units)
    """
    return TSB[Quantity].from_ts(
        value=switch_(
            {
                True: lambda q, u: q.value,
                False: lambda q, u: _value_as(q, u),
            }, qty.unit == unit, qty, unit
        ),
        unit=unit
    )

