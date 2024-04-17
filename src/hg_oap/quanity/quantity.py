from dataclasses import dataclass
from typing import TypeVar, Generic, cast

from hgraph import TimeSeriesSchema, TS, compute_node, TSB, graph, switch_, AUTO_RESOLVE

from hg_oap.units.unit import Unit

UNIT = TypeVar("UNIT", bound=Unit)
UNIT_1 = TypeVar("UNIT_1", bound=Unit)


@dataclass
class Quantity(TimeSeriesSchema, Generic[UNIT]):
    """
    Quantities belong to a unit family, that is the magnitude (or value) is a representation of a particular dimension,
    for example length. A unit conversion is only possible within a unit family. Thus, length cannot be converted
    to temperature for example.
    """
    qty: TS[float]
    unit: TS[UNIT]


@graph
def value_as(qty: TSB[Quantity[UNIT]], unit: TS[UNIT_1], _u_tp: type[UNIT], _u_1_tp: type[UNIT_1]) -> TS[float]:
    """Returns the quantity in the unit requested"""
    if _u_tp == _u_1_tp:
        return qty.qty.value
    else:
        return _value_as(qty, unit)


@compute_node(valid=("qty", "unit"))
def _value_as(qty: TSB[Quantity[UNIT]], unit: TS[UNIT_1]) -> TS[float]:
    """
    Helper function to calculate the value of a quantity in a different unit
    """
    return cast(Unit, qty.unit.value).convert_to(qty.qty.value, unit.value)


@graph
def convert_to(qty: TSB[Quantity[UNIT]], unit: TS[UNIT_1], _u_tp: type[UNIT] = AUTO_RESOLVE,
               _u_1_tp: type[UNIT_1] = AUTO_RESOLVE) -> TSB[Quantity[UNIT_1]]:
    """
    Returns a quantity instance using the unit provided (converting the value to the new units)
    """
    qty = qty.qty if _u_tp == _u_1_tp else _value_as(qty, unit)
    return TSB[Quantity[_u_1_tp]].from_ts(
        qty=qty,
        unit=unit
    )
