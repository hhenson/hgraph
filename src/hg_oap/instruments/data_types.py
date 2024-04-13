from dataclasses import dataclass

from hgraph import CompoundScalar


__all__ = ("INSTRUMENT_ID", "Instrument")

from hg_oap.quanity.quantity import Quantity
from hg_oap.quanity.unit import Unit

INSTRUMENT_ID = str  # A useful alias to help make it clear when a string is intended to represent an instrument id.


@dataclass
class Instrument(CompoundScalar):
    symbol: str


class InstrumentUnit(Unit):
    ...


InstrumentQuantity = Quantity[InstrumentUnit]
