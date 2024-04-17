from dataclasses import dataclass

from hgraph import CompoundScalar


__all__ = ("INSTRUMENT_ID", "Instrument")


INSTRUMENT_ID = str  # A useful alias to help make it clear when a string is intended to represent an instrument id.


@dataclass
class Instrument(CompoundScalar):
    symbol: str



