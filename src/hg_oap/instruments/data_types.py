from dataclasses import dataclass

from hgraph import CompoundScalar


@dataclass
class Instrument(CompoundScalar):
    symbol: str
