from dataclasses import dataclass

from hgraph import CompoundScalar, TimeSeriesSchema, TS

__all__ = ("INSTRUMENT_ID", "Instrument")

from hg_oap.assets.asset import Asset

INSTRUMENT_ID = str  # A useful alias to help make it clear when a string is intended to represent an instrument id.


@dataclass
class InstrumentId(TimeSeriesSchema):
    """
    Use this to represent an instrument with its identification type, this allows for identification of the symbology
    the id is using. (For example FIGI, FactSet, etc.)
    """
    id_: TS[INSTRUMENT_ID]
    id_type: TS[str]


@dataclass(frozen=True)
class Instrument(CompoundScalar):
    symbol: str


