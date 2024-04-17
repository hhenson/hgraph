from abc import abstractmethod
from dataclasses import dataclass

from hgraph import CompoundScalar, TimeSeriesSchema, TS

__all__ = ("INSTRUMENT_ID", "Instrument")

from hg_oap.assets.asset import Asset
from hg_oap.dates.tenor import Tenor

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


@dataclass(frozen=True)
class Future(Instrument):
    """
    A standardized legal agreement to buy or sell the underlyer at a predetermined price at a specific time in the
    future.
    """
    tenor: Tenor
    asset: Asset | None = None
    instrument: Instrument | None = None

    @property
    def underlyer(self) -> Asset | Instrument:
        """The underlyer that is to be bought or sold."""
        return self.asset if self.asset is not None else self.instrument


@dataclass(frozen=True)
class Forward(Instrument):
    """
    Like a Future, but traded as OTC instruments. These can be customised as needed. Forwards operate on assets.
    """
    tenor: Tenor
    asset: Asset

    @property
    def underlyer(self) -> Asset | Instrument:
        """The underlyer that is to be bought or sold."""
        return self.asset

