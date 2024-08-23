from dataclasses import dataclass

from hg_oap.units.unit_system import UnitConversionContext
from hg_oap.utils import ExprClass
from hgraph import CompoundScalar

__all__ = ("INSTRUMENT_ID", "Instrument")


INSTRUMENT_ID = str  # A useful alias to help make it clear when a string is intended to represent an instrument id.


@dataclass
class InstrumentId(CompoundScalar):
    """
    Use this to represent an instrument with its identification type, this allows for identification of the symbology
    the id is using. (For example FIGI, FactSet, etc.)
    """
    id_: INSTRUMENT_ID
    id_type: str


@dataclass(frozen=True)
class Instrument(CompoundScalar, ExprClass, UnitConversionContext):
    symbol: INSTRUMENT_ID
