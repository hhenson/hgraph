from dataclasses import dataclass

from hg_oap.dates.tenor import Tenor
from hg_oap.instruments.instrument import Instrument


@dataclass(frozen=True)
class Future(Instrument):
    """
    A standardized legal agreement to buy or sell the underlyer at a predetermined price at a specific time in the
    future.
    """
    tenor: Tenor
    underlyer: Instrument
