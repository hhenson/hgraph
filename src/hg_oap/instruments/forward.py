from dataclasses import dataclass

from hg_oap.assets.asset import Asset
from hg_oap.dates.tenor import Tenor
from hg_oap.instruments.instrument import Instrument


@dataclass(frozen=True)
class Forward(Instrument):
    """
    Like a Future, but traded as OTC instruments. These can be customised as needed. Forwards operate on assets.
    """
    tenor: Tenor
    underlyer: Asset
