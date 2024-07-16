from dataclasses import dataclass
from enum import Enum

from hg_oap.assets.commodities import Commodity
from hg_oap.instruments.instrument import Instrument
from hg_oap.units import Quantity


@dataclass(frozen=True)
class PhysicalCommodity(Instrument):
    asset: Commodity

    unit_conversion_factors: tuple[Quantity[float], ...] = lambda self: self.asset.unit_conversion_factors


class PhysicalCommodities(Enum):
    ...
    # MAL = PhysicalCommodity("MAL", asset=BaseMetals.MAL)
