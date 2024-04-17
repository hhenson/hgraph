from enum import Enum

from polars.dependencies import dataclasses

from hg_oap.assets.commodities import Commodity, BaseMetals
from hg_oap.instruments.instrument import Instrument


@dataclasses(frozen=True)
class PhysicalCommodity(Instrument):
    asset: Commodity


class PhysicalCommodities(Enum):
    MAL = PhysicalCommodity("MAL", asset=BaseMetals.MAL)
