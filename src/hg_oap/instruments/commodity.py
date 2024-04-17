from enum import Enum

from hg_oap.assets.commodities import BaseMetals
from hg_oap.dates.tenor import Tenor
from hg_oap.instruments.future import Future


class ThreeMonthBaseMetals(Enum):
    MAL_3M = Future("MAL_3M", Tenor("3m"), underlyer=BaseMetals.MAL)
    MCU_3M = Future("MCU_3M", Tenor("3m"), asset=BaseMetals.MCU)
    MNI_3M = Future("MNI_3M", Tenor("3m"), asset=BaseMetals.MNI)
    MPB_3M = Future("MPB_3M", Tenor("3m"), asset=BaseMetals.MPB)
    MSN_3M = Future("MSN_3M", Tenor("3m"), asset=BaseMetals.MSN)
    MZN_3M = Future("MZN_3M", Tenor("3m"), asset=BaseMetals.MZN)

