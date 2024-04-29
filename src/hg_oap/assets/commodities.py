from dataclasses import dataclass
from enum import Enum

from hg_oap.assets.asset import PhysicalAsset


@dataclass(frozen=True)
class Commodity(PhysicalAsset):
    """
    Metals, Food, etc.
    """


@dataclass(frozen=True)
class BaseMetal(Commodity):
    """
    Metals such as Copper.
    """


class BaseMetals(Enum):
    ...
    # MAL = BaseMetal("MAL", "Aluminium")
    # MCU = BaseMetal("MCU", "Copper")
    # MNI = BaseMetal("MNI", "Nickel")
    # MPB = BaseMetal("MPB", "Lead")
    # MSN = BaseMetal("MSN", "Tin")
    # MZN = BaseMetal("MZN", "Zinc")


@dataclass(frozen=True)
class PreciousMetal(Commodity):
    """
    Gold, Silver, Platinum, Palladium.
    """


class PreciousMetals(Enum):
    ...
    # XAU = PreciousMetal("XAU", "Gold")
    # XAG = PreciousMetal("XAG", "Silver")
    # XPD = PreciousMetal("XPD", "Palladium")
    # XPT = PreciousMetal("XPT", "Platinum")
