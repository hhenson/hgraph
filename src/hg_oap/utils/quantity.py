from dataclasses import dataclass

from hg_oap.utils.unit import Unit


@dataclass(frozen=True)
class Quantity:
    value: float
    unit: Unit

