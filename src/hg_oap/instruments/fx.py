from dataclasses import dataclass

from hg_oap.assets.currency import Currency
from hg_oap.instruments.instrument import Instrument
from hg_oap.units import Unit
from hg_oap.utils import SELF


@dataclass(frozen=True, kw_only=True)
class FXSpot(Instrument):
    SELF: Currency = SELF

    base: Currency
    quote: Currency

    currency_unit: Unit = SELF.quote.unit
    unit: Unit = SELF.base.unit
