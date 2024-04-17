from dataclasses import dataclass

from hg_oap.assets.currency import Currency
from hg_oap.instruments.instrument import Instrument


@dataclass(frozen=True)
class Cash(Instrument):
    currency: Currency


