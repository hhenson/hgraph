from dataclasses import dataclass
from enum import Enum

from hg_oap.assets.currency import Currency, Currencies
from hg_oap.instruments.instrument import Instrument


@dataclass(frozen=True)
class Cash(Instrument):
    currency: Currency


class CashInstruments(Enum):
    EUR = Cash("EUR", asset=Currencies.EUR)
    GBP = Cash("GBP", asset=Currencies.GBP)
    GBX = Cash("GBX", asset=Currencies.GBX)
    USD = Cash("USD", asset=Currencies.USD)
    USX = Cash("USX", asset=Currencies.USX)

