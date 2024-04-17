from dataclasses import dataclass
from enum import Enum

from hg_oap.assets.currency import Currency, Currencies
from hg_oap.instruments.instrument import Instrument


@dataclass(frozen=True)
class Cash(Instrument):
    currency: Currency


class CashInstruments(Enum):
    EUR = Cash("EUR", currency=Currencies.EUR)
    GBP = Cash("GBP", currency=Currencies.GBP)
    GBX = Cash("GBX", currency=Currencies.GBX)
    USD = Cash("USD", currency=Currencies.USD)
    USX = Cash("USX", currency=Currencies.USX)

