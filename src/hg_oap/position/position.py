from hgraph import TimeSeriesSchema, TS, TSB

from hg_oap.instruments.instrument import Instrument
from hg_oap.pricing.price import Price


class InstrumentQuantity(TimeSeriesSchema):
    """The quantity of an instrument held"""
    instrument: TS[Instrument]
    qty: TS[float]


class Position(InstrumentQuantity):
    """A position represent a holding in a particular instrument along with a notional"""
    notional: TSB[Price]

