from hgraph import TimeSeriesSchema, TS

from hg_oap.assets.currency import Currency


class Price(TimeSeriesSchema):
    """
    A bundle schema representing the price as a float and the associated currency asset that the price is representing.
    """
    price: TS[float]
    currency: TS[Currency]

