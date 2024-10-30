import polars as pl
from frozendict import frozendict as fd
from hgraph import *
from hgraph.adaptors.data_frame import *
from hgraph.test import eval_node

from hg_oap.assets.currency import Currencies
from hg_oap.pricing.price import Price
from hg_oap.pricing.pricing_services import price_mid_table_impl, price_mid


class PriceDataFrame(DataFrameSource):

    def data_frame(self, start_time: datetime = None, end_time: datetime = None) -> pl.DataFrame:
        import polars as pl
        return pl.DataFrame({
            "date": [MIN_ST] * 2 + [MIN_ST+MIN_TD] * 2,
            "inst_id": ["a", "b", "a", "c"],
            "price": [1.0, 2.0, 3.0, 4.0],
        })

class PriceCurrDataFrame(DataFrameSource):

    def data_frame(self, start_time: datetime = None, end_time: datetime = None) -> pl.DataFrame:
        import polars as pl
        return pl.DataFrame({
            "date": [MIN_ST] * 2 + [MIN_ST+MIN_TD] * 2,
            "inst_id": ["a", "b", "a", "c"],
            "price": [1.0, 2.0, 3.0, 4.0],
            "curr": ["EUR", "USD", "EUR", "USD"]
        })

def test_price_mid():

    @graph
    def g() -> TSB[Price]:
        register_service(default_path, price_mid_table_impl, data_frame_source=PriceDataFrame, static_currency="USD")
        return price_mid("a")

    assert eval_node(g) == [fd(price=1.0, currency=Currencies.USD.value), fd(price=3.0)]


def test_price_mid_with_currency():
    @graph
    def g() -> TSB[Price]:
        register_service(default_path, price_mid_table_impl, data_frame_source=PriceCurrDataFrame, currency_col="curr")
        return price_mid("a")

    assert eval_node(g) == [fd(price=1.0, currency=Currencies.EUR.value), fd(price=3.0)]


