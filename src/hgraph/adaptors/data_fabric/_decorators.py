from typing import Callable, Sequence

from hgraph import subscription_service, TS, TSD, TIME_SERIES_TYPE
#from hgraph.nodes import


def fabric_node(fn=None, depends_on=Sequence[Callable[[TS[str]], TIME_SERIES_TYPE]]):
    ...


def fabric_node_impl(fn=None, node_type: Callable = None):
    ...


def fabric_attach(node: Callable[[TS[str]], TIME_SERIES_TYPE] = None, **inputs) -> Callable[[TS[str]], TIME_SERIES_TYPE]:
    ...


def compute_fabric(path: str, tp_: type[TIME_SERIES_TYPE]) -> TSD[str, TIME_SERIES_TYPE]:
    """
    Use this to register the fabric runner, this will create the master node that will evaluate the fabric nodes
    """
    ...


#### Example:


@subscription_service
def exchange_price(path: str, symbol: str) -> TS[float]:
    ...


@fabric_node
def fair_price(key: TS[str]) -> TS[float]:
    """
    Produces a fair price for a given instrument id.
    """


@fabric_node(depends_on=[fair_price])
def tier_price(key: TS[str]) -> TS[float]:
    """
    Produces a price, based on a fair price
    """


@fabric_node_impl(node_type=fair_price)
def price_fair_price_3m_base_metal(key: TS[str]) -> TS[float]:
    mkt_data = exchange_price('MarketData/LME',  key)
    return mkt_data


@fabric_node_impl(node_type=tier_price)
def price_tier_price(key: TS[str]) -> TS[float]:
    tier, instrument = split_(":", key)
    config = configuration(tier)
    fair_price_ = fabric_attach(fair_price, instrument)
    tier_price_ = ...
    return tier_price_
