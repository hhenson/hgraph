from hgraph import subscription_service, TS, TSB, service_impl, TSS, TSD, SCALAR, TS_SCHEMA, drop_dups, cast_, SCALAR_1, \
    default_path, REF, const, map_, graph, ts_schema
from hgraph.adaptors.data_frame import DATA_FRAME_SOURCE, tsd_k_b_from_data_source, tsd_k_v_from_data_source

from hg_oap.assets.currency import Currency, Currencies
from hg_oap.pricing.price import Price


@subscription_service
def price_mid(instrument_id: TS[str], path: str = default_path) -> TSB[Price]:
    """
    Simple pricing where the price is a simple value and currency, this is true of mid-prices, fixings, and other
    referenced prices that are often used for systematic trading strategies.
    """


@service_impl(interfaces=[price_mid])
def price_mid_table_impl(
        instrument_id: TSS[str],
        data_frame_source: type[DATA_FRAME_SOURCE],
        date_col: str = 'date',
        instrument_id_col: str = 'inst_id',
        price_col: str = 'price',
        currency_col: str = 'currency',
        static_currency: str = None
) -> TSD[str, TSB[Price]]:
    """
    Produces a stream of pricing for the instruments subscribed to from the data frame produced by the DataFrameSource
    provided. To keep this simple, the input ids are ignored, we just produce a complete TSD of all potential
    results. The mapping logic should select the values of interest as the subscriptions come and go.

    The schema must have:

    date[_col]
        This can be date or datetime. Is used to time release of pricing information

    instrument_id[_col]
        This is the id (as a string) to associate to the price stream and is matched to subscription requests.

    price[_col]
        The price (as a float) for the date and instrument.

    currency[_col]
        The currency (as a str) for the instrument and price. This is expected to remain constant for the duration of the
        stream. As such, this will be ticked once at the beginning of the stream and not after.

    static_currency
        If a static currency is provided, assume the data frame is made up of only those currency prices and no currency
        column exists, this will then extract a TSD[str, TS[float]] from the data frame. and then suppliment
        with the static currency.
    """
    if static_currency:
        values = tsd_k_v_from_data_source[SCALAR: str, SCALAR_1: float](data_frame_source, date_col, instrument_id_col)
        clean_up = map_(
            _clean_up_static_currency,
            price=values,
            currency=const(getattr(Currencies, static_currency).value, TS[Currency])
        )
        return clean_up
    else:
        values = tsd_k_b_from_data_source[SCALAR: str](data_frame_source, date_col, instrument_id_col)
        clean_up = map_(
            _clean_up_dynamic_currency,
            price=values,
            price_col=price_col,
            currency_col=currency_col,
        )
        return clean_up


@graph
def _clean_up_static_currency(price: REF[TS[float]], currency: REF[TS[Currency]]) -> TSB[Price]:
    return TSB[Price].from_ts(price=price, currency=currency)


@graph
def _clean_up_dynamic_currency(price: TSB[TS_SCHEMA], price_col: str, currency_col: str) -> TSB[Price]:
    return TSB[Price].from_ts(
        price=getattr(price, price_col),
        currency=cast_(Currency, drop_dups(getattr(price, currency_col)))
    )