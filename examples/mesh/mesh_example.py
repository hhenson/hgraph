from dataclasses import dataclass

from frozendict import frozendict

from hgraph import (
    graph,
    mesh_,
    TS,
    CompoundScalar,
    reference_service,
    default_path,
    TSD,
    service_impl,
    const,
    register_service,
    debug_print,
    GraphConfiguration,
    evaluate_graph,
    format_,
    mul_,
    map_,
    dispatch,
)


# An overly simplified approach for market data
@reference_service
def market_data(path: str = default_path) -> TSD[str, TS[float]]: ...


@service_impl(interfaces=[market_data])
def market_data_impl() -> TSD[str, TS[float]]:
    return const(frozendict({"MCU_3M": 12345.0, "MCU_Feb24": 12340.0}), TSD[str, TS[float]])


# Below is an incredibly simplified concept of an instrument
# This is not providing a description of how to model instruments or even focusing on correctness
# Just enough here to show how the mesh logic works


@dataclass(frozen=True)
class Instrument(CompoundScalar):
    symbol: str
    asset: str


@dataclass(frozen=True)
class Forward(Instrument):
    tenor: str


@dataclass(frozen=True)
class Carry(Instrument):
    start_tenor: str
    end_tenor: str


# Mapping from symbol to instrument
@reference_service
def instruments(path: str = default_path) -> TSD[str, TS[Instrument]]: ...


@service_impl(interfaces=[instruments])
def instruments_impl() -> TSD[str, TS[Instrument]]:
    return const(
        frozendict({
            "MCU_3M": Forward(symbol="MCU_3M", asset="MCU", tenor="3M"),
            "MCU_Feb24": Forward(symbol="MCU_Feb24", asset="MCU", tenor="Feb24"),
            "MCU_Feb24-3M": Carry(symbol="MCU_Feb24-3M", asset="MCU", start_tenor="Feb24", end_tenor="3M"),
        }),
        TSD[str, TS[Instrument]],
    )


@dispatch
def price_instrument(instrument: TS[Instrument]) -> TS[float]:
    return const(float("Nan"))


@graph(overloads=price_instrument)
def price_forward(instrument: TS[Forward]) -> TS[float]:
    mkt_data = market_data()
    price = mkt_data[instrument.symbol]
    return price


@graph(overloads=price_instrument)
def price_carry(instrument: TS[Carry]) -> TS[float]:
    lhs_inst = format_("{}_{}", instrument.asset, instrument.start_tenor)
    rhs_inst = format_("{}_{}", instrument.asset, instrument.end_tenor)
    return mesh_("prices")[lhs_inst] - mesh_("prices")[rhs_inst]


@graph
def price(inst_id: TS[str]) -> TS[float]:
    instrument = instruments()[inst_id]
    return price_instrument(instrument)


@graph
def mesh_example():
    register_service(default_path, market_data_impl)
    register_service(default_path, instruments_impl)
    positions: TSD[str, TS[float]] = const(
        frozendict({
            "MCU_3M": 10.0,
            "MCU_Feb24-3M": 5.0,
        }),
        TSD[str, TS[float]],
    )
    prices = mesh_(price, __key_arg__="inst_id", __keys__=positions.key_set, __name__="prices")
    position_prices = map_(mul_, prices, positions)
    debug_print("Positions", position_prices)


def main():
    config = GraphConfiguration()
    evaluate_graph(mesh_example, config)


if __name__ == "__main__":
    main()
