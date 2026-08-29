"""Join two Fabric datasets and publish a lineage-aware derived Frame."""

from dataclasses import dataclass

import hgraph as hg
import hgraph_fabric as fabric


@dataclass(frozen=True)
class Price(hg.CompoundScalar):
    symbol: str
    price: float


@dataclass(frozen=True)
class Instrument(hg.CompoundScalar):
    symbol: str
    currency: str


@dataclass(frozen=True)
class EnrichedPrice(hg.CompoundScalar):
    symbol: str
    price: float
    currency: str


@hg.graph
def enrich_prices(
    prices: hg.TS[hg.Frame[Price]],
    instruments: hg.TS[hg.Frame[Instrument]],
) -> hg.TS[hg.Frame]:
    """Use typed views internally and return Fabric's complete Frame shape."""

    enriched: hg.TS[hg.Frame[EnrichedPrice]] = hg.join(
        prices, instruments, on="symbol", how="left"
    )
    return hg.convert[hg.TS[hg.Frame]](enriched)


@hg.graph
def build_enriched_prices() -> None:
    """Publish with the preferred automatic upstream-lineage discovery."""

    raw_prices = fabric.subscribe_data(
        "prices/raw", mode=fabric.SubscriptionMode.LIVE
    )
    instrument_reference = fabric.subscribe_data(
        "instruments/reference", mode=fabric.SubscriptionMode.LIVE
    )

    enriched = enrich_prices(
        hg.convert[hg.TS[hg.Frame[Price]]](raw_prices),
        hg.convert[hg.TS[hg.Frame[Instrument]]](instrument_reference),
    )
    fabric.publish_data("prices/enriched", enriched)


@hg.graph
def build_enriched_prices_with_explicit_lineage() -> None:
    """Name dependencies explicitly when value ancestry cannot represent them."""

    raw_prices = fabric.subscribe_data(
        "prices/raw", mode=fabric.SubscriptionMode.LIVE
    )
    instrument_reference = fabric.subscribe_data(
        "instruments/reference", mode=fabric.SubscriptionMode.LIVE
    )

    enriched = enrich_prices(
        hg.convert[hg.TS[hg.Frame[Price]]](raw_prices),
        hg.convert[hg.TS[hg.Frame[Instrument]]](instrument_reference),
    )
    fabric.publish_data(
        "prices/enriched-explicit",
        enriched,
        dependencies=fabric.DependencySelection.explicit(
            fabric.dependency_handle(raw_prices),
            fabric.dependency_handle(instrument_reference),
        ),
    )


@hg.graph
def local_automatic_app() -> None:
    fabric.register_memory_fabric_service(prefix="examples/derived-auto")
    build_enriched_prices()


@hg.graph
def local_explicit_app() -> None:
    fabric.register_memory_fabric_service(prefix="examples/derived-explicit")
    build_enriched_prices_with_explicit_lineage()

