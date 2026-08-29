"""Choose one Fabric subscription policy for each data id in a root graph."""

from datetime import datetime

import hgraph as hg
import hgraph_fabric as fabric


@hg.graph
def consume_live_prices() -> None:
    """Follow accepted revisions as the production transport announces them."""

    prices = fabric.subscribe_data(
        "prices/enriched", mode=fabric.SubscriptionMode.LIVE
    )
    hg.debug_print("live prices/enriched", prices)


@hg.graph
def consume_replayed_prices() -> None:
    """Replay revisions over the enclosing executor's start/end interval."""

    prices = fabric.subscribe_data(
        "prices/enriched", mode=fabric.SubscriptionMode.REPLAY
    )
    hg.debug_print("replayed prices/enriched", prices)


@hg.graph
def consume_price_snapshot(as_of: datetime) -> None:
    """Load one consistent image at a wiring-time cutoff."""

    prices = fabric.subscribe_data(
        "prices/enriched",
        mode=fabric.SubscriptionMode.SNAPSHOT,
        as_of=as_of,
    )
    hg.debug_print("snapshot prices/enriched", prices)


# The wrappers below make each component independently wireable against the
# deterministic local host. A production host registers its service once and
# calls the corresponding consume_* component directly.


@hg.graph
def local_live_app() -> None:
    fabric.register_memory_fabric_service(prefix="examples/live")
    consume_live_prices()


@hg.graph
def local_replay_app() -> None:
    fabric.register_memory_fabric_service(prefix="examples/replay")
    consume_replayed_prices()


@hg.graph
def local_snapshot_app() -> None:
    fabric.register_memory_fabric_service(prefix="examples/snapshot")
    consume_price_snapshot(datetime(2026, 1, 2, 12, 0))

