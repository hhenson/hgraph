"""Run one unchanged Fabric graph live or as deterministic replay."""

import hgraph as hg
import hgraph_fabric as fabric


ENRICHED_PRICES_DATA_ID = "prices/enriched"


@hg.graph
def consume_prices() -> None:
    """Subscribe without embedding the run policy in application logic."""

    prices = fabric.subscribe_data(ENRICHED_PRICES_DATA_ID)
    hg.debug_print("prices/enriched", prices)


# The wrappers below keep the graph identical. Run ``local_live_app`` with
# ``EvaluationMode.REAL_TIME``; simulation replays the configured time range.


@hg.graph
def local_live_app() -> None:
    fabric.register_memory_fabric_service(prefix="examples/live")
    consume_prices()


@hg.graph
def local_replay_app() -> None:
    fabric.register_memory_fabric_service(prefix="examples/replay")
    consume_prices()
