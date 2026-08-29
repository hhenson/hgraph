"""Publish one complete Frame through the run-scoped memory Fabric."""

from datetime import timedelta

import pyarrow as pa

import hgraph as hg
import hgraph_fabric as fabric


@hg.graph
def publish_prices() -> None:
    """Reusable producer component; the outer host owns Fabric registration."""

    prices = hg.const(
        pa.table({"symbol": ["AAPL", "MSFT"], "price": [201.5, 415.0]}),
        tp=hg.TS[hg.Frame],
    )
    hg.debug_print("publishing prices/raw", prices)
    fabric.publish_data("prices/raw", prices)


@hg.graph
def local_app() -> None:
    """Deterministic local host for the producer component."""

    fabric.register_memory_fabric_service(prefix="examples/basic")
    publish_prices()


if __name__ == "__main__":
    hg.run_graph(
        local_app,
        run_mode=hg.EvaluationMode.SIMULATION,
        start_time=hg.MIN_ST,
        end_time=hg.MIN_ST + timedelta(microseconds=20),
    )

