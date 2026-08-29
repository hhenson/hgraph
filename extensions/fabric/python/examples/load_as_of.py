"""Publish locally, then perform a standalone historical point lookup."""

from datetime import datetime, timedelta

import pyarrow as pa

import hgraph as hg
import hgraph_fabric as fabric


def local_load_example():
    """Return the newest stored prices Frame at or before the cutoff."""

    config = fabric.make_memory_fabric_config(prefix="examples/load-as-of")

    @hg.graph
    def publish_prices() -> None:
        fabric.register_fabric_service(config)
        prices = hg.const(
            pa.table({"symbol": ["AAPL", "MSFT"], "price": [201.5, 415.0]}),
            tp=hg.TS[hg.Frame],
        )
        fabric.publish_data("prices/raw", prices)

    with hg.GlobalState():
        hg.run_graph(
            publish_prices,
            start_time=hg.MIN_ST,
            end_time=hg.MIN_ST + timedelta(microseconds=20),
        )

    # Point lookup is deliberately outside the graph. It selects only this
    # dataset's newest version <= the requested instant; it does not solve a
    # dependency-consistent forest.
    return fabric.load_data_as_of(config, "prices/raw", datetime.max)


if __name__ == "__main__":
    print(local_load_example())
