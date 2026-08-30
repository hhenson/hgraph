"""Publish locally, then perform a standalone direct load."""

from datetime import timedelta

import pyarrow as pa

import hgraph as hg
import hgraph_fabric as fabric


def local_load_example():
    """Return the latest stored prices Frame."""

    config = fabric.make_memory_fabric_config(prefix="examples/load-data")

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
    # dataset's latest version; passing as_of would select the newest version
    # at or before that instant. It does not solve a consistent forest.
    return fabric.load_data(config, "prices/raw")


if __name__ == "__main__":
    print(local_load_example())
