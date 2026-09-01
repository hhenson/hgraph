"""Compose a small streaming price-analysis pipeline."""

import hgraph_analytics as analytics

import hgraph as hg


class PriceMetrics(hg.TimeSeriesSchema):
    change: hg.TS[float]
    smoothed_change: hg.TS[float]
    moving_average: hg.TS[float]


@hg.graph
def price_metrics(price: hg.TS[float]) -> hg.TSB[PriceMetrics]:
    """Return fractional returns and two differently smoothed views."""

    change = analytics.pct_change(price)
    bounded = analytics.clip(change, -0.25, 0.25)
    return hg.TSB[PriceMetrics].from_ts(
        change=change,
        smoothed_change=analytics.ewma(bounded, alpha=0.5),
        moving_average=analytics.rolling_mean(price, period=3, min_window_period=2),
    )


def run_example():
    """Evaluate four price ticks and return the emitted metric bundles."""

    return hg.eval_node(price_metrics, [100.0, 102.0, 101.0, 105.0])


if __name__ == "__main__":
    for tick in run_example():
        print(tick)
