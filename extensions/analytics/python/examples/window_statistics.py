"""Compute several statistics from one trailing tick window."""

import hgraph_analytics as analytics

import hgraph as hg


class WindowMetrics(hg.TimeSeriesSchema):
    mean: hg.TS[float]
    sample_std: hg.TS[float]
    median: hg.TS[float]


@hg.graph
def window_metrics(value: hg.TS[float]) -> hg.TSB[WindowMetrics]:
    """Emit after two observations and retain at most three observations."""

    window = hg.to_window(value, period=3, min_window_period=2)
    return hg.TSB[WindowMetrics].from_ts(
        mean=analytics.rolling_mean(value, period=3, min_window_period=2),
        sample_std=analytics.std(window, ddof=1),
        median=analytics.quantile(window, 0.5),
    )


def run_example():
    return hg.eval_node(window_metrics, [1.0, 2.0, 5.0, 8.0])


if __name__ == "__main__":
    for tick in run_example():
        print(tick)
