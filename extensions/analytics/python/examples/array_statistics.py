"""Apply analytics to a fixed-shape numeric array time series."""

import hgraph_analytics as analytics
import numpy as np

import hgraph as hg

Vector4 = hg.Array[float, hg.Size[4]]


class ArrayMetrics(hg.TimeSeriesSchema):
    cumulative: hg.TS[Vector4]
    median: hg.TS[float]
    correlation: hg.TS[float]


@hg.graph
def array_metrics(values: hg.TS[Vector4]) -> hg.TSB[ArrayMetrics]:
    """Produce array and scalar reductions from every valid array tick."""

    return hg.TSB[ArrayMetrics].from_ts(
        cumulative=analytics.cumulative_sum(values),
        median=analytics.quantile(values, 0.5),
        correlation=analytics.correlation(values),
    )


def run_example():
    return hg.eval_node(array_metrics, [np.array([1.0, 2.0, 3.0, 4.0])])


if __name__ == "__main__":
    print(run_example()[0])
