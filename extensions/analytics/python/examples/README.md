# Python analytics examples

These examples build from a scalar price pipeline to windowed and shaped-array
analytics:

- [`price_pipeline.py`](price_pipeline.py) composes percentage change,
  clipping, EWMA smoothing, and a moving average.
- [`window_statistics.py`](window_statistics.py) computes mean, sample standard
  deviation, and median from one trailing tick window.
- [`array_statistics.py`](array_statistics.py) demonstrates fixed-shape array
  typing, cumulative sums, quantiles, and correlation.

Each file is directly runnable after installing `hgraph` and
`hgraph-analytics`:

```sh
python extensions/analytics/python/examples/price_pipeline.py
```

The graph functions are the reusable part. Their `run_example()` helpers only
supply deterministic input ticks with `eval_node`, which makes the same files
useful as small experiments and as application starting points.
