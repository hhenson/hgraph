# hgraph-analytics

C++-first numerical analytics for hgraph. The package owns the numerical
analytical family migrated from core—`diff`, `count`, `clip`, `ewma`, and
`pct_change`—plus `quantile`, shaped-array `array_std`, shaped-array
`rolling_window`, `window_values`, `array_get_item`, `cumulative_sum`,
`correlation`, and the EWMA parameter conversion helpers.

```python
import hgraph as hg
import hgraph_analytics as hga

change = hga.pct_change(
    value,
    period=12,
    divide_by_zero=hg.DivideByZero.NAN,
)

bounded = hga.clip(change, -0.25, 0.25)
smoothed = hga.ewma(bounded, alpha=0.2)
median = hga.quantile(observations, 0.5)
volatility = hga.array_std(observations, ddof=1)
recent = hga.rolling_window(value, period=20, min_window_period=5)
window = hga.window_values(hg.to_window(value, 20))
running = hga.cumulative_sum(observations, axis=0)
correlation = hga.correlation(observations, rowvar=False)
```

The result is fractional: `0.05` denotes five percent. `period` counts valid
source observations and must be positive. The operator does not infer
dataframe ordering, elapsed-time sampling, market sessions, or financial price
adjustment.

Native consumers link `hgraph::analytics`, call
`hgraph::analytics::register_analytics_operators()`, and wire
the markers in `hgraph::analytics`, including `diff`, `count`, `clip`, `ewma`,
`pct_change`, `window_values`, `array_get_item`, `cumulative_sum`,
`correlation`, `quantile`, `array_std`, and `rolling_window`.

See the hgraph user-guide migration note for the complete Python and C++ name
mapping from the former core API.
