# hgraph-analytics

C++-first numerical analytics for hgraph. The initial operator implements RFC
0018's causal, observation-count `pct_change` contract.

```python
import hgraph as hg
import hgraph_analytics as hga

change = hga.pct_change(
    value,
    period=12,
    divide_by_zero=hg.DivideByZero.NAN,
)
```

The result is fractional: `0.05` denotes five percent. `period` counts valid
source observations and must be positive. The operator does not infer
dataframe ordering, elapsed-time sampling, market sessions, or financial price
adjustment.

Native consumers link `hgraph::analytics`, call
`hgraph::analytics::register_analytics_operators()`, and wire
`hgraph::analytics::pct_change`.
