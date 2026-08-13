# Changelog

## 0.8.0

- Move `diff`, `count`, `clip`, `ewma`, `center_of_mass_to_alpha`,
  `span_to_alpha`, and `pct_change` from core into `hgraph-analytics`.
- Extend the C++-first `pct_change` graph with observation-count periods and an
  explicit divide-by-zero policy.
- Move `np_quantile`, `np_std`, and `np_rolling_window` from core as
  `quantile`, `array_std`, and `rolling_window`; retire the NumPy-prefixed
  compatibility names.
- Move the remaining `hgraph.numpy_` operators into `hgraph-analytics` as
  `window_values`, `array_get_item`, `cumulative_sum`, and `correlation`, and
  retire the obsolete module.
- Move generic `std`, `var`, and `resample` from core without changing their
  shape-dependent contracts, and rename `rolling_average` to `rolling_mean`.
