# Changelog

## 0.8.0

- Move `diff`, `count`, `clip`, `ewma`, `center_of_mass_to_alpha`,
  `span_to_alpha`, and `pct_change` from core into `hgraph-analytics`.
- Extend the C++-first `pct_change` graph with observation-count periods and an
  explicit divide-by-zero policy.
