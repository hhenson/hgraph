# Changelog

## Unreleased

- Replay recovered records at their Kafka timestamps across all subscriptions
  instead of serializing them in shared-drain arrival order.
- Keep real-time live records behind their subscription's queued timestamped
  recovery tail during the recovery-to-live handoff.
- Stage record-time recovery cohorts from independent subscriptions before
  releasing their globally ordered records into the graph.
- Keep cohort loading independent of live-ingress watermarks, release failed
  participants, and retain bounded lifecycle capacity through preload.

## 0.8.0

- Introduce the C++-first Kafka service implementation, native and Python APIs,
  with the core-owned `hgraph.adaptors.kafka` compatibility shim delegating to
  the extension when it is installed.
