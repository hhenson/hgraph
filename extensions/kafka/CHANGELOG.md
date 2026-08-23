# Changelog

## Unreleased

- Rebuild manual/independent assignments as a new recovery generation after a
  broker disconnect, allowing live consumers to resume from committed offsets
  and exposing `Retrying` -> `Recovering` -> `Live` lifecycle transitions.
- Route all real-time results through one ordered standard-FIFO push source,
  with subscription, delivery, event, stop-policy, and delivery-commit logic
  implemented by graph nodes and sinks rather than a private bridge queue.
- Preserve bounded deterministic Kafka recovery in simulation through an
  ordinary scheduled service graph with no push source or consumer worker.
- Replay recovered records at their Kafka timestamps across all subscriptions
  instead of serializing them in shared-drain arrival order.
- Keep real-time live records behind their subscription's queued timestamped
  recovery tail during the recovery-to-live handoff.
- Stage record-time recovery cohorts from independent subscriptions before
  releasing their globally ordered records into the graph.
- Coordinate record-time recovery with a graph-side barrier so independent
  consumer sessions are sorted before their records are released.
- Remove byte-capacity configuration and accounting; Kafka recovery and
  outbound staging limits are expressed only as record counts.

## 0.8.0

- Introduce the C++-first Kafka service implementation, native and Python APIs,
  with the core-owned `hgraph.adaptors.kafka` compatibility shim delegating to
  the extension when it is installed.
