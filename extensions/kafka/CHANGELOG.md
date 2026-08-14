# Changelog

## Unreleased

- Replay recovered records at their Kafka timestamps across all subscriptions
  instead of serializing them in shared-drain arrival order.

## 0.8.0

- Introduce the C++-first Kafka service implementation, native and Python APIs,
  with the core-owned `hgraph.adaptors.kafka` compatibility shim delegating to
  the extension when it is installed.
