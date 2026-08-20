# Changelog

## Unreleased

- Scaffold the RFC 0026 extension and its installed public C++/Python contracts.
- Add canonical revision/reference metadata encoding and the memory notifier.
- Add canonical durable keys and the resumable, acknowledgement-gated
  publication state machine with schema locking and crash/index repair.
- Add the native wiring-time dependency planner, explicit subscription handles,
  independent consistency forests, and hidden publisher lineage cuts.
- Add the shared root Fabric service, Snapshot/Replay/Live subscription
  sessions, service-owned publication and load paths, complete-revision live
  cache ingestion, and the local Python memory-service host.
- Add the optional graph-native Kafka transport: strict Fabric producer/queue
  profiles, full accepted-revision payloads, root push-source ingress through
  the Kafka service, lifecycle-gated durable handoff, explicit cursor commits,
  correlated bounded delivery retry, and transport diagnostics.
- Add local/S3 and Arrow IPC/Parquet behavior coverage, a cross-process
  publication race, resolver/cache and bounded-queue diagnostics, service
  lifecycle logs, installed wheel SDK consumers, release artifacts, and
  production operating guidance.
