---
name: hgraph-realtime-adaptors
description: Implement or review C++-first hgraph real-time adaptors and service implementations that bridge external threads, callbacks, or event loops through push sources and sink nodes. Use for sender admission and backpressure, start/stop task ownership, service_impl resource sharing, real-time versus simulation wiring, protocol acknowledgements, or native/Python adaptor lifecycle tests.
---

# HGraph Real-Time Adaptors

Build asynchronous boundaries that preserve the graph's real-time execution,
ownership, and lifecycle rules.

## Establish the boundary

1. Read the current repository's `AGENTS.md`, the relevant adaptor or service
   interfaces and implementations, and their tests before editing.
2. In the hgraph core checkout, also read
   `docs/source/developer_guide/real_time_adaptors.rst`,
   `docs/source/developer_guide/services.rst`, and the push-source section of
   `docs/source/user_guide/cpp/authoring_nodes.rst`.
3. In a downstream project, use its extension layout and the public APIs and
   documentation of its pinned hgraph version. Do not assume core sources are
   available or promote domain-specific policy into core without the required
   RFC and evidence.
4. Classify the implementation before choosing nodes:
   - A simple source-only adaptor uses one push source with lifecycle state.
   - A bidirectional or multi-interface adaptor uses a `service_impl` composed
     from push sources, sink nodes, and one graph-scoped task/resource.
   - Simulation uses a distinct scheduled or pull-source implementation
     selected at wiring time; it never uses a simulated push source.

Keep implementation logic in node `eval` and graph `compose` methods. Extract
a helper only for a well-defined operation or genuinely shared logic.

## Enter the graph only through the sender

External threads, callbacks, and event loops may retain `PushSourceSender`.
They must not retain or access the graph or executor, schedule nodes, or invoke
evaluation directly.

Treat the sender as opaque. Its selected policy owns queue or conflating
storage, locks, condition variables, receiver lifetime, and executor wake-up.
The sender stores work first and then marks push work pending and notifies the
real-time executor. Do not reproduce or expose those internals in an adaptor.

Choose admission deliberately:

- `try_send(value) -> bool` never waits. Use it only with an explicit plan for
  `false`, such as pausing upstream reads while continuing protocol
  heartbeats. Never ignore refusal when it would lose data.
- `send_blocking(value) -> bool` waits for bounded capacity and returns
  `false` only if the receiver stops before admission. It may be discarded
  when the adaptor's stop path already ends the producer.
- Python maps its sender call to blocking admission and must release the GIL
  while waiting.

Admission is not processing confirmation. Dequeue only means the graph has
started processing the buffered value.

## Own simple adaptor lifecycle in the push source

For a source-only adaptor:

1. Store the external task, stop mechanism, and thread handles in node
   `State`.
2. In `start`, receive the framework-created sender, construct the task with
   it, and start the task's threads.
3. In `stop`, request shutdown, wake task-owned waits, and join every thread
   started by `start`.
4. Make stop safe after partial start. Use the existing guards from
   `hgraph/util/scope.h` for rollback and cleanup.

Do not capture call-scoped Python time-series, `STATE`, scheduler, node, or
global-state wrappers in an external thread. Copy task-owned configuration and
retain only the sender and the task's own data.

## Compose bidirectional adaptors as services

Use `service_impl` as the graph-scoped singleton for a shared external
resource. Compose it from:

- one or more push sources receiving asynchronous results;
- sink nodes sending graph requests or commands to the task;
- service/adaptor boundary nodes exposing the public interfaces; and
- one central task/resource instance shared by those nodes.

Use one push source per independently ordered result stream. When related
result types require one order, carry a discriminated envelope through one
sender and demultiplex it in the graph.

Prefer graph-scoped `GlobalState` resource ownership when several nodes need
the task. If one push source is the natural owner, it may lazily construct the
task and expose passive access to sinks, but that passive connection is a graph
edge and can delay inputs. Make one node responsible for starting the shared
task and one corresponding stop path responsible for stopping and joining it.
Never substitute a process global for graph-scoped ownership.

Use the compute/sink-node skill for the concrete sink lifecycle and hot path,
the graph skill for composition, and the operator skill when type or policy
selection should be resolved through overloads.

## Provide simulation by wiring a different implementation

Push sources are real-time-root-only. There is no simulation-capable push
source and no external thread may wake a simulation executor.

When simulation is supported, inspect the wiring context's run mode and select
an ordinary scheduled or pull-source implementation. Historical Kafka replay,
for example, reads the stored log from a pull source instead of running a live
consumer task. Keep live and replay implementations behind the same public
service interfaces.

## Confirm protocols downstream

Never acknowledge or commit because enqueue succeeded. When a protocol needs
confirmation after graph processing:

1. carry the correlation key, cursor, or offset with the input;
2. emit it from the point that defines successful processing; and
3. wire a sink node that performs the acknowledgement or commit.

Kafka transactional processing must therefore commit from a confirmation that
flows through the graph, not from the callback that buffered the record.

## Validate lifecycle, refusal, and ordering

Test through public wiring interfaces with equivalent C++ coverage for every
Python-visible behavior. Cover:

- exactly one task construction, orderly stop request, and thread join;
- stop while a producer is blocked, with `false` returned and no deadlock;
- retained/default/moved-from senders remaining inert after teardown;
- the explicit producer response to `try_send` refusal;
- concurrent producers and the selected FIFO, burst, or conflating semantics;
- executor wake only after accepted work is stored;
- acknowledgement only after the explicit confirmation sink; and
- simulation selecting a scheduled or pull source with no producer thread.

Run the repository acceptance gates from `AGENTS.md`; for cross-thread
lifetime changes, also use the required Linux validation and ASan/TSan where
applicable.
