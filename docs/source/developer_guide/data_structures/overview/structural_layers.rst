Structural Layers
=================

The runtime has five major structural layers:

``GraphExecutor``
    Owns or coordinates a complete graph run. It constructs the runtime graph, installs observers, starts and stops lifecycle processing, and drives the evaluation loop.

``EvaluationEngine``
    Provides the clock, run mode, lifecycle observer dispatch, one-shot evaluation callbacks, stop state, and optional push-message receiver state. Conceptually, this is the engine that evaluates a graph.

``Graph``
    Holds the flattened runtime node set, the graph schedule table, graph identity, boundary bindings, push-source partition, and links to parent or nested graph context.

``Node``
    Holds node identity, runtime operations, lifecycle state, time-series inputs and outputs, optional node-local scheduler, and optional node-local state.

``TimeSeries``
    Holds value validity, modification time, parent/child relationships, subscribers, structural delta state, and a separate TS data component containing the current payload plus delta payload.

.. note::

   **Current implementation.** ``GraphExecutor`` (``runtime/executor.h``)
   currently subsumes the ``EvaluationEngine`` responsibilities: it owns
   ``start_time`` / ``end_time`` / run mode and drives the simulation loop
   directly, and there is no separate ``EvaluationClock`` type yet — nodes
   receive ``evaluation_time`` explicitly. A distinct ``EvaluationEngine`` /
   ``EvaluationClock`` split (with lifecycle observers, callback queues, and a
   push-message receiver) is only **one candidate** design (see *Execution
   Layer*); the current leaning is to keep these
   responsibilities folded into the executor's type-erased ops. Treat the
   separate engine/clock as a recorded alternative, not a committed plan.

The high-level relationship is:

.. mermaid::

   flowchart TD
      Executor[GraphExecutor]
      Engine[EvaluationEngine]
      Clock[EvaluationClock]
      Observers[Lifecycle Observers]
      Callbacks[Before/After Callback Queues]
      Receiver[Push Message Receiver]
      Graph[Graph]
      Schedule[Graph Schedule Table]
      Nodes[Flattened Node Storage]
      Boundaries[Graph Boundaries]
      Node[Node]
      NodeScheduler[Optional Node Scheduler]
      Inputs[Inputs]
      Outputs[Outputs]
      State[Node State]
      TS[TimeSeries Tree]
      Value[Value Storage]

      Executor --> Engine
      Executor --> Graph
      Engine --> Clock
      Engine --> Observers
      Engine --> Callbacks
      Engine --> Receiver
      Graph --> Schedule
      Graph --> Nodes
      Graph --> Boundaries
      Nodes --> Node
      Node --> NodeScheduler
      Node --> Inputs
      Node --> Outputs
      Node --> State
      Inputs --> TS
      Outputs --> TS
      TS --> Value
      Receiver -.push source node index + message.-> Graph
      NodeScheduler -.earliest event.-> Schedule
      Schedule -.scheduled time + node index.-> Nodes

The Execution Layer above ``Graph`` is described in its own page (see
*Execution Layer*). The remaining structural layers — Graph, Node, and
the scheduling primitives — are expanded below. The ``TimeSeries`` layer
contents are described in *Schemas* (kinds, tick semantics) and
*Allocation, Plans and Ops* (memory layout, slot stores).

Graph Layer
~~~~~~~~~~~

The graph layer is the hot path for evaluation. It should be compact and directly indexed.

Core elements:

``GraphIdentity``
    Stable graph id, optional label, and optional parent-node link for nested graphs.

``FlattenedNodeArray``
    The fully ordered node set. The index order is rank order and is the
    evaluation order. Physically, graph storage uses a heterogeneous node
    storage tuple plus a node-location table, so this is an indexed runtime
    view rather than a homogeneous ``NodeValue`` array.

``GraphScheduleTable``
    Parallel array indexed by node index. Each entry is the next visible
    scheduled time for that node, or ``MIN_DT`` when the node has no visible
    schedule.

``PushSourcePartition``
    Index boundary that separates push-source nodes from the normal rank-ordered scan. Push-source nodes occupy the prefix ``[0, push_source_nodes_end)``.

``BoundaryBindings``
    Runtime bindings for graph inputs, outputs, nested graph boundaries, and parent/child graph communication.

``GraphStorage``
    Memory owned by the graph instance. A compiled graph type record carries a
    fixed-size storage plan with a graph header, a heterogeneous node-storage
    tuple, and a parallel ``DateTime`` schedule array sized from the graph
    topology. The graph record's ops context also carries the node-location table that maps
    ``node_index`` to ``(NodeTypeRef, storage_offset)``. Creating a graph
    instance therefore performs one graph storage allocation rather than
    allocating separate vectors for nodes and schedules.

The graph's most important paired structure is:

.. code-block:: text

   FlattenedNodeArray[index] -> Node
   GraphScheduleTable[index] -> scheduled_time

Together these form the scheduled time-node relationship used by evaluation. The schedule table does not own scheduler events; it only exposes the next time each node may need to run.

Node Layer
~~~~~~~~~~

Each node is a runtime object in the flattened graph. The node object should be small enough to scan efficiently and should point to heavier state only when needed.

Core elements:

``NodeHeader``
    Node index, rank position, owning graph pointer, lifecycle state, and schema/runtime metadata needed for dispatch.

``RuntimeOps``
    Behaviour vtable for ``start``, ``eval``, and ``stop``. Error handling is **opt-in**: it is wired in only when the node's schema turns on exception capture. With capture off (the default), an exception thrown by ``eval`` propagates normally; with capture on, the runtime catches it and routes a ``NodeError`` value through the node's ``error_output``. Construction and destruction of the node's runtime memory are not on this vtable; they are handled by the bound plan's ``LifecycleOps``.

``Input``
    Optional. A node has **at most one** time-series input, and that top-level input is **always** a TSB. The bundle's fields are the named arguments — so a node with one argument has a single-field TSB, a node with several arguments has a TSB whose fields are those arguments, and a node with no arguments (a source node) carries no input at all. There is never a non-TSB top-level input.

Named outputs
    A node has zero or more named outputs. The runtime does not represent these as a flat ordered set; each is a distinct, named position addressed by the wiring layer:

    ``output``
        The primary output — present whenever the node produces a normal output value.

    ``error_output``
        Optional. Present only when the node's schema turns on exception capture; in that mode, exceptions thrown by ``eval`` are caught and surfaced here instead of propagating. With capture off, this output is absent and exceptions propagate normally.

    ``recordable_state``
        Optional. Carries replay-recordable state when the node opts into recording.

    A node that has any output has at least ``output``; ``error_output`` and ``recordable_state`` are independent of ``output`` and are added only when the schema declares them.

``NodeState``
    Optional node-local state. This is present only for nodes whose schema or implementation requires state.

``NodeScheduler``
    Optional node-local scheduler. This is present only when the schema declares that the node requires scheduling.

Which of these elements are present is determined by the node's kind:

- **Source nodes** — outputs only, no input.
- **Compute nodes** — both an input and outputs.
- **Sink nodes** — input only; no ``output``, though they may still expose an ``error_output``.

The optional elements are important. A node that does not require a scheduler must not allocate scheduler state. A stateless node must not pay for state storage. A sink must not allocate an unused ``output``.

Scheduling Structures
~~~~~~~~~~~~~~~~~~~~~

Scheduling uses two layers:

Graph schedule table
    A dense ``DateTime`` array indexed by ``node_index`` and stored inside the
    graph instance's fixed storage plan. Each slot stores the node's single
    visible scheduled time, or ``MIN_DT`` when the node has no visible graph
    schedule. Normal evaluation starts after ``MIN_DT``, so this floor value
    cannot become due in an evaluation cycle.

Cached next scheduled time
    A graph-level ``DateTime`` cache maintained alongside the schedule table.
    Graph start seeds it from the schedule table, allowing ``schedule(now())``
    during start to drive the first cycle. Normal scheduling updates it
    incrementally; evaluation resets it for the cycle and folds in only entries
    strictly after the current evaluation time. ``GraphView::next_scheduled_time()``
    returns ``MAX_DT`` when no pending time is cached. Root executors read this
    cache directly, and nested graphs use it to schedule the parent node before
    the nested evaluation block returns.

``NodeScheduledEvent``
    A node-local event containing ``scheduled_time`` and optional ``tag``. Real-time scheduling may also be backed by a wall-clock alarm handle.

``NodeScheduler``
    A per-node structure that stores multiple ``NodeScheduledEvent`` values, supports tag replacement and cancellation, and exposes the earliest pending event to the graph schedule table.

The intended relationship is:

.. mermaid::

   flowchart LR
      Event1[NodeScheduledEvent time=t1 tag=a]
      Event2[NodeScheduledEvent time=t2 tag=b]
      Event3[NodeScheduledEvent time=t3]
      NodeScheduler[NodeScheduler]
      GraphEntry[GraphScheduleTable node_index = min time]
      GraphNext[Graph cached next time]
      Clock[Executor next evaluation time]

      Event1 --> NodeScheduler
      Event2 --> NodeScheduler
      Event3 --> NodeScheduler
      NodeScheduler --> GraphEntry
      GraphEntry --> GraphNext
      GraphNext --> Clock

The node scheduler owns the complete set of pending events. The graph schedule
table owns only the next visible time for the node; the graph-level cache owns
the next real evaluation time exposed to the executor or parent graph.
