Completed-day component recovery
================================

A deterministic strategy can finish one bounded simulation run, persist a
complete component image, and continue in a new process from that image. Only
new input events are evaluated after recovery. This capability is opt-in and
separate from the existing component ``RECOVER`` mode, which seeds boundary
values and does not restore a running strategy's internal state.

The runtime mechanics are native C++. Durable storage and the Python store
adapter belong to ``hgraph-persistence``. Install matching core and extension
builds; extensions must be rebuilt when the runtime operations ABI changes.

Define the recovery boundary
----------------------------

Use a component with explicit input ports. Keep external data sources, order
submission, and other external effects outside the component. Inside it, use
pure compute nodes and ``RecordableState`` / ``RECORDABLE_STATE`` for values
that affect subsequent events. Configuration is a declaration that these
nodes are deterministic; the runtime cannot inspect arbitrary user code for
undeclared host observations or side effects.

Each external input must come directly from an owned pull-source endpoint,
dedicated to that component input. Its current endpoint image is included in
the boundary and restored before the source starts. This baseline matters for
collections: a future removal must reach a source that already knows the key
exists. The source retains responsibility for its cursor and scheduling;
supply only events belonging to the new interval. Source cursors are not
checkpointed. Computed or projected external inputs and sources shared with
other consumers are refused in this first version. Move preprocessing inside
the component, or supply a dedicated input source.

Keep the boundary closed over values: component inputs and outputs must have
fully dereferenced time-series schemas, including their collection children.
Internal ``REF`` endpoints may select or retain other endpoints inside this
boundary. Consume a selected reference through an ordinary value input before
returning the component result. References to endpoints outside the saved
component, or references escaping through its result, are refused.

.. code-block:: python

   import hgraph as hg
   import hgraph_persistence as persistence

   class StrategyState(hg.TimeSeriesSchema):
       total: hg.TS[int]

   @hg.compute_node
   def accumulate(value: hg.TS[int],
                  state: hg.RECORDABLE_STATE[StrategyState] = None) -> hg.TS[int]:
       total = (state.total.value if state.total.valid else 0) + value.value
       state.total.value = total
       return total

   @hg.component(recordable_id="strategy")
   def strategy(value: hg.TS[int]) -> hg.TS[int]:
       return accumulate(value, __recordable_id__="total")

State is already an output-backed endpoint. No separate observer is required
to copy its updates into a second internal object. Checkpoints automatically
include it even when a state update produces no ordinary output.

A start hook sees restored state. Initialize a state field only when it is
invalid; overwriting a restored endpoint in ``start`` is rejected. Python
compute callbacks with captured closure state are refused because those values
are not part of the declared checkpoint contract. Supply immutable scalar
parameters explicitly and semantic mutable state through ``RECORDABLE_STATE``.
A node may also hold ``State<T>`` / Python ``STATE`` as a reconstructible cache
beside that durable state. Recovery creates a fresh cache; rebuild it from the
restored state in ``start``. The cache is never serialized. Typed Python cache
classes participate in checkpoint identity by module and qualified name; change
the application revision when their behavior changes. Local-state-only compute
nodes still need an explicit native checkpoint contract.

Configure each completed day
----------------------------

Create a durable store, select an explicit predecessor, and name the new image
before wiring the application:

.. code-block:: python

   store = persistence.ComponentCheckpointStore("./strategy-checkpoints")
   with hg.GlobalState() as state:
       persistence.configure_component_recovery(
           store, "strategy", "day-2", restore_key="day-1",
           revision="strategy-v1", global_state=state)
       result = hg.run_graph(application, start_time=day_start,
                             end_time=day_end)

``application`` supplies fresh input data to ``strategy``. ``day_start`` and
``day_end`` are explicit datetimes, and the end is exclusive. The first run
omits ``restore_key``. A recovery starts after the saved cut and at or after
the previous completed interval. A missing predecessor is an error, rather
than a silently fresh strategy.

The application ``revision`` identifies the strategy code. Change it when the
meaning of computation changes. Runtime validation additionally checks the
component's node topology, input policies, scalar configuration, endpoint
schemas, complete returned binding, and compiled child graph contracts. Changing
which producer or bundle/list field is returned is an incompatible wiring change,
even if the internal nodes are otherwise identical. It does not hash arbitrary
function bodies. The current configuration selects one component per run;
configuring another replaces that selection.

Both identifiers remain optional in Python: ``@component`` defaults its
``recordable_id`` to the function name, and nodes receive automatic structural
IDs when ``__recordable_id__`` is omitted. Explicit node IDs must be unique
within a component. A topology change still requires compatible graph identity.
Nested components and mapped children retain their separate identities.

The native equivalent is:

.. code-block:: cpp

   #include <hgraph/persistence/component_checkpoint_store.h>

   hgraph::GlobalContext context;
   hgraph::persistence::store::FrameStoreConfig storage;
   storage.location = hgraph::persistence::store::LocalLocation{"./checkpoints"};
   hgraph::persistence::ComponentCheckpointStore store{storage};
   hgraph::persistence::configure_component_recovery(
       context.state().view(), store, "strategy", "day-2", "day-1", "strategy-v1");
   // Wire stdlib::component<Strategy>(...) and execute with finite bounds.

Core-only callers can configure ``hgraph::ComponentRecoveryConfig`` with
owned-image load and commit callbacks, for example an in-memory test store.
Use ``testing::eval_node_with_options`` with ``EvalNodeRunOptions`` to test
successive finite intervals through native public graph wiring.

The executable ``extensions/persistence/python/examples/completed_days.py``
runs one calendar day per process. With a new directory, run day 1 and then
day 2. The first produces totals 1 and 3; the second restores 3 and produces
6 and 10 without evaluating day 1 again.

For Python tests of resumed collection removals, supply deltas through a
generator/runtime source. The list-input form of ``eval_node`` validates
collection deltas against a fresh temporary collection before graph recovery,
so a removal of a key that exists only in the checkpoint can fail during test
setup. The mapped recovery regression demonstrates the generator form.

What is preserved
-----------------

The image contains direct input source baselines and boundary alias clocks,
ordinary outputs, hidden recordable state and error outputs,
validity, and original modification times.

Values are stored with the binary value codec, so any value that codec can
carry is ordinary component state. That includes ``Frame`` and ``Series`` --
untyped, ``Frame[Row]`` and ``Frame[Row, Metadata]`` alike -- held in an output,
in recordable state, inside a bundle or as an input baseline. A frame recovers
with its table contents, Arrow schema and schema metadata. Floating-point
values recover bit for bit, including infinities, NaN payloads and signed zero.
Fixed and dynamic lists, bundles, sets, and dictionaries use representation-owned
checkpoint operations. Unsupported representations refuse checkpointing.

Sets and dictionaries retain their integer slots, capacity, and free-slot
allocation order. Restore does not compact holes or reconstruct membership by
inserting keys in a different order. Deferred erasures are normalized to the
same free-slot order that the uninterrupted graph's next structural mutation
would produce. Thus existing keys retain their slots and the next insertion
chooses the same slot. Component input boundaries forward the original
endpoint; they do not copy deltas into a second collection whose allocation
order might differ.

Supported keyed maps preserve membership and each live child graph at its
original slot, including child state and the key's modification time. Untouched
keys survive the next day's partial delta. A removed and subsequently re-added
key starts a fresh child rather than resurrecting a retired checkpoint state.
Saved capacity and the live/inactive slot partition are validated before child
storage is allocated.
The parent owns mapped output storage; child forwarding outputs are rebound
to it instead of imported a second time.

Dynamic-list maps similarly retain child indices and their creation clocks.
Shrinking a list retires its removed children; regrowing it creates fresh state.
Nested supported maps recover recursively.

Reductions preserve leaf order, source-slot associations, tree capacity,
combiner state, and hidden publication endpoints. Ordered reductions over
dictionaries and dynamic lists retain their input order. Fixed-list ordered
reductions can retain their internal reference selection as well. The component
result still follows the closed value-boundary rule. Recovering these structures
does not recompute old leaves.

Owned-output meshes preserve keyed instances, including dependency-created
instances, dependency edges, ranks, and pending removals. Their private sibling
and key-set subscriptions bind to the reconstructed instances without historical
notifications. This remains an owner-specific topology contract; ordinary
internal references use the locator contract below.

``dmap_`` recovers a component placed *inside* its child, in both hosting modes:

.. code-block:: python

   @component
   def pricing(ticks: TS[float]) -> TS[float]: ...      # recovered, per key

   prices = dmap_(pricing, ticks_by_symbol)

Configure recovery for ``pricing`` as you would if it were in the main graph; the
``dmap_`` itself need not be in any component. Each completed day saves the
component's state for every key from the workers that host it, and a restarted
run raises those workers with it restored. The child may hold more than the
component -- ``dmap_(lambda t: publish_ready(pricing(t)), ...)`` -- and whatever
is outside the component is *processed*, not recovered: it starts afresh with
each run, so keep state that has to survive inside the component.

* The ``dmap_``'s inputs are held to a component's input rules, because for
  recovery they *are* the component's inputs: each has to come straight from a
  source, and that source may feed nothing else. A computed input is refused
  when the graph is wired. If that is what you have, wrap the ``dmap_`` and
  whatever computes its inputs in a component instead, which also recovers: the
  workers are then saved whole, so everything in the child has to be
  recoverable.
* The worker count and hosting mode are part of the saved contract. Keys are
  placed by ``hash % workers`` and the placement is not stored, so a different
  count is an incompatible checkpoint, not a silent re-partition.
* A worker that cannot capture fails the completed day, and one that refuses its
  image fails the start. Neither falls back to a fresh worker. The other workers
  are not harmed by it: they are stopped as any graph is, so their stop hooks
  run.
* **Changing the child changes the contract, even outside the component.** Today
  the saved contract covers every node of the ``dmap_`` child, not only the
  component's, so a release that edits the merely *processed* part of the child
  starts cold: the old checkpoint is refused as incompatible rather than
  restored. Nothing is restored wrongly. If the processed part changes often,
  put it in a later ``spawn_`` stage or outside the ``dmap_``, where it is no part
  of the contract (RFC 0039, "Known limits").
* One worker's image has to fit one transport frame (64 MiB). A larger one fails
  the completed day and says so; use more workers.
* Over a fixed-size ``TSL`` the component is wired once per index and recovers
  with any worker count. Over an unbounded ``TSL`` it recovers with one worker
  only.
* Wrapping the child in a component costs nothing when nothing is being
  recovered: it adds no node.

``spawn_`` recovers a component placed *inside* a stage. A stage is a graph you
wrote, and the recoverable unit inside it is the same one as anywhere else:

.. code-block:: python

   @component
   def pricing(ticks: TS[float]) -> TS[float]: ...      # recovered

   @graph
   def stage(ticks: TS[float]) -> None:
       publish(pricing(ticks))                          # publish: processed

   spawn_(stage, ticks)

Configure recovery for ``pricing`` as you would if it were in the main graph.
Each completed day saves the component's state from the worker that hosts it,
and a restarted run raises that worker with the component restored and does not
re-send the input baselines it already holds. The component can share a stage
with the sink, as above, or sit in an earlier stage of a ``pipeline_``.

The pipeline's inputs are held to a component's input rules, because for
recovery they *are* the component's inputs: each has to come straight from a
source, and that source may feed nothing else. A computed input is refused when
the graph is wired. If that is what you have, wrap ``spawn_`` and whatever
computes its inputs in a component instead (below).

Everything outside the component is *processed*, not recovered -- above all the
sink the pipeline ends in. It acts in a worker process, recovery restores what
the component knew and cannot replay what the sink did, and the sink declares
nothing. Nodes outside the component start afresh on each run, so keep state
that has to survive a restart inside the component. Inside the stage the
component takes its inputs straight from the stage's inputs: if a node in the
stage computes one of them, move that node inside the component, or the graph is
refused when it is wired. A component nested under a ``map_`` in the stage is
not reached. Wrapping part of a stage in a component costs nothing when nothing
is being recovered: it adds no node.

If ``spawn_`` is itself wired inside a recoverable component, the whole pipeline
is that component's and every stage is saved whole, so every stage node has to
be recoverable.

A sink may sit inside a component. If it has recordable state it is recovered
through that state; if it has none it is *transient*: recovery leaves it alone,
it starts again on every run, and it can be added, removed or changed without
invalidating a checkpoint. A sink has no output, so nothing in the recovered
component can see what it forgot -- put whatever must survive a restart in
``RECORDABLE_STATE`` and the rest is free. A recoverable sink's pending
``NodeScheduler`` events are saved independently of that state. A transient
sink remains outside the image and starts normally on every run. Recovery never
replays what a sink did.

Two limits are ``map_``'s rather than ``dmap_``'s, and reach through it: a
``dmap_`` child has to end in a node that writes its own output, not in a
``reduce`` (the forwarding-terminal form ``map_`` cannot checkpoint yet), and a
child that holds ``STATE`` instead of ``RECORDABLE_STATE`` has nothing a
checkpoint can see. Both are refused at wiring with the node's name.

Count and duration ``TSW`` endpoints store one typed sequence of live samples
and a parallel sequence of original timestamps. Storage and loading are linear
in live samples; unused ring capacity and per-sample schemas are not serialized.
Restore preserves warmup and retained samples even when the endpoint has been
invalidated. It neither pushes historical values nor expires them while loading.
Duration windows retain their current behavior of expiring on incoming samples.
``to_window`` and its reset form use this support; operators with additional
undeclared private buffers still require their own checkpoint contract.

Internal references retain their kind (empty, peered, or a structural group
of references), declared target schema, and original endpoint modification
time. A bound target may still have an invalid value. Targets are saved as
component-relative graph, node, endpoint, and child-slot ordinals; runtime
addresses are never stored. Exact restored membership makes those ordinals
meaningful. Synthetic reference adapters also retain their construction recipe
and clocks, so a recovered selection keeps following its source when it changes.
This includes ordinary ``RECORDABLE_STATE`` fields declared as ``REF``.

Import does not publish ticks. The previous day's last value remains readable,
but it is not processed as a new event. Recovery first constructs saved dynamic
membership and imports owned endpoint values, collecting reference fixups.
After every target exists, it allocates required adapters, resolves internal
references, restores adapter clocks, and finalizes owner bindings. User start
hooks then run with restored state and references available; nested owners start
their prepared children before fresh evaluation.
Input value/structural activation and passivation are restored after each
node's start hook. This preserves stream operators that stop observing an
input after receiving a value. Activity paths through fixed input bundles,
including Python argument bundles, are supported; per-element activation
inside a peered collection is refused at capture.

Completion and failure
----------------------

After the last successful root evaluation and its deferred notifications,
the runtime captures an owned image before teardown. It publishes that image
only after normal stop succeeds. A failed evaluation, failed stop, failed
encoding, or early stop request does not create a completed-day object.

A checkpoint is a single immutable object. Native local storage stages and
syncs the file, publishes it with a no-replace operation, and attempts to sync
the directory; actual power-loss durability depends on the filesystem. The
store does not publish several independent state streams and infer a completed
day from their presence. Existing checkpoint keys cannot be overwritten.

Encoding fails closed. A value the codec cannot represent -- opaque Python
storage, for example -- raises while the image is being encoded, before
anything is published, and leaves the predecessor intact. Every image carries
a checksum over its whole content, verified on every read before any of it is
interpreted. ``ComponentCheckpointStore.write(..., verify=True)`` additionally
decodes the encoded image and requires that encoding it again reproduces the
same bytes; it roughly doubles the cost of a write and is off by default.

The caller selects the predecessor and new key explicitly. There is no mutable
latest pointer, graph migration, multi-writer run-head protocol, broker
transaction, or exactly-once external-effect guarantee in this first version.
A process that dies after publication can find the completed key on restart.

Current limits
--------------

Error capture inside a recoverable component is refused: swallowing an
evaluation failure would allow a partial day to appear complete. Exceptions
must propagate to the run boundary.

Ordinary semantic ``State`` in compute nodes, external sources
inside the boundary, and dynamic owners without checkpoint operations are
refused. Sinks are not: see above. A stateless compute node's declarative ``schedule_on_start`` bootstrap
is allowed; recovery discards that historical bootstrap instead of evaluating
the saved inputs again.

Pending ``NodeScheduler`` events (Python ``SCHEDULER``) have their own checkpoint
element, independent of user state. Completed events and cancelled alarms are
not saved. After the normal node ``start`` hook, recovery replaces its scheduler
data, rebuilds tagged lookup, and notifies the graph of the earliest pending
time, including propagation through nested graphs. An empty image clears
bootstrap alarms. A pending deadline at the restart time fires in that cycle;
a restart after a pending deadline is refused. Wall-clock recovery remains
outside the simulation-only component contract.

``schedule`` preserves its emitted-tick count as well as its pending deadlines.
For example, a three-tick schedule checkpointed after one tick resumes with two
remaining ticks at the original times. This applies to scalar and time-series
delays, with immediate or delayed first ticks. A completed budget remains
exhausted after restart; a fresh time-series ``start`` input still resets the
budget and re-bases the grid, replacing old alarms even when one is due in the
restart cycle. Changing scalar scheduling configuration makes an
old checkpoint incompatible. Recovery remains simulation-only.

``SingleShotScheduler`` is best effort: its schedules are not saved or recovered,
and its existing startup behaviour is unchanged. Recovering a native scheduler
does not recover an operator's private counters or buffers; these still require
recordable state or explicit checkpoint operations.

Checkpoint wire format 4 adds the scheduler element. Formats 2 and 3 remain
readable, but contain no scheduler recovery data and retain their previous
startup behaviour. Rebuild runtime extensions against the updated operations ABI.

Supported map and mesh forms write child terminal outputs into owned parent elements; general forwarding-terminal
variants still require further topology/reference contracts. The separate
``window`` operator family with private buffered state is not covered merely
because ``TSW`` endpoint recovery is available. Immediate ``const`` values and
``nothing`` placeholders are supported; delayed constants still require their
own source checkpoint contract.

Keyed interior reference adapters, such as observing ``TSD[K, REF[V]]`` as
``TSD[K, V]``, remain refused. This differs from a single ``REF[TSD[K, V]]``
pointing at an owned dictionary. References inside a custom owner's hidden
endpoint images also require that owner's explicit reference-aware recovery
contract; ordinary recordable-state endpoints already receive the component
reference context. General references crossing the component boundary remain
outside this implementation.

Images are written in format version 2
(:doc:`../rfc/rfc_0039_compact_checkpoint_images`). Version 1 images, published
by hgraph 0.8.25-0.8.27, remain readable: a day recovered from one publishes a
version 2 successor, and nothing needs migrating. Any other version is refused
explicitly, with no implicit schema or topology migration.

Image size and cost follow the state, not its shape. Each distinct schema, node
identifier and contract signature is written once per image; a collection
child names no schema, and a dense keyed collection stores no slot data. A
``TSD[int, TS[float]]`` costs about 19 bytes per key, which is less than the
three packed arrays a hand-written record of the same keys, values and times
would need.

An image is a full component checkpoint at a completed run boundary. Online
snapshot requests, suspend triggers, incremental physical chunks, and a durable
input journal for replaying the tail after a checkpoint remain later stages of
:doc:`../rfc/rfc_0023_graph_checkpoint_recovery`. Applications can choose daily
or larger completed intervals, but this API does not claim arbitrary running-
graph checkpoint support.
